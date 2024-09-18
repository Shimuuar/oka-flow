{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
-- |
module TM.Flow (tests) where

import Control.Lens
import Data.IORef
import Data.Proxy
import Data.Foldable
import Data.List        (sort)
import Data.Map.Strict  qualified as Map
import Data.Map.Strict  (Map)
import Effectful
import System.IO.Temp   (withSystemTempDirectory)
import System.FilePath  ((</>))
import Test.Tasty
import Test.Tasty.HUnit
import GHC.Generics     (Generic)
import GHC.TypeLits

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Run
import OKA.Flow.Std
import OKA.Flow.Resources
import OKA.Flow

tests :: TestTree
tests = testGroup "Run flow"
  [ testCase "Single workflow" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata ( CounterMeta 100 :: CounterMeta "nA")
          flow = flowA ()
      runFlow ctx meta flow
      observe "flow A" obsA [100]
    -- Forcing of pair works
  , testCase "Pair of workflows" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsB, flowB) <- flowProduceInt @"nB"
      let meta = toMetadata ( CounterMeta 100 :: CounterMeta "nA"
                            , CounterMeta 200 :: CounterMeta "nB")
          flow = do a <- flowA ()
                    b <- flowB ()
                    pure (a,b)
      runFlow ctx meta flow
      observe "flow A" obsA [100]
      observe "flow B" obsB [200]
    -- Hashes for dependency are computed correctly
  , testCase "Hash for dependency" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsB, flowB) <- flowProduceInt @"nB"
      (obsS, flowS) <- flowSquare
      let meta = toMetadata ( CounterMeta 100 :: CounterMeta "nA"
                            , CounterMeta 200 :: CounterMeta "nB")
          flow = do a <- flowA ()
                    b <- flowB ()
                    want =<< flowS a
                    want =<< flowS b
      runFlow ctx meta flow
      observe "flow A" obsA [100]
      observe "flow B" obsB [200]
      observe "flow S" obsS [100_00, 400_00]
    -- Unused workflow is not executed
  , testCase "Dead workflow" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsB, flowB) <- flowProduceInt @"nB"
      let meta = toMetadata ( CounterMeta 100 :: CounterMeta "nA"
                            , CounterMeta 200 :: CounterMeta "nB")
          flow = do a <- flowA ()
                    _ <- flowB ()
                    pure a
      runFlow ctx meta flow
      observe "flow A" obsA [100]
      observe "flow B" obsB []
    -- Chain of workflow
  , testCase "Chain" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsS, flowS) <- flowSquare
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do a <- flowA ()
                    flowS a
      runFlow ctx meta flow
      observe "flow A" obsA [100]
      observe "flow S" obsS [10000]
    -- Caching works
  , testCase "Caching" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsS, flowS) <- flowSquare
      let meta  = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow1 = flowA ()
          flow2 = do a <- flowA ()
                     flowS a
      runFlow ctx meta flow1
      runFlow ctx meta flow2
      observe "flow A" obsA [100]
      observe "flow S" obsS [10000]
    -- Caching works for phony targets
  , testCase "Phony" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsS, flowS) <- flowPhony
      let meta  = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow1 = flowA ()
          flow2 = do a <- flowA ()
                     s <- flowS a
                     pure s
      runFlow ctx meta flow1
      runFlow ctx meta flow2
      observe      "flow A" obsA [100]
      observePhony "flow S" obsS [10000]
   -- Duplicate workflows are evaluated only once
  , testCase "Duplicate" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do want =<< flowA ()
                    want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA [100]
    -- External metadata works
  , testCase "externalMeta" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do e1 <- stdSaveMeta (CounterMeta 200 :: CounterMeta "nA")
                    withExtMeta e1 $ want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA [200]
  , testCase "externalMeta2" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do e1 <- stdSaveMeta (CounterMeta 200 :: CounterMeta "nA")
                    e2 <- stdSaveMeta (CounterMeta 300 :: CounterMeta "nA")
                    withExtMeta e1 $ withExtMeta e2 $ want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA [300]
  ]

withSimpleFlow :: (FlowCtx '[] -> IO a) -> IO a
withSimpleFlow action = withSystemTempDirectory "oka-flow" $ \dir -> do
  res <- createResource (LockCoreCPU 4)
     =<< pure mempty
  action FlowCtx { root      = dir
                 , runEffect = raise
                 , res       = res
                 , logger    = mempty
                 }


----------------------------------------------------------------
-- Flows
----------------------------------------------------------------

-- Metadata data type we're using
data CounterMeta (a :: Symbol) = CounterMeta
  { count :: Int
  }
  deriving stock Generic
  deriving MetaEncoding via AsRecord    (CounterMeta a)
  deriving IsMeta       via AsMeta '[a] (CounterMeta a)


-- Tool for observation of execution of normal flows
newtype Observe a = Observe (IORef (Map FilePath a))

-- Create new ovservatory
newObserve :: IO (Observe a)
newObserve = Observe <$> newIORef mempty

-- Save execution of a workflow
saveObservation :: Observe a -> FilePath -> a -> IO ()
saveObservation (Observe ref) out a = atomicModifyIORef' ref $ \m ->
  case Map.lookup out m of
    Just _  -> error "saveObservation: workflow run twice"
    Nothing -> (Map.insert out a m, ())

-- Observe single execution of a workflow
observe :: (Ord a, Show a) => String -> Observe a -> [a] -> IO ()
observe msg (Observe ref) a = do
  assertEqual msg a =<< (sort . toList <$> readIORef ref)


-- Tool for observation of execution of phony flows
newtype ObsPhony a = ObsPhony (IORef [a])

-- Create new ovservatory
newObsPhony :: IO (ObsPhony a)
newObsPhony = ObsPhony <$> newIORef mempty

-- Save execution of a workflow
saveObsPhony :: ObsPhony a -> a -> IO ()
saveObsPhony (ObsPhony ref) a = atomicModifyIORef' ref $ \xs -> ((a:xs),())

-- Require that workflow was not executed at all
observePhony :: (Ord a, Show a) => String -> ObsPhony a -> [a] -> IO ()
observePhony msg (ObsPhony ref) xs =
  assertEqual msg xs =<< (sort . toList <$> readIORef ref)





flowProduceInt
  :: forall name eff. (KnownSymbol name)
  => IO (Observe Int, () -> Flow eff (Result Int))
flowProduceInt = do
  obs <- newObserve
  pure ( obs
       , liftWorkflow (LockCoreCPU 1) $ Action
         { name = "produce-" ++ (symbolVal (Proxy @name))
         , run  = \_ meta [] out -> do
             let n = meta ^. metadata @(CounterMeta name) . to (.count)
             saveObservation obs out n
             writeFile (out </> "out.txt") (show n)
         }
       )

flowSquare :: IO (Observe Int, Result Int -> Flow eff (Result Int))
flowSquare = do
  obs <- newObserve
  pure ( obs
       , liftWorkflow (LockCoreCPU 1) $ Action
         { name = "square"
         , run  = \_ _ [p] out -> do
             n <- read @Int <$> readFile (p </> "out.txt")
             let n' = n * n
             saveObservation obs out n'
             writeFile (out </> "out.txt") (show n')
         }
       )

flowPhony :: IO (ObsPhony Int, Result Int -> Flow eff ())
flowPhony = do
  obs <- newObsPhony
  pure ( obs
       , basicLiftPhony (LockCoreCPU 1) $ \_ _ [p] -> do
           n <- read @Int <$> readFile (p </> "out.txt")
           let n' = n * n
           saveObsPhony obs n'
       )
