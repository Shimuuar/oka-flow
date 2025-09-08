{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE OverloadedStrings   #-}
-- |
module TM.Flow (tests) where

import Control.Lens
import Control.Monad
import Data.IORef
import Data.Proxy
import Data.Maybe
import Data.Foldable
import Data.List        (sort)
import Data.Map.Strict  qualified as Map
import Data.Map.Strict  (Map)
import Effectful
import System.IO.Temp   (withSystemTempDirectory)
import System.FilePath  ((</>), splitPath)
import Test.Tasty
import Test.Tasty.HUnit
import GHC.Generics     (Generic)
import GHC.TypeLits

import OKA.Metadata
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Run
import OKA.Flow.Core.S
import OKA.Flow.Std
import OKA.Flow.Core.Resources
import OKA.Flow.Eff.Cache
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
    -- Hashes are computed in the same way
  , testCase "Hash stability" $ withSimpleFlow $ \ctx -> do
      (_, flowA) <- flowProduceInt @"nA"
      (_, flowB) <- flowProduceInt @"nB"
      (_, flowS) <- flowSquare
      obs <- newIORef []
      let meta = toMetadata ( CounterMeta 100 :: CounterMeta "nA"
                            , CounterMeta 200 :: CounterMeta "nB")
          flow = do a  <- flowA ()
                    b  <- flowB ()
                    rA <- flowS a
                    rB <- flowS b
                    observeOutput obs "a2cd17720b4b99db295d163503f2f964c56fa212" a
                    observeOutput obs "81f0aaa39ae1659e8f28a8e4e30fb4fde1769b4b" b
                    observeOutput obs "5593eb1a59553d6de638c48fbd06cd36324064c9" rA
                    observeOutput obs "6d44756d6dc99dc79ded6a84052f6002f3cc75a7" rB
      runFlow ctx meta flow
      readIORef obs >>= \case
        [] -> pure ()
        e  -> error $ unlines e
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
  , testCase "Caching of outputs" $ withSimpleFlow $ \ctx -> do
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
                    addExtMeta e1
                    want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA [200]
    -- Overwrite by another external meta works
  , testCase "externalMeta2" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do e1 <- stdSaveMeta (CounterMeta 200 :: CounterMeta "nA")
                    e2 <- stdSaveMeta (CounterMeta 300 :: CounterMeta "nA")
                    addExtMeta e1
                    addExtMeta e2
                    want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA [300]
    -- Overwrite by normal value works
  , testCase "externalMeta3" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do e1 <- stdSaveMeta (CounterMeta 200 :: CounterMeta "nA")
                    addExtMeta e1
                    appendMeta (CounterMeta 300 :: CounterMeta "nA")
                    want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA [300]
    -- Saving tuple of metadata works
  , testCase "externalMeta-pair" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsB, flowB) <- flowProduceInt @"nB"
      let flow = do e <- stdSaveMeta ( CounterMeta 100 :: CounterMeta "nA"
                                     , CounterMeta 200 :: CounterMeta "nB"
                                     )
                    addExtMeta e
                    want =<< flowA ()
                    want =<< flowB ()
      runFlow ctx mempty flow
      observe "flowA" obsA [100]
      observe "flowB" obsB [200]
    -- CacheE tests
    --
    -- We cheat here a bit by using IO to observe number of times
    -- cached computation is evaluated
  , testCase "cache metadata handling" $ withSimpleFlow $ \ctx -> do
      cnt <- newIORef (0::Int)
      runFlow ctx mempty $ do
        appendMeta (CounterMeta 100 :: CounterMeta "A")
        fun <- cache $ \i -> do
          CounterMeta n <- use (metadata @(CounterMeta "A"))
          appendMeta (CounterMeta 111 :: CounterMeta "A")
          liftEff $ liftIO $ modifyIORef' cnt succ
          pure (n*i)
        appendMeta (CounterMeta 200 :: CounterMeta "A")
        -- Cache sees metadata from call site of cache
        assertFlowEq "result" 300 =<< fun 3
        assertFlowEq "N eval 1" 1 =<< liftEff (liftIO (readIORef cnt))
        assertFlowEq "result" 300 =<< fun 3
        assertFlowEq "N eval 1" 1 =<< liftEff (liftIO (readIORef cnt))
        assertFlowEq "result" 400 =<< fun 4
        assertFlowEq "N eval 1" 2 =<< liftEff (liftIO (readIORef cnt))
        -- Metadata changes in cached function are discarded
        assertFlowEq "metadata" (CounterMeta 200) =<< use (metadata @(CounterMeta "A"))
  , testCase "memoize" $ withSimpleFlow $ \ctx -> do
      cnt <- newIORef (0::Int)
      runFlow ctx mempty $ do
        let memoized :: (CacheE :> eff, IOE :> eff) => Int -> Flow eff Int
            memoized = memoize (Proxy @MemoKey) $ \i -> do
              assertFlowEq "memoized" Nothing
                =<< lookupMeta @(Maybe (CounterMeta "A"))
              appendMeta (CounterMeta 200 :: CounterMeta "A")
              liftEff $ liftIO $ modifyIORef' cnt succ
              pure i
        assertFlowEq "result"   1 =<< memoized 1
        assertFlowEq "N eval 1" 1 =<< liftEff (liftIO (readIORef cnt))
        assertFlowEq "result"   1 =<< memoized 1
        assertFlowEq "N eval 1" 1 =<< liftEff (liftIO (readIORef cnt))
        assertFlowEq "result"   2 =<< memoized 2
        assertFlowEq "N eval 1" 2 =<< liftEff (liftIO (readIORef cnt))
  ]

withSimpleFlow :: (FlowCtx '[CacheE,IOE] -> IO a) -> IO a
withSimpleFlow action = withSystemTempDirectory "oka-flow" $ \dir -> do
  res <- createResource (LockCoreCPU 4)
  action FlowCtx { root      = dir
                 , runEffect = runCacheE
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
  deriving stock (Show,Eq,Generic)
  deriving MetaEncoding        via AsRecord    (CounterMeta a)
  deriving (IsMetaPrim,IsMeta) via AsMeta '[a] (CounterMeta a)

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

assertFlowEq :: (Eq a, Show a, Monad m) => String -> a -> a -> m ()
assertFlowEq msg expected got
  | expected == got = pure ()
  | otherwise       = error $ msg ++ ": expected " ++ show expected ++ " got " ++ show got


data MemoKey

flowProduceInt
  :: forall name eff. (KnownSymbol name)
  => IO (Observe Int, () -> Flow eff (Result Int))
flowProduceInt = do
  obs <- newObserve
  pure ( obs
       , liftWorkflow (LockCoreCPU 1) $ Action
         { name = "produce-" ++ (symbolVal (Proxy @name))
         , run  = \_ p -> do
             let n   = p.meta ^. metadata @(CounterMeta name) . to (.count)
                 out = fromJust p.out
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
         , run  = \_ p -> do
             let Param arg = p.args
                 out       = fromJust p.out
             n <- read @Int <$> readFile (arg </> "out.txt")
             let n' = n * n
             saveObservation obs out n'
             writeFile (out </> "out.txt") (show n')
         }
       )

flowPhony :: IO (ObsPhony Int, Result Int -> Flow eff ())
flowPhony = do
  obs <- newObsPhony
  pure ( obs
       , basicLiftPhony (LockCoreCPU 1) $ PhonyAction $ \_ p -> do
           let Param arg = p.args
           n <- read @Int <$> readFile (arg </> "out.txt")
           let n' = n * n
           saveObsPhony obs n'
       )

observeOutput :: IORef [String] -> FilePath -> Result a -> Flow eff ()
observeOutput out expected = basicLiftPhony () $ PhonyAction $ \_ p -> do
  let Param arg = p.args
  let hash = last (splitPath arg)
  when (hash /= expected) $
    atomicModifyIORef' out $ \e ->
      ( [ "Hash mismatch:"
        , "  Expected: " ++ expected
        , "  Got:      " ++ hash
        ] ++ e
      , ())
