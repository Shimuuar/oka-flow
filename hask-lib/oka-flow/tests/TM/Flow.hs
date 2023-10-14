{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
-- |
module TM.Flow (tests) where

import Control.Lens     hiding ((.=))
import Data.Aeson       ((.=),object)
import Data.IORef
import Data.Proxy
import Data.Text        (Text,unpack)
import System.IO.Temp   (withSystemTempDirectory)
import System.FilePath  ((</>))
import System.Directory (doesDirectoryExist)
import Test.Tasty
import Test.Tasty.HUnit
import GHC.Generics     (Generic)
import GHC.TypeLits

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Run
import OKA.Flow

tests :: TestTree
tests = testGroup "Run flow"
  [ testCase "Single workflow" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata ( CounterMeta 100 :: CounterMeta "nA")
          flow = flowA ()
      runFlow ctx meta flow
      observe "flow A" obsA (Just 100)
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
      observe "flow A" obsA (Just 100)
      observe "flow B" obsB (Just 200)
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
      observe "flow A" obsA (Just 100)
      observe "flow B" obsB Nothing
    -- Chain of workflow
  , testCase "Chain" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      (obsS, flowS) <- flowSquare
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do a <- flowA ()
                    flowS a
      runFlow ctx meta flow
      observe "flow A" obsA (Just 100)
      observe "flow S" obsS (Just 10000)
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
      observe "flow A" obsA (Just 100)
      observe "flow S" obsS (Just 10000)
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
      observe "flow A" obsA (Just 100)
      observe "flow S" obsS (Just 10000)
   -- Duplicate workflows are evaluated only once
  , testCase "Duplicate" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt @"nA"
      let meta = toMetadata (CounterMeta 100 :: CounterMeta "nA")
          flow = do want =<< flowA ()
                    want =<< flowA ()
      runFlow ctx meta flow
      observe "flow" obsA (Just 100)
  ]

withSimpleFlow :: (FlowCtx IO () -> IO a) -> IO a
withSimpleFlow action = withSystemTempDirectory "oka-flow" $ \dir -> do
  action FlowCtx { flowCtxRoot    = dir
                 , flowTgtExists  = doesDirectoryExist
                 , flowCtxEff     = id
                 , flowCtxRes     = ()
                 , flowEvalReport = \_ _ -> pure ()
                 }


----------------------------------------------------------------
-- Flows
----------------------------------------------------------------

data Observe a = Observe
  { obsVal  :: IORef (Maybe a)
  , obsPath :: IORef FilePath
  }

observe :: (Eq a, Show a) => String -> Observe a -> Maybe a -> IO ()
observe msg (Observe val _) a = do
  assertEqual (msg ++ " value") a =<< readIORef val

data CounterMeta (a :: Symbol) = CounterMeta
  { count :: Int
  }
  deriving stock Generic
  deriving MetaEncoding via AsRecord    (CounterMeta a)
  deriving IsMeta       via AsMeta '[a] (CounterMeta a)


-- exists :: Observe a -> IO (Maybe a)
-- exists = readIORef . obsVal



flowProduceInt
  :: forall name res eff. (KnownSymbol name)
  => IO (Observe Int, () -> Flow res eff (Result Int))
flowProduceInt = do
  obs <- Observe <$> newIORef Nothing <*> newIORef ""
  pure ( obs
       , liftWorkflow $ Workflow Action 
         { workflowName = "produce-" ++ (symbolVal (Proxy @name))
         , workflowRun  = \_ meta [] out -> do
             let n = meta ^. metadata @(CounterMeta name) . to count
             assertBool "Flow must called only once" =<<
               atomicModifyIORef' (obsVal  obs)
                 (\case
                     Nothing -> (Just n, True)
                     Just _  -> (Just n, False))
             writeIORef (obsPath obs) out
             writeFile (out </> "out.txt") (show n)
         }
       )

flowSquare :: IO (Observe Int, Result Int -> Flow res eff (Result Int))
flowSquare = do
  obs <- Observe <$> newIORef Nothing <*> newIORef ""
  pure ( obs
       , liftWorkflow $ Workflow Action
         { workflowName = "square"
         , workflowRun  = \_ _ [p] out -> do
             n <- read @Int <$> readFile (p </> "out.txt")
             let n' = n * n
             assertBool "Flow must called only once" =<<
               atomicModifyIORef' (obsVal  obs)
                 (\case
                     Nothing -> (Just n', True)
                     Just _  -> (Just n', False))
             writeIORef (obsPath obs) out
             writeFile (out </> "out.txt") (show n')
         }
       )

flowPhony :: IO (Observe Int, Result Int -> Flow res eff (Result ()))
flowPhony = do
  obs <- Observe <$> newIORef Nothing <*> newIORef ""
  pure ( obs
       , liftWorkflow $ Phony $ \_ _ [p] -> do
           n <- read @Int <$> readFile (p </> "out.txt")
           let n' = n * n
           assertBool "Flow must called only once" =<<
             atomicModifyIORef' (obsVal  obs)
               (\case
                   Nothing -> (Just n', True)
                   Just _  -> (Just n', False))
       )
