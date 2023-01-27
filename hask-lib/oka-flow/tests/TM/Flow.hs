{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE TypeApplications    #-}
-- |
module TM.Flow (tests) where

import Data.Aeson       ((.=),object)
import Data.IORef
import Data.Text        (Text,unpack)
import System.IO.Temp   (withSystemTempDirectory)
import System.FilePath  ((</>))
import System.Directory (doesDirectoryExist)
import Test.Tasty
import Test.Tasty.HUnit

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Run
import OKA.Flow

tests :: TestTree
tests = testGroup "Run flow"
  [ testCase "Single workflow" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt "nA"
      let meta = Metadata $ object [ "nA" .= (100::Int) ]
          flow = flowA ()
      runFlow ctx meta flow
      observe "flow A" obsA (Just 100)
    -- Forcing of pair works
  , testCase "Pair of workflows" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt "nA"
      (obsB, flowB) <- flowProduceInt "nB"
      let meta = Metadata $ object [ "nA" .= (100::Int)
                                   , "nB" .= (200::Int)
                                   ]
          flow = do a <- flowA ()
                    b <- flowB ()
                    pure (a,b)
      runFlow ctx meta flow
      observe "flow A" obsA (Just 100)
      observe "flow B" obsB (Just 200)
    -- Unused workflow is not executed
  , testCase "Dead workflow" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt "nA"
      (obsB, flowB) <- flowProduceInt "nB"
      let meta = Metadata $ object [ "nA" .= (100::Int)
                                   , "nB" .= (200::Int)
                                   ]
          flow = do a <- flowA ()
                    _ <- flowB ()
                    pure a
      runFlow ctx meta flow
      observe "flow A" obsA (Just 100)
      observe "flow B" obsB Nothing
    -- Chain of workflow
  , testCase "Chain" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt "nA"
      (obsS, flowS) <- flowSquare
      let meta = Metadata $ object [ "nA" .= (100::Int) ]
          flow = do a <- flowA ()
                    flowS a
      runFlow ctx meta flow
      observe "flow A" obsA (Just 100)
      observe "flow S" obsS (Just 10000)
    -- Caching works
  , testCase "Caching" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt "nA"
      (obsS, flowS) <- flowSquare
      let meta  = Metadata $ object [ "nA" .= (100::Int) ]
          flow1 = flowA ()
          flow2 = do a <- flowA ()
                     flowS a
      runFlow ctx meta flow1
      runFlow ctx meta flow2
      observe "flow A" obsA (Just 100)
      observe "flow S" obsS (Just 10000)
    -- Caching works for phony targets
  , testCase "Phony" $ withSimpleFlow $ \ctx -> do
      (obsA, flowA) <- flowProduceInt "nA"
      (obsS, flowS) <- flowPhony
      let meta  = Metadata $ object [ "nA" .= (100::Int) ]
          flow1 = flowA ()
          flow2 = do a <- flowA ()
                     s <- flowS a
                     pure s
      runFlow ctx meta flow1
      runFlow ctx meta flow2
      observe "flow A" obsA (Just 100)
      observe "flow S" obsS (Just 10000)
  ]

withSimpleFlow :: (FlowCtx IO () -> IO a) -> IO a
withSimpleFlow action = withSystemTempDirectory "oka-flow" $ \dir -> do
  action FlowCtx { flowCtxRoot   = dir
                 , flowTgtExists = doesDirectoryExist
                 , flowCtxEff    = id
                 , flowCtxRes    = ()
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


-- exists :: Observe a -> IO (Maybe a)
-- exists = readIORef . obsVal



flowProduceInt :: Text -> IO (Observe Int, () -> Flow res eff (Result Int))
flowProduceInt k = do
  obs <- Observe <$> newIORef Nothing <*> newIORef ""
  pure ( obs
       , liftWorkflow $ Workflow Action 
         { workflowName = "produce-" ++ unpack k
         , workflowRun  = \_ meta [] out -> do
             let n = lookupMeta meta [k]
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
