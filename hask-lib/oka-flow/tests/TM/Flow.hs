{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE TypeApplications    #-}
-- |
module TM.Flow (tests) where

import Control.Lens hiding((.=))
import Control.Monad.Operational
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State.Strict
import Data.Aeson       (Value(..),(.=),object)
import Data.Set         qualified as Set
import Data.IORef
import Data.Text (Text,unpack)
import Data.HashMap.Strict qualified as HM
import System.IO.Temp   (withSystemTempDirectory)
import System.FilePath  ((</>))
import System.Directory (doesDirectoryExist,listDirectory)
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
  ]

withSimpleFlow :: (FlowCtx IO () -> IO a) -> IO a
withSimpleFlow action = withSystemTempDirectory "oka-flow" $ \dir -> do
  action FlowCtx { flowCtxRoot   = dir
                 , flowTgtExists = doesDirectoryExist
                 , flowCtxEff    = id
                 , flowCtxRes    = ()
                 }

emptyMetadata :: Metadata
emptyMetadata = Metadata $ Object mempty

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
       , liftWorkflow Workflow
         { workflowName = "produce-" ++ unpack k
         , workflowRun  = ActNormal $ pure $ \(Metadata (Object m)) [] out -> do
             let Just (Number n) = m ^. at k
                 n' = round n :: Int
             assertBool "Flow must called only once" =<<
               atomicModifyIORef' (obsVal  obs)
                 (\case
                     Nothing -> (Just n', True)
                     Just _  -> (Just n', False))
             writeIORef (obsPath obs) out
             writeFile (out </> "out.txt") (show n')
         }
       )

flowSquare :: IO (Observe Int, Result Int -> Flow res eff (Result Int))
flowSquare = do
  obs <- Observe <$> newIORef Nothing <*> newIORef ""
  pure ( obs
       , liftWorkflow Workflow
         { workflowName = "square"
         , workflowRun  = ActNormal $ pure $ \_ [p] out -> do
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
