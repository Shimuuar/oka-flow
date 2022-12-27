{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE RankNTypes          #-}
-- |
module TM.Flow (tests) where

import Control.Monad.Operational
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State.Strict
import Data.Aeson       (Value(..))
import Data.Set         qualified as Set
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
  [ testCase "Run simple workflow" $ withSystemTempDirectory "oka-flow" $ \dir -> do
      let flow = flowProduceA ()
      runFlow dir doesDirectoryExist (Metadata (Object mempty)) () id flow
      [a] <- listDirectory dir
      print =<< readFile (dir </> a)
  ]

runFlow
  :: ResultSet r
  => FilePath
  -> (FilePath -> IO Bool)
  -> Metadata
  -> res
  -> (forall a. eff a -> IO a)
  -> Flow res eff r
  -> IO ()
runFlow root tgtExists meta res interpret (Flow m) = do
  (r,fgr) <- interpretWithMonad interpret
           $ flip runStateT  (FlowGraph mempty mempty)
           $ flip runReaderT meta
           $ m
  let FlowGraph gr tgt = fgr
      gr' = hashFlowGraph gr
  target <- shakeFlowGraph
              (\path -> tgtExists (root </> storePath path))
              (Set.fromList (toResultSet r) <> tgt)
              gr'
  executeWorkflow root res gr' target

flowProduceA, flowProduceB :: () -> Flow res eff (Result Int)
flowProduceA = liftWorkflow (produceInt 100 )
flowProduceB = liftWorkflow (produceInt 2000)

produceInt :: Int -> Workflow res
produceInt n = Workflow
  { workflowRun  = ActNormal $ \_ -> pure $ \_ [] out -> do putStrLn out
                                                            writeFile out (show n)
  , workflowName = "produceInt"
  }
