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
          ctx  = FlowCtx
            { flowCtxRoot   = dir
            , flowTgtExists = doesDirectoryExist
            , flowCtxEff    = id
            , flowCtxRes    = ()
            }
      runFlow ctx (Metadata (Object mempty)) flow
      [a] <- listDirectory dir
      print =<< readFile (dir </> a)
  , testCase "Run simple workflow 2x" $ withSystemTempDirectory "oka-flow" $ \dir -> do
      let flow = do a <- flowProduceA ()
                    b <- flowProduceB ()
                    pure (a,b)
          ctx  = FlowCtx
            { flowCtxRoot   = dir
            , flowTgtExists = doesDirectoryExist
            , flowCtxEff    = id
            , flowCtxRes    = ()
            }
      runFlow ctx (Metadata (Object mempty)) flow
      [a,b] <- listDirectory dir
      print =<< readFile (dir </> a)
  ]


flowProduceA, flowProduceB :: () -> Flow res eff (Result Int)
flowProduceA = liftWorkflow (produceInt 100 )
flowProduceB = liftWorkflow (produceInt 2000)

produceInt :: Int -> Workflow res
produceInt n = Workflow
  { workflowRun  = ActNormal $ pure $ \_ [] out -> do putStrLn out
                                                      writeFile out (show n)
  , workflowName = "produceInt-" ++ show n
  }


main = defaultMain tests
