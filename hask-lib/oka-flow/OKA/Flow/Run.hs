{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TupleSections       #-}
-- |
module OKA.Flow.Run
  ( FlowCtx(..)
  , runFlow
  ) where

import Control.Applicative
import Control.Concurrent.Async
import Control.Concurrent.STM.TMVar
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.Operational
import Control.Monad.Trans.Reader
import Control.Monad.Trans.State.Strict
import Control.Monad.STM
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Map.Strict              ((!))
import Data.Set                     qualified as Set
import System.FilePath              ((</>))
import System.Directory             (createDirectory,renameDirectory,removeDirectoryRecursive)

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Types

----------------------------------------------------------------
--
----------------------------------------------------------------


-- | Evaluation context for dataflow program
data FlowCtx eff res = FlowCtx
  { flowCtxRoot   :: FilePath
    -- ^ Root directory for cache
  , flowTgtExists :: FilePath -> IO Bool
    -- ^ Check that target does exists
  , flowCtxEff    :: forall a. eff a -> IO a
    -- ^ Evaluator for effect allowed in dataflow program
  , flowCtxRes    :: res
    -- ^ Resources
  }


----------------------------------------------------------------
-- Execution
----------------------------------------------------------------

-- | Execute dataflow program
runFlow
  :: (ResultSet r)
  => FlowCtx eff res -- ^ Evaluation context
  -> Metadata        -- ^ Metadata for program
  -> Flow res eff r  -- ^ Dataflow program
  -> IO ()
runFlow ctx@FlowCtx{..} meta (Flow m) = do
  -- Evaluate dataflow graph
  gr <- interpretWithMonad flowCtxEff
      $ fmap addTargets
      $ flip runStateT  (FlowGraph mempty mempty)
      $ flip runReaderT meta
      $ m
  -- Prepare graph for evaluation
  let gr_hashed = hashFlowGraph gr
  targets <- shakeFlowGraph targetExists gr_hashed
  gr_exe  <- addTMVars gr_hashed
  -- Evaluator
  runTasks flowCtxRes []
    [ prepareFun ctx targets (flowGraph gr_exe ! i)
    | i <- Set.toList $ fidWanted targets ]
  where
    addTargets (r,gr) = gr & flowTgtL %~ mappend (Set.fromList (toResultSet r))
    targetExists path = flowTgtExists (flowCtxRoot </> storePath path)


-- Add TMVars to each node of graph. This is needed in order to 
addTMVars :: FlowGraph res a -> IO (FlowGraph res (TMVar (), a))
addTMVars = traverse $ \f -> do v <- newEmptyTMVarIO
                                pure (v,f)

-- Prepare function for evaluation
prepareFun
  :: FlowCtx eff res                        -- Evaluation context
  -> FIDSet                                 -- Our target set
  -> Fun res (FunID, (TMVar (), StorePath)) -- Function to evaluate
  -> BracketSTM res (IO ())  
prepareFun FlowCtx{..} FIDSet{..} Fun{..}
  =  checkAtFinish (putTMVar tmvar ())
  *> checkAtStart depsEvaluated
  *> case workflowRun funWorkflow of
       ActNormal act -> prepareNormal <$> act
       ActPhony  act -> preparePhony  <$> act
  where
    meta   = funMetadata
    params = toPath <$> funParam
    out    = toPath     funOutput -- Output directory
    build  = out ++ "-build"      -- Temporary build directory
    --
    (_,(tmvar,_))       = funOutput  
    toPath (_,(_,path)) = flowCtxRoot </> storePath path
    -- We need to check that all dependencies which are not completed
    -- already are evaluated
    depsEvaluated = sequence_
      [ when (fid `Set.notMember` fidExists) $ readTMVar v
      | (fid, (v,_)) <- funParam
      ]
    -- Prepare output directory
    prepareNormal action = do
      createDirectory build
      BL.writeFile (build </> "meta.json") $ JSON.encode $ let Metadata m = meta in m
      _ <- action meta params build `onException` removeDirectoryRecursive build
      renameDirectory build out
    preparePhony action = action meta params

    
-- Main loop which concurrently runs tasks
runTasks
  :: res
  -> [(STM (), Async ())]     -- Already running actions
  -> [BracketSTM res (IO ())] -- Tasks to be run
  -> IO ()
runTasks res = go where
  go []     []    = pure ()
  go asyncs tasks = do
    r <- atomically $ asum
      [ CmdRunTask  <$> pickSTM (runBracketSTM res) tasks
      , CmdTaskDone <$> pickSTM (\(fini,a) -> do r <- waitCatchSTM a
                                                 pure (fini,r)
                                ) asyncs
      -- FIXME: deadlock detection
      ]
    case r of
      CmdRunTask (tasks',   (fini, io      )) -> withAsync io $ \a -> go ((fini,a):asyncs) tasks'
      CmdTaskDone (_,       (_   , Left  e )) -> throwIO e
      CmdTaskDone (asyncs', (fini, Right ())) -> do atomically fini
                                                    go asyncs' tasks

data Cmd res
  = CmdRunTask  ([BracketSTM res (IO ())], (STM (), IO ()))
  | CmdTaskDone ([(STM (), Async ())],     (STM (), Either SomeException ()))

pickSTM :: (a -> STM b) -> [a] -> STM ([a], b)
pickSTM fun = go id where
  go _   []     = retry
  go asF (a:as) =  do b <- fun a
                      pure (asF as, b)
               <|> go (asF . (a:)) as
