{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TupleSections       #-}
-- |
module OKA.Flow.Run where
  -- ( executeWorkflow
  -- ) where

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
import Data.Foldable
import Data.Map.Strict              ((!))
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import System.FilePath              ((</>))

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow
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

addTMVars :: FlowGraph res a -> IO (FlowGraph res (TMVar (), a))
addTMVars = traverse $ \f -> do v <- newEmptyTMVarIO
                                pure (v,f)


prepareFun
  :: FlowCtx eff res
  -> FIDSet
  -> Fun res (FunID, (TMVar (), StorePath))
  -> BracketSTM res (IO ())  
prepareFun FlowCtx{..} FIDSet{..} Fun{..}
  =  checkAtFinish (putTMVar tmvar ())
  *> checkAtStart depsEvaluated
  *> case workflowRun funWorkflow of
       ActNormal act -> (\f -> f meta params out) <$> act
       ActPhony  act -> (\f -> f meta params)     <$> act
  where
    meta   = funMetadata
    params = toPath <$> funParam
    out    = toPath     funOutput
    --
    (_,(tmvar,_))       = funOutput  
    toPath (_,(_,path)) = flowCtxRoot </> storePath path
    -- We need to check that all dependencies which are not completed
    -- already are evaluated
    depsEvaluated = sequence_
      [ when (fid `Set.notMember` fidExists) $ readTMVar v
      | (fid, (v,_)) <- funParam
      ]


runFlow
  :: (ResultSet r)
  => FlowCtx eff res
  -> Metadata
  -> Flow res eff r
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

  -- -- Prepare for evaluation
  -- let FlowGraph gr tgt = fgr
  --     gr' = hashFlowGraph gr
  -- target <- shakeFlowGraph
  --             (\path -> flowTgtExists (flowCtxRoot </> storePath path))
  --             (Set.fromList (toResultSet r) <> tgt)
  --             gr'
  -- --
  -- let 
  -- undefined


runTasks
  :: res
  -> [(STM (), Async ())]     -- ^ Already running actions
  -> [BracketSTM res (IO ())] -- ^ Tasks to be run
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
-- executeWorkflow
--   :: FilePath
--   -> res
--   -> Map FunID (Fun res (FunID, StorePath))
--   -> FIDSet
--   -> IO ()
-- executeWorkflow root res workflows FIDSet{..} = do
--   -- ready <- traverse (const newEmptyTMVarIO) workflows
--   -- go ready [] (toList fidWanted)
--   -- where
--   --   go _     []     []    = pure ()
--   --   go ready asyncs tasks = do
--   --     r <- atomically $ asum
--   --       [ CmdRunTask  <$> pickSTM (useFun ready) tasks
--   --       , CmdTaskDone <$> pickSTM waitCatchSTM   asyncs
--   --       ]
--   --     case r of
--   --       CmdRunTask  (tasks',  io)        -> withAsync io $ \a -> go ready (a:asyncs) tasks'
--   --       CmdTaskDone (asyncs', Right fid) -> do atomically $ putTMVar (ready ! fid) ()
--   --                                              go ready asyncs' tasks
--   --       CmdTaskDone (_      , Left  e)   -> throwIO e
--   --       CmdNothingToDo                   -> error "Deadlock"                                           
--   --   --
--   --   useFun ready fid = paramOK >> case workflowRun $ funWorkflow fun of
--   --     ActNormal run -> (\f -> fid <$ f meta param out) <$> run res
--   --     ActPhony  run -> (\f -> fid <$ f meta param)     <$> run res
--   --     where
--   --       fun     = workflows ! fid
--   --       meta    = funMetadata fun
--   --       param   = [root </> storePath path | (_,path) <- funParam fun]
--   --       paramOK = sequence_ [ readTMVar (ready ! i) | (i,_) <- funParam fun]
--   --       out     = root </> storePath (snd (funOutput fun))
--   undefined

data Cmd res
  = CmdRunTask  ([BracketSTM res (IO ())], (STM (), IO ()))
  | CmdTaskDone ([(STM (), Async ())],     (STM (), Either SomeException ()))


pickSTM :: (a -> STM b) -> [a] -> STM ([a], b)
pickSTM fun = go id where
  go _   []     = retry
  go asF (a:as) =  do b <- fun a
                      pure (asF as, b)
               <|> go (asF . (a:)) as
