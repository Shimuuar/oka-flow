{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
-- |
module OKA.Flow.Run
  ( FlowCtx(..)
  , runFlow
  ) where

import Control.Concurrent.Async
import Control.Concurrent.STM.TMVar
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.Operational
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
  , flowEvalReport :: [StorePath] -> [StorePath] -> IO ()
    -- ^ Function to report
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
  gr <- fmap hashFlowGraph
      $ interpretWithMonad flowCtxEff
      $ fmap (\(r,(_,gr)) -> addTargets r gr)
      $ runStateT m (meta, FlowGraph mempty mempty)
  -- Prepare graph for evaluation
  targets <- shakeFlowGraph targetExists gr
  gr_exe  <- addTMVars gr
  let getStorePath fids = concat [ gr ^.. flowGraphL . at fid . _Just . to (.output) . _Just
                                 | fid <- toList fids
                                 ]
      paths_wanted = getStorePath targets.wanted
      paths_exist  = getStorePath targets.exists
  flowEvalReport paths_exist paths_wanted
  -- Evaluator
  mapConcurrently_ id
    [ prepareFun ctx gr_exe targets (gr_exe.graph ! i) flowCtxRes
    | i <- Set.toList targets.wanted
    ]
  where
    addTargets r gr = gr & flowTgtL %~ mappend (Set.fromList (toResultSet r))
    targetExists path = flowTgtExists (flowCtxRoot </> storePath path)


-- Add TMVars to each node of graph. This is needed in order to
addTMVars :: FlowGraph res a -> IO (FlowGraph res (TMVar (), a))
addTMVars = traverse $ \f -> do v <- newEmptyTMVarIO
                                pure (v,f)

-- Prepare function for evaluation
prepareFun
  :: FlowCtx eff res                           -- Evaluation context
  -> FlowGraph res (TMVar (), Maybe StorePath) -- Full dataflow graph
  -> FIDSet                                    -- Our target set
  -> Fun res FunID (TMVar (), Maybe StorePath) -- Function to evaluate
  -> res
  -> IO ()
prepareFun FlowCtx{..} FlowGraph{graph=gr} FIDSet{..} fun res = do
  -- Check that all function parameters are already evaluated:
  for_ fun.param $ \fid -> when (fid `Set.notMember` exists) $
    atomically $ readTMVar (fst $ outputOf fid)
  -- Run action
  case fun.workflow of
    -- Prepare normal action. We first create output directory and
    -- write everything there. After we're done we rename it.
    Workflow (Action _ act) -> prepareNormal act
    -- Execute phony action. We don't need to bother with setting up output
    Phony    act            -> act res meta params
  -- Signal that we successfully complete execution
  atomically $ putTMVar (fst fun.output) ()
  where
    outputOf k = case gr ^. at k of
      Just f  -> f.output
      Nothing -> error "INTERNAL ERROR: inconsistent flow graph"
    --
    meta   = fun.metadata                        -- Metadata
    paramP = toPath . outputOf <$> fun.param     -- Parameters relative to store
    params = [ flowCtxRoot </> p | p <- paramP ] -- Parameters as real path
    --
    toPath (_,Just path) = storePath path
    toPath (_,Nothing  ) = error "INTERNAL ERROR: dependence on PHONY node"
    --
    prepareNormal action = do
      let out    = flowCtxRoot </> toPath fun.output   -- Output directory
          build  = out ++ "-build"                     -- Temporary build directory
      createDirectory build
      BL.writeFile (build </> "meta.json") $ JSON.encode $ encodeMetadataDyn meta
      writeFile    (build </> "deps.txt")  $ unlines paramP
      _ <- action res meta params build `onException` removeDirectoryRecursive build
      renameDirectory build out
