{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MultiWayIf       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE RecordWildCards  #-}
-- |
-- Evaluator of dataflow graph.
module OKA.Flow.Run
  ( FlowCtx(..)
  , FlowLogger(..)
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
import Data.Time                    (NominalDiffTime,getCurrentTime,diffUTCTime)
import Data.Typeable
import System.FilePath              ((</>))
import System.Directory             (createDirectory,createDirectoryIfMissing,renameDirectory,removeDirectoryRecursive)

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Resources
import OKA.Flow.Types
import OKA.Flow.Tools



----------------------------------------------------------------
--
----------------------------------------------------------------


-- | Structure which carry callbacks for logging of events during
--   execution
data FlowLogger = FlowLogger
  { init  :: [StorePath] -> [StorePath] -> IO ()
    -- | Called when we evaluation of workflow starts. First
    --   parameter is already existing output, second is one
    --   which we need to evaluate.
  , start :: StorePath -> IO ()
    -- | Called when job is started
  , done  :: StorePath -> NominalDiffTime -> IO ()
    -- | Called when job finishes execution
  , crash :: Maybe StorePath -> SomeException -> IO ()
    -- | Called when job crashes
  }

instance Semigroup FlowLogger where
  a <> b = FlowLogger { init  = a.init  <> b.init
                      , start = a.start <> b.start
                      , done  = a.done  <> b.done
                      , crash = a.crash <> b.crash
                      }

instance Monoid FlowLogger where
  mempty = FlowLogger { init  = mempty
                      , start = mempty
                      , done  = mempty
                      , crash = mempty
                      }

-- | Evaluation context for dataflow program
data FlowCtx eff = FlowCtx
  { flowCtxRoot    :: FilePath
    -- ^ Root directory for cache
  , flowTgtExists  :: FilePath -> IO Bool
    -- ^ Function which is used to check whether output exists
  , flowCtxEff     :: forall a. eff a -> IO a
    -- ^ Evaluator for effects allowed in dataflow program
  , flowCtxRes     :: ResourceSet
    -- ^ Resources which are available for evaluator.
  , logger         :: FlowLogger
    -- ^ Logger for evaluator
  }


----------------------------------------------------------------
-- Execution
----------------------------------------------------------------

-- | Execute dataflow program
runFlow
  :: (ResultSet r)
  => FlowCtx eff -- ^ Evaluation context
  -> Metadata    -- ^ Metadata for program
  -> Flow eff r  -- ^ Dataflow program
  -> IO ()
runFlow ctx@FlowCtx{..} meta (Flow m) = do
  -- Evaluate dataflow graph
  gr <- fmap (deduplicateGraph . hashFlowGraph)
      $ interpretWithMonad flowCtxEff
      $ fmap (\(r,(_,gr)) -> addTargets r gr)
      $ runStateT m (meta, FlowGraph mempty mempty)
  -- Prepare graph for evaluation
  targets <- shakeFlowGraph targetExists gr
  gr_exe  <- addTMVars gr
  let getStorePath fids = concat [ gr ^.. flowGraphL . at fid . _Just . to (.output) . _Just
                                 | fid <- toList fids
                                 ]
  ctx.logger.init (getStorePath targets.exists)
                  (getStorePath targets.wanted)
  -- Evaluator
  mapConcurrently_ id
    [ prepareFun ctx gr_exe targets (gr_exe.graph ! i) flowCtxRes
    | i <- Set.toList targets.wanted
    ]
  where
    addTargets r gr = gr & flowTgtL %~ mappend (Set.fromList (toResultSet r))
    targetExists path = flowTgtExists (flowCtxRoot </> storePath path)


-- Add TMVars to each node of graph. This is needed in order to
addTMVars :: FlowGraph a -> IO (FlowGraph (TMVar (), a))
addTMVars = traverse $ \f -> do v <- newEmptyTMVarIO
                                pure (v,f)

-- Prepare function for evaluation
prepareFun
  :: FlowCtx eff                           -- Evaluation context
  -> FlowGraph (TMVar (), Maybe StorePath) -- Full dataflow graph
  -> FIDSet                                -- Our target set
  -> Fun FunID (TMVar (), Maybe StorePath) -- Function to evaluate
  -> ResourceSet
  -> IO ()
prepareFun ctx@FlowCtx{..} FlowGraph{graph=gr} FIDSet{..} fun res = crashReport ctx fun $ do
  -- Check that all function parameters are already evaluated:
  for_ fun.param $ \fid -> when (fid `Set.notMember` exists) $
    atomically $ readTMVar (fst $ outputOf fid)
  -- Request resources
  atomically $ fun.requestRes res
  -- Run action
  case fun.workflow of
    -- Prepare normal action. We first create output directory and
    -- write everything there. After we're done we rename it.
    Workflow (Action _ act) -> prepareNormal (act res)
    WorkflowExe exe         -> prepareExe    exe
    -- Execute phony action. We don't need to bother with setting up output
    Phony    act            -> act res meta params
  -- Signal that we successfully completed execution
  atomically $ do
    putTMVar (fst fun.output) ()
    fun.releaseRes res
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
    -- Execution of haskell action
    prepareNormal action = normalExecution $ \build -> do
      BL.writeFile (build </> "meta.json") $ JSON.encode $ encodeMetadata meta
      writeFile    (build </> "deps.txt")  $ unlines paramP
      action meta params build
    -- Execution of an extenrnal executable
    prepareExe exe@Executable{} = normalExecution $ \build -> do
      BL.writeFile (build </> "meta.json") $ JSON.encode $ encodeMetadata meta
      writeFile    (build </> "deps.txt")  $ unlines paramP
      exe.io res
      runExternalProcess exe.executable meta (build:params)
    -- Standard wrapper for execution of workflows that create outputs
    normalExecution action = do
      let path  = case fun.output of
            (_, Just p) -> p
            _           -> error "INTERNAL ERROR: phony node treated as normal"
      let out   = flowCtxRoot </> storePath path -- Output directory
          build = out ++ "-build"                -- Temporary build directory
      -- Create output directory
      createDirectoryIfMissing False (flowCtxRoot </> path.name)
      createDirectory build
      ctx.logger.start path
      t1 <- getCurrentTime
      _  <- action build `onException` removeDirectoryRecursive build
      t2 <- getCurrentTime
      ctx.logger.done path (diffUTCTime t2 t1)
      --
      renameDirectory build out


crashReport :: FlowCtx eff -> Fun i (a, Maybe StorePath) -> IO x -> IO x
crashReport ctx fun = handle $ \e0@(SomeException e) -> do
  let path = snd fun.output
  if | Just AsyncCancelled <- cast e -> pure ()
     | otherwise -> ctx.logger.crash path e0
  throwIO e
