{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE RecordWildCards #-}
-- |
-- Evaluator of dataflow graph.
module OKA.Flow.Core.Run
  ( FlowCtx(..)
  , FlowLogger(..)
  , runFlow
  ) where

import Control.Applicative
import Control.Monad
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Lens
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Map.Strict              (Map,(!))
import Data.Map.Strict              qualified as Map
import Data.Monoid                  (Endo(..))
import Data.Set                     qualified as Set
import Data.Time                    (NominalDiffTime,getCurrentTime,diffUTCTime)
import Data.Typeable
import Data.Void
import Effectful
import Effectful.State.Static.Local qualified as Eff

import System.FilePath              ((</>))
import System.Process.Typed
import System.Environment           (getEnvironment)
import System.Directory             (createDirectory,createDirectoryIfMissing,renameDirectory,removeDirectoryRecursive,
                                     doesDirectoryExist
                                    )
import System.Posix.Signals         (signalProcess, sigINT)

import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Flow
import OKA.Flow.Core.Resources
import OKA.Flow.Core.S
import OKA.Flow.Core.Types
import OKA.Flow.Internal.Util

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
  { root      :: FilePath
    -- ^ Root directory for cache
  , runEffect :: forall a. Eff eff a -> Eff '[IOE] a
    -- ^ Evaluator for effects allowed in dataflow program
  , res       :: ResourceSet
    -- ^ Resources which are available for evaluator.
  , logger    :: FlowLogger
    -- ^ Logger for evaluator
  }


data Barricaded a = Barricaded
  { barrier :: Barrier
  , val     :: a
  }

----------------------------------------------------------------
-- Execution
----------------------------------------------------------------

-- | Execute dataflow program
runFlow
  :: (ToS r)
  => FlowCtx eff -- ^ Evaluation context
  -> Metadata    -- ^ Metadata for program
  -> Flow eff r  -- ^ Dataflow program
  -> IO ()
runFlow ctx@FlowCtx{runEffect} meta (Flow m) = do
  -- Evaluate dataflow graph
  gr <- fmap (deduplicateGraph . hashFlowGraph)
      $ runEff
      $ runEffect
      $ fmap (\(r,st) -> addTargets r st.graph)
      $ Eff.runState FlowSt{ meta  = absurd <$> meta
                           , graph = FlowGraph mempty mempty
                           }
      $ m
  -- Prepare graph for evaluation
  targets <- shakeFlowGraph targetExists gr
  gr_exe  <- addMVars targets gr
  let getStorePath fids = concat [ gr ^.. flowGraphL . at fid . _Just . to (.output) . _Just
                                 | fid <- toList fids
                                 ]
  ctx.logger.init (getStorePath targets.exists)
                  (getStorePath targets.wanted)
  -- Prepare dictionary of external metadata which is loaded from
  -- store path
  ext_meta <- prepareExtMetaCache ctx gr_exe targets
  -- Evaluator
  mapConcurrently_ id
    [ prepareFun ctx gr_exe ext_meta (gr_exe.graph ! i)
    | i <- Set.toList targets.wanted
    ]
  where
    addTargets r = flowTgtL %~ mappend ((Set.fromList . toList . toS) r)
    targetExists path = doesDirectoryExist (ctx.root </> storePath path)

-- Add MVars to each node of graph. They are used to block evaluator
-- until all dependencies are ready
addMVars :: FIDSet -> FlowGraph a -> IO (FlowGraph (Barricaded a))
addMVars FIDSet{exists}
  = traverseOf flowGraphL
  $ Map.traverseWithKey $ \fid fun -> do
      v <- if | fid `Set.member` exists -> newOpenBarrier
              | otherwise               -> newBarrier
      pure (Barricaded v <$> fun)

-- Prepare functixon for evaluation
prepareFun
  :: FlowCtx eff                              -- Evaluation context
  -> FlowGraph (Barricaded (Maybe StorePath)) -- Full dataflow graph
  -> ExtMetaCache                             -- External metadata
  -> Fun FunID (Barricaded (Maybe StorePath)) -- Function to evaluate
  -> IO ()
prepareFun ctx FlowGraph{graph=gr} ext_meta fun = crashReport ctx.logger fun $ do
  -- Check that all function parameters are already evaluated:
  for_ fun.param $ \fid -> checkBarrier (outputOf fid).barrier
  -- Compute metadata which should be passed to the workflow by
  -- applying data loaded from
  meta <- traverseMetadata (lookupExtCache ext_meta) fun.metadata
  -- Request resources & run action
  withResources ctx.res fun.resources $ do
    case fun.workflow of
      -- Prepare normal action. We first create output directory and
      -- write everything there. After we're done we rename it.
      Workflow (Action _ act)    -> prepareNormal meta (act ctx.res)
      WorkflowExe exe            -> prepareExe    meta exe
      -- Execute phony action. We don't need to bother with setting up output
      Phony    (PhonyAction act) -> preparePhony  meta (act ctx.res)
    -- Signal that we successfully completed execution
    clearBarrier fun.output.barrier
  where
    outputOf k = case gr ^. at k of
      Just f  -> f.output
      Nothing -> error "INTERNAL ERROR: inconsistent flow graph"
    --
    paramP = toPath . outputOf      <$> fun.param  -- Parameters relative to store
    params = (\p -> ctx.root </> p) <$> paramP     -- Parameters as real path
    
    toPath (Barricaded _ (Just path)) = storePath path
    toPath (Barricaded _ Nothing    ) = error "INTERNAL ERROR: dependence on PHONY node"
    -- Execution of haskell action
    prepareNormal meta action = normalExecution meta $ \build ->
      action ParamFlow { meta = meta
                       , args = params
                       , out  = Just build
                       }
    preparePhony meta action = normalExecution meta $ \_ ->
      action ParamFlow { meta = meta
                       , args = params
                       , out  = Nothing
                       }
    -- Execution of an external executable
    prepareExe meta exe@Executable{call} = normalExecution meta $ \build -> do
      let param = ParamFlow { meta = meta
                            , args = params
                            , out  = Just build
                            }
      call param $ \process -> do
        env <- case process.env of
          [] -> pure []
          es -> do env <- getEnvironment
                   pure $ env ++ es
        let run = case process.workdir of
                    Nothing -> id
                    Just p  -> setWorkingDir p
                $ case process.stdin of
                    Nothing -> id
                    Just bs -> setStdin (byteStringInput bs)
                $ case env of
                    [] -> id
                    _  -> setEnv env
                $ proc exe.name process.args
        withProcessWait_ run $ \pid -> do
          _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
          pure ()
    -- Standard wrapper for execution of workflows that create outputs
    normalExecution meta action = do
      let path  = case fun.output.val of
            Just p -> p
            _      -> error "INTERNAL ERROR: phony node treated as normal"
      ctx.logger.start path
      t1 <- getCurrentTime
      () <- withBuildDirectory ctx.root path $ \build -> do
        BL.writeFile (build </> "meta.json") $ JSON.encode $ encodeMetadata meta
        -- FIXME: I need to properly write deps.txt
        -- writeFile    (build </> "deps.txt")  $ unlines paramP
        action build
      t2 <- getCurrentTime
      ctx.logger.done path (diffUTCTime t2 t1)



-- Report crash in case of exception
crashReport :: FlowLogger -> Fun i (Barricaded (Maybe StorePath)) -> IO x -> IO x
crashReport logger fun = handle $ \e0@(SomeException e) -> do
  let path = fun.output.val
  if | Just AsyncCancelled <- cast e
       -> pure ()
     | Just (SomeAsyncException e') <- cast e
     , Just AsyncCancelled          <- cast e'
       -> pure ()
     | otherwise -> logger.crash path e0
  throwIO e

-- Create directory with -build prefix and rename it to final name
withBuildDirectory :: FilePath -> StorePath -> (FilePath -> IO a) -> IO a
withBuildDirectory root path action = do
  createDirectoryIfMissing False group
  createDirectory build
  a <- action build `onException` removeDirectoryRecursive build
  a <$ renameDirectory build out
  where
    group = root </> path.name
    out   = root </> storePath path
    build = out <> "-build"



----------------------------------------------------------------
-- Cache for external metadata. We only want to load data once
----------------------------------------------------------------


newtype ExtMetaCache = ExtMetaCache (Map (TypeRep, FunID) SomeExtMeta)

data SomeExtMeta where
  SomeExtMeta :: IsMetaPrim a => IO a -> SomeExtMeta

lookupExtCache :: forall a. Typeable a => ExtMetaCache -> FunID -> IO a
lookupExtCache (ExtMetaCache cache) fid = 
  case Map.lookup (ty, fid) cache of
    Nothing -> error "INTERNAL ERROR: missing entry in cache"
    Just (SomeExtMeta io) -> do
      a <- io
      case cast a of
        Nothing -> error "INTERNAL ERROR: corrupted ext_meta cache"
        Just a' -> pure a'
  where
    ty = typeRep (Proxy @a)


prepareExtMetaCache
  :: FlowCtx eff
  -> FlowGraph (Barricaded (Maybe StorePath))
  -> FIDSet
  -> IO ExtMetaCache
prepareExtMetaCache ctx gr targets = do
  cache <- traverse (\(SomeExtMeta io) -> SomeExtMeta <$> once io)
         $ Map.fromList
         $ flip appEndo []
         $ foldMap
           (\fun -> getConst $ traverseMetadata toCache fun.metadata)
           gr.graph
  pure $ ExtMetaCache cache
  where
    toCache :: forall a. IsMetaPrim a => FunID -> Const (Endo [((TypeRep, FunID), SomeExtMeta)]) a
    toCache fid
      = Const
      $ Endo
      ( ( (typeRep (Proxy @a), fid)
        , SomeExtMeta $ if
            | fid `Set.member` targets.wanted ->
                 do checkBarrier (gr.graph ! fid).output.barrier
                    readMeta @a path
            | otherwise ->
                do readMeta @a path
        )
      :)
      where
        path = case (gr.graph ! fid).output.val of
          Just p  -> ctx.root </> storePath p </> "saved.json"
          Nothing -> error "Dependence on PHONY target"
    --
    readMeta :: IsMetaPrim a => FilePath -> IO a
    readMeta path = do
      (JSON.eitherDecode <$> BL.readFile path) >>= \case
        Left  e  -> error e
        Right js -> case decodeMetadataPrimEither js of
          Left  e -> throw e
          Right a -> pure a


----------------------------------------------------------------
-- Helpers
----------------------------------------------------------------

-- Kill process but allow it to die gracefully by sending SIGINT
-- first. GHC install handler for it but not for SIGTERM
softKill :: Process stdin stdout stderr -> IO ()
softKill p = getPid p >>= \case
  Nothing  -> pure ()
  Just pid -> do
    delay <- registerDelay 1_000_000
    signalProcess sigINT pid
    atomically $  (check =<< readTVar delay)
              <|> (void $ waitExitCodeSTM p)
