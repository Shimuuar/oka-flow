-- |
-- Simple framework for dataflow programming for data
-- analysis. Primary target is OKA experiment.
--
-- It uses two-stage evaluation. There's computation with metadata and
-- building of dependency graph that's assumed to be fast and
-- execution of dataflow stages which are long lasting and produce
-- their results as data in file system. This is needed for caching.
--
-- Spawning workers as separate processes is needed in order to solve
-- following problems: 1) we need to mix code written in different
-- languages 2) do something about long compile times of haskell
-- programs. Latter is solved by having many small programs instead
-- one large one. This way we can recompile only few modules at time.
module OKA.Flow
  ( -- * Flow monad
    Flow
  , appendMeta
  , scopeMeta
  , restrictMeta
  , Result
  , ResultSet(..)
  , want
  , liftEff
    -- * Defining workflows
  , Workflow(..)
  , Action(..)
  , basicLiftWorkflow
  , liftWorkflow
  , basicLiftPhony
  , liftPhony
    -- * Resources
  , Resource(..)
  , ResAsMutex(..)
  , ResAsCounter(..)
    -- * Execution
  , FlowCtx(..)
  , FlowLogger(..)
  , runFlow
  ) where

import Control.Lens
import Control.Monad.State.Strict
import Control.Monad.Operational
import Data.Map.Strict            ((!))
import Data.Map.Strict            qualified as Map
import Data.Set                   qualified as Set

import OKA.Metadata
import OKA.Flow.Graph
import OKA.Flow.Types
import OKA.Flow.Run
import OKA.Flow.Tools
import OKA.Flow.Resources


----------------------------------------------------------------
-- Flow monad
----------------------------------------------------------------

-- | We want given workflow evaluated
want :: ResultSet a => a -> Flow eff ()
want a = Flow $ stGraphL . flowTgtL %= (<> Set.fromList (toResultSet a))


-- | Lift effect
liftEff :: eff a -> Flow eff a
liftEff = Flow . lift . singleton


----------------------------------------------------------------
-- Defining effects
----------------------------------------------------------------

-- | Basic primitive for creating workflows. It doesn't offer any type
--   safety so it's better to use other tools
basicLiftWorkflow
  :: (ResultSet params, Resource res)
  => res      -- ^ Resource required by workflow
  -> Workflow -- ^ Workflow to be executed
  -> params   -- ^ Parameters of workflow
  -> Flow eff (Result a)
basicLiftWorkflow resource exe p = Flow $ do
  st <- get
  -- Allocate new ID
  let fid = case Map.lookupMax st.graph.graph of
              Just (FunID i, _) -> FunID (i + 1)
              Nothing           -> FunID 0
  -- Dependence on function without result is an error
  let res = toResultSet p
      phonyDep i = isPhony $ (st.graph.graph ! i).workflow
  when (any phonyDep res) $ do
    error "Depending on phony target"
  -- Add workflow to graph
  stGraphL . flowGraphL . at fid .= Just Fun
    { workflow   = exe
    , metadata   = st.meta
    , output     = ()
    , resources  = resourceLock resource
    , param      = res
    }
  return $ Result fid

-- | Create new primitive workflow.
--
--   This function does not provide any type safety by itself!
liftWorkflow
  :: (ResultSet params, Resource res)
  => res    -- ^ Resources required by workflow
  -> Action -- ^ Action to execute
  -> params -- ^ Parameters
  -> Flow eff (Result a)
liftWorkflow res action p = basicLiftWorkflow res (Workflow action) p

-- | Lift phony workflow (not checked)
basicLiftPhony
  :: (ResultSet params, Resource res)
  => res
     -- ^ Resources required by workflow
  -> (ResourceSet -> Metadata -> [FilePath] -> IO ())
     -- ^ Action to execute
  -> params
     -- ^ Parameters to pass to workflow
  -> Flow eff ()
basicLiftPhony res exe p = want =<< basicLiftWorkflow res (Phony exe) p

-- | Lift phony action using standard tools
liftPhony
  :: (Resource res, FlowArgument args)
  => res
     -- ^ Resources required by workflow
  -> (Metadata -> args -> IO ())
  -> AsRes args
  -> Flow eff ()
liftPhony res exe = basicLiftPhony res $ \_ meta args -> do
  a <- runFlowArguments args
  exe meta a
