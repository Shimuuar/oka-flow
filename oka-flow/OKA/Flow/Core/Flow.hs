{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE RecordWildCards     #-}
-- |
-- Implementation of flow monad which is used for defining dataflow
-- graph and primitives for working with it
module OKA.Flow.Core.Flow
  ( -- * Flow monad
    Flow(..)
  , FlowSt(..)
  , appendMeta
  , scopeMeta
  , withEmptyMeta
  , restrictMeta
    -- * Defining workflows
  , want
  , basicLiftWorkflow
  , liftWorkflow
  , basicLiftPhony
  , basicLiftExe
    -- * Lens
  , stMetaL
  , stGraphL
  ) where


import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Monad.State.Strict
import Data.Map.Strict              ((!))
import Data.Map.Strict              qualified as Map
import Data.Set                     qualified as Set
import Effectful
import Effectful.State.Static.Local qualified as Eff

import OKA.Metadata
import OKA.Flow.Types
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Resources


----------------------------------------------------------------
-- Flow monad and primitives
----------------------------------------------------------------

-- | Flow monad which we use to build workflow
newtype Flow eff a = Flow
  (Eff (Eff.State FlowSt : eff) a)
  deriving newtype (Functor, Applicative, Monad)

-- | State of 'Flow' monad
data FlowSt = FlowSt
  { meta  :: !MetadataFlow
  , graph :: !(FlowGraph ())
  }

instance MonadFail (Flow eff) where
  fail = error

instance MonadState MetadataFlow (Flow eff) where
  get   = Flow $ Eff.gets   @FlowSt (.meta)
  put m = Flow $ Eff.modify @FlowSt (stMetaL .~ m)

instance Semigroup a => Semigroup (Flow eff a) where
  (<>) = liftA2 (<>)
instance Monoid a => Monoid (Flow eff a) where
  mempty = pure mempty


-- | Add value which could be serialized to metadata to full medataset
appendMeta :: IsMeta a => a -> Flow eff ()
appendMeta a = metadata .= a

-- | Scope metadata modifications. All changes to metadata done in
--   provided callback will be discarded.
scopeMeta :: Flow eff a -> Flow eff a
scopeMeta action = do
  m <- get
  action <* put m

-- | Execute workflow with empty metadata
withEmptyMeta :: Flow eff a -> Flow eff a
withEmptyMeta action = scopeMeta $ do
  put mempty
  action

-- | Restrict metadata to set necessary for encoding value of given type
restrictMeta
  :: forall meta eff a. IsMeta meta
  => Flow eff a
  -> Flow eff a
restrictMeta action = scopeMeta $ do
  modify (restrictMetaByType @meta)
  action


----------------------------------------------------------------
-- Defining effects
----------------------------------------------------------------

-- | We want given workflow evaluated
want :: ResultSet a => a -> Flow eff ()
want a = Flow $ Eff.modify $ stGraphL . flowTgtL %~ (<> Set.fromList (toResultSet a))

-- | Basic primitive for creating workflows. It doesn't offer any type
--   safety so it's better to use other tools
basicLiftWorkflow
  :: (ResultSet params, ResourceClaim res)
  => res      -- ^ Resource required by workflow
  -> Workflow -- ^ Workflow to be executed
  -> params   -- ^ Parameters of workflow
  -> Flow eff (Result a)
basicLiftWorkflow resource exe p = Flow $ do
  st  <- Eff.get @FlowSt
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
  Eff.modify $ stGraphL . flowGraphL . at fid .~ Just Fun
    { workflow  = exe
    , metadata  = st.meta
    , output    = ()
    , resources = claimResource resource
    , param     = res
    }
  return $ Result fid

-- | Create new primitive workflow.
--
--   This function does not provide any type safety by itself!
liftWorkflow
  :: (ResultSet params, ResourceClaim res)
  => res    -- ^ Resources required by workflow
  -> Action -- ^ Action to execute
  -> params -- ^ Parameters
  -> Flow eff (Result a)
liftWorkflow res action p = basicLiftWorkflow res (Workflow action) p

-- | Lift phony workflow (not checked)
basicLiftPhony
  :: (ResultSet params, ResourceClaim res)
  => res
     -- ^ Resources required by workflow
  -> (ResourceSet -> Metadata -> [FilePath] -> IO ())
     -- ^ Action to execute
  -> params
     -- ^ Parameters to pass to workflow
  -> Flow eff ()
basicLiftPhony res exe p = want =<< basicLiftWorkflow res (Phony (PhonyAction exe)) p

-- | Lift executable into workflow
basicLiftExe
  :: (ResultSet params, ResourceClaim res)
  => res
     -- ^ Resources required by workflow
  -> Executable
     -- ^ Action to execute
  -> params
     -- ^ Parameters to pass to workflow
  -> Flow eff (Result a)
basicLiftExe res exe p = basicLiftWorkflow res (WorkflowExe exe) p


----------------------------------------------------------------
-- Lens
----------------------------------------------------------------

stMetaL :: Lens' FlowSt MetadataFlow
stMetaL = lens (.meta) (\FlowSt{..} x -> FlowSt{meta=x, ..})

stGraphL :: Lens' FlowSt (FlowGraph ())
stGraphL = lens (.graph) (\FlowSt{..} x -> FlowSt{graph=x, ..})
