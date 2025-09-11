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
  , basicLiftPhony
    -- * Lens
  , stMetaL
  , stGraphL
  ) where


import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Monad.State.Strict
import Data.Foldable                (toList)
import Data.Map.Strict              ((!))
import Data.Map.Strict              qualified as Map
import Data.Set                     qualified as Set
import Data.Proxy
import Effectful
import Effectful.State.Static.Local qualified as Eff

import OKA.Metadata
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Resources
import OKA.Flow.Core.Result
import OKA.Flow.Core.S

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
  , graph :: !(FlowGraph Proxy)
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
want :: ToS a => a -> Flow eff ()
want a = Flow $ Eff.modify $ stGraphL . flowTgtL %~ (<> (Set.fromList . toList . toS) a)

-- | Basic primitive for creating workflows. It doesn't offer any type
--   safety so it's better to use other tools
basicLiftWorkflow
  :: (ToS params, ResourceClaim res)
  => res      -- ^ Resource required by workflow
  -> Dataflow -- ^ Workflow to be executed
  -> params   -- ^ Parameters of workflow
  -> Flow eff (Result a)
basicLiftWorkflow resource exe p = Flow $ do
  st <- Eff.get @FlowSt
  let fid = AResult $ freshFunID st.graph
  Eff.modify $ stGraphL . flowGraphL . at fid .~ Just Fun
    { workflow  = WorkflowNormal exe
    , metadata  = st.meta
    , output    = Proxy
    , resources = claimResource resource
    , param     = toS p
    }
  return $ Result fid

-- | Lift phony workflow (not checked)
basicLiftPhony
  :: (ToS params, ResourceClaim res)
  => res
     -- ^ Resources required by workflow
  -> Action
     -- ^ Action to execute
  -> params
     -- ^ Parameters to pass to workflow
  -> Flow eff ()
basicLiftPhony resource exe p = Flow $ do
  st <- Eff.get @FlowSt
  let fid = APhony $ freshFunID st.graph
  Eff.modify $ stGraphL . flowPhonyL . at fid .~ Just Fun
    { workflow  = WorkflowPhony exe
    , metadata  = st.meta
    , output    = Proxy
    , resources = claimResource resource
    , param     = toS p
    }
   

freshFunID :: FlowGraph f -> FunID
freshFunID gr = max
  (case Map.lookupMax gr.graph of
      Just (AResult (FunID i), _) -> FunID (i + 1)
      Nothing                     -> FunID 0
  )
  (case Map.lookupMax gr.phony of
      Just (APhony (FunID i), _) -> FunID (i + 1)
      Nothing                    -> FunID 0
  )

----------------------------------------------------------------
-- Lens
----------------------------------------------------------------

stMetaL :: Lens' FlowSt MetadataFlow
stMetaL = lens (.meta) (\FlowSt{..} x -> FlowSt{meta=x, ..})

stGraphL :: Lens' FlowSt (FlowGraph Proxy)
stGraphL = lens (.graph) (\FlowSt{..} x -> FlowSt{graph=x, ..})
