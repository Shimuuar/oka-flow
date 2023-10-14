{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE DeriveFoldable             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveTraversable          #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost        #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeApplications           #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Graph
  ( -- * Dataflow graph
    Fun(..)
  , FlowGraph(..)
  , FIDSet(..)
  , Flow(..)
  , appendMeta
  , modifyMeta
  , scopeMeta
  , restrictMeta
  , projectMeta
    -- * Graph operations
  , hashFlowGraph
  , shakeFlowGraph
    -- * Lens
  , flowTgtL
  , flowGraphL
  , fidExistsL
  , fidWantedL
  ) where

import Control.Lens
import Control.Monad
import Control.Monad.Operational    hiding (view)
import Control.Monad.Trans.State.Strict
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.Aeson.KeyMap            qualified as KM
import Data.Aeson.Key               (toText)
import Data.ByteString.Lazy         qualified as BL
import Data.List                    (sortOn)
import Data.Map.Strict              (Map, (!))
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import Data.Vector                  qualified as V

import OKA.Metadata

import OKA.Flow.Types


----------------------------------------------------------------
-- Dataflow graph
----------------------------------------------------------------

-- | Single workflow bound in dataflow graph.
data Fun res k v = Fun
  { funWorkflow :: Workflow res
    -- ^ Execute workflow. It takes resource handle as parameter and
    --   tries to acquire resources and return actual function to run.
  , funMetadata :: Metadata
    -- ^ Metadata that should be supplied to the workflow
  , funParam    :: [k]
    -- ^ Parameters to workflow.
  , funOutput   :: v
    -- ^ Output of workflow
  }
  deriving stock (Functor, Foldable, Traversable)

-- | Complete dataflow graph
data FlowGraph res a = FlowGraph
  { flowGraph :: Map FunID (Fun res FunID a)
    -- ^ Dataflow graph with dependencies
  , flowTgt   :: Set FunID
    -- ^ Set of values for which we want to evaluate
  }
  deriving stock (Functor, Foldable, Traversable)


-- | Flow monad which we use to build workflow
newtype Flow res eff a = Flow
  (StateT (Metadata, FlowGraph res ()) (Program eff) a)
  deriving newtype (Functor, Applicative, Monad)

instance MonadFail (Flow res eff) where
  fail = error

-- | Add value which could be serialized to metadata to full medataset
appendMeta :: IsMeta a => a -> Flow res eff ()
appendMeta a = Flow $ _1 . metadata .= a

-- | Modify metadata using given function
modifyMeta :: (Metadata -> Metadata) -> Flow res eff ()
modifyMeta f = Flow $ _1 %= f

-- | Scope metadata modifications. All changes to metadata done in
--   provided callback will be discarded.
scopeMeta :: Flow res eff a -> Flow res eff a
scopeMeta (Flow action) = Flow $ do
  m <- use _1
  a <- action
  _1 .= m
  pure a

-- | Restrict metadata to set necessary for encoding value of given type
restrictMeta
  :: forall meta res eff a. IsMeta meta
  => Flow res eff a
  -> Flow res eff a
restrictMeta action = scopeMeta $ do
  modifyMeta (toMetadata . view (metadata @meta))
  action

projectMeta :: IsMeta a => Flow res eff a
projectMeta = Flow $ use (_1 . metadata)


----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

-- | Compute all hashes and resolve all nodes to corresponding paths
hashFlowGraph :: FlowGraph res () -> FlowGraph res (Maybe StorePath)
hashFlowGraph gr = res where
  res      = gr & flowGraphL . mapped %~ hashFun oracle
  oracle k = case res ^. flowGraphL . at k of
    Nothing -> error "INTERNAL ERROR: flow graph is not consistent"
    Just f  -> case funOutput f of
      Nothing -> error "INTERNAL ERROR: dependence on PHONY node"
      Just p  -> p

hashFun :: (k -> StorePath) -> Fun res k a -> Fun res k (Maybe StorePath)
hashFun oracle Fun{..} = Fun
  { funOutput = case funWorkflow of
      Workflow (Action nm _) -> Just $ StorePath nm (Hash hash)
      Phony{}                -> Nothing
  , ..
  }
  where
    hash   = SHA1.hashlazy $ BL.fromChunks [h | Hash h <- hashes]
    hashes = hashMeta funMetadata
           : [ case oracle k of StorePath _ h -> h
             | k <- funParam
             ]

-- Compute hash of metadata
hashMeta :: Metadata -> Hash
hashMeta
  = Hash
  . SHA1.hashlazy
  . JSONB.encodingToLazyByteString
  . encodeToBuilder
  . encodeMetadataDyn

encodeToBuilder :: JSON.Value -> JSONB.Encoding
encodeToBuilder JSON.Null       = JSONB.null_
encodeToBuilder (JSON.Bool b)   = JSONB.bool b
encodeToBuilder (JSON.Number n) = JSONB.scientific n
encodeToBuilder (JSON.String s) = JSONB.text s
encodeToBuilder (JSON.Array v)  = jsArray v
encodeToBuilder (JSON.Object m) = JSONB.dict JSONB.text encodeToBuilder
  (\step z m0 -> foldr (\(k,v) a -> step (toText k) v a) z $ sortOn fst $ KM.toList m0)
  m

jsArray :: V.Vector JSON.Value -> JSONB.Encoding
jsArray v
  | V.null v  = JSONB.emptyArray_
  | otherwise = JSONB.wrapArray
              $  encodeToBuilder (V.unsafeHead v)
             <@> V.foldr withComma (JSONB.Encoding mempty) (V.unsafeTail v)
  where
    withComma a z = JSONB.comma <@> encodeToBuilder a <@> z
    JSONB.Encoding e1 <@> JSONB.Encoding e2 = JSONB.Encoding (e1 <> e2)



-- | Remove all workflows that already completed execution.
shakeFlowGraph
  :: (Monad m)
  => (StorePath -> m Bool)           -- ^ Predicate to check whether path exists
  -> FlowGraph res (Maybe StorePath) -- ^ Dataflow graph
  -> m FIDSet
shakeFlowGraph tgtExists (FlowGraph workflows targets)
  = foldM addFID (FIDSet mempty mempty) targets
  where
    addFID fids@FIDSet{..} fid
      -- We already wisited these workflows
      | fid `Set.member` fidExists = pure fids
      | fid `Set.member` fidWanted = pure fids
      -- Check if result has been computed already
      | otherwise = do
          exists <- case funWorkflow f of
            -- Phony targets are always executed
            Phony{}    -> pure False
            Workflow{} -> case funOutput f of
              Just path -> tgtExists path
              Nothing   -> error "INTERNAL ERROR: dependence on phony target"
          case exists of
            True  -> pure $ fids & fidExistsL %~ Set.insert fid
            False -> foldM addFID fids' (funParam f)
      where
        f     = workflows ! fid
        fids' = fids & fidWantedL %~ Set.insert fid

data FIDSet = FIDSet
  { fidExists :: !(Set FunID) -- ^ Workflow already computed
  , fidWanted :: !(Set FunID) -- ^ We need to compute these workflows
  }
  deriving (Show)

----------------------------------------------------------------
-- Lens
----------------------------------------------------------------

fidExistsL, fidWantedL :: Lens' FIDSet (Set FunID)
fidExistsL = lens fidExists (\f x -> f {fidExists = x})
fidWantedL = lens fidWanted (\f x -> f {fidWanted = x})

flowTgtL :: Lens' (FlowGraph res a) (Set FunID)
flowTgtL = lens flowTgt (\x s -> x {flowTgt = s})

flowGraphL :: Lens (FlowGraph res a) (FlowGraph res b) (Map FunID (Fun res FunID a)) (Map FunID (Fun res FunID b))
flowGraphL = lens flowGraph (\x s -> x {flowGraph = s})
