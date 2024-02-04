{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Graph
  ( -- * Dataflow graph
    Fun(..)
  , FlowGraph(..)
  , FIDSet(..)
  , Flow(..)
  , appendMeta
  , scopeMeta
  , restrictMeta
    -- * Graph operations
  , hashFlowGraph
  , deduplicateGraph
  , shakeFlowGraph
    -- * Lens
  , flowTgtL
  , flowGraphL
  , fidExistsL
  , fidWantedL
  ) where

import Control.Concurrent.STM       (STM)
import Control.Lens
import Control.Monad
import Control.Monad.Operational    hiding (view)
import Control.Monad.State.Strict
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.Aeson.KeyMap            qualified as KM
import Data.Aeson.Key               (toText)
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.List                    (sortOn)
import Data.List.NonEmpty           qualified as NE
import Data.List.NonEmpty           (NonEmpty(..))
import Data.Map.Strict              (Map, (!))
import Data.Map.Strict              qualified as Map
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import Data.Text                    qualified as T
import Data.Text.Encoding           qualified as T
import Data.Vector                  qualified as V

import OKA.Metadata

import OKA.Flow.Types


----------------------------------------------------------------
-- Dataflow graph
----------------------------------------------------------------

-- | Single workflow bound in dataflow graph.
data Fun k v = Fun
  { workflow :: Workflow
    -- ^ Execute workflow. It takes resource handle as parameter and
    --   tries to acquire resources and return actual function to run.
  , metadata :: Metadata
    -- ^ Metadata that should be supplied to the workflow
  , requestRes :: ResourceSet -> STM ()
    -- ^ Request resources for evaluation
  , releaseRes :: ResourceSet -> STM ()
    -- ^ Release resources after evaluation
  , param    :: [k]
    -- ^ Parameters to workflow.
  , output   :: v
    -- ^ Output of workflow
  }
  deriving stock (Functor, Foldable, Traversable)

-- | Complete dataflow graph
data FlowGraph a = FlowGraph
  { graph   :: Map FunID (Fun FunID a)
    -- ^ Dataflow graph with dependencies
  , targets :: Set FunID
    -- ^ Set of values for which we want to evaluate
  }
  deriving stock (Functor, Foldable, Traversable)


-- | Flow monad which we use to build workflow
newtype Flow eff a = Flow
  (StateT (Metadata, FlowGraph ()) (Program eff) a)
  deriving newtype (Functor, Applicative, Monad)

instance MonadFail (Flow eff) where
  fail = error

instance MonadState Metadata (Flow eff) where
  get   = Flow $ gets fst
  put m = Flow $ _1 .= m

-- | Add value which could be serialized to metadata to full medataset
appendMeta :: IsMeta a => a -> Flow eff ()
appendMeta a = metadata .= a

-- | Scope metadata modifications. All changes to metadata done in
--   provided callback will be discarded.
scopeMeta :: Flow eff a -> Flow eff a
scopeMeta action = do
  m <- get
  action <* put m

-- | Restrict metadata to set necessary for encoding value of given type
restrictMeta
  :: forall meta eff a. IsMeta meta
  => Flow eff a
  -> Flow eff a
restrictMeta action = scopeMeta $ do
  modify (restrictMetadata @meta)
  action



----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

-- | Compute all hashes and resolve all nodes to corresponding paths
hashFlowGraph :: FlowGraph () -> FlowGraph (Maybe StorePath)
hashFlowGraph gr = res where
  res      = gr & flowGraphL . mapped %~ hashFun oracle
  oracle k = case res ^. flowGraphL . at k of
    Nothing -> error "INTERNAL ERROR: flow graph is not consistent"
    Just f  -> case f.output of
      Nothing -> error "INTERNAL ERROR: dependence on PHONY node"
      Just p  -> p

hashFun :: (k -> StorePath) -> Fun k a -> Fun k (Maybe StorePath)
hashFun oracle fun = fun
  { output = case fun.workflow of
      Phony{}                -> Nothing
      Workflow (Action nm _) ->
        let hash   = SHA1.hashlazy $ BL.fromChunks [h | Hash h <- hashes]
            hashes = hashMeta fun.metadata
                   : Hash (T.encodeUtf8 $ T.pack nm)
                   : [ case oracle k of StorePath _ h -> h
                     | k <- fun.param
                     ]
        in Just $ StorePath nm (Hash hash)
  }
  where

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

-- | Remove duplicate nodes where different FunID correspond to same
--   workflow
deduplicateGraph
  :: FlowGraph (Maybe StorePath)
  -> FlowGraph (Maybe StorePath)
deduplicateGraph gr
  | null dupes = gr
  | otherwise  = gr & flowGraphL %~ clean
                    & flowTgtL   %~ (Set.\\ dupes)
  where
    clean = fmap replaceKey
          . flip Map.withoutKeys dupes
    replaceKey f = f { param = replace <$> f.param }
    -- FIDs of duplicate
    dupes       = Set.fromList [ fid | _ :| fids <- toList fid_mapping
                                     , fid       <- fids
                                     ]
    -- Mapping for replacing FID
    replace fid = replacement ^. at fid . non fid
    replacement = Map.fromList [ (dup,fid) | fid :| fids <- toList fid_mapping
                                           , dup <- fids
                                           ]
    -- Workflows that has more than one FID
    fid_mapping
      = Map.mapMaybe NE.nonEmpty
      $ Map.fromListWith (<>) [(hash, [fid])
                              | (fid,f)                 <- Map.toList gr.graph
                              , Just (StorePath _ hash) <- [f.output]
                              ]

-- | Remove all workflows that already completed execution.
shakeFlowGraph
  :: (Monad m)
  => (StorePath -> m Bool)       -- ^ Predicate to check whether path exists
  -> FlowGraph (Maybe StorePath) -- ^ Dataflow graph
  -> m FIDSet
shakeFlowGraph tgtExists (FlowGraph workflows targets)
  = foldM addFID (FIDSet mempty mempty) targets
  where
    addFID fids fid
      -- We already wisited these workflows
      | fid `Set.member` fids.exists = pure fids
      | fid `Set.member` fids.wanted = pure fids
      -- Check if result has been computed already
      | otherwise = do
          exists <- case f.workflow of
            -- Phony targets are always executed
            Phony{}    -> pure False
            Workflow{} -> case f.output of
              Just path -> tgtExists path
              Nothing   -> error "INTERNAL ERROR: dependence on phony target"
          case exists of
            True  -> pure $ fids & fidExistsL %~ Set.insert fid
            False -> foldM addFID fids' (f.param)
      where
        f     = workflows ! fid
        fids' = fids & fidWantedL %~ Set.insert fid

data FIDSet = FIDSet
  { exists :: !(Set FunID)    -- ^ Workflow already computed
  , wanted :: !(Set FunID) -- ^ We need to compute these workflows
  }
  deriving (Show)

----------------------------------------------------------------
-- Lens
----------------------------------------------------------------

fidExistsL, fidWantedL :: Lens' FIDSet (Set FunID)
fidExistsL = lens (.exists) (\f x -> f {exists = x})
fidWantedL = lens (.wanted) (\f x -> f {wanted = x})

flowTgtL :: Lens' (FlowGraph a) (Set FunID)
flowTgtL = lens (.targets) (\x s -> x {targets = s})

flowGraphL :: Lens (FlowGraph a) (FlowGraph b) (Map FunID (Fun FunID a)) (Map FunID (Fun FunID b))
flowGraphL = lens (.graph) (\x s -> x {graph = s})
