{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost        #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TupleSections              #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Graph
  ( -- * Dataflow graph
    Fun(..)
  , FlowGraph(..)
  , FIDSet(..)
  , Flow(..)
  , appendMeta
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
import Control.Monad.Operational
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Reader
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
data Fun res a = Fun
  { funWorkflow :: Workflow res
    -- ^ Execute workflow. It takes resource handle as parameter and
    --   tries to acquire resources and return actual function to run.
  , funMetadata :: Metadata
    -- ^ Metadata that should be supplied to the workflow
  , funParam    :: [a]
    -- ^ Parameters to workflow.
  , funOutput   :: a
    -- ^ Output of workflow
  }
  deriving stock (Functor)

-- | Complete dataflow graph
data FlowGraph res a = FlowGraph
  { flowGraph :: Map FunID (Fun res (FunID, a))
    -- ^ Dataflow graph with dependencies
  , flowTgt   :: Set FunID
    -- ^ Set of values for which we want to evaluate
  }
  deriving (Functor) -- Safe but evaluates function for each graph node and vertex

instance Foldable (FlowGraph res) where
  foldMap f = foldMap (f . snd . funOutput) . flowGraph
instance Traversable (FlowGraph res) where
  traverse fun (FlowGraph gr tgt)
    = (\m -> FlowGraph (fixup m) tgt) <$> gr'
    where
      gr' = traverse (\f -> (,f) <$> fun (snd $ funOutput f)) gr
      -- Fixup graph
      fixup gr0 = r where
        r = gr0 <&> \(b,f) -> f
          { funOutput = funOutput f & _2 .~ b
          , funParam  = [ (i, snd $ funOutput $ r ! i) | (i,_) <- funParam f ]
          }

fmapFlowGraph
  :: (Fun res b -> b)
  -> FlowGraph res a
  -> FlowGraph res b
fmapFlowGraph fun gr = gr { flowGraph = r } where
  r = convert <$> flowGraph gr
  convert f = f' where
    f' = f { funOutput = funOutput f & _2 .~ fun (snd <$> f')
           , funParam  = [ (i, snd $ funOutput $ r ! i) | (i,_) <- funParam f ]
           }


-- | Flow monad which we use to build workflow
newtype Flow res eff a = Flow
  (ReaderT Metadata (StateT (FlowGraph res ()) (Program eff)) a)
  deriving newtype (Functor, Applicative, Monad)

appendMeta :: Metadata -> Flow res eff a -> Flow res eff a
appendMeta meta (Flow act) = Flow $ ReaderT $ \m ->
  runReaderT act (m <> meta)


----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

-- | Compute all hashes and resolve all nodes to corresponding paths
hashFlowGraph :: FlowGraph res () -> FlowGraph res StorePath
hashFlowGraph = fmapFlowGraph hashFun

-- Compute hash of metadata
hashMeta :: Metadata -> Hash
hashMeta (Metadata json)
  = Hash
  $ SHA1.hashlazy
  $ JSONB.encodingToLazyByteString
  $ encodeToBuilder json

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


hashFun :: Fun res StorePath -> StorePath
hashFun Fun{..} = StorePath (workflowName funWorkflow) (Hash hash)
  where
    hash   = SHA1.hashlazy $ BL.fromChunks [h | Hash h <- hashes]
    hashes = hashMeta funMetadata
           : [ h | StorePath _ h <- funParam]


-- | Remove all workflows that already completed execution.
shakeFlowGraph
  :: (Monad m)
  => (StorePath -> m Bool)   -- ^ Predicate to check whether path exists
  -> FlowGraph res StorePath -- ^ Dataflow graph
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
          exists <- case isPhony (funWorkflow f) of
            -- Phony targets are always executed
            True  -> pure False
            False -> tgtExists (snd (funOutput f))
          case exists of
            True  -> pure $ fids & fidExistsL %~ Set.insert fid
            False -> foldM addFID fids' (fst <$> funParam f)
      where
        f     = workflows ! fid
        fids' = fids & fidWantedL %~ Set.insert fid

data FIDSet = FIDSet
  { fidExists :: !(Set FunID)
    -- ^ Workflow already computed
  , fidWanted :: !(Set FunID)
    -- ^ We need to compute these workflows
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

flowGraphL :: Lens' (FlowGraph res a) (Map FunID (Fun res (FunID,a)))
flowGraphL = lens flowGraph (\x s -> x {flowGraph = s})
