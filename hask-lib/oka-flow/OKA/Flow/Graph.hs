{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TupleSections       #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Graph
  ( FunID(..)
  , Action(..)
  , Workflow(..)
  , isPhony
  , Fun(..)
  , FlowGraph(..)
  , flowTgtL
  , flowGraphL
    -- * Storage
  , Hash(..)
  , StorePath(..)
  , storePath
  , FIDSet(..)
  , hashFlowGraph
  , shakeFlowGraph
  ) where

-- import Control.Applicative
-- import Control.Exception
import Control.Lens
import Control.Monad
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.ByteString              (ByteString)
import Data.ByteString.Lazy         qualified as BL
import Data.ByteString.Base16       qualified as Base16
import Data.ByteString.Char8        qualified as BC8
-- import Data.Foldable
import Data.HashMap.Strict          qualified as HM
import Data.List                    (sortOn)
import Data.Map.Strict              (Map, (!))
-- import Data.Map.Strict              qualified as Map
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import Data.Vector                  qualified as V

import OKA.Metadata

import OKA.Flow.Types


----------------------------------------------------------------
-- Workflow description
----------------------------------------------------------------

-- | Single action to be performed.
data Action res
  = ActNormal (BracketSTM res (Metadata -> [FilePath] -> FilePath -> IO ()))
    -- ^ Action which produces output
  | ActPhony  (BracketSTM res (Metadata -> [FilePath] -> IO ()))
    -- ^ Action which doesn't produce any outputs


-- | Descritpion of workflow function. It knows how to build
data Workflow res = Workflow
  { workflowRun    :: Action res
    -- ^ Start workflow. This function takes resources as input and
    --   return STM action which either returns actual IO function to
    --   run or retries if it's unable to secure necessary resources.
  , workflowName   :: String
    -- ^ Name of workflow. Used for caching
  }

isPhony :: Workflow res -> Bool
isPhony f = case workflowRun f of
  ActNormal{} -> False
  ActPhony{}  -> True


----------------------------------------------------------------
-- Dataflow graph
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

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
                          
      

-- | Path in nix-like storage. 
data StorePath = StorePath String Hash

-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm ++ "-" ++ BC8.unpack (Base16.encode hash)


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


----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

-- | SHA1 hash
newtype Hash = Hash ByteString

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
  (\step z m0 -> foldr (\(k,v) a -> step k v a) z $ sortOn fst $ HM.toList m0)
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
      -- Phony targets are always executed
      | isPhony (funWorkflow f)    = pure fids'
      -- Check if result has been computed already
      | otherwise = tgtExists (snd (funOutput f)) >>= \case
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
