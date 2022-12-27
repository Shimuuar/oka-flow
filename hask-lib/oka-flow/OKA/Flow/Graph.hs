{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
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

import Control.Applicative
import Control.Concurrent.Async
import Control.Exception
import Control.Lens
import Control.Monad
import Control.Monad.STM
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.ByteString              (ByteString)
import Data.ByteString.Lazy         qualified as BL
import Data.ByteString.Base16       qualified as Base16
import Data.ByteString.Builder      qualified as BB
import Data.ByteString.Char8        qualified as BC8
import Data.Foldable
import Data.HashMap.Strict          qualified as HM
import Data.List                    (sortOn)
import Data.Map.Strict              (Map, (!))
import Data.Map.Strict              qualified as Map
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import Data.Vector                  qualified as V
import System.FilePath              ((</>))

import OKA.Metadata


----------------------------------------------------------------
-- Data types
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

-- | Single action to be performed.
data Action res
  = ActNormal (res -> STM (Metadata -> [FilePath] -> FilePath -> IO ()))
    -- ^ Action which produces output
  | ActPhony  (res -> STM (Metadata -> [FilePath] -> IO ()))
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
data FlowGraph res = FlowGraph
  { flowGraph :: Map FunID (Fun res FunID)
    -- ^ Dataflow graph with dependencies
  , flowTgt   :: Set FunID
    -- ^ Set of values for which we want to evaluate
  }

flowTgtL :: Lens' (FlowGraph res) (Set FunID)
flowTgtL = lens flowTgt (\x s -> x {flowTgt = s})

flowGraphL :: Lens' (FlowGraph res) (Map FunID (Fun res FunID))
flowGraphL = lens flowGraph (\x s -> x {flowGraph = s})

----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

newtype Hash = Hash ByteString

data StorePath = StorePath String Hash

storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm ++ "-" ++ BC8.unpack (Base16.encode hash)

-- | Compute hash of metadata
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
             <.> V.foldr withComma (JSONB.Encoding mempty) (V.unsafeTail v)
  where
    withComma a z = JSONB.comma <.> encodeToBuilder a <.> z
    JSONB.Encoding e1 <.> JSONB.Encoding e2 = JSONB.Encoding (e1 <> e2)


hashFun :: Fun res (FunID, StorePath) -> StorePath
hashFun Fun{..} = StorePath (workflowName funWorkflow) (Hash hash)
  where
    hash   = SHA1.hashlazy $ BL.fromChunks [h | Hash h <- hashes]
    hashes = hashMeta funMetadata
           : [ h | (_, StorePath _ h) <- funParam]


-- | Resolve all dependencies in graph. Graph must be acyclic or
--   function will hang.
hashFlowGraph
  :: Map FunID (Fun res FunID)
  -> Map FunID (Fun res (FunID, StorePath))
hashFlowGraph m = r where
  r = convertFun <$> m
  convertFun f = f' where
    f' = f { funOutput = ( funOutput f, hashFun f')
           , funParam  = [ (i, snd $ funOutput $ r ! i) | i <- funParam f ]
           }

-- | Remove all workflows that already completed execution.
shakeFlowGraph
  :: (Monad m)
  => (StorePath -> m Bool)                  -- ^ Predicate to check whether
  -> Set FunID                              -- ^ Set of targets
  -> Map FunID (Fun res (FunID, StorePath)) -- ^ Workflow graph
  -> m FIDSet
shakeFlowGraph tgtExists targets workflows
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
        deps  = funParam f

data FIDSet = FIDSet
  { fidExists :: !(Set FunID)
  , fidWanted :: !(Set FunID)
  }

fidExistsL, fidWantedL :: Lens' FIDSet (Set FunID)
fidExistsL = lens fidExists (\f x -> f {fidExists = x})
fidWantedL = lens fidWanted (\f x -> f {fidWanted = x})
