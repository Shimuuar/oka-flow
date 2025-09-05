{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Core.Graph
  ( -- * Dataflow graph
    Action(..)
  , PhonyAction(..)
  , Executable(..)
  , Workflow(..)
  , isPhony
  , FunID(..)
  , Result(..)
  , Fun(..)
  , FlowGraph(..)
  , MetadataFlow
    -- * Graph operations
  , FIDSet(..)
  , hashFlowGraph
  , deduplicateGraph
  , shakeFlowGraph
    -- * Lens
  , flowTgtL
  , flowGraphL
  ) where

import Control.Applicative
import Control.Lens
import Control.Monad
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.Aeson.KeyMap            qualified as KM
import Data.Aeson.Key               (toText)
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Coerce
import Data.Monoid                  (Endo(..))
import Data.List                    (sortOn,intercalate)
import Data.List.NonEmpty           qualified as NE
import Data.List.NonEmpty           (NonEmpty(..))
import Data.Map.Strict              (Map, (!))
import Data.Map.Strict              qualified as Map
import Data.Set                     (Set)
import Data.Set                     qualified as Set
import Data.Text                    qualified as T
import Data.Text.Encoding           qualified as T
import Data.Typeable
import Data.Vector                  qualified as V

import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Core.Resources
import OKA.Flow.Core.Types
import OKA.Flow.Core.S

----------------------------------------------------------------
-- Dataflow graph definition
----------------------------------------------------------------

-- | Single action to be performed. It contains both workflow name and
--   action to pefgorm workflow
data Action = Action
  { name :: String
    -- ^ Name of a workflow
  , run  :: ResourceSet -> ParamFlow FilePath -> IO ()
    -- ^ Execute action on store
  }


-- | Phony action which is executed always and doesn't produce any output
newtype PhonyAction = PhonyAction
  { run :: ResourceSet -> ParamPhony FilePath -> IO ()
  }


-- | Action which execute executable. It should be always used if one
--   want to call external executable. This way executor can correctly
--   pass metadata to it.
data Executable = Executable
  { name       :: String
    -- ^ Name of a workflow
  , executable :: FilePath
    -- ^ Executable to start
  , callCon    :: ParamFlow FilePath -> ProcessData
    -- ^ IO action which could be executed to prepare program.
  }


-- | Descritpion of workflow function. It knows how to build
data Workflow
  = Workflow Action
    -- ^ Standard workflow which executes haskell action
  | WorkflowExe Executable
    -- ^ Target which run executable
  | Phony    PhonyAction
    -- ^ Phony target which always executes action


isPhony :: Workflow -> Bool
isPhony = \case
  Workflow{}    -> False
  WorkflowExe{} -> False
  Phony{}       -> True


-- | Metadata which is used in @Flow@. It contains both immediate
--   values and promises which are loaded from outputs at execution
--   time.
type MetadataFlow = MetadataF FunID


-- | Single workflow bound in dataflow graph.
data Fun k v = Fun
  { workflow  :: Workflow
    -- ^ Execute workflow. It takes resource handle as parameter and
    --   tries to acquire resources and return actual function to run.
  , metadata  :: MetadataF k
    -- ^ Metadata that should be supplied to the workflow
  , resources :: Claim
    -- ^ Resources required by workflow
  , param     :: S k
    -- ^ Parameters to workflow.
  , output    :: v
    -- ^ Output of workflow
  }
  deriving stock (Functor, Foldable, Traversable)


-- | Complete dataflow graph
data FlowGraph a = FlowGraph
  { graph   :: Map FunID (Fun FunID a)
    -- ^ Dataflow graph with dependencies
  , targets :: Set FunID
    -- ^ Set of values which we want to evaluate
  }
  deriving stock (Functor, Foldable, Traversable)





----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

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
    replaceKey f = f { param    = replace <$> f.param
                     , metadata = replace <$> f.metadata
                     }
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
            WorkflowExe{} -> case f.output of
              Just path -> tgtExists path
              Nothing   -> error "INTERNAL ERROR: dependence on phony target"
          case exists of
            True  -> pure $ fids & fidExistsL %~ Set.insert fid
            False -> do
              fids'' <- foldM addFID fids'  f.param
              foldM addFID fids'' f.metadata
      where
        f     = workflows ! fid
        fids' = fids & fidWantedL %~ Set.insert fid

data FIDSet = FIDSet
  { exists :: !(Set FunID) -- ^ Workflow already computed
  , wanted :: !(Set FunID) -- ^ We need to compute these workflows
  }
  deriving (Show)



----------------------------------------------------------------
-- Hashing
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
      Workflow (Action nm _) -> Just $ mkStorePath nm
      WorkflowExe exe        -> Just $ mkStorePath exe.name
  }
  where
    -- Partition 
    mkStorePath name
      = StorePath name
      $ hashHashes
      $ hashMeta ( runIdentity
                 $ traverseMetadataMay (\_ -> pure Nothing) fun.metadata)
      : hashFlowName name
      -- FIXME: hash S expression
      : []
      -- : [ case oracle k of StorePath _ h -> h
      --   | k <- fun.param        
      --   ] ++ hashExtMeta oracle fun.metadata

hashHashes :: [Hash] -> Hash
hashHashes = Hash . SHA1.hashlazy . coerce BL.fromChunks

hashFlowName :: String -> Hash
hashFlowName = Hash . T.encodeUtf8 . T.pack

hashExtMeta :: forall k. (k -> StorePath) -> MetadataF k -> [Hash]
hashExtMeta oracle meta = case extra [] of
  [] -> []
  hs -> Hash "?EXT_META?" : hs
  where
    -- Here we trying to be clever and collect keys using traversal and Const
    --
    -- NOTE: TypeReps inside metadata are sorted so traversal order is
    --       well defined and we don't need to sort anything
    extra
      = appEndo $ getConst
      $ traverseMetadata collectK meta
    collectK :: forall x. IsMetaPrim x => k -> Const (Endo [Hash]) x
    collectK k
      = Const $ Endo
      $ (hashTypeRep ty                        :)
      . ((case oracle k of StorePath _ h -> h) :)
      where
        ty = typeRep (Proxy @x)



-- Compute hash of metadata
hashMeta :: Metadata -> Hash
hashMeta
  = Hash
  . SHA1.hashlazy
  . JSONB.encodingToLazyByteString
  . encodeToBuilder
  . encodeMetadata

-- Hash TypeRep. Hopefully this scheme will be stable enough
hashTypeRep :: TypeRep -> Hash
hashTypeRep = Hash . SHA1.hash . T.encodeUtf8 . T.pack . showTy
  where
    showTy ty = case splitTyConApp ty of
      (con,param) -> "("++intercalate " " (showCon con : map showTy param)++ ")"
    showCon con = tyConModule con <> "." <> tyConName con


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



----------------------------------------------------------------
-- Lens
----------------------------------------------------------------

fidExistsL, fidWantedL :: Lens' FIDSet (Set FunID)
fidExistsL = lens (.exists) (\f x -> f {exists = x})
fidWantedL = lens (.wanted) (\f x -> f {wanted = x})

flowTgtL :: Lens' (FlowGraph a) (Set FunID)
flowTgtL = lens (.targets) (\x s -> x {targets = s})

flowGraphL :: Lens (FlowGraph a) (FlowGraph b) (Map FunID (Fun FunID a)) (Map FunID (Fun FunID b))
flowGraphL = lens (.graph) (\FlowGraph{..} s -> FlowGraph{graph = s, ..})
