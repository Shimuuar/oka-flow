{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE MonoLocalBinds      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Core.Graph
  ( -- * Data types
    AResult
  , APhony
  , Result
  , Phony
  , CallingConv
    -- ** Single dataflow description 
  , Executable(..)
  , Action(..)
  , Dataflow(..)
  , Workflow(..)
  , Fun(..)
  , MetadataFlow
    -- ** Dataflow graph
  , FlowGraph(..)
  , OutputPath(..)
    -- * Graph operations
  , FIDSet(..)
  , hashFlowGraph
  , deduplicateGraph
  , shakeFlowGraph
    -- * Lens
  , flowTgtL
  , flowGraphL
  , flowPhonyL
  )  where

import Control.Applicative
import Control.Lens
import Control.Monad
import Crypto.Hash.SHA1             qualified as SHA1
import Data.ByteString.Builder      qualified as BB
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.Aeson.KeyMap            qualified as KM
import Data.Aeson.Key               (toText)
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Coerce
import Data.Monoid                  (Endo(..))
import Data.List                    (sortOn,intercalate,intersperse)
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


-- | Action which runs executable. It should be used if one want to
--   call external executable. This way executor can correctly pass
--   metadata to it.
data Executable = Executable
  { call :: CallingConv -- ^ IO action which could be executed to prepare program.
  }

-- | Action to be execute by dataflow either normal or phony.
data Action
  = ActionIO  (ResourceSet -> ParamFlow FilePath -> IO ())
    -- ^ Some haskell function 
  | ActionExe Executable
    -- ^ Run executable

-- | Dataflow which produces output in the store.
data Dataflow = Dataflow
  { name :: String  -- ^ Name of dataflow. Should be unique
  , flow :: Action  -- ^ Action to perform
  }

-- | Workflow which could either be 
data Workflow r where
  WorkflowNormal :: Dataflow -> Workflow Result
  WorkflowPhony  :: Action   -> Workflow Phony

-- | Metadata which is used in @Flow@. It contains both immediate
--   values and promises which are loaded from outputs at execution
--   time.
type MetadataFlow = MetadataF AResult

-- | Single workflow bound in dataflow graph.
data Fun r v = Fun
  { workflow  :: Workflow r   -- ^ Workflow to execute
  , metadata  :: MetadataFlow -- ^ Metadata that should be supplied to the workflow
  , resources :: Claim        -- ^ Resources required by workflow
  , param     :: S AResult    -- ^ Parameters to workflow.
  , output    :: v            -- ^ Output of workflow
  }
  deriving stock (Functor, Foldable, Traversable)


-- | Complete dataflow graph
data FlowGraph f = FlowGraph
  { graph   :: Map AResult (Fun Result (f Result))
    -- ^ Dataflow graph with dependencies
  , phony   :: Map APhony  (Fun Phony  (f Phony))
    -- ^ Set of phony targets
  , targets :: Set AResult
    -- ^ Set of values which we want to evaluate
  }
  -- deriving stock (Functor, Foldable, Traversable)




----------------------------------------------------------------
-- Execution of the workflow
----------------------------------------------------------------

-- | Remove duplicate nodes where different FunID correspond to same
--   workflow
deduplicateGraph
  :: FlowGraph OutputPath
  -> FlowGraph OutputPath
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
                              | (fid,f)          <- Map.toList gr.graph
                              , let PathDataflow (StorePath _ hash) = f.output
                              ]

-- | Remove all workflows that already completed execution.
shakeFlowGraph
  :: forall m. (Monad m)
  => (StorePath -> m Bool)  -- ^ Predicate to check whether path exists
  -> FlowGraph OutputPath   -- ^ Dataflow graph
  -> m FIDSet
shakeFlowGraph tgtExists gr
  =   flip (foldM addPhony) gr.phony 
  <=< foldAddFID gr.targets
    $ FIDSet mempty mempty
  where
    -- We always want to evaluate phony workflows
    addPhony :: FIDSet -> Fun Phony b -> m FIDSet
    addPhony fids fun
      = foldM addFID fids fun.param
    --
    foldAddFID :: Foldable f => f AResult -> FIDSet -> m FIDSet
    foldAddFID = flip (foldM addFID)
    --
    addFID :: FIDSet -> AResult -> m FIDSet
    addFID fids fid
      -- We already wisited these workflows
      | fid `Set.member` fids.exists = pure fids
      | fid `Set.member` fids.wanted = pure fids
      -- Check if result has been computed already
      | otherwise = do
          exists <- case f.workflow of
            WorkflowNormal _ -> case f.output of
              PathDataflow p -> tgtExists p
          case exists of
            True  -> pure $ fids & fidExistsL %~ Set.insert fid
            False -> foldAddFID f.param
                 <=< foldAddFID f.metadata
                   $ fidWantedL %~ Set.insert fid
                   $ fids 
      where
        f = gr.graph ! fid


data FIDSet = FIDSet
  { exists :: !(Set AResult) -- ^ Workflow already computed
  , wanted :: !(Set AResult) -- ^ We need to compute these workflows
  }
  deriving (Show)



----------------------------------------------------------------
-- Hashing
----------------------------------------------------------------

data OutputPath f where
  PathDataflow :: StorePath -> OutputPath Result
  PathPhony    ::              OutputPath Phony

-- | Compute all hashes and resolve all nodes to corresponding paths
hashFlowGraph :: FlowGraph Proxy -> FlowGraph OutputPath
hashFlowGraph gr = res where
  res = gr { graph = hashFun oracle <$> gr.graph
           , phony = (fmap . fmap) (\Proxy -> PathPhony) gr.phony
           }
  oracle k = case res ^. flowGraphL . at k of
    Nothing -> error "INTERNAL ERROR: flow graph is not consistent"
    Just f  -> case f.output of
      PathDataflow p -> p

hashFun
  :: (AResult -> StorePath)
  -> Fun Result a
  -> Fun Result (OutputPath Result)
hashFun oracle fun = fun
  { output = case fun.workflow of
      WorkflowNormal flow -> PathDataflow $ mkStorePath flow.name
  }
  where
    mkStorePath name
      = StorePath name
      $ hashHashes
      $ hashMeta ( runIdentity
                 $ traverseMetadataMay (\_ -> pure Nothing) fun.metadata)
      : hashFlowName name
      : hashS oracle fun.param
      : hashExtMeta oracle fun.metadata

hashHashes :: [Hash] -> Hash
hashHashes = Hash . SHA1.hashlazy . coerce BL.fromChunks

hashFlowName :: String -> Hash
hashFlowName = Hash . T.encodeUtf8 . T.pack

hashS :: (k -> StorePath) -> S k -> Hash
hashS oracle s0
  = Hash $ SHA1.hashlazy $ BB.toLazyByteString $ BB.string7 "?ARGS?" <> go s0
  where
    go = \case
      Param k -> BB.char7 '/' <> BB.byteString (case oracle k of StorePath _ (Hash h) -> h)
      Atom  a -> BB.char7 ':' <> BB.string8 a
      Nil     -> BB.char7 '-'
      S ss    -> BB.char7 '('
              <> mconcat (intersperse (BB.char7 ',') (go <$> ss))
              <> BB.char7 ')'

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

fidExistsL, fidWantedL :: Lens' FIDSet (Set AResult)
fidExistsL = lens (.exists) (\f x -> f {exists = x})
fidWantedL = lens (.wanted) (\f x -> f {wanted = x})

flowTgtL :: Lens' (FlowGraph f) (Set AResult)
flowTgtL = lens (.targets) (\x s -> x {targets = s})

-- flowGraphL :: Lens (FlowGraph ph gr) (FlowGraph ph x) (Map AResult (Fun Result gr)) (Map AResult (Fun Result x))
flowGraphL :: Lens' (FlowGraph f) (Map AResult (Fun Result (f Result)))
flowGraphL = lens (.graph) (\FlowGraph{..} s -> FlowGraph{graph = s, ..})

-- flowPhonyL :: Lens (FlowGraph ph gr) (FlowGraph x gr) (Map APhony (Fun Phony ph)) (Map APhony (Fun Phony x))
flowPhonyL :: Lens' (FlowGraph f) (Map APhony (Fun Phony (f Phony)))
flowPhonyL = lens (.phony) (\FlowGraph{..} s -> FlowGraph{phony = s, ..})
