{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
-- |
-- Implementation of dataflow graph.
module OKA.Flow.Graph
  ( -- * Dataflow graph
    Fun(..)
  , FlowGraph(..)
  , ExtMeta(..)
    -- * Flow monad and primitives
  , Flow(..)
  , FlowSt(..)
  , appendMeta
  , scopeMeta
  , restrictMeta
  , withoutExtMeta
    -- * Graph operations
  , FIDSet(..)
  , hashFlowGraph
  , deduplicateGraph
  , shakeFlowGraph
    -- * Lens
  , flowTgtL
  , flowGraphL
  , fidExistsL
  , fidWantedL
  , stMetaL
  , stGraphL
    -- * Defining workflows
  , want
  , basicLiftWorkflow
  , liftWorkflow
  , basicLiftPhony
  ) where

import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Monad.Operational    hiding (view)
import Control.Monad.State.Strict
import Control.Monad.Reader
import Crypto.Hash.SHA1             qualified as SHA1
import Data.Aeson                   qualified as JSON
import Data.Aeson.Encoding          qualified as JSONB
import Data.Aeson.Encoding.Internal qualified as JSONB
import Data.Aeson.KeyMap            qualified as KM
import Data.Aeson.Key               (toText)
import Data.ByteString.Lazy         qualified as BL
import Data.Foldable
import Data.Coerce
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

import OKA.Flow.Types
import OKA.Flow.Resources

----------------------------------------------------------------
-- Dataflow graph
----------------------------------------------------------------

-- | Single workflow bound in dataflow graph.
data Fun k v = Fun
  { workflow  :: Workflow
    -- ^ Execute workflow. It takes resource handle as parameter and
    --   tries to acquire resources and return actual function to run.
  , metadata  :: Metadata
    -- ^ Metadata that should be supplied to the workflow
  , resources :: Lock
    -- ^ Resources required by workflow
  , param     :: [k]
    -- ^ Parameters to workflow.
  , output    :: v
    -- ^ Output of workflow
  , extMeta   :: [ExtMeta k]
    -- ^ All parts of metadata which should be read from output of
    --   some other workflow
  }
  deriving stock (Functor, Foldable, Traversable)

-- | Part of metadata which is read from external storage
data ExtMeta k = ExtMeta
  { key   :: !k
    -- ^ Key of a
  , tyRep :: !TypeRep
    -- ^ Type of
  , load  :: JSON.Value -> Metadata
    -- ^ Function which loads metadata from bare JSON
  }

-- | Complete dataflow graph
data FlowGraph a = FlowGraph
  { graph   :: Map FunID (Fun FunID a)
    -- ^ Dataflow graph with dependencies
  , targets :: Set FunID
    -- ^ Set of values for which we want to evaluate
  }
  deriving stock (Functor, Foldable, Traversable)


----------------------------------------------------------------
-- Flow monad and primitives
----------------------------------------------------------------

-- | Flow monad which we use to build workflow
newtype Flow eff a = Flow
  (ReaderT [ExtMeta FunID] (StateT FlowSt (Program eff)) a)
  deriving newtype (Functor, Applicative, Monad)

-- | State of 'Flow' monad
data FlowSt = FlowSt
  { meta  :: !Metadata
  , graph :: !(FlowGraph ())
  }

instance MonadFail (Flow eff) where
  fail = error

instance MonadState Metadata (Flow eff) where
  get   = Flow $ gets (.meta)
  put m = Flow $ stMetaL .= m

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

-- | Restrict metadata to set necessary for encoding value of given type
restrictMeta
  :: forall meta eff a. IsMeta meta
  => Flow eff a
  -> Flow eff a
restrictMeta action = scopeMeta $ do
  modify (restrictMetaByType @meta)
  action

-- | Execute workflow without loading external metadata
withoutExtMeta :: Flow eff a -> Flow eff a
withoutExtMeta (Flow action) = Flow $ local (\_ -> []) action

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
        let hash = hashHashes
                 $ hashMeta fun.metadata
                 : hashFlowName nm
                 : [ case oracle k of StorePath _ h -> h
                   | k <- fun.param
                   ] ++ (hashExtMeta oracle <$> fun.extMeta)
        in Just $ StorePath nm hash
      WorkflowExe exe ->
        let hash = hashHashes
                 $ hashMeta fun.metadata
                 : hashFlowName exe.name
                 : [ case oracle k of StorePath _ h -> h
                   | k <- fun.param
                   ] ++ (hashExtMeta oracle <$> fun.extMeta)
        in Just $ StorePath exe.name hash
  }
  where

hashHashes :: [Hash] -> Hash
hashHashes = Hash . SHA1.hashlazy . coerce BL.fromChunks

hashFlowName :: String -> Hash
hashFlowName = Hash . T.encodeUtf8 . T.pack

hashExtMeta :: (k -> StorePath) -> ExtMeta k -> Hash
hashExtMeta oracle m = Hash $ SHA1.hashlazy $ BL.fromChunks
  [ "?EXT_META?"
  , case oracle m.key        of StorePath _ (Hash h) -> h
  , case hashTypeRep m.tyRep of Hash h               -> h
  ]



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
    replaceKey f = f { param   = replace <$> f.param
                     , extMeta = [ ExtMeta{key=replace key, ..} | ExtMeta{..} <- f.extMeta]
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
              fids'' <- foldM addFID fids' f.param
              foldM addFID fids'' ((.key) <$> f.extMeta)
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
flowGraphL = lens (.graph) (\FlowGraph{..} s -> FlowGraph{graph = s, ..})

stMetaL :: Lens' FlowSt Metadata
stMetaL = lens (.meta) (\FlowSt{..} x -> FlowSt{meta=x, ..})

stGraphL :: Lens' FlowSt (FlowGraph ())
stGraphL = lens (.graph) (\FlowSt{..} x -> FlowSt{graph=x, ..})



----------------------------------------------------------------
-- Defining effects
----------------------------------------------------------------

-- | We want given workflow evaluated
want :: ResultSet a => a -> Flow eff ()
want a = Flow $ stGraphL . flowTgtL %= (<> Set.fromList (toResultSet a))

-- | Basic primitive for creating workflows. It doesn't offer any type
--   safety so it's better to use other tools
basicLiftWorkflow
  :: (ResultSet params, Resource res)
  => res      -- ^ Resource required by workflow
  -> Workflow -- ^ Workflow to be executed
  -> params   -- ^ Parameters of workflow
  -> Flow eff (Result a)
basicLiftWorkflow resource exe p = Flow $ do
  ext <- ask
  st  <- get
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
    { workflow  = exe
    , metadata  = st.meta
    , output    = ()
    , resources = resourceLock resource
    , param     = res
    , extMeta   = ext
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
