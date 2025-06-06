{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DefaultSignatures    #-}
{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE PatternGuards        #-}
{-# LANGUAGE PatternSynonyms      #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE ViewPatterns         #-}
-- |
-- Definition of dynamically typed metadata dictionary, its JSON
-- serialization.
module OKA.Metadata.Meta
  ( -- * Metadata
    -- $metadata
    Metadata
  , MetadataF
  , metadata
  , metadataMay
  , traverseMetadata
  , traverseMetadataMay
  , restrictMetaByType
  , restrictMetaByKeys
  , deleteFromMetaByType
  , deleteFromMetaByKeys
  , metadataKeySet
  , storeExternal
    -- * 'IsMeta' type class
  , IsMetaPrim(..)
  , IsMeta(..)
  , fromMetadata
    -- ** Parser of metadata
  , ParserM
  , runMetaParser
  , runMetaParserWith
  , primMetaParser
    -- ** Trees of metadata
  , MetaTree
  , primToMetadata
  , singletonMetaTree
  , optionalMetaTree
  , decodeMetadataPrimEither
  , MetaPath(..)
    -- ** Encoding & decoding
  , encodeMetadataEither
  , encodeMetadata
  , encodeToMetadataEither
  , encodeToMetadata
  , decodeMetadataEither
  , decodeMetadata
    -- * Exceptions
  , MetadataError(..)
    -- ** Deriving via
  , AsAeson(..)
  , AsReadShow(..)
  , AsRecord(..)
  , AsMeta(..)
  ) where

import Control.Applicative
import Control.Lens
import Control.Monad
import Control.Exception

import Data.Aeson                 ((.:?))
import Data.Aeson                 qualified as JSON
import Data.Aeson.KeyMap          qualified as KM
import Data.Aeson.Key             (fromText,toText)
import Data.Aeson.Types           qualified as JSON
import Data.Coerce
import Data.Foldable              (toList,foldl')
import Data.Functor.Contravariant
import Data.Maybe
import Data.Map.Strict            qualified as Map
import Data.Map.Strict            (Map)
import Data.Set                   (Set)
import Data.Set                   qualified as Set
import Data.Monoid                (Dual(..),Endo(..))
import Data.Typeable
import Data.Text                  (Text)
import Data.Text                  qualified as T
import Data.Yaml                  qualified as YAML
import Data.Void                  (Void)
import GHC.TypeLits
import GHC.Generics
import GHC.Exts                   (Proxy#,proxy#)

import OKA.Metadata.Encoding
import OKA.Metadata.Util

----------------------------------------------------------------
-- Metadata representation
----------------------------------------------------------------

-- $metadata
--
-- As defined in this package metadata is open union of data types
-- which are instance of 'IsMetaPrim' and which could be roundtripped
-- using JSON. it's used to pass multiple parameters to workflows
-- in addition to data being processed: data selection and
-- transformation parameters etc. 
--
-- This approach trades type safety for simplicity. Tracking many
-- parameters in types get very cumbersome while error when metadata
-- is missing tend to surface early and easy to fix.
--
-- Note on serialization. Metadata is serialized as tree of JSON
-- objects. Overlapping of keys is not allowed. Static checking of
-- such property is not practical so it done in runtime.


-- | 'Metadata' which allows to replace actual metadata values with
--   values of type @k@ which describe how to obtain metadata.
newtype MetadataF k = Metadata (Map TypeRep (MetaEntry k))
  deriving stock (Functor, Foldable, Traversable)

-- | Dynamically typed collection of values which could be looked up
--   by their type and serialized into JSON. API supports both
--   operations with single types and products of several types (as
--   tuples).
type Metadata = MetadataF Void

-- | Right biased
deriving via Dual (Map TypeRep (MetaEntry f)) instance Semigroup (MetadataF f)
-- | Right biased
deriving via Dual (Map TypeRep (MetaEntry f)) instance Monoid (MetadataF f)


-- | Resolve keys to actual metadata values.
traverseMetadata
  :: forall m k. Applicative m
  => (forall x. IsMetaPrim x => k -> m x)
  -> MetadataF k
  -> m Metadata
traverseMetadata fun (Metadata m) = Metadata <$> traverse go m
  where
    go :: MetaEntry k -> m (MetaEntry Void)
    go (MetaEntry a)  = pure (MetaEntry a)
    go Tombstone      = pure Tombstone
    go (External p k) = MetaEntry <$> (fun k `asProxy` p)
    --
    asProxy :: m a -> Proxy a -> m a
    asProxy x _ = x

-- | Resolve keys to actual metadata values.
traverseMetadataMay
  :: forall m k. Applicative m
  => (forall x. IsMetaPrim x => k -> m (Maybe x))
  -> MetadataF k
  -> m Metadata
traverseMetadataMay fun (Metadata m) = Metadata <$> traverse go m
  where
    go :: MetaEntry k -> m (MetaEntry Void)
    go (MetaEntry a)  = pure (MetaEntry a)
    go Tombstone      = pure Tombstone
    go (External p k) = (fun k `asProxy` p) <&> \case
      Nothing -> Tombstone
      Just a  -> MetaEntry a
    --
    asProxy :: m (Maybe a) -> Proxy a -> m (Maybe a)
    asProxy x _ = x


-- Helper existential wrapper for metadata
data MetaEntry k where
  MetaEntry :: (IsMetaPrim a) => a -> MetaEntry k
  Tombstone :: MetaEntry k
  -- We need tombstones in order to encode that we surely don't have
  -- value in the map. Which is needed to correctly implement
  -- semigroup instance
  External  :: (IsMetaPrim a) => Proxy a -> k -> MetaEntry k

deriving stock instance Functor     MetaEntry
deriving stock instance Foldable    MetaEntry
deriving stock instance Traversable MetaEntry



-- | Only keep keys which corresponds to a type
restrictMetaByType :: forall a f. IsMeta a => MetadataF f -> MetadataF f
restrictMetaByType = restrictMetaByKeys (metadataKeySet (proxy# @a))

-- | Only keep keys that are in the set of keys
restrictMetaByKeys :: Set TypeRep -> MetadataF f -> MetadataF f
restrictMetaByKeys keys (Metadata m) = Metadata $ Map.restrictKeys m keys

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetaByType :: forall a f. IsMeta a => MetadataF f -> MetadataF f
deleteFromMetaByType = deleteFromMetaByKeys (metadataKeySet (proxy# @a))

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetaByKeys :: Set TypeRep -> MetadataF f -> MetadataF f
deleteFromMetaByKeys keys (Metadata meta)
  = Metadata
  $ foldl' (\m k -> Map.insert k Tombstone m) meta keys


storeExternal :: forall a k. IsMeta a => k -> MetadataF k -> MetadataF k
storeExternal k
  = appEndo $ metadataKeys (proxy# @a) store
  where
    store :: forall x. IsMetaPrim x => Proxy# x -> Endo (MetadataF k)
    store _ = Endo $ \(Metadata m) -> Metadata $ Map.insert (typeRep ty) (External ty k) m
      where ty = Proxy @x


-- | Lens for accessing dictionary from dynamic 'Metadata'. Will fail if
--   dictionary is not part of bundle
metadata :: forall a k. (IsMeta a) => Lens' (MetadataF k) a
metadata = lens fromM toM where
  keyF    = error "metadata: delayed data encountered"
  fromM m = case runIdentity $ runMetaParserWith parseMetadata keyF m of
    Just a  -> a
    Nothing -> error $ "metadata: Metadata doesn't have data type: " ++ typeName @a
  toM m a = m <> toMetadata a

-- | Lens for accessing dictionary from dynamic 'Metadata'.
metadataMay :: forall a f. (IsMeta a) => Lens' (MetadataF f) (Maybe a)
metadataMay = lens fromM toM where
  keyF    = error "metadata: delayed data encountered"
  fromM m = runIdentity $ runMetaParserWith parseMetadata keyF m
  toM m a = m <> toMetadata a  



----------------------------------------------------------------
-- Parser for metadata
----------------------------------------------------------------

type role ParserM representational representational representational

-- | Parser for  extracting values from 'MetadataF'.
newtype ParserM k m a = ParserM 
  { _unP :: forall r.
            MetadataF k                                  -- Metadata to parse
         -> (forall x. IsMetaPrim x => k -> m (Maybe x)) -- Load from key
         -> (a -> r)                                     -- Success continuation
         -> r                                            -- Failure continuation
         -> (m r -> r)                                   -- Free monad bind
         -> r
  }

instance Functor (ParserM k m) where
  fmap f (ParserM cont) = ParserM $ \meta key succC failC bindC ->
    cont meta key (succC . f) failC bindC

instance Applicative (ParserM k m) where
  pure a = ParserM $ \_ _ succC _ _ -> succC a
  (<*>) = ap

instance Monad (ParserM k m) where
  ParserM cont >>= f = ParserM $ \meta key succC failC bindC ->
    cont meta key (\a -> case f a of
                 ParserM g -> g meta key succC failC bindC)
      failC bindC

instance Alternative (ParserM k m) where
  empty = ParserM $ \_ _ _ failC _ -> failC
  ParserM p1 <|> ParserM p2 = ParserM $ \meta key succC failC bindC ->
    p1 meta key succC (p2 meta key succC failC bindC) bindC



-- | Execute parser for a value
runMetaParser
  :: ParserM Void Identity a -- ^ Parser
  -> Metadata                -- ^ Metadata to parse
  -> Maybe a
runMetaParser p
  = runIdentity
  . runMetaParserWith p (error "runMetaParser: UNREACHABLE")

-- | Execute parser for a metadata with external keys
runMetaParserWith
  :: Monad m
  => ParserM k m a                                -- ^ Parser
  -> (forall x. IsMetaPrim x => k -> m (Maybe x)) -- ^ Function for loading metadata
  -> MetadataF k                                  -- ^ Metadata to parse
  -> m (Maybe a)
runMetaParserWith (ParserM parser) key meta =
  parser meta key (pure . Just) (pure Nothing) join
    

-- | Primitive parser for data type
primMetaParser :: forall a k m. (IsMetaPrim a, Functor m) => ParserM k m a
primMetaParser = ParserM $ \(Metadata meta) key succC failC bindC ->
  case Map.lookup (typeRep (Proxy @a)) meta of
    Nothing              -> failC
    Just (MetaEntry x)
      | Just a <- cast x -> succC a
      | otherwise        -> error "primMetaParser: INTERNAL ERROR"
    Just Tombstone       -> failC
    Just (External _ k)  -> bindC $ key @a k <&> \case
      Nothing -> failC
      Just a  -> succC a



----------------------------------------------------------------
-- Conversion type classes
----------------------------------------------------------------

-- | Type class for metadata entries. It encodes data type's location
--   in JSON dictionary for encoded metadata.
class (MetaEncoding a, Typeable a) => IsMetaPrim a where
  -- | Path to place of metadata in the JSON dictionary.
  metaLocation :: [JSON.Key]


-- | Convenience type class for interacting with 'Metadata' it allows
--   to decode from JSON and lookup from metadata arbitrary products
--   of instance of 'IsMetaPrim'.
--
--   Default implementation is in terms of 'IsMetaPrim'.
class Typeable a => IsMeta a where
  -- | Description on how to serialize metadata into JSON tree. This
  --   bidirectional parser allows to detect key collisions during
  --   both encoding and decoding.
  metaTree :: MetaTree a
  -- | Way to parse
  parseMetadata :: Monad m => ParserM k m a
  -- | Convert data type to dynamic dictionary
  toMetadata ::  a -> MetadataF k
  -- | Set of keys for the type corresponding to the metadata
  metadataKeys :: Monoid m
               => Proxy# a
               -> (forall x. IsMetaPrim x => Proxy# x -> m)
               -> m


metadataKeySet :: forall a. IsMeta a => Proxy# a -> Set TypeRep
metadataKeySet p = metadataKeys p tyRep where
  tyRep :: forall x. IsMetaPrim x => Proxy# x -> Set TypeRep
  tyRep _ = Set.singleton $ typeRep (Proxy @x)

-- | Look up type in dictionary.
fromMetadata :: IsMeta a => Metadata -> Maybe a
fromMetadata = runMetaParser parseMetadata


----------------------------------------------------------------
-- Exceptions
----------------------------------------------------------------

-- | Errors in metadata processing
data MetadataError
  = KeyClashes [(TypeRep, [Text])]
    -- ^ Metadata entries contain key clashes
  | YamlError  !YAML.ParseException
    -- ^ Decoding of YAML file to aeson's @Value@ failed.
  | JsonError  !String
    -- ^ Failure of decoding of JSON to haskell data type,
  | MetaError !TypeRep
    -- ^ Failure to decode value of given type from set of metadata.

instance Show MetadataError where
  show = \case
    KeyClashes err -> unlines
      $ ("[MetadataError] Key clashes when decoding metadata")
      : [ "  - " ++ T.unpack (T.intercalate "." e) ++ "["++show ty++"]"
        | (ty,e) <- err
        ]
    JsonError s -> "[MetadataError][MetaEncoding]\n" ++ s
    YamlError e -> "[MetadataError][YAML]\n" ++ show e
    MetaError e -> "[MetadataError][Meta]\n" ++ show e

instance Exception MetadataError



----------------------------------------------------------------
-- JSON encoding of metadata
----------------------------------------------------------------

-- | Encode dynamic dictionary. Throw exception in case there's key
--   overlap.
encodeMetadata :: Metadata -> JSON.Value
encodeMetadata = either throw id . encodeMetadataEither

-- | Encode dynamic dictionary. Returns @Left@ in case there's key
--   overlap.
encodeMetadataEither :: Metadata -> Either MetadataError JSON.Value
encodeMetadataEither (Metadata m) =
  case checkForClashes $ mconcat entries of
    Err err -> Left  $ KeyClashes [(ty, path) | ((ty,_),path) <- err]
    OK  xs  -> Right $ encodeTreeWith snd xs
  where
    entries = [ (\e -> (e.tyRep, e.encoder a)) <$> singletonMetaTree.get
              | MetaEntry a <- toList m
              ]

-- | Encode metadata as JSON value. Will throw error in case of key clash
encodeToMetadata :: forall a. IsMeta a => a -> JSON.Value
encodeToMetadata = either throw id . encodeToMetadataEither

-- | Encode metadata as JSON value. Returns @Left@ in case of key clash.
encodeToMetadataEither :: forall a. IsMeta a => a -> Either MetadataError JSON.Value
encodeToMetadataEither a = case checkForClashes metaTree.get of
  Err err  -> Left  $ KeyClashes [(e.tyRep, path) | (e,path) <- err]
  OK  tree -> Right $ encodeTreeWith (\e -> e.encoder a) tree


-- | Decode metadata from JSON value
decodeMetadata :: forall a. IsMeta a => JSON.Value -> a
decodeMetadata = either throw id . decodeMetadataEither

-- | Decode metadata from JSON value
decodeMetadataEither :: forall a. IsMeta a => JSON.Value -> Either MetadataError a
decodeMetadataEither json =
  case checkForClashes (metaTree @a).get of
    Err err  -> Left $ KeyClashes [(e.tyRep, path) | (e,path) <- err]
    OK  tree -> do
      m <- bimap JsonError id
         $ JSON.parseEither (annotate . decodeTree tree) json
      case fromMetadata m of
        Just a  -> Right a
        Nothing -> Left $ MetaError (typeRep (Proxy @a))
  where
    annotate = JSON.prependFailure ("\nFailed to decode metadata " ++ show (typeOf (undefined :: a)) ++ "\n")

-- | Decode metadata from JSON value
decodeMetadataPrimEither :: forall a. IsMetaPrim a => JSON.Value -> Either MetadataError a
decodeMetadataPrimEither json
  = bimap JsonError id
  $ JSON.parseEither (foldr descend parseMeta (metaLocation @a)) json

descend
  :: JSON.Key
  -> (JSON.Value -> JSON.Parser a)
  -> (JSON.Value -> JSON.Parser a)
descend k parser
  = JSON.prependFailure (" - " ++ T.unpack (toText k) ++ "\n")
  . \case
       JSON.Null     -> parser JSON.Null
       JSON.Object o -> (o .:? k) >>= \case
         Nothing -> parser JSON.Null
         Just js -> parser js
       o -> fail $ "Expected object but got " ++ constrName o



----------------------------------------------------------------
-- Bidirectional parser
----------------------------------------------------------------

-- | Data structure which describe where in JSON tree metadata of type
--   @a@ is placed and detects key clashes.
newtype MetaTree a = MetaTree { get :: MTree (Entry a) }
  deriving newtype (Semigroup,Monoid)

-- Leaf corresponding to a single record in metadata tree
--
-- NOTE: We can't use GADTs here since it will interfere with
--       coercions of MetaTree
data Entry a = Entry
  { tyRep   :: TypeRep
  , encoder :: a -> Maybe JSON.Value
  , parser  :: JSON.Value -> JSON.Parser Metadata
  }

instance Contravariant Entry where
  contramap f e = Entry { tyRep   = e.tyRep
                        , encoder = e.encoder . f
                        , parser  = e.parser
                        }

instance Contravariant MetaTree where
  contramap f (MetaTree m) = MetaTree (contramap f <$> m)


-- | JSON objects' tree which doesn't contain any key clashes
data Tree a
  = Leaf   a                    -- ^ Leaf containing JSON object
  | Branch (KM.KeyMap (Tree a)) -- ^ Branch in JSON objects
  deriving stock (Functor)

-- | JSON objects' tree which allows multiple values at nodes
data MTree a
  = MLeaf     a
  | MBranch   (KM.KeyMap (MTree a))
  | Collision [a] (KM.KeyMap (MTree a))
  deriving stock (Functor)

instance Semigroup (MTree a) where
  -- We have collision already
  MLeaf     a    <> Collision b m  = Collision (a : b)   m
  MBranch     m1 <> Collision b m2 = Collision b        (KM.unionWith (<>) m1 m2)
  Collision a m1 <> Collision b m2 = Collision (a<>b)   (KM.unionWith (<>) m1 m2)
  Collision a m  <> MLeaf     b    = Collision (a<>[b])  m
  Collision a m1 <> MBranch     m2 = Collision a        (KM.unionWith (<>) m1 m2)
  -- Collision doesn't occur at this level
  MLeaf     a    <> MBranch   Null = MLeaf a
  MBranch   Null <> MLeaf     a    = MLeaf a
  MBranch   m1   <> MBranch   m2   = MBranch (KM.unionWith (<>) m1 m2)
  -- We've found a collision!
  MLeaf     a    <> MLeaf     b    = Collision [a,b] mempty
  MBranch     m  <> MLeaf     a    = Collision [a] m
  MLeaf     a    <> MBranch     m  = Collision [a] m

instance Monoid (MTree a) where
  mempty = MBranch mempty




-- Check that JSON tree doesn't have any key conflicts
checkForClashes :: MTree a -> Err [(a,[Text])] (Tree a)
checkForClashes = go id where
  go pfx = \case
    MLeaf   a -> OK (Leaf a)
    MBranch m -> Branch <$> KM.traverseWithKey (\k -> go (pfx . (toText k:))) m
    Collision as m -> Err $ [(a,path) | a <- as]
                         <> KM.foldMapWithKey (\k -> collect (pfx . (toText k:))) m
    where path = pfx []
  -- No need to optimize. We'll fail with error anyway
  collect pfx = \case
    MLeaf     a    -> [errA a]
    MBranch   m    -> errM m
    Collision as m -> fmap errA as <> errM m
    where path = pfx []
          errA a = (a,path)
          errM m = KM.foldMapWithKey (\k -> collect (pfx . (toText k:))) m

encodeTreeWith :: (a -> Maybe JSON.Value) -> Tree a -> JSON.Value
encodeTreeWith enc = fromMaybe (JSON.Object mempty) . go where
  go = \case
    Leaf   js -> enc js
    Branch m  -> case KM.mapMaybe go m of
      js | KM.null js -> Nothing
         | otherwise  -> Just $ JSON.Object js


decodeTree :: Tree (Entry a) -> JSON.Value -> JSON.Parser Metadata
decodeTree (Leaf   leaf) js = leaf.parser js
decodeTree (Branch kmap) js =
  mconcat <$> sequence [ descend k (decodeTree tr) js
                       | (k,tr) <- KM.toList kmap
                       ]


----------------------------------------------------------------
-- Deriving & instances
----------------------------------------------------------------

-- | Implementation of 'toMetadata' for primitive entry
primToMetadata :: (IsMetaPrim a) => a -> MetadataF k
primToMetadata a = Metadata $ Map.singleton (typeOf a) (MetaEntry a)

-- | Implementation of 'metaTree' for primitive entry
singletonMetaTree :: forall a. (IsMetaPrim a) => MetaTree a
singletonMetaTree
  = MetaTree
  $ flip (foldr $ \k -> MBranch . KM.singleton k) (metaLocation @a)
  $ MLeaf Entry
    { tyRep   = typeOf (undefined :: a)
    , encoder = Just . metaToJson @a
    , parser  = \json -> do
        a <- JSON.prependFailure ("While parsing metadata " ++ typeName @a ++ "\n")
           $ parseMeta @a json
        return $! primToMetadata a
    }

-- | Mark every leaf in MetaTree as an optional
optionalMetaTree :: MetaTree a -> MetaTree (Maybe a)
optionalMetaTree (MetaTree mtree)
  = MetaTree (toOpt <$> mtree)
  where
    toOpt e = Entry { tyRep   = e.tyRep
                    , encoder = \ma -> e.encoder =<< ma
                    , parser  = \case
                        JSON.Null -> pure mempty
                        js        -> e.parser js
                    }


instance IsMeta a => IsMeta (Maybe a) where
  metaTree            = optionalMetaTree metaTree
  parseMetadata       = optional parseMetadata
  metadataKeys _      = metadataKeys (proxy# @a)
  toMetadata (Just a) = toMetadata a
  toMetadata Nothing  = mempty

-- | Derive 'IsMeta' instance with given path
newtype AsMeta (path :: [Symbol]) a = AsMeta a
  deriving newtype MetaEncoding

instance (Typeable path, MetaEncoding a, MetaPath path) => IsMetaPrim (AsMeta path a) where
  metaLocation = getMetaPath @path

instance (Typeable path, MetaEncoding a, IsMetaPrim a, MetaPath path) => IsMeta (AsMeta path a) where
  metaTree              = coerce $ singletonMetaTree @a
  metadataKeys _ f      = f (proxy# @a)
  parseMetadata         = AsMeta <$> primMetaParser
  toMetadata (AsMeta a) = primToMetadata a

-- | Type class for converting type level literals into sequence of
--   JSON keys
class MetaPath (xs :: [Symbol]) where
  getMetaPath :: [KM.Key]

instance MetaPath '[] where
  getMetaPath = []

instance (KnownSymbol n, MetaPath ns) => MetaPath (n ': ns) where
  getMetaPath = fromText (fieldName @n) : getMetaPath @ns


----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance IsMeta () where
  metaTree         = mempty
  parseMetadata    = pure ()
  metadataKeys _ _ = mempty
  toMetadata   _   = mempty

deriving via Generically (a,b)
    instance (IsMeta a, IsMeta b) => IsMeta (a,b)

deriving via Generically (a,b,c)
    instance (IsMeta a, IsMeta b, IsMeta c) => IsMeta (a,b,c)

deriving via Generically (a,b,c,d)
    instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d) => IsMeta (a,b,c,d)


deriving via Generically (a,b,c,d,e)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e
             ) => IsMeta (a,b,c,d,e)

deriving via Generically (a,b,c,d,e,f)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f
             ) => IsMeta (a,b,c,d,e,f)

deriving via Generically (a,b,c,d,e,f,g)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f, IsMeta g
             ) => IsMeta (a,b,c,d,e,f,g)

deriving via Generically (a,b,c,d,e,f,g,h)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f, IsMeta g, IsMeta h
             ) => IsMeta (a,b,c,d,e,f,g,h)

deriving via Generically (a,b,c,d,e,f,g,h,i)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f, IsMeta g, IsMeta h
             , IsMeta i
             ) => IsMeta (a,b,c,d,e,f,g,h,i)

deriving via Generically (a,b,c,d,e,f,g,h,i,j)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f, IsMeta g, IsMeta h
             , IsMeta i, IsMeta j
             ) => IsMeta (a,b,c,d,e,f,g,h,i,j)

deriving via Generically (a,b,c,d,e,f,g,h,i,j,k)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f, IsMeta g, IsMeta h
             , IsMeta i, IsMeta j, IsMeta k
             ) => IsMeta (a,b,c,d,e,f,g,h,i,j,k)

deriving via Generically (a,b,c,d,e,f,g,h,i,j,k,l)
    instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
             , IsMeta e, IsMeta f, IsMeta g, IsMeta h
             , IsMeta i, IsMeta j, IsMeta k, IsMeta l
             ) => IsMeta (a,b,c,d,e,f,g,h,i,j,k,l)

instance ( Typeable a
         , Generic a
         , GIsMetaProd (Rep a)
         ) => IsMeta (Generically a) where
  metaTree        = (\(Generically a) -> GHC.Generics.from a) >$< gmetaTree @(Rep a) @()
  toMetadata = gtoMetadata . GHC.Generics.from . (\(Generically a) -> a)
  parseMetadata    = Generically . GHC.Generics.to <$> gparseMetadata
  metadataKeys _   = gmetadataKeys (proxy# @(Rep a))



-- Type class which exists solely for deriving of IsFromMeta for tuples
class GIsMetaProd f where
  gmetaTree      :: MetaTree (f p)
  gparseMetadata :: Monad m => ParserM k m (f p)
  gmetadataKeys  :: Monoid m
                 => Proxy# f
                 -> (forall x. IsMetaPrim x => Proxy# x -> m)
                 -> m
  gtoMetadata    :: f p -> MetadataF k

deriving newtype instance GIsMetaProd f => GIsMetaProd (M1 i c f)



instance (GIsMetaProd f, GIsMetaProd g) => GIsMetaProd (f :*: g) where
  gmetaTree = ((\(f :*: _) -> f) >$< gmetaTree)
           <> ((\(_ :*: g) -> g) >$< gmetaTree)
  gparseMetadata = liftA2 (:*:) gparseMetadata gparseMetadata
  gmetadataKeys _ f = gmetadataKeys (proxy# @f) f
                   <> gmetadataKeys (proxy# @g) f
  gtoMetadata (f :*: g) = gtoMetadata f <> gtoMetadata g

instance (IsMeta a) => GIsMetaProd (K1 i a) where
  gmetaTree        = coerce (metaTree @a)
  gmetadataKeys  _ = metadataKeys (proxy# @a)
  gparseMetadata   = K1 <$> parseMetadata
  gtoMetadata      = toMetadata . unK1



----------------------------------------------------------------
-- Helpers
----------------------------------------------------------------

-- | Either with accumulating Applicative instance
data Err e a = Err e
             | OK  a
             deriving stock (Show,Functor)

instance Monoid e => Applicative (Err e) where
  pure = OK
  Err e1 <*> Err e2 = Err (e1 <> e2)
  Err e  <*> OK  _  = Err e
  OK  _  <*> Err e  = Err e
  OK  f  <*> OK  a  = OK (f a)

pattern Null :: Foldable f => f a
pattern Null <- (null -> True)
