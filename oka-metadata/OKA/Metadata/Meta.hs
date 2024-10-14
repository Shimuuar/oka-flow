{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DefaultSignatures    #-}
{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE OverloadedStrings    #-}
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
  , metadata
  , metadataMay
  , restrictMetaByType
  , restrictMetaByKeys
  , deleteFromMetaByType
  , deleteFromMetaByKeys
    -- * HKD metadata
  , UnPure(..)
  , MetadataF
  , hkdMetadata
  , hkdMetadataMay
  , mapMetadataF
  , mapMaybeMetadataF
  , traverseMetadataF
  , foldMetadataF
  , foldrMetadataF
  , foldMMetadataF
  , metadataFToList
    -- * 'IsMeta' type class
  , IsMetaPrim(..)
  , IsFromMeta(..)
  , IsMeta(..)
  , toMetadata
  , fromMetadata
  , MetaTree
  , primToMetadata
  , primFromMetadata
  , singletonMetaTree
  , decodeMetadataPrimEither
  , MetaPath(..)
  , Optional(..)
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
import Data.Foldable              (toList)
import Data.Functor.Contravariant
import Data.Map.Strict            qualified as Map
import Data.Map.Strict            (Map)
import Data.Set                   (Set)
import Data.Set                   qualified as Set
import Data.Monoid                (Dual(..))
import Data.Typeable
import Data.Text                  (Text)
import Data.Text                  qualified as T
import Data.Yaml                  qualified as YAML
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
-- Here metadata is collection of parameters which are passed to
-- analysis in addition to data being processed: data selection and
-- transformation parameters etc.
--
-- It's modelled as an opaque open product of haskell data types. We
-- don't track types since it not practical. Types will get way too
-- complicated: we could easily get to tens of parameters passed
-- around and we don't want to keep track of it.
--
-- Note on serialization. Metadata is serialized as tree of JSON
-- objects. Overlapping of keys is not allowed. Static checking of
-- such property is not practical so it done in runtime.


-- | Higher kinded version of 'Metadata'
newtype MetadataF f = Metadata (Map TypeRep (MetaEntry f))

-- | Dynamically typed collection of values which could be looked up
--   by their type and serialized into JSON. API supports both
--   operations with single types and products of several types (as
--   tuples).
type Metadata = MetadataF Identity

-- | Right biased
deriving via Dual (Map TypeRep (MetaEntry f)) instance Semigroup (MetadataF f)
-- | Right biased
deriving newtype instance Monoid (MetadataF f)


-- Helper existential wrapper for metadata
data MetaEntry f where
  MetaEntry :: (IsMetaPrim a) => f a -> MetaEntry f


-- | Applicatives for which exists partial inverse of pure.
class Applicative f => UnPure f where
  unPure :: f a -> Maybe a

instance UnPure Identity where
  unPure = Just . runIdentity

-- | Lens for accessing dictionary from dynamic 'Metadata'. Will fail if
--   dictionary is not part of bundle
metadata :: forall a f. (IsMeta a, UnPure f) => Lens' (MetadataF f) a
metadata = hkdMetadata . lens unpack (\_ -> pure)
  where
    unpack f = case unPure f of
      Just a -> a
      Nothing -> error $ "metadata: cannot unwrap value of type: " ++ typeName @a

-- | Lens for accessing dictionary from dynamic 'Metadata'.
metadataMay :: forall a f. (IsMeta a, UnPure f) => Lens' (MetadataF f) (Maybe a)
metadataMay = hkdMetadataMay . lens unpack (\_ -> fmap pure)
  where
    unpack Nothing = Nothing
    unpack (Just f) = case unPure f of
      Just a  -> Just a
      Nothing -> error $ "metadataMay: cannot unwrap value of type: " ++ typeName @a


-- | Lens for accessing dictionary from dynamic 'Metadata'.
hkdMetadataMay :: forall a f. (IsMeta a, Applicative f) => Lens' (MetadataF f) (Maybe (f a))
hkdMetadataMay = lens fromHkdMetadata
  (\m -> \case
      Just a  -> m <> toHkdMetadata a
      Nothing -> deleteFromMetaByType @a m
  )

-- | Lens for accessing dictionary from dynamic 'Metadata'.
hkdMetadata :: forall a f. (IsMeta a, Applicative f) => Lens' (MetadataF f) (f a)
hkdMetadata = hkdMetadataMay . lens unpack (const Just)
  where
    unpack (Just a) = a
    unpack Nothing  = error $ "Metadata doesn't have data type: " ++ typeName @a



-- | Only keep keys which corresponds to a type
restrictMetaByType :: forall a f. IsFromMeta a => MetadataF f -> MetadataF f
restrictMetaByType = restrictMetaByKeys (metadataKeySet (proxy# @a))

-- | Only keep keys that are in the set of keys
restrictMetaByKeys :: Set TypeRep -> MetadataF f -> MetadataF f
restrictMetaByKeys keys (Metadata m) = Metadata $ Map.restrictKeys m keys

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetaByType :: forall a f. IsFromMeta a => MetadataF f -> MetadataF f
deleteFromMetaByType = deleteFromMetaByKeys (metadataKeySet (proxy# @a))

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetaByKeys :: Set TypeRep -> MetadataF f -> MetadataF f
deleteFromMetaByKeys k (Metadata m) = Metadata $ Map.withoutKeys m k


mapMetadataF :: (forall x. IsMetaPrim x => f x -> g x) -> MetadataF f -> MetadataF g
mapMetadataF fun (Metadata m)
  = Metadata
  $ (\(MetaEntry a) -> MetaEntry (fun a)) <$> m

mapMaybeMetadataF :: (forall x. IsMetaPrim x => f x -> Maybe (g x)) -> MetadataF f -> MetadataF g
mapMaybeMetadataF fun (Metadata m)
  = Metadata
  $ Map.mapMaybe (\(MetaEntry a) -> MetaEntry <$> fun a) m

traverseMetadataF
  :: Applicative m
  => (forall x. IsMetaPrim x => f x -> m (g x))
  -> MetadataF f
  -> m (MetadataF g)
traverseMetadataF fun (Metadata m)
  = Metadata <$> traverse (\(MetaEntry a) -> MetaEntry <$> fun a) m

foldMetadataF
  :: (Monoid m)
  => (forall x. IsMetaPrim x => f x -> m)
  -> MetadataF f
  -> m
foldMetadataF fun (Metadata m) = foldMap (\(MetaEntry a) -> fun a) m

foldrMetadataF
  :: (forall x. IsMetaPrim x => f x -> b -> b)
  -> b
  -> MetadataF f
  -> b
foldrMetadataF fun b0 (Metadata m) = foldr (\(MetaEntry a) b -> fun a b) b0 m

foldMMetadataF
  :: Monad m
  => (forall x. IsMetaPrim x => a -> f x -> m a)
  -> a -> MetadataF f -> m a
foldMMetadataF fun a0 (Metadata m)
  = foldM (\a (MetaEntry x) -> fun a x) a0 m

metadataFToList
  :: (forall x. IsMetaPrim x => f x -> Maybe a)
  -> MetadataF f
  -> [a]
metadataFToList fun = foldrMetadataF
  (\m -> case fun m of
           Just a -> (a:)
           _      -> id
  ) []



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
class Typeable a => IsFromMeta a where
  -- | Description on how to serialize metadata into JSON tree. This
  --   bidirectional parser allows to detect key collisions during
  --   both encoding and decoding.
  metaTree :: MetaTree a
  default metaTree :: IsMetaPrim a => MetaTree a
  metaTree = singletonMetaTree
  -- | Look up type in dictionary
  fromHkdMetadata :: (Applicative f) => MetadataF f -> Maybe (f a)
  default fromHkdMetadata :: (IsMetaPrim a) => MetadataF f -> Maybe (f a)
  fromHkdMetadata = primFromMetadata
  -- | Set of keys for the type corresponding to the metadata
  metadataKeySet :: Proxy# a -> Set TypeRep
  default metadataKeySet :: Proxy# a -> Set TypeRep
  metadataKeySet _ = Set.singleton (typeOf (undefined :: a))


-- | Convenience type class which allows to store arbitrary products
--   to the metadata.
--
--   Default implementation is in terms of 'IsMetaPrim'.
class IsFromMeta a => IsMeta a where
  -- | Convert data type to dynamic dictionary
  toHkdMetadata :: Applicative f => f a -> MetadataF f
  default toHkdMetadata :: (IsMetaPrim a) => f a -> MetadataF f
  toHkdMetadata = primToMetadata

-- | Convert data type to dynamic dictionary
toMetadata :: (IsMeta a, Applicative f) => a -> MetadataF f
toMetadata = toHkdMetadata . pure

-- | Look up type in dictionary
fromMetadata :: IsFromMeta a => Metadata -> Maybe a
fromMetadata = fmap runIdentity . fromHkdMetadata


----------------------------------------------------------------
-- Exceptions
----------------------------------------------------------------

-- | Errors in metadata processing
data MetadataError
  = KeyClashes [(TypeRep, [Text])]
  | JsonError  String
  | YamlError  YAML.ParseException

instance Show MetadataError where
  show = \case
    KeyClashes err -> unlines
      $ ("[MetadataError] Key clashes when decoding metadata")
      : [ "  - " ++ T.unpack (T.intercalate "." e) ++ "["++show ty++"]"
        | (ty,e) <- err
        ]
    JsonError s -> "[MetadataError]\n" ++ s
    YamlError e -> "[MetadataError]\n" ++ show e

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
    OK  xs  -> Right $ encodeTreeWith (runIdentity . snd) xs
  where
    entries = [ (\e -> (e.tyRep, Identity (e.encoder a))) <$> singletonMetaTree.get
              | MetaEntry (Identity a) <- toList m
              ]

-- | Encode metadata as JSON value. Will throw error in case of key clash
encodeToMetadata :: forall a. IsFromMeta a => a -> JSON.Value
encodeToMetadata = either throw id . encodeToMetadataEither

-- | Encode metadata as JSON value. Returns @Left@ in case of key clash.
encodeToMetadataEither :: forall a. IsFromMeta a => a -> Either MetadataError JSON.Value
encodeToMetadataEither a = case checkForClashes metaTree.get of
  Err err  -> Left  $ KeyClashes [(e.tyRep, path) | (e,path) <- err]
  OK  tree -> Right $ encodeTreeWith (\e -> e.encoder a) tree


-- | Decode metadata from JSON value
decodeMetadata :: forall a. IsFromMeta a => JSON.Value -> a
decodeMetadata = either throw id . decodeMetadataEither

-- | Decode metadata from JSON value
decodeMetadataEither :: forall a. IsFromMeta a => JSON.Value -> Either MetadataError a
decodeMetadataEither json =
  case checkForClashes (metaTree @a).get of
    Err err  -> Left $ KeyClashes [(e.tyRep, path) | (e,path) <- err]
    OK  tree -> bimap JsonError id $ do
      m <- JSON.parseEither (annotate . decodeTree tree) json
      case fromMetadata m of
        Just a  -> pure a
        Nothing -> error "Invalid conversion. IsFromMeta is bugged"
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
  , encoder :: a -> JSON.Value
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

encodeTreeWith :: (a -> JSON.Value) -> Tree a -> JSON.Value
encodeTreeWith enc = go where
  go = \case
    Leaf   js -> enc js
    Branch m  -> JSON.Object (go <$> m)

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
primToMetadata :: (IsMetaPrim a) => f a -> MetadataF f
primToMetadata fa = Metadata $ Map.singleton (typeRep fa) (MetaEntry fa)

-- | Implementation of 'fromMetadata' for primitive entry
primFromMetadata :: forall a f. (IsMetaPrim a) => MetadataF f -> Maybe (f a)
primFromMetadata (Metadata m) = do
  MetaEntry a <- Map.lookup (typeOf (undefined :: a)) m
  gcast a

-- | Implementation of 'metaTree' for primitive entry
singletonMetaTree :: forall a. (IsMetaPrim a) => MetaTree a
singletonMetaTree
  = MetaTree
  $ flip (foldr $ \k -> MBranch . KM.singleton k) (metaLocation @a)
  $ MLeaf Entry
    { tyRep   = typeOf (undefined :: a)
    , encoder = metaToJson @a
    , parser  = \json -> do
        a <- JSON.prependFailure ("While parsing metadata " ++ typeName @a ++ "\n")
           $ parseMeta @a json
        return $! primToMetadata (pure a)
    }

-- | Newtype wrapper for existing metadata which turns its presence
--   optional. Missing entry will be decoded as
newtype Optional a = Optional { get :: Maybe a }
  deriving stock (Show,Eq,Ord)

instance IsFromMeta a => IsFromMeta (Optional a) where
  metaTree = case metaTree @a of
    MetaTree mtree -> MetaTree (toOpt <$> mtree)
    where
      toOpt e = Entry { tyRep   = e.tyRep
                      , encoder = \case
                          Optional Nothing  -> JSON.Null
                          Optional (Just a) -> e.encoder a
                      , parser  = \case
                          JSON.Null -> pure mempty
                          js        -> e.parser js
                      }
  fromHkdMetadata m = case fromHkdMetadata m of
    Nothing -> Just (pure (Optional Nothing))
    Just fa -> Just (Optional . Just <$> fa)
  metadataKeySet _ = metadataKeySet (proxy# @a)


-- | Derive 'IsMeta' instance with given path
newtype AsMeta (path :: [Symbol]) a = AsMeta a
  deriving newtype MetaEncoding

instance (Typeable path, MetaEncoding a, MetaPath path) => IsMetaPrim (AsMeta path a) where
  metaLocation = getMetaPath @path



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

instance IsFromMeta () where
  metaTree          = mempty
  fromHkdMetadata _ = Just (pure ())
  metadataKeySet  _ = mempty
instance IsMeta () where
  toHkdMetadata     = mempty

instance (IsFromMeta a, IsFromMeta b) => IsFromMeta (a,b) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance (IsMeta a, IsMeta b) => IsMeta (a,b) where
  toHkdMetadata   = genericToHkdMetadata

instance (IsFromMeta a, IsFromMeta b, IsFromMeta c) => IsFromMeta (a,b,c) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance (IsMeta a, IsMeta b, IsMeta c) => IsMeta (a,b,c) where
  toHkdMetadata   = genericToHkdMetadata

instance (IsFromMeta a,IsFromMeta b,IsFromMeta c,IsFromMeta d) => IsFromMeta (a,b,c,d) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d) => IsMeta (a,b,c,d) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e
         ) => IsFromMeta (a,b,c,d,e) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e
         ) => IsMeta (a,b,c,d,e) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f
         ) => IsFromMeta (a,b,c,d,e,f) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f
         ) => IsMeta (a,b,c,d,e,f) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f, IsFromMeta g
         ) => IsFromMeta (a,b,c,d,e,f,g) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f, IsMeta g
         ) => IsMeta (a,b,c,d,e,f,g) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f, IsFromMeta g, IsFromMeta h
         ) => IsFromMeta (a,b,c,d,e,f,g,h) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f, IsMeta g, IsMeta h
         ) => IsMeta (a,b,c,d,e,f,g,h) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f, IsFromMeta g, IsFromMeta h
         , IsFromMeta i
         ) => IsFromMeta (a,b,c,d,e,f,g,h,i) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f, IsMeta g, IsMeta h
         , IsMeta i
         ) => IsMeta (a,b,c,d,e,f,g,h,i) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f, IsFromMeta g, IsFromMeta h
         , IsFromMeta i, IsFromMeta j
         ) => IsFromMeta (a,b,c,d,e,f,g,h,i,j) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f, IsMeta g, IsMeta h
         , IsMeta i, IsMeta j
         ) => IsMeta (a,b,c,d,e,f,g,h,i,j) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f, IsFromMeta g, IsFromMeta h
         , IsFromMeta i, IsFromMeta j, IsFromMeta k
         ) => IsFromMeta (a,b,c,d,e,f,g,h,i,j,k) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f, IsMeta g, IsMeta h
         , IsMeta i, IsMeta j, IsMeta k
         ) => IsMeta (a,b,c,d,e,f,g,h,i,j,k) where
  toHkdMetadata   = genericToHkdMetadata

instance ( IsFromMeta a, IsFromMeta b, IsFromMeta c, IsFromMeta d
         , IsFromMeta e, IsFromMeta f, IsFromMeta g, IsFromMeta h
         , IsFromMeta i, IsFromMeta j, IsFromMeta k, IsFromMeta l
         ) => IsFromMeta (a,b,c,d,e,f,g,h,i,j,k,l) where
  metaTree        = genericMetaTree
  fromHkdMetadata = genericFromHkdMetadata
  metadataKeySet  = genericMetadataKeySet
instance ( IsMeta a, IsMeta b, IsMeta c, IsMeta d
         , IsMeta e, IsMeta f, IsMeta g, IsMeta h
         , IsMeta i, IsMeta j, IsMeta k, IsMeta l
         ) => IsMeta (a,b,c,d,e,f,g,h,i,j,k,l) where
  toHkdMetadata   = genericToHkdMetadata



genericMetaTree :: (Generic a, GIsFromMeta (Rep a))
                => MetaTree a
genericMetaTree = GHC.Generics.from >$< gmetaTree

genericToHkdMetadata :: (Generic a, GIsMetaProd (Rep a), Applicative f)
                     => f a -> MetadataF f
genericToHkdMetadata = gtoHkdMetadata . fmap GHC.Generics.from

genericFromHkdMetadata :: (Generic a, GIsFromMeta (Rep a), Applicative f)
                       => MetadataF f -> Maybe (f a)
genericFromHkdMetadata = (fmap . fmap) GHC.Generics.to . gfromHkdMetadata

genericMetadataKeySet :: forall a. (GIsFromMeta (Rep a)) => Proxy# a -> Set TypeRep
genericMetadataKeySet _ = gmetadataKeySet (proxy# @(Rep a))

-- Type class which exists solely for deriving of IsFromMeta for tuples
class GIsFromMeta f where
  gmetaTree        :: MetaTree (f p)
  gfromHkdMetadata :: (Applicative q) => MetadataF q -> Maybe (q (f p))
  gmetadataKeySet  :: Proxy# f -> Set TypeRep
class GIsMetaProd f where
  gtoHkdMetadata   :: Applicative q => q (f p) -> MetadataF q

instance GIsFromMeta f => GIsFromMeta (M1 i c f) where
  gmetaTree        = (coerce `asTypeOf` contramap unM1) $ gmetaTree @f
  gfromHkdMetadata = (fmap . fmap) M1 . gfromHkdMetadata
  gmetadataKeySet  = coerce (gmetadataKeySet @f)
instance GIsMetaProd f => GIsMetaProd (M1 i c f) where
  gtoHkdMetadata   = gtoHkdMetadata   . fmap unM1

instance (GIsFromMeta f, GIsFromMeta g) => GIsFromMeta (f :*: g) where
  gmetaTree = ((\(f :*: _) -> f) >$< gmetaTree)
           <> ((\(_ :*: g) -> g) >$< gmetaTree)
  gfromHkdMetadata m = (liftA2 . liftA2) (:*:) (gfromHkdMetadata m) (gfromHkdMetadata m)
  gmetadataKeySet  _ = gmetadataKeySet (proxy# @f) <> gmetadataKeySet (proxy# @g)
instance (GIsMetaProd f, GIsMetaProd g) => GIsMetaProd (f :*: g) where
  gtoHkdMetadata   m = gtoHkdMetadata ((\(f :*: _) -> f) <$> m)
                    <> gtoHkdMetadata ((\(_ :*: g) -> g) <$> m)

instance (IsFromMeta a) => GIsFromMeta (K1 i a) where
  gmetaTree         = coerce (metaTree @a)
  gfromHkdMetadata  = (fmap . fmap) K1 . fromHkdMetadata
  gmetadataKeySet _ = metadataKeySet (proxy# @a)
instance (IsMeta a) => GIsMetaProd (K1 i a) where
  gtoHkdMetadata    = toHkdMetadata . fmap unK1



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
