{-# LANGUAGE AllowAmbiguousTypes  #-}
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
    Metadata
  , metadata
  , metadataF
  , metadataMay
  , restrictMetaByType
  , restrictMetaByKeys
  , deleteFromMetaByType
  , deleteFromMetaByKeys
    -- * 'IsMeta' type class
  , IsMeta(..)
  , MetaTree
  , primToMetadata
  , primFromMetadata
  , singletonMetaTree
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

import Control.Lens
import Control.Exception

import Data.Aeson                 ((.:))
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

import OKA.Metadata.Encoding
import OKA.Metadata.Util

----------------------------------------------------------------
-- Metadata representation
----------------------------------------------------------------

-- | Dynamically typed collection of values which could be looked up
--   by their type and serialized into JSON. API supports both
--   operations with single types and products of several types (as
--   tuples).
--
--   Semigroup instance is right biased.
newtype Metadata = Metadata (Map TypeRep MetaEntry)
  deriving Semigroup via Dual (Map TypeRep MetaEntry)
  deriving newtype Monoid

-- Helper existential wrapper for metadata
data MetaEntry where
  MetaEntry :: (IsMeta a) => a -> MetaEntry

-- | Lens for accessing dictionary from dynamic 'Metadata'. Will fail if
--   dictionary is not part of bundle
metadata :: forall a. IsMeta a => Lens' Metadata a
metadata = metadataMay . lens unpack (const Just)
  where
    unpack (Just a) = a
    unpack Nothing  = error $ "Metadata doesn't have data type: " ++ typeName @a

-- | Fold for accessing dictionary from dynamic 'Metadata'.
metadataF :: forall a. IsMeta a => Fold Metadata a
metadataF = metadataMay . _Just

-- | Lens for accessing dictionary from dynamic 'Metadata'.
metadataMay :: forall a. IsMeta a => Lens' Metadata (Maybe a)
metadataMay = lens fromMetadata (\m -> \case
                                    Just a  -> m <> toMetadata a
                                    Nothing -> deleteFromMetaByType @a m
                                )

-- | Only keep keys which corresponds to a type
restrictMetaByType :: forall a. IsMeta a => Metadata -> Metadata
restrictMetaByType = restrictMetaByKeys (metadataKeySet @a)

-- | Only keep keys that are in the set of keys
restrictMetaByKeys :: Set TypeRep -> Metadata -> Metadata
restrictMetaByKeys keys (Metadata m) = Metadata $ Map.restrictKeys m keys

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetaByType :: forall a. IsMeta a => Metadata -> Metadata
deleteFromMetaByType = deleteFromMetaByKeys (metadataKeySet @a)

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetaByKeys :: Set TypeRep -> Metadata -> Metadata
deleteFromMetaByKeys k (Metadata m) = Metadata $ Map.withoutKeys m k

-- | Type class for data types for types that represent metadata and
--   their products. It describes how to serialize them and how to
--   access them in dynamic product 'Metadata'.
--
--   Note on serialization. Metadata is serialized as tree of JSON
--   objects. Overlapping of keys is not allowed. Static checking of
--   such property is not practical so it done in runtime.
class Typeable a => IsMeta a where
  -- | Description on how to serialize metadata into JSON tree. This
  --   bidirectional parser allows to detect key collisions during
  --   both encoding and decoding.
  metaTree :: MetaTree a
  -- | Convert data type to dynamic dictionary
  toMetadata :: a -> Metadata
  -- | Look up type in dictionary
  fromMetadata :: Metadata -> Maybe a
  -- | Set of keys for the type corresponding to the metadata
  metadataKeySet :: Set TypeRep


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
    OK  xs  -> Right $ encodeTreeWith snd xs
  where
    entries = [ (\e -> (e.tyRep, e.encoder a)) <$> metaTree.get
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
    OK  tree -> bimap JsonError id $ do
      m <- JSON.parseEither (annotate . decodeTree tree) json
      case fromMetadata m of
        Just a  -> pure a
        Nothing -> error "Invalid conversion. IsMeta is bugged"
  where
    annotate = JSON.prependFailure ("\nFailed to decode metadata " ++ show (typeOf (undefined :: a)) ++ "\n")



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
  contramap f e = Entry e.tyRep (e.encoder . f) e.parser

instance Contravariant MetaTree where
  contramap f (MetaTree m) = MetaTree (fmap (contramap f) m)


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
  where
    descend k parser
      = JSON.prependFailure (" - " ++ T.unpack (toText k) ++ "\n")
      . metaWithObject (\o -> parser =<< (o .: k))


----------------------------------------------------------------
-- Deriving & instances
----------------------------------------------------------------

-- | Implementation of 'toMetadata' for primitive entry
primToMetadata :: (IsMeta a) => a -> Metadata
primToMetadata a = Metadata $ Map.singleton (typeOf a) (MetaEntry a)

-- | Implementation of 'fromMetadata' for primitive entry
primFromMetadata :: forall a. (IsMeta a) => Metadata -> Maybe a
primFromMetadata (Metadata m) = do
  MetaEntry a <- Map.lookup (typeOf (undefined :: a)) m
  cast a

-- | Implementation of 'metaTree' for primitive entry
singletonMetaTree :: forall a. (MetaEncoding a, IsMeta a) => [KM.Key] -> MetaTree a
singletonMetaTree path
  = MetaTree
  $ flip (foldr $ \k -> MBranch . KM.singleton k) path
  $ MLeaf Entry
    { tyRep   = typeOf (undefined :: a)
    , encoder = metaToJson @a
    , parser  = \json -> do
        a <- JSON.prependFailure ("While parsing metadata " ++ typeName @a ++ "\n")
           $ parseMeta @a json
        return $ mempty & metadata .~ a
    }

-- | Derive 'IsMeta' instance with given path
newtype AsMeta (path :: [Symbol]) a = AsMeta a

instance (Typeable path, MetaEncoding a, IsMeta a, MetaPath path) => IsMeta (AsMeta path a) where
  metaTree       = coerce (singletonMetaTree @a (getMetaPath @path))
  toMetadata     = coerce (primToMetadata    @a)
  fromMetadata   = coerce (primFromMetadata  @a)
  metadataKeySet = Set.singleton (typeOf (undefined :: a))



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
  metaTree       = mempty
  toMetadata   _ = mempty
  fromMetadata _ = Just ()
  metadataKeySet = mempty

instance (IsMeta a, IsMeta b) => IsMeta (a,b) where
  metaTree = (fst >$< metaTree)
          <> (snd >$< metaTree)
  toMetadata (a,b) = toMetadata a <> toMetadata b
  fromMetadata m = (,) <$> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b


instance (IsMeta a, IsMeta b, IsMeta c) => IsMeta (a,b,c) where
  metaTree = ((\(a,_,_) -> a) >$< metaTree)
          <> ((\(_,a,_) -> a) >$< metaTree)
          <> ((\(_,_,a) -> a) >$< metaTree)
  toMetadata (a,b,c) = mconcat [ toMetadata a, toMetadata b, toMetadata c]
  fromMetadata m = (,,) <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c


instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d) => IsMeta (a,b,c,d) where
  metaTree = ((\(a,_,_,_) -> a) >$< metaTree)
          <> ((\(_,a,_,_) -> a) >$< metaTree)
          <> ((\(_,_,a,_) -> a) >$< metaTree)
          <> ((\(_,_,_,a) -> a) >$< metaTree)
  toMetadata (a,b,c,d) = mconcat
    [ toMetadata a, toMetadata b, toMetadata c, toMetadata d
    ]
  fromMetadata m = (,,,) <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m

  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e) => IsMeta (a,b,c,d,e) where
  metaTree = ((\(a,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,a,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,a,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,a,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,a) -> a) >$< metaTree)
  toMetadata (a,b,c,d,e) = mconcat
    [ toMetadata a, toMetadata b, toMetadata c, toMetadata d
    , toMetadata e
    ]
  fromMetadata m = (,,,,)
                <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
                <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d
                <> metadataKeySet @e

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f) => IsMeta (a,b,c,d,e,f) where
  metaTree = ((\(a,_,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,a,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,a,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,a,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,a,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,_,a) -> a) >$< metaTree)
  toMetadata (a,b,c,d,e,f) = mconcat
    [ toMetadata a, toMetadata b, toMetadata c, toMetadata d
    , toMetadata e, toMetadata f
    ]
  fromMetadata m = (,,,,,)
                <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
                <*> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d
                <> metadataKeySet @e
                <> metadataKeySet @f

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f,IsMeta g) => IsMeta (a,b,c,d,e,f,g) where
  metaTree = ((\(a,_,_,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,a,_,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,a,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,a,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,a,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,_,a,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,_,_,a) -> a) >$< metaTree)
  toMetadata (a,b,c,d,e,f,g) = mconcat
    [ toMetadata a, toMetadata b, toMetadata c, toMetadata d
    , toMetadata e, toMetadata f, toMetadata g
    ]
  fromMetadata m = (,,,,,,)
                <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
                <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d
                <> metadataKeySet @e
                <> metadataKeySet @f
                <> metadataKeySet @g

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f,IsMeta g,IsMeta h) => IsMeta (a,b,c,d,e,f,g,h) where
  metaTree = ((\(a,_,_,_,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,a,_,_,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,a,_,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,a,_,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,a,_,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,_,a,_,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,_,_,a,_) -> a) >$< metaTree)
          <> ((\(_,_,_,_,_,_,_,a) -> a) >$<  metaTree)
  toMetadata (a,b,c,d,e,f,g,h) = mconcat
    [ toMetadata a, toMetadata b, toMetadata c, toMetadata d
    , toMetadata e, toMetadata f, toMetadata g, toMetadata h
    ]
  fromMetadata m = (,,,,,,,)
                <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
                <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d
                <> metadataKeySet @e
                <> metadataKeySet @f
                <> metadataKeySet @g
                <> metadataKeySet @h


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
