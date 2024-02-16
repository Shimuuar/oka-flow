{-# LANGUAGE AllowAmbiguousTypes   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE UndecidableInstances  #-}
-- |
-- Metadata for data analysis. Usually any nontrivial data analysis
-- requires specifying multiple groups of parameters and without some
-- overarching methodology it's difficult to work with. We need
-- following features:
--
-- 1. Standard and easy way for JSON\/YAML marshalling.
--
-- 2. Ability to either specify all necessary types statically or to
--    work with dynamic dictionary of parameters.
--
-- We model each group as ordinary haskell records. For purposes of
-- serialization they are leaves on JSON tree.
module OKA.Metadata
  ( -- * Metadata
    Metadata
  , metadata
  , metadataF
  , metadataMay
  , deleteFromMetadata
  , restrictMetadata
  , restrictMetadataByKey
  , IsMeta(..)
    -- ** Writing 'IsMeta' instances
  , MetaTree
  , metaTreeIso
  , metaTreeProduct
    -- ** Encoding & decoding
  , encodeMetadataDynEither
  , encodeMetadataDyn
  , encodeMetadataEither
  , encodeMetadata
  , decodeMetadataEither
  , decodeMetadata
  , readMetadataEither
  , readMetadata
    -- * JSON serialization
  , MetaEncoding(..)
    -- ** Deriving via
  , AsAeson(..)
  , AsReadShow(..)
  , AsRecord(..)
  , AsMeta(..)
  ) where

import Control.Exception                 (throwIO)
import Control.Monad
import Control.Monad.IO.Class
import Control.Lens

import Data.Aeson                 ((.:))
import Data.Aeson                 qualified as JSON
import Data.Aeson.KeyMap          qualified as KM
import Data.Aeson.Key             (fromText,toText)
import Data.Aeson.Types           qualified as JSON
import Data.These
import Data.Yaml                  qualified as YAML
import Data.Foldable              (toList)
import Data.Map.Strict            qualified as Map
import Data.Map.Strict            (Map)
import Data.Set                   (Set)
import Data.Set                   qualified as Set
import Data.Monoid                (Dual(..))
import Data.Typeable
import Data.Text                  (Text)
import Data.Text                  qualified as T
import GHC.TypeLits

import OKA.Metadata.Encoding
import OKA.Metadata.Util

----------------------------------------------------------------
-- Metadata representation
----------------------------------------------------------------

-- | Dynamic product of metadata dictionaries. Each is instance of
--   'IsMeta' type class. Note this type class has instances for
--   products for primitive dicts. Those are unpacked.
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
                                    Nothing -> deleteFromMetadata @a m
                                )

-- | Only keep keys which corresponds to a type
restrictMetadata :: forall a. IsMeta a => Metadata -> Metadata
restrictMetadata = restrictMetadataByKey (metadataKeySet @a)

-- | Only keep keys that are in the set of keys
restrictMetadataByKey :: Set TypeRep -> Metadata -> Metadata
restrictMetadataByKey keys (Metadata m) = Metadata $ Map.restrictKeys m keys

-- | Delete dictionaries corresponding to this data type from metadata
deleteFromMetadata :: forall a. IsMeta a => Metadata -> Metadata
deleteFromMetadata (Metadata m) = Metadata $ Map.withoutKeys m (metadataKeySet @a)

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
-- JSON encoding of metadata
----------------------------------------------------------------



encodeMetadataDyn :: Metadata -> JSON.Value
encodeMetadataDyn = either error id . encodeMetadataDynEither

encodeMetadataDynEither :: Metadata -> Either String JSON.Value
encodeMetadataDynEither (Metadata m) = do
  onErr (sequenceA entries) >>= \case
    []   -> Right $ JSON.Object mempty
    s:ss -> asJSON <$> onErr (reduce s ss)
  where
    onErr (Err err) = Left $ keyClashesMsg @Metadata err
    onErr (OK  a  ) = Right a
    --
    entries = [ fmap (\e -> e.encoder a) <$> metaTree.get
              | MetaEntry a <- toList m
              ]
    --
    reduce s0 []     = OK s0
    reduce s0 (s:ss) = case zipSpine id id s0 s of
      Err err -> Err err
      OK  s'  -> reduce s' ss
    --
    asJSON (Leaf   json) = json
    asJSON (Branch mp)   = JSON.Object $ asJSON <$> mp


-- | Encode metadata as JSON value
encodeMetadata :: forall a. IsMeta a => a -> JSON.Value
encodeMetadata = either error id . encodeMetadataEither

-- | Encode metadata as JSON value
encodeMetadataEither :: forall a. IsMeta a => a -> Either String JSON.Value
encodeMetadataEither a = case metaTree.get of
  Err err  -> Left  $ keyClashesMsg @a err
  OK  tree -> Right $ reduce tree
  where
    reduce (Leaf Entry{..}) = encoder a
    reduce (Branch m)       = JSON.Object $ reduce <$> m

-- | Decode metadata from JSON value
decodeMetadataEither :: forall a. IsMeta a => JSON.Value -> Either String a
decodeMetadataEither json =
  case (metaTree @a).get of
    Err err  -> Left $ keyClashesMsg @a err
    OK  tree -> do
      m <- consumeKM tree
      case fromMetadata m of
        Just a  -> pure a
        Nothing -> error "Invalid conversion. IsMeta is bugged"
  where
    consumeKM :: Spine (Entry a) -> Either String Metadata
    consumeKM (Leaf leaf) = do
      e@(MetaEntry a) <- leaf.parser json
      pure $ Metadata $ Map.singleton (typeOf a) e
    consumeKM (Branch kmap)
      = foldM (\m tree -> (m <>) <$> consumeKM tree) mempty kmap

-- | Decode metadata from JSON value
decodeMetadata :: forall a. IsMeta a => JSON.Value -> a
decodeMetadata json = case decodeMetadataEither json of
  Right a -> a
  Left  e -> error e

-- | Read metadata described by given data type from file.
readMetadataEither :: forall a m. (IsMeta a, MonadIO m) => FilePath -> m (Either String a)
readMetadataEither path = liftIO $ do
  YAML.decodeFileEither path <&> \case
    Left  err  -> Left $ show err
    Right json -> decodeMetadataEither json

-- | Read metadata described by given data type from file.
readMetadata :: forall a m. (IsMeta a, MonadIO m) => FilePath -> m a
readMetadata path = liftIO $ do
  YAML.decodeFileEither path >>= \case
    Left  err  -> throwIO err
    Right json -> case decodeMetadataEither json of
      Left  err  -> error err
      Right a    -> pure a



----------------------------------------------------------------
-- Bidirectional parser
----------------------------------------------------------------

-- | Bidirectional parser for metadata. Only product types are
--   supported
newtype MetaTree a = MetaTree { get :: Err [[Text]] (Spine (Entry a)) }

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

-- | Spine of a key tree
data Spine a
  = Leaf   a
  | Branch (KM.KeyMap (Spine a))
  deriving stock (Functor)

-- | Leaf corresponding to a single record in metadata tree
data Entry a = Entry
  { encoder :: a -> JSON.Value
  , parser  :: JSON.Value -> Either String MetaEntry
  }

instance Contravariant Entry where
  contramap f e = Entry (e.encoder . f) e.parser


-- | Apply isomorphism to tree of metadata
metaTreeIso :: Iso' a b -> MetaTree a -> MetaTree b
metaTreeIso len (MetaTree tree) = MetaTree $ go <$> tree
  where
    go t = contramap (view (from len)) <$> t

-- | Product of two trees.
metaTreeProduct
  :: MetaTree a
  -> MetaTree b
  -> MetaTree (a,b)
metaTreeProduct (MetaTree meta1) (MetaTree meta2) =
  case merge <$> meta1 <*> meta2 of
    Err e      -> MetaTree (Err e)
    OK (Err e) -> MetaTree (Err e)
    OK (OK  r) -> MetaTree (OK r)
  where
    merge treeA treeB =
      case zipSpine (contramap fst) (contramap snd) treeA treeB of
        Err e    -> Err e
        OK  keys -> OK keys

zipSpine :: (a -> c) -> (b -> c) -> Spine a -> Spine b -> Err [[Text]] (Spine c)
zipSpine a2c b2c = go []
  where
    go path (Branch m1) (Branch m2)
      = fmap Branch
      $ sequenceA
      $ KM.alignWithKey (merge path) m1 m2
    go path _ _ = Err [reverse path]
    --
    merge path k = \case
      This  a   -> OK (a2c <$> a)
      That  b   -> OK (b2c <$> b)
      These a b -> go (toText k : path) a b



----------------------------------------------------------------
-- Deriving & instances
----------------------------------------------------------------




-- | Derive 'IsMeta' instance with given path
newtype AsMeta (path :: [Symbol]) a = AsMeta a

instance (Typeable path, MetaEncoding a, IsMeta a, MetaPath path) => IsMeta (AsMeta path a) where
  metaTree = MetaTree $ OK
    $ flip (foldr $ \nm -> Branch . KM.singleton (fromText nm)) path
    $ Leaf Entry
      { encoder = \(AsMeta a) -> metaToJson a
      , parser  = \json -> do
          let descend k parser
                = JSON.prependFailure (" - " ++ T.unpack k ++ "\n")
                . metaWithObject (\o -> parser =<< (o .: fromText k))
          let parser
                = JSON.prependFailure ("While parsing metadata " ++ typeName @a ++ "\n")
                . foldr descend (parseMeta @a) path
          a <- JSON.parseEither parser json
          pure $ MetaEntry a
      }
    where
      path = getMetaPath @path
  --
  toMetadata (AsMeta a) = Metadata $ Map.singleton (typeOf a) (MetaEntry a)
  fromMetadata (Metadata m) = do MetaEntry a <- Map.lookup (typeOf (undefined :: a)) m
                                 AsMeta <$> cast a
  metadataKeySet = Set.singleton (typeOf (undefined :: a))




class MetaPath (xs :: [Symbol]) where
  getMetaPath :: [Text]

instance MetaPath '[] where
  getMetaPath = []

instance (KnownSymbol n, MetaPath ns) => MetaPath (n ': ns) where
  getMetaPath = fieldName @n : getMetaPath @ns

----------------------------------------------------------------
-- Instances
----------------------------------------------------------------


instance (IsMeta a, IsMeta b) => IsMeta (a,b) where
  metaTree = metaTreeProduct metaTree metaTree
  toMetadata (a,b) = toMetadata a <> toMetadata b
  fromMetadata m = (,) <$> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b


instance (IsMeta a, IsMeta b, IsMeta c) => IsMeta (a,b,c) where
  metaTree
    = metaTreeIso (iso (\((a,b),c) -> (a,b,c)) (\(a,b,c) -> ((a,b),c)))
    $ metaTree `metaTreeProduct` metaTree `metaTreeProduct` metaTree
  toMetadata (a,b,c) = mconcat [ toMetadata a, toMetadata b, toMetadata c]
  fromMetadata m = (,,) <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c


instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d) => IsMeta (a,b,c,d) where
  metaTree
    = metaTreeIso (iso (\((a,b),(c,d)) -> (a,b,c,d)) (\(a,b,c,d) -> ((a,b),(c,d))))
    $ metaTree `metaTreeProduct` metaTree
  toMetadata (a,b,c,d) = mconcat [ toMetadata a, toMetadata b, toMetadata c
                                 , toMetadata d]
  fromMetadata m = (,,,) <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e) => IsMeta (a,b,c,d,e) where
  metaTree
    = metaTreeIso (iso (\((a,b),(c,d,e)) -> (a,b,c,d,e)) (\(a,b,c,d,e) -> ((a,b),(c,d,e))))
    $ metaTree `metaTreeProduct` metaTree
  toMetadata (a,b,c,d,e) = mconcat [ toMetadata a, toMetadata b, toMetadata c
                                   , toMetadata d, toMetadata e]
  fromMetadata m = (,,,,)
                <$> fromMetadata m <*> fromMetadata m <*> fromMetadata m <*> fromMetadata m
                <*> fromMetadata m
  metadataKeySet = metadataKeySet @a
                <> metadataKeySet @b
                <> metadataKeySet @c
                <> metadataKeySet @d
                <> metadataKeySet @e

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f) => IsMeta (a,b,c,d,e,f) where
  metaTree
    = metaTreeIso (iso (\((a,b,c),(d,e,f)) -> (a,b,c,d,e,f)) (\(a,b,c,d,e,f) -> ((a,b,c),(d,e,f))))
    $ metaTree `metaTreeProduct` metaTree
  toMetadata (a,b,c,d,e,f) = mconcat [ toMetadata a, toMetadata b, toMetadata c
                                     , toMetadata d, toMetadata e, toMetadata f]
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
  metaTree
    = metaTreeIso (iso (\((a,b,c),(d,e,f,g)) -> (a,b,c,d,e,f,g)) (\(a,b,c,d,e,f,g) -> ((a,b,c),(d,e,f,g))))
    $ metaTree `metaTreeProduct` metaTree
  toMetadata (a,b,c,d,e,f,g) = mconcat [ toMetadata a, toMetadata b, toMetadata c
                                       , toMetadata d, toMetadata e, toMetadata f
                                       , toMetadata g]
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
  metaTree
    = metaTreeIso (iso (\((a,b,c,d),(e,f,g,h)) -> (a,b,c,d,e,f,g,h))
                       (\(a,b,c,d,e,f,g,h)     -> ((a,b,c,d),(e,f,g,h))))
    $ metaTree `metaTreeProduct` metaTree
  toMetadata (a,b,c,d,e,f,g,h) = mconcat [ toMetadata a, toMetadata b, toMetadata c
                                         , toMetadata d, toMetadata e, toMetadata f
                                         , toMetadata g, toMetadata h]
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

keyClashesMsg :: forall a. Typeable a => [[Text]] -> String
keyClashesMsg err
  = unlines
  $ ("Key clashes for data type " ++ typeName @a)
  : [ "  - " ++ T.unpack (T.intercalate "." e)
    | e <- err
    ]
