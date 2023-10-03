{-# LANGUAGE AllowAmbiguousTypes        #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost        #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MagicHash                  #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PatternSynonyms            #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE ViewPatterns               #-}
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
    Metadata(..)
  , meta
  , metaMay
    -- * Serialization
  , MetaEncoding(..)
  , IsMeta(..)
  , MetaTree
  , encodeMetadataEither
  , encodeMetadata
  , decodeMetadataEither
  , decodeMetadata
  , readMetadataEither
  , readMetadata
    -- * Writing instances
  , (.::)
  , (.==)
  , metaWithObject
  , metaWithRecord
    -- ** Deriving via
  , AsAeson(..)
  , AsReadShow(..)
  , AsRecord(..)
  , AsMeta(..)
    -- ** Special purpose parsers
  , metaSExp1
  , metaSExp1With
  , metaSExp2
  , metaSExp2With
  , metaSExp3
  , metaSExp3With
    -- ** Exhaustive parser
  , ObjParser
  , runObjParser
  , metaField
  , metaFieldM
  ) where

import Control.Applicative
import Control.Exception                 (throwIO)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class
import Control.Lens

import Data.Aeson                 (Value(..),(.:))
import Data.Aeson                 qualified as JSON
import Data.Aeson.Key             qualified as JSON
import Data.Aeson.KeyMap          qualified as KM
import Data.Aeson.Key             (fromText,toText)
import Data.Aeson.Types           qualified as JSON
import Data.These
import Data.Histogram.Bin
import Data.Yaml                  qualified as YAML
import Data.Int
import Data.Functor.Compose
import Data.Map.Strict            qualified as Map
import Data.Map.Strict            (Map)
import Data.Typeable
import Data.Word
import Data.Coerce
import Data.Text                  (Text)
import Data.Text                  qualified as T
import Data.Vector                qualified as V
import Data.Vector.Generic        ((!))
import Data.Vector.Unboxed        qualified as VU
import Data.Vector.Storable       qualified as VS
import Data.Vector.Fixed          qualified as F
import Data.Vector.Fixed.Cont     qualified as F(arity)
import Data.Vector.Fixed.Unboxed  qualified as FU
import Data.Vector.Fixed.Boxed    qualified as FB
import Data.Vector.Fixed.Storable qualified as FS
import Text.Printf
import GHC.TypeLits
import GHC.Generics               hiding (from,to)
import GHC.Generics               qualified as Generics


----------------------------------------------------------------
-- Metadata representation
----------------------------------------------------------------

-- | Dynamic dictionary of metadata for analysis. It's collection of
--   data types which could be looked up using types. See ('meta',
--   'metaMay').
newtype Metadata = Metadata { unMetadata :: Map TypeRep MetaEntry }

-- Helper data type which holds actual data
data MetaEntry where
  MetaEntry :: (IsMeta a) => a -> MetaEntry

-- | @<>@ is right biased.
instance Semigroup Metadata where
  (<>) = coerce (flip ((<>) @(Map TypeRep MetaEntry)))

instance Monoid Metadata where
  mempty = Metadata mempty

-- | Lens for looking up dictionary from metadata bundle
metaMay :: forall a. (IsMeta a) => Lens' Metadata (Maybe a)
{-# INLINE metaMay #-}
metaMay = lens getM putM
  where
    getM (Metadata m) = do
      MetaEntry a <- Map.lookup k m
      cast a
    --
    putM (Metadata m) Nothing  = Metadata $ Map.delete k m
    putM (Metadata m) (Just a) = Metadata $ Map.insert k (MetaEntry a) m
    --
    k = typeOf (undefined :: a)

-- | Lens for looking up dictionary from metadata bundle. Will fail if
--   dictionary is not part of bundle
meta :: forall a. IsMeta a => Lens' Metadata a
meta = metaMay . lens unpack (const Just)
  where
    unpack (Just a) = a
    unpack Nothing  = error $ "Metadata doesn't have data type: " ++ typeName @a



----------------------------------------------------------------
-- JSON encoding of metadata
----------------------------------------------------------------

-- | JSON encoding for data types which constitute metadata.  It's
--   basically same as @FromJSON/ToJSON@ pair but we define top level
--   type class to allow different encodings and to provide better
--   error messages.
class Typeable a => MetaEncoding a where
  parseMeta :: JSON.Value -> JSON.Parser a
  toMeta    :: a -> JSON.Value

-- | Type class for top level metadata values. For serialization
--   purposes they are organized as tree (nested JSON objects) with
--   some JSON objects as a leaf. Overlapping of keys is not permitted
--   and will cause error on either serialization or deserialization.
class Typeable a => IsMeta a where
  metaTree :: MetaTree a


-- | Encode metadata as JSON value
encodeMetadata :: forall a. IsMeta a => a -> JSON.Value
encodeMetadata = either error id . encodeMetadataEither

-- | Encode metadata as JSON value
encodeMetadataEither :: forall a. IsMeta a => a -> Either String JSON.Value
encodeMetadataEither a = case unMetaTree metaTree of
  Err err     -> Left  $ keyClashesMsg @a err
  OK Tree{..} -> Right $ reduce keyTree
  where
    reduce (Leaf Entry{..}) = entryEncoder a
    reduce (Branch m)       = JSON.Object $ reduce <$> m

-- | Decode metadata from JSON value
decodeMetadataEither :: forall a. IsMeta a => JSON.Value -> Either String a
decodeMetadataEither json =
  case unMetaTree $ metaTree @a of
    Err err -> Left $ keyClashesMsg @a err
    OK Tree{..} -> do
      m <- consumeKM keyTree
      decoder m
  where
    consumeKM :: Spine (Entry a) -> Either String Metadata
    consumeKM (Leaf Entry{..}) = do
      e@(MetaEntry a) <- entryParser json
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



instance (IsMeta a, IsMeta b) => IsMeta (a,b) where
  metaTree = metaTreeProduct metaTree metaTree

instance (IsMeta a, IsMeta b, IsMeta c) => IsMeta (a,b,c) where
  metaTree
    = metaTreeIso (iso (\((a,b),c) -> (a,b,c)) (\(a,b,c) -> ((a,b),c)))
    $ metaTree `metaTreeProduct` metaTree `metaTreeProduct` metaTree

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d) => IsMeta (a,b,c,d) where
  metaTree
    = metaTreeIso (iso (\((a,b),(c,d)) -> (a,b,c,d)) (\(a,b,c,d) -> ((a,b),(c,d))))
    $ metaTree `metaTreeProduct` metaTree


instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e) => IsMeta (a,b,c,d,e) where
  metaTree
    = metaTreeIso (iso (\((a,b),(c,d,e)) -> (a,b,c,d,e)) (\(a,b,c,d,e) -> ((a,b),(c,d,e))))
    $ metaTree `metaTreeProduct` metaTree

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f) => IsMeta (a,b,c,d,e,f) where
  metaTree
    = metaTreeIso (iso (\((a,b,c),(d,e,f)) -> (a,b,c,d,e,f)) (\(a,b,c,d,e,f) -> ((a,b,c),(d,e,f))))
    $ metaTree `metaTreeProduct` metaTree

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f,IsMeta g) => IsMeta (a,b,c,d,e,f,g) where
  metaTree
    = metaTreeIso (iso (\((a,b,c),(d,e,f,g)) -> (a,b,c,d,e,f,g)) (\(a,b,c,d,e,f,g) -> ((a,b,c),(d,e,f,g))))
    $ metaTree `metaTreeProduct` metaTree

instance (IsMeta a,IsMeta b,IsMeta c,IsMeta d,IsMeta e,IsMeta f,IsMeta g,IsMeta h) => IsMeta (a,b,c,d,e,f,g,h) where
  metaTree
    = metaTreeIso (iso (\((a,b,c,d),(e,f,g,h)) -> (a,b,c,d,e,f,g,h))
                       (\(a,b,c,d,e,f,g,h)     -> ((a,b,c,d),(e,f,g,h))))
    $ metaTree `metaTreeProduct` metaTree

      
----------------------------------------------------------------
-- Bidirectional parser
----------------------------------------------------------------

-- | Bidirectional parser for metadata. Only product types are
--   supported
newtype MetaTree a = MetaTree { unMetaTree :: Err [[Text]] (Tree a) }

-- Either with accumulating Applicative instance
data Err e a = Err e
             | OK  a
             deriving (Show,Functor)

instance Monoid e => Applicative (Err e) where
  pure = OK
  Err e1 <*> Err e2 = Err (e1 <> e2)
  Err e  <*> OK  _  = Err e
  OK  _  <*> Err e  = Err e
  OK  f  <*> OK  a  = OK (f a)

-- Decoder tree
data Tree a = Tree
  { decoder :: Metadata -> Either String a -- Decoder from set of records
  , keyTree :: Spine (Entry a)             -- Tree itself
  }

-- Spine of a key tree
data Spine a
  = Leaf   a
  | Branch (KM.KeyMap (Spine a))
  deriving (Functor)

-- Leaf corresponding to a single record in metadata tree
data Entry a = Entry
  { entryEncoder :: a -> JSON.Value
  , entryParser  :: JSON.Value -> Either String MetaEntry
  }

instance Contravariant Entry where
  contramap f Entry{..} = Entry
    { entryEncoder = entryEncoder . f
    , ..
    }


-- | Apply isomorphism to tree of metadata
metaTreeIso :: Iso' a b -> MetaTree a -> MetaTree b
metaTreeIso len (MetaTree tree) = MetaTree $ go <$> tree
  where
    go Tree{..} = Tree { decoder = (fmap . fmap) (view len) decoder
                       , keyTree = contramap (view (from len)) <$> keyTree
                       }

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
      case zipSpine (contramap fst) (contramap snd) (keyTree treeA) (keyTree treeB) of
        Err e    -> Err e
        OK  keys -> OK Tree
          { decoder = (liftA2 . liftA2) (,) (decoder treeA) (decoder treeB)
          , keyTree = keys
          }

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

-- | Newtype for deriving 'MetaEncoding' instances from 'FromJSON' and 'ToJSON' instances
newtype AsAeson a = AsAeson a

instance (Typeable a, JSON.ToJSON a, JSON.FromJSON a) => MetaEncoding (AsAeson a) where
  parseMeta = coerce (JSON.parseJSON @a)
  toMeta    = coerce (JSON.toJSON    @a)

-- | Encode value using 'Show' and 'Read' instances.
newtype AsReadShow a = AsReadShow a

instance (Show a, Read a, Typeable a) => MetaEncoding (AsReadShow a) where
  parseMeta o
    = JSON.prependFailure ("While parsing " ++ typeName @a ++ "\n")
    $ case o of
        String (T.unpack -> s) -> case reads s of
          [(a,"")] -> pure (AsReadShow a)
          _        -> fail $ "Unable to read string: " ++ show s
        _ -> fail $ "Expected string but got " ++ constrName o
  toMeta (AsReadShow a) = (String . T.pack . show) a

-- | Derive 'IsMeta' using generics. It will derive instance which
--   uses exhaustive parser and will work only for record types
newtype AsRecord a = AsRecord a

instance ( Generic a
         , GRecParse  (Rep a)
         , GRecToMeta (Rep a)
         , Typeable a
         ) => MetaEncoding (AsRecord a) where
  parseMeta = JSON.prependFailure ("While parsing " ++ typeName @a ++ "\n")
            . metaWithObject (runObjParser $ AsRecord . Generics.to <$> grecParse)
  toMeta (AsRecord a) = JSON.object $ grecToMeta $ Generics.from a

class GRecParse f where
  grecParse :: ObjParser (f p)

class GRecToMeta f where
  grecToMeta :: f p -> [(JSON.Key, JSON.Value)]

deriving newtype instance GRecParse f => GRecParse (M1 D i f)
deriving newtype instance GRecParse f => GRecParse (M1 C i f)
instance (GRecParse f, GRecParse g) => GRecParse (f :*: g) where
  grecParse = (:*:) <$> grecParse <*> grecParse
instance {-# OVERLAPPABLE #-} (MetaEncoding a, KnownSymbol fld
         ) => GRecParse (M1 S ('MetaSel ('Just fld) u s d) (K1 i a)) where
  grecParse = M1 . K1 <$> metaField (fieldName @fld)
instance {-# OVERLAPPING #-} (MetaEncoding a, KnownSymbol fld
         ) => GRecParse (M1 S ('MetaSel ('Just fld) u s d) (K1 i (Maybe a))) where
  grecParse = M1 . K1 <$> metaFieldM (fieldName @fld)

deriving newtype instance GRecToMeta f => GRecToMeta (M1 D i f)
deriving newtype instance GRecToMeta f => GRecToMeta (M1 C i f)
instance (GRecToMeta f, GRecToMeta g) => GRecToMeta (f :*: g) where
  grecToMeta (f :*: g) = grecToMeta f <> grecToMeta g
instance {-# OVERLAPPABLE #-} (MetaEncoding a, KnownSymbol fld
         ) => GRecToMeta (M1 S ('MetaSel ('Just fld) u s d) (K1 i a)) where
  grecToMeta (M1 (K1 a)) = [fieldName @fld .== a]
instance {-# OVERLAPPING #-} (MetaEncoding a, KnownSymbol fld
         ) => GRecToMeta (M1 S ('MetaSel ('Just fld) u s d) (K1 i (Maybe a))) where
  grecToMeta (M1 (K1 (Just a))) = [fieldName @fld .== a]
  grecToMeta (M1 (K1 Nothing )) = []

fieldName :: forall fld. (KnownSymbol fld) => Text
fieldName = T.pack $ symbolVal (Proxy @fld)


-- | Derive 'IsMeta' instance with given path
newtype AsMeta (path :: [Symbol]) a = AsMeta a

instance (Typeable path, MetaEncoding a, IsMeta a, MetaPath path) => IsMeta (AsMeta path a) where
  metaTree = MetaTree $ OK $ Tree
    { decoder = \m -> case m ^. metaMay @a of        
        Nothing -> Left $ "Missing type: " ++ typeName @a
        Just a  -> Right (AsMeta a)
    , keyTree
        = flip (foldr $ \nm -> Branch . KM.singleton (fromText nm)) path
        $ Leaf Entry
          { entryEncoder = \(AsMeta a) -> toMeta a
          , entryParser  = \json -> do
              let descend k parser
                    = JSON.prependFailure (" - " ++ T.unpack k ++ "\n")
                    . metaWithObject (\o -> parser =<< (o .: fromText k))
              let parser
                    = JSON.prependFailure ("While parsing metadata " ++ typeName @a ++ "\n")
                    . foldr descend (parseMeta @a) path
              a <- JSON.parseEither parser json
              pure $ MetaEntry a
          }
    }
    where
      path = getMetaPath @path

class MetaPath (xs :: [Symbol]) where
  getMetaPath :: [Text]

instance MetaPath '[] where
  getMetaPath = []

instance (KnownSymbol n, MetaPath ns) => MetaPath (n ': ns) where
  getMetaPath = fieldName @n : getMetaPath @ns

----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

deriving via AsAeson Value  instance MetaEncoding Value
deriving via AsAeson Text   instance MetaEncoding Text
deriving via AsAeson Bool   instance MetaEncoding Bool
deriving via AsAeson Float  instance MetaEncoding Float
deriving via AsAeson Double instance MetaEncoding Double
deriving via AsAeson Int8   instance MetaEncoding Int8
deriving via AsAeson Int16  instance MetaEncoding Int16
deriving via AsAeson Int32  instance MetaEncoding Int32
deriving via AsAeson Int64  instance MetaEncoding Int64
deriving via AsAeson Int    instance MetaEncoding Int
deriving via AsAeson Word8  instance MetaEncoding Word8
deriving via AsAeson Word16 instance MetaEncoding Word16
deriving via AsAeson Word32 instance MetaEncoding Word32
deriving via AsAeson Word64 instance MetaEncoding Word64
deriving via AsAeson Word   instance MetaEncoding Word

instance (MetaEncoding a, MetaEncoding b) => MetaEncoding (a,b) where
  parseMeta = JSON.withArray "(a, b)" $ \arr -> case V.length arr of
    2 -> (,) <$> parseMeta (V.unsafeIndex arr 0)
             <*> parseMeta (V.unsafeIndex arr 1)
    n -> fail $ "Expecting 2-element array, got " ++ show n
  toMeta (a,b) = Array $ V.fromList [toMeta a, toMeta b]

instance (MetaEncoding a, MetaEncoding b, MetaEncoding c) => MetaEncoding (a,b,c) where
  parseMeta = JSON.withArray "(a, b, c)" $ \arr -> case V.length arr of
    3 -> (,,) <$> parseMeta (V.unsafeIndex arr 0)
              <*> parseMeta (V.unsafeIndex arr 1)
              <*> parseMeta (V.unsafeIndex arr 2)
    n -> fail $ "Expecting 3-element array, got " ++ show n
  toMeta (a,b,c) = Array $ V.fromList [toMeta a, toMeta b, toMeta c]

instance (MetaEncoding a) => MetaEncoding (Maybe a) where
  parseMeta Null  = pure Nothing
  parseMeta o     = Just <$> parseMeta o
  toMeta Nothing  = Null
  toMeta (Just x) = toMeta x

instance (Typeable k, JSON.FromJSONKey k, JSON.ToJSONKey k, Ord k, MetaEncoding a
         ) => MetaEncoding (Map.Map k a) where
  parseMeta
    = JSON.prependFailure "Parsing map\n"
    . case JSON.fromJSONKey @k of
        JSON.FromJSONKeyCoerce -> JSON.withObject "Map"
          $ fmap (Map.mapKeys (coerce . JSON.toText))
          . Map.traverseWithKey (\k -> errK k . parseMeta)
          . KM.toMap
        JSON.FromJSONKeyText f -> JSON.withObject "Map"
          $ fmap (Map.mapKeys (f . JSON.toText))
          . Map.traverseWithKey (\k -> errK k . parseMeta)
          . KM.toMap
        JSON.FromJSONKeyTextParser parser -> JSON.withObject "Map"
          $ fmap Map.fromList
          . traverse (\(k,v) -> errK k $ (,) <$> parser (JSON.toText k) <*> parseMeta v)
          . Map.toList
          . KM.toMap
        JSON.FromJSONKeyValue parser -> JSON.withArray "Map"
          $ fmap Map.fromList
          . traverse (\o -> do (k,v) <- parseMeta o
                               (,) <$> parser k <*> parseMeta v)
          . V.toList
        where
          errK k = JSON.prependFailure (" - key: "<>JSON.toString k<>"\n")
  toMeta m = case JSON.toJSONKey of
    JSON.ToJSONKeyText  f _ -> JSON.object [ JSON.toText (f k) .== v | (k,v) <- Map.toList m ]
    JSON.ToJSONKeyValue f _ -> Array $ V.fromList
      [ Array $ V.fromList [f k, coerce $ toMeta v] | (k,v) <- Map.toList m ]


instance (MetaEncoding a) => MetaEncoding [a] where
  parseMeta = fmap V.toList . parseMeta
  toMeta    = Array . V.fromList . map toMeta

instance (MetaEncoding a, VU.Unbox a) => MetaEncoding (VU.Vector a) where
  parseMeta = fmap VU.convert . parseMeta @(V.Vector a)
  toMeta    = Array . V.map toMeta . V.convert

instance (MetaEncoding a, VS.Storable a) => MetaEncoding (VS.Vector a) where
  parseMeta = fmap VS.convert . parseMeta @(V.Vector a)
  toMeta    = Array . V.map toMeta . V.convert

instance (MetaEncoding a) => MetaEncoding (V.Vector a) where
  parseMeta = JSON.prependFailure " - traversing Array\n"
            . JSON.withArray "Vector" (traverse parseMeta)
  toMeta    = Array . V.map toMeta

instance (MetaEncoding a, F.Arity n) => MetaEncoding (FB.Vec n a) where
  parseMeta = parseFixedVec
  toMeta    = fixedVecToMeta
instance (MetaEncoding a, FU.Unbox n a) => MetaEncoding (FU.Vec n a) where
  parseMeta = parseFixedVec
  toMeta    = fixedVecToMeta
instance (MetaEncoding a, VS.Storable a, F.Arity n) => MetaEncoding (FS.Vec n a) where
  parseMeta = parseFixedVec
  toMeta    = fixedVecToMeta

parseFixedVec :: forall a v. (MetaEncoding a, F.Vector v a) => JParser (v a)
parseFixedVec
  = JSON.prependFailure " - traversing Array\n"
  . JSON.withArray "Vec"
    (\v -> do
        let n   = V.length v
            dim = F.arity (Proxy @(F.Dim v))
        when (n /= dim) $ fail $ printf "Array length mismatch: expected %i but got %i" dim n
        F.generateM $ \i -> JSON.prependFailure (" - index " <> show i)
                          $ parseMeta (v ! i)
    )

fixedVecToMeta :: (MetaEncoding a, F.Vector v a) => v a -> JSON.Value
fixedVecToMeta = Array . V.fromList . map toMeta . F.toList

instance MetaEncoding BinD where
  parseMeta o =  metaSExp3 "BinD"     binD     o
             <|> metaSExp3 "BinDstep" binDstep o
  toMeta b = toMeta
    [ "BinDstep"
    , toMeta $ lowerLimit b
    , toMeta $ binSize    b
    , toMeta $ nBins      b
    ]


----------------------------------------------------------------
-- Parsers
----------------------------------------------------------------

-- | Parse sinlge field of metadata
(.::) :: MetaEncoding a => JSON.Object -> JSON.Key -> JSON.Parser a
o .:: k = parseMeta =<< (o .: k)

(.==) :: MetaEncoding a => Text -> a -> (JSON.Key, JSON.Value)
k .== v = (fromText k, toMeta v)

metaSExp1 :: (Typeable r, MetaEncoding a)
          => Text -> (a -> r) -> JParser r
metaSExp1 con f = metaSExp1With con f parseMeta

metaSExp2 :: (Typeable r, MetaEncoding a, MetaEncoding b)
          => Text -> (a -> b -> r) -> JParser r
metaSExp2 con f = metaSExp2With con f parseMeta parseMeta

metaSExp3 :: (Typeable r, MetaEncoding a, MetaEncoding b, MetaEncoding c)
          => Text -> (a -> b -> c -> r) -> JParser r
metaSExp3 con f = metaSExp3With con f parseMeta parseMeta parseMeta

metaSExp1With
  :: forall a r. Typeable r
  => Text -> (a -> r)
  -> JParser a -> JParser r
metaSExp1With con mk pA val
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ typeName @r ++ "\n")
  $ case val of
      Array arr -> case V.toList arr of
        [String c, vA]
          | c == con -> mk <$> pA vA
        _            -> fail "Cannot parse sexp"
      o -> fail $ "Expected array but got " ++ constrName o

metaSExp2With
  :: forall a b r. Typeable r
  => Text -> (a -> b -> r)
  -> JParser a -> JParser b -> JParser r
metaSExp2With con mk pA pB val
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ typeName @r ++ "\n")
  $ case val of
      Array arr -> case V.toList arr of
        [String c, vA, vB]
          | c == con -> mk <$> pA vA <*> pB vB
        _            -> fail "Cannot parse sexp"
      o -> fail $ "Expected array but got " ++ constrName o

metaSExp3With
  :: forall a b c r. (Typeable r)
  => Text -> (a -> b -> c -> r)
  -> JParser a -> JParser b -> JParser c -> JParser r
metaSExp3With con mk pA pB pC val
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ typeName @r ++ "\n")
  $ case val of
      Array arr -> case V.toList arr of
        [String c, vA, vB, vC]
          | c == con -> mk <$> pA vA <*> pB vB <*> pC vC
        _            -> fail "Cannot parse sexp"
      o -> fail $ "Expected array but got " ++ constrName o


----------------------------------------------------------------
-- Exhaustive parser
----------------------------------------------------------------

-- | Parser for exhaustive matching of an object. Each key could only
--   be parsed once.
newtype ObjParser a = ObjParser (StateT (KM.KeyMap JSON.Value) JSON.Parser a)
  deriving newtype (Functor, Applicative, Alternative, Monad, MonadFail)

-- | Run object parser on given object
runObjParser :: ObjParser a -> JSON.Object -> JSON.Parser a
runObjParser (ObjParser m) o = do
  (a, o') <- runStateT m o
  unless (null o') $ fail $ unlines
    $ "Unknown keys:" : [ " - " ++ T.unpack (toText k) | k <- KM.keys o']
  pure a

-- | Lookup mandatory field in the object
metaField :: MetaEncoding a => Text -> ObjParser a
metaField k = ObjParser $ do
  (v, o) <- popFromMap k =<< get
  put o
  lift $ JSON.prependFailure (" - key: " ++ T.unpack k ++ "\n")
       $ parseMeta v

-- | Lookup optional field in the object
metaFieldM :: MetaEncoding a => Text -> ObjParser (Maybe a)
metaFieldM k = ObjParser $ do
  (v, o) <- popFromMapM k =<< get
  put o
  lift $ JSON.prependFailure (" - key: " ++ T.unpack k ++ "\n")
       $ case v of
           Nothing -> pure Nothing
           Just x  -> parseMeta x

popFromMap :: MonadFail m => Text -> JSON.Object -> m (JSON.Value, JSON.Object)
popFromMap k o = getCompose $ KM.alterF go (fromText k) o
  where
    go Nothing  = Compose $ fail $ "No such key: " ++ T.unpack k
    go (Just v) = Compose $ pure (v, Nothing)


popFromMapM :: MonadFail m => Text -> JSON.Object -> m (Maybe JSON.Value, JSON.Object)
popFromMapM k o = getCompose $ KM.alterF go (fromText k) o
  where
    go Nothing  = Compose $ pure (Nothing, Nothing)
    go (Just v) = Compose $ pure (Just v,  Nothing)

-- | Convert exhaustive object parser to standard JSON parser
metaWithRecord
  :: forall a. Typeable a
  => ObjParser a -> JParser a
metaWithRecord parser
  = JSON.prependFailure ("While parsing " ++ typeName @a ++ "\n")
  . metaWithObject (runObjParser parser)

-- | Helper for parsing JSON objects
metaWithObject
  :: forall a. ()
  => (JSON.Object -> JSON.Parser a)
  -> (JSON.Value  -> JSON.Parser a)
metaWithObject parser = \case
  Object o -> parser o
  o        -> fail $ "Expected object but got " ++ constrName o


----------------------------------------------------------------
-- Helpers
----------------------------------------------------------------

type JParser a = JSON.Value -> JSON.Parser a

constrName :: JSON.Value -> String
constrName = \case
  Object{} -> "object"
  Array{}  -> "array"
  Number{} -> "number"
  String{} -> "string"
  Bool{}   -> "boolean"
  Null     -> "null"

typeName :: forall a. Typeable a => String
typeName = show (typeOf (undefined :: a))


keyClashesMsg :: forall a. Typeable a => [[Text]] -> String
keyClashesMsg err
  = unlines
  $ ("Key clashes for data type " ++ typeName @a)
  : [ "  - " ++ T.unpack (T.intercalate "." e)
    | e <- err
    ]
