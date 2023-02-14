{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE ImportQualifiedPost        #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiWayIf                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE PatternSynonyms            #-}
{-# LANGUAGE PolyKinds                  #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE ViewPatterns               #-}
-- |
module OKA.Metadata
  ( -- * Metadata
    Metadata(..)
  , IsMeta(..)
  , readMetadata
  , fromMeta
  , lookupMeta
  , lookupMetaDef
    -- * Writing instances
  , (.::)
  , metaWithObject
    -- ** Deriving via
  , AsAeson(..)
  , AsSubdict(..)
  , MProd(..)
    -- ** Special purpose parsers
  , metaSExp1
  , metaSExp1With
  , metaSExp2
  , metaSExp2With
  , metaSExp3
  , metaSExp3With
  , metaObjectAt
  , metaObject
    -- ** Exhaustive parser
  , ObjParser
  , runObjParser
  , metaField
  , metaFieldM
    -- ** Construction of metadata
  , mkObject
  , (.==)
  ) where

import Control.Applicative
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class

import Data.Aeson             (Value(..),(.:))
import Data.Aeson             qualified as JSON
import Data.Aeson.Key         qualified as JSON
import Data.Aeson.KeyMap      qualified as KM
import Data.Aeson.Key         (fromText,toText)
import Data.Aeson.Types       qualified as JSON
import Data.Histogram.Bin
import Data.Yaml              qualified as Yaml
import Data.Int
import Data.Functor.Compose
import Data.Typeable
import Data.Word
import Data.Coerce
import Data.Text                          (Text)
import Data.Text              qualified as T
import Data.Vector            qualified as V
import Data.Vector.Unboxed    qualified as VU
import GHC.TypeLits
import GHC.Generics


----------------------------------------------------------------
--
----------------------------------------------------------------

-- | Metadata for analysis. It contains cut parameters, constants,
--   etc. Here we represent it as values that could be stored as
--   JSON(YAML)
newtype Metadata = Metadata JSON.Value
  deriving stock   (Show, Eq)
  deriving newtype (JSON.FromJSON, JSON.ToJSON)

instance Semigroup Metadata where
  m1 <> m2 = case mergeMetadata m1 m2 of
    Left  e -> throw e
    Right m -> m

instance Monoid Metadata where
  mempty = Metadata $ Object mempty

----------------------------------------------------------------
-- Merging of metadata
----------------------------------------------------------------

-- | Error during merge of metadata
data MergeError = MergeError [MetaKey] JSON.Value JSON.Value
  deriving stock    (Show)
  deriving anyclass (Exception)

-- | Path in the metadata tree
data MetaKey = KeyTxt !Text
             | KeyIdx !Int
             deriving stock (Show)

-- | Add layer of metadata to existing set.
mergeMetadata
  :: Metadata  -- ^ Old metadata
  -> Metadata  -- ^ New layer
  -> Either MergeError Metadata
mergeMetadata (Metadata m1) (Metadata m2) = Metadata <$> go [] m1 m2
  where
    -- We merge dictionaries and merge matching keys simultaneously
    go path (JSON.Object o1) (JSON.Object o2)
      = fmap JSON.Object
      $ sequence
      $ KM.unionWithKey (\k ma mb -> do a <- ma
                                        b <- mb
                                        go (KeyTxt (toText k) : path) a b
                        ) (Right <$> o1) (Right <$> o2)
    -- FIXME: Is semantics for arrays reasonable???
    go path (JSON.Array v1)  (JSON.Array v2)
      = fmap JSON.Array
      $ sequence
      $ V.izipWith (\i -> go (KeyIdx i:path)) v1 v2
    -- Numbers/strings/bools are replaced
    go _    JSON.Number{} x@JSON.Number{} = Right x
    go _    JSON.String{} x@JSON.String{} = Right x
    go _    JSON.Bool{}   x@JSON.Bool{}   = Right x
    -- NULL replaces everything
    go _    _             JSON.Null       = Right JSON.Null
    go _    JSON.Null     x               = Right x
    -- Error case
    go path a b = Left $ MergeError path a b


----------------------------------------------------------------
-- Reading from metadata
----------------------------------------------------------------

-- | Type class for decoding values from metadata, it's very similar
--   to 'FromJSON' type class but here we define another one n order
--   to be able to define conflicting instances
class IsMeta a where
  parseMeta :: JSON.Value -> JSON.Parser a
  toMeta    :: a -> Metadata

-- | Read metadata from file
readMetadata :: MonadIO m => FilePath -> m Metadata
readMetadata path = liftIO $ do
  Yaml.decodeFileEither path >>= \case
    Left  e -> throwIO e
    Right a -> pure (Metadata a)

-- | Convert value from metadata. Throws error if conversion fails
fromMeta :: IsMeta a => Metadata -> a
fromMeta (Metadata m) = case JSON.parse parseMeta m of
  JSON.Success a -> a
  JSON.Error   e -> error $ "IsMeta: cannot convert:\n" ++ e

-- | Lookup value from metadata with default
lookupMetaDef :: IsMeta a => Metadata -> [Text] -> a -> a
lookupMetaDef (Metadata meta) path a0 = go meta path []
  where
    go m          []     _  = fromMeta (Metadata m)
    go (Object o) (p:ps) up = case KM.lookup (fromText p) o of
      Just m  -> go m ps (p:up)
      Nothing -> a0
    go _ _ up = error $ unlines
      $ "Failed to lookup following keys. Encountered non-object:"
      : [ " - " <> T.unpack k | k <- reverse up ]

-- | Lookup value from metadata with default
lookupMeta :: IsMeta a => Metadata -> [Text] -> a
lookupMeta (Metadata meta) path = go meta path []
  where
    go m          []     _  = fromMeta (Metadata m)
    go (Object o) (p:ps) up = case KM.lookup (fromText p) o of
      Just m  -> go m ps (p:up)
      Nothing -> error $ unlines
        $ "Failed to lookup following keys:"
        : [ " - " <> T.unpack k | k <- p : reverse up ]
    go _ _ up = error $ unlines
      $ "Failed to lookup following keys. Encountered non-object:"
      : [ " - " <> T.unpack k | k <- reverse up ]

----------------------------------------------------------------
-- Parsers
----------------------------------------------------------------

-- | Parse sinlge field of metadata
(.::) :: IsMeta a => JSON.Object -> JSON.Key -> JSON.Parser a
o .:: k = JSON.prependFailure (" - key: " ++ T.unpack (JSON.toText k) ++ "\n")
        $ parseMeta =<< (o .: k)

-- | Newtype for deriving IsMeta instances from FromJSON instances
newtype AsAeson a = AsAeson a

instance (JSON.ToJSON a, JSON.FromJSON a) => IsMeta (AsAeson a) where
  parseMeta = coerce (JSON.parseJSON @a)
  toMeta    = coerce (JSON.toJSON    @a)

type JParser a = JSON.Value -> JSON.Parser a


-- | Direct product of several metadata entries. It's intended for use
--   with single product type where fields are represented as
--   dictionaries with nonintersecting keys.
newtype MProd a = MProd a
  deriving newtype (Show,Eq,Ord,Generic)

instance (Generic a, GMetaProd (Rep a)) => IsMeta (MProd a) where
  parseMeta = fmap to . gparseProd @(Rep a)
  toMeta    = gtoMeta . from

class GMetaProd f where
  gparseProd :: JSON.Value -> JSON.Parser (f p)
  gtoMeta    :: f p -> Metadata

deriving newtype instance GMetaProd f => GMetaProd (M1 i c f)
instance GMetaProd U1 where
  gparseProd _ = pure U1
  gtoMeta    _ = mempty
instance (GMetaProd f, GMetaProd g) => GMetaProd (f :*: g) where
  gparseProd o = (:*:) <$> gparseProd o <*> gparseProd o
  gtoMeta (f :*: g) = gtoMeta f <> gtoMeta g
instance (IsMeta a) => GMetaProd (K1 i a) where
  gparseProd = coerce (parseMeta @a)
  gtoMeta    = coerce (toMeta    @a)


-- | Encode value using 'IsMeta' instance for @a@ placed inside
--   dictionary under key @k@.
newtype AsSubdict (key :: Symbol) a = AsSubdict a

instance (KnownSymbol key, IsMeta a, Typeable a) => IsMeta (AsSubdict key a) where
  parseMeta
    = JSON.prependFailure ("While parsing " ++ show (typeOf (undefined :: a)) ++ "\n")
    . metaWithObject (\o -> AsSubdict <$> (parseMeta =<< (o .:: k)))
    where k = JSON.fromText $ T.pack $ symbolVal (Proxy @key)
  toMeta (AsSubdict a) = mkObject [ k .== a ]
    where k = T.pack $ symbolVal (Proxy @key)



metaSExp1 :: (Typeable r, IsMeta a)
          => Text -> (a -> r) -> JParser r
metaSExp1 con f = metaSExp1With con f parseMeta

metaSExp2 :: (Typeable r, IsMeta a, IsMeta b)
          => Text -> (a -> b -> r) -> JParser r
metaSExp2 con f = metaSExp2With con f parseMeta parseMeta

metaSExp3 :: (Typeable r, IsMeta a, IsMeta b, IsMeta c)
          => Text -> (a -> b -> c -> r) -> JParser r
metaSExp3 con f = metaSExp3With con f parseMeta parseMeta parseMeta

metaSExp1With
  :: forall a r. Typeable r
  => Text -> (a -> r)
  -> JParser a -> JParser r
metaSExp1With con mk pA val
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ (show (typeOf (undefined :: r))) ++ "\n")
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
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ (show (typeOf (undefined :: r))) ++ "\n")
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
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ (show (typeOf (undefined :: r))) ++ "\n")
  $ case val of
      Array arr -> case V.toList arr of
        [String c, vA, vB, vC]
          | c == con -> mk <$> pA vA <*> pB vB <*> pC vC
        _            -> fail "Cannot parse sexp"
      o -> fail $ "Expected array but got " ++ constrName o

-- | Parse object at given position inside JSON tree.
metaObjectAt
  :: forall a. Typeable a
  => [Text]      -- ^ Path to entry
  -> ObjParser a -- ^ Parser for an object
  -> JParser a
metaObjectAt path parser v0
  = JSON.prependFailure ("While parsing " ++ show (typeOf (undefined :: a)) ++ "\n")
  $ go path v0
  where
    go []     (Object o) = runObjParser parser o
    go (k:ks) (Object o) = JSON.prependFailure (" - key: " ++ T.unpack k ++ "\n")
                         $ go ks =<< (o .: fromText k)
    go _      o          = fail $ "Expected object but got " ++ constrName o

-- | Convert exhaustive object parser to standard JSON parser
metaObject
  :: forall a. Typeable a
  => ObjParser a -> JParser a
metaObject parser
  = JSON.prependFailure ("While parsing " ++ show (typeOf (undefined :: a)) ++ "\n")
  . metaWithObject (runObjParser parser)

metaWithObject
  :: forall a. Typeable a
  => (JSON.Object -> JSON.Parser a)
  -> (JSON.Value  -> JSON.Parser a)
metaWithObject parser = \case
  Object o -> parser o
  o        -> fail $ "Expected object but got " ++ constrName o

constrName :: JSON.Value -> String
constrName = \case
  Object{} -> "object"
  Array{}  -> "array"
  Number{} -> "number"
  String{} -> "string"
  Bool{}   -> "boolean"
  Null     -> "null"

----------------------------------------------------------------
-- Exhaustive parser
----------------------------------------------------------------

-- | Parser for exhaustive matching of an object. Each key could only
--   be parsed once.
newtype ObjParser a = ObjParser (StateT (KM.KeyMap JSON.Value) JSON.Parser a)
  deriving newtype (Functor, Applicative, Monad, MonadFail)

-- | Run object parser on given object
runObjParser :: ObjParser a -> JSON.Object -> JSON.Parser a
runObjParser (ObjParser m) o = do
  (a, o') <- runStateT m o
  unless (null o') $ fail $ unlines
    $ "Unknown keys:" : [ " - " ++ T.unpack (toText k) | k <- KM.keys o']
  pure a

-- | Lookup mandatory field in the object
metaField :: IsMeta a => Text -> ObjParser a
metaField k = ObjParser $ do
  (v, o) <- popFromMap k =<< get
  put o
  lift $ JSON.prependFailure (" - key: " ++ T.unpack k ++ "\n")
       $ parseMeta v

-- | Lookup optional field in the object
metaFieldM :: IsMeta a => Text -> ObjParser (Maybe a)
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


mkObject :: [(JSON.Key,Metadata)] -> Metadata
mkObject = coerce JSON.object

(.==) :: IsMeta a => Text -> a -> (JSON.Key,Metadata)
k .== v = (fromText k, toMeta v)

----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance IsMeta Metadata where
  parseMeta = pure . Metadata
  toMeta    = id

deriving via AsAeson Value  instance IsMeta Value
deriving via AsAeson Bool   instance IsMeta Bool
deriving via AsAeson Float  instance IsMeta Float
deriving via AsAeson Double instance IsMeta Double
deriving via AsAeson Int8   instance IsMeta Int8
deriving via AsAeson Int16  instance IsMeta Int16
deriving via AsAeson Int32  instance IsMeta Int32
deriving via AsAeson Int64  instance IsMeta Int64
deriving via AsAeson Int    instance IsMeta Int
deriving via AsAeson Word8  instance IsMeta Word8
deriving via AsAeson Word16 instance IsMeta Word16
deriving via AsAeson Word32 instance IsMeta Word32
deriving via AsAeson Word64 instance IsMeta Word64
deriving via AsAeson Word   instance IsMeta Word

instance (IsMeta a, IsMeta b) => IsMeta (a,b) where
  parseMeta = JSON.withArray "(a, b)" $ \arr -> case V.length arr of
    2 -> (,) <$> parseMeta (V.unsafeIndex arr 0)
             <*> parseMeta (V.unsafeIndex arr 1)
    n -> fail $ "Expecting 2-element array, got " ++ show n
  toMeta (a,b) = Metadata $ Array $ V.fromList [toMetaValue a, toMetaValue b]

instance (IsMeta a, IsMeta b, IsMeta c) => IsMeta (a,b,c) where
  parseMeta = JSON.withArray "(a, b, c)" $ \arr -> case V.length arr of
    3 -> (,,) <$> parseMeta (V.unsafeIndex arr 0)
              <*> parseMeta (V.unsafeIndex arr 1)
              <*> parseMeta (V.unsafeIndex arr 2)
    n -> fail $ "Expecting 3-element array, got " ++ show n
  toMeta (a,b,c) = Metadata $ Array $ V.fromList [toMetaValue a, toMetaValue b, toMetaValue c]

instance (IsMeta a) => IsMeta (Maybe a) where
  parseMeta Null  = pure Nothing
  parseMeta o     = Just <$> parseMeta o
  toMeta Nothing  = Metadata Null
  toMeta (Just x) = toMeta x


instance (IsMeta a) => IsMeta [a] where
  parseMeta = fmap V.toList . parseMeta
  toMeta    = Metadata . Array . V.fromList . map toMetaValue
instance (IsMeta a, VU.Unbox a) => IsMeta (VU.Vector a) where
  parseMeta = fmap VU.convert . parseMeta @(V.Vector a)
  toMeta = Metadata . Array . V.map toMetaValue . V.convert
instance (IsMeta a) => IsMeta (V.Vector a) where
  parseMeta = JSON.prependFailure " - traversing Array\n"
            . JSON.withArray "Vector" (traverse parseMeta)
  toMeta = Metadata . Array . V.map toMetaValue

instance IsMeta BinD where
  parseMeta o =  metaSExp3 "BinD"     binD     o
             <|> metaSExp3 "BinDstep" binDstep o
  toMeta b = toMeta
    [ Metadata "BinDstep"
    , toMeta $ lowerLimit b
    , toMeta $ binSize    b
    , toMeta $ nBins      b
    ]


toMetaValue :: forall a. IsMeta a => a -> Value
toMetaValue = coerce (toMeta @a)
