{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE UndecidableInstances #-}
-- |
-- Tools for defining how haskell values are encoded\/decode
-- into\/from JSON.
module OKA.Metadata.Encoding
  ( -- * Type class
    MetaEncoding(..)
    -- ** Manual writing of instances
  , (.==)
  , (.::)
  , (.::?)
  , metaWithObject
    -- *** S-expressions
  , metaSExp1
  , metaSExp1With
  , metaSExp2
  , metaSExp2With
  , metaSExp3
  , metaSExp3With
    -- *** Fixed-vectors
  , parseFixedVec
  , fixedVecToMeta
    -- ** Exhaustive parser
  , ObjParser
  , runObjParser
  , metaObject
  , metaField
  , metaFieldM
  , metaWithRecord
    -- ** Helpers
  , liftAttoparsec
    -- ** Deriving via
  , AsAeson(..)
  , AsReadShow(..)
  , AsRecord(..)
  ) where

import Control.Applicative
import Control.Monad
import Control.Monad.Trans.State.Strict
import Control.Monad.Trans.Class
import Data.Aeson                  (Value(..),(.:),(.:?))
import Data.Aeson                  qualified as JSON
import Data.Aeson.Key              qualified as JSON
import Data.Aeson.KeyMap           qualified as KM
import Data.Aeson.Types            qualified as JSON
import Data.Attoparsec.Text        qualified as Atto
import Data.Coerce
import Data.Functor.Compose
import Data.Int
import Data.Text                   (Text)
import Data.Text                   qualified as T
import Data.Vector                 qualified as V
import Data.Vector.Unboxed         qualified as VU
import Data.Vector.Storable        qualified as VS
import Data.Vector.Generic         ((!))
import Data.Vector.Fixed           qualified as F
import Data.Vector.Fixed.Cont      qualified as FC
import Data.Vector.Fixed.Unboxed   qualified as FU
import Data.Vector.Fixed.Strict    qualified as FF
import Data.Vector.Fixed.Boxed     qualified as FB
import Data.Vector.Fixed.Storable  qualified as FS
import Data.Vector.Fixed.Primitive qualified as FP
import Data.Map.Strict             qualified as Map
import Data.IntMap.Strict          qualified as IntMap
import Data.Typeable
import Data.Word
import Text.Printf

import GHC.Exts                   (proxy#)
import GHC.Generics               hiding (from,to)
import GHC.Generics               qualified as Generics
import GHC.TypeLits               (KnownSymbol,KnownNat)
import OKA.Metadata.Util

----------------------------------------------------------------
-- Metadata encoding
----------------------------------------------------------------

-- | JSON encoding for data types which constitute metadata.  It's
--   basically same as @FromJSON/ToJSON@ pair but we define top level
--   type class to allow different encodings and to provide better
--   error messages.
class Typeable a => MetaEncoding a where
  parseMeta  :: JSON.Value -> JSON.Parser a
  metaToJson :: a -> JSON.Value


----------------------------------------------------------------
-- Manual instances writing
----------------------------------------------------------------

-- | Analog of '.=' from @aeson@
(.==) :: MetaEncoding a => Text -> a -> (JSON.Key, JSON.Value)
k .== v = (JSON.fromText k, metaToJson v)

-- | Parse sinlge field of metadata
(.::) :: MetaEncoding a => JSON.Object -> JSON.Key -> JSON.Parser a
o .:: k = parseMeta =<< (o .: k)

-- | Parse sinlge field of metadata. Returns @Nothing@ if key is not
--   present or null.
(.::?) :: MetaEncoding a => JSON.Object -> JSON.Key -> JSON.Parser (Maybe a)
o .::? k = traverse parseMeta =<< (o .:? k)

-- | Helper for parsing JSON objects
metaWithObject
  :: forall a. ()
  => (JSON.Object -> JSON.Parser a)
  -> (JSON.Value  -> JSON.Parser a)
metaWithObject parser = \case
  Object o -> parser o
  o        -> fail $ "Expected object but got " ++ constrName o



metaSExp1 :: (Typeable r, MetaEncoding a)
          => Text -> (a -> r) -> Value -> JSON.Parser r
metaSExp1 con f = metaSExp1With con f parseMeta

metaSExp2 :: (Typeable r, MetaEncoding a, MetaEncoding b)
          => Text -> (a -> b -> r) -> Value -> JSON.Parser r
metaSExp2 con f = metaSExp2With con f parseMeta parseMeta

metaSExp3 :: (Typeable r, MetaEncoding a, MetaEncoding b, MetaEncoding c)
          => Text -> (a -> b -> c -> r) -> Value -> JSON.Parser r
metaSExp3 con f = metaSExp3With con f parseMeta parseMeta parseMeta

metaSExp1With
  :: forall a r. Typeable r
  => Text -> (a -> r)
  -> (Value -> JSON.Parser a)
  -> (Value -> JSON.Parser r)
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
  -> (Value -> JSON.Parser a)
  -> (Value -> JSON.Parser b)
  -> (Value -> JSON.Parser r)
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
  -> (Value -> JSON.Parser a)
  -> (Value -> JSON.Parser b)
  -> (Value -> JSON.Parser c)
  -> (Value -> JSON.Parser r)
metaSExp3With con mk pA pB pC val
  = JSON.prependFailure (" - sexp " ++ T.unpack con ++ " for " ++ typeName @r ++ "\n")
  $ case val of
      Array arr -> case V.toList arr of
        [String c, vA, vB, vC]
          | c == con -> mk <$> pA vA <*> pB vB <*> pC vC
        _            -> fail "Cannot parse sexp"
      o -> fail $ "Expected array but got " ++ constrName o



-- | Parser for fixed vectors
parseFixedVec :: forall a v. (MetaEncoding a, F.Vector v a) => Value -> JSON.Parser (v a)
parseFixedVec
  = JSON.prependFailure " - traversing Array\n"
  . JSON.withArray "Vec"
    (\v -> do
        let n   = V.length v
            dim = FC.peanoToInt (proxy# @(F.Dim v))
        when (n /= dim) $ fail $ printf "Array length mismatch: expected %i but got %i" dim n
        F.generateM $ \i -> JSON.prependFailure (" - index " <> show i)
                          $ parseMeta (v ! i)
    )

-- | Encoder for fixed vectors
fixedVecToMeta :: (MetaEncoding a, F.Vector v a) => v a -> Value
fixedVecToMeta = Array . V.fromList . map metaToJson . F.toList

instance (MetaEncoding a, Typeable v, F.Vector v a) => MetaEncoding (F.ViaFixed v a) where
  parseMeta  = parseFixedVec
  metaToJson = fixedVecToMeta
  

----------------------------------------------------------------
-- Exhaustive parser
----------------------------------------------------------------

-- | Parser for exhaustive matching of an object. It requires that
--   each key from object was used exactly once. One reason for this
--   is to protect against typos in optional keys.
newtype ObjParser a = ObjParser (StateT (KM.KeyMap JSON.Value) JSON.Parser a)
  deriving newtype (Functor, Applicative, Alternative, Monad, MonadFail)

-- | Run object parser on given object
runObjParser :: ObjParser a -> JSON.Object -> JSON.Parser a
runObjParser (ObjParser m) o = do
  (a, o') <- runStateT m o
  unless (null o') $ fail $ unlines
    $ "Unknown keys:" : [ " - " ++ T.unpack (JSON.toText k) | k <- KM.keys o']
  pure a

-- | Convert exhaustive object parser to standard JSON parser
metaObject
  :: forall a. Typeable a
  => ObjParser a -> Value -> JSON.Parser a
metaObject parser
  = JSON.prependFailure ("While parsing " ++ show (typeOf (undefined :: a)) ++ "\n")
  . metaWithObject (runObjParser parser)

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
popFromMap k o = getCompose $ KM.alterF go (JSON.fromText k) o
  where
    go Nothing  = Compose $ fail $ "No such key: " ++ T.unpack k
    go (Just v) = Compose $ pure (v, Nothing)


popFromMapM :: MonadFail m => Text -> JSON.Object -> m (Maybe JSON.Value, JSON.Object)
popFromMapM k o = getCompose $ KM.alterF go (JSON.fromText k) o
  where
    go Nothing  = Compose $ pure (Nothing, Nothing)
    go (Just v) = Compose $ pure (Just v,  Nothing)

-- | Convert exhaustive object parser to standard JSON parser
metaWithRecord
  :: forall a. Typeable a
  => ObjParser a -> Value -> JSON.Parser a
metaWithRecord parser
  = JSON.prependFailure ("While parsing " ++ typeName @a ++ "\n")
  . metaWithObject (runObjParser parser)


----------------------------------------------------------------
-- Deriving via
----------------------------------------------------------------

-- | Newtype for deriving 'MetaEncoding' instances from 'FromJSON' and 'ToJSON' instances
newtype AsAeson a = AsAeson a

instance (Typeable a, JSON.ToJSON a, JSON.FromJSON a) => MetaEncoding (AsAeson a) where
  parseMeta  = coerce (JSON.parseJSON @a)
  metaToJson = coerce (JSON.toJSON    @a)


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
  metaToJson (AsReadShow a) = (String . T.pack . show) a

-- | Derive 'MetaEncoding' using generics. It will derive instance
--   which uses exhaustive parser and will work only for record types
newtype AsRecord a = AsRecord a

instance ( Generic a
         , GRecParse  (Rep a)
         , GRecToMeta (Rep a)
         , Typeable a
         ) => MetaEncoding (AsRecord a) where
  parseMeta = JSON.prependFailure ("While parsing " ++ typeName @a ++ "\n")
            . metaWithObject (runObjParser $ AsRecord . Generics.to <$> grecParse)
  metaToJson (AsRecord a) = JSON.object $ grecToMeta $ Generics.from a

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



----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

deriving via AsAeson Char   instance MetaEncoding Char
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
  metaToJson (a,b) = Array $ V.fromList [metaToJson a, metaToJson b]

instance (MetaEncoding a, MetaEncoding b, MetaEncoding c) => MetaEncoding (a,b,c) where
  parseMeta = JSON.withArray "(a, b, c)" $ \arr -> case V.length arr of
    3 -> (,,) <$> parseMeta (V.unsafeIndex arr 0)
              <*> parseMeta (V.unsafeIndex arr 1)
              <*> parseMeta (V.unsafeIndex arr 2)
    n -> fail $ "Expecting 3-element array, got " ++ show n
  metaToJson (a,b,c) = Array $ V.fromList [metaToJson a, metaToJson b, metaToJson c]

instance (MetaEncoding a) => MetaEncoding (Maybe a) where
  parseMeta Null  = pure Nothing
  parseMeta o     = Just <$> parseMeta o
  metaToJson Nothing  = Null
  metaToJson (Just x) = metaToJson x

instance (MetaEncoding a) => MetaEncoding [a] where
  parseMeta  = fmap V.toList . parseMeta
  metaToJson = Array . V.fromList . map metaToJson

instance (MetaEncoding a, VU.Unbox a) => MetaEncoding (VU.Vector a) where
  parseMeta  = fmap VU.convert . parseMeta @(V.Vector a)
  metaToJson = Array . V.map metaToJson . V.convert

instance (MetaEncoding a, VS.Storable a) => MetaEncoding (VS.Vector a) where
  parseMeta  = fmap VS.convert . parseMeta @(V.Vector a)
  metaToJson = Array . V.map metaToJson . V.convert

instance (MetaEncoding a) => MetaEncoding (V.Vector a) where
  parseMeta  = JSON.prependFailure " - traversing Array\n"
             . JSON.withArray "Vector" (traverse parseMeta)
  metaToJson = Array . V.map metaToJson


deriving via F.ViaFixed (FB.Vec n) a
    instance (MetaEncoding a, KnownNat n, F.Arity n) => MetaEncoding (FB.Vec n a)
deriving via F.ViaFixed (FF.Vec n) a
    instance (MetaEncoding a, KnownNat n, F.Arity n) => MetaEncoding (FF.Vec n a)
deriving via F.ViaFixed (FU.Vec n) a
    instance (MetaEncoding a, KnownNat n, FU.Unbox n a) => MetaEncoding (FU.Vec n a)
deriving via F.ViaFixed (FS.Vec n) a
    instance (MetaEncoding a, KnownNat n, F.Arity n, FS.Storable a
             ) => MetaEncoding (FS.Vec n a)
deriving via F.ViaFixed (FP.Vec n) a
    instance (MetaEncoding a, KnownNat n, F.Arity n, FP.Prim a
             ) => MetaEncoding (FP.Vec n a)


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
          . KM.toList
        JSON.FromJSONKeyValue parser -> JSON.withArray "Map"
          $ fmap Map.fromList
          . traverse (\o -> do (k,v) <- parseMeta o
                               (,) <$> parser k <*> parseMeta v)
          . V.toList
        where
          errK k = JSON.prependFailure (" - key: "<>JSON.toString k<>"\n")
  metaToJson m = case JSON.toJSONKey of
    JSON.ToJSONKeyText  f _ -> JSON.object [ JSON.toText (f k) .== v | (k,v) <- Map.toList m ]
    JSON.ToJSONKeyValue f _ -> Array $ V.fromListN (Map.size m)
      [ Array $ V.fromListN 2 [f k, coerce $ metaToJson v]
      | (k,v) <- Map.toList m ]


instance (MetaEncoding a) => MetaEncoding (IntMap.IntMap a) where
  parseMeta
    = JSON.prependFailure "Parsing IntMap\n"
    . JSON.withObject "IntMap"
      ( fmap IntMap.fromList
      . traverse (\(k,v) -> errK k $ do
                     i <- liftAttoparsec (Atto.signed Atto.decimal) (JSON.toText k)
                     a <- parseMeta v
                     pure (i,a))
      . KM.toList
      )
    where
      errK k = JSON.prependFailure (" - key: "<>JSON.toString k<>"\n")
  metaToJson m = JSON.object
    [ T.pack (show k) .== v | (k,v) <- IntMap.toList m ]



liftAttoparsec :: Atto.Parser a -> Text -> JSON.Parser a
liftAttoparsec parser txt =
  case Atto.parse parser txt of
    Atto.Done rest a
      | T.null rest -> pure a
      | otherwise   -> fail "Not all input is consumed"
    Atto.Fail _ _ err -> fail err
    Atto.Partial _    -> fail "Incomplete input"
