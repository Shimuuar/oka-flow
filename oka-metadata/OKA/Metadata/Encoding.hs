{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE GADTs                #-}
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
  , AsPositionalSExp(..)
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
import Data.Maybe
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
--
--   By conventions we don't encode missing values in records instead
--   of representing them as @null@s. This allows to add optional
--   fields without changing hashes of existing values.
class Typeable a => MetaEncoding a where
  -- | Parser for JSON values.
  parseMeta  :: JSON.Value -> JSON.Parser a
  -- | Encode value to JSON.
  metaToJson :: a -> JSON.Value

  parseMetaList :: JSON.Value -> JSON.Parser [a]
  parseMetaList  = fmap V.toList . parseMeta

  metaListToJSON :: [a] -> JSON.Value
  metaListToJSON = Array . V.fromList . map metaToJson

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

-- | Lookup mandatory field in the object. Missing fields are
--   interpreted as null fields.
metaField :: MetaEncoding a => Text -> ObjParser a
metaField k = ObjParser $ do
  (v, o) <- popFromMapM k =<< get
  put o
  lift $ JSON.prependFailure (" - key: " ++ T.unpack k ++ "\n")
       $ parseMeta (fromMaybe Null v)


-- | Lookup field in the object. Missing fields are returned as @Nothing@.
metaFieldM :: MetaEncoding a => Text -> ObjParser (Maybe a)
metaFieldM k = ObjParser $ do
  (v, o) <- popFromMapM k =<< get
  put o
  lift $ JSON.prependFailure (" - key: " ++ T.unpack k ++ "\n")
       $ case v of
           Nothing -> pure Nothing
           Just x  -> parseMeta x



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
type role AsAeson          representational
type role AsRecord         representational
type role AsReadShow       representational
type role AsPositionalSExp representational

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
instance ( MetaEncoding a, KnownSymbol fld
         ) => GRecParse (M1 S ('MetaSel ('Just fld) u s d) (K1 i a)) where
  grecParse = M1 . K1 <$> metaField (fieldName @fld)
 

deriving newtype instance GRecToMeta f => GRecToMeta (M1 D i f)
deriving newtype instance GRecToMeta f => GRecToMeta (M1 C i f)
instance (GRecToMeta f, GRecToMeta g) => GRecToMeta (f :*: g) where
  grecToMeta (f :*: g) = grecToMeta f <> grecToMeta g
instance (MetaEncoding a, KnownSymbol fld
         ) => GRecToMeta (M1 S ('MetaSel ('Just fld) u s d) (K1 i a)) where
  grecToMeta (M1 (K1 a)) = case metaToJson a of
    Null -> []
    js   -> [fieldName @fld .== js]



-- | Encode sum type as a positional S-expression. in form
--   @["ConstructorName", field1, field2]@.
newtype AsPositionalSExp a = AsPositionalSExp a

instance ( Generic a
         , GSExpEncode   (Rep a)
         , GSExpParseCon (Rep a)
         , Typeable a
         ) => MetaEncoding (AsPositionalSExp a) where
  parseMeta
    = JSON.prependFailure ("While parsing " ++ typeName @a ++ "\n")
    . JSON.withArray "S-exp"
      (\arr -> case V.toList arr of
          JSON.String tag : fields -> gsexpParseCon tag fields >>= \case
            Nothing -> fail $ "Unknown constructor " ++ T.unpack tag
            Just a  -> pure (AsPositionalSExp $ Generics.to a)
          [] -> fail "No constructor tag"
          _  -> fail "Constructor tag should be a string"
      )
  metaToJson (AsPositionalSExp a)
    = JSON.Array $ V.fromList $ gsexpToMeta $ Generics.from a


-- NOTE. Parser should not attempt to backtrack if we already committed to
--       constructor. Thus use of maybe to signal unknown constructor
class GSExpParseCon f where
  gsexpParseCon :: Text -> [JSON.Value] -> JSON.Parser (Maybe (f p))

class GSExpParseFields f where
  gsexpParseFld :: StateT [JSON.Value] JSON.Parser (f p)

class GSExpEncode f where
  gsexpToMeta :: f p -> [JSON.Value]

deriving newtype instance GSExpParseCon f => GSExpParseCon (M1 D i f)
instance (GSExpParseCon f, GSExpParseCon g) => GSExpParseCon (f :+: g) where
  gsexpParseCon con fields =
    gsexpParseCon con fields >>= \case
      Just f  -> pure (Just (L1 f))
      Nothing -> fmap R1 <$> gsexpParseCon con fields
instance (GSExpParseFields f, Constructor con) => GSExpParseCon (M1 C con f) where
  gsexpParseCon con fields
    -- Constructor don't match. Backtrack
    | con /= T.pack (conName (undefined :: M1 C con f ())) = pure Nothing
    -- Construct match. Commit to parse
    | otherwise = runStateT gsexpParseFld fields >>= \case
        (a,[]) -> pure (Just (M1 a))
        (_,_)  -> fail "Too many fields"


deriving newtype instance GSExpParseFields f => GSExpParseFields (M1 S i f)
instance (GSExpParseFields f, GSExpParseFields g) => GSExpParseFields (f :*: g) where
  gsexpParseFld = (:*:) <$> gsexpParseFld <*> gsexpParseFld
instance GSExpParseFields U1 where
  gsexpParseFld = pure U1
instance MetaEncoding a => GSExpParseFields (K1 i a) where
  gsexpParseFld = get >>= \case
    []   -> fail "Not enough fields"
    j:js -> put js >> lift (K1 <$> parseMeta j)


deriving newtype instance GSExpEncode f => GSExpEncode (M1 D i f)
deriving newtype instance GSExpEncode f => GSExpEncode (M1 S i f)
instance (GSExpEncode f, Constructor con) => GSExpEncode (M1 C con f) where
  gsexpToMeta c@(M1 f) = JSON.String (T.pack (conName c))
                       : gsexpToMeta f
instance (GSExpEncode f, GSExpEncode g) => GSExpEncode (f :+: g) where
  gsexpToMeta (L1 f) = gsexpToMeta f
  gsexpToMeta (R1 g) = gsexpToMeta g
instance (GSExpEncode f, GSExpEncode g) => GSExpEncode (f :*: g) where
  gsexpToMeta (f :*: g) = gsexpToMeta f <> gsexpToMeta g
instance GSExpEncode U1 where
  gsexpToMeta U1 = []
instance (MetaEncoding a) => GSExpEncode (K1 i a) where
  gsexpToMeta (K1 a) = [metaToJson a]

----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance MetaEncoding Char where
  parseMeta      = JSON.parseJSON
  metaToJson     = JSON.toJSON
  parseMetaList  = JSON.parseJSONList
  metaListToJSON = JSON.toJSONList


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
  parseMeta = \case
    Null  -> pure Nothing
    o     -> Just <$> parseMeta o
  metaToJson = \case
    Nothing -> Null
    Just x  -> metaToJson x

instance (MetaEncoding a) => MetaEncoding [a] where
  parseMeta  = parseMetaList
  metaToJson = metaListToJSON

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
  case Atto.parseOnly (parser <* Atto.endOfInput) txt of
    Right a -> pure a
    Left  e -> fail e
