{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
module OKA.Metadata.Util where

import Data.Aeson    (Value(..))
import Data.Text     qualified as T
import Data.Typeable
import GHC.TypeLits


typeName :: forall a. Typeable a => String
typeName = show (typeOf (undefined :: a))

constrName :: Value -> String
constrName = \case
  Object{} -> "object"
  Array{}  -> "array"
  Number{} -> "number"
  String{} -> "string"
  Bool{}   -> "boolean"
  Null     -> "null"

fieldName :: forall fld. (KnownSymbol fld) => T.Text
fieldName = T.pack $ symbolVal (Proxy @fld)
