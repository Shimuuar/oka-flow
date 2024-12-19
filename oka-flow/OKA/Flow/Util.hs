{-# LANGUAGE AllowAmbiguousTypes #-}
-- |
module OKA.Flow.Util where

import Data.Typeable

typeName :: forall a. Typeable a => String
typeName = show (typeOf (undefined :: a))
