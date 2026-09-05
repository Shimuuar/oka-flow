{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE OverloadedStrings #-}
-- |
-- Type class and data types for event selection
module OKA.Metadata.Std.Cuts
  ( -- * Type class for cuts
    Cut(..)
    -- * Standard cuts
    -- ** Range cuts
  , Greater(..)
  , GreaterEQ(..)
  , Less(..)
  , LessEQ(..)
  , Equal(..)
  , Range(..)
  , CutRange(..)
    -- ** Other cuts
  , BooleanCut(..)
  ) where

import Control.Applicative
import Control.Lens
import Data.Coerce
import Data.Text           (Text)
import Data.Typeable
import GHC.Generics        (Generic)

import OKA.Metadata.Encoding


----------------------------------------------------------------
-- Cut type class
----------------------------------------------------------------

-- | Type class for serializable cut. We want to be able to read cuts
--   values from configs and to store them as lookup keys.
class Cut c a where
  -- | Whether we should accept value of type @a@ for given selection
  -- @c@.
  cut :: c -> a -> Bool

-- | Optional cut. @Nothing@ means that all values of @a@ are accepted
instance Cut c a => Cut (Maybe c) a where
  {-# INLINE cut #-}
  cut Nothing  _ = True
  cut (Just c) a = cut c a

----------------------------------------------------------------
-- Range cuts
----------------------------------------------------------------

-- | Quantity is larger than given value.
newtype Greater a = Greater a
  deriving stock    (Show, Eq, Generic)
  deriving anyclass (Wrapped)

instance (Ord a, a ~ a') => Cut (Greater a) a' where
  cut (Greater a) x = x > a


-- | Quantity is larger or equal than given value.
newtype GreaterEQ a = GreaterEQ a
  deriving stock    (Show, Eq, Generic)
  deriving anyclass (Wrapped)

instance (Ord a, a ~ a') => Cut (GreaterEQ a) a' where
  cut (GreaterEQ a) x = x >= a


-- | Quantity is larger than given value.
newtype Less a = Less a
  deriving stock    (Show, Eq, Generic)
  deriving anyclass (Wrapped)

instance (Ord a, a ~ a') => Cut (Less a) a' where
  cut (Less a) x = x < a


-- | Quantity is larger or equal than given value.
newtype LessEQ a = LessEQ a
  deriving stock    (Show, Eq, Generic)
  deriving anyclass (Wrapped)

instance (Ord a, a ~ a') => Cut (LessEQ a) a' where
  cut (LessEQ a) x = x <= a

-- | Quantity is equal to the given value.
newtype Equal a = Equal a
  deriving stock    (Show, Eq, Generic)
  deriving anyclass (Wrapped)

instance (Eq a, a ~ a') => Cut (Equal a) a' where
  cut (Equal a) x = x == a


-- | Value lies in given inclusive range. Note it's not generic in
--   data type in order to allow unboxing
data Range = Range !Double !Double
  deriving stock    (Show,Eq,Generic)

instance a ~ Double => Cut Range a where
  cut (Range a b) x = a <= x && x <= b

-- | Range cut on double-valued quantity
newtype CutRange a = CutRange Range
  deriving stock    (Show, Eq, Generic)
  deriving anyclass (Wrapped)

instance (a ~ a', Coercible a Double) => Cut (CutRange a) a' where
  cut (CutRange rng) x = rng `cut` coerce x

deriving newtype instance (Typeable a) => MetaEncoding (CutRange a)



instance MetaEncoding Range where
  parseMeta v =  metaSExp2 "Range"  Range                       v
             <|> metaSExp2 "RangeC" (\a b -> Range (a-b) (a+b)) v
  metaToJson (Range a b) = metaToJson ("Range"::Text, a, b)

instance (Typeable a, MetaEncoding a) => MetaEncoding (Less a) where
  parseMeta = metaSExp1 "Less" Less
  metaToJson (Less x) = metaToJson ("Less"::Text, x)

instance (Typeable a, MetaEncoding a) => MetaEncoding (LessEQ a) where
  parseMeta = metaSExp1 "LessEQ" LessEQ
  metaToJson (LessEQ x) = metaToJson ("LessEQ"::Text, x)

instance (Typeable a, MetaEncoding a) => MetaEncoding (Greater a) where
  parseMeta = metaSExp1 "Greater" Greater
  metaToJson (Greater x) = metaToJson ("Greater"::Text, x)

instance (Typeable a, MetaEncoding a) => MetaEncoding (GreaterEQ a) where
  parseMeta = metaSExp1 "GreaterEQ" GreaterEQ
  metaToJson (GreaterEQ x) = metaToJson ("GreaterEQ"::Text, x)

instance (Typeable a, MetaEncoding a) => MetaEncoding (Equal a) where
  parseMeta = metaSExp1 "Equal" Equal
  metaToJson (Equal x) = metaToJson ("Equal"::Text, x)



----------------------------------------------------------------
-- Other cuts
----------------------------------------------------------------

-- | Cut on some binary condition
data BooleanCut
  = RequireTrue  -- ^ Require condition to be true
  | RequireFalse -- ^ Require condition to be false
  | Ignore       -- ^ Ignore condition.
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsReadShow BooleanCut

instance Cut BooleanCut Bool where
  cut RequireTrue  = id
  cut RequireFalse = not
  cut Ignore       = const True



----------------------------------------------------------------
-- Lens instances
----------------------------------------------------------------

instance Field1 Range Range Double Double where
instance Field2 Range Range Double Double where
instance Field1 (CutRange a) (CutRange a) Double Double where
  _1 = _Wrapped' . _1
  {-# INLINE _1 #-}
instance Field2 (CutRange a) (CutRange a) Double Double where
  _2 = _Wrapped' . _2
  {-# INLINE _2 #-}
