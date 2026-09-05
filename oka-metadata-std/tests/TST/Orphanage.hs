-- |
module TST.Orphanage where

import Control.Monad
import Test.Tasty.QuickCheck
import Test.QuickCheck.Arbitrary.Generic

import OKA.Metadata.Std.Cuts

deriving newtype instance Arbitrary a => Arbitrary (Less      a)
deriving newtype instance Arbitrary a => Arbitrary (LessEQ    a)
deriving newtype instance Arbitrary a => Arbitrary (Greater   a)
deriving newtype instance Arbitrary a => Arbitrary (GreaterEQ a)

deriving via GenericArbitrary Range instance Arbitrary Range
