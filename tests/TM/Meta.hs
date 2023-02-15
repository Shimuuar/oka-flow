{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- {-# OPTIONS_GHC -Wno-orphans #-}
-- |
module TM.Meta (tests) where

import Data.Typeable
import OKA.Metadata
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.QuickCheck.Arbitrary.Generic
import Data.Histogram.Bin
import Data.Histogram.QuickCheck ()
import GHC.Generics (Generic)


tests :: TestTree
tests = testGroup "Roundtrip"
  [ testSerialise @BinD
  , testSerialise @Int
  , testSerialise @[Int]
  , testSerialise @(Int,Double)
  , testSerialise @(Int,Double,(Int,Int))
  , testSerialise @(Maybe [Int])
    --
  , testSerialise @ENUM
  , testSerialise @Record
  , testSerialise @Record2
  ]

testSerialise :: forall a. (Typeable a, Arbitrary a, Show a, Eq a, IsMeta a) => TestTree
testSerialise
  = testProperty (show (typeOf (undefined :: a)))
  $ \(a::a) -> fromMeta (toMeta a) == a


----------------------------------------------------------------
-- Derivations
----------------------------------------------------------------

data ENUM = A | B | C
  deriving stock (Show,Read,Eq,Generic)
  deriving IsMeta    via AsReadShow ENUM
  deriving Arbitrary via GenericArbitrary ENUM

data Record = Record
  { foo :: Int
  , bar :: Maybe Int
  }
  deriving stock (Show,Read,Eq,Generic)
  deriving IsMeta    via AsRecord Record
  deriving Arbitrary via GenericArbitrary Record

data Record2 = Record2
  { rec'foo    :: Int
  , rec'BarBaz :: Maybe Int
  }
  deriving stock (Show,Read,Eq,Generic)
  deriving IsMeta    via AsRecord Record2
  deriving Arbitrary via GenericArbitrary Record2

