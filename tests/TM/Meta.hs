{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveGeneric        #-}
{-# LANGUAGE DerivingStrategies   #-}
{-# LANGUAGE DerivingVia          #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE ImportQualifiedPost  #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeApplications     #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}

{-# OPTIONS_GHC -Wno-orphans #-}
-- |
module TM.Meta (tests) where

import Data.Typeable
import OKA.Metadata
import Data.Vector.Fixed       qualified as F
import Data.Vector.Fixed.Boxed qualified as FB
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.Tasty.HUnit
import Test.QuickCheck.Arbitrary.Generic
import Data.Histogram.Bin
import Data.Histogram.QuickCheck ()
import GHC.Generics (Generic)


tests :: TestTree
tests = testGroup "Metadata"
  [ testGroup "Roundtrip"
    [ testSerialise @BinD
    , testSerialise @Int
    , testSerialise @[Int]
    , testSerialise @(Int,Double)
    , testSerialise @(Int,Double,(Int,Int))
    , testSerialise @(Maybe [Int])
    , testSerialise @(FB.Vec 4 Int)
      --
    , testSerialise @ENUM
    , testSerialise @Record
    , testSerialise @Record2
    ]
  , testGroup "Mangler-Tick"
    [ testMangle "asd"        "asd"
    , testMangle "Adf"        "adf"
    , testMangle "AAb"        "AAb"
    , testMangle "AAAb"       "AAAb"
    , testMangle "AAAAbc"     "AAAAbc"
    , testMangle "asdX"       "asd_X"
    , testMangle "asdXX"      "asd_XX"
    , testMangle "FooBar"     "foo_bar"
    , testMangle "met'FooBar" "foo_bar"
    ]
  ]

testSerialise :: forall a. (Typeable a, Arbitrary a, Show a, Eq a, IsMeta a) => TestTree
testSerialise
  = testProperty (show (typeOf (undefined :: a)))
  $ \(a::a) -> fromMeta (toMeta a) == a

testMangle :: String -> String -> TestTree
testMangle field key = testCase (field ++ " -> " ++ key) $ do
  key @=? mangleFieldName @ManglerTick field

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
  deriving IsMeta    via AsRecord ManglerTick Record
  deriving Arbitrary via GenericArbitrary Record

data Record2 = Record2
  { rec'foo    :: Int
  , rec'BarBaz :: Maybe Int
  }
  deriving stock (Show,Read,Eq,Generic)
  deriving IsMeta    via AsRecord ManglerTick Record2
  deriving Arbitrary via GenericArbitrary Record2


----------------------------------------------------------------

instance (Arbitrary a, F.Arity n) => Arbitrary (FB.Vec n a) where
  arbitrary = F.replicateM arbitrary
