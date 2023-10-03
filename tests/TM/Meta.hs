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
import Data.Aeson.Types           qualified as JSON
import OKA.Metadata
import Data.Map.Strict         qualified as Map
import Data.Vector.Fixed       qualified as F
import Data.Vector.Fixed.Boxed qualified as FB
import Data.Text               (Text)
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.Tasty.HUnit
import Test.QuickCheck.Arbitrary.Generic
import Test.QuickCheck.Instances ()
import Data.Histogram.Bin
import Data.Histogram.QuickCheck ()
import GHC.Generics (Generic)


tests :: TestTree
tests = testGroup "Metadata"
  [ testGroup "Roundtrip MetaEncoding"
    [ testSerialise @BinD
    , testSerialise @Int
    , testSerialise @[Int]
    , testSerialise @(Int,Double)
    , testSerialise @(Int,Double,(Int,Int))
    , testSerialise @(Maybe [Int])
    , testSerialise @(FB.Vec 4 Int)
    , testSerialise @(Map.Map Int       Int)
    , testSerialise @(Map.Map Text      Int)
    , testSerialise @(Map.Map String    Int)
    , testSerialise @(Map.Map (Int,Int) Int)
    , testSerialise @ENUM
    ]
  , testGroup "Roundtrip IsMeta"
    [ testIsMeta @Record
    , testIsMeta @Record2
    , testIsMeta @(Record2,Record)
      -- Check clash detection
    , testCase "Clash detected" $ case encodeMetadataEither (undefined :: (Record,Record)) of
        Left  _ -> pure ()
        Right _ -> assertFailure "Should detect key clash"
    ]
  ]

----------------------------------------------------------------
-- Roundtrip tests
----------------------------------------------------------------

testSerialise :: forall a. (Typeable a, Arbitrary a, Show a, Eq a, MetaEncoding a) => TestTree
testSerialise
  = testProperty (show (typeOf (undefined :: a)))
  $ \(a::a) -> fromMeta (toMeta a) == a

fromMeta :: MetaEncoding a => JSON.Value -> a
fromMeta = either error id . JSON.parseEither parseMeta

testIsMeta :: forall a. (Typeable a, Arbitrary a, Show a, Eq a, IsMeta a) => TestTree
testIsMeta
  = testProperty (show (typeOf (undefined :: a)))
  $ \(a::a) -> decodeMetadata (encodeMetadata a) == a
  
----------------------------------------------------------------
-- Derivations
----------------------------------------------------------------

data ENUM = A | B | C
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsReadShow       ENUM
  deriving Arbitrary    via GenericArbitrary ENUM

data Record = Record
  { foo :: Int
  , bar :: Maybe Int
  }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord Record
  deriving IsMeta       via AsMeta '["rec1"] Record
  deriving Arbitrary    via GenericArbitrary Record

data Record2 = Record2
  { foo2 :: Int
  , bar2 :: Maybe Int
  }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord             Record2
  deriving IsMeta       via AsMeta ["rec2","xx"] Record2
  deriving Arbitrary    via GenericArbitrary     Record2


----------------------------------------------------------------

instance (Arbitrary a, F.Arity n) => Arbitrary (FB.Vec n a) where
  arbitrary = F.replicateM arbitrary
