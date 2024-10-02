{-# LANGUAGE AllowAmbiguousTypes  #-}
{-# LANGUAGE DataKinds            #-}
{-# LANGUAGE DeriveAnyClass       #-}
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
import GHC.Generics (Generic)


tests :: TestTree
tests = testGroup "Metadata"
  [ testGroup "Roundtrip MetaEncoding"
    [ testSerialise @Int
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
    , testIsMeta @(Record,Record2,Record3)
    , testIsMeta @(Record,Record2,Record3,Record4)
    , testIsMeta @(Record,Record2,Record3,Record4,Record5)
    , testIsMeta @(Record,Record2,Record3,Record4,Record5,Record6)
    , testIsMeta @(Record,Record2,Record3,Record4,Record5,Record6,Record7)
    , testIsMeta @(Record,Record2,Record3,Record4,Record5,Record6,Record7,Record8)
    , testIsMeta @((Record,Record2),Record3,Record4)
    -- Check clash detection
    , testCase "Clash detected" $ case encodeToMetadataEither (undefined :: (Record,Record)) of
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
  $ \(a::a) -> fromMeta (metaToJson a) == a

fromMeta :: MetaEncoding a => JSON.Value -> a
fromMeta = either error id . JSON.parseEither parseMeta

testIsMeta :: forall a. (Typeable a, Arbitrary a, Show a, Eq a, IsMeta a) => TestTree
testIsMeta = testGroup (show (typeOf (undefined :: a)))
  [ testProperty "JSON"  (testIsMetaJSON   @a)
  , testProperty "JSON2" (testEncodeIsSame @a)
  , testProperty "Meta"  (testIsMetaMeta   @a)
  ]

testIsMetaJSON :: (IsMeta a, Eq a) => a -> Property
testIsMetaJSON a
  = property
  $ decodeMetadata (encodeToMetadata a) == a

testEncodeIsSame :: (IsMeta a) => a -> Property
testEncodeIsSame a
  = property
  $ encodeToMetadata a == encodeMetadata (toMetadata a)

testIsMetaMeta :: (IsMeta a, Eq a) => a -> Property
testIsMetaMeta a
  = property
  $ fromMetadata (toMetadata a) == Just a


  
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
  deriving IsMetaPrim   via AsMeta '["rec1"] Record
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary Record

data Record2 = Record2
  { foo2 :: Int
  , bar2 :: Maybe Int
  }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord             Record2
  deriving IsMetaPrim   via AsMeta ["rec2","xx"] Record2
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary     Record2

data Record3 = Record3 { foo3 :: Int }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord          Record3
  deriving IsMetaPrim   via AsMeta '["rec3"]  Record3
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary  Record3

data Record4 = Record4 { foo4 :: Int }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord          Record4
  deriving IsMetaPrim   via AsMeta '["rec4"]  Record4
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary  Record4

data Record5 = Record5 { foo5 :: Int }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord          Record5
  deriving IsMetaPrim   via AsMeta '["rec5"]  Record5
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary  Record5

data Record6 = Record6 { foo6 :: Int }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord          Record6
  deriving IsMetaPrim   via AsMeta '["rec6"]  Record6
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary  Record6

data Record7 = Record7 { foo7 :: Int }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord          Record7
  deriving IsMetaPrim   via AsMeta '["rec7"]  Record7
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary  Record7

data Record8 = Record8 { foo8 :: Int }
  deriving stock (Show,Read,Eq,Generic)
  deriving MetaEncoding via AsRecord          Record8
  deriving IsMetaPrim   via AsMeta '["rec8"]  Record8
  deriving anyclass (IsMeta)
  deriving Arbitrary    via GenericArbitrary  Record8

----------------------------------------------------------------

instance (Arbitrary a, F.Arity n) => Arbitrary (FB.Vec n a) where
  arbitrary = F.replicateM arbitrary
