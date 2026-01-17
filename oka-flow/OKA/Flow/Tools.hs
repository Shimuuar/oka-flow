{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE UndecidableInstances #-}
-- |
-- Tools for defining concrete workflows. It contain both tools for
-- defining workflows themselves and @Flow@ wrappers.
module OKA.Flow.Tools
  ( -- * Interaction with store
    -- $store
    FlowOutput(..)
  , FlowInput(..)
  , Action(..)
  , Dataflow(..)
  , ParamFlow(..)
  , S(..)
    -- ** Argument reading
  , FlowArgument(..)
    -- ** Deriving via
  , AsMetaEncoded(..)
  , AsFlowInput(..)
    -- * Class for polymorphic parameters
  , SequenceOf(..)
    -- * Lifting of haskell function
  , liftHaskellFun
  , liftHaskellFunMeta
  , liftHaskellFun_
  , liftHaskellFunMeta_
  , liftHaskellPhonyFun
  , liftHaskellPhonyFunMeta
    -- * Calling of external executables
  , liftExecutable
  , liftPhonyExecutable
    -- ** Calling conventions
  , callStandardExe
  , callInEnvironment
  , callViaArgList
  , ccPrependRelPaths
  , ccPrependArgs
  , ccAddEnv
    -- * Primitives for creating flows
  , basicLiftWorkflow
  , basicLiftPhony
    -- * Encoding\/decoding of S-expressions
  , sexpToArgs
  , sexpFromArgs
    -- * Creating executables
  , metaFromStdin
  , loadParametersFromCLI
  ) where

import Control.Applicative
-- import Control.Concurrent.STM
import Control.Exception
import Control.Lens                 ((^?))
import Data.Coerce
import Data.Aeson                   qualified as JSON
import Data.Aeson.Types             qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Monoid                  (Endo(..))
import Data.Functor
import System.FilePath              ((</>))
import System.Directory             (makeAbsolute)
import System.IO.Temp
import System.IO                    (hClose)
import System.Environment           (getArgs)

import GHC.Generics hiding (S)

import OKA.Metadata
import OKA.Flow.Core.Resources
import OKA.Flow.Core.Types
import OKA.Flow.Core.Graph
import OKA.Flow.Core.S
import OKA.Flow.Core.Flow
import OKA.Flow.Internal.Parser


----------------------------------------------------------------
-- Interaction with store
----------------------------------------------------------------

-- $store
--
-- Type class 'FlowOutput' and 'FlowInput' define simple standard
-- method of reading and writing data to store paths. If data type
-- implements both following law should hold:
--
-- > do writeOutput path a
-- >    a' <- readOutput path
-- >    a == a'


-- | Type class which describes how data type should be stored to
--   output.
class FlowOutput a where
  -- | Write output to the given directory.
  writeOutput :: FilePath -- ^ Output directory
              -> a        -- ^ Value to be saved
              -> IO ()

-- | Type class which describes how data type should be read from
--   output.
class FlowInput a where
  -- | Read output from given directory
  readOutput :: FilePath -- ^ Directory to read from
             -> IO a


-- | This type class describes how we parse and load data type of type
--   @a@ from S-expression containing path in store.
class (ToS (AsRes a)) => FlowArgument a where
  -- | 'Result' or tuple of 'Result's which represent this data type.
  type AsRes a
  -- | How data types should be parsed from list of absolute path to
  --   directories.
  parseFlowArguments :: S FilePath -> Either String (IO a)
  -- |
  parameterShape :: (forall x. Result x) -> a -> AsRes a



----------------------------------------------------------------
-- SequenceOf
----------------------------------------------------------------

-- | Convenience type class for collecting list of results for passing
--   for flow which simply accept uniform lists of parameters
class SequenceOf x a where
  sequenceOf :: a -> [Result x]

instance SequenceOf x (Result x) where
  sequenceOf p = [p]
deriving via Generically (a,b)
    instance (SequenceOf x a, SequenceOf x b) => SequenceOf x (a,b)
deriving via Generically (a,b,c)
    instance (SequenceOf x a, SequenceOf x b, SequenceOf x c) => SequenceOf x (a,b,c)
deriving via Generically (a,b,c,d)
    instance (SequenceOf x a, SequenceOf x b, SequenceOf x c, SequenceOf x d
             ) => SequenceOf x (a,b,c,d)
deriving via Generically (a,b,c,d,e)
    instance ( SequenceOf x a, SequenceOf x b, SequenceOf x c, SequenceOf x d
             , SequenceOf x e
             ) => SequenceOf x (a,b,c,d,e)
deriving via Generically (a,b,c,d,e,f)
    instance ( SequenceOf x a, SequenceOf x b, SequenceOf x c, SequenceOf x d
             , SequenceOf x e, SequenceOf x f
             ) => SequenceOf x (a,b,c,d,e,f)
deriving via Generically (a,b,c,d,e,f,g)
    instance ( SequenceOf x a, SequenceOf x b, SequenceOf x c, SequenceOf x d
             , SequenceOf x e, SequenceOf x f, SequenceOf x g
             ) => SequenceOf x (a,b,c,d,e,f,g)
deriving via Generically (a,b,c,d,e,f,g,h)
    instance ( SequenceOf x a, SequenceOf x b, SequenceOf x c, SequenceOf x d
             , SequenceOf x e, SequenceOf x f, SequenceOf x g, SequenceOf x h
             ) => SequenceOf x (a,b,c,d,e,f,g,h)


instance (SequenceOf x a) => SequenceOf x [a] where
  sequenceOf = concatMap sequenceOf

-- | This instance could be used to derive SequenceOf instance
--   with @DerivingVia@
instance (Generic a, GSequenceOf x (Rep a)) => SequenceOf x (Generically a) where
  sequenceOf (Generically a) = gsequenceOf (from a)


class GSequenceOf x f where
  gsequenceOf :: f p -> [Result x]

deriving newtype instance GSequenceOf x f => GSequenceOf x (M1 c i f)

instance (GSequenceOf x f, GSequenceOf x g) => GSequenceOf x (f :*: g) where
  gsequenceOf (f :*: g) = gsequenceOf f <> gsequenceOf g

instance (SequenceOf x a) => GSequenceOf x (K1 i a) where
  gsequenceOf = coerce (sequenceOf @x @a)


----------------------------------------------------------------
-- Deriving via
----------------------------------------------------------------

-- | Newtype wrapper for deriving 'FlowInput' and 'FlowOutput'
--   instance where data is store as JSON file @data.json@ encoded
--   using 'MetaEncoding' instance.
newtype AsMetaEncoded a = AsMetaEncoded a
  deriving FlowArgument via AsFlowInput a

instance MetaEncoding a => FlowOutput (AsMetaEncoded a) where
  writeOutput dir (AsMetaEncoded a) =
    BL.writeFile (dir </> "data.json") $ JSON.encode $ metaToJson a

instance MetaEncoding a => FlowInput (AsMetaEncoded a) where
  readOutput dir = do
    bs <- BL.readFile (dir </> "data.json")
    case JSON.decode bs of
      Nothing -> error "Invalid JSON"
      Just js -> case JSON.parseEither parseMeta js of
        Left  e -> error $ "Cannot decode metadata: " ++ show e
        Right x -> pure (AsMetaEncoded x)


-- | Derive instance of 'FlowArgument' for instance of 'FlowInput'
newtype AsFlowInput a = AsFlowInput a

instance (FlowInput a) => FlowArgument (AsFlowInput a) where
  type AsRes (AsFlowInput a) = Result a
  parseFlowArguments = \case
    Param p -> Right $ AsFlowInput <$> readOutput p
    _       -> Left "Expecting single parameter"
  parameterShape x _ = x


----------------------------------------------------------------
-- Instances
----------------------------------------------------------------

instance FlowArgument () where
  type AsRes () = ()
  parseFlowArguments = \case
    Nil -> pure (pure ())
    _   -> Left "Expecting nil"
  parameterShape _   ()  = ()

instance FlowArgument a => FlowArgument [a] where
  type AsRes [a] = [AsRes a]
  parseFlowArguments = \case
    S xs -> do
      as <- traverse parseFlowArguments xs
      pure $ sequence as
    _ -> Left "Expecting list"
  parameterShape p   xs = parameterShape p <$> xs

instance FlowArgument a => FlowArgument (Maybe a) where
  type AsRes (Maybe a) = Maybe (AsRes a)
  parseFlowArguments = \case
    Nil -> pure (pure Nothing)
    s   -> do a <- parseFlowArguments s
              pure $ Just <$> a
  parameterShape p xs = parameterShape p <$> xs

instance (FlowArgument a, FlowArgument b) => FlowArgument (a,b) where
  type AsRes (a,b) = (AsRes a, AsRes b)
  parseFlowArguments = \case
    S [sa,sb] -> do
      ioa <- parseFlowArguments sa
      iob <- parseFlowArguments sb
      pure $ (,) <$> ioa <*> iob
    _ -> Left "Expecting 2-element list"
  parameterShape p (a,b) = (parameterShape p a, parameterShape p b)

instance (FlowArgument a, FlowArgument b, FlowArgument c) => FlowArgument (a,b,c) where
  type AsRes (a,b,c) = (AsRes a, AsRes b, AsRes c)
  parseFlowArguments = \case
    S [sa,sb,sc] -> do
      ioa <- parseFlowArguments sa
      iob <- parseFlowArguments sb
      ioc <- parseFlowArguments sc
      pure $ (,,) <$> ioa <*> iob <*> ioc
    _ -> Left "Expecting 3-element list"
  parameterShape p (a,b,c) = ( parameterShape p a
                             , parameterShape p b
                             , parameterShape p c)

instance (FlowArgument a, FlowArgument b, FlowArgument c, FlowArgument d
         ) => FlowArgument (a,b,c,d) where
  type AsRes (a,b,c,d) = (AsRes a, AsRes b, AsRes c, AsRes d)
  parseFlowArguments = \case
    S [sa,sb,sc,sd] -> do
      ioa <- parseFlowArguments sa
      iob <- parseFlowArguments sb
      ioc <- parseFlowArguments sc
      iod <- parseFlowArguments sd
      pure $ (,,,) <$> ioa <*> iob <*> ioc <*> iod
    _ -> Left "Expecting 3-element list"
  parameterShape p (a,b,c,d) = ( parameterShape p a
                               , parameterShape p b
                               , parameterShape p c
                               , parameterShape p d)



----------------------------------------------------------------
-- Lifting haskell function
--
-- FIXME: How to properly encode result of b? Is Result OK?
----------------------------------------------------------------

-- | Convert haskell function into dataflow. Arguments are loaded and
--   stored using type classes methods.
liftHaskellFun
  :: (FlowArgument a, FlowOutput b, IsMeta meta, ResourceClaim res)
  => String              -- ^ Name of flow
  -> res                 -- ^ Resources required by workflow
  -> (meta -> a -> IO b) -- ^ IO action
  -> (AsRes a -> Flow eff (Result b))
liftHaskellFun name res action = basicLiftWorkflow res $ Dataflow
  { name = name
  , flow = ActionIO $ \_ p -> do
      meta <- case p.meta ^? metadata of
        Just m  -> pure m
        Nothing -> error $ "loadFlowArguments: Cannot get metadata"
      a    <- case parseFlowArguments p.args of
        Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
        Right ioa -> ioa
      b <- action meta a
      case p.out of
        Just out -> writeOutput out b
        Nothing  -> error "liftHaskellFun: output is required"
  }

-- | Same as 'liftHaskellFun' but passes noninterpreted metadata.
liftHaskellFunMeta
  :: (FlowArgument a, FlowOutput b, ResourceClaim res)
  => String                  -- ^ Name of flow
  -> res                     -- ^ Resources required by workflow
  -> (Metadata -> a -> IO b) -- ^ IO action
  -> (AsRes a -> Flow eff (Result b))
liftHaskellFunMeta name res action = basicLiftWorkflow res $ Dataflow
  { name = name
  , flow = ActionIO $ \_ p -> do
      a <- case parseFlowArguments p.args of
        Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
        Right ioa -> ioa
      b <- action p.meta a
      case p.out of
        Just out -> writeOutput out b
        Nothing  -> error "liftHaskellFun: output is required"
  }


-- | Convert haskell function into dataflow. Arguments are loaded
--   using type 'FlowArgument' type class and provided action takes
--   output directory as first argument and is responsible for writing
--   outputs.
liftHaskellFun_
  :: (FlowArgument a, IsMeta meta, ResourceClaim res)
  => String                           -- ^ Name of flow
  -> res                              -- ^ Resources required by workflow
  -> (FilePath -> meta -> a -> IO ()) -- ^ IO action
  -> (AsRes a -> Flow eff (Result b))
liftHaskellFun_ name res action = basicLiftWorkflow res $ Dataflow
  { name = name
  , flow = ActionIO $ \_ p -> do
      meta <- case p.meta ^? metadata of
        Just m  -> pure m
        Nothing -> error $ "loadFlowArguments: Cannot get metadata"
      a    <- case parseFlowArguments p.args of
        Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
        Right ioa -> ioa
      case p.out of
        Just out -> action out meta a
        Nothing  -> error "liftHaskellFun: output is required"
  }

-- | Same as 'liftHaskellFun_' but passes noninterpreted metadata.
liftHaskellFunMeta_
  :: (FlowArgument a, ResourceClaim res)
  => String                               -- ^ Name of flow
  -> res                                  -- ^ Resources required by workflow
  -> (FilePath -> Metadata -> a -> IO ()) -- ^ IO action
  -> (AsRes a -> Flow eff (Result b))
liftHaskellFunMeta_ name res action = basicLiftWorkflow res $ Dataflow
  { name = name
  , flow = ActionIO $ \_ p -> do
      a <- case parseFlowArguments p.args of
        Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
        Right ioa -> ioa
      case p.out of
        Just out -> action out p.meta a
        Nothing  -> error "liftHaskellFun: output is required"
  }


----------------------------------------------------------------
-- Calling conventions
----------------------------------------------------------------

liftExecutable
  :: (ToS args, ResourceClaim res)
  => String      -- ^ Name of flow
  -> FilePath    -- ^ Executable name
  -> res         -- ^ Resources required by workflow
  -> CallingConv -- ^ Calling convention
  -> (args -> Flow eff (Result b))
liftExecutable name exe res call =
  basicLiftWorkflow res $ Dataflow
    { name = name
    , flow = ActionExe $ Executable exe call
    }

liftPhonyExecutable
  :: (ToS args, ResourceClaim res)
  => FilePath    -- ^ Executable name
  -> res         -- ^ Resources required by workflow
  -> CallingConv -- ^ Calling convention
  -> (args -> Flow eff ())
liftPhonyExecutable exe res call =
  basicLiftPhony res $ ActionExe $ Executable exe call


-- | Convert haskell function into dataflow. Arguments are loaded and
--   stored using type classes methods.
liftHaskellPhonyFun
  :: (FlowArgument a, IsMeta meta, ResourceClaim res)
  => res                  -- ^ Resources required by workflow
  -> (meta -> a -> IO ()) -- ^ IO action
  -> (AsRes a -> Flow eff ())
liftHaskellPhonyFun res action = basicLiftPhony res $ ActionIO $ \_ p -> do
  meta <- case p.meta ^? metadata of
    Just m  -> pure m
    Nothing -> error $ "loadFlowArguments: Cannot get metadata"
  a    <- case parseFlowArguments p.args of
    Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
    Right ioa -> ioa
  action meta a


-- | Same as 'liftHaskellFun' but passes noninterpreted metadata.
liftHaskellPhonyFunMeta
  :: (FlowArgument a, ResourceClaim res)
  => res                      -- ^ Resources required by workflow
  -> (Metadata -> a -> IO ()) -- ^ IO action
  -> (AsRes a -> Flow eff ())
liftHaskellPhonyFunMeta res action = basicLiftPhony res $ ActionIO $ \_ p -> do
  a <- case parseFlowArguments p.args of
    Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
    Right ioa -> ioa
  action p.meta a


-- | Standard calling conventions for external process.
--
--  * JSON-encoded metadata is passed to process via stdin.
--
--  * Arguments are passed via command line serialized as S-expression
--
--  * Working directory is set to output directory
callStandardExe
  :: ParamFlow FilePath
  -> (ProcessData -> IO a)
  -> IO a
callStandardExe p action = action ProcessData
  { stdin   = Just $ JSON.encode $ encodeMetadata p.meta
  , env     = []
  , args    = sexpToArgs p.args
  , workdir = p.out
  }

-- | Pass arguments in the environment.
--
-- * Metadata is written in temporary file. It's stored in
--   @OKA_META@ environment variable.
--
-- * Arguments are passed in the same way as in 'callStandardExe' but
--   in environment variables @OKA_ARG_1@, @OKA_ARG_2@, etc.
--
-- * Working directory is set to output directory. Additionally it's
--   stored in @OKA_OUT@ environment variable.
callInEnvironment
  :: ParamFlow FilePath
  -> (ProcessData -> IO a)
  -> IO a
callInEnvironment p action =
  withSystemTempFile "oka-flow-metadata-" $ \file_meta h -> do
    -- Write metadata to temporary file
    BL.hPutStr h $ JSON.encode $ encodeMetadata p.meta
    hClose h
    -- Populate environment
    let env = case p.out of
                Nothing -> id
                Just d  -> (("OKA_OUT", d):)
            $ ("OKA_META", file_meta)
            : [ ("OKA_ARG_" ++ show i, arg)
              | (i,arg) <- [1::Int ..] `zip` sexpToArgs p.args
              ]
    action ProcessData
      { stdin   = Nothing
      , env     = env
      , args    = []
      , workdir = p.out
      }


-- | Call executable where all parameters are passing via command line
--   parameters.
--
--  * Metadata is discarded
--
--  * Working dir is set to output directory
callViaArgList
  :: (S FilePath -> [FilePath]) -- ^ How to transform list of store pathes
  -> ParamFlow FilePath
  -> (ProcessData -> IO a)
  -> IO a
callViaArgList transform p action = action $ ProcessData
  { stdin   = Nothing
  , env     = []
  , args    = transform p.args
  , workdir = p.out
  }

-- | Prepend path relative to flow working directory. It will be
--   converted to absolute path.
ccPrependRelPaths :: [FilePath] -> CallingConv -> CallingConv
ccPrependRelPaths paths call p action = call p $ \ProcessData{..} -> do
  paths' <- traverse makeAbsolute paths
  action ProcessData{args = paths' ++ args, ..}

-- | Prepend arguments to list being passed to flow. They are not
--   modified.
ccPrependArgs :: [FilePath] -> CallingConv -> CallingConv
ccPrependArgs xs call p action = call p $ \ProcessData{..} -> do
  action ProcessData{args = xs ++ args, ..}

-- | Add values to environment variables
ccAddEnv :: [(String,String)] -> CallingConv -> CallingConv
ccAddEnv xs cc p action =
  cc p (\ProcessData{..} -> action ProcessData{env = xs ++ env, ..})


----------------------------------------------------------------
-- Defining subprocesses
----------------------------------------------------------------

-- | Read metadata from stdin
metaFromStdin :: IsMeta a => IO a
metaFromStdin = do
  (BL.getContents <&> JSON.eitherDecode) >>= \case
    Left  e  -> error $ "Cannot read metadata: " ++ e
    Right js -> evaluate $ decodeMetadata js

-- | Load arguments from CLI parameters
loadParametersFromCLI :: FlowArgument a => IO a
loadParametersFromCLI = do
  args <- getArgs
  s    <- case sexpFromArgs args of
    Left  err -> error $ "loadFlowArguments: Cannot parse S-expression: " ++ err
    Right s   -> pure s
  case parseFlowArguments s of
    Left  err -> error $ "loadFlowArguments: Malformed S-expresion: " ++ err
    Right ioa -> ioa


----------------------------------------------------------------
-- Conversion
----------------------------------------------------------------

sexpToArgs :: S FilePath -> [FilePath]
sexpToArgs Nil = []
sexpToArgs s0  = ($ []) $ appEndo $ go s0 where
  go = \case
    Nil     -> cons "-"
    Atom  a -> cons ('!':a)
    Param p -> cons p
    S xs    -> cons "(" <> foldMap go xs <> cons ")"
  cons s = Endo (s:)

sexpFromArgs :: [FilePath] -> Either String (S FilePath)
sexpFromArgs [] = pure Nil
sexpFromArgs xs = runListParser parserS xs where
  parserS = consume >>= \case
    ""    -> fail "Empty parameter encountered"
    "("   -> S <$> parserL
    "-"   -> pure Nil
    '!':a -> pure $ Atom  a
    p     -> pure $ Param p
  parserL =  eol
         <|> liftA2 (:) parserS parserL
  eol     = consume >>= \case
    ")" -> pure []
    _   -> fail "Expecting )"




-- ----------------------------------------------------------------
-- -- GHC compilation
-- ----------------------------------------------------------------

-- -- | Compile program with GHC. Uses 'LockGHC' to ensure that only one
-- --   compilation runs at a time.
-- compileProgramGHC
--   :: FilePath    -- ^ Path to Main module
--   -> [String]    -- ^ Options to pass to GHC
--   -> ResourceSet -- ^ Set of resources
--   -> IO ()
-- compileProgramGHC exe opts res
--   = withResources res LockGHC
--   $ withProcessWait_ ghc
--   $ \_ -> pure ()
--   where
--     ghc = proc "ghc" (exe:opts)

-- -- | Default GHC options
-- defGhcOpts :: [String]
-- defGhcOpts = [ "-O2"
--              , "-threaded"
--              , "-with-rtsopts=-T -A8m"
--              ]
