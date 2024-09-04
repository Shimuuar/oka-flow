{-# LANGUAGE DerivingVia          #-}
{-# LANGUAGE FlexibleContexts     #-}
{-# LANGUAGE RoleAnnotations      #-}
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
  , AsMetaEncoded(..)
    -- * Standard tools
  , metaFromStdin
    -- * Command line arguments
  , ArgumentParser
  , FlowArgument(..)
  , runFlowArguments
  , runFlowArgumentsFromEnv
  , parseSingleArgument
  , AsFlowOutput(..)
    -- * Resources
  , compileProgramGHC
  , defGhcOpts
    -- * External process
  , runExternalProcess
  , runExternalProcessNoMeta
  , withParametersInEnv
  ) where

import Control.Applicative
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Strict
import Data.Aeson                   qualified as JSON
import Data.Aeson.Types             qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Functor
import System.Process.Typed
import System.FilePath              ((</>))
import System.IO.Temp
import System.Environment           (getArgs)
import System.Posix.Signals         (signalProcess, sigINT)
import System.Process               (getPid)
import OKA.Metadata
import OKA.Flow.Types
import OKA.Flow.Resources

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



-- | Save value as a JSON encoded with 'MetaEncoding' in @data.json@ file.
newtype AsMetaEncoded a = AsMetaEncoded a
  deriving FlowArgument via AsFlowOutput (AsMetaEncoded a)

instance MetaEncoding a => FlowOutput (AsMetaEncoded a) where
  writeOutput dir (AsMetaEncoded a) =
    BL.writeFile (dir </> "data.json") $ JSON.encode $ metaToJson a

instance MetaEncoding a => FlowInput (AsMetaEncoded a) where
  readOutput dir = do
    bs <- BL.readFile (dir </> "data.json")
    case JSON.decode bs of
      Nothing -> error "Invalid JSON"
      Just js -> case JSON.parseEither parseMeta js of
        Left  e -> error $ "Cannot decode PErr: " ++ show e
        Right x -> pure (AsMetaEncoded x)



----------------------------------------------------------------
-- Standard tools for writing executables
----------------------------------------------------------------

-- | Read metadata from stdin
metaFromStdin :: IsMeta a => IO a
metaFromStdin = do
  (BL.getContents <&> JSON.eitherDecode) >>= \case
    Left  e  -> error $ "Cannot read metadata: " ++ e
    Right js -> evaluate $ decodeMetadata js



type role ArgumentParser representational

-- | Monad for parsing command line arguments
newtype ArgumentParser a = ArgumentParser
  { get :: [FilePath] -> IO (Either String (a, [FilePath]))
  }
  deriving (Functor,Applicative,Monad,MonadIO,MonadError String,MonadState [FilePath])
       via StateT [FilePath] (ExceptT String IO)

-- | Type class for decoding arguments that are passed on command line
--   haskell data types.
class (ResultSet (AsRes a)) => FlowArgument a where
  -- | Representation of value of type @a@ as set of @Result@s. It's
  --   expected that when passed to workflow value of this type should
  --   parse successfully.
  type AsRes a
  -- | Parse value from arguments
  parserFlowArguments :: ArgumentParser a

runFlowArguments :: FlowArgument a => [FilePath] -> IO a
runFlowArguments paths = parserFlowArguments.get paths >>= \case
  Left  e      -> error $ "runFlowArguments: " ++ e
  Right (a,[]) -> pure a
  Right (_,_)  -> error "runFlowArguments: not all inputs are consumed"


-- | Parse command line arguments following flow's conventions: first
--   one is output directory rest are arguments.
runFlowArgumentsFromEnv :: FlowArgument a => IO (FilePath,a)
runFlowArgumentsFromEnv = getArgs >>= \case
  [] -> error "runFlowArgumentsFromEnv: No output directory provided"
  (out:paths) -> do a <- runFlowArguments paths
                    pure (out,a)

-- | Parse single input.
parseSingleArgument :: ArgumentParser FilePath
parseSingleArgument = do
  get >>= \case
    []   -> throwError "Not enough inputs"
    s:ss -> s <$ put ss

instance FlowArgument () where
  type AsRes () = ()
  parserFlowArguments = pure ()
instance (FlowArgument a, FlowArgument b) => FlowArgument (a,b) where
  type AsRes (a,b) = (AsRes a, AsRes b)
  parserFlowArguments = (,) <$> parserFlowArguments <*> parserFlowArguments
instance (FlowArgument a, FlowArgument b, FlowArgument c) => FlowArgument (a,b,c) where
  type AsRes (a,b,c) = (AsRes a, AsRes b, AsRes c)
  parserFlowArguments = (,,) <$> parserFlowArguments <*> parserFlowArguments <*> parserFlowArguments

-- | Derive instance of 'FlowArgument' for instance of 'FlowOutput'
newtype AsFlowOutput a = AsFlowOutput a

instance FlowInput a => FlowArgument (AsFlowOutput a) where
  type AsRes (AsFlowOutput a) = Result a
  parserFlowArguments = do
    path <- parseSingleArgument
    a    <- liftIO $ readOutput path
    pure $ AsFlowOutput a



----------------------------------------------------------------
-- GHC compilation
----------------------------------------------------------------

-- | Compile program with GHC. Uses 'LockGHC' to ensure that only one
--   compilation runs at a time.
compileProgramGHC
  :: FilePath    -- ^ Path to Main module
  -> [String]    -- ^ Options to pass to GHC
  -> ResourceSet -- ^ Set of resources
  -> IO ()
compileProgramGHC exe opts res
  = withResources res LockGHC
  $ withProcessWait_ ghc
  $ \_ -> pure ()
  where
    ghc = proc "ghc" (exe:opts)

-- | Default GHC options
defGhcOpts :: [String]
defGhcOpts = [ "-O2"
             , "-threaded"
             , "-with-rtsopts=-T -A8m"
             ]


----------------------------------------------------------------
-- Run external processes
----------------------------------------------------------------

-- | Run external process that adheres to standard calling
--   conventions: metadata is written to the stdin of a process and
--   parameters are passed as command line parameters.
runExternalProcess
  :: FilePath   -- ^ Path to executable
  -> Metadata   -- ^ Metadata to pass to process
  -> [FilePath] -- ^ Parameter list. When called as part of a workflow
                --   absolute paths will be passed.
  -> IO ()
runExternalProcess exe meta args = do
  withProcessWait_ run $ \pid -> do
    _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
    pure ()
  where
    run = setStdin (byteStringInput $ JSON.encode $ encodeMetadata meta)
        $ proc exe args

-- | Run external process that adheres to standard calling conventions
--   but don't pass metadata to it. Useful for phony targets.
runExternalProcessNoMeta
  :: FilePath   -- ^ Path to executable
  -> [FilePath] -- ^ Parameter list. When called as part of a workflow
                --   absolute paths will be passed.
  -> IO ()
runExternalProcessNoMeta exe args = do
  withProcessWait_ run $ \pid -> do
    _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
    pure ()
  where
    run = proc exe args

-- Kill process but allow it to die gracefully by sending SIGINT
-- first. GHC install handler for it but not for SIGTERM
softKill :: Process stdin stdout stderr -> IO ()
softKill p = getPid (unsafeProcessHandle p) >>= \case
  Nothing  -> pure ()
  Just pid -> do
    delay <- registerDelay 1_000_000
    signalProcess sigINT pid
    atomically $  (check =<< readTVar delay)
              <|> (void $ waitExitCodeSTM p)


-- | Pass metadata parameters in the environment. Metadata is written
--   in temporary file which is deleted when callback returns.
--
--   Path to JSON encoded metadata will be passed in @OKA_META@
--   variable, absolute paths to parameters will be passed in
--   @OKA_ARG_#@ variables.
withParametersInEnv
  :: Metadata                    -- ^ Metadata
  -> [FilePath]                  -- ^ Parameters
  -> ([(String,String)] -> IO a) -- ^ Callback
  -> IO a
withParametersInEnv meta param action = do
  withSystemTempFile "oka-flow-metadata-" $ \file_meta h -> do
    BL.hPutStr h $ JSON.encode $ encodeMetadata meta
    action $ ("OKA_META", file_meta)
           : [ ("OKA_ARG_" ++ show i, arg)
             | (i,arg) <- [1::Int ..] `zip` param
             ]
