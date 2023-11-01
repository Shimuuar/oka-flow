{-# LANGUAGE FlexibleContexts #-}
-- |
-- Tools for defining concrete workflows.
module OKA.Flow.Tools
  ( -- * Executable implementation
    metaFromStdin
    -- * Command line arguments
  , FlowArgument(..)
  , parseFlowArguments
  , parseSingleArgument
    -- * GHC compilation
  , compileProgramGHC
  , LockGHC
  , newLockGHC
  , defGhcOpts
    -- * Rate limit
  , LockLMDB
  , newLockLMDB
  , rateLimitLMDB
    -- * External process
  , runExternalProcess
  , runExternalProcessNoMeta
  , runJupyter
  ) where

import Control.Applicative
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.State.Strict
import Data.Aeson                   qualified as JSON
import Data.ByteString.Lazy         qualified as BL
import Data.Functor
import Data.Generics.Product.Typed
import System.Process.Typed
import System.Directory
import System.FilePath              ((</>))
import System.IO.Temp
import System.Environment           (getEnvironment, getArgs)
import System.Posix.Signals         (signalProcess, sigINT)
import System.Process               (getPid)
import OKA.Metadata

----------------------------------------------------------------
-- Standard tools for writing executables
----------------------------------------------------------------

-- | Read metadata from stdin
metaFromStdin :: IsMeta a => IO a
metaFromStdin = do
  (BL.getContents <&> JSON.eitherDecode) >>= \case
    Left  e  -> error $ "Cannot read metadata: " ++ e
    Right js -> evaluate $ decodeMetadata js


-- | Type class for decoding arguments that are passed on command line
--   haskell data types.
class FlowArgument a where
  parserFlowArguments :: StateT [FilePath] (ExceptT String IO) a

-- | Parse command line arguments following flow's conventions: first
--   one is output directory rest are arguments.
parseFlowArguments :: FlowArgument a => IO (FilePath,a)
parseFlowArguments = getArgs >>= \case
  [] -> error "parseFlowArguments: No output directory provided"
  (out:paths) -> runExceptT (runStateT parserFlowArguments paths) >>= \case
    Left  e      -> error $ "parseFlowArguments: " ++ e
    Right (a,[]) -> pure (out,a)
    Right (_,_)  -> error "parseFlowArguments: not all inputs are consumed"

-- | Parse single input.
parseSingleArgument :: StateT [FilePath] (ExceptT String IO) FilePath
parseSingleArgument = do
  get >>= \case
    []   -> throwError "Not enough inputs"
    s:ss -> s <$ put ss

instance FlowArgument () where
  parserFlowArguments = pure ()
instance (FlowArgument a, FlowArgument b) => FlowArgument (a,b) where
  parserFlowArguments = (,) <$> parserFlowArguments <*> parserFlowArguments
instance (FlowArgument a, FlowArgument b, FlowArgument c) => FlowArgument (a,b,c) where
  parserFlowArguments = (,,) <$> parserFlowArguments <*> parserFlowArguments <*> parserFlowArguments


----------------------------------------------------------------
-- GHC compilation
----------------------------------------------------------------

-- | We want to have one concurrent build. Thus mutex
newtype LockGHC = LockGHC (MVar ())

-- | Create new lock
newLockGHC :: IO LockGHC
newLockGHC = LockGHC <$> newMVar ()

compileProgramGHC
  :: (HasType LockGHC res)
  => FilePath -- ^ Path to Main module
  -> [String] -- ^ Options to pass to GHC
  -> (res -> IO ())
compileProgramGHC exe opts res
  = withMVar lock $ \() -> withProcessWait_ ghc $ \_ -> pure ()
  where
    LockGHC lock = getTyped res
    ghc = proc "ghc" (exe:opts)

defGhcOpts :: [String]
defGhcOpts = [ "-O2"
             , "-threaded"
             , "-with-rtsopts=-T -A8m"
             ]

----------------------------------------------------------------
-- Limit number of programs using LMDB
----------------------------------------------------------------

-- | We want to restrict number of simultaneous programs which connect to DB
newtype LockLMDB = LockLMDB { get :: TVar Int }

newLockLMDB :: Int -> IO LockLMDB
newLockLMDB n = LockLMDB <$> newTVarIO n

rateLimitLMDB
  :: (HasType LockLMDB res)
  => res
  -> IO a
  -> IO a
rateLimitLMDB res io =
  (acquire >> io) `finally` release
  where
    lock = (getTyped @LockLMDB res).get
    acquire = atomically $ do n <- readTVar lock
                              when (n <= 0) retry
                              modifyTVar' lock (subtract 1)
    release = atomically $ do modifyTVar lock (+1)


----------------------------------------------------------------
-- Run external processes
----------------------------------------------------------------

-- | Run external process that adheres to standard calling conventions
runExternalProcess
  :: FilePath   -- ^ Path to executable
  -> Metadata   -- ^ Metadata to pass to process
  -> [FilePath] -- ^ Parameter list
  -> IO ()
runExternalProcess exe meta args = do
  withProcessWait_ run $ \pid -> do
    _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
    pure ()
  where
    run = setStdin (byteStringInput $ JSON.encode $ encodeMetadataDyn meta)
        $ proc exe args

-- | Run external process that adheres to standard calling conventions
runExternalProcessNoMeta
  :: FilePath   -- ^ Path to executable
  -> [FilePath] -- ^ Parameter list
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


-- | Run jupyter notebook as an external process
runJupyter
  :: FilePath   -- ^ Notebook name
  -> Metadata   -- ^ Metadata
  -> [FilePath] -- ^ Parameters
  -> IO ()
-- FIXME: We need mutex although not badly. No reason to run two
--        notebooks concurrently
runJupyter notebook meta param = do
  withSystemTempDirectory "oka-juju" $ \tmp -> do
    let dir_config  = tmp </> "config"
        dir_data    = tmp </> "data"
        file_meta   = tmp </> "meta.json"
    createDirectory dir_config
    createDirectory dir_data
    cwd <- getCurrentDirectory
    BL.writeFile file_meta $ JSON.encode $ encodeMetadataDyn meta
    --
    env <- getEnvironment
    let run = setEnv ( ("JUPYTER_DATA_DIR",   dir_data)
                     : ("JUPYTER_CONFIG_DIR", dir_config)
                     : ("OKA_META", file_meta)
                     : [ ("OKA_ARG_" ++ show i, cwd </> arg)
                       | (i,arg) <- [1::Int ..] `zip` param
                       ]
                     ++ env
                     )
            $ proc "jupyter" [ "notebook" , notebook
                             , "--browser", "chromium"
                             ]
    withProcessWait_ run $ \_ -> pure ()
