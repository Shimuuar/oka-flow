-- |
-- Basic data types sued for definition of dataflow graph.
module OKA.Flow.Core.Types
  ( -- * Dataflow graph handles
    Result(..)
  , Phony(..)
  , AResult
  , APhony
    -- * Store path
  , Hash(..)
  , StorePath(..)
  , storePath
    -- * Flow parameters
  , ParamFlow(..)
  , ProcessData(..)
  , CallingConv
  , toTypedProcess
  , startSubprocessAndWait
  ) where

import Control.Applicative
import Control.Monad
import Control.Exception
import Control.Concurrent.STM
import Data.ByteString        (ByteString)
import Data.ByteString.Char8  qualified as BC8
import Data.ByteString.Lazy   qualified as BL
import Data.ByteString.Base16 qualified as Base16
import System.FilePath        ((</>))
import System.Environment     (getEnvironment)
import System.Process.Typed
import System.Posix.Signals         (signalProcess, sigINT)

import OKA.Metadata
import OKA.Flow.Core.S
import OKA.Flow.Core.Result



----------------------------------------------------------------
-- Paths in store
----------------------------------------------------------------

-- | SHA1 hash
newtype Hash = Hash ByteString
  deriving newtype (Eq,Ord)

instance Show Hash where
  show (Hash hash) = show $ BC8.unpack $ Base16.encode hash

-- | Path in nix-like storage.
data StorePath = StorePath
  { name :: String
  , hash :: Hash
  }
  deriving (Show)

-- | Compute file name of directory in nix-like store.
storePath :: StorePath -> FilePath
storePath (StorePath nm (Hash hash)) = nm </> BC8.unpack (Base16.encode hash)

----------------------------------------------------------------
-- Calling Flows
----------------------------------------------------------------

-- | Parameters for a standard flow
data ParamFlow a = ParamFlow
  { meta :: Metadata
  , args :: S a
  , out  :: Maybe a
  }
  deriving stock (Functor,Foldable,Traversable)

-- | Data for calling external process
data ProcessData = ProcessData
  { stdin   :: !(Maybe BL.ByteString) -- ^ Data to pass stdin
  , env     :: [(String,String)]      -- ^ Data for putting into environment. Otherwise environment
                                      --   is inherited.
  , args    :: [String]               -- ^ Arguments for a process
  , workdir :: !(Maybe FilePath)      -- ^ Working directory for subprocess.
  }


-- | Bracket for passing parameters to a subprocess
type CallingConv = forall a. (ParamFlow FilePath -> (ProcessData -> IO a) -> IO a)

-- | Convert dictionary into data structure for calling external
--   process using @typed-process@
toTypedProcess
  :: FilePath    -- ^ Executable name
  -> ProcessData -- ^ Parameters
  -> IO (ProcessConfig () () ())
toTypedProcess exe process = do
  env <- case process.env of
    [] -> pure []
    es -> do env <- getEnvironment
             pure $ env ++ es
  pure $ case process.workdir of
           Nothing -> id
           Just p  -> setWorkingDir p
       $ case process.stdin of
           Nothing -> id
           Just bs -> setStdin (byteStringInput bs)
       $ case env of
           [] -> id
           _  -> setEnv env
       $ proc exe process.args

-- | Create subprocess and wait for its completions. If execution is
--   interrupted subprocess is killed. If process exits with nonzero
--   exit code exception is raised.
startSubprocessAndWait
  :: FilePath           -- ^ Executable name
  -> CallingConv        -- ^ Calling conventions
  -> ParamFlow FilePath -- ^ Parameters
  -> IO ()
startSubprocessAndWait exe call param = 
  call param $ \process -> do
    run <- toTypedProcess exe process
    withProcessWait_ run $ \pid -> do
      _ <- atomically (waitExitCodeSTM pid) `onException` softKill pid
      pure ()

-- Kill process but allow it to die gracefully by sending SIGINT
-- first. GHC install handler for it but not for SIGTERM
softKill :: Process stdin stdout stderr -> IO ()
softKill p = getPid p >>= \case
  Nothing  -> pure ()
  Just pid -> do
    delay <- registerDelay 1_000_000
    signalProcess sigINT pid
    atomically $  (check =<< readTVar delay)
              <|> (void $ waitExitCodeSTM p)
