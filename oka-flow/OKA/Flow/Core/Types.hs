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
  , Stdin(..)
  , Stdout(..)
  , CmdArg(..)
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
import Data.String
import Data.Traversable
import System.FilePath        ((</>),pathSeparator)
import System.Directory       (makeAbsolute)
import System.Environment     (getEnvironment)
import System.Process.Typed   (ProcessConfig,Process,proc,setStdin,setStdout,setWorkingDir,setEnv,
                               nullStream,byteStringInput,useHandleClose,withProcessWait_,waitExitCodeSTM,
                               getPid
                              )
import System.IO              (openFile,IOMode(..))
import System.Posix.Signals   (signalProcess, sigINT)

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

-- | What to do with stdin of subprocess
data Stdin
  = DevNullIn                 -- ^ Close @stdin@ of  subprocess
  | StdinBS    !BL.ByteString -- ^ Write lazy bytestring to stdin
  | StdinFileA !FilePath      -- ^ Read stdin from file. File path is
                              --   relative to workdir of dataflow manager
  | StdinFileO !FilePath      -- ^ Read stdin from file. File path is relative to
                              --   output directory.
  deriving stock (Show,Eq)

-- | What to do with stdout of subprocess
data Stdout
  = Inherit              -- ^ Inherit @stdout@
  | DevNullOut           -- ^ Close @std
  | StdoutFile !FilePath -- ^ Write stdout to file
  deriving stock (Show,Eq)

-- | Command line argument. We need to distinguish between relative
--   path and command line arguments. We need to convert former to
--   absolute paths since we don't know working dir of subprocess.
--   Note that paths from store are already absolute there's little
--   reason to represent them as @Path@.
data CmdArg
  = CmdArg !FilePath -- ^ Command line argument which is
  | PathA  !FilePath -- ^ Path relative to working directory of workflow manager.
  | PathO  !FilePath -- ^ Path relative to working directory of store
                     --   output of corresponding subprocess.
  deriving stock (Show,Eq)

instance IsString CmdArg where
  fromString = CmdArg


-- | Data for calling external process
data ProcessData = ProcessData
  { stdin   :: !Stdin            -- ^ Data to pass to stdin
  , stdout  :: !Stdout           -- ^ Stdout treatment
  , env     :: [(String,String)] -- ^ Data for putting into environment. Otherwise environment
                                 --   is inherited.
  , args    :: [CmdArg]          -- ^ Arguments for a process
  , workdir :: !(Maybe FilePath) -- ^ Working directory for subprocess.
  }


-- | Bracket for passing parameters to a subprocess
type CallingConv = forall a. (ParamFlow FilePath -> (ProcessData -> IO a) -> IO a)

-- | Convert dictionary into data structure for calling external
--   process using @typed-process@
toTypedProcess
  :: FilePath    -- ^ Executable name. If it contains path separators
                 --   it will be converted to absolute path relative
                 --   to current working dir.
  -> ProcessData -- ^ Parameters
  -> IO (ProcessConfig () () ())
toTypedProcess exe process = do
  -- Executable is searched relative to the working directory of
  -- process. We need to convert executable to absolute path when
  -- setting working dir. At the same we don't want to convert to
  -- absolute path executables from PATH.
  exe' <- if
    | pathSeparator `elem` exe -> makeAbsolute exe
    | otherwise                -> pure exe
  env <- case process.env of
    [] -> pure []
    es -> do env <- getEnvironment
             pure $ env ++ es
  --
  args <- for process.args $ \case
    CmdArg p -> pure p
    PathA  p -> makeAbsolute p
    PathO  p -> case process.workdir of
      Nothing -> error "Workdir for subprocess is not set. Path relative to output is not defined"
      Just _  -> pure p
  let set_stdin p = case process.stdin of
        DevNullIn       -> pure $ setStdin nullStream           p
        StdinBS    bs   -> pure $ setStdin (byteStringInput bs) p
        StdinFileA path -> do path' <- makeAbsolute path
                              h     <- openFile path' ReadMode
                              pure $ setStdin (useHandleClose h) p
        StdinFileO path -> do h <- openFile path ReadMode
                              pure $ setStdin (useHandleClose h) p
  let set_stdout p = case process.stdout of
        Inherit         -> pure p
        DevNullOut      -> pure $ setStdout nullStream p
        StdoutFile path -> case process.workdir of
          Nothing -> error "Workdir for subprocess is not set. Path relative to output is not defined"
          Just _  -> do h <- openFile path ReadMode
                        pure $ setStdout (useHandleClose h) p
  id $ set_stdout <=< set_stdin
     $ case process.workdir of
         Nothing -> id
         Just p  -> setWorkingDir p
     $ case env of
         [] -> id
         _  -> setEnv env
     $ proc exe' args

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
