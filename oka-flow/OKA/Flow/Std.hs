{-# LANGUAGE DeriveAnyClass       #-}
{-# LANGUAGE MagicHash            #-}
{-# LANGUAGE UndecidableInstances #-}
-- |
-- Standard workflows
module OKA.Flow.Std
  ( -- * Saved metadata
    SavedMeta(..)
  , stdSaveMeta
  , stdSaveSomeMeta
  , narrowSavedMeta
    -- * Reports
  , ReportPDF
  , runPdfReader
  , stdConcatPDF
    -- * Jupyter
  , stdJupyter
  ) where

import Control.Concurrent         (threadDelay)
import Control.Exception
import Control.Monad.State.Strict
import Control.Monad.Trans.Maybe
import Data.Aeson                 qualified as JSON
import Data.Coerce
import Data.Maybe
import Data.List                  (stripPrefix,isPrefixOf,isSuffixOf)
import Data.Set                   qualified as Set
import Data.ByteString.Lazy       qualified as BL
import Data.Void
import Effectful                  ((:>))
import System.FilePath            ((</>), splitFileName,splitPath,joinPath)
import System.Directory           (createFileLink,createDirectory,canonicalizePath,listDirectory)
import System.Process.Typed
import System.IO.Temp
import GHC.Generics
import GHC.Exts                   (proxy#)

import OKA.Flow.Tools
import OKA.Flow.Core.Graph
import OKA.Flow.Core.Flow
import OKA.Flow.Core.S
import OKA.Flow.Core.Types
import OKA.Flow.Eff
import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Internal.Util


----------------------------------------------------------------
-- Saved metadata
----------------------------------------------------------------

-- | This data type represents metadata saved as output of a flow.
--   Output is stored in @saved.json@ file and uses standard metadata
--   encoding. @saved.json@ instead of @meta.json@ (which is written
--   as well) is used in order to allow other flows to generate
--   compatible outputs.
data SavedMeta a = SavedMeta a
  deriving FlowArgument via AsFlowInput (SavedMeta a)

instance (IsMeta a) => FlowInput (SavedMeta a) where
  readOutput path = SavedMeta <$> readMetadata (path </> "saved.json")

instance (IsMeta a) => FlowOutput (SavedMeta a) where
  writeOutput path (SavedMeta a)
    = BL.writeFile (path </> "saved.json")
    $ JSON.encode $ encodeToMetadata a

-- | Save metadata value so it could be passed as parameter.
stdSaveMeta :: (IsMeta a) => a -> Flow eff (Result (SavedMeta a))
stdSaveMeta meta = scopeMeta $ do
  put $ toMetadata meta
  liftHaskellFun "std.SavedMeta" ()
    (\(a::a) () -> pure $ SavedMeta a)
    ()

-- | Save metadata value so it could be passed as parameter.
stdSaveSomeMeta :: Metadata -> Flow eff (Result (SavedMeta Metadata))
stdSaveSomeMeta meta = scopeMeta $ do
  put $ absurd <$> meta
  liftHaskellFunMeta_ "std.SavedMeta" ()
    (\out _ () -> createFileLink "meta.json" (out </> "saved.json"))
    ()

-- | Convert one saved metadata to another which possibly uses less
--   data.
narrowSavedMeta
  :: forall a b. (IsMeta a, IsMeta b)
  => Result (SavedMeta a)
  -> Result (SavedMeta b)
narrowSavedMeta r
  | keysB `Set.isSubsetOf` keysA = coerce r
  | otherwise = error $ "Cannot narrow saved meta " ++ typeName @a ++ " to " ++ typeName @b
  where
    keysA = metadataKeySet (proxy# @a)
    keysB = metadataKeySet (proxy# @b)

----------------------------------------------------------------
-- PDF reports
----------------------------------------------------------------

-- | Type tag for outputs which contains @report.pdf@
data ReportPDF


-- | Run PDF viewer as phony workflow. Reader is picked from runtime
--   configuration.
runPdfReader :: (SequenceOf ReportPDF a, ProgConfigE :> eff) => a -> Flow eff ()
runPdfReader a = do 
  pdf <- fromMaybe "xdg-open" . (.pdf) <$> askProgConfig
  liftPhonyExecutable pdf ()
    (callViaArgList $ \args -> [p </> "report.pdf" | p <- args])
    (sequenceOf @ReportPDF a)

-- | Concatenate PDFs using @pdftk@ program
stdConcatPDF :: SequenceOf ReportPDF a => a -> Flow eff (Result ReportPDF)
stdConcatPDF reports = restrictMeta @() $ do
  liftExecutable "std.pdftk.concat" "pdftk" ()
    (callViaArgList $ \args ->
        [a</>"report.pdf" | a <- args] ++ ["cat", "output", "report.pdf"]
        )
    (sequenceOf @ReportPDF reports)



----------------------------------------------------------------
-- Jupyter
----------------------------------------------------------------


-- | Run jupyter notebook. Metadata and parameters are passed in
--   environment variables.
stdJupyter
  :: (ToS p, ProgConfigE :> eff)
  => [FilePath] -- ^ Notebooks' names
  -> p          -- ^ Parameters to pass to notebook.
  -> Flow eff ()
-- NOTE: We want to be able to start multiple notebooks at the same
--       time. Jupyter doesn't seem to support that natively. We have
--       to jump through quite a few of flaming hoops:
--
--       First we start notebook and wait until it creates JSON file
--       with server description, read it and then use parameters in
--       it to start browser.
--
-- FIXME: We need mutex although not badly. No reason to run two
--        notebooks concurrently
stdJupyter notebooks params = do
  cfg <- askProgConfig
  let browser = case cfg.browser of
        Nothing -> "chromium"
        Just b  -> b
  basicLiftPhony ()
    (ActionIO $ HaskellIO $ \_ param -> do
      -- Figure out notebook directory. We use common prefix as
      -- heuristic.
      --
      -- NOTE: this code relies on directory names with trailing slash.
      cfg_dir       <- fmap (++"/") <$> traverse canonicalizePath cfg.jupyterNotebookDir
      notebooks_abs <- traverse canonicalizePath notebooks
      let notebook_dir = case commonPrefix $ splitPath . fst . splitFileName <$> notebooks_abs of
            [] -> fromMaybe "." cfg_dir
            s  -> case cfg_dir of
              Nothing -> dir
              Just p | p `isPrefixOf` dir -> p
                     | otherwise          -> dir
              where dir = joinPath s
      -- Notebook names relative to notebook directory
      let notebooks_rel = case traverse (stripPrefix notebook_dir) notebooks_abs of
            Just s  -> s
            Nothing -> error "Error during processing notebooks"
      callInEnvironment param $ \proc_jupyter -> do
        withSystemTempDirectory "oka-flow-jupyter-" $ \tmp -> do
          let dir_config  = tmp </> "config"
              dir_data    = tmp </> "data"
          createDirectory dir_config
          createDirectory dir_data
          run_jupyter <- toTypedProcess "jupyter"
            proc_jupyter{ env  = [ ("JUPYTER_DATA_DIR",   dir_data)
                                 , ("JUPYTER_CONFIG_DIR", dir_config)
                                 ] ++ proc_jupyter.env
                        , args = [ "notebook"
                                 , "--no-browser"
                                 , "--notebook-dir=" ++ notebook_dir
                                 ]
                        }
          withProcessWait_ run_jupyter $ \_ -> do
            -- Wait until server starts and launch browser.
            jp <- waitForJupyter dir_data >>= \case
              Nothing -> error "stdJupyter: can't find server config"
              Just a  -> pure a
            let nb_url = [ jp.url ++ "notebooks/"++path++"?token="++jp.token
                         | path <- notebooks_rel
                         ]
                tree_url | cfg.jupyterBrowser = ((jp.url++"tree/"++"?token="++jp.token):)
                         | otherwise          = id
            _ <- startProcess (proc browser (tree_url nb_url))
            pure ()
    ) params


commonPrefix :: forall a. Eq a => [[a]] -> [a]
commonPrefix []     = []
commonPrefix (x:xs) = go x xs where
  go []     _     = []
  go (a:as) other = case traverse (sameHead a) other of
    Nothing     -> []
    Just other' -> a : go as other'

  sameHead y (a:as) | y==a = Just as
  sameHead _ _             = Nothing


-- We use curl to check availability of URL. If there's no curl we simply won't wait.
-- I don't want to add network
waitForJupyter :: FilePath -> IO (Maybe JupyterConfig)
waitForJupyter datadir = backoff delays wait where
  delays = takeWhile (<5_000_000) $ iterate (*2) 100_000
  wait   = runMaybeT $ do
    cfg <- MaybeT $ findJupyterConfig datadir
    MaybeT $ readJupyterConfig cfg

backoff :: [Int] -> IO (Maybe a) -> IO (Maybe a)
backoff delays action = go delays where
  go [] = pure Nothing
  go (t:ts) = threadDelay t >> action >>= \case
    Nothing  -> go ts
    a@Just{} -> pure a

findJupyterConfig :: FilePath -> IO (Maybe FilePath)
findJupyterConfig datadir = ignoreIOException $ do
  let runtime_dir = datadir </> "runtime"
  paths <- listDirectory runtime_dir
  let cfg = [ nm
            | nm <- paths
            , "jpserver" `isPrefixOf` nm
            , ".json"    `isSuffixOf` nm
            ]
  case cfg of
    []   -> pure Nothing
    [nm] -> pure $ Just (runtime_dir </> nm)
    _    -> error "stdJupyter: something wrong with jupyter. Multiple configs"

readJupyterConfig :: FilePath -> IO (Maybe JupyterConfig)
readJupyterConfig path = ignoreIOException $ do
  -- We may read file only partially due to races. So if we fail to
  -- decode JSON it should be OK. But failure to parse valid JSON is
  -- a problem
  bs <- BL.readFile path
  case JSON.decode bs of
    Nothing -> pure Nothing
    Just js -> case JSON.fromJSON js of
      JSON.Error   e -> error $ "stdJupyter: Cannot parse jpserver....json: " ++ e
      JSON.Success c -> pure (Just c)


ignoreIOException :: IO (Maybe a) -> IO (Maybe a)
ignoreIOException = handle (\(_::IOException) -> pure Nothing)

-- | Parts of Jupyter server config generated on server start.
--   We only read parts we're interested in
data JupyterConfig = JupyterConfig
  { token :: FilePath
  , url   :: FilePath
  }
  deriving stock (Show,Generic)
  deriving anyclass (JSON.FromJSON)

