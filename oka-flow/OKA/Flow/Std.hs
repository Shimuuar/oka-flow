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
  , CollectReports(..)
  , runPdfReader
  , stdConcatPDF
    -- * Jupyter
  , stdJupyter
  ) where

import Control.Monad.State.Strict
import Data.Coerce
import Data.Maybe
import Data.Set                   qualified as Set
import Data.Void
import Effectful                  ((:>))
import System.FilePath            ((</>))
import System.Directory           (createFileLink,createDirectory)
import System.Process.Typed
import System.IO.Temp
import System.Environment         (getEnvironment)
import GHC.Generics
import GHC.Exts                   (proxy#)

import OKA.Flow.Tools
import OKA.Flow.Parser
import OKA.Flow.Graph
import OKA.Flow.Types
import OKA.Flow.Eff
import OKA.Metadata
import OKA.Metadata.Meta
import OKA.Flow.Util

----------------------------------------------------------------
-- Saved metadata
----------------------------------------------------------------

-- | This data type represents metadata saved as output of a flow.
--   Output is stored in @saved.json@ file and uses standard metadata
--   encoding. @saved.json@ instead of @meta.json@ (which is written
--   as well) is used in order to allow other flows to generate
--   compatible outputs.
data SavedMeta a = SavedMeta a
  deriving FlowArgument via AsFlowOutput (SavedMeta a)

instance (IsMeta a) => FlowInput (SavedMeta a) where
  readOutput path = SavedMeta <$> readMetadata (path </> "saved.json")

-- | Save metadata value so it could be passed as parameter.
stdSaveMeta :: (IsMeta a) => a -> Flow eff (Result (SavedMeta a))
stdSaveMeta a = scopeMeta $ do
  put $ toMetadata a
  liftWorkflow () Action
    { name = "std.SavedMeta"
    , run  = \_ _meta args out -> do
        case args of [] -> pure ()
                     _  -> error "stdSaveMeta does not take any arguments"
        createFileLink "meta.json" (out </> "saved.json")
    } ()

-- | Save metadata value so it could be passed as parameter.
stdSaveSomeMeta :: Metadata -> Flow eff (Result (SavedMeta Metadata))
stdSaveSomeMeta meta = scopeMeta $ do
  put $ absurd <$> meta
  liftWorkflow () Action
    { name = "std.SavedMeta"
    , run  = \_ _meta args out -> do
        case args of [] -> pure ()
                     _  -> error "stdSaveMeta does not take any arguments"
        createFileLink "meta.json" (out </> "saved.json")
    } ()

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
-- Report PDF
----------------------------------------------------------------

-- | Type tag for outputs which contains @report.pdf@
data ReportPDF

-- | Convenience type class for collecting list of reports from tuples
--   and lists of parameters.
class CollectReports a where
  collectReports :: a -> [Result ReportPDF]

instance CollectReports (Result ReportPDF) where
  collectReports p = [p]
deriving via Generically (a,b)
    instance (CollectReports a, CollectReports b) => CollectReports (a,b)
deriving via Generically (a,b,c)
    instance (CollectReports a, CollectReports b, CollectReports c) => CollectReports (a,b,c)
deriving via Generically (a,b,c,d)
    instance (CollectReports a, CollectReports b, CollectReports c, CollectReports d
             ) => CollectReports (a,b,c,d)
deriving via Generically (a,b,c,d,e)
    instance ( CollectReports a, CollectReports b, CollectReports c, CollectReports d
             , CollectReports e
             ) => CollectReports (a,b,c,d,e)
deriving via Generically (a,b,c,d,e,f)
    instance ( CollectReports a, CollectReports b, CollectReports c, CollectReports d
             , CollectReports e, CollectReports f
             ) => CollectReports (a,b,c,d,e,f)
deriving via Generically (a,b,c,d,e,f,g)
    instance ( CollectReports a, CollectReports b, CollectReports c, CollectReports d
             , CollectReports e, CollectReports f, CollectReports g
             ) => CollectReports (a,b,c,d,e,f,g)
deriving via Generically (a,b,c,d,e,f,g,h)
    instance ( CollectReports a, CollectReports b, CollectReports c, CollectReports d
             , CollectReports e, CollectReports f, CollectReports g, CollectReports h
             ) => CollectReports (a,b,c,d,e,f,g,h)


instance (CollectReports a) => CollectReports [a] where
  collectReports = concatMap collectReports

-- | This instance could be used to derive CollectReports instance
--   with @DerivingVia@
instance (Generic a, GCollectReports (Rep a)) => CollectReports (Generically a) where
  collectReports (Generically a) = gcollectReports (from a)


class GCollectReports f where
  gcollectReports :: f p -> [Result ReportPDF]
  
deriving newtype instance GCollectReports f => GCollectReports (M1 c i f)

instance (GCollectReports f, GCollectReports g) => GCollectReports (f :*: g) where
  gcollectReports (f :*: g) = gcollectReports f <> gcollectReports g

instance (CollectReports a) => GCollectReports (K1 i a) where
  gcollectReports = coerce (collectReports @a)


-- | Run PDF viewer as phony workflow. Reader is picked from runtime
--   configuration.
runPdfReader :: (CollectReports a, ProgConfigE :> eff) => a -> Flow eff ()
runPdfReader a = do
  pdf <- fromMaybe "xdg-open" . (.pdf) <$> askProgConfig
  basicLiftPhony ()
    (\_ _ paths -> runExternalProcessNoMeta pdf [p </> "report.pdf" | p <- paths])
    (collectReports a)


-- | Concatenate PDFs using @pdftk@ program
stdConcatPDF :: CollectReports a => a -> Flow eff (Result ReportPDF)
stdConcatPDF reports = restrictMeta @() $ do
  liftWorkflow () Action
    { name = "std.pdftk.concat"
    , run  = \_res _meta args out -> do
        runExternalProcessNoMeta "pdftk"
          ([a</>"report.pdf" | a <- args] ++ ["cat", "output", out</>"report.pdf"])
    } (collectReports reports)


----------------------------------------------------------------
-- Jupyter
----------------------------------------------------------------

-- | Run jupyter notebook. Metadata and parameters are passed in
--   environment variables.
stdJupyter
  :: (ResultSet p, ProgConfigE :> eff)
  => [FilePath] -- ^ Notebooks' names
  -> p          -- ^ Parameters to pass to notebook.
  -> Flow eff ()
-- FIXME: We need mutex although not badly. No reason to run two
--        notebooks concurrently
stdJupyter notebooks params = do
  cfg <- askProgConfig
  let browser = case cfg.browser of
        Nothing -> []
        Just b  -> ["--browser", b]
  --
  basicLiftPhony ()
    (\_ meta param -> do
      withParametersInEnv meta param $ \env_param -> do
        withSystemTempDirectory "oka-flow-jupyter-" $ \tmp -> do
          let dir_config  = tmp </> "config"
              dir_data    = tmp </> "data"
          createDirectory dir_config
          createDirectory dir_data
          env <- getEnvironment
          let run = setEnv ([ ("JUPYTER_DATA_DIR",   dir_data)
                            , ("JUPYTER_CONFIG_DIR", dir_config)
                            ] ++ env_param ++ env)
                  $ proc "jupyter" (("notebook" : notebooks) <> browser)
          withProcessWait_ run $ \_ -> pure ()
    ) params
