{-# LANGUAGE ApplicativeDo    #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# LANGUAGE RecordWildCards  #-}
-- |
module Main (main) where

import Control.Monad
import Control.Lens         hiding (argument,set)
import Control.Exception
import Control.Monad.IO.Class
import Effectful
import Effectful.Writer.Static.Local
import Data.Aeson           qualified as JSON
import Data.ByteString.Lazy qualified as BL
import Data.Monoid      (Ap(..))
import Data.List        (isSuffixOf)
import Data.Foldable
import Data.Map.Strict  qualified as Map
import Data.Map.Strict  (Map)
import Data.Set         qualified as Set
import Options.Applicative
import System.Directory
import System.FilePath
import System.Exit

import OKA.Flow.Core.Types
import OKA.Flow.Core.S



main :: IO ()
main
  = join
  $ customExecParser (prefs showHelpOnError)
  $ (helper <*> parser)
    `info`
    (  fullDesc
    <> header   "Program for working with oka-flow store"
    <> progDesc
       "Manipulations and checking of oka-flow store"
    )


parser :: Parser (IO ())
parser = do
  store <- strOption ( long    "store"
                    <> short   's'
                    <> metavar "DIR"
                    <> help    "path to store (default .)"
                    <> value   ".")
  act <- subparser $ mconcat
    [ cmd "fsck"    parserFsck     "Perform consistency check for a store"
    , cmd "closure" parserClosure  "Compute closure of store path (all dependencies)"
    , cmd "revdeps" parserRevdeps  "Compute outputs that depend on given one"
    -- , cmd "prune"   cmdPrune       "Remove all paths with given prefix and all paths that depend on them"
    ]
  pure $ act store
  where
    cmd name fun hlp = command name ((helper <*> fun) `info` header hlp)


parserClosure :: Parser (FilePath -> IO ())
parserClosure = do
  path <- argument (maybeReader parseStorePath) (help "Store path" <> metavar "DIR")
  pure $ \store -> cmdClosure store path

parserRevdeps :: Parser (FilePath -> IO ())
parserRevdeps = do
  path <- argument (maybeReader parseStorePath) (help "Store path" <> metavar "DIR")
  pure $ \store -> cmdRevdeps store path

parserFsck :: Parser (FilePath -> IO ())
parserFsck = do
  shrt  <- switch (long "short" <> help "Write only store path that failed checks")
  prune <- switch (long "prune" <> help "Remove offending paths from storage")
  pure $ \store -> cmdFsck shrt prune store

parseStorePath :: String -> Maybe StorePath
parseStorePath path = do
  (nm,'/':hash_b16) <- Just $ span (/='/') path
  hash <- hashFromString $ filter (/='/') hash_b16
  Just $ StorePath nm hash


----------------------------------------------------------------
-- Command implementations
----------------------------------------------------------------

-- | Check store
cmdFsck :: Bool -> Bool -> FilePath -> IO ()
cmdFsck shrt prune store = do
  -- Read all store paths
  (paths_info, err) <- runEff $ runWriter @[Error] $ do
    paths <- readStoreEntries store
    Map.traverseWithKey (\p () -> readDependency store p) paths
  -- Print found errors
  traverse_ (putStrLn . pprError) err
  -- Cbeck for error
  let missingDependency p = case p `Map.lookup` paths_info of
        Nothing -> Just p
        Just _  -> Nothing
      check :: PathInfo -> [CheckFailure]
      check p = (p.deps ^.. _Just . traverse @S . to missingDependency . _Just . to MissingDependency)
             ++ (p.ext  ^.. _Just . traverse @[] . _1 . to missingDependency . _Just . to MissingExtMeta)
  let paths_info_checked
        = Map.filter (not . null)
        $ fmap check paths_info
  -- Find bad transitive dependencies
  let dep_graph    = fmap (\p -> (p.deps ^.. _Just . traverse)
                              ++ (p.ext  ^.. _Just . traverse . _1)
                   ) paths_info
  let failed_check = Map.keysSet paths_info_checked
      closure      = computeClosureSet (invertMap dep_graph) failed_check
                     `Set.difference`
                     failed_check
  let bad = Map.unionWith (<>)
            (Map.fromSet (\_ -> [BadDependency]) closure)
            (paths_info_checked)
  -- Report
  forM_ (Map.toList bad) $ \case
    (_, []  ) -> pure ()
    (p, errs) -> case shrt of
      True  -> putStrLn $ storePath p
      False -> do
        putStrLn $ "Check failure: " ++ storePath p
        forM_ errs $ \e -> putStrLn $ "  - " ++ pprCheckFailure e
  when (prune && not (null bad)) $ do
    putStrLn $ "=== WILL DELETE PATHS ABOVE (y/n) ==="
    responce <- getLine
    when (responce == "y" || responce == "yes") $ do
      forM_ (Map.keys bad) $ \p -> do
        removeDirectoryRecursive $ store </> storePath p
        putStrLn $ "Removed " ++ storePath p


-- | Compute closure (all dependencies)
cmdClosure :: FilePath -> StorePath -> IO ()
cmdClosure store path = do
  (paths_info, err) <- runEff $ runWriter @[Error] $ do
    paths <- readStoreEntries store
    Map.traverseWithKey (\p () -> readDependency store p) paths
  case err of
    [] -> pure ()
    _  -> putStrLn "Corrupted store" >> exitFailure
  --
  let dep_graph = fmap (\p -> (p.deps ^.. _Just . traverse)
                           ++ (p.ext  ^.. _Just . traverse . _1)
                ) paths_info
      closure = computeClosure dep_graph path
  forM_ closure (putStrLn . storePath)

cmdRevdeps :: FilePath -> StorePath -> IO ()
cmdRevdeps store path = do
  (paths_info, err) <- runEff $ runWriter @[Error] $ do
    paths <- readStoreEntries store
    Map.traverseWithKey (\p () -> readDependency store p) paths
  case err of
    [] -> pure ()
    _  -> putStrLn "Corrupted store" >> exitFailure
  --
  let dep_graph = fmap (\p -> (p.deps ^.. _Just . traverse)
                           ++ (p.ext  ^.. _Just . traverse . _1)
                ) paths_info
      closure = computeClosure (invertMap dep_graph) path
  forM_ closure (putStrLn . storePath)






----------------------------------------------------------------
-- Implementation
----------------------------------------------------------------

-- | Errors in store
data Error
  = FileInStore !FilePath                  -- ^ Store contains file where directory is expected
  | MissingDeps !StorePath                 -- ^ No list of dependencies
  | BadExtMeta  !StorePath !(Maybe String) -- ^ Unreadable external metadata
  | MissingMeta !StorePath                 -- ^ No Metadata
  | BadDeps     !StorePath                 -- ^ Cannot parse dependencies
  | BadMeta     !StorePath                 -- ^ Cannot parse metadata
  | BadHash     !FilePath !FilePath
  deriving Show

-- | Failures for otherwise well formed store outputs
data CheckFailure
  = MissingDependency !StorePath -- ^ Dependency is not in a store
  | MissingExtMeta    !StorePath -- ^ Linked external metadata is not present
  | BadDependency                -- ^ Transitively depends on broken output

pprError :: Error -> String
pprError = \case
  FileInStore path -> "File where directory expected: " ++ path
  MissingDeps path -> "Missing deps.json in:     " <> storePath path
  BadExtMeta  p e  -> "Bad external metadata in: " <> storePath p <> maybe "" (": "++) e
  MissingMeta path -> "Missing meta.json in:     " <> storePath path
  BadDeps     path -> "Cannot parse deps.txt in: " <> storePath path
  BadMeta     path -> "Cannot parse meta.json in:" <> storePath path
  BadHash     p h  -> "Cannot parse hash in:     " <> (p </> h)

pprCheckFailure :: CheckFailure -> String
pprCheckFailure = \case
  MissingDependency p -> "Missing dependency: " ++ storePath p
  MissingExtMeta    p -> "Missing external metadata: " ++ storePath p
  BadDependency       -> "Depends on broken output"


data PathInfo = PathInfo
  { path :: !StorePath
  , deps :: !(Maybe (S StorePath))
  , ext  :: !(Maybe ([(StorePath,[JSON.Key])]))
  }


-- | Get list of all entries in a store
readStoreEntries
  :: (Writer [Error] :> eff, IOE :> eff)
  => FilePath -> Eff eff (Map StorePath ())
readStoreEntries store = do
  flows <- listOnlyDirs ""
  flip foldMapM flows $ \flow -> do
    hashes0 <- listOnlyDirs flow
    let hashes = filter (not . isSuffixOf "-build") hashes0
    flip foldMapM hashes $ \hash_b16 -> do
      case hashFromString hash_b16 of
        Just hash -> pure $ Map.singleton (StorePath flow hash) ()
        Nothing   -> memptyErr $ BadHash flow hash_b16
  where
    listOnlyDirs path
      = filterM check =<< liftIO (listDirectory root )
      where
        root      = store</>path
        check dir = liftIO (doesDirectoryExist (root</>dir)) >>= \case
          False -> False <$ tell [FileInStore dir]
          True  -> pure True


-- | Read information about flow output.
readDependency
  :: (Writer [Error] :> eff, IOE :> eff)
  => FilePath -> StorePath -> Eff eff PathInfo
readDependency store path = do
  deps <- liftIO (try (BL.readFile path_dep)) >>= \case
    Left  (_ :: IOException) -> Nothing <$ tell [MissingDeps path]
    Right json -> case JSON.eitherDecode json of
      Left  _ -> Nothing <$ tell [BadDeps path]
      Right x -> pure $ Just x
  ext <- liftIO (doesFileExist path_ext) >>= \case
    False -> pure $ Just []
    True  -> liftIO (try (BL.readFile path_ext)) >>= \case
      Left  (_::IOException) -> Nothing <$ tell [BadExtMeta path Nothing]
      Right json -> case JSON.eitherDecode json of
        Left  e -> Nothing <$ tell [BadExtMeta path (Just e)]
        Right x -> pure $ Just x
  liftIO (doesFileExist path_meta) >>= \case
    True  -> pure ()
    False -> tell [MissingMeta path]
  pure PathInfo{..}
  where
    path_flow = store </> storePath path
    path_dep  = path_flow </> "deps.json"
    path_meta = path_flow </> "meta.json"
    path_ext  = path_flow </> "ext_meta.json"


----------------------------------------------------------------
-- Utils
----------------------------------------------------------------

computeClosure :: (Ord a) => Map a [a] -> a -> Set.Set a
computeClosure set = addToClosure set mempty

computeClosureSet :: (Foldable t, Ord a) => Map a [a] -> t a -> Set.Set a
computeClosureSet set = foldl' (addToClosure set) mempty

addToClosure :: (Ord a) => Map a [a] -> Set.Set a -> a -> Set.Set a
addToClosure set = go where
  go acc a
    | a `Set.member` acc = acc
    | otherwise          = case a `Map.lookup` set of
        Nothing -> acc'
        Just as -> foldl' go acc' as
    where acc' = Set.insert a acc

invertMap :: Ord a => Map a [a] -> Map a [a]
invertMap m = Map.fromListWith (<>) [ (b,[a]) | (a,bs) <- Map.toList m
                                              , b      <- bs
                                              ]


memptyErr
  :: (Writer [Error] :> eff, Monoid m)
  => Error -> Eff eff m
memptyErr err = mempty <$ tell [err]

foldMapM :: (Foldable t, Monoid m, Applicative f) => (a -> f m) -> t a -> f m
foldMapM f = getAp . foldMap (Ap . f)
