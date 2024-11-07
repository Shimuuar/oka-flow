{-# LANGUAGE ApplicativeDo #-}
-- |
module Main (main) where

import Control.Monad
import Control.Exception
import Control.Monad.IO.Class
import Control.Monad.Trans.Writer.CPS
import Data.Traversable
import Data.Foldable
import Data.Map.Strict  qualified as Map
import Data.Map.Strict  (Map)
import Data.Set         qualified as Set
import Options.Applicative
import System.Directory
import System.FilePath
import System.IO

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
    [ cmd "fsck"    (pure cmdFsck) "Perform consistency check for a store"
    , cmd "closure" cmdClosure     "Compute closure of store path"
    , cmd "revdep"  cmdRevdeps     "Compute all dependencies of a path"
    , cmd "prune"   cmdPrune       "Remove all paths with given prefix and all paths that depend on them"
    ]
  pure $ act store
  where
    cmd name fun hlp = command name ((helper <*> fun) `info` header hlp)


----------------------------------------------------------------
-- Command implementations
----------------------------------------------------------------

-- | Check store
cmdFsck :: FilePath -> IO ()
cmdFsck store = do
  -- Read all store paths
  (paths,err) <- runWriterT $ readStoreEntries store
  paths_dep   <- Map.traverseWithKey (\p _ -> readDependency store p) paths
  -- Print found errors
  forM_ err (putStrLn . pprError)
  forM_ paths_dep $ \case
    Left  e -> putStrLn (pprError e)
    Right _ -> pure ()

-- | Compute closure (all dependencies)
cmdClosure :: Parser (FilePath -> IO ())
cmdClosure = do
  path <- argument (maybeReader pathToPath) (help "Store path" <> metavar "DIR")
  pure $ \store -> do
    (paths0, err) <- runWriterT $ readStoreEntries store
    paths1 <- fmap sequence $ Map.traverseWithKey (\p _ -> readDependency store p) paths0
    for_ err (hPutStrLn stderr . pprError)
    case paths1 of
      Left  e     -> error $ show e
      Right paths -> forM_ (computeClosure paths path) (putStrLn . pprPath)

cmdRevdeps :: Parser (FilePath -> IO ())
cmdRevdeps = do
  path <- argument (maybeReader pathToPath) (help "Store path" <> metavar "DIR")
  pure $ \store -> do
    (paths0, err) <- runWriterT $ readStoreEntries store
    paths1 <- fmap sequence $ Map.traverseWithKey (\p _ -> readDependency store p) paths0
    for_ err (hPutStrLn stderr . pprError)
    case paths1 of
      Left  e     -> error $ show e
      Right paths -> forM_ (computeClosure (invertMap paths) path) (putStrLn . pprPath)

cmdPrune :: Parser (FilePath -> IO ())
cmdPrune = do
  name <- strArgument (metavar "DIR" <> help "Path prefix to remove")
  yes  <- switch (short 'y' <> help "Really delete store paths")
  pure $ \store -> do
    when ('/' `elem` name) $ error "Only single path is OK"
    -- Load paths
    (paths0,_) <- runWriterT $ readStoreEntries store
    paths1     <- fmap sequence $ Map.traverseWithKey (\p _ -> readDependency store p) paths0
    pathset <- case paths1 of
      Left  e -> error $ show e
      Right p -> pure p
    -- Compute reverse closure
    hashes <- listDirectory (store </> name) 
    let paths   = Path name <$> hashes
        revdeps = foldl' (addToClosure (invertMap pathset)) mempty paths
    --
    let prefix = case yes of
          True  -> "Deleting: "
          False -> "Dry-run: "
    forM_ revdeps $ \p@(Path nm h) -> do
      putStrLn $ prefix ++ pprPath p
      when yes $ do
        removeDirectoryRecursive (store </> nm </> h)
        


computeClosure :: (Ord a) => Map a [a] -> a -> Set.Set a
computeClosure set = addToClosure set mempty 

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
  



----------------------------------------------------------------
-- Implementation
----------------------------------------------------------------

-- | Errors in store 
data Error
  = FileInStore FilePath  -- ^ Store contains file where directory is expected
  | MissingDeps Path      -- ^ No list of dependencies
  | MissingMeta Path      -- ^ No Metadata
  | BadDeps     Path      -- ^ Cannot parse dependencies
  | BadMeta     Path      -- ^ Cannot parse metadata
  deriving Show

pprError :: Error -> String
pprError = \case
  FileInStore path -> "File where directory expected: " ++ path
  MissingDeps path -> "Missing deps.txt in:      " <> pprPath path
  MissingMeta path -> "Missing meta.json in:     " <> pprPath path
  BadDeps     path -> "Cannot parse deps.txt in: " <> pprPath path
  BadMeta     path -> "Cannot parse meta.json in:" <> pprPath path

pprPath :: Path -> String
pprPath (Path nm hash) = nm </> hash

-- | Path in store
data Path = Path FilePath FilePath
  deriving (Show,Eq,Ord)


-- | Get list of all entries in a store
readStoreEntries :: FilePath -> WriterT [Error] IO (Map Path ())
readStoreEntries store = do
  store_names <- liftIO $ listDirectory store
  fmap mconcat $ for store_names $ \nm -> do
    let path_nm = store </> nm
    liftIO (doesDirectoryExist path_nm) >>= \case
      False -> mempty <$ tell [FileInStore nm]
      True  -> do
        hashes <- liftIO (listDirectory path_nm)
        fmap mconcat $ for hashes $ \hash -> do
          liftIO (doesDirectoryExist (path_nm </> hash)) >>= \case
            False -> mempty <$ tell [FileInStore (nm </> hash)]
            True  -> pure $ Map.singleton (Path nm hash) ()


-- | Read list of dependencies 
readDependency :: FilePath -> Path -> IO (Either Error [Path])
readDependency store path@(Path nm hash) = do
  try (readFile (store </> nm </> hash </> "deps.txt")) >>= \case
    Left  (_ :: IOException) -> pure $ Left $ MissingDeps path
    Right deps -> case traverse pathToPath $ lines deps of
      Nothing -> pure $ Left $ BadDeps path
      Just d  -> pure $ Right d


pathToPath :: FilePath -> Maybe Path
pathToPath = \case
                [a,b] -> Just (Path a b)
                _     -> Nothing
           . map (filter (/='/'))
           . splitPath
