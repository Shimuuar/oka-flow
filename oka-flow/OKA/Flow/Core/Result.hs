-- |
module OKA.Flow.Core.Result where


----------------------------------------------------------------
-- Nodes in dataflow graph
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving stock (Show,Eq,Ord)


newtype AResult = AResult FunID
  deriving stock (Show,Eq,Ord)
newtype APhony  = APhony  FunID
  deriving stock (Show,Eq,Ord)


-- | Opaque handle to result of evaluation of single dataflow
--   function. It doesn't contain any real data and in fact is just a
--   promise to evaluate result.
newtype Result a = Result AResult

-- | Opaque handle to result of evaluation of single phony dataflow.
--   This is dataflow which doesn't produce any output.
newtype Phony a = Phony APhony 
