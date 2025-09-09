-- |
module OKA.Flow.Core.Result where


----------------------------------------------------------------
-- Nodes in dataflow graph
----------------------------------------------------------------

-- | Internal identifier of dataflow function in a graph.
newtype FunID = FunID Int
  deriving (Show,Eq,Ord)

-- | Opaque handle to result of evaluation of single dataflow
--   function. It doesn't contain any real data and in fact is just a
--   promise to evaluate result.
newtype Result a = Result FunID
