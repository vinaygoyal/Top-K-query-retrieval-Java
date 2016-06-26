# Top-K-query-retrieval-Java
Implement Top-K queries over a relation Table of arity N + 1 using Java

The program computes the set of rows of Table, defined by the following query:

SELECT *
FROM Table
ORDER BY FV (A1...AN) DESC
LIMIT K

The program computes the results in two different ways. It should be invoked as:

(1)
%java topk K N
topk>init tfile
topk>run1 v1 v2 ... vN

(2)
%java topk K N
topk>init tfile
topk>run2 v1 v2 ... vN

where tfile is the name of the CSV file containing the contents of Table.
