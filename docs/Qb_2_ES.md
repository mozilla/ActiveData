Facets, Edges, GroupBy and Joins
--------------------------------
ES facets are simple group-by operations; they are single dimension without allowing group-by on multiple attributes, and only aggregating attributes from the root document.
The ES facet is distinctly different from an edge
A facet collapses all but one dimension, it can not handle more than one dimension
Edges are a convenient mix of join and group-by.  Technically edges are an outer join (http://en.wikipedia.org/wiki/Relational_algebra#Full_outer_join) combined with an aggregate.   The motivation behind outer joins is to ensure both sets are covered in the result.  Furthermore, aggregates are limited to aggregating records once and only once.
There are cases, when dealing with normalized data, and in ETL situations, where counting a record more than once, or not at all, is preferred.  In this case, use the test property along with allowNulls=false to get the effect of an inner join.