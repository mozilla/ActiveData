

Cubes, Data Frames, and Pivot Tables
------------------------------------

The `groupby` clause introduces some problems for data analysis.  The biggest problem is it's unaware of the cardinality of the columns (dimensions) of the data.  Filtering a result-set necessarily impacts the number of rows returned, which does not makes sense for aggregates; one expects filtering to affect the statistics, not the number of statistics.

