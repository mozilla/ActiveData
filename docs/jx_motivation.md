# JSON Query Expressions

A solution to querying a data lake

## Problem


* ETL effort
* Understand data shape
* declaring schema
* defining relations
* scrubbing data to conform

## Solution

We let the datastore accept any JSON, and we let it manage the schema.



for the variety of JSON that enters it. Nested object arrays result in new relations, managed.  

The query language treats property names as independent dimensions; with nested object arrays as subspaces.  


 Only at query time does the data need to be understood, and then only at a superficial level.

