Qb (pronounced kyo͞ob) Queries
==============================

Overview
--------

Qb queries are JSON structures that declare set operations, list comprehensions and data transformations.   Qbic queries mimic much of SQL for the sake of familiarity, but include more powerful features like:

  * [Window functions](http://www.postgresql.org/docs/9.1/static/tutorial-window.html) (called [analytic functions](http://oracle-base.com/articles/misc/analytic-functions.php) in Oracle)
  * [Multidimensional Expressions](http://en.wikipedia.org/wiki/MultiDimensional_eXpressions)
  * Manipulating JSON using JSON path expressions (a basic version of [JsonPath](http://goessner.net/articles/JsonPath/) but consistent with URL conventions)
  * Simplified join syntax for hierarchical relations

Benefits
--------

  * Queries are trivially serialized over the wire
  * JSON has excellent support in both Javascript and Python
  * Easy to get started: Knowing SQL allows you to write Qb queries with minimal instruction

Qb is Not
-----------
   * A general graph query language - Qb was inspired by MDX, and has been optimized for hierarchical relations and data cubes, and is not succinct for manipulating many-to-many relations.

Motivation
----------

[Data cubes](http://en.wikipedia.org/wiki/OLAP_cube) facilitate strong typing of data volumes.  Their cartesian nature
makes counting and aggregation trivial, provably correct, operations. [MultiDimensional query eXpressions (MDX)](http://en.wikipedia.org/wiki/MultiDimensional_eXpressions)
takes advantage of the data cube uniformity to provide a simple query language to filter and group by.  Unfortunately,
MDX is too simple for general use, and requires copious up-front work to get the data in the cubic form required.

My experience with ETL has shown existing languages to be lacking:  Javascript, and procedural languages in general,
are not suited for general transformations because the logic is hidden in loops and handling edge case of those loops.
SQL has been my preferred ETL language because it can state many common data transformations simply, but [SQL has many
of it's own shortcomings](SQL Shortcomings.md)

I want to extend SQL with the good parts of MDX to provide a ETL data transformation language which will avoid common
ETL bugs.


Design
------

Generally, Qb queries are JSON structures meant to mimic the Abstract Syntax
Tree (AST) of SQL for the same.  It is hoped the similarity with SQL will
make it accessible to a wider audience.   The JSON nature is simply to avoid
making another language parser.

