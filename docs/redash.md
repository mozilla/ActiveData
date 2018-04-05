# ActiveData SQL and the Redash Connector

[sql.telemetry.mozilla.org](https://sql.telemetry.mozilla.org/) has a simple Redash connector that communicates with the [ActiveData SQL endpoint](https://activedata.allizom.org/sql). The endpoint does the work of parsing and executing the query sent to it.   

## SQL Flavour

ActiveData uses the [moz-sql-parser](https://github.com/mozilla/moz-sql-parser) to parse a MySQL flavour of SQL. 

### Table and Column Names

Since most tables and column names in ActiveData have non-word characters you must put them in double quotes to ensure they are valid:

    SELECT * FROM "coverage-summary"   

One exception is the dot (`.`), which is allowed in a name without quoting:

    SELECT * FROM task.action.timings  


### Sort order

ActiveData sorts according to data type first, and value second. Smallest type to largest type is

1. boolean (`false`, then `true`)
2. number
3. string (case-insensitive alphabetical order) 

`mull` (or missing) values are last no matter the order requested.       





