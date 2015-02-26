
Dimension Definitions
---------------------

To simplify query writing, we borrow from the [Business Intelligence](http://en.wikipedia.org/wiki/Business_intelligence)
field and define a type hierarchy over the various fields and measures found
in the cube.  Beyond query simplification, we also get minor ETL, a central
schema definition, improved formatting, and organization.

A dimension can consists of child edges or child partitions.  Child edges are
allowed to overlap each others domains, and represent orthogonal ways to cut
the same data.  Child partitions divide the dimension into mutually exclusive
parts.  A well-behaved partition will cover the dimension's domain,
ill-behaved partitions are "compiled" to be well-behaved so nothing is missed.

Dimensions are similar to domain clauses in Qb queries, with additional
attributes to help apply that domain over many different queries.

  - **name** - humane words to describe the child dimension
  - **field** - full path name of a field (eg ```"field":"test_build.branch"```) in the json record.  This will be used as the value in the query's edge.  You may also provide an array of fields (eg ```"field":["test_machine.os", "test_machine.osversion"]```) which will define a unique tuple for each domain part (see Hierarchical Partitions, below).
  - **value** - function to convert a part object into a humane string for final presentation.  **Default:** ```function(v){return v.name;}```
  - **format** - convenience attribute instead of using the value function to convert a part to a string.  This will look at the type of domain, and use the default formatting function for that type.
  - **type** - type of domain ("set", "time", "duration", "numeric", "count", "boolean")
  - **esfilter** - limits the domain to a subset.  If this is undefined, then it will default to the union of all child dimensions.  This can often be bad because the esfilter can get complicated.  You will often see "esfilter":ESQuery.TrueFilter to simply set the domain to everything.
  - **fullFilter** - partition filters are applied in the order the parts are declared in theory.  A record that matches a part will not match any other part in the partition.  Due to technical limitations of ES, this is not done in practice yet.  Until then, ```fullFilter``` can be used to get the non-overlapping filter rules for any part.
  - **isFacet** - force the partitions of this dimension to be split into ES facets.  This is required when the documents need to be multiply counted
  - **path** -  function that will map a value (usually pulled from ```field```) and converts it to partition path.  The dimension definition allows the programmer to define partitions of partitions, forming a tree.  A partition path is a path through that tree.  In the event the tree is left undefined, the datasource will be used to find all values and build the tree using the path function.  Return a single value if you want to simply parse a value into a nicer set of parts.
  - **edges** - the child edges, with further dimension attributes
  - **default** - suggested limits for algebraic dimensions ( min, max, interval)
  - **partitions** - he child partitions, which can be further partitioned if required
  - **requiredFields** - (optional)


Hierarchical Partitions
-----------------------

Some properties domain values are functionally dependent on another.  You can
have the field attribute be a list of property names, in dependency order.
This will make a hierarchy of partitions for you,  while merging the two
properties in to the effective one that they are.

Example:  Perfy test results have multiple benchmarks, each with a suite of
tests.  Here is a simplified example of one test result

    {
        "info":{
            "benchmark":"octane",
            "test":"Box2D"
        },
        "score":200
    }

The ```Box2D``` test can not be found in the other benchmarks, so is
effectively one part of ```octane```.  The ```test``` property is a child of
the ```benchmark``` property, and we declare it as such in the dimension
definition:

    {
        "name":"Benchmark",
        "type":"set",
        "field":["info.benchmark", "info.test"]
    }

If we were to organize the data to reflect the hierarchical nature,
we could have used

    {
        "octane":{
            "Box2D":{
                "score":200
            }
        }
    }

But this deeper structure would probably require more ETL, it looses the name
of the properties ("benchmark" and "test"), and will require a more
complicated dimension definition (which is not even supported yet).
