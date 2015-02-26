
Incomplete Bits
---------------

There are some parts of Qb that have not been fully thought out and refactored to match.

Qb.merge
---------

Technically, cubes should be implemented as a single (multidimensional) array of
uniform type values.  This would facilitate conversion to asm.js (or numpy in Python).
Currently, cubes can also be whole records, and this causes complexity in the code
that should be removed.  Records should be represented as a set of coordinates
in a set of value cubes; where each column in the record refers to a value cube.

What does it mean to "join" cubes?  In the case where all edges of the two cubes
match, then the join is simply a union.  In the case where one cube has a single
dimension, then the join is on the edges; and just like an edge, the edge value
projects as a constant over the other dimensions in the cube.  Again, we see edges as simply another attribute of the cells that are projected as constants over the whole cube.  With this we can conclude joins with zero-dimension cubes act as a constant attribute on all records in the cube, and joins on an incomplete set of edges is simply projected as constant over the remainder dimensions.

Here is some old symantics, It must be expanded to any number of dimensions.

    Qb.merge({"cubes":[
        {"from":s0, "edges":["test_name", "date"]},
    	{"from":s1, "edges":["test_name", "date"]}
    ]})
