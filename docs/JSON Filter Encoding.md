Philosophy of JSON Encoding
---------------------------

When serializing data structures, specifically data structures involving
operations, there are three operator positions available:

* Prefix - ```+ a b```
* Infix  - ```a + b```
* Suffix - ```a b +```

Encoding these as JSON gives us:

* Prefix - ```{"add": {"a": "b"}}```
* Infix  - ```{"a": {"add": "b"}}```
* Suffix - ```{"a": {"b": "add"}}```

Personally, I find infix ordering aesthetically pleasing in the limited case
of binary commutative operators.  Unfortunately, many operators have
a variable number of operands, which makes infix clumsy.

Even if I believe infix should not be used, there is still benefit
to reusing existing JSON-encoded operations found in other applications
But, it seems no planning was put into the existing serializations:

* MongoDB uses a combination of [infix notation](http://docs.mongodb.org/manual/reference/operator/query/gt/#op._S_gt),
[prefix notation](http://docs.mongodb.org/manual/reference/operator/query/and/#op._S_and),
and [nofix notation](http://caffinc.com/blog/2014/02/mongodb-eq-operator-for-find/),
which is clearly a mess.
* ElasticSearch has standardized on a [prefix notation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-term-filter.html),
and has some oddities like the [range filter](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-filter.html)
which is a combination of prefix and suffix, and probably a side effect of some
leaky abstraction.

Since there is no standard, we will declare yet-another JSON filter format:  It uses prefix
ordering; and is consistent with functional notation.

| Operation                     | Qb Query                       | MongoDB                           | ElasticSearch                       |
|:------------------------------|:-------------------------------|:----------------------------------|:------------------------------------|
|Equality                       |`{"eq": {field: value}}`        |`{field: value}`                   |`{"term": {field: value}}`           |
|Inequality `gt, gte, ne, lte, lt`|`{"gt": {field: value}}`      |`{field: {"$gt": value} }`         |`{"range": {field: {"gt":value}}}`   |
|Logical Operators `and, or`    |`{"and": [a, b, c, ...]}`       |`{"$and": [a, b, c, ...]}`         |`{"and": [a, b, c, ...]}`            |
|Match All                      |`true`                          |`{}`                               |`{"match_all": {}}`                  |
|Exists                         |`{"exists": field}`             |`{field: {"$exists": true}}        |`{"exists": {"field": field}}`       |
|Missing                        |`{"missing": field}`            |`{field: {"$exists": false}}`      |`{"missing": {"field": field}}`      |
|Match one of many              |`{"in": {field:[a, b, c, ...]}` |`{field {"$in":[a, b, c, ...]}`    |`{"terms": {field: [a, b, c, ...]}`  |
|Prefix                         |`{"prefix": {field: prefix}}`   |`{field: {"$regex": /^prefix\.*/}}`|`{"prefix": {field: prefix}}`        |
|Regular Expression             |`{"regex": {"field":regex}`     |`{field: {"$regex": regex}}`       |`{"regexp":{field: regex}}`          |
|Script                         |`{"script": javascript}`        |`{"$where": javascript}`           |`{"script": {"script": mvel_script}}`|

**Special note on nulls**
  * Qb - null values do not `exists` and are considered `missing`
  * MongoDB and ES - null values `exist` and are not `missing`
