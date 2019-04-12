# JSONPath vs JSON Query Expressions

[JSONPath](http://goessner.net/articles/JsonPath/) is a mini query language inspired by the XPath line noise. Here is a summary of the common operators [from goessner.net](http://goessner.net/articles/JsonPath/) in both: 


| XPath  | JSONPath           | Description |
|:------:|:------------------:|:-------------|
|   `/`  | `$`                |the root object/element
|   `.   | `@`                |the current object/element
|   `/`  | `.` or `[]`        |child operator
|   `..` | n/a                |parent operator
|   `//` | `..`               |recursive descent. JSONPath borrows this syntax from E4X.
|   `*`  | `*`                |wildcard. All objects/elements regardless their names.
|   `@`  | n/a                |attribute access. JSON structures don't have attributes.
|   `[]` | `[]`               |subscript operator. XPath uses it to iterate over element collections and for predicates. In Javascript and JSON it is the native array operator.
|   `|`  | `[,]`              |Union operator in XPath results in a combination of node sets. JSONPath allows alternate names or array indices as a set.
|   n/a  | `[start:end:step]` |array slice operator borrowed from ES4.
|   `[]` | `?()`              |applies a filter (script) expression.
|   n/a  | `()`               |script expression, using the underlying script engine. 


## Fixed-Property JSON 

### Example

We will use the example from [goessner.net](http://goessner.net/articles/JsonPath/) which is data representing the inventory of a small store:  

    {"store":{
        "book":[
            {
                "category":"reference",
                "author":"Nigel Rees",
                "title":"Sayings of the Century",
                "price":8.95
            },
            {
                "category":"fiction",
                "author":"Evelyn Waugh",
                "title":"Sword of Honour",
                "price":12.99
            },
            {
                "category":"fiction",
                "author":"Herman Melville",
                "title":"Moby Dick",
                "isbn":"0-553-21311-3",
                "price":8.99
            },
            {
                "category":"fiction",
                "author":"J. R. R. Tolkien",
                "title":"The Lord of the Rings",
                "isbn":"0-395-19395-8",
                "price":22.99
            }
        ],
        "bicycle":{"color":"red","price":19.95}
    }}

This is badly designed data: There is no upper limit on the property names because they are being used to encode the inventory category (eg `book`, `bicycle`).  Putting data values in property names is similar to using positional arguments: The values are placed at a numbered position, and the numbered position relates to an implicit named type. For example, the `book` type is position-encoded. At an extreme, we can do that for all properties:

    {"store":{
        "book": {
            "reference": {
                "Nigel Rees": {
                    "Sayings of the Century": { 
                         "8.95": {}
                    }
                }
            },
            "fiction": {
                "Evelyn Waugh": {
                    "Sword of Honour": {
                        "12.99": {}
                    }
                },
                "Herman Melville": {
                    "Moby Dick": {
                        "8.99": {
                            "isbn":"0-553-21311-3"
                        }
                    }
                },
                "J. R. R. Tolkien": {
                    "The Lord of the Rings": {
                        "12.99":{
                            "isbn":"0-395-19395-8"
                        }
                    }
                }
            }
        ],
        "bicycle":{"red":{"19.95":{}}}
    }}

This above form maps path depth, or path position, to an implicit named typed; Definitely more compact, but context is lost about what the property names mean. A similar effect can be had by storing the data in tuples.

    {"store":[
        ["book", "reference", "Nigel Rees", "Sayings of the Century", 8.95],
        ["book", "fiction", "Evelyn Waugh", "Sword of Honour", 12.99],
        ["book", "fiction", "Herman Melville", "Moby Dick", 8.99, "0-553-21311-3"],
        ["book", "fiction", "J. R. R. Tolkien", "The Lord of the Rings", 12.99, "isbn":"0-395-19395-8"],
        ["bicycle", "red", 19.95]
    ]}

In either case, and many others, we achieve data compression by removing context. This is a bad data format.

JSON data should be in ***fixed-property form***, which means fixing the number of properties, as much as is reasonable, for the given data source. We do this by assigning names to keys. In the example of store inventory we label the item type, and the inventory explicitly: 

    {"store":[
        {
            "item_type":"book",
            "inventory": [
            {
                "category":"reference",
                "author":"Nigel Rees",
                "title":"Sayings of the Century",
                "price":8.95
            },
            {
                "item_type":"book",
                "category":"fiction",
                "author":"Evelyn Waugh",
                "title":"Sword of Honour",
                "price":12.99
            },
            {
                "item_type":"book",
                "category":"fiction",
                "author":"Herman Melville",
                "title":"Moby Dick",
                "isbn":"0-553-21311-3",
                "price":8.99
            },
            {
                "item_type":"book",
                "category":"fiction",
                "author":"J. R. R. Tolkien",
                "title":"The Lord of the Rings",
                "isbn":"0-395-19395-8",
                "price":22.99
            }
        },
        {
            "item_type":"bicycle",
            "inventory": [
                {
                    "item_type":"bicycle",
                    "color":"red",
                    "price":19.95
                }
            ]
        }
    ]}

This is not fully denormalized: The `item_type` context is required to interpret any inventory item. It is better to annotate all the inventory items with the `item_type`; which will allow use to flatten the structure further. 

Which results well formed data: It is explicit; every property is names. It is flatter, so easier to manipulate. It is denormalized, so each record stands on its own.

    {"store":[
        {
            "item_type":"book",
            "category":"reference",
            "author":"Nigel Rees",
            "title":"Sayings of the Century",
            "price":8.95
        },
        {
            "item_type":"book",
            "category":"fiction",
            "author":"Evelyn Waugh",
            "title":"Sword of Honour",
            "price":12.99
        },
        {
            "item_type":"book",
            "category":"fiction",
            "author":"Herman Melville",
            "title":"Moby Dick",
            "isbn":"0-553-21311-3",
            "price":8.99
        },
        {
            "item_type":"book",
            "category":"fiction",
            "author":"J. R. R. Tolkien",
            "title":"The Lord of the Rings",
            "isbn":"0-395-19395-8",
            "price":22.99
        },
        {
            "item_type":"bicycle",
            "color":"red",
            "price":19.95
        }
    ]}


&lt; ADD JX EXPRESSION TO TRANSFORM ORIGINAL TO DENORMALIZED FORM >

## Comparision

Given well formed data, we can now compare to XPath:


**the authors of all books in the store**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$.store.book[*].author`  | `{"from": store, "select":"author"}`      |


**all authors**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..author`               | `{"from": store, "groupby":"author"}`      |


**all things in store, which are some books and a red bicycle**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `/store/*`                | `"from": store`    |


**the price of everything in the store**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$.store..price`          | `{"from": store, "select":"price"}` |  

**the third book**

If you need this type of query, then the data is in bad form: Something about the order is important, yet undocumented. The store will sell books, or acquire new ones, messing with the order. Does the store have only one shelf? Are the books stacked on a table?  Are they ranked by how long they have been in the store?  

To proceed we will add an `order` property to the inventory using a window function:

    store = {
        "from": store, 
        "window":{
            "name":"order", 
            "value":"rownum", 
            "groupby":"item_type"
        }
    }

<table>
<tr><th>XPath</th><th>JSON Expression</th></tr>
<tr><td>
<pre>$..book[2]
$..book[(@.length-1)]</pre>
</td><td>
<pre>{
    "from": store,
    "where": {"and":[
        {"eq":{"item_type":"book":}}, 
        {"lt":{"order":2}}
    ]}
}</pre>
</td></tr>
</table>

     
**the last book in order.**

Getting the first or last elements in a list make sense.

<table>
<tr><th>XPath</th><th>JSON Expression</th></tr>
<tr><td>
<pre>$..book[-1:]</pre>
</td><td>
<pre>{
    "from": store, 
    "select":{"aggregate":"last"},
    "where": {"eq":{"item_type":"book":}}
}</pre>
</td></tr>
</table>

**the first two books**

<table>
<tr><th>XPath</th><th>JSON Expression</th></tr>
<tr><td>
<pre>$..book[0,1]
$..book[:2]</pre>
</td><td>
<pre>{
    "from": store,
    "where": {"and":[
        {"eq":{"item_type":"book":}}, 
        {"lt":{"order":2}}
    ]}
}</pre>
</td></tr>
</table>

**filter all books with isbn number**

<table>
<tr><th>XPath</th><th>JSON Expression</th></tr>
<tr><td>
<pre>$..book[?(@.isbn)]</pre>
</td><td>
<pre>{
    "from": store,
    "where":{"exists":"isbn"}
}</pre>
</td></tr>
</table>

**filter all books cheaper than 10**

<table>
<tr><th>XPath</th><th>JSON Expression</th></tr>
<tr><td>
<pre>book[price<10]
$..book[?(@.price<10)]</pre>
</td><td>
<pre>{
    "from": store,
    "where":{"and":[
        {"eq":{"item_type":"book"}},
        {"lt":{"price":10}}
    ]}
}</pre>
</td></tr>
</table>



**all Elements in XML document. All members of JSON structure.** 

<table>
<tr><th>XPath</th><th>JSON Expression</th></tr>
<tr><td>
<pre>$..*</pre>
</td><td>
<pre>{"from": store}</pre>
</td></tr>
</table>
     

## Conclusion

JSONPath is definitely more compact than JSON Query Expressions, but JSON query expressions benefit 

* Any language where `[?(@._<1)]` is legitimate syntax should not be used: It is unreadable.
* The recursive decent operator (`..`) is dangerous: When dealing with messy data, you do not know what properties, at what depth, you will capture with this operator.
* The [grammar is more complex](https://github.com/dchester/jsonpath/blob/master/lib/grammar.js), which makes it difficult [to interpret](https://github.com/browserify/static-eval/blob/master/index.js) and translate to other data stores. It also inhibits automated composition. 

------

**Notes**

* XML normal form - http://www.cs.toronto.edu/tox/papers/xnf_pods02.pdf