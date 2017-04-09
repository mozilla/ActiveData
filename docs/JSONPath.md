# JSONPath vs JSON Query Expressions

[JSONPath](http://goessner.net/articles/JsonPath/) is a mini query language inspired by the XPath line noise.

Path expressions are related to graph query languages, with limitation

# XML NORMAL FORM
http://www.cs.toronto.edu/tox/papers/xnf_pods02.pdf





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


# Property Limited JSON 





## Example

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

This is badly designed data: There is no upper limit on the property names because they are being used to encode the inventory category (eg `book`, `bycycle`).  Putting data values in property names is similar to positional arguments: The values are place at a numbered position, and the numbered position relates to an implicit named type. For example, the `book` type is positional encoded, but we can do the same for other properties:

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

Definitely more compact, but context is lost about what the property names mean. A similar effect can be had by storing the data in tuples.

    {"store":[
        ["book", "reference", "Nigel Rees", "Sayings of the Century", 8.95],
		["book", "fiction", "Evelyn Waugh", "Sword of Honour", 12.99],
		["book", "fiction", "Herman Melville", "Moby Dick", 8.99, "0-553-21311-3"],
	    ["book", "fiction", "J. R. R. Tolkien", "The Lord of the Rings", 12.99, "isbn":"0-395-19395-8"],
        ["bicycle", "red", 19.95]
    ]}

In either case, and many others, we achieve data compression by removing context.  This is bad for JSON 


JSON data should be in ***limited property form***, which means fixing the number of properties, as much as is reasonable for the given datasource. One option is to convert the {"property":value} form to    If the property names 


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


If we are willing to annotate the inventory items with the `item_type`, we can flatten the structure further 

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


A better format will encode the category explicitly:

This transformation can be done with 

    {
        "from":{"items":"store"},
        "select":{
            "name":"store",
            "aggregate":"union", 
            "value":{
                "from":"..value",
                "select":[
                    {"name":"item_type", "value":"..name"}
                    "."
                ]
            }
        }
    }

Which results in an explicit, and flatter structure:



**the authors of all books in the store**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$.store.book[*].author`  | `{"from":"store", "select":"author"}`      |


**all authors**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..author`               | `{"from":"store", "select":"author"}`      |


**all things in store, which are some books and a red bicycle**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `/store/*`                | `"from":"store"`    |


**the price of everything in the store**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$.store..price`          | `{"from":"store", "select":"price"}` |       |



**the third book**

This type of query should never be needed: The store will sell books, or acquire new ones, messing with the order.  There is something in the order of the books that is somehow meaningful: Does the store have only one shelf? Are the books stacked on a table?  Are they ranked by how long they have been in the store? 

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..book[2]`<br>              
  `$..book[(@.length-1)]`   |   <pre>{
    "from":{"from":"store", "window":{"name":"order", "value":"rownum", "groupby":"item_type"}},
    "where": {"eq":{"order":2}}
}</pre>       |

     
**the last book in order.**

Getting the first or last elements in a list make sense.

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..book[-1:]`            | <pre>{
    "from":"store", 
    "select":{"aggregate":"last"}
}</pre> |

**the first two books**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..book[0,1]`<br>
  `$..book[:2]`             | <pre>{
    "from":{"from":"store", "window":{"name":"order", "value":"rownum", "groupby":"item_type"}},
    "where": {"lt":{"order":2}}
}</pre>       |       |

**filter all books with isbn number**

|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..book[?(@.isbn)]`      | <pre>{
    "from":"store",
    "where":{"exists":"isbn"}
}</pre>    |


**filter all books cheaper than 10**
|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `book[price<10]`<br>             
  `$..book[?(@.price<10)]`  | <pre>{
    "from":"store",
    "where":{"and":[
        {"eq":{"item_type":"book"}},
        {"lt":{"price":10}}
    ]}
}</pre>|


**all Elements in XML document. All members of JSON structure.** 
|           XPath           |   JSON Expression   |
|---------------------------|---------------------|
| `$..*`                    | `{"from":"source"}` |
     

