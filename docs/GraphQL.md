
GraphQL vs JSON Query Expressions
---------------------------------

GraphQL, as of October 2016, is a relatively new language.  It is an explicitly typed, data retrieval language.

### The good parts

**Select by Shape**

GraphQL has the elegant feature where the inner-object path of the property acts as a selector from a logical database table, through a snowflake schema, to terminate at a specific column.  For example:

    {
        user {
			name
            age
            friends {
                name
            }
        }
    }

will assume the `user` table is the centre of the snowflake (the grain), select some properties, join with `friends` table, and select some properties off them too.  This is a significant gain over SQL (which can not even operate on documents), and a modest gain over noSQL datastores; which require a query envelope to do the same.

**Computation Bounded** 

GraphQL is not capable of aggregation, nor table joins; you only get back what you put in. This is an acceptable limitation for OLTP applications, and provides a computational upper-bound on the requests received by the server, namely O(n), where n is the number of objects returned. This allows servers to provide a QoS, and continue to be responsive, or predictable, under load.  

Compared to SQL, where joins can have O(e^N) performance, GraphQL can serve a niche.  


### The bad parts 

**"Query Language"**

Calling GraphQL a "query language" is good marketing, and like all good marketing, misleading. GraphQL has limited data manipulation capabilities, clumsy filtering and no aggregations. Generally it is inferior to the variety of query languages provided by noSQL datastores.   

**Complex Language** 

XML got it wrong when it combined maps and lists into a single type: Every tag has a name, has attributes for describing the tag itself, and has children tags to hold content. I blame this fundamental triple as the source of all complexity built atop the XML landscape. GraphQL makes the same mistake; every element is triple: a name, a map, and a list.

	{user (id: 1) {name}}

This may not seem complex for big-brained humans, but getting code to manipulate this structure is hard. So much so, that meta-programming operators must be introduced to help.

**Pre-Processing Operators**

If you consider conglomerations of GraphQL triples to be a language, then you can call the operators "pre-processors" or meta-programming features. Given that the language unit is a triple, composition is difficult without these higher level operators:

* `$` - escapes the language to define variables
* `@` - escapes the language to for pre-processors 
* `...` - escapes the language for conditional query composition

Of course, you can also see these pre-proccessing operators as language features; in which case we have a sophisticated language that should be contrasted with another query language, to give perspective.

**Line Noise**

I realize many people enjoy "$#:%@" in their languages, and maybe you can defend it on the basis of "internationalization", but I personally despise punctuation when words can be used: Bits are cheap, please use words instead of symbols. They are easier to Google too.

## Detailed Comparison

The rest of this document goes into detail. My argument depends on the use of a "host language"; which is the turning-complete language you are using to send JSON Expressions. The host language is most likely Javascript, or Python because they make composing JSON easy, but other languages can host JSON Expressions too. What's important to point out is GraphQL is also used in the context of a host language; its extra "features" result in two ways of doing the same thing.

My objective is to demonstrate that the simple semantics of JSON Expressions allow the host language to compose JSON queries, negating the need for pre-processing operators, and making for a simpler query language to reason about. 

The GraphQL examples were snagged from [http://graphql.org/learn/queries/](http://graphql.org/learn/queries/), which turns out to a be reasonable overview of GraphQL features.

### Filtering

GraphQL elements are a compound structure consisting of a name, a map inside the parenthesis `()`, and list inside the braces `{}`.  The map is used to define the filter, and the list declares multiple paths to the data.

    {
        user(id: 1) {
            name
        }
    }

JSON Queries are explicit about the arguments, this makes them more verbose, but also makes them easy to compose in code.  Here is the same in JSON Query Expressions:

	{
		"from" "user",
		"select": "name"
		"where": {"eq":{"id":1}} 
	}


### Selecting specific fields


GraphQL can traverse database relations, and return the particular fields your application requires

    {
        user(id: 1) {
            age
            friends {
                name
            }
        }
    }

JSON Queries can do the same, using dot-separated path names: 

	{
		"from": "user",
		"select" [
			"age"
			"friends.name"
		],
		"where": {"eq":{"id":1}}
	}


.. furthermore JSON Queries can traverse relations in both directions. In this case, we return one row for each friend:

	{
		"from": "user.friends",
		"select" [
			"user.age"
			"name"
		],
		"where": {"eq":{"user.id": 1}}
	}

### Aliases

GraphQL can accept multiple relational queries to build a single object. 


    {
        empireHero: hero(episode: EMPIRE) {
            name
        }
        jediHero: hero(episode: JEDI) {
            name
        }
    }

For JSON Expressions, the `select` expression can build an object using an array of name/value pairs:


    {
        "select":[
             {
                 "name":"empireHero", 
                 "value":{
                     "from": "hero", 
                     "select": "name", 
                     "where":{"eq":{"episode": "EMPIRE"}}
                 }
             },
             {
                 "name":"jediHero"
                 "value":{
                     "from": "hero", 
                     "select": "name", 
                     "where":{"eq":{"episode": "JEDI"}}
                 }
             }
        ]
    }

### Fragments

GraphQL allows the definition of query fragments; meant for reuse in a query:

	{
		leftComparison: hero(episode: EMPIRE) {
			...comparisonFields
		}
		rightComparison: hero(episode: JEDI) {
			...comparisonFields
		}
	}

	fragment comparisonFields on Character {
		name
		appearsIn
		friends {
			name
		}
	}

There is no equivalent in JSON Expressions; it is expected that JSON Expressions are used in the context of a host language, like Javascript or Python, which is capable of managing fragments:

	comparisonFields = [
		"name",
		"appearsIn",
		"friends.name"
	]


	query = {"select": [
		{
			"name": "leftComparision",
			"value": {
				"from": "hero",
				"select": comparisonFields,
				"where": {"eq":{"episode":"EMPIRE"}}
			}
		},
		{
			"name": "rightComparision",
			"value": {
				"from": "hero",
				"select": comparisonFields,
				"where": {"eq":{"episode":"JEDI"}}
			}
		}
	]}
		
definitely more verbose, but easier to compose.


### Variables

GraphQL - use `$` as a prefix to declare variables, and their type:


    query HeroNameAndFriends($episode: Episode) {
        hero(episode: $episode) {
            name
            friends {
                name
            }
        }
    }

JSON Queries do not have variables, again resting on the host language. In this case, setting the `where` clause to the condition required using a `setdefault` function [(python example)](https://github.com/klahnakoski/pyLibrary/blob/5ba9998dee239ec74a0905123a823069581cd8f2/pyLibrary/dot/__init__.py#L144). The technique shown here is to use JSON as a template, and then add the required annotations to build the final result.

	HeroNameAndFriends = {
		"from": "hero"
		"select": [
			"name",
			"friends.name"
		],
	}

	query = setdefault(
		{"where":{"eq":{"episode":"JEDI"}}}, 
		HeroNameAndFriends
	)

### Directives

GraphQL allows you to define conditional composition (`@`):

	query Hero($episode: Episode, $withFriends: Boolean!) {
	  hero(episode: $episode) {
	    name
	    friends @include(if: $withFriends) {
	      name
	    }
	  }
	}

with variables...

	{
	  "episode": "JEDI",
	  "withFriends": false
	}

JSON Queries do not have directives. Instead, use a standard expression, albeit with constants.  The [`when` expression](https://github.com/klahnakoski/ActiveData/blob/dev/docs/jx_expressions.md#when-operator) can provide identical switching logic used by the `@include(if: .)` idiom:

	vars = {
	  "episode": "JEDI",
	  "withFriends": false
	}

	query = {
		"from": "hero",
		"select": [
			"name", 
			{"name":"friends.name", "value":{"when": vars.withFriends, "then":"friends.name"}}
		],
		"where":{"eq":{"episode": vars.episode}}
	}

this assumes you want all the logic in one query. Alternately, the host language can build the query from a template:

	query = {
		"from": "hero",
		"select": [
			"name" 
		],
		"where":{"eq":{"episode": vars.episode}}
	}

	if vars.withFriends:
		query.select.append("friends.name")


### Inline fragments

GraphQL has an `... on` operator to distinguish query parameters by type.  

    query HeroForEpisode($ep: Episode!) {
        hero(episode: $ep) {
            name
            ... on Droid {
              primaryFunction
            }
            ... on Human {
                height
            }
        }
    }

JSON Queries operate on dynamically typed data. But, let us assume you are tracking the type of the object in the `_type` property.  You are free to perform similar GraphQL coding acrobatics:  

	{
		"from": "hero"
		"select":[
			"name",
			{
				"name":"primaryFunction", 
				"value":{
					"when":{"eq":{"_type":"Droid"}}, 
					"then":"primaryFunction"
				}
			},
			{
				"name":"height", 
				"value":{
					"when":{"eq":{"_type":"Human"}}, 
					"then":"height"
				}
			}
		]
	}

Usually, the simpler option is to leverage the dynamic-typed nature of JSON Queries, and return the properties, if they exist:

	{
		"from": "hero"
		"select":[
			"name",
			"primaryFunction",
			"height"
		]
	}
 
## Summary

GraphQL is a complex language with a limited niche.  I have not touched upon the explicit typing declaration required to make GraphQL queries workk. JSON Queries are more expressive, but any noSQL document query language would make a better choice.  
