`columns` Property
------------------

The `columns` property is an array of objects, each declaring the domain
values that are possible for each of the columns.

  - **name** – The name used to match column found in the data 
  - **domain** – The range of values for the column

`columns.domain` Inner Property
-------------------------------

The `domain` is defined for every column.  

  - **type** – One of a few predefined types (Default `{"type":"default"}`)
  - **units** - The measurement units, (see below)
  - **name** – (Optional) Name given to this domain definition
  - **higher_is_better** - Boolean to help with orientation  


`columns.domain.type` Inner Property
------------------------------------

Every domain is one of a few basic domain types. Which further defines the
other domain attributes which can be assigned.

  - **count** – just like numeric, but limited to integers >= 0
  - **set** – An explicit set of unique values, in the order expected to be shown
      - **columns.domain.partitions** – the set of values allowed.  
  - **time** – Defines parts of a time domain.
      - **columns.domain.min** – Minimum value of domain (optional)
      - **columns.domain.max** – Supremum of domain (optional)
      - **columns.domain.interval** – (optional) group time to a course interval 
      (max-min)/interval must be an integer
  - **duration** – Defines an time interval
      - **columns.domain.min** – Minimum value of domain (optional)
      - **columns.domain.max** – Supremum of domain (optional)
      - **columns.domain.interval** – (optional) group duration to a course interval 
      (max-min)/interval must be an integer
  - **numeric** – Defines a range of values
      - **columns.domain.min** – Minimum value of domain (optional)
      - **columns.domain.max** – Supremum of domain (optional)
      - **columns.domain.interval** – (optional) group numeric value to a course interval
      (max-min)/interval must be an integer
  - **default** - (Optional) scanning the `data` will determine the domain.  
  This is default behaviour when `domain.type: null` or the column definition 
  is missing entirely 

`columns.domain.units` Inner Property
------------------------------------

Valid units are strings:

 - **count** - for measuring number of test results, or steps, or whatever
 - **second** - of course
 - **amps** - measuring current
 - **watts** - measuring power consumption
 - **volts** - measuring battery charge 
 - **bytes** - measuring memory
 - **hertz** - measuring clocks speeds
 - **meters** - just in case
 - **gram** - just in case

we can include unit-less constants, for the sake of clarity
 
 - **milli** - constant: 1/1000 
 - **kilo** - constant: 1,000
 - **mega** - constant: 1,000,000
 - **giga** - constant: 1,000,000,000
 - **tera** - constant: 1,000,000,000,000

We should be able to combine these with `*`, `/`, and `()` operators for compound measures

Examples
--------

Performance result

    {
        "name": "result", 
        "domain" {
			"type": "numeric",
			"units": "count/second"
			"higher_is_better": true
		}
	}


Page Load Time

    {
        "name": "result", 
        "domain" {
			"type": "numeric",
			"units": "second/count"
			"higher_is_better": false
		}
	}


Power consumption

    {
        "name": "measure", 
        "domain" {
			"type": "numeric",
			"units": "milli * volt * amps"
			"higher_is_better": false
		}
	}

Memory


    {
        "name": "memory", 
        "domain" {
			"type": "numeric",
			"units": "mega bytes"
			"higher_is_better": false
		}
	}


Acceleration (not an actual Mozilla test)

    {
        "name": "value", 
        "domain" {
			"type": "numeric",
			"units": "meter / (second * second)"
			"higher_is_better": true
		}
	}


