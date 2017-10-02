# Time

All points in time are referenced as seconds since epoch GMT. UTC is not used
due to the irregular leap seconds. No one needs UTC unless you need accuracy
over 7 digits (measuring duration of months with accuracy of seconds). The
regularity of GMT allows us to define a vector space.

## Date, Time and Duration

The [`date` operator](jx_expressions.md#date-operator) is used to convert literal strings into time or duration values. It accepts absolute time, relative time, durations and simple expressions. 

### Absolute Time

Time can be 

* a numeric value - which is interpreted as a unix timestamp, or
* a string - which is can be a variety of formats; assumed to be GMT, unless timezone information is included.     

**Example**

    {"date": "June 30, 2015 19:50:10"}

### Relative Time

There are a few relative time values:

* `now` - current time with millisecond accuracy
* `today` - beginning of the current day (GMT)
* `tomorrow` - beginning of tomorrow
* `eod` - end of day (same as tomorrow) 

**Example**

	{"date": "today"}

### Duration

There are string constants to refer to the 

* regular durations `second`, `minute`, `hour`, `day`, `week` and the
* irregular durations: `month`, `quarter`, `year`.

notice they are singular, not plural. 

**Example**

	{"date": "day"}
	{"date": "week"}
	{'date": "year"}

### Expressions

The `date` operator also accepts simple algebraic expressions. Duration can be added (`+`) and subtracted (`-`) from dates, and each other. Duration can also be multiplied by a scalar which is simply prefix numeric value (no operator).

	{"date": "today-3month"}            # three months before beginning of today (GMT)
	{"date": "now-today"}               # number of seconds since beginning of today (GMT)
	{"date": "2month + week - 13hour"}  # legitimate, but meaningless


More Expressions
----------------

JSON Expressions can also operate on time (as unix timestamp) and duration (in seconds). These are regular mathematical operators that also prove useful for bucketing and aggregation.

### Floor

The [`floor` operator](jx_expressions.md#floor-operator) rounds down to closest time that is a multiple of the given duration since epoch GMT.  

Here are some examples using the floor function:

	# Let example = {"date": "June 30, 2015 19:50:10"}

	{"floor": {"example": {"date": "hour" }}} ⇒ June 30, 2015 19:00:00
	{"floor": {"example": {"date": "day"  }}} ⇒ June 30, 2015 00:00:00
	{"floor": {"example": {"date": "week" }}} ⇒ June 28, 2015 00:00:00
	{"floor": {"example": {"date": "month"}}} ⇒ June 1, 2015 00:00:00

`floor` can be used to group timestamps into familiar sized buckets.


    {
        "from": "task",
        "groupby": {
            "name": "date", 
            "value": {"floor": {"start_time": {"date": "month"}}}
        }
    }


### Mod

The [`mod` operator](jx_expressions.md#mod-operator) can help reveal seasonal patterns

    
    time_of_day = {"mod": {"start_time": {"date": "day" }}}

	seconds_into_week = {"mod": [
        "start_time",  
        {"date": "week"}
    ]}

	day_of_week = {"mod": [
        {"floor": {"start_time": {"date": "day"}}}, 
        {"date": "week"}
    ]}



 




