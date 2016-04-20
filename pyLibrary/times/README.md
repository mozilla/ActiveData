




`Date` class
============

A simplified date and time class for time manipulation. This library is intended for personal and business applications where assuming every solar day has 24 * 60 * 60 seconds is considered accurate. [See *GMT vs UTC* below](#GMT%20vs%20UTC). 


Assumptions
-----------

* **All time is in GMT** - Timezone math is left to be resolved at the human endpoints: Machines should only be dealing with one type of time; without holes, without overlap, and with minimal context.
* **Single time type** - There is no distinction between dates, datetime and times; all measurements in the time dimension are handled by one type called `Date`. This is important for treating time as a vector space.
* **Exclusive ceiling time ranges** - All time comparisons have been standardized to `min <= value < max`. The minimum is inclusive, and the maximum is excluded. (please word this better) 


`Date` properties
-----------------

### `Date()` constructor###

The `Date()` method will convert unix timestamps, millisecond timestamps, various string formats and simple time formulas to create a GMT time 


### `now()` staticmethod ###

Return `Date` instance with millisecond resolution (in GMT).

### `eod()` staticmethod ###

Return end-of-day: Smallest `Date` which is greater than all time points in today. Think of it as tomorrow. Same as `now().ceiling(DAY)`

### `today()` staticmethod ###

The beginning of today. Same as `now().floor(DAY)`


### range(min, max, interval) staticmethod ###

Return an explicit list of `Dates` starting with `min`, each `interval` more than the last, but now including `max`.   Used in defining partitions in time domains.


###`floor(duration=None)` method###

This method is usually used to perform date comparisons at the given resolution (aka `duration`). Round down to the nearest whole duration. `duration` as assumed to be `DAY` if not provided.

###`format(format="%Y-%m-%d %H:%M:%S")` method###

Just like `strftime`

###`milli` property###

Number of milliseconds since epoch

###`unix` property###

Number of seconds since epoch


###`add(duration)` method###

Add a `duration` to the time to get a new `Date` instance. The `self` is not modified.

###`addDay()` method###

Convenience method for `self.add(DAY)`




`Duration` class
================

Represents the difference between two `Dates`. There are two scales:
  
*  **`DAY` scale** - includes seconds, minutes, hours, days and weeks.
*  **`YEAR` scale** - includes months, quarters, years, and centuries. 

### `Duration()` constructor###

Create a new `Duration` by name, by formula, by timespan, or (more rarely) number of milliseconds.


### `floor(interval=None)` method###

Round down to nearest `interval` size.


### `seconds` property###

return total number of seconds (including partial) in this duration (estimate given for `YEAR` scale)

### `total_seconds()` method ###

Same as the `seconds` property

### `round(interval, decimal=0)` method ###

Return number of given `interval` rounded to given `decimal` places

### `format(interval, decimal=0)` method ###

Return a string representing `self` using given `interval` and `decimal` rounding


`Time Vector Space`

The `Date` and `Duration` objects form a one dimensional vector space, upon which `+` and `-` operators are allowed.   Comparisons with (`>`, `>=', `<=`, `<`) are also supported. 



GMT vs UTC
----------

The solar day is he most popular timekeeping unit. This library chose GMT (UT1) for its base clock because of its consistent seconds in a solar day. UTC suffers from inconsistent leap seconds and makes time-math difficult, even while forcing us to make pedantic conclusions like some minutes do not have 60 seconds. Lucky for us Python's implementation of UTC (`datetime.utcnow()`) is wrong, and implements GMT: Which is what we use.    

Error Analysis
--------------

Assuming we need a generous leap second each 6 months (the past decade saw only 4 leap seconds), then GMT deviates from UTC by up to 1 seconds over 181 days (December to June, 15,638,400 seconds) which is an error rate `error = 1/15,638,400 = 0.00000006395%`. If we want to call the error "noise", we have a 70dB signal/noise ratio. All applications that can tolerate this error should use GMT as their time basis.

  
