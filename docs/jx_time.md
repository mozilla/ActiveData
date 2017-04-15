Time
====

All points in time are referenced as seconds since epoch GMT. UTC is not used
due to the irregular leap seconds. No one needs UTC unless you need accuracy
over 7 digits (measuring duration of months with accuracy of seconds). The
regularity of GMT allows us to define a vector space.

Absolute Time
-------------

Time can be 

* a numeric value - which is interpreted as a unix timestamp, or
* a string - which is can be a variety of formats; assumed to be GMT, unless timezone information is included.     

Relative Time
-------------

There are a couple of relative time values:

* `now` - current time with millisecond accuracy
* `today` - beginning of the current day (GMT)
* `tomorrow` - beginning of tomorrow
* `eod` - end of day (same as tomorrow) 

Duration
--------

There are string constants to refer to the 

* regular durations `second`, `minute`, `hour`, `day`, `week` and the
* irregular durations: `month`, `quarter`, `year`.

notice they are singular, not plural. 

Durations can be added (`+`) and subtracted (`-`) from each other. They can also be multiplied by a scalar which is simply prefix numeric value (no operator).

	2month + week - 13hour



Time Expressions
----------------

### Floor

Time expressions are simplified with the floor function, denoted by pipe (`|`).   Floor only works on time points, and rounds down to closest time that is a multiple of the given duration since epoch GMT:

* `June 30, 2015 19:50:10 | hour  ⇒ June 30, 2015 19:00:00`
* `June 30, 2015 19:50:10 | day   ⇒ June 30, 2015 00:00:00`
* `June 30, 2015 19:50:10 | week  ⇒ June 28, 2015 00:00:00`
* `June 30, 2015 19:50:10 | month ⇒ June 1, 2015 00:00:00` 

It is convenient to use the floor function on the relative time 

* `now | day` same as `today`
* `today | month` for start of the current month

You can also perform modulo arithmetic with the following identity:

	mod(x, d) = x - x|d

For example, `now - now|day` for current time into this day 



 




