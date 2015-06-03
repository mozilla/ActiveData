 
Multithreading
==============



Module `threads`
----------------

The main distinction between this library and Python's is two-fold:

1. **Multi-threaded queues do not use serialization** - Serialization is great in the general case, where you may also be communicating between processes, but it is a needless overhead for single-process multi-threading.  It is left to the programmer to ensure the messages put on the queue are not changed, which is not ominous demand.
2. **Shutdown order is deterministic and explicit, if desired** - If there is one aspect of threading library that is missing, it will be a lack of controlled and orderly shutdown.  
  * All threads are required to accept a `please_stop` token and are expected to test for its signal in a timely manner and exit when signalled.
  * All threads have a parent, which are ultimately responsible for ensuring their children get the `please_stop` signal, and are dead before stopping themselves.
 
These conventions eliminate the need for `interrupt()` and `abort()`, both of which are unstable idioms when there are resources.   Each thread can shutdown on its own terms, but is expected to do so expediently.

###What's it used for###

A good amount of time is spent waiting for underlying C libraries and OS services to respond to network and file access requests.  Multiple threads can make your code faster despite the GIL when dealing with those requests.  For example, by moving logging off the main thread, we can get up to 15% increase in overall speed because we no longer have the main thread waiting for disk writes or .  Please note, this level of speed improvement can only be realized if there is no serialization happening at the multi-threaded queue.  

###Asynch vs. Actors###

My personal belief is that [actors](http://en.wikipedia.org/wiki/Actor_model) are easier to reason about, and 

Synchronization Primitives
--------------------------

There are three major aspects of a synchronization primitive:

* **Resource** - Monitors and locks can only be owned by one thread at a time
* **Binary** - The primitive has only two states
* **Reversible** - The state of the primitive can be set, or advanced, and reversed again

The last, *reversibility* is very useful, but ignored in many threading libraries.  The lack of reversibility allows us to model progression; and we can allow threads to poll for progress, or be notified of progress. 

###Class `Signal`###


