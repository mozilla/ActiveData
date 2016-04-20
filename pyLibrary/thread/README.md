 
Multithreading
==============


Module `threads`
----------------

The main distinction between this library and Python's is:

1. **Multi-threaded queues do not use serialization** - Serialization is great in the general case, where you may also be communicating between processes, but it is a needless overhead for single-process multi-threading. It is left to the programmer to ensure the messages put on the queue are not changed, which is not ominous demand.
2. **Shutdown order is deterministic and explicit** - Python's threading library is missing strict conventions for controlled and orderly shutdown. These conventions eliminate the need for `interrupt()` and `abort()`, both of which are unstable idioms when using resources.   Each thread can shutdown on its own terms, but is expected to do so expediently.

  * All threads are required to accept a `please_stop` token and are expected to test for its signal in a timely manner and exit when signalled.
  * All threads have a parent - The parent is responsible for ensuring their children get the `please_stop` signal, and are dead, before stopping themselves.

3. **Logging and Profiling is Integrated** - Logging and exception handling is seamlessly included: This means logs are centrally handled, and thread safe. Parent threads have access to uncaught child thread exceptions, and the cProfiler properly aggregates results from the multiple threads.  
 

###What's it used for###

A good amount of time is spent waiting for underlying C libraries and OS 
services to respond to network and file access requests. Multiple 
threads can make your code faster despite the GIL when dealing with those 
requests. For example, by moving logging off the main thread, we can get 
up to 15% increase in overall speed because we no longer have the main thread 
waiting for disk writes or remote logging posts. Please note, this level of 
speed improvement can only be realized if there is no serialization happening 
at the multi-threaded queue.  

###Asynch vs. Actors###

My personal belief is that [actors](http://en.wikipedia.org/wiki/Actor_model) 
are easier to reason about than [asynch tasks](https://docs.python.org/3/library/asyncio-task.html).
Mixing regular methods and co-routines (with their `yield from` pollution) is 
dangerous because:

1. calling styles between methods and co-routines can be easily confused
2. actors can use methods, co-routines can not
3. there is no way to manage resource priority with co-routines.
4. stack traces are lost with co-routines

Synchronization Primitives
--------------------------

There are three major aspects of a synchronization primitive:

* **Resource** - Monitors and locks can only be owned by one thread at a time
* **Binary** - The primitive has only two states
* **Irreversible** - The state of the primitive can only be set, or advanced, never reversed

The last, *irreversibility* is very useful, but ignored in many threading 
libraries. The irreversibility allows us to model progression; and 
we can allow threads to poll for progress, or be notified of progress. 

These three aspects can be combined to give us 8 synchronization primitives:

* `- - -` - Semaphore
* `- B -` - Binary Semaphore
* `R - -` - Monitor
* `R B -` - Lock
* `- - I` - Progress
* `- B I` - Signal
* `R - I` - ?limited usefulness?
* `R B I` - ?limited usefulness?

##Class `Signal`

An irreversible binary semaphore used to signal state progression.

**Method `wait_for_go(self, timeout=None, till=None)`**

Put a thread into a waiting state until the signal is activated

**Method `go(self)`**

Activate the signal. Does nothing if already activated.

**Method `is_go(self)`**

Test if the signal is activated, do not wait

**Method `on_go(self, target)`**

Run the `target` method when the signal is activated. The activating thread will be running the target method, so be sure you are not accessing resources.
