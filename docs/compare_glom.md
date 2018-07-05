

`glom` is a Python data access library, similar to GraphQL in its data transformation syntax.


* [tutorial](http://glom.readthedocs.io/en/latest/tutorial.html)
* [code](https://github.com/mahmoud/glom)


The major differences between `glom` and `mo-dots` are:

* `glom` requires the use of the `glom()` method, while `mo-dots` overrides `__getattr__` and `__getitem__` to achieve the same
* `glom` works on any Python objects. `mo-dots` only works with `Data` (or objects wrapped as `Data`)
* `glom` can perform complex transformations
 
