Proposed Chart API
==================

Yet another chart API!  The priorities are:

1. No magic labels or prefixes - A surprising number APIs use a concatenation-
of-names to inject parameter values.  Use objects for compound values.
2. Use CSS style names - except when CSS names break rule #1
3. `data` is assumed to be an Array of Objects, or a Qb query result.
4. `value` is can be a property name, or a Qb expression

Angle brackets (`<>`) are used to indicate a compound object of the same name.  
Axis can be given any name, but default to `x` and `y` with position `bottom`, 
`left` respectively.  More axis are allowed.

If two, or more, charts share the same `axis` (as in `===`), they are 
considered *linked*, and act as a single chart. 

```javascript
{
	"<chart>":{
		"area":{
			"style":"<style>"
		},
		"data":[],
		"target":"",
		"axis":{
			"x":"<axis>",
			"y":"<axis>"
		},
		"title":{"position":"","label":"","description":""},
		"legend":{"label":"", "style":"<style>"},
		"series":{
			"value":"",
			"axis":"x",
			"label":{},
			"marker":{"size":1,"symbol":"","style":"<style>"}
			"hoverStyle":{"size":1,"symbol":"","style":"<style>"}
		}
	},
	"<axis>":{
		"position":"top/right",
		"rug": false, //"show projection as a series of ticks along the axis",
		"label":"",
		"value":"",
		"normalized":true,  //Convert to % of total
		"range":{"min":0,"max":1},
		"ticks":{"interval":1,"quantity":1,"style":"<style>"},
		"lines":{"style":"<style>"},
		"bands":[{"min":0,"max":1,"label":"","id":"","style":"<style>"}],
		"marks":[{"value":0,"label":"","id":"", "style":"<style>"}]
	},
	"<style>":{
		"font":"",
		"width":1,
		"color":"",
		"style":"solid, dotted, etc",
		"z-index":0,
		"border":"<style>",
		"line":"<style>",
		"padding":{
			"right":""
		}
	}
}
```
