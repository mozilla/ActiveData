/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("aLibrary.js");
importScript("qb/ESQuery.js");


var Dimension = {};


var DEFAULT_QUERY_LIMIT = 20;

Dimension.prototype = {
	"getDomain": function (param) {
		//param.fullFilter  SET TO true TO HAVE FULL FILTER IN PARTITIONS
		//param.depth IS MEANT TO REACH INTO SUB-PARTITIONS
		if (param === undefined) {
			param = {
				"depth": 0,
				"separator": "."
			};
		}//endif
		param.depth = coalesce(param.depth, 0);
		param.separator = coalesce(param.separator, ".");

		var useFullFilter = coalesce(param.fullFilter, false);

		var self = this;
		var partitions = null;

		if (!this.partitions && this.edges) {
			//USE EACH EDGE AS A PARTITION, BUT isFacet==true SO IT ALLOWS THE OVERLAP
			partitions = this.edges.map(function (v, i) {
				if (i >= coalesce(self.limit, DEFAULT_QUERY_LIMIT))
					return undefined;
				if (v.esfilter === undefined) return;
				v.style = coalesce(v.style, {});
				var temp = v.parent;
				v.parent=undefined;
				var output = Map.clone(v);
				v.parent=temp;
				output.esfilter = useFullFilter ? v.fullFilter : v.esfilter;
				return output;
			});
			self.isFacet = true;
		} else if (param.depth == 0) {
			partitions = this.partitions.map(function (v, i) {
				if (i >= coalesce(self.limit, DEFAULT_QUERY_LIMIT)) return undefined;
				v.style = coalesce(v.style, {});
				var temp = v.parent;
				v.parent=undefined;
				var output = Map.clone(v);
				v.parent=temp;
				output.esfilter = useFullFilter ? v.fullFilter : v.esfilter;
				return output;
			})
		} else if (param.depth == 1) {
			partitions = [];
			var rownum = 0;
			self.partitions.forall(function (part, i) {
				if (i >= coalesce(self.limit, DEFAULT_QUERY_LIMIT)) return undefined;
				rownum++;
				part.partitions.forall(function (subpart, j) {
					var temp = subpart.parent;
					subpart.parent=undefined;
					var newPart= Map.clone(subpart);
					subpart.parent=temp;

					newPart.name = [subpart.name, subpart.parent.name].join(param.separator);
					newPart.esfilter = useFullFilter ? subpart.fullFilter : subpart.esfilter;
					newPart.style = Map.setDefault({}, subpart.style, subpart.parent.style);
					partitions.append(newPart);
				})
			})
		} else {
			Log.error("deeper than 2 is not supported yet")
		}//endif

		var output = {
			"type": this.type,
			"name": this.name,
			"partitions": partitions,
			"min": this.min,
			"max": this.max,
			"interval": this.interval,
			//THE COMPLICATION IS THAT SOMETIMES WE WANT SIMPLE PARTITIONS, LIKE
			//STRINGS, DATES, OR NUMBERS.  OTHER TIMES WE WANT PARTITION OBJECTS
			//WITH NAME, VALUE, AND OTHER MARKUP.
			//USUALLY A "set" IS MEANT TO BE SIMPLE, BUT THE end() FUNCTION IS
			//OVERRIDES EVERYTHING AND IS EXPLICIT.  - NOT A GOOD SOLUTION BECAUSE
			//end() IS USED BOTH TO INDICATE THE QUERY PARTITIONS *AND* DISPLAY
			//COORDINATES ON CHARTS

			//PLEASE SPLIT end() INTO value() (replacing the string value) AND
			//label() (for presentation)
			"value": (!this.value && this.partitions) ? "name" : this.value,
			"label": coalesce(this.label, (this.type == "set" && this.name !== undefined) ? function (v) {
				return v.name;
			} : undefined),
			"end": coalesce(this.end, (this.type == "set" && this.name !== undefined) ? function (v) {
				return v;
			} : undefined),  //I DO NOT KNOW WHY IS NOT return v.name
//			"value":(!this.value && this.partitions) ? "name" : this.value,
			"isFacet": this.isFacet

		};
		return output;


//		var output=Map.copy(this);
//		output.field=undefined;
//		output.parent=undefined;
//		output["default"]=undefined;
//		output.index=undefined;
//		return Map.copy(output);
	},//method

	"getSelect": function (param) {
		var domain = this.getDomain(param);
		if (domain.getKey === undefined) domain.getKey = function (v) {
			return v.name;
		}; //BASIC COMPILE
		if (domain.NULL === undefined) domain.NULL = {"name": "Other"};

		var output = {
			"name": this.name,
			"value": MVEL.Parts2TermScript(coalesce(param !== undefined ? param.index : null, this.index), domain)
		};
		return output;
	}
};


(function () {



	////////////////////////////////////////////////////////////////////////////
	// BUILD A HIERARCHY BY REPEATEDLY CALLING THIS METHOD WITH VARIOUS childPaths
	// count IS THE NUMBER FOUND FOR THIS PATH
	function addParts(parentPart, childPath, count, index) {
		if (index === undefined) index = 0;
		if (index == childPath.length) return;
		var c = childPath[index];
		parentPart.count = coalesce(parentPart.count, 0) + count;

		if (parentPart.partitions === undefined) parentPart.partitions = [];
		for (var i = 0; i < parentPart.partitions.length; i++) {
			if (parentPart.partitions[i].name == c.name) {
				addParts(parentPart.partitions[i], childPath, count, index + 1);
				return;
			} //endif
		}//for
		parentPart.partitions.push(c);
//		parentPart[c.name]=c;  //HANDLED BY convertPart()
//		c.parent=parentPart;    //HANDLED BY convertPart()
		addParts(c, childPath, count, index + 1);
	}//method


	Dimension.addEdges = function (lowerCaseOnly, parentDim, edges) {
		function convertPart(part, siblingFilters) {
			if (part.partitions) {
				var otherFilters = [];
				part.partitions.forall(function (p, i) {
					var siblingFilter = p.esfilter;
					p.parent = part;
					convertPart(p, otherFilters);
					if (siblingFilter != undefined) otherFilters.append(siblingFilter);
					p.value = coalesce(p.value, p.name);
					if (part.index) p.index = part.index;   //COPY INDEX DOWN
					part[p.name] = coalesce(part[p.name], p);
				});
			}//endif

			if (part.esfilter) {
				if (part.parent && part.parent.esfilter !== undefined && part.parent.esfilter.match_all === undefined) {
					part.esfilter = {"and": [part.parent.esfilter, part.esfilter]}
				}//endif
//				TOO EXPENSIVE FOR ES TO CALCULATE, NEED AN EQUATION SIMPLIFIER
				part.fullFilter = {"and": [part.esfilter]};
				if (siblingFilters !== undefined) {
					part.fullFilter["and"].appendArray(siblingFilters.map(function (f) {
						return {"not": f}
					}))
				}//endif
				if (lowerCaseOnly) part.esfilter = convert.json2value(convert.value2json(part.esfilter).toLowerCase());
			} else if (part.partitions) {
				//DEFAULT esfilter IS THE UNION OF ALL CHILD FILTERS
				if (part.partitions.length > 600) Log.error("Must define an esfilter on " + part.name + ", there are too many partitions (" + part.partitions.length + ")");
				part.esfilter = {"or": part.partitions.select("esfilter")};
			} else if (part.edges) {
				//DEFAULT esfilter IS THE UNION OF ALL CHILD FILTERS
				if (part.edges.length > 600) Log.error("Must define an esfilter on " + part.name + ", there are too many partitions (" + part.partitions.length + ")");
				part.esfilter = {"or": part.edges.select("esfilter")};
			}//endif
		}

		function convertDim(dim) {
			if (dim.edges) {
				//ALLOW ACCESS TO SUB-PART BY NAME (IF ONLY THERE IS NO NAME COLLISION)
				dim.edges.forall(function (e, i) {
					dim[e.name] = coalesce(dim[e.name], e);
					e.parent = dim;
					if (dim.index) e.index = dim.index;   //COPY INDEX DOWN
					convertDim(e);
				});
			}//endif

			convertPart(dim);

			if (dim.limit === undefined) dim.limit = DEFAULT_QUERY_LIMIT;

			if (dim.field !== undefined && Qb.domain.PARTITION.contains(dim.type) && dim.partitions === undefined) {
				dim.field = Array.newInstance(dim.field);

				dim.partitions = Thread.run(function*() {
					//IF dim.field IS A NUMBER, THEN SET-WISE EDGES DO NOT WORK (CLASS CAST EXCEPTION)
					var edges = dim.field.map(function (f) {
						return {"name": f, "value": f}
					});

					if (dim.path !== undefined) {
						var a = Log.action("Get parts of " + dim.name, true);
						var parts = yield (ESQuery.run({
							"from": dim.index,
							"select": {"name": "count", "value": "1", "aggregate": "count"},
							"edges": edges,
							"esfilter": dim.esfilter,
							"limit": dim.limit
						}));
						Log.actionDone(a);

						var d = parts.edges[0].domain;

						if (edges.length > 1) Log.error("Not supported yet");
						//EACH TERM RETURNED IS A PATH INTO A PARTITION TREE
						var temp = {"partitions": []};
						parts.cube.forall(function (count, i) {
							var a = dim.path(d.end(d.partitions[i]));
							if (!(a instanceof Array)) Log.error("The path function on " + dim.name + " must return an ARRAY of parts");
							addParts(
								temp,
								dim.path(d.end(d.partitions[i])),
								count,
								0
							);
						});
						if (dim.value === undefined) dim.value = "name";
						dim.partitions = temp.partitions;
					} else if (edges.length == 1) {
						var a = Log.action("Get parts of " + dim.name, true);
						try{
							var parts = yield (ESQuery.run({
								"from": dim.index,
								"select": {"name": "count", "value": "1", "aggregate": "count"},
								"edges": edges,
								"esfilter": dim.esfilter,
								"limit": dim.limit
							}));
						}finally{
							Log.actionDone(a);
						}

						var d = parts.edges[0].domain;

						dim.value = "name";  //USE THE "name" ATTRIBUTE OF PARTS

						//SIMPLE LIST OF PARTS RETURNED, BE SURE TO INTERRELATE THEM
						dim.partitions = parts.cube.map(function (count, i) {
							var part = {
								"name": "" + d.partitions[i].name,  //CONVERT TO STRING
								"value": d.end(d.partitions[i]),
								"esfilter": {"term": Map.newInstance(dim.field[0], d.partitions[i].value)},
								"count": count
							};
							return part;
						});
					} else if (edges.length == 2) {
						dim.value = "name";  //USE THE "name" ATTRIBUTE OF PARTS

						//TODO: THIS IS REALLY SLOW!!
						var a = Log.action("Get parts of " + dim.name, true);
						try{
							var parts = yield (ESQuery.run({
								"from": dim.index,
								"select": {"name": "count", "value": "1", "aggregate": "count"},
								"edges": edges,
								"esfilter": dim.esfilter,
								"limit": dim.limit
							}));
						}finally{
							Log.actionDone(a);
						}


						var d = parts.edges[0].domain;
						var d2 = parts.edges[1].domain;

						//SIMPLE LIST OF PARTS RETURNED, BE SURE TO INTERRELATE THEM
						dim.partitions = parts.cube.map(function (subcube, i) {
							var part = {
								"name": "" + d.partitions[i].name,  //CONVERT TO STRING
								"value": d.end(d.partitions[i]),
								"esfilter": {"term": Map.newInstance(dim.field[0], d.partitions[i].value)},
								"count": aMath.sum(subcube)
							};
							part.partitions = subcube.map(function (count2, j) {
								if (count2 > 0) {  //ONLY INCLUDE PROPERTIES THAT EXIST
									return {
										"name": "" + d2.partitions[j].name,  //CONVERT TO STRING
										"value": d.end(d.partitions[i]) + "." + d2.end(d2.partitions[j]),
										"parent": part,
										"esfilter": {"and": [
											{"term": Map.newInstance(dim.field[0], d.partitions[i].value)},
											{"term": Map.newInstance(dim.field[1], d2.partitions[j].value)}
										]},
										"count": count2
									};
								}//endif
							});
							return part;
						});
					} else {
						Log.error("Not supported")
					}//endif

					convertPart(dim);//RELATE THE PARTS TO THE PARENTS

					yield (null)
				});

			}//endif


//			dim.isFacet=true;		//FORCE TO BE A FACET IN ES QUERIES
			if (dim.type === undefined) dim.type = "set";

			//ADD CONVENIENCE METHODS
			Map.copy(Dimension.prototype, dim);
		}

		// CODE STARTS HERE
		edges.forall(function (e, i) {
			convertDim(e);
			parentDim[e.name] = e;
			e.parent = parentDim;

			parentDim.edges.push(e);
		});

	};


})();
