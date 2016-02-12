
var layoutAll;       //INITIAL LAYOUT
var dynamicLayout;   //SUBSEQUENT LAYOUT WHEN DOM CHANGES

(function(){
	var DELAY_JAVASCRIPT = 200;
	var DEBUG = false;

	var allExpression = undefined;
	var mapSelf2DynamicFormula = undefined;

	layoutAll = function layout_all(){
		allExpression = [];
		mapSelf2DynamicFormula = {"v": {}, "h": {}};

		//GRASP ALL LAYOUT FORMULA
		$("[layout]").each(function(){
			var self = $(this);
			var layout = self.attr("layout");
			parseLayout(self, layout);
		});

		//FIND THE STATIC RELATIONS (USING CSS, WHEN POSSIBLE)
		var mapSelf2TrueParents = {};
		var mapSelf2StaticFormula = {};
		forall(allExpression, function(e){
			if (!mapSelf2TrueParents[e.l.id]) mapSelf2TrueParents[e.l.id]=[];
			if (mapSelf2TrueParents[e.l.id].indexOf(e.r.id)==-1) mapSelf2TrueParents[e.l.id].push(e.r.id);
		});

		//PICK A SINGLE PARENT FOR EACH ELEMENT
		items(mapSelf2TrueParents, function(selfID, parents){
			var self = $("#" + selfID);
			var parent = $("#" + parents[0]);
			prep(self, parent);
		});

		//PARTITION INTO VERTICAL/HORIZONTAL AND MARK AS STATIC/DYNAMIC
		forall(['v', 'h'], function(dim){
			forall(allExpression, function(e){
				if (e.d != dim) return;
				var lhsID=e.l.id;
				if (e.r.id == mapSelf2TrueParents[lhsID][0]){
					if (!mapSelf2StaticFormula[lhsID]) mapSelf2StaticFormula[lhsID] = {"v": [], "h": []};
					mapSelf2StaticFormula[lhsID][dim].push(e);
					e.dynamic = false;
				} else {
					e.dynamic = true;
				}//endif
				if (!mapSelf2DynamicFormula[dim][lhsID]) mapSelf2DynamicFormula[dim][lhsID]=[];
				mapSelf2DynamicFormula[dim][lhsID].push(e);
			});
		});

		//FIND PURE CSS SOLUTIONS
		//FIND CSS WIDTH/HEIGHT DESIGNATIONS
		items(mapSelf2StaticFormula, function(selfID, rel){
			var self = $("#" + selfID);
			items(dimMap, function(dim, mapper){
				//formula IS A {"l":{}, "r":{}} OBJECT DESCRIBING THE LHS AND RHS OF THE EQUATION RESPECTIVLY
				var formula = rel[dim];
				if (formula.length == 0) {
					//DEFAULT IS TO CENTER EVERYTHING
					self.css(mapper.left, "50%");
					self.css("transform", translate(-0.5, dim));
					return;
				}//endif

				formula = removeRedundancy(formula);
				if (formula.length == 1) {
					//position ONLY
					formula = formula[0];
				} else if (formula.length == 2) {
					//HEIGHT
					if (formula[0].l.coord == formula[1].l.coord && formula[0].r.coord != formula[1].r.coord) {
						Log.error("Constraint is wrong");
					}//endif

					if (formula[0].l.coord > formula[0].r.coord){
						var temp = formula[0];
						formula[0] = formula[1];
						formula[1] = temp;
					}//endif

					self.css("box-sizing", "border-box");
					self[mapper.width]((100 * (formula[0].r.coord - formula[1].r.coord) / (formula[0].l.coord - formula[1].l.coord)) + "%");

					if (formula[0].l.coord == formula[0].r.coord) {
						formula = formula[0];
					} else if (formula[1].l.coord == formula[1].l.coord) {
						formula = formula[1];
					} else {
						Log.error("Do not know how to handle");
					}//endif
				} else {
					Log.error("too many constraints")
				}//endif

				//POSITION
				if (formula.l.coord == 0.0) {
					if (DEBUG){
						Log.note(selfID+"."+mapper.left+"="+ (100 * formula.r.coord) + "%");
					}//endif
					self.css(mapper.left, (100 * formula.r.coord) + "%");
				} else if (formula.l.coord != 1.0) {
					if (DEBUG){
						Log.note(selfID + "." + mapper.left + "=" + (100 * (1 - formula.r.coord)) + "%");
						Log.note(selfID + ".transform=translate(" + (-formula.l.coord) + "," + dim + ")");
					}//endif
					self.css(mapper.left, (100 * formula.r.coord) + "%");
					self.css("transform", translate(-formula.l.coord, dim));
				} else if (formula.l.coord == 1.0) {
					if (DEBUG){
						Log.note(selfID + "." + mapper.right + "=" + (100 * (1 - formula.r.coord)) + "%");
					}//endif
					self.css(mapper.right, (100 * (1 - formula.r.coord)) + "%");
				} else {
					Log.error("can not handle lhs of " + formula.l.coord);
				}//endif
			});
		});

		//POSSIBLE TOPOLOGICAL ORDERING
		//APPLY ALL FORMULA
		positionDynamic();
	};//function

	function translate(amount, dim){
		if (dim == "h") {
			return "translate(" + (amount * 100) + "%, 0)";
		} else {
			return "translate(0, " + (amount * 100) + "%)";
		}//endif
	}//function


	var dimMap = {
		"v": {
			"left": "top",
			"right": "bottom",
			"width": "height",
			"outerWidth": "outerHeight"
		},
		"h": {
			"left": "left",
			"right": "right",
			"width": "width",
			"outerWidth": "outerWidth"
		}
	};

	function positionDynamic(){
		//COMPILE FUNCTION TO HANDLE RESIZE
		var layoutFunction = "function(){\n";
		items(dimMap, function(dim, mapper){

			//TOPOSORT THE EXPRESSIONS IN THIS DIMENSION
			var edges=map(allExpression, function(e){
				if (e.d != dim) return;
				return [e.r.id, e.l.id];
			});
			var sortedID = toposort(edges);

			map(sortedID, function(lhsID){
				var formula = mapSelf2DynamicFormula[dim][lhsID];
				if (!formula) return;  //TOPOSORT WILL INCLUDE NODES WITH NO FORMULA (THE BASIS)
				formula = removeRedundancy(formula);

				var position = undefined;
				var width = undefined;

				//FIGURE OUT IF WE ARE SETTING THE WIDTH, OR POSITION, OR BOTH
				//formula==true MEANS THE FORMULA IS ENCODED AS CSS RULES, SO NOT DYNAMIC
				if (formula.length==1){
					if (!formula[0].dynamic) return;  //NOTHING DYNAMIC
					position = formula[0];
				}else if (formula.length == 2) {
					if (!formula[0].dynamic) {
						if (!formula[1].dynamic) {
							return;  //NOTHING DYNAMIC
						} else {
							position = formula[0];  //NOT DYNAMIC
							width = formula[1];  //DYNAMIC
						}//endif
					} else {
						if (!formula[1].dynamic) {
							position = formula[1];  //NOT DYNAMIC
							width = formula[0];  //DYNAMIC
						} else {
							position = formula[0];  //DYNAMIC
							width = formula[1];  //DYNAMIC
						}//endif
					}//endif
				}else{
					Log.error("More than two expressions along one dimension can not be solved.");
				}//endif

				//RHS WIDTH
				var points = map([position, width], function(e){
					if (e===undefined) return;
					var leftPosition = '$("#' + e.r.id + '").offset().' + mapper.left;
					var pixelWidth = '$("#' + e.r.id + '").' + mapper.outerWidth + '()';

					var code;
					if (e.r.coord == 0) {
						code = leftPosition;
					} else if (e.r.coord == 1) {
						code = '(' + leftPosition + ' + ' + pixelWidth + ')'
					} else {
						code = '(' + leftPosition + ' + ' + pixelWidth + ' * ' + e.r.coord + ')';
					}//endif

					return {
						"l": e.l.coord,
						"rcode": code
					};
				});

				//WIDTH IS ALWAYS DYNAMIC
				var selfWidth;
				if (width) {
					selfWidth = '((' + points[0].rcode + '-' + points[1].rcode + ')/' + (points[0].l - points[1].l) + ')';
					layoutFunction += '$("#' + lhsID + '").' + mapper.outerWidth + '(' + selfWidth + ');\n';
				}else{
					selfWidth = '$("#' + lhsID + '").' + mapper.outerWidth + '()';
				}//endif

				//POSITION ALWAYS EXISTS
				if (position.dynamic){
					var leftMost;
					if (points[1]) {
						leftMost = 'Math.min(' + points[0].rcode + ',' + points[1].rcode + ')';
					} else {
						leftMost = points[0].rcode
					}//endif

					if (points[0].l==0){
						layoutFunction += '$("#' + lhsID + '").offset({"' + mapper.left+'": '+ leftMost+ '});\n';
					}else{
						selfWidth = '$("#' + lhsID + '").' + mapper.outerWidth + '()';
						layoutFunction += '$("#' + lhsID + '").offset({"' + mapper.left+'": '+ leftMost+'-'+selfWidth+'*'+points[0].l+'});\n';
					}//endif
				}//endif

			});
		});
		layoutFunction += "};";

		if (DEBUG){
			Log.note("dynamicLayout="+layoutFunction)
		}//endif

		eval("dynamicLayout=" + layoutFunction);
		dynamicLayout();
		dynamicLayout = debounce(dynamicLayout, DELAY_JAVASCRIPT);
		window.onresize = dynamicLayout;
	}//function


	function prep(self, parent){
		var parentID=parent.attr("id");
		if (DEBUG) Log.note(self.attr("id")+" is child of "+parentID);

		if (parentID=="window"){
			self.css({"position": "fixed"});
			return;
		}//endif

		var p = parent.css("position");
		if (p === undefined || p=="static") {
			parent.css({"position": "relative"});
		}//endif

		if (self.parent().attr("id") != parentID) {
			parent.append(self);
		}//endif
		self.css({"position": "absolute"})
	}//method


	var canonical = {
		"top": {"v": 0.0},
		"middle": {"v": 0.5},
		"bottom": {"v": 1.0},
		"left": {"h": 0.0},
		"center": {"h": 0.5},
		"right": {"h": 1.0},
		"tr": {"v": 0.0, "h": 1.0},
		"rt": {"v": 0.0, "h": 1.0},
		"tc": {"v": 0.0, "h": 0.5},
		"ct": {"v": 0.0, "h": 0.5},
		"tl": {"v": 0.0, "h": 0.0},
		"lt": {"v": 0.0, "h": 0.0},
		"mr": {"v": 0.5, "h": 1.0},
		"rm": {"v": 0.5, "h": 1.0},
		"mc": {"v": 0.5, "h": 0.5},
		"cm": {"v": 0.5, "h": 0.5},
		"ml": {"v": 0.5, "h": 0.0},
		"lm": {"v": 0.5, "h": 0.0},
		"br": {"v": 1.0, "h": 1.0},
		"rb": {"v": 1.0, "h": 1.0},
		"bc": {"v": 1.0, "h": 0.5},
		"cb": {"v": 1.0, "h": 0.5},
		"bl": {"v": 1.0, "h": 0.0},
		"lb": {"v": 1.0, "h": 0.0}
	};

	var canonical_vertical = {
		"top": 0.0,
		"middle": 0.5,
		"bottom": 1.0,
		"t": 0.0,
		"m": 0.5,
		"b": 1.0
	};

	var canonical_horizontal = {
		"left": 0.0,
		"center": 0.5,
		"right": 1.0,
		"l": 0.0,
		"c": 0.5,
		"r": 1.0
	};

	function parseLayout(self, layout){
		forall(layout.split(";"), function(f){
			parseFormula(self, f);
		});
	}//function

	function parseFormula(self, expr){
		if (expr.trim().length == 0) return;

		var temp = expr.split("=");
		if (temp.length!=2) Log.error("Formula "+convert.value2json(expr)+" is not in <lhs> \"=\" <rhs> format");
		var lhs = temp[0].trim();
		var rhs = temp[1].trim();

		if (lhs.startsWith("..")) {
			Log.error("'..' on left of assignment is not allowed")
		} else if (lhs.startsWith(".")) {
			lhs = lhs.substring(1);
		}//endif

		var selfID = getID(self);
		var coord = canonical[lhs];
		if (coord===undefined){
			Log.error("Can not recognize "+convert.value2json(lhs))
		}//endif
		rhs = parseRHS(self, rhs);

		forall(["v", "h"], function(d){
			if (coord[d] !== undefined) {
				allExpression.push({
					"l": {"id": selfID, "coord": coord[d]},
					"r": {"id": rhs.id, "coord": rhs.coord[d]},
					"d": d
				});
			}//endif
		});

	}//function

	function getID(element){
		var output = element.attr('id');
		if (output === undefined) {
			output = "uid_" + UID();
			element.attr('id', output);
		}//endif
		return output;
	}//function

	function parseRHS(self, rhs){
		try {
			var parentID;
			var coord;

			if (rhs.startsWith(".")) {
				//EXPECTING List(".") <coordinates>
				var parent = self;
				coord = rhs.substring(1);
				while (coord.startsWith(".")) {
					parent = parent.parent();
					coord = coord.substring(1);
				}//while
				parentID = getID(parent);
			} else {
				//EXPECTING <parentName> "." <coordinates>
				var temp = rhs.split(".");
				parentID = temp[0];

				if (parentID == "page") {
					var body = $("body");
					parentID = body.attr("id");
					if (parentID === undefined) {
						body.attr("id", "page");
						parentID = "page";
					}//endif
				} else if (parentID == "window") {
					var w = $("#window");
					if (w.length == 0) {
						$("html").css({"height":"100%"});
						body = $("body");
						body.css({"position": "relative", "height":"100%"});
						body.prepend('<div id="window" style="position:fixed;top:0;left:0;right:0;bottom:0;z-index:-1000"></div>');
					}//endif
				}//endif

				coord = temp[1];
			}//endif

			if (coord.indexOf("[") >= 0) {
				temp = coord.split("[");
				if (temp.length == 3) {
					//EXPECTING "[" <h-expression> "]" "[" <v-expression> "]"
					coord = {"v": eval(temp[1].split("]")[0]), "h": eval(temp[2].split("]")[0])};
				} else if (temp[0].length == 0) {
					//EXPECTING "[" <h-expression> "]" <v-position>
					coord = {"v": eval(temp[0].split("]")[0]), "h": canonical_horizontal[temp[1]]};
				} else {
					//EXPECTING <h-position> "[" <v-expression> "]"
					coord = {"v": canonical_vertical[temp[0]], "h": eval(temp[1].split("]")[0])};
				}//endif
			} else {
				//EXPECTING <h-position> <v-position>
				coord = canonical[coord]
			}//endif

			return {"id": parentID, "coord": coord};
		}catch(e){
			Log.error("Problem parsing "+convert.value2json(rhs), e);
		}//try
	}//function

	function removeRedundancy(formula){
		var output = [];
		for (var i = 0; i < formula.length; i++) {
			var g = formula[i];
			for (var u = 0; u < i; u++){
				try {
					if (g.l.coord == formula[u].l.coord && g.r.coord == formula[u].r.coord && g.r.id == formula[u].r.id) {
						g = undefined;
						break;
					}//endif
				}catch(e){
					Log.error("Logic error!");
				}//try
			}//for
			if (g) output.push(g);
		}//for
		return output;
	}//function


	// RETURN FAST, AND QUEUE f FOR EXECUTION
	// ENSURE f WILL ALWAYS RUN AFTER LAST CALL
	function debounce(f){
		var pending = false;
		var running = false;

		function output(){
			var self = this;
			var args = arguments;

			if (running){
				pending = pending || true;
				return;
			}//endif
			running=true;
			pending=false;

			var g = function(){
				f.apply(self, args);
				running=false;
			};
			setTimeout(g, 0);
			output();
		}//function
		return output;
	}//function

	function forall(self, func){
		for (var i = 0; i < self.length; i++) {
			func(self[i], i, self);
		}//for
		return self;
	}//function

	function items(map, func){
		//func MUST ACCEPT key, value PARAMETERS
		var keys=Object.keys(map);
		for(var i=keys.length;i--;){
			var key=keys[i];
			var val=map[key];
			if (val!==undefined) func(key, val);
		}//for
	}//function

	var UID;
	(function(){
		var next=0;

		UID=function(){
			next++;
			return next;
		};//method
	})();

	function map(arr, func){
		var output=[];
		for(var i=0;i<arr.length;i++){
			var v=func(arr[i], i);
			if (v===undefined || v==null) continue;
			output.push(v);
		}//for
		return output;
	}//method

	function toposort(edges, nodes){
		/*
		edges IS AN ARRAY OF [parent, child] PAIRS
		nodes IS AN OPTIONAL ARRAY OF ALL NODES
		 */
		if (!nodes){
			nodes = uniqueNodes(edges);
		}//endif
		var sorted = [];
		var visited = {};

		function visit(node, predecessors){
			if (predecessors.indexOf(node) >= 0) {
				Log.error(node + " found in loop" + JSON.stringify(predecessors))
			}//endif

			if (visited[node]) return;
			visited[node] = true;

			edges.forEach(function(edge){
				if (edge[1] == node) {
					visit(edge[0], predecessors.concat([node]))
				}//endif
			});
			sorted.push(node);
		}//function

		nodes.forEach(function(n){
			if (!visited[n]) visit(n, [])
		});
		return sorted;
	}

	function uniqueNodes(arr){
		var res = {};
		arr.forEach(function(edge){
			res[edge[0]] = true;
			res[edge[1]] = true;
		});
		return Object.keys(res);
	}//function

})();
