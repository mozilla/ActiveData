


var PickDim=function(div, dimensions, callback){
	var self=this;

	this.div=$(div);
	this.dimensions=dimensions;
	this.callback=callback;
	this.div.css("position:relative;overflow:visible;");
	this.div.append(
		'<table class="layout" style="width:100%;"><tbody></tbody></table>'
	);
	this.cover=$('<div style="width:100%;height:100%;top:0px;left:0px;position:absolute;z-index: 1;"></div>');  //CATCH MOUSE AND CLICKS
	this.div.append(this.cover);

	this.focus=undefined;	//CURRENT SHOWN PARTITIONS (REFERENCES THE MAIN DIMENSION HIERARCHY, DO NOT CHANGE)
	this.filters=[];		//STACK OF FILTERS

	this.addPart(self.dimensions);
};

(function(){

var DEFAULT_CHILD_LIMIT=20;     //IN THE EVENT THE PARTITION DOES NOT DECLARE A LIMIT, IMPOSE ONE SO THE GUI IS NOT OVERWHELMED

var BUTTON_SIZE=20;
var ADD_BUTTON='<div class="smalltoolbutton" title="Add Filter"><img src="'+Settings.imagePath+'/Add.png"  style="height:'+BUTTON_SIZE+'px;width:'+BUTTON_SIZE+'px;"></div>';
var REMOVE_BUTTON='<div class="smalltoolbutton" title="Remove Filter"><img src="'+Settings.imagePath+'/Remove.png"  style="height:'+BUTTON_SIZE+'px;width:'+BUTTON_SIZE+'px;"></div>';





//SELECT DIM/PART
PickDim.prototype._findPart=function(name){
	var selected;

	if (!this.focus) Log.error("Must add a dimension before finding a part");
	var parts = nvl(this.focus.edge.edges, this.focus.edge.partitions);
	if (!parts) Log.error("No part by name of " + name);

	parts.forall(function(v, i){
		if (v.name == name) selected = v;
	});
	if (!selected)
		Log.error("No part by name of " + name);

	return selected;
};//method

//MAKE A NEW ADD BUTTON.  THE CLICK HANDLER IS LOST IF remove() RUN ON IT
PickDim.prototype._addButton=function(row){
	var self=this;
	self.addButton=$(ADD_BUTTON);
	row.find("td").eq(1).prepend(self.addButton);

	self.addButton.click(function(e){
		self.addPart(self.dimensions);
	});
};//method



////////////////////////////////////////////////////////////////////////////////
// "SELECT" IS FOR CHOOSING THE PARTS AVAILABLE ON A DIMENSION
////////////////////////////////////////////////////////////////////////////////
PickDim.prototype.select = function(name){
	var selected = this._findPart(name);

	if (this.focus.edge.edges){
		//CAN ONLY SELECT ONE EDGE AT A TIME
		var self=this;
		//FIND DIV FOR THE PART, AND UNSELECT
		Map.forall(this.focus.selected, function(k, v){
			if (v){
				self.unselect(k);
			}//endif
//			this.focus.div.find(".dim-part").removeClass("selected");
		});
	}//endif
	this.focus.selected[name] = selected;
	this.addPart(selected);
	return selected;
};

PickDim.prototype.unselect = function(name){
	var selected = this._findPart(name);

	//FIND DIV FOR THE PART, AND UNSELECT
	this.focus.div.find('.dim-part[name$="'+name+'"]').removeClass("selected");
	this.focus.selected[name] = undefined;
};

//RETURNS THE CURRENT SELECTED STATE
PickDim.prototype.toggle = function(name){
	if (this.focus.selected[name]){
		this.focus.selected[name]=undefined;
		return undefined;
	}//endif

	return this.select(name);
};


//GET AVAILABLE LIST

//ADD CHILDREN PARTS

// ADD NEW TOP LEVEL DIMENSION SELECTOR
//PickDim.prototype.addNew=function(){
//	this.addPart(Mozilla);
//};//method

PickDim.prototype.addPart=function(edge){
	//NO NEED TO SHOW ANOTHER LEVEL, IF THERE ARE NONE
	var parts=nvl(edge.edges, edge.partitions);
	if (!parts) return;


	//IF NOTHING SELECTED ON focus THEN REMOVE IT.
	if (this.focus){
		if (countAllKey(this.focus.selected)==0){
			//NOTHING SELECTED MEANS EITHER PARTS SHOWING, OR USELESS EDGE
			this.focus.div.remove();
		}else{
			this.filters.prepend(this.focus);
		}//endif
	}//endif


	var row=$(
		'<tr>'+
			'<td><div class="dim-container"><div class="position">'+
				parts.map(function(v, i){
					if (i>=nvl(edge.limit, DEFAULT_CHILD_LIMIT)) return undefined;
					return '<span class="dim-part" name="'+v.name+'">'+v.name+'</span>';
				}).join("")+
			'</div></td>'+
			'<td class="dim-addremove">'+
				REMOVE_BUTTON+
			'</td>'+
		'</tr>'
	);

	var removeButton=row.find(".smalltoolbutton").first();
	removeButton.click(function(e){
		self.removeEdge(focus);
		if (self.callback) self.callback(self.getQuery());
	});

	if (this.addButton) this.addButton.remove();
	this._addButton(row);

	var focus={"edge":edge, "selected":{}, "div":row};

	var self=this;
	var pos=row.find(".position");

	var container=row.find(".dim-container").first();
	container.mousemove(function(e){
	//DYNAMIC OFFSET
		if (container.width() < pos.width()){
//
//			range({
//				"domain":{
//					"min":row.offset().left+left,
//					"range":oWidth-left-right
//				},
//				"codomain":{
//					"min": oX,
//					"range":oWidth - iWidth
//				}
//			})(dx);
//
			var left=pos.find(".dim-part").first().width()/2;
			var right=pos.find(".dim-part").last().width()/2;

			var oWidth=container.width();
			var iWidth=pos.width();
			var eX=e.pageX;
			var oX=row.offset().left;
			var dx=eX-oX-left;
			dx=aMath.max(0, aMath.min(dx, oWidth-left-right));

			var offset=aMath.round(oX + (oWidth - iWidth) * (dx) / (oWidth-left-right));
//			pos.animate({"left":offset}, 400, function(){
				pos.offset({"left":offset});
//			});
		}//endif
	});



	container.mouseleave(function(e){
	//CENTER THE SELECTED PARTS ON EXIT
		if (container.width() < pos.width()){
			var iX=pos.offset().left;
			var selected=pos.find(".selected");
			if (selected.length>0){
				var leftPos=selected.first().offset().left-iX;//RELATIVE TO PARENT
				var right=selected.last();
				var rightPos=right.offset().left+right.width()-iX;

				var oWidth=container.width();
				var iWidth=pos.width();
				var offset=(oWidth-leftPos-rightPos)/2;

				offset=aMath.min(0, aMath.max(offset, oWidth-iWidth));
				pos.animate({"left":offset}, 700, function(){
					pos.position({"left":offset});
				});
			}//endif
		}//endif
	});

	////////////////////////////////////////////////////////////////////////////
	// CLICK
	////////////////////////////////////////////////////////////////////////////
	row.find(".dim-part").click(function(e){
		var o=$(this);
		var selected=self.toggle(o.attr("name"));
		if (selected){
//			self.focus.selected[name]=selected;
			o.addClass("selected");
		}else{
			o.removeClass("selected");
		}//endif
		if (self.callback) self.callback(self.getQuery());
	});


	//INSERT AFTER THE FIRST
	this.div.find("tbody").prepend(row);

	if (container.width() > pos.width()) pos.offset({"left":(row.width() - pos.width()) / 2 + row.offset().left});
	this.cover.offset({"top":row.height()+row.offset().top, "left":row.offset().left});
	this.cover.css("width", container.width());

	this.focus=focus;

};//method


//REMOVE THE EDGE BASED ON GIVEN filter={"edge", "selected", "div"}
PickDim.prototype.removeEdge=function(filter){
	//PUSH ON STACK, JUST TO MAKE LOGIC SIMPLE
	if (this.focus) this.filters.prepend(this.focus);
	this.focus=undefined;


	for(var f=0;f<this.filters.length;f++){
		if (this.filters[f]==filter){
			//IF THERE IS A CHILD PART HIGHER IN STACK, DELETE IT TOO
			if (f>0 && !this.filters[f-1].edge.edges){
				this.removeEdge(this.filters[f-1]);
				f--;
			}//endif

			if (f==0){
				if (this.addButton) this.addButton.remove();
				this.addButton=undefined;
			}//endif

			filter.div.remove();
			this.filters.splice(f,1);

			this.focus=this.filters.splice(0,1)[0];
			if (!this.addButton) this._addButton(this.focus.div);
			return;
		}//endif
	}//for

	Log.error("should never happen");

};//method


//RETURN THE QUERY PARTS THAT THIS UI ELEMENT DEFINES
PickDim.prototype.getQuery=function(){
	var self=this;
	var edges=[];
	var filter={"and":[]};

	var numSelected=countAllKey(self.focus.selected);
	var currentSelection=Map.codomain(self.focus.selected);

	//MAKE AN EDGE FOR PARTITIONING
	if (self.focus){

		//ARE WE SHOWING MORE THAN ONE SERIES?
		var selectedParts=self.focus.edge.partitions;
		if (selectedParts && numSelected>0){
			selectedParts=currentSelection;
		}//endif

		if (selectedParts){

			if (self.focus.edge.field!==undefined && self.focus.edge.path===undefined){
				//IF path IS DEFINED, THEN IT'S GUNNA BE COMPLICATED, THIS IS FOR SIMPLE FIELDS ONLY
				//PARTS CAME FROM THE FIELD DEFINITION
				//THE ACTUAL SELECTION CAN BE DONE LOCALLY, SO WE WILL SIMPLY REQUEST ALL PARTS???
				edges.push({
					"name":self.focus.edge.name,
					"value":self.focus.edge.field,
					"domain":{
						"type":self.focus.edge.type,
						"partitions":selectedParts.map(function(p,i){
							if (i>=nvl(self.focus.edge.limit, DEFAULT_CHILD_LIMIT)) return undefined;
							return {
								"name":p.name,
								"value": p.value,
								"esfilter":p.esfilter
							};
						}),
						"value":"value",    //COMPLEX PARTITION OBJECTS, SO NEED TO SPECIFY WHICH ATTRIBUTE TO USE TO LABEL AXIS
						"isFacet":self.focus.edge.isFacet
					}
				});
			}else if (self.focus.edge.isFacet){
				Log.error("not completed");
				//PARTS NOT DEFINED BY FIELD, BUT RATHER AN esfilter

			}else{
				//PARTS NOT DEFINED BY FIELD, BUT RATHER AN esfilter

				//COMPILE INDIVIDUAL PARTS TO MAKE A SINGLE CASE STATEMENT
				//SHOULD NOT DO THIS, THE QUERY OBJECT SHOULD BE ABLE TO HANDLE IT FINE
				//(?MOVE THIS LOGIC OVER TO ESQuery?)

				var edge={
					"name":self.focus.edge.name,
					"domain":{
						"type":"set",
						"partitions":selectedParts.map(function(p,i){
							if (i>=nvl(self.focus.edge.limit, DEFAULT_CHILD_LIMIT)) return undefined;
							return p;
						}),
						"getKey":function(p){return p.name;},
						"NULL":{"name":"Other"}
					}
				};

				edges.push(edge);
			}//endif
		}else if (numSelected>0 && currentSelection[0].field&& Qb.domain.ALGEBRAIC.contains(currentSelection[0].type)){
			//ONLY ONE DIMENSION CAN EVER BE SELECTED
			//ADD SOME min/max/interval ATTRIBUTES
			var s=currentSelection[0];
			edges.push({
				"name":s.name,
				"value":s.field,
				"domain":{
					"type":s.type,
					"min":s.min,
					"max":s.max,
					"interval":s.interval
				}
			});
		}else if (numSelected>0 && currentSelection[0].field && Qb.domain.PARTITION.contains(currentSelection[0].type)){
			var s=currentSelection[0];
			edges.push({
				"name":s.name,
				"value":s.field,
				"domain":{
					"type":s.type,
					"partitions":s.partitions.map(function(p,i){
						return p.value;
					}),
					"isFacet":self.focus.edge.isFacet
				}
			});
		}//endif

		if (self.focus.edge.esfilter){
			filter.and.push(self.focus.edge.esfilter);
		}//endif
	}//endif

	for(var i=0;i<self.filters.length;i++){
		var esfilters=mapAllKey(self.filters[i].selected, function(k, v){
			return v.esfilter;
		});
		if (esfilters.length==0){
			//DO NOTHING
		}else if (esfilters.length==1){
			filter.and.push(esfilters[0]);
		}else{
			filter.and.push({"or":esfilters});
		}//endif
	}//for

	if (filter.and.length==0) filter=undefined;
	return {"edges":edges, "esfilter":filter};
};

})();
