/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


PartitionFilter = function(){};



(function(){


var DEFAULT_CHILD_LIMIT=20;     //IN THE EVENT THE PARTITION DOES NOT DECLARE A LIMIT, IMPOSE ONE SO THE GUI IS NOT OVERWHELMED
var WAITING_FOR_RESULTS={"name":"WAITING_FOR_RESULTS"};

PartitionFilter.newInstance=function(param){
	//treeDepth - TO LIMIT THE TREE SELECTION DEPTH
	//showAll - SET TO true(default) IF THE TOP OPTION IS "All"
	//YOU MAY ALSO PASS A callback FUNCTION THAT ACCEPTS TWO ARRAYS:
	//1st - LIST OF CHECKED NODES
	//2nd - LIST OF PREVIOUSLY CHECKED NODES
	//AND OPTIONALLY RETURNS AN ARRAY OF IDS TO CHECK INSTEAD

	ASSERT.hasAttributes(param, [
		"id",       //TO NAME THE DOM ELEMENTS
		"name",
		"dimension",
		["onlyOne","callback"], //CALLBACK WILL OVERRIDE THE onlyOne SHORTCUT
		["expandDepth","expandAll"]
	]);

	var self=new PartitionFilter();
	Map.copy(param, self);
	self.showAll=coalesce(self.showAll, true);

	if (self.dimension.partitions===undefined && self.dimension.edges===undefined) Log.error(self.dimension.name+" does not have a partition defined");

//	self.id=self.dimension.parent.name.replaceAll(" ", "_");
	if (self.treeDepth===undefined) self.treeDepth=100;
	self.isFilter=true;
	self.treeDone=false;
	self.DIV_ID=self.id.replaceAll(" ", "_")+"_id";
	self.DIV_LIST_ID=self.id.replaceAll(" ", "_")+"_list";
	self.FIND_TREE="#"+convert.String2JQuery(self.DIV_LIST_ID);
	self.disableUI=false;
	self.numLater=0;

	self.selectedIDs=[];
	self.id2part={};        //MAP IDs TO PART OBJECTS
	self.id2node=[];

	self.parents={};
	var tree=convertToTree(self, {"id":undefined}, 0, self.dimension);
	self.hierarchy=tree.children;

	return self;
};


function convertToTreeLater(self, treeNode, dimension){
	self.numLater++;
	GUI.pleaseRefreshLater=true;
	Thread.run(function*(){
		//DO THIS ONE LATER
//		treeNode.children = [];
		if (dimension.partitions instanceof Thread){
			var threadResult = yield (Thread.join(dimension.partitions));
			if (threadResult.threadResponse instanceof Exception){
				Log.error("Can not setup PartitionFilter", threadResult.threadResponse);
			}//endif
		}//while
		var pleaseUpdate = (treeNode.children==WAITING_FOR_RESULTS);
		treeNode.children = dimension.partitions.map(function (v, i) {
			if (i < coalesce(dimension.limit, DEFAULT_CHILD_LIMIT)){
				v.limit = coalesce(v.limit, dimension.limit, DEFAULT_CHILD_LIMIT);
				return convertToTree(self, {}, 1, v);
			}//endif
		});
		var allNode={"id":"__all__", "attr":{"id":"__all__"}, "data":"All"};
		treeNode.children.prepend(allNode);
		self.id2node["__all__"]=allNode;

		if (pleaseUpdate){
			self.hierarchy=treeNode.children;
			self.setSimpleState(self.selectedIDs.join(","));
			GUI.refresh();
		}//endif
		self.numLater--;
		yield (null);
	});
}

function convertToTree(self, parent, depth, dimension){
	//depth IS CURRENT DEPTH  (0==EDGE, 1==DOMAIN PARTITIONS, etc)
	var node={};
	node.id=(parent.id===undefined ? "" : parent.id+".")+dimension.name;
	node.attr={id:node.id};
	node.data=dimension.name;

	self.id2part[node.id]=dimension;   //STORE THE WHOLE EDGE/PART
	self.id2node[node.id]=node;
	self.parents[node.id]=parent;

	if (dimension.partitions){
		if (dimension.partitions instanceof Thread){
			node.children=WAITING_FOR_RESULTS;
			convertToTreeLater(self, node, dimension);
		}else{
			if (depth < self.treeDepth){
				node.children=dimension.partitions.map(function(v,i){
					if (i<coalesce(dimension.limit, DEFAULT_CHILD_LIMIT))
						v.limit = coalesce(v.limit, dimension.limit, DEFAULT_CHILD_LIMIT);
						return convertToTree(self, depth==0 ? {} : node, depth+1, v);
				});
				if (depth==0){
					var allNode={"id":"__all__", "attr":{"id":"__all__"}, "data":"All"};
					node.children.prepend(allNode);
					self.id2node["__all__"]=allNode;
				}//endif
			}//endif
		}//endif
	}//endif
	if (dimension.edges){
		node.children=dimension.edges.map(function(v,i){
			v.limit = coalesce(v.limit, dimension.limit, DEFAULT_CHILD_LIMIT);
			return convertToTree(self, node, 0, v);
		});
	}//endif
	return node;
}//function





PartitionFilter.prototype.getSelectedNodes=function(){
	var self=this;

	//CONVERT SELECTED LIST INTO PART OBJECTS
	return this.selectedIDs.map(function(id){
		return self.id2node[id];
	});
};//method



PartitionFilter.prototype.getSelectedParts=function(){
	var self=this;

	return this.selectedIDs.map(function(id){
		if (id !="__all__")	return self.id2part[id];
	});
};//method



//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
PartitionFilter.prototype.getSimpleState=function(){
	var selected=this.selectedIDs.filter(function(v){ return v!="__all__";});
	if (selected.length==0) return undefined;
	return selected.join(",");
};


//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
PartitionFilter.prototype.setSimpleState=function(value){
	if (!value || value.trim()==""){
		this.selectedIDs=[];
	}else{
		this.selectedIDs=value.split(",");
	}//endif

	//SOME VALUES WILL BE IMPOSSIBLE, SO SHOULD BE REMOVED
	if (this.hierarchy!=WAITING_FOR_RESULTS){
		this.selectedIDs=this.getSelectedNodes().map(function(v, i){return v.id;});
	}//endif
	if (this.selectedIDs.length==0){
		this.selectedIDs=["__all__"];
	}//endif
};


PartitionFilter.prototype.getSummary=function(){
	if (this.selectedIDs.length==0) return this.name+": All";
	return this.name+": "+this.getSelectedNodes().map(function(p){return p.data;}).join(", ");
};//method


PartitionFilter.prototype.makeTree=function(){
	if (this.treeDone) return;      //ALREADY MADE TREE
	if ($(this.FIND_TREE).length==0) return; //NOT BEEN CREATED YET

	var self=this;
	if (self.numLater>0){
		if (self.refreshLater===undefined){
			//WAIT FOR LOADING TO COMPLETE
			self.refreshLater=Thread.run(function*(){
				while(self.numLater>0) yield(Thread.sleep(200));
				self.makeTree();
				self.refreshLater=undefined;
			});
		}//endif
		return;
	}//endif
	this.treeDone=true;

	$(this.FIND_TREE).jstree({
		"json_data":{
			"data":self.hierarchy		 //EXPECTING id, name, children FOR ALL NODES IN TREE
		},
		"themes":{
			"icons":false,
			"dots":false
		},
//		"checkbox":{
//			"two_state":true
//		},
		"plugins":[ "themes", "json_data", "ui", "checkbox" ]
	}).bind("change_state.jstree", function (e, data){
		if (self.disableUI) return;

		var checked = {};
		if (!self.callback && (self.onlyOne || (self.showAll && data.rslt[0].id=="__all__"))){
			//DID WE JUST CHECK OR UNCHECK?
			$(".jstree-checked").each(function(){
				if (data.rslt[0].id==$(this).attr("id")) checked=Map.newInstance(data.rslt[0].id, true);
			});
		}else{
			//FIRST MAKE A HASH OF CHECKED ITEMS
			data.inst.get_checked(null, true).each(function(){
//			$(".jstree-checked").each(function(){
				checked[$(this).attr("id")] = true;
			});
			//CLICKING ON SOMETHING OTHER THAN ALL WILL CLEAR ALL
			if (data.rslt[0].id!="__all__"){
				checked["__all__"]=undefined;
			}//endif
		}//endif

		//WE NOW HAVE A RIDICULOUS NUMBER OF CHECKED ITEMS, REDUCE TO MINIMUM COVER

		//IF PARENT IS CHECKED, THEN DO NOT INCLUDE
		var minCover;
		if (checked["__all__"]){  //ULTIMATE PARENT
			minCover =["__all__"];
		}else{
			minCover= mapAllKey(checked, function(id){
				if (checked[self.parents[id].id]) return;
				return id;
			});
			minCover.sort();
			if (minCover.length==0) minCover=["__all__"];
		}//endif

		//HAS ANYTHING CHANGED?
		var hasChanged = false;
		if (self.selectedIDs.length != minCover.length) hasChanged = true;
		if (!hasChanged) for(var i = minCover.length; i--;){
			if (minCover[i] != self.selectedIDs[i]) hasChanged = true;
		}//for

		if (hasChanged){
			if (self.callback) var oldParts=self.getSelectedParts();

			self.selectedIDs = minCover;

			if (self.callback){
				try{
					var moreChanges=self.callback(self.getSelectedParts(), oldParts);
					if (moreChanges){
						//CONVERT FROM PARTS BACK TO
						var idList=reverseMap(self.id2part, moreChanges);
						self.selectedIDs=idList;
					}
				}catch(e){
					Log.warning("Can not callback to parameter "+self.name, e)
				}//try
			}//endif

			GUI.refresh();
		}//endif
	}).bind("loaded.jstree", function(c, data){
		if (self.expandDepth!==undefined){
			var t=data.inst;
			t.get_container().find('li').each(function(i) {
				if(t.get_path($(this)).length<=self.expandDepth){
					t.open_node($(this));
				}
			});
		}//endif

		if (self.expandAll) $(self.FIND_TREE).jstree('open_all');
		self.refresh();
	});
};


PartitionFilter.prototype.makeHTML=function(){
	return '<div id="'+convert.String2HTML(this.DIV_LIST_ID)+'" style="300px">'+
		'<div id="'+convert.String2HTML(this.DIV_LIST_ID)+'"></div>'+
		'</div>';
};


//RETURN AN ES FILTER
PartitionFilter.prototype.makeFilter = function(){
	var selected = this.getSelectedParts();
	if (selected.length == 0) return ESQuery.TrueFilter;
	return {"or":selected.map(function(v){return v.esfilter;})};
};//method



PartitionFilter.prototype.refresh = function(){
	//FIND WHAT IS IN STATE, AND UPDATE STATE
	this.disableUI=true;
	this.makeTree();
	var selected=this.getSelectedNodes();

	var f=$('#'+convert.String2JQuery(this.DIV_LIST_ID));
	f.jstree("deselect_all");
	f.jstree("uncheck_all");
	selected.forall(function(p){
		f.jstree("select_node", ("#"+convert.String2JQuery(p.id)));
		f.jstree("check_node", ("#"+convert.String2JQuery(p.id)));
	});

	this.disableUI=false;
};


})();
