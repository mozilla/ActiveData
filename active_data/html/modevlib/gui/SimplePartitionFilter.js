/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


SimplePartitionFilter = function () {
};


(function () {
	var DEFAULT_CHILD_LIMIT = 20;     //IN THE EVENT THE PARTITION DOES NOT DECLARE A LIMIT, IMPOSE ONE SO THE GUI IS NOT OVERWHELMED

	SimplePartitionFilter.newInstance = function (param) {
		//YOU MAY ALSO PASS A callback FUNCTION THAT ACCEPTS TWO ARRAYS:
		//1st - LIST OF CHECKED NODES
		//2nd - LIST OF PREVIOUSLY CHECKED NODES
		//AND OPTIONALLY RETURNS AN ARRAY OF IDS TO CHECK INSTEAD

		ASSERT.hasAttributes(param, [
			"id",       //TO NAME THE DOM ELEMENT
			"name",     //GUI NAME
			"domain",//DIMENSION/PARTITION WITH PARTS
			"template", //LOOK OF ONE PART
			"separator", //HTML BETWEEN EACH PART
			["onlyOne", "callback"] //CALLBACK WILL OVERRIDE THE onlyOne SHORTCUT
		]);


		var self = new SimplePartitionFilter();
		Map.copy(param, self);

		self.DIV_ID=convert.String2HTML(self.id);
		self.id2DIV_ID=[]; //MAPPING TO PART IDS IN HTML
	};

	SimplePartitionFilter.prototype.makeButtons=function(){
		if (this.dimension.partitions instanceof Thread){
			Log.error("parts have not arrived yet")
		}//endif
		var self=this;
		var style=[];
		var content=[];
		this.domain.partitions.forall(function(part){
			var key=self.domain.getKey(part);
			var partDIV_ID=this.DIV_ID+"_"+convert.String2HTML(key);
			self.id2DIV_ID[key]=partDIV_ID;

			var inst = $(self.template.replace(part))
				.attr("id", partDIV_ID)
				.click(function(){
					var self=$(this);
					var id=self.attr("id");
					if (self.hasClass("selected")){
						self.removeClass("selected");
					}else{
						if (self.onlyOne){
							Map.forall(self.id2DIV_ID, function(k,v){
								self.removeClass("selected");
							});
						}//endif
						self.addClass("selected");
					}//endif
				}).dynamicStyle();
			content.append(inst);
			content.append(self.separator);
		});
		$("#"+self.DIV_ID).html(content);
	};//method


	SimplePartitionFilter.prototype.getSelectedNodes = function () {
		var self = this;

		//CONVERT SELECTED LIST INTO PART OBJECTS
		return this.selectedIDs.map(function (id) {
			return self.id2node[id];
		});
	};//method


	SimplePartitionFilter.prototype.getSelectedParts = function () {
		var self = this;

		return this.selectedIDs.map(function (id) {
			return self.id2part[id];
		});
	};//method


	//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
	SimplePartitionFilter.prototype.getSimpleState = function () {
		if (this.selectedIDs.length == 0) return undefined;
		return this.selectedIDs.join(",");
	};


	SimplePartitionFilter.prototype.setSimpleState = function (value) {
		if (!value || value.trim() == "") {
			this.selectedIDs = [];
		} else {
			this.selectedIDs = value.split(",");
		}//endif

		//SOME VALUES WILL BE IMPOSSIBLE, SO SHOULD BE REMOVED
		if (this.hierarchy != WAITING_FOR_RESULTS)
			this.selectedIDs = this.getSelectedNodes().map(function (v, i) {
				return v.id;
			});
	};


	SimplePartitionFilter.prototype.getSummary = function () {
		if (this.selectedIDs.length == 0) return this.name + ": All";
		return this.name + ": " + this.getSelectedNodes().map(function (p) {
			return p.data;
		}).join(", ");
	};//method


	SimplePartitionFilter.prototype.makeHTML = function () {
		return '<div id="'+convert.String2HTML(this.DIV_ID)+'"></div>';
	};


//RETURN AN ES FILTER
	SimplePartitionFilter.prototype.makeFilter = function () {
		if (this.selectedIDs.length == 0) return ESQuery.TrueFilter;

		var selected = this.getSelectedParts();
		if (selected.length == 0) return ESQuery.TrueFilter;

		var self = this;
		return {"or": selected.map(function (v) {
			return v.esfilter;
		})};
	};//method


	SimplePartitionFilter.prototype.refresh = function () {
		//FIND WHAT IS IN STATE, AND UPDATE STATE
		this.disableUI = true;
		this.makeTree();
		var selected = this.getSelectedNodes();

		var f = $('#' + convert.String2JQuery(this.DIV_LIST_ID));
		f.jstree("deselect_all");
		f.jstree("uncheck_all");
		selected.forall(function (p) {
			f.jstree("select_node", ("#" + convert.String2JQuery(p.id)));
			f.jstree("check_node", ("#" + convert.String2JQuery(p.id)));
		});

		this.disableUI = false;
	};


})();
