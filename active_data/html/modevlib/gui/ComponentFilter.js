/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

ComponentFilter = function(){
	this.name="Components";
	this.isFilter=true;
	this.selected=[];
};


ComponentFilter.prototype.makeFilter = function(){
	if (this.selected.length==0) return ESQuery.TrueFilter;
	return {"terms":{"component":this.selected}};
};//method


ComponentFilter.prototype.refresh = function(){
	if (this.refreshThread!==undefined)
		this.refreshThread.kill();

	var self=this;
	this.refreshThread=Thread.run("get components", function*(){
		try{
			var components=yield (ESQuery.run({
				"from":"bugs",
				"select":{"name":"count", "value":"bug_id", "aggregate":"count"},
				"edges":[
					{"name":"term", "value":"component"}
				],
				"esfilter":{"and":[
					Mozilla.CurrentRecords.esfilter,
					Mozilla.BugStatus.Open.esfilter,
					GUI.state.productFilter.makeFilter()   //PULL DATA FROM THE productFilter
				]}
			}));

			components = Qb.Cube2List(components);
			var terms = components.map(function(v, i){return v.term;});

			self.selected = self.selected.intersect(terms);
	//		var self=this;


	//		GUI.State2URL();
			self.injectHTML(components);
			$("#componentsList").selectable({
				selected: function(event, ui){
					var didChange = false;
					if (ui.selected.id == "component_ALL"){
						if (self.selected.length > 0) didChange = true;
						self.selected = [];
					} else{
						if (!self.selected.contains(ui.selected.id.rightBut("component_".length))){
							self.selected.push(ui.selected.id.rightBut("component_".length));
							didChange = true;
						}//endif
					}//endif

					if (didChange) GUI.refresh();
				},
				unselected: function(event, ui){
					var i = self.selected.indexOf(ui.unselected.id.rightBut("component_".length));
					if (i != -1){
						self.selected.splice(i, 1);
						GUI.refresh();
					}
				}
			});
		}catch(e){
			Log.warning("component filter got exception", e);
		}
	});
};


ComponentFilter.prototype.getSummary=function(){
	var html = "Components: ";
	if (this.selected.length == 0){
		html += "All";
	} else{
		html += this.selected.join(", ");
	}//endif
	return html;
};

//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
ComponentFilter.prototype.getSimpleState=function(){
	if (this.selected.length==0) return undefined;
	return this.selected.join(",");
};


ComponentFilter.prototype.setSimpleState=function(value){
	if (!value || value==""){
		this.selected=[];
	}else{
		this.selected=value.split(",").map(function(v){return v.trim();});
	}//endif
//	this.refresh();

};

ComponentFilter.prototype.makeHTML=function(){
	return '<div id="components"></div>';
};//method




ComponentFilter.prototype.injectHTML = function(components){
	var html = '<ul id="componentsList" class="menu ui-selectable">';
	var item = new Template('<li class="{{class}}" id="component_{{name}}">{{name}} ({{count}})</li>');

	//GIVE USER OPTION TO SELECT ALL PRODUCTS
	var total = 0;
	for(var i = 0; i < components.length; i++) total += components[i].count;
	html += item.replace({
		"class" : ((this.selected.length == 0) ? "ui-selectee ui-selected" : "ui-selectee"),
		"name" : "ALL",
		"count" : total
	});

	for(var i = 0; i < components.length; i++){
		html += item.replace({
			"class" : (this.selected.contains(components[i].term) ? "ui-selectee ui-selected" : "ui-selectee"),
			"name" : components[i].term,
			"count" : components[i].count
		});
	}//for

	html += '</ul>';


	$("#components").html(html);
};

