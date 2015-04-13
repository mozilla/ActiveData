
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


importScript("../debug/aLog.js");
importScript("../util/aTemplate.js");
importScript("../util/convert.js");


RadioFilter = function(){};

RadioFilter.newInstance=function(param){
	var self = new RadioFilter();
	Map.expecting(param, ["id", "name", "options", "default"]);
	Map.copy(param, self);
	self.description = coalesce(self.description, self.message);
	self.selected=self["default"];
	self.isFilter=true;
	self.doneSetup=false;
	return self;
};


RadioFilter.prototype.getSummary=function(){
	var html = this.name+": "+this.selected;
	return html;
};//method


//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
RadioFilter.prototype.getSimpleState=function(){
	return this.selected;
};

RadioFilter.prototype.setSimpleState=function(value){
	if (this.options.contains(value)){
		this.selected=value;
	}//endif
	var radios=$("input:radio[name='"+this.id+"']");
	radios.val([self.selected]);
	this.refresh();
};

RadioFilter.prototype.makeHTML=function(){
	var html = new Template([
		'<div id="{{id}}">',
		this.description===undefined ? "" : "<div>{{description}}</div>",
		{
			"from": "options",
			"template": '<input type="radio" name="{{id}}" value="{{.}}">{{.}}</input><br>'
		},
		'</div>'
	]);

	return html.expand(this);
};//method


RadioFilter.prototype.setup=function(){
	if (this.doneSetup) return;

	var self=this;
	var radios=$("input:radio[name='"+this.id+"']");
	if (radios.length==0) return; //NOT BEEN CREATED YET
	radios.val([self.selected]);
	radios.change(function(){
		self.selected=$(this).val();
		if (self.refreshCallback){
			self.refreshCallback();
			GUI.refresh(false);
		}else{
			GUI.refresh();
		}//endif
	});
	this.doneSetup=true;
};//method


RadioFilter.prototype.refresh = function(){
	this.setup();
};

