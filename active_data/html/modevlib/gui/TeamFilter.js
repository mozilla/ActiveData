/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */



importScript([
	"../../lib/jquery.js",
	"../../lib/jstree/jquery.jstree.js"
]);


TeamFilter = function(){};


TeamFilter.newInstance=function(field_name){
	var self=new TeamFilter();

	self.isFilter=true;
	self.name="Teams";
	self.disableUI=false;
	self.field_name=field_name;
	self.selectedEmails=[];

	Thread.run("get people", function*(){
		//GET ALL PEOPLE
		var people=(yield (ESQuery.run({
			"from":"org_chart",
			"select":[
				{"name":"id", "value":"id"},
				{"name":"name", "value":"name"},
				{"name":"email", "value":"email"},
				{"name":"manager", "value":"manager"}
			],
			"esfilter":{"and":[
				{"exists":{"field":"id"}},
			]}
		}))).list;

		var others={
			"email":"other@mozilla.com",
			"name":"Other MoCo",
			"id":"mail=other@mozilla.com,"
		};
		people.push(others);

		people.push({
			"email":"nobody@mozilla.org",
			"name":"Nobody",
			"id":"mail=nobody@mozilla.org,"
		});

		people.push({
			"email":"community@mozilla.org",
			"name":"Community",
			"id":"mail=community@mozilla.org,"
		});



		//USE THIS TO SIMPLIFY THE TREE SELECTION
		self.managers={};
		people.forall(function(v, i){
			v.id=v.id.between("mail=", ",");

			if (v.manager==undefined || v.manager==null || v.manager=="null"){
				if (!["other@mozilla.com", "nobody@mozilla.org", "community@mozilla.org"].contains(v.id)){
					v.manager="other@mozilla.com";
				}//endif
			}else{
				v.manager = v.manager.between("mail=", ",");
			}//endif

			if (v.email==null) v.email=v.id;

			if (self.managers[v.id])
				Log.warning(v.id+" is not unique");
			self.managers[v.id]= v.manager;

			//USED BY TREE VIEW
			v.data=v.name;
			v.attr={"id":v.id};
		});

		var hier=Hierarchy.fromList({
			"from":people,
			"id_field":"id",
			"child_field":"children",
			"parent_field":"manager"
		});

		if (others.children){
			others.children.map(function(v, i){
				//PULL OUT THE TOP LEVEL 'PEOPLE' WITH CHILDREN
				if (v.children && v.manager=="other@mozilla.com"){
					v.manager=null;
					others.children.remove(v);
					hier.prepend(v);
				}//endif
				return v;
			});
			others.children = others.children.filter({"not":{"terms":hier.select("id")}});
		}//endif

		self.injectHTML(hier);

		//JSTREE WILL NOT BE LOADED YET
		//HOPEFULLY IT WILL EXIST WHEN THE HEAD EXISTS
//		'#' + myid.replace(/(:|\.)/g,'\\$1');

		while($("#"+convert.String2JQuery("other@mozilla.com")).length==0){
			yield (Thread.sleep(100));
		}//while

		self.people=people;
	});
	return self;
};


TeamFilter.prototype.makeHTML=function(){
	return '<div id="teams" style="300px"></div>';
};


TeamFilter.prototype.getSummary=function(){
	var html = "Teams: ";
	var teams=Thread.runSynchronously(this.getSelectedPeople());
	if (teams.length == 0){
		html += "All";
	} else{
		html +=teams.map(function(p, i){return p.name;}).join(", ");
	}//endif
	return html;
};//method


TeamFilter.prototype.getSelectedPeople=function*(){
	var self=this;

	while(!self.people){
		yield (Thread.sleep(100));
	}//while

	//CONVERT SELECTED LIST INTO PERSONS
	var selected = this.selectedEmails.map(function(email){
		for(var i = self.people.length; i--;){
			if (self.people[i].id==email) return self.people[i];
		}//for
	});
	yield selected;
};//method

TeamFilter.prototype.getAllSelectedPeople = function*(){
	var selected = yield(this.getSelectedPeople());
	if (selected.length == 0){
		yield [];
		return
	}//endif

	//FIND BZ EMAILS THAT THE GIVEN LIST MAP TO
	var output=[];
	var getEmail=function(children){
		children.forall(function(child, i){
			output.append(child);
			if (child.children)
				getEmail(child.children);
		});
	};//method
	getEmail(selected);
	yield output;
};//method


//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
TeamFilter.prototype.getSimpleState=function(){
	if (this.selectedEmails.length==0) return undefined;
	return this.selectedEmails.join(",");
};

//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
TeamFilter.prototype.setSimpleState=function(value){
	if (!value || value==""){
		this.selectedEmails=[];
	}else{
		this.selectedEmails=value.split(",");
	}//endif
	this.refresh();
};


TeamFilter.prototype.makeFilter = function(field_name){
	if (this.selectedEmails.length == 0) return ESQuery.TrueFilter;
	field_name=coalesce(field_name, this.field_name);
	if (field_name==null) return ESQuery.TrueFilter;

	var selected = Thread.runSynchronously(this.getSelectedPeople());
	if (selected.length == 0) return ESQuery.TrueFilter;

	//FIND BZ EMAILS THAT THE GIVEN LIST MAP TO
	var bzEmails=[];
	var getEmail=function(children){
		children.forall(function(child, i){
			if (child.email)
				bzEmails.appendArray(Array.newInstance(child.email));
			if (child.children)
				getEmail(child.children);
		});
	};//method
	getEmail(selected);

	if (bzEmails.length==0) return ESQuery.TrueFilter;

	var output={"or":[]};

	if (bzEmails.contains("community@mozilla.org")){
		bzEmails.remove("community@mozilla.org");
		var allEmails=this.people.map(function(v, i){return v.email;});
		allEmails.push("nobody@mozilla.org");

		output.or.push({"not":{"terms":Map.newInstance(field_name, allEmails)}});
	}//endif

	if (bzEmails.length!=0) output.or.push({"terms":Map.newInstance(field_name, bzEmails)});

	return output;
};//method


TeamFilter.prototype.refresh = function*()
{
	//FIND WHAT IS IN STATE, AND UPDATE STATE
	this.disableUI = true;
	var selected = yield(this.getSelectedPeople());

	var f = $('#teamList');
	f.jstree("deselect_all");
	selected.forall(function(p){
		f.jstree("select_node", "#" + convert.String2JQuery(p.id));
		f.jstree("check_node", "#" + convert.String2JQuery(p.id));
	});

	this.disableUI = false;
}
;


TeamFilter.prototype.injectHTML = function(hier){
	var html = '<div id="teamList"></div>';
	$("#teams").html(html);

	var self=this;

	$("#teamList").jstree({
		"json_data":{
			"data":hier
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

		//WE NOW HAVE A RIDICULOUS NUMBER OF CHECKED ITEMS, REDUCE TO MINIMUM COVER
		var minCover=[];

		//FIRST MAKE A HASH OF CHECKED ITEMS
		var checked={};
		$(".jstree-checked").each(function(){
			checked[$(this).attr("id")]={};
		});

		//IF MANAGER IS CHECKED, THEN DO NOT INCLUDE
		Map.forall(checked, function(id, v, m){
			if (checked[self.managers[id]]) return;
			minCover.push(id);
		});
		minCover.sort();

		//HAS ANYTHING CHANGED?
		var hasChanged=false;
		if (self.selectedEmails.length!=minCover.length) hasChanged=true;
		if (!hasChanged) for(var i=minCover.length;i--;){
			if (minCover[i]!=self.selectedEmails[i]) hasChanged=true;
		}//for

		if (hasChanged){
			self.selectedEmails=minCover;
			GUI.refresh();
		}//endif
	});


};


