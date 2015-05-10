
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("../MozillaPrograms.js");
importScript("../debug/aLog.js");
importScript("../util/convert.js");


ProgramFilter = function(indexName){
	this.indexName=coalesce(indexName, "bugs");
	this.name="Programs";
	this.refresh();
	this.selected=[];
	this.isFilter=true;
};

ProgramFilter.allPrograms = convert.Table2List(MozillaPrograms);

ProgramFilter.prototype.makeFilter = function(indexName, selectedPrograms){
	indexName=coalesce(indexName, this.indexName);
	selectedPrograms=coalesce(selectedPrograms, this.selected);

	if (selectedPrograms.length == 0) return ESQuery.TrueFilter;

	var or = [];
	for(var i=0;i<selectedPrograms.length;i++){
		for(var j=0;j<ProgramFilter.allPrograms.length;j++){
			if (ProgramFilter.allPrograms[j].projectName == selectedPrograms[i]){
				if (ProgramFilter.allPrograms[j].esfilter){
					or.push(ProgramFilter.allPrograms[j].esfilter);
					continue;
				}//endif

				var name = ProgramFilter.allPrograms[j].attributeName;
				var value = ProgramFilter.allPrograms[j].attributeValue;

				if (indexName!="bugs"){//ONLY THE ORIGINAL bugs INDEX HAS BOTH whiteboard AND keyword
					if (name.startsWith("cf_")) value=name+value;		//FLAGS ARE CONCATENATION OF NAME AND VALUE
					name="keywords";
				}//endif

				or.push({"term":Map.newInstance(name, value)});
			}//endif
		}//for
	}//for

	return {"or":or};
};//method


ProgramFilter.makeQuery = function(filters){
	var programCompares={};

	ProgramFilter.allPrograms.forall(function(program){
		var name = program.attributeName;
		var value = program.attributeValue;

		var esfilter;
		if (program.esfilter){
			esfilter=program.esfilter;
		}else{
			esfilter={"term":Map.newInstance(name, value)};
		}//endif

		var project=program.projectName;
		programCompares[project]=coalesce(programCompares[project], []);
		programCompares[project].push(esfilter);
	});


	var output = {
		"query":{
			"filtered":{
				"query":{
					"match_all":{}
				},
				"filter":{
					"and":[
						{"exists":{"field":"expires_on"}},
						{ "range":{ "expires_on":{ "gt" : Date.now().getMilli() } } },
						{"not":{"terms":{ "bug_status":["resolved", "verified", "closed"] }}}
					]
				}
			}
		},
		"from": 0,
		"size": 0,
		"sort": [],
		"facets":{
		}
	};

	//INSERT INDIVIDUAL FACETS
	//I WOULD HAVE USED FACET FILTERS, BUT THEY SEEM NOT ABLE TO RUN indexOf() ON _source ATTRIBUTES
	for(var c in programCompares){
		output.facets[c]={
			"terms":{
				"field":"exists"
			},
			"facet_filter":{
				"or":programCompares[c]
			}
		}
	}

	var and = output.query.filtered.filter.and;
	for(var f=0;f<filters.length;f++) and.push(filters[f]);

	return output;
};//method

ProgramFilter.prototype.getSummary=function(){
	 var html = "Programs: ";
	if (this.selected.length == 0){
		html += "All";
	} else{
		html += this.selected.join(", ");
	}//endif
	return html;
};//method


//RETURN SOMETHING SIMPLE ENOUGH TO BE USED IN A URL
ProgramFilter.prototype.getSimpleState=function(){
	if (this.selected.length==0) return undefined;
	return this.selected.join(",");
};


ProgramFilter.prototype.setSimpleState=function(value){
	if (!value || value==""){
		this.selected=[];
	}else{
		this.selected=value.split(",").map(function(v){return v.trim();});
	}//endif
	this.refresh();
};

ProgramFilter.prototype.makeHTML=function(){
	return '<div id="programs"></div>';
};//method


//programs IS A LIST OF OBJECTS WITH A term AND count ATTRIBUTES
ProgramFilter.prototype.injectHTML = function(programs){

	var html ='<i><a href="http://people.mozilla.com/~klahnakoski/es/modevlib/MozillaPrograms.js">click here for definitions</a></i><br>';
	html += '<ul id="programsList" class="menu ui-selectable">';
	var item = new Template('<li class="{{class}}" id="program_{{name}}">{{name}} ({{count}})</li>');

	//REMINDER OF THE DEFINITION



	//GIVE USER OPTION TO SELECT ALL PRODUCTS
	var total = 0;
	for(var i = 0; i < programs.length; i++) total += programs[i].count;
	html += item.replace({
		"class" : ((this.selected.length == 0) ? "ui-selectee ui-selected" : "ui-selectee"),
		"name" : "ALL",
		"count" : total
	});

	for(var i = 0; i < programs.length; i++){
		html += item.replace({
			"class" : this.selected.contains(programs[i].term) ? "ui-selectee ui-selected" : "ui-selectee",
			"name" : programs[i].term,
			"count" : programs[i].count
		});
	}//for

	html += '</ul>';

	var p = $("#programs");
	p.html(html);
};


ProgramFilter.prototype.refresh = function(){
	var self = this;
	Thread.run(function*(){
		self.query = ProgramFilter.makeQuery([]);


		var data = yield (ElasticSearch.search("bugs", self.query));

		//CONVERT MULTIPLE EDGES INTO SINGLE LIST OF PROGRAMS
		var programs=[];
		Map.forall(data.facets, function(name, edge){
			if (name=="Programs") return;  //ALL PROGRAMS (NOT ACCURATE COUNTS)
			programs.push({"term":name, "count":edge.total});
		});
		programs.reverse();
		self.injectHTML(programs);

		$("#programsList").selectable({
			selected: function(event, ui){
				var didChange = false;
				if (ui.selected.id == "program_ALL"){
					if (self.selected.length > 0) didChange = true;
					self.selected = [];
				} else{
					if (!self.selected.contains(ui.selected.id.rightBut("program_".length))){
						self.selected.push(ui.selected.id.rightBut("program_".length));
						didChange = true;
					}//endif
				}//endif

				if (didChange)GUI.refresh();
			},
			unselected: function(event, ui){
				var i = self.selected.indexOf(ui.unselected.id.rightBut("program_".length));
				if (i != -1){
					self.selected.splice(i, 1);
					GUI.refresh();
				}
			}
		});
	});
};

//RETURN MINIMUM VALUE OF ALL SELECTED PROGRAMS
ProgramFilter.prototype.bugStatusMinimum_fromDoc=function(){
	var idTime;
	if (this.selected.length==0){
		idTime="doc[\"create_time\"].value";
	}else{
		idTime=ProgramFilter.minimum(this.selected.map(function(v, i){return "doc[\""+v+"_time\"].value"}));
	}//endif

	return idTime;
};//method

//RETURN MINIMUM VALUE OF ALL SELECTED PROGRAMS
ProgramFilter.prototype.bugStatusMinimum_fromSource=function(){
	var idTime;
	if (this.selected.length==0){
		idTime="bug_summary.create_time";
	}else{
		idTime=ProgramFilter.minimum(this.selected.map(function(v, i){return "bug_summary[\""+v+"_time\"]"}));
	}//endif

	return idTime;
};//method

ProgramFilter.prototype.bugStatusMinimum=function(){
	var idTime;
	if (this.selected.length==0){
		idTime="create_time";
	}else{
		idTime=this.selected[0]+"_time";
	}//endif

	return idTime;
};//method






	//TAKE THE MINIMIM OF ALL GIVEN vars
ProgramFilter.minimum=function(vars){
	if (vars.length==1) return vars[0];

	var output=[];
	for(var i=0;i<vars.length-1;i+=2){
		output.push("minimum("+vars[i]+","+vars[i+1]+")");
	}//for
	if (i!=vars.length) output.push(vars[i]);
	return ProgramFilter.minimum(output);
};
