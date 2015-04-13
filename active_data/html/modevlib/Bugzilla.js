/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

importScript("util/aHTML.js");
importScript("aLibrary.js");
importScript("rest/BugzillaClient.js");


Bugzilla={};
Bugzilla.URL="https://bugzilla.mozilla.org/buglist.cgi";

Bugzilla.showBugs=function(bugList){
	var url=Bugzilla.searchBugsURL(bugList);
	window.open(url);
};//method


Bugzilla.searchBugsURL=function(bugList){
	if (bugList instanceof Array){
		return Bugzilla.URL+"?quicksearch="+bugList.join('%2C');
	}else if (typeof(bugList)=="string"){
		return Bugzilla.URL+"?quicksearch="+bugList.replaceAll(", ", "%2C");
	}else{
		return Bugzilla.URL+"?quicksearch="+bugList;
	}//endif
};//method

Bugzilla.linkToBug=function(bugList){
	return new HTML("<a href='"+Bugzilla.searchBugsURL(bugList)+"'>"+bugList+"</a>");
};//method




Bugzilla.search=function*(bugList, fields){
	var BLOCK_SIZE=100;

	var resume=yield (Thread.Resume);
	var result=[];
	var numCalls=aMath.floor((bugList.length+BLOCK_SIZE-1)/BLOCK_SIZE);
	if (!fields.contains("id")) fields.prepend("id");

	for(i=0;i<numCalls;i++){
		var subList=bugList.substring(i*BLOCK_SIZE, aMath.min(bugList.length, i*BLOCK_SIZE+BLOCK_SIZE));

		new BugzillaClient({}).searchBugs({
			"id":subList,
			"include_fields":fields.join(",")
		}, function(status, data){
			if (status=="error"){
				numCalls--;
				Log.error("can not get bugs!");
			}//endif
			numCalls--;
			Log.note(result.length+"+"+data.length);

			for(var r=data.length;r--;){
				var b=data[r];
				for(var c=fields.length;c--;){
					var f=fields[c];
					b[f]=coalesce(b[f], null);
				}
			}

			result.appendArray(data);
			if (numCalls==0){
				var missing=bugList.subtract(result.map(function(b){return b.id;}));
				result.appendArray(missing.map(function(m){
					var output={};
					for(var c=fields.length;c--;) output[fields[c]]=null;
					output.id=m;
					return output;
				}));
				resume(result);
			}
		});

	}//for
	yield (Thread.suspend());
};//method




