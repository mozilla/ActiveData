importScript("aParse.js");
importScript("convert.js");


window.Session = window.Session || function(){
	};

(function(){

	var state={};

	Session.get = function(field){
		return Map.get(state, field);
	};

	Session.set = function(field, value){
		return Map.set(state, field, value);
	};

	Session.URL = function(){
	};

	Session.URL.getFragment = function(){
		//GET WHOLE FRAGMENT, AS OBJECT
		var url = parse.URL(window.location.href);
		return url.fragment;
	};

	Session.URL.setFragment = function(value){
		//SET WHOLE FRAGMENT, OLD VALUES ARE LOST
		var url = parse.URL(window.location.href);
		url.fragment = value;
		window.history.replaceState(url, "", serialize(url))
	};

	Session.URL.set = function(field, value){
		//SET JUST ONE FRAGMENT PARAMETER
		var url = parse.URL(window.location.href);
		Map.set(url.fragment, field, value);

		var newURL = serialize(url);
		window.history.replaceState(url, "", newURL)
	};


	function serialize(url){
		//var newURL = url.scheme + "://" + url.host;
		//if (url.port) newURL += ":" + url.port;
		var newURL = url.path.split("/").last();
		if (url.param && Map.getKeys(url.param).length>0) newURL += "?" + convert.Object2URLParam(url.param);
		if (url.fragment && Map.getKeys(url.fragment).length>0) newURL += "#" + convert.Object2URLParam(url.fragment);

		return newURL;
	}

})();
