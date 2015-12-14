importScript("aParse.js");
importScript("convert.js");


window.Session = window.Session || function(){
	};

(function(){

	Session.URL = function(){
	};

	Session.URL.getFragment = function(){
		var url = parse.URL(window.location.href);
		return convert.URLParam2Object(url.fragment);
	};

	Session.URL.setFragment = function(value){
		var url = parse.URL(window.location.href);
		url.fragment = convert.Object2URLParam(value);

		var newURL = url.scheme + "://" + url.host;
		if (url.port) newURL += ":" + url.port;
		newURL += url.path;
		if (url.param) newURL += "?" + url.param;
		if (url.fragment) newURL += "#" + url.fragment;
		window.history.replaceState(value, "", newURL)
	};


})();
