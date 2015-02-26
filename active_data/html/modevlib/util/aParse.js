
parse = {};

parse.URL = function parseURL(url){
	var parser = document.createElement('a');
	parser.href = url;
	return parser;
};