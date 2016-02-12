
parse = {};

parse.URL = function parseURL(url){
	var parser = document.createElement('a');
	parser.href = url;
	return {
		"scheme": parser.protocol,
		"host": parser.hostname,
		"port": convert.String2Integer(parser.port),
		"path": parser.pathname,
		"params": convert.URLParam2Object(parser.search.slice(1)),
		"fragment": convert.URLParam2Object(parser.hash.slice(1))
	};
};
