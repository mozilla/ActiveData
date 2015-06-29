

importScript("../debug/aLog.js");

assert = {};

(function(){

	assert.equal = function(test, expected, errorMessage){
		if (test==expected) return;

		Log.error(errorMessage);
	};




})();
