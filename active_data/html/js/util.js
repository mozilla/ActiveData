/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */



importScript("../modevlib/aLibrary.js");
importScript("../modevlib/Settings.js");

importScript("../modevlib/MozillaPrograms.js");
importScript("../modevlib/qb/ESQuery.js");
importScript("../modevlib/charts/mgChart.js");
importScript("../modevlib/charts/aColor.js");
importScript([
	"../css/menu.css"
]);
importScript("../modevlib/math/Stats.js");
importScript("../modevlib/qb/qb.js");


var search = function*(query){
	var output = yield (Rest.post({
		url: "http://activedata.allizom.org/query",
		json: query
	}));

	yield (output);
};

