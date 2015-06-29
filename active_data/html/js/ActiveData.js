/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("../modevlib/debug/aLog.js");
importScript("../modevlib/util/aUtil.js");
importScript("../modevlib/rest/Rest.js");


function ActiveData(url) {
	this.url = coalesce(url, "http://activedata.allizom.org/query")
}

ActiveData.prototype.query=function*(query){
	try {
		var response = yield(Rest.post({
			"url": this.url,
			"data": convert.value2json(query)
		}));

		yield (response);
	} catch (e) {
		Log.error("Call to ActiveData failed", e)
	}//try
};//function
