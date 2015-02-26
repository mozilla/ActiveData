////////////////////////////////////////////////////////////////////////////////
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
////////////////////////////////////////////////////////////////////////////////
// Author: Kyle Lahnakoski  (kyle@lahnakoski.com)
////////////////////////////////////////////////////////////////////////////////

importScript("../../lib/jquery.js");

///////////////////////////////////////////////////////////////////////////////
// DROP-DOWN ACCORDIONS
//
// MARKUP DOM ELEMENTS WITH class="accordion_header" TO USE THIS LIBRARY
//
// class="accordion_header_open" - WHEN BODY IS OPEN
// class="accordion_header_closed" - WHEN BODY IS CLOSED (ASSIGNED IMMEDIATELY)
// accordion_id - POINTS TO THE ID OF ELEMENT TO EXPAND/CONTRACT
// accordion_group - ONLY ONE MEMBER OF THE GROUP CAN STAY OPEN
//
// jquery is extended with updateAccordion() TO RESIZE THE BODY
//
///////////////////////////////////////////////////////////////////////////////


$(document).ready(function () {
	function close(e) {
		return e.animate({"height": 0}, 500);
	}

	function open(e) {
		var height=0;
		e.children().each(function(){
			var oh=$(this).outerHeight();
			height+=oh;
		});
		e.css({"display":"block"});
		e.animate({"height": height+"px"}, 500);
	}

	//MAKE SURE OPEN DIV RESIZE TO FULL
	$.fn.updateAccordion = function update(){
		var self=$(this);
		$(".accordion_header_open[accordion_id='" + self.attr("id") + "']").each(function(){
			open(self);
		});
		return this;
	};

	$(".accordion_header, .accordion_header_open, .accordion_header_closed")
		.click(function (e) {
			var self = $(this);
			var group=self.attr("accordion_group");
			//CLOSE THE OTHERS
			$(".accordion_header_open[accordion_group='" + group + "']").each(function () {
				var other=$(this);
				if (self.get(0) !== other.get(0)) {
					other.addClass("accordion_header_closed").removeClass("accordion_header_open");
					close($("#" + other.attr("accordion_id")));
				}//endif
			});
			//TOGGLE THIS' STATE
			if (["accordion_header_open"].contains(self.attr("class"))) {
				self.addClass("accordion_header_closed").removeClass("accordion_header_open");
				close($("#" + self.attr("accordion_id")));
			} else {
				self.addClass("accordion_header_open").removeClass("accordion_header_closed");
				open($("#" + self.attr("accordion_id")));
			}//endif
		})
		.each(function () {
			var self = $(this);
			var body = $("#" + self.attr("accordion_id"));
			body.addClass("accordion_body");

			if (self.attr("class") == "accordion_header") {
				//DEFAULT IS CLOSED
				self.addClass("accordion_header_closed");
				self.removeClass("accordion_header");
				body.css({"display":"none"});
				close(body);
			}else if (self.attr("class") == "accordion_header_closed") {
				body.css({"display":"none"});
				close(body);
			}//endif
		});
});

