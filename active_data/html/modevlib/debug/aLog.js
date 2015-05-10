/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript([
		"../../lib/jquery.js",
		"../../lib/jquery-ui/js/jquery-ui-1.10.2.custom.js",
		"../../lib/jquery-ui/css/start/jquery-ui-1.10.2.custom.css",
		"../../lib/jquery.ba-bbq/jquery.ba-bbq.js"
]);
importScript("aException.js");
importScript("../util/aTemplate.js");





var Log = new function(){
};

(function(){
	Log.FORMAT = new Template("{{type}} : {{timestamp|datetime('MM dd HH:mm:ss.fff')}} : {{message}}");

	Log.loggers=[];

	//ACCEPT A FUNCTION THAT HANDLES OBJECT
	Log.addLog=function(logger){
		if (typeof(logger ) != "function"){
			Log.error("Expecting a function")
		}//endif
		Log.loggers.push(logger);
	};

	//TODO: MAKE THIS BETTER SO IT SHOWS NICELY
	function log2string(message){
		if (typeof(message)=="string" || message instanceof String){
			return message;
		}else if (message.type=="WARNING"){
			return message.warning.toString();
		}else if (message.message){
			return Log.FORMAT.expand(message);
		}else{
			return convert.value2json(message);
		}//endif
	}

	function log2html(message){
		return "<p>"+convert.String2HTML(JSON.stringify(message))+"</p>"
	}

	//ADD THE DEFAULT CONSOLE LOGGING
	Log.addLog(function(message){
		console.info(log2string(message))
	});

	Log.addLogToElement=function(id){
		if (id===undefined) return;
		var logHolder=$("#"+id);
		logHolder.html("");
		Log.addLog(function(message){
			logHolder.append(log2html(message));
		});
	};


	// ADD AN INVISIBLE DOM NODE WITH id TO PUT ALL LOGS INTO
	Log.addInvisibleLog=function(id){
		if (id===undefined) return;
		$("body").append('<div id="'+id+'" style="display: none"></div>');
		var logHolder=$("#"+id);
		Log.addLog(function(message){
			logHolder.append(log2html(message));
		});
	};

	try{
		$(document).ready(function(){
			var id=$.bbq.getState("log");
			Log.addInvisibleLog(id);
		});
	}catch(e){

	}//try


	//EXPECTING AN OBJECT TO DESCRIBE THE MESSAGE
	//IT CAN BE A STRING, OR HAVE PROPERTIES;
	//
	// type - "NOTE", "WARNING", "ERROR", or others
	// message - The humane message, or template
	// params - For if the template has paramters
	// timestamp - When this message was issued (gmt)
	//
	// ... AND MORE
	Log.note = function(message){
		if (typeof(message)=="string" || message instanceof String){
			message = {
				"timestamp":Date.now(),
				"message":message,
				"type":"NOTE"
			}
		}else{
			//ASSUME IT IS ALREADY A JSON OBJECT
		}//endif

		Log.loggers.forall(function(v){
			v(message);
		});
	};//method

	//USED AS SIDE-EFFECT BREAK POINTS
	Log.debug=function(){
		Log.note("debug message");
	};//method

	Log.error = function(description, cause, stackOffset){
		var ex=new Exception(description, cause, coalesce(stackOffset, 0)+1);
//		console.error(ex.toString());
		throw ex;
	};//method

	Log.warning = function(description, cause){
		var e=new Exception(description, cause, 1);
		Log.note({
			"type": "WARNING",
			"warning":e
		})
	};//method


	Log.gray=function(message, ok_callback, cancel_callback){
		//GRAY OUT THE BODY, AND SHOW ERRO IN WHITE AT BOTTOM
		Log.note({
			"type":"ALERT",
			"timestamp":Date.now(),
			"message":message
		});

		if (!window.log_alert){
			window.log_alert = true;
			$('body').css({"position":"relative"}).append('<div id="log_alert" style="background-color:rgba(00, 00, 00, 0.5);position:absolute;bottom:0;height:100%;width:100%;vertical-align:bottom;zindex:10"></div>');
		}//endif

		var template = new Template(
			'<div style="width:100%;text-align:center;color:white;position:absolute;bottom:0;">{{message|html}}</div>'
		);

		var html = template.expand({"message":message.replaceAll("\n", " ").replaceAll("\t", " ").replaceAll("  ", " ")});
		$('#log_alert').html(html);
	};//method

	var red_uid=0;

	Log.red=function(message){
		//GRAY OUT THE BODY, AND SHOW ERROR IN WHITE AT BOTTOM
		window.log_alert_till = Date.now().add("20second").getMilli();
		if (!window.log_alert){
			window.log_alert = true;
			$('body').css({"position":"relative"}).append('<div id="log_alert" style="position:fixed;top:0;bottom:0;width:100%;vertical-align:bottom;pointer-events:none;"></div>');
		}//endif

		var uid = "log_alert"+red_uid;
		red_uid++;

		function erase() {
			Log.note("removing "+uid);
			var diff = window.log_alert_till - Date.now().add("20second").getMilli();
			if (diff>0){
				setTimeout(erase, diff);
			}else{
				$('#'+uid).remove();
			}//endif
		}//function
		setTimeout(erase, 20000);
		Thread.currentThread.addChild({"kill":erase});

		var template = new Template(
			'<div id="{{uid}}" style="{{style}}">{{message|html}}</div>'
		);
		var html = template.expand({
			"uid":uid,
			"style":convert.Object2CSS({
				"width":"100%",
				"text-align":"center",
				"color":"white",
				"position":"absolute",
				"bottom":0,
				"height":"23px",
				"zindex":10,
				"background-color":Color.RED.darker().toHTML()
			}),
			"message":message.replaceAll("\n", " ").replaceAll("\t", " ").replaceAll("  ", " ")
		});

		$('#log_alert').html(html);
	};//method

	Log.alert = function (message, ok_callback, cancel_callback) {
		Log.note({
			"type":"ALERT",
			"timestamp":Date.now(),
			"message":message
		});

		if (cancel_callback===undefined && ok_callback===undefined) {
			Log.red(message);
		} else {
			var d = $('<div>' + message + "</div>").dialog({
				title: "Alert",
				draggable: false,
				modal: true,
				resizable: false,

				buttons: {
					"OK": function () {
						$(this).dialog("close");
						if (ok_callback) ok_callback();
					},
					"Cancel": cancel_callback ? function () {
						$(this).dialog("close");
						cancel_callback();
					} : undefined
				}
			});
		}//endif
	};//method


	//TRACK ALL THE ACTIONS IN PROGRESS
	Log.actionStack=[];
	Log.action=function(message, waitForDone){
		var action={"message":message, "start":Date.now()};

		if (message.length>30){
			message=message.left(27)+"...";
		}//endif

		Log.actionStack.push(action);
		$("#status").html(message);
		if (message.toLowerCase()=="done"){
			$("#status").html(message);
			return;
		}//endif

		Log.note({
			"type":"timer.start",
			"timestamp":action.start,
			"message":message
		});


		//JUST SHOW MESSAGE FOR THREE SECONDS
		$("#status").html(message);
		if (!waitForDone) setTimeout(function(){Log.actionDone(action, true);}, 3000);
		return action;		//RETURNED IF YOU WANT TO REMOVE IT SOONER
	};//method


	Log.actionDone=function(action){
		action.end=Date.now();

		if (Log.actionStack.length==0) {
			$("#status").html("Done");
			return;
		}//endif

		var i=Log.actionStack.indexOf(action);
		if (i>=0) Log.actionStack.splice(i, 1);

		Log.note({
			"type":"timer.stop",
			"timestamp":action.end,
			"message":action.message+" ("+action.end.subtract(action.start).floor(Duration.SECOND).toString()+")"
		});

		if (Log.actionStack.length==0){
			$("#status").html("Done");
		}else{
			$("#status").html(Log.actionStack[Log.actionStack.length-1].message);
		}//endif
	};//method


})();


ASSERT=function(test, errorMessage){
	if (!test){
		Log.error(errorMessage);
	}//endif
};
ASSERT.hasAttributes=function(obj, keyList){
	A: for(i=0;i<keyList.length;i++){
		if (keyList[i] instanceof Array){
			for(j=0;j<keyList[i].length;j++){
				if (obj[keyList[i][j]]!==undefined) continue A;
			}//for
			Log.error("expecting object to have one of "+convert.value2json(keyList[i])+" attribute");
		}else{
			if (obj[keyList[i]]===undefined) Log.error("expecting object to have '"+keyList[i]+"' attribute");
		}//endif
	}//for
};







