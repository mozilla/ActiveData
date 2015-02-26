importScript("aDate.js");
importScript("aDuration.js");
importScript("../debug/aLog.js");


var aTimer=function(){};

//PROVIDE warningDuration IF ONLY NEED TO PRINT TIMES LONGER THAN GIVEN
aTimer.start=function(name, warningDuration){
	var t=new aTimer();
	t.warningDuration=warningDuration==undefined ? undefined : warningDuration.getMilli();
	t.name=name;
	t.start=window.performance.now();
	return t;
};//method

aTimer.prototype.end=function(){
	var end=window.performance.now();
	var dur=Duration.newInstance(end-this.start);
	if (this.warningDuration===undefined){
		Log.note(this.name+" ("+dur.toString()+")");
	}else if (dur.getMilli()>this.warningDuration){
		Log.warning(this.name+" ("+dur.toString()+")");
	}//endif
};//method

aTimer.prototype.stop=aTimer.prototype.end;