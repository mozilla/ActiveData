/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */


importScript("aArray.js");






////////////////////////////////////////////////////////////////////////////////
// A QUEUE IS A FIFO BUFFER THAT ENSURES ELEMENTS ARE UNIQUE AND DO NOT LOOSE
// THEIR POSITION POSITION IN THE QUEUE:
//
//  new aQueue().add('A').add('B').add('A').pop()=='A'  (with only B remaining in queue)
//
////////////////////////////////////////////////////////////////////////////////

var aQueue; //FAST, BUT ONLY GOOD FOR STRINGS (OR THINGS THAT COERCE TO STRINGS)
var aQueue_usingArray;  //SLOW, BUT GOOD FOR WHEN DEALING WITH PURE OBJECTS

(function(){
	var DEBUG=false;




	var aQueue_test=function(data){
			this.A=new aQueue_simple(data);
			this.B=new aQueue_usingReserve(data);
		};


	aQueue_test.prototype.add=function(v){
		if (typeof(v) != "string")
			Log.error("Can only work with strings");
		this.A.add(v);
		this.B.add(v);
		return this;
	};

	aQueue_test.prototype.remove=function(v){
		this.A.remove(v);
		this.B.remove(v);
		return this;
	};

	aQueue_test.prototype.pop=function(){
		var a=this.A.pop();
		var b=this.B.pop();
		if (a!=b){
			Log.error();
		}
		return a;
	};

	aQueue_test.prototype.push=function(v){
		this.A.push(v);
		this.B.push(v);
		return this;
	};

	aQueue_test.prototype.isEmpty=function(){
		var a=this.A.isEmpty();
		var b=this.B.isEmpty();
		if (a!=b){
			Log.error();
		}
		return a;
	};//method



	aQueue_test.prototype["length"]=function(){
		var a=this.A.length();
		var b=this.B.length();
		if (a!=b){
			Log.warning("diff queue lengths ("+a+" vs "+b+")");
		}
		return a;
	};











	//AN ATTEMPT TO REDUCE MEMORY USAGE BY GENERATING LESS INTERMEDIATE OBJECTS
	var aQueue_complex=function(data){
		this.curr=0;
		this.array=[];
		this.map={};

		var self=this;
		if (data!==undefined) data.forall(function(v, i){self.add(v);});
	};



	aQueue_complex.prototype.add=function(v){
		if (this.map[v]!==undefined) return;
		this.map[v]=1;
		this.array.push(v);
		return this;
	};

	aQueue_complex.prototype.remove=function(v){
		if (this.map[v]===undefined) return;
		this.map[v]=undefined;
		this.array.remove(v, this.curr);
		return this;
	};

	aQueue_complex.prototype.pop=function(){
		var o=this.array[this.curr];
		this.map[o]=undefined;
		this.curr++;

		if (this.curr>5000){
			this.array=this.array.substring(this.curr);
			this.curr=0;
		}//endif

		return o;
	};

	aQueue_complex.prototype.push=function(v){
		if (this.map[v]!==undefined){
			this.array.remove(v);
		}//endif
		this.map[v]=1;
		if (this.curr==0){
			this.array.prepend(v);
		}else{
			this.curr--;
			this.array[this.curr]=v;
		}//endif
		return this;
	};

	aQueue_complex.prototype.isEmpty=function(){
		return this.array.length==0;
	};//method


	aQueue_complex.prototype["length"]=function(){
		return this.array.length-this.curr;
	};





	//IT FIGURES THE SIMPLEST IS FASTEST
	var aQueue_simple=function(data){
		this.array=[];
		this.map={};

		if (data!==undefined) this.appendArray(data);
	};

	aQueue_simple.newInstance=function(data){
		return new aQueue_simple(data);
	};//method

	
	aQueue_simple.prototype.appendArray=function(data){
		var self=this;
		data.forall(function(v, i){self.add(v);});
		return this;
	};//method


	aQueue_simple.prototype.add=function(v){
		if (this.map[v]!==undefined) return;
		this.map[v]=1;
		this.array.prepend(v);
		return this;
	};

	aQueue_simple.prototype.remove=function(v){
		if (this.map[v]===undefined) return;
		this.map[v]=undefined;
		this.array.remove(v);
		return this;
	};

	aQueue_simple.prototype.pop=function(){
		var o=this.array.pop();
		this.map[o]=undefined;
		return o;
	};

	aQueue_simple.prototype.push=function(v){
		if (this.map[v]!==undefined){
			this.array.remove(v);
		}//endif
		this.map[v]=1;
		this.array.push(v);
		return this;
	};

	aQueue_simple.prototype.isEmpty=function(){
		return this.array.length==0;
	};//method

	aQueue_simple.prototype["length"]=function(){
		return this.array.length;
	};



	//SLOW BECAUSE NO HASH USED TO VERIFY EXISTING OBJECTS
	var aQueue_usingReserve=function(data){
		this.array=[];
		this.reserve=[];    //PACK IN REVERSE ORDER FOR SPEED

		var self=this;
		if (data!==undefined) this.array.appendArray(data);
	};

	aQueue_usingReserve.prototype.add=function(v){
		if (this.reserve.indexOf(v)>=0) return this;        //REALLY SLOW
		if (this.array.indexOf(v)>=0) return this;       //REALLY SLOW
		this.reserve.push(v);
		return this;
	};

	aQueue_usingReserve.prototype.remove=function(v){
		while(true){
			var i=this.array.indexOf(v);
			if (i==-1) break;
			this.array.removeAt(i);
		}//while

		while(true){
			var i=this.reserve.indexOf(v);
			if (i==-1) break;
			this.reserve.removeAt(i);
		}//while

		return this;
	};

	aQueue_usingReserve.prototype.pop=function(){
		if (this.array.length>0) return this.array.pop();

		this.reserve.reverse();
		this.array=this.reserve;
		this.reserve=[];
		return this.array.pop();
	};

	aQueue_usingReserve.prototype.push=function(v){
		this.remove(v);
		this.array.push(v);
		return this;
	};


//	Object.defineProperty(aQueue_usingReserve.prototype, 'length', {
//		writable: false,
//		enumerable: false,
//		configurable: false,
//		get: function(){
//			return this.array.length;
//		}
//	});

	aQueue_usingReserve.prototype.isEmpty=function(){
		return this.array.length==0 && this.reserve.length==0;
	};//method

	aQueue_usingReserve.prototype["length"]=function(){
		return this.array.length+this.reserve.length;
	};


	//SLOW BECAUSE NO HASH USED TO VERIFY EXISTING OBJECTS
	var aQueue_usingReserve=function(data){
		this.array=[];
		this.reserve=[];    //PACK IN REVERSE ORDER FOR SPEED

		var self=this;
		if (data!==undefined) this.array.appendArray(data);
	};

	aQueue_usingReserve.prototype.add=function(v){
		if (this.reserve.indexOf(v)>=0) return this;        //REALLY SLOW
		if (this.array.indexOf(v)>=0) return this;       //REALLY SLOW
		this.reserve.push(v);
		return this;
	};

	aQueue_usingReserve.prototype.remove=function(v){
		while(true){
			var i=this.array.indexOf(v);
			if (i==-1) break;
			this.array.removeAt(i);
		}//while

		while(true){
			var i=this.reserve.indexOf(v);
			if (i==-1) break;
			this.reserve.removeAt(i);
		}//while

		return this;
	};

	aQueue_usingReserve.prototype.pop=function(){
		if (this.array.length>0) return this.array.pop();

		this.reserve.reverse();
		this.array=this.reserve;
		this.reserve=[];
		return this.array.pop();
	};

	aQueue_usingReserve.prototype.push=function(v){
		this.remove(v);
		this.array.push(v);
		return this;
	};


//	Object.defineProperty(aQueue_usingReserve.prototype, 'length', {
//		writable: false,
//		enumerable: false,
//		configurable: false,
//		get: function(){
//			return this.array.length;
//		}
//	});

	aQueue_usingReserve.prototype.isEmpty=function(){
		return this.array.length==0 && this.reserve.length==0;
	};//method

	aQueue_usingReserve.prototype["length"]=function(){
		return this.array.length+this.reserve.length;
	};





	//SLOW BECAUSE NO HASH USED TO VERIFY EXISTING OBJECTS
	aQueue_usingArray=function(data){
		this.array=[];

		var self=this;
		if (data!==undefined) this.array.appendArray(data);
	};

	aQueue_usingArray.prototype.appendArray=function(data){
		var self=this;
		data.forall(function(v, i){self.add(v);});
		return this;
	};//method


	aQueue_usingArray.prototype.add=function(v){
		for(var i=this.array.length;i--;) if (this.array[i]===v) return this;       //REALLY SLOW
		this.array.push(v);
		return this;
	};

	aQueue_usingArray.prototype.remove=function(v){
		var a=this.array;
		for(var i=a.length;i--;) if (a[i]===v) a.removeAt(i);
		return this;
	};

	aQueue_usingArray.prototype.pop=function(){
		return this.array.pop();
	};

	aQueue_usingArray.prototype.push=function(v){
		this.remove(v);
		this.array.push(v);
		return this;
	};


//	Object.defineProperty(aQueue_usingArray.prototype, 'length', {
//		writable: false,
//		enumerable: false,
//		configurable: false,
//		get: function(){
//			return this.array.length;
//		}
//	});

	aQueue_usingArray.prototype.isEmpty=function(){
		return this.array.length==0 && this.reserve.length==0;
	};//method

	aQueue_usingArray.prototype["length"]=function(){
		return this.array.length+this.reserve.length;
	};







	if (!DEBUG){
		aQueue=aQueue_simple
		return;
	}//endif

	aQueue=aQueue_test;      //NO DEBUGGING
	var test=new aQueue(["23", "42"]);
	test.pop();


})();