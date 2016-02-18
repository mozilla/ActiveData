function qb2function(expr){
  if (expr == null) return function(){
    return function(){
      return null;
    }
  };
  if (expr === true) return function(){
    return function(){
      return true;
    }
  };
  if (expr === false) return function(){
    return function(){
      return false;
    }
  };
  if (isString(expr) && MVEL.isKeyword(expr)) {
    if (expr.indexOf("." == -1)) {
      return function(value){
        return value[expr];
      }
    } else {
      return function(value){
        return Map.get(value, expr);
      }
    }//endif
  }//endif
  if (typeof(expr)=="number"){
    return function(){
      return expr;
    };//endif
  }//endif

  var keys = Object.keys(expr);
  for (var i = keys.length; i--;) {
    var val = expressions[keys[i]];
    if (val) return val(expr);
  }//for

  Log.error("Can not handle expression `{{name}}`", {"name": keys[0]});
}//method


expressions = {};

expressions.when = function(expr){
    var test = qb2function(expr.when, true);
    var pass = qb2function(expr.then);
    var fail = qb2function(expr.else);
    return function(value){
      var condition=test(value);

      if (condition!=null && condition!=false) {
        return pass(value);
      } else {
        return fail(value);
      }//endif
    };
};


expressions.eq = function (expr) {
  if (isArray(expr.eq)) {
    var exprs = expr.eq.map(qb2function);
    return function (value) {
      return exprs[0](value) == exprs[1](value);
    };
  } else {
    return function (value) {
      return Array.AND(Map.map(expr.eq, function (k, v) {
        return Map.get(value, k) == v;
      }));
    };
  }//endif
};


expressions.literal=function(expr) {
  return function(){
    return expr.literal;
  }
};

expressions.case = function(expr){
  var test = [];
  var then = [];
  var els_ = function(){};

  expr.case.forall(function(s, i, switchs){
    if (i==switchs.length-1){
      els_ = qb2function(s);
    }else{
      test[i] = qb2function(s.when);
      then[i] = qb2function(s.then);
    }//endif
  });

  //(function(test, then, els_){
    return function(value){
      for (var i = 0; i < test.length; i++) {
        var cond = test[i](value);
        cond = cond != null && cond != false;
        if (cond) {
          return then[i](value);
        }//endoif
      }//for
      return els_(value);
    };
  //})(test, then, els_);

};


expressions.missing = function(expr){
  var missing = qb2function(expr.missing);

  return function(value){
    return missing(value) == null;
  }
};


