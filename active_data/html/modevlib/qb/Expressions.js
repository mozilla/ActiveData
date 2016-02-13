function qb2function(expr){

  if (expr == null) return function(){
    return null;
  };
  if (expr === true) return function(){
    return true;
  };
  if (expr === false) return function(){
    return false;
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
  };

  if (expr.eq) {
    if (isArray(expr.eq)) {
      var exprs = expr.eq.map(qb2function);
      return function(value){
        return exprs[0](value) == exprs[1](value);
      };
    } else {
      return function(value){
        return Array.AND(Map.map(expr.eq, function(k, v){
          return Map.get(value, k) == v;
        }));
      };
    }//endif
  } else if (expr.when) {
    var test = qb2function(expr.when);
    var pass = qb2function(expr.then);
    var fail = qb2function(expr.else);
    return function(value){
      if (test(value)) {
        return pass(value);
      } else {
        return fail(value);
      }//endif
    };
  } else {
    var key = Map.getItems(expr).first();
    Log.error("Can not handle expression `{{name}}`", {"name": key});
  }//endif
}//method
