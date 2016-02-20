
function ESQueryRunner(query, callback){
  Thread.run(function*(){
    yield(ESQuery.loadColumns(query));
    var cubeQuery = new ESQuery(query);
    var data = yield(cubeQuery.run());
    callback(data);
  });
}//method

function ESQueryRunMany(queries, callback){
  //ASSUME queries IS AN ARRAY
  Thread.run(function*(){
    //LAUNCH ALL QUERIES
    var threads=[];
    for(var i=0;i<queries.length;i++){
      threads[i]=Thread.run((function*(j){
        yield(ESQuery.loadColumns(query));
        var cubeQuery = new ESQuery(queries[j]);
        var data = yield(cubeQuery.run());
        yield (data);
      })(i));
    }//for

    //COLLECT ALL RESULTS
    data=[];
    for(var i=0;i<queries.length;i++){
      data[i]=threads[i].join().threadResponse
    }//for

    //EXECUTE CALLBACK
    callback(data);
  });
}//method



importScript([
  "aLibrary.js",
  "jx/ESQuery.js"
], function(){
  return "done";
});
