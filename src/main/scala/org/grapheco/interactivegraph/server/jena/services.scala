package org.grapheco.interactivegraph.server.jena

import org.apache.jena.query.{Dataset, QueryExecutionFactory, QueryFactory, ResultSet}
import org.apache.jena.rdf.model.Model
import org.apache.jena.tdb.TDBFactory
import org.grapheco.interactivegraph.server.util.Logging
import org.springframework.beans.factory.InitializingBean

import scala.collection.mutable.ArrayBuffer

trait SpaRQLService extends Logging {

  def queryForRelFinder[T](qr: String, fn: (ResultSet => T)): T = {
    val resultSet = queryEntry(qr)
    fn(resultSet)
  }

  def queryByURI(uri: String): ResultSet={
    val queryString = s"""SELECT DISTINCT ?p ?o WHERE { $uri ?p ?o }""";
    queryEntry(queryString)
  }

  //TO DO
  def queryURI(qr: String): ResultSet={
    val queryString = s"""SELECT DISTINCT ?p ?o WHERE { $qr ?p ?o }""";
    queryEntry(queryString)
  }

  def queryEdgeURI(): ArrayBuffer[String] = {
    val queryStirng = s"""SELECT DISTINCT ?s
    WHERE {
    ?s ?p ?o
    FILTER regex(str(?s), "^http://interactivegraph.org/data/edge/")
  }"""
    val edge_list = ArrayBuffer[String]()
    val results = queryEntry(queryStirng)
    while(results.hasNext){
      val soln = results.nextSolution()
      edge_list += "<"+soln.get("s").toString+">"
    }
    return edge_list
  }

  //TO DO
  def queryNodeURI(): ArrayBuffer[String] = {
    val queryStirng = s"""SELECT DISTINCT ?s
    WHERE {
    ?s ?p ?o
    FILTER regex(str(?s), "^http://interactivegraph.org/data/node/")
  }"""
    val node_list = ArrayBuffer[String]()
    val results = queryEntry(queryStirng)
    while(results.hasNext){
      val soln = results.nextSolution()
      node_list += "<"+soln.getResource("s").toString+">"
    }
    return node_list
  }

  def queryPath(startId: String, endId: String, maxDepth: Int): Array[Array[String]] = {
    var queryList1 = new ArrayBuffer[String]()
    var queryList2 = new ArrayBuffer[String]()
    //var queryList = new ArrayBuffer[String]()
    val startURI = s"<http://interactivegraph.org/data/node/$startId>"
    val endURI = s"<http://interactivegraph.org/data/node/$endId>"
    val from_predicate = "<http://interactivegraph.org/app/honglou/from>"
    val to_predicate = "<http://interactivegraph.org/app/honglou/to>"

    //?s1
    if (maxDepth == 1){
      //start to end
      queryList1 +=
        s"""SELECT ?s1 WHERE {  ?s1 $from_predicate $startURI ;
           $to_predicate $endURI. }"""
      //end to start
      queryList1 +=
        s"""SELECT ?s1 WHERE {  ?s1 $from_predicate $endURI ;
           $to_predicate $startURI. }"""
    }

    //?s1, ?s2, ?n1
    if (maxDepth == 2){
      queryList2 +=
        s"""SELECT ?s1 WHERE {  ?s1 $from_predicate $startURI ;
           $to_predicate $endURI. }"""
      queryList2 +=
        s"""SELECT ?s1 WHERE {  ?s1 $from_predicate $endURI ;
           $to_predicate $startURI. }"""
      // start to n, n to end
      queryList2 += s"""SELECT ?s1 ?s2 ?n1 WHERE {
                  ?s1 $from_predicate $startURI ; $to_predicate ?n1.
                         ?s2 $from_predicate ?n1;  $to_predicate $endURI }"""
      // n to start, n to end
      queryList2 += s"""SELECT ?s1 ?s2 ?n1 WHERE {
                  ?s1 $from_predicate ?n1 ; $to_predicate $startURI.
                         ?s2 $from_predicate ?n1;  $to_predicate $endURI }"""
      // n to start, end to n
      queryList2 += s"""SELECT ?s1 ?s2 ?n1 WHERE {
                  ?s1 $from_predicate ?n1 ; $to_predicate $startURI.
                         ?s2 $from_predicate $endURI;  $to_predicate ?n1 }"""
      // start to n, end to n
      queryList2 += s"""SELECT ?s1 ?s2 ?n1 WHERE {
                  ?s1 $from_predicate $startURI ; $to_predicate ?n1.
                         ?s2 $from_predicate $endURI;  $to_predicate ?n1 }"""

    }

    //?si, ?s2, ?s3, ?n1, ?n2
    if (maxDepth == 3){
      // TO DO
    }
    queryList1.toArray
    queryList2.toArray
    var result_nodesURI = new ArrayBuffer[String]()
    var result_edgesURI = new ArrayBuffer[String]()

    //查询1层深度
    if(maxDepth == 1){
      for(query <- queryList1){
        val result = queryEntry(query)
        while(result.hasNext){
          val soln = result.nextSolution()
          result_edgesURI += soln.get("s1").toString
        }
      }
    }
    //查询2层深度
    if(maxDepth == 2){
      for(query <- queryList2){
        val result = queryEntry(query)
        while(result.hasNext){
          val soln = result.nextSolution()
          result_edgesURI += soln.get("s1").toString
          result_edgesURI += soln.get("s2").toString
          result_nodesURI += soln.get("n1").toString
        }
      }
    }

    var finalResult = new Array[Array[String]](2)
    finalResult(0) = result_nodesURI.toArray
    finalResult(1) = result_edgesURI.toArray

    return finalResult
  }

  def queryEntry(queryString: String): ResultSet ;

}

class TDBService extends Logging with SpaRQLService with  InitializingBean{
  //var _directory = "D:\\ProgramFiles\\IdeaWorkSpace\\Interactivegraph-RDF\\web\\WEB-INF\\Dataset1";
  //var _modelName = "honglou";
  var _directory = ""
  var _modelName = ""
  def setDirectory(value: String) = _directory = value;
  def setModelName(value: String) = _modelName = value;

  var dataset = TDBFactory.createDataset(_directory);
  var model = dataset.getNamedModel(_modelName);

  def afterPropertiesSet(): Unit ={
    dataset = TDBFactory.createDataset(_directory)
    model = dataset.getNamedModel(_modelName)
    //model.begin()
  }


  override def queryEntry(queryString: String): ResultSet = {
    val query = QueryFactory.create(queryString)
    val qexec = QueryExecutionFactory.create(query, model)
    val results:ResultSet = qexec.execSelect()
    return results;
  }
}