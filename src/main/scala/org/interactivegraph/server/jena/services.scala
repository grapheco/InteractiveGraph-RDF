package org.interactivegraph.server.jena
import org.apache.jena.query.{Dataset, QueryExecutionFactory, QueryFactory, ResultSet}
import org.apache.jena.rdf.model.Model
import org.apache.jena.tdb.TDBFactory
import org.interactivegraph.server.util.Logging

import scala.collection.mutable.ArrayBuffer

trait SpaRQLService extends Logging {

  def queryByURI(uri: String): ResultSet={
    val queryString = s"""SELECT DISTINCT ?p ?o WHERE { "$uri" ?p ?o }""";
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
      edge_list += soln.get("s").toString
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
      node_list += soln.get("s").toString
    }
    return node_list
  }

  def queryEntry(queryString: String): ResultSet ;
}

class TDBService extends Logging with SpaRQLService {
  var _directory = "D:\\ProgramFiles\\IdeaWorkSpace\\Interactivegraph-RDF\\web\\WEB-INF\\Dataset1";
  var _modelName = "honglou";
  def setDirectory(value: String) = _directory = value;
  def setModelName(value: String) = _modelName = value;

  var dataset = TDBFactory.createDataset(_directory);
  var model = dataset.getNamedModel(_modelName);
//  def afterPropertiesSet(): Unit ={
//    dataset = TDBFactory.createDataset(_directory)
//    model = dataset.getNamedModel(_modelName)
//    model.begin()
//  }

  override def queryEntry(queryString: String): ResultSet = {
    println(queryString)
    val query = QueryFactory.create(queryString)
    val qexec = QueryExecutionFactory.create(query, model)
    val results:ResultSet = qexec.execSelect()
    return results;
  }
}