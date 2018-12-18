package org.interactivegraph.server

import org.apache.jena.query._
import org.apache.jena.rdf.model.Model
import org.apache.jena.tdb.TDBFactory
import org.apache.jena.util.FileManager
import org.springframework.beans.factory.annotation.Autowired

import scala.collection.mutable.ArrayBuffer

class QueryEngine{
  @Autowired
  //val inputFileName = "D:\\ProgramFiles\\IdeaWorkSpace\\JenaTest1\\DATA\\honglou.n3" ;
  var inputFileName = ""

  //def setInputfileName(s: String) = inputFileName = s;

  val directory: String  = "D:\\ProgramFiles\\IdeaWorkSpace\\JenaTest1\\DATA\\Databases\\Dataset1";
  //def setDirectory(s: String) = directory = s;
  var dataset:Dataset = TDBFactory.createDataset(directory);

  var modelName = "honglou";

  //def setModelName(s: String) = modelName = s;
  var model: Model  = null;

  model = dataset.getNamedModel(modelName);
  model.begin();
  FileManager.get().readModel(model,inputFileName);
  model.commit();
  dataset.begin(ReadWrite.WRITE);
  dataset.commit();
  var readmodel = dataset.getNamedModel(modelName);
  readmodel.begin();

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

  def queryEntry(queryString: String):ResultSet = {
    val query = QueryFactory.create(queryString);
    val qexec = QueryExecutionFactory.create(query,readmodel)
    val results:ResultSet = qexec.execSelect();
    return results;
  }

}
