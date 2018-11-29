package org.interactivegraph.server.jena

import java.util.{Map => JMap}

import org.apache.jena.graph.Node
import org.interactivegraph.server.Setting
import org.interactivegraph.server.util.VelocityUtils

import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired

class JenaSetting extends Setting {
  @Autowired
  var _sparqlService: SparqlService = _;
  var _backendType = "";
  var _categories: Map[String, String] = _;

  def setBackendType(s: String) = _backendType = s;

  def setNodeCategories(s: String) = _categories =
    s.split(",").
      filter(_.contains(":")).
      map(_.split(":")).
      map(x => x(0) -> x(1)).toMap;

  var _regexpSearchFields: Array[String] = Array();
  var _strictSearchFields: Map[String, String] = Map[String, String]();

  def setRegexpSearchFields(s: String) = _regexpSearchFields = s.split(".")

  def setStrictSearchFields(s: String) = _strictSearchFields =
    s.split(",").
      filter(_.contains(":")).
      map(_.split(":")).
      map(x => x(0) -> x(1)).toMap;

  def setSparqlService(value: SparqlService) = _sparqlService = value;
  var _graphMetaDB: GraphMetaDB = _;

  def setGraphMetaDB(value: GraphMetaDB) = _graphMetaDB = value

}

trait GraphMetaDB{
  def getNodesCount(): Option[Int]

  def getEdgesCount(): Option[Int]

  def getNodeMeta(node: Node): Map[String, _]
}

class JenaGraphMetaDBInMemory extends GraphMetaDB with InitializingBean {
  @Autowired
  var _sparqlService: SparqlService = _;
  var _nodesDegreeMap = collection.mutable.Map[String, Int]()
  var _nodesCount: Option[Int] = None
  var _edgesCount: Option[Int] = None
  var _graphNodeProperties = collection.mutable.Map[String, String]()

  def setVisNodeProperties(v: JMap[String, String]) = {
    _graphNodeProperties ++= v;
  }

  def updateMeta() = {
    _nodesCount = Some(_sparqlService.querySingleObject())
  }
}

