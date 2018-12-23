package org.interactivegraph.server.jena

import java.util.{Map => JMap}

import org.interactivegraph.server.Setting
import org.interactivegraph.server.util.VelocityUtils
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired

import scala.collection.JavaConversions._

class JenaSetting extends Setting {
  @Autowired
  var _sparqlService: SpaRQLService = _;
  var _backendType = "";
  var _categories: Map[String, String] = _;

  def setBackendType(s: String) = _backendType = s;

  def setNodeCategories(s: String) = _categories =
    s.split(",").
      filter(_.contains(":")).
      map(_.split(":")).
      map(x => x(0) -> x(1)).toMap;

  var _regexpSearchFields: String = _;
  var _strictSearchFields: Map[String, String] = Map[String, String]();

  def setRegexpSearchFields(s: String) = _regexpSearchFields = s

  def setStrictSearchFields(s: String) = _strictSearchFields =
    s.split(",").
      filter(_.contains(":")).
      map(_.split(":")).
      map(x => x(0) -> x(1)).toMap;

  def setSparqlService(value: SpaRQLService) = _sparqlService = value;
  var _graphMetaDB: GraphMetaDB = _;

  def setGraphMetaDB(value: GraphMetaDB) = _graphMetaDB = value

}

trait GraphMetaDB{
  def getNodesCount(): Option[Int]

  def getEdgesCount(): Option[Int]

  //def getNodeMeta(nodeURI: String): Map[String, _]
}

class JenaGraphMetaDBInMemory extends GraphMetaDB with InitializingBean {
  @Autowired
  var _sparqlService: SpaRQLService = _;
  var _nodesDegreeMap = collection.mutable.Map[String, Int]()
  var _nodesCount: Option[Int] = None
  var _edgesCount: Option[Int] = None
  var _graphNodeProperties = collection.mutable.Map[String, String]()

  def setVisNodeProperties(v: JMap[String, String]) = {
    _graphNodeProperties ++= v;
  }

  def updateMeta() = {
    val node_prefix = "http://interactivegraph.org/data/node/"
    val edge_prefix = "http://interactivegraph.org/data/edge/"
    val queryNodesCount = s"""SELECT (count(DISTINCT ?s) as ?count) WHERE {?s ?p ?o FILTER regex(str(?s), "^$node_prefix")}"""
    val queryEdgesCount = s"""SELECT (count(DISTINCT ?s) as ?count) WHERE {?s ?p ?o FILTER regex(str(?s), "^$edge_prefix")}"""
    _nodesCount = Some(_sparqlService.queryEntry(queryNodesCount).nextSolution().get("count").asLiteral().getInt)
    _edgesCount = Some(_sparqlService.queryEntry(queryEdgesCount).nextSolution().get("count").asLiteral().getInt)

    _nodesDegreeMap.clear()
    val node_list = _sparqlService.queryNodeURI()
    for(nodeURI <- node_list){
      val id = nodeURI.replace(">","").split("/").last
      val queryDegree = s"""SELECT (count(DISTINCT ?s) as ?count)
                    WHERE {
                      ?s <http://interactivegraph.org/app/honglou/from> $nodeURI
                    }"""
      val degree = _sparqlService.queryEntry(queryDegree).nextSolution().get("count").asLiteral().getInt
      _nodesDegreeMap.update(id,degree)
    }
  }

  def afterPropertiesSet(): Unit = updateMeta()

  override def getEdgesCount(): Option[Int] = _edgesCount
  override def getNodesCount(): Option[Int] = _nodesCount

//  override def getNodeMeta(nodeURI: String): Map[String, _] = {
//    val node = _sparqlService.queryByURI(nodeURI)
//  }


}
