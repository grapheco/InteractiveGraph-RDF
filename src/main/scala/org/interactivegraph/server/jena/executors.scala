package org.interactivegraph.server.jena

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.google.gson.JsonObject
import org.interactivegraph.server.util.Wrapper
import org.interactivegraph.server.{CommandExecutor, JsonCommandExecutor, QueryEngine, Setting}
import org.apache.jena.query.ResultSet
import org.interactivegraph.server.util
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Triple}
import org.apache.jena.ontology.OntTools.Path
import org.apache.jena.vocabulary.RDFS.Nodes

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


trait JenaCommandExecutor extends CommandExecutor {
  var _setting : JenaSetting = _;

  override def initialize(setting: Setting): Unit = {
    _setting = setting.asInstanceOf[JenaSetting]
  }

  def setting() :JenaSetting = _setting;
}

class Init extends JsonCommandExecutor with JenaCommandExecutor {
  override def initialize(setting: Setting): Unit = {
    _setting = setting.asInstanceOf[JenaSetting]
  }
  override def execute(request: JsonObject): Map[String, _] = {
    Map("product" -> "jena",
      "backendType" -> setting()._backendType,
      "categories" -> setting()._categories
    ) ++
      setting()._graphMetaDB.getNodesCount().map("nodesCount" -> _) ++
      setting()._graphMetaDB.getEdgesCount().map("edgesCount" -> _)
  }
}

class GetNodesInfo extends JsonCommandExecutor with JenaCommandExecutor{

  val infos = new ArrayBuffer[Map[String, _]]()
  def execute(request: JsonObject): Map[String,_] = {
    //val engine = new QueryEngine()
    val ids = request.getAsJsonArray("nodeIds").toArray
    for (id <- ids){
      val uri = "<http://interactivegraph.org/data/node/"+id+">"
      val node = _setting._sparqlService.queryByURI(uri)
      val wrappedNode = Wrapper.wrapNode(uri,node).getOrElse("info",null).asInstanceOf[Map[String,_]]
      infos += wrappedNode
    }
    Map("infos" -> infos)
  }
}

class FilterNodesByCategory extends JsonCommandExecutor with JenaCommandExecutor{

  def execute(request: JsonObject): Map[String, _] = {
    val engine = new QueryEngine()
    val filteredNodeIds = new ArrayBuffer[Long]()
    val category = request.get("category").getAsString
    val ids = request.getAsJsonArray("nodeIds").toArray
    for(id <- ids){
      val query = s"""prefix honglou: <http://interactivegraph.org/app/honglou/>
                   prefix node: <http://interactivegraph.org/data/node/>
                   select ?s
                   where {node:$id honglou:categories "$category"} """
      if(engine.queryEntry(query).hasNext){
            filteredNodeIds += id.getAsLong
      }
    }
    Map("filteredNodeIds" -> filteredNodeIds)
  }
}

//class Search extends JsonCommandExecutor with JenaCommandExecutor {
//  def execute(request: JsonObject): Map[String, _] = {
//    val jexper = request.get("expr")
//    val limit = request.getAsJsonPrimitive("limit").getAsNumber
//  }
//
//  def getNodePropertyName(searchFiled: String): String = {
//    val ssf = setting()._strictSearchFields
//    ssf.getOrDefault(searchFiled, searchFiled)
//  }
//
//  def strictSearch(filters: Array[JsonObject], limit: Number): Array[_] = {
//
//  }
//}
class GetNeighbours extends JsonCommandExecutor with JenaCommandExecutor{
  def execute(request: JsonObject): Map[String, _] = {
    val engine = new QueryEngine()
    val id = request.get("nodeId").getAsString
    val neighbourEdges = queryNeighbourEdges(id, engine)
    val neighbourNodes = queryNeighbourNodes(id, engine)
    Map("neighbourEdges" -> neighbourEdges, "neighbourNodes" -> neighbourNodes)

  }
  def queryNeighbourEdges(nodeId: String,engine: QueryEngine): Array[Map[String, Any]] ={
    val neighbourEdges = new ArrayBuffer[Map[String,Any]]()
    val query_edgeuri = s"""PREFIX honglou: <http://interactivegraph.org/app/honglou/>
                            PREFIX node: <http://interactivegraph.org/data/node/>
                            SELECT DISTINCT ?s
                            WHERE {
                                 ?s honglou:from node:$nodeId
                              }""";
    val edge_uri_list = engine.queryEntry(query_edgeuri)
    for(item <- edge_uri_list){
      val result = engine.queryByURI(item.toString)
      neighbourEdges += Wrapper.wrapEdge(item.toString,result)
    }
    return neighbourEdges.toArray
  }
  def queryNeighbourNodes(nodeId: String, engine: QueryEngine): Array[Map[String, Any]] = {
    val neighbourNodes = new ArrayBuffer[Map[String, Any]]()
    val query_nodeuri = s"""PREFIX honglou: <http://interactivegraph.org/app/honglou/>
                            PREFIX node: <http://interactivegraph.org/data/node/>
                            SELECT DISTINCT ?o
                            WHERE {
                                 ?s honglou:from node:$nodeId ;
                                    honglou:to ?o
                              }""";
    val node_uri_list = engine.queryEntry(query_nodeuri)
    for (item <- node_uri_list){
      val result = engine.queryByURI(item.toString)
      neighbourNodes += Wrapper.wrapNode(item.toString,result)
    }
    return neighbourNodes.toArray
  }

}

class LoadGraph extends JsonCommandExecutor with JenaCommandExecutor {
  override def execute(request: JsonObject): Map[String, _] = {
    //val engine = new QueryEngine()
    val nodes = queryNodes()
    val edges = queryEdges()
    Map[String,Any]("nodes" -> nodes,"edges"->edges)

  }
  private def queryNodes(): Array[Map[String, Any]] = {
    val node_list = _setting._sparqlService.queryNodeURI()
    val nodes = new ArrayBuffer[Map[String,Any]]()
    for(item <- node_list){
      val result = _setting._sparqlService.queryByURI(item)
      nodes += Wrapper.wrapNode(item, result)
    }
    return nodes.toArray
  }
  private def queryEdges(): Array[Map[String, _]] = {
    val edge_list = _setting._sparqlService.queryEdgeURI()
    val edges = new ArrayBuffer[Map[String,Any]]()
    for(item <- edge_list){
      val result = _setting._sparqlService.queryByURI(item)
      edges += Wrapper.wrapEdge(item, result)
    }
    return edges.toArray
  }
}

