package org.interactivegraph.server.jena

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.google.gson.JsonObject
import org.interactivegraph.server.util.Wrapper
import org.interactivegraph.server.{CommandExecutor, JsonCommandExecutor, Setting}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.mutable

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

  val infos = new ArrayBuffer[String]()
  def execute(request: JsonObject): Map[String,_] = {

    val ids = request.getAsJsonArray("nodeIds").toArray

    infos.clear()
    for (i <- ids){
      val id = i.toString.replace("\"","")
      val uri = "<http://interactivegraph.org/data/node/"+id+">"
      val node = _setting._sparqlService.queryByURI(uri)
      val info = Wrapper.wrapNode(uri,node).getOrElse("info",null)
      infos += info.toString
    }
    Map("infos" -> infos.toArray)
  }
}

class FilterNodesByCategory extends JsonCommandExecutor with JenaCommandExecutor{

  def execute(request: JsonObject): Map[String, _] = {
    val filteredNodeIds = new ArrayBuffer[Long]()
    val category = request.get("category").getAsString
    val ids = request.getAsJsonArray("nodeIds").toArray
    for(id <- ids){
      val query = s"""prefix honglou: <http://interactivegraph.org/app/honglou/>
                   prefix node: <http://interactivegraph.org/data/node/>
                   select ?s
                   where {node:$id honglou:categories "$category"} """
      if(_setting._sparqlService.queryEntry(query).hasNext){
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

    val id = request.get("nodeId").getAsString
    val neighbourEdges = queryNeighbourEdges(id)
    val neighbourNodes = queryNeighbourNodes(id)
    Map("neighbourEdges" -> neighbourEdges, "neighbourNodes" -> neighbourNodes)

  }
  def queryNeighbourEdges(nodeId: String): Array[Map[String, Any]] ={
    val neighbourEdges = new ArrayBuffer[Map[String,Any]]()
    val query_edgeuri = s"""PREFIX honglou: <http://interactivegraph.org/app/honglou/>
                            PREFIX node: <http://interactivegraph.org/data/node/>
                            SELECT DISTINCT ?s
                            WHERE {
                                 ?s honglou:from node:$nodeId
                              }""";
    val edge_uri_list = _setting._sparqlService.queryEntry(query_edgeuri)
    for(item <- edge_uri_list){
      val result = _setting._sparqlService.queryByURI(item.toString)
      neighbourEdges += Wrapper.wrapEdge(item.toString,result)
    }
    return neighbourEdges.toArray
  }
  def queryNeighbourNodes(nodeId: String): Array[Map[String, Any]] = {
    val neighbourNodes = new ArrayBuffer[Map[String, Any]]()
    val query_nodeuri = s"""PREFIX honglou: <http://interactivegraph.org/app/honglou/>
                            PREFIX node: <http://interactivegraph.org/data/node/>
                            SELECT DISTINCT ?o
                            WHERE {
                                 ?s honglou:from node:$nodeId ;
                                    honglou:to ?o
                              }""";
    val node_uri_list = _setting._sparqlService.queryEntry(query_nodeuri)
    for (item <- node_uri_list){
      val result = _setting._sparqlService.queryByURI(item.toString)
      neighbourNodes += Wrapper.wrapNode(item.toString,result)
    }
    return neighbourNodes.toArray
  }

}

class LoadGraph extends JsonCommandExecutor with JenaCommandExecutor {
  override def execute(request: JsonObject): Map[String, _] = {
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
    for(edgeURI <- edge_list){
      val result = _setting._sparqlService.queryByURI(edgeURI)
      edges += Wrapper.wrapEdge(edgeURI, result)
    }
    return edges.toArray
  }
}

class Search extends JsonCommandExecutor with JenaCommandExecutor {
  def execute(request: JsonObject): Map[String, _] = {
    val jexpr = request.get("expr")
    val limit = request.getAsJsonPrimitive("limit").getAsNumber
    val nodes =
      if(jexpr.isJsonArray){
        strictSearch(jexpr.getAsJsonArray.map(_.getAsJsonObject).toArray,limit)
      }
      else {
        regexpSearch(jexpr.getAsString,limit)
      }
    Map("nodes" -> nodes)
  }

  def getNodePropertyName(searchField: String): String = {
    val ssf = setting()._strictSearchFields
    ssf.getOrDefault(searchField, searchField)
  }

  def strictSearch(objects: Array[JsonObject], limit: Number): Array[_] = {
    val nodes = new ArrayBuffer[Map[String,Any]]()
    return nodes.toArray
  }
  def regexpSearch(expr: String, limit: Number): Array[_] = {
    val nodes = new ArrayBuffer[Map[String,Any]]()
    val predictate = setting()._regexpSearchFields.toString
    val query = s"""SELECT ?s
                  WHERE {
                    ?s $predictate ?o
                    FILTER regex(str(?o), ".*$expr.*")}"""
    val uriResultSet = _setting._sparqlService.queryEntry(query)
    var uriList = new ArrayBuffer[String]()
    while (uriResultSet.hasNext){
      val item = uriResultSet.nextSolution().get("s")
      uriList += "<"+item.toString+">"
    }
    for(uri <- uriList){
      val result = _setting._sparqlService.queryByURI(uri)
      nodes += Wrapper.wrapNode(uri,result)
    }
    return nodes.toArray
  }
}

object FindRelationsTaskManager {
  val seq = new AtomicInteger(1224)
  val allTasks = mutable.Map[String, FindRelationsTask]()

  class FindRelationsTask(executor: JenaCommandExecutor, startID: String, endId:String, maxDepth: Int) {
    var _isCompleted = false
    var lock = new CountDownLatch(1)
    val taskId = seq.incrementAndGet()
    allTasks("" + taskId) = this
    var flagStop = false
    val paths = ArrayBuffer[Map[_, _]]()
    val thread = new Thread(new Runnable() {
      override def run(): Unit = {
        val urilist = executor.setting()._sparqlService.queryPath(startID,endId,maxDepth)
        val nodeURIList = urilist(0)
        val edgeURIList = urilist(1)
        var nodes = new ArrayBuffer[Map[String,_]]()
        var edges = new ArrayBuffer[Map[String,_]]()
        lock.countDown()
        while(!flagStop){
          for(i <- nodeURIList){
            val result = executor.setting()._sparqlService.queryByURI(i)
            nodes += Wrapper.wrapNode(i,result)
          }
          for(i <- edgeURIList){
            val result = executor.setting()._sparqlService.queryByURI(i)
            edges += Wrapper.wrapEdge(i,result)
          }
          val path = Wrapper.wrapPath(nodes.toArray,edges.toArray)
          paths.append(path)
        }
        _isCompleted = true
        }
      });
    thread.start()
    def isCompleted() = _isCompleted

    def readMore(limit: Int): (Array[_], Boolean) = {
      val token = paths.take(limit)
      paths.remove(0, token.size)
      (token.toArray, !paths.isEmpty)
    }

    def waitForExecution(): Unit = {
      lock.await()
    }

    def stop(): Unit = {
      flagStop = true
      thread.interrupt()
    }
  }

  def createTask(executor: JenaCommandExecutor, startID: String, endId:String, maxDepth: Int): FindRelationsTask = {
    new FindRelationsTask(executor, startID,endId,maxDepth)
  }
  def getTask(taskId: String) = allTasks(taskId)
}

class FindRelations extends JsonCommandExecutor with JenaCommandExecutor {
  def execute(request: JsonObject):Map[String, _] = {
    val startNodeId = request.get("startNodeId").getAsString
    val endNodeId = request.get("endNodeId").getAsString
    val maxDepth = request.get("maxDepth").getAsInt

    val task = FindRelationsTaskManager.createTask(this,startNodeId,endNodeId,maxDepth)
    Thread.sleep(1)
    task.waitForExecution()

    Map("queryId" -> task.taskId)
  }
}

class GetMoreRelations extends JsonCommandExecutor with JenaCommandExecutor {
  def execute(request: JsonObject): Map[String, _] = {
    val queryId = request.get("queryId").getAsString

    val task = FindRelationsTaskManager.getTask(queryId)
    val (paths, hasMore) = task.readMore(10)

    Map("completed" -> task.isCompleted,
        "queryId" -> task.taskId,
        "paths" -> paths);
  }
}

class StopFindRelations extends JsonCommandExecutor with JenaCommandExecutor {
  def execute(request: JsonObject): Map[String, _] = {
    val queryId = request.get("queryId").getAsString

    val task = FindRelationsTaskManager.getTask(queryId)
    task.stop()

    Map(
      "queryId" -> task.taskId,
      "stopped" -> true
    );
  }
}
