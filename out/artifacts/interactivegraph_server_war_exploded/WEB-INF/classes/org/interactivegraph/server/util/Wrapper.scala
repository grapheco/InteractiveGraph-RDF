package org.interactivegraph.server.util

import org.apache.jena.query.ResultSet

object Wrapper{
  def wrapNode(uri: String, elements: ResultSet): Map[String, _] ={
    val nodeMap = collection.mutable.Map[String, Any]()
    while(elements.hasNext){
      val soln = elements.nextSolution()
      val item_p = soln.get("p").toString
      val item_o = soln.get("o").toString
      val prop = item_p.split("/").last
      val value = item_o
      nodeMap += (prop -> value)
    }
    val id = uri.split("/").last.replaceAll(">","")
    nodeMap.toMap + ("id" -> id)
  }

  def wrapEdge(uri: String, elements: ResultSet): Map[String, _] = {
    val edgeMap = collection.mutable.Map[String, Any]()
    while(elements.hasNext){
      val soln = elements.nextSolution()
      val item_p = soln.get("p").toString
      val item_o = soln.get("o").toString
      val prop = item_p.split("/").last
      val value = item_o
      edgeMap += (prop -> value)
    }
    edgeMap.toMap
  }
}