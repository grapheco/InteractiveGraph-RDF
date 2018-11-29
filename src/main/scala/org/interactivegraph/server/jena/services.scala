package org.interactivegraph.server.jena
//
//import javax.servlet.http.HttpSession
//import org.apache.jena.query.ResultSet
//import org.apache.jena.tdb.base.record.Record
import org.interactivegraph.server.util.Logging
//import org.apache.jena.jdbc._
//
//import scala.reflect.ClassTag
//
//trait SparqlService extends Logging {
//  def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Array[T]
//
//  def execute[T](f: (HttpSession) => T): T;
//
//  def executeQuery[T](queryString: String, fn: (ResultSet => T)): T;
//
//  final def querySingleObject[T](queryString: String, fnMap: (Record => T)): T = {
//    executeQuery(queryString, (rs: ResultSet) => {
//      fnMap(rs.next());
//    });
//  }
//
//class FusekiService extends Logging with SparqlService {
//  var _url = ""
//  var _user = ""
//  var _pass = ""
//
//  def setFusekiUrl(value: String) = _url = value;
//  def setFusekiUser(value: String) = _user = value;
//  def setFusekiPassword(value: String) = _pass = value;
//
//  override def execute[T](f: (HttpSession) => T): T = {
//    val driver = JenaJDBC.
//  }
//
//  }
//
//}