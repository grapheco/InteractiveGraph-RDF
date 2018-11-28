package org.interactivegraph.server

import java.io.OutputStream

import com.google.gson.JsonObject
import org.interactivegraph.server.util.JsonUtils


class UnrecognizedCommandException(command: String)
  extends RuntimeException(s"unrecognized command: $command") {
}

trait CommandExecutorRegistry{
  def executorOf(command: String): Option[CommandExecutor];
}

trait ContentTag {
  def setContentType(ct: String)
  def setContentLength(len: Int)
  def setCatacterEncoding(en: String)
}

trait CommandExecutor{
  def initialize(setting: Setting)

  def execute(requestBody: JsonObject, ct: ContentTag, out: OutputStream)
}

trait JsonCommandExecutor extends CommandExecutor{
  override final def execute(requestBody: JsonObject, ct: ContentTag, out: OutputStream): Unit = {
    ct.setCatacterEncoding("utf-8")
    ct.setContentType("text/json")

    val responseBody = execute(requestBody)
    out.write(JsonUtils.stringify(responseBody).getBytes("utf-8"))
  }

  def execute(request: JsonObject): Map[String, _];
}