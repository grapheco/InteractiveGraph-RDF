package org.interactivegraph.server.util

import org.apache.log4j.Logger

trait Logging {
  protected lazy  val logger = Logger.getLogger(this.getClass)
}