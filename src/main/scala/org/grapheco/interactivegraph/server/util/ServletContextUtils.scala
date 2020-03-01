package org.grapheco.interactivegraph.server.util

import javax.servlet.ServletContext

object ServletContextUtils {
  private var _servletContext: ServletContext = _;

  def setServletContext(value: ServletContext) = _servletContext = value

  def getServletContext = _servletContext
}