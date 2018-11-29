package org.interactivegraph.server.jena

import java.util.concurrent.CountDownLatch

import org.interactivegraph.server.CommandExecutor


trait JenaCommandExecutor extends CommandExecutor {
  var _setting = JenaSetting = _;

  override def initialize(setting: Setting)
}