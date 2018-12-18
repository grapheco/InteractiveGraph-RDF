package org.interactivegraph.server

class Setting {
  var _executors: CommandExecutorRegistry = _;

  def setCommandExecutorRegistry(value: CommandExecutorRegistry) = _executors = value;

}