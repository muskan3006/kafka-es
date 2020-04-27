package com.knoldus.common.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Mixin for objects that "have [Typesafe] configs"
 */
trait HasConfig {
  protected val config: Config
}

/**
 * Mixin for objects that use a config in default (usually "application.conf") location
 */
trait HasDefaultConfig extends HasConfig {
  override protected val config: Config = ConfigFactory.load()
}
