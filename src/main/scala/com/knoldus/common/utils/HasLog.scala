package com.knoldus.common.utils

import org.slf4j.{ Logger, LoggerFactory }

/**
 * Mixin this trait to get a Logger named log
 */
trait HasLog {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
}

