package com.knoldus.common

import org.slf4j.MDC

import scala.language.implicitConversions

object MDCProvider {
  def clear(): Unit = MDC.clear()
  implicit def mdcProviderFromMap(map: Map[String, String]): MDCProvider = new MDCProvider {
    override def mdcData: Map[String, String] = map
  }

  val empty: MDCProvider = Map.empty[String, String]
}

/**
 * Trait for types which can load diagnostic/tracing information into a Mapped Diagnostic Context.
 * While the decision to take advantage of the MDC is up to the glue etc. logic, the schema used in
 * the MDC is a concern of the type.
 */
trait MDCProvider {
  /**
   * The data to load into the MDC.
   *
   * Commonly-used keys (won't necessarily always apply)
   * systemId
   * filename
   * fileType (convention: prepend with a _ to indicate a data object that's not a file)
   */
  def mdcData: Map[String, String] = Map.empty[String, String]

  /**
   * Loads data into the MDC.  By default, just loads the contents of mdcData.
   */
  def setMDC(): Unit = // no arguments, Unit result: pure side effecting bliss...
    mdcData.foreach {
      case (k, v) =>
        MDC.put(k, v)
    }

  /**
   * Removes data from the MDC.  Removes all keys in `mdcData`.
   */
  def removeMDC(): Unit =
    mdcData.keys.foreach {
      k => MDC.remove(k)
    }
}
