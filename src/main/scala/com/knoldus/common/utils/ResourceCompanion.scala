package com.knoldus.common.utils

import com.typesafe.config.{Config, ConfigFactory}

object ResourceCompanion {
  lazy val config: Config = ConfigFactory.load()
}
trait ResourceCompanion[A] {

  def id(obj: A): String

  /**
   * Elasticsearch index to store documents of the given type
   */
  def index: String

  /**
   * Resource type in the JSON
   */
  def resourceType: String

  /**
   * Elasticsearch document type
   */
  def docType: String = resourceType

  def indexDoc: (String, String) = (index, docType)
  def indexDocPath: String = s"$index/$docType"
 // val meta: ModelMeta = ModelMeta(Seq())

  /**
   * ignoreUnknownFields defaults to false (ES dynamic mapping 'strict'), which means that unknown fields are
   * not ignored, ES will not persist a document with unknown fileds, it will report an error.
   *
   * Setting ignoreUnknownFields to true (ES dynamic mapping 'false') means that unknown fields written to ES
   * will be persisted, but not indexed - they will effectively be ignored.
   */
  def ignoreUnknownFields: Boolean = false

  /**
   * allowDynamicMapping defaults to false in which case only ignoreUnknownFields dictates the behaviour of the ES mapping.
   * Setting ignoreUnknownFields to false in conjunction with ignoreUnknownFields true means that unknown fields are
   * persisted to ES and indexed for querying
   */
  def allowDynamicMapping: Boolean = false

  /**
   * If the Resource is a child document, then return it's parentID.
   * Note, that because the Service#get(id) method only has the resource ID,
   * we need to determine the parent ID from the child ID.
   * Currently, this is how we create child IDs anyway, so it's not a problem,
   * but is worth being aware of.
   */
  def parentId(id: String): Option[String] = None

  def parentType: Option[ResourceCompanion[_]] = None
}
