package com.knoldus.akka

import com.knoldus.common.utils.ResourceCompanion
import com.knoldus.elasticsearch.api.Elasticsearch
import com.knoldus.models.{Company, Employee}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
object CreateIndex {
  private val mappings: List[ResourceCompanion[_]] = List(Employee,Company)

  private val config: Config = ConfigFactory.load()

  private val esHosts = config.getString("elasticsearch.host")

  private val esHostsList = esHosts.split(",")
  private val esIndex = config.getString("elasticsearch.index.default")

  implicit val ec: ExecutionContext = ExecutionContext.global

  def run(): Unit = {
    val es = new Elasticsearch(esHostsList)
    val indexCreated = es.createIndex(esIndex)
     Await.result(indexCreated, 2.minutes)

  }
}
