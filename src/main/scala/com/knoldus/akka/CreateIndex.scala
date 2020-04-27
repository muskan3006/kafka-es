package com.knoldus.akka

import com.knoldus.common.utils.ResourceCompanion
import com.knoldus.elasticsearch.api.Elasticsearch
import com.knoldus.models.Employee
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
object CreateIndex {
 // private val mappings: Seq[ResourceCompanion[_]] = Seq(Employee)
  private val config: Config = ConfigFactory.load()

  private val esHosts = "localhost"
    //config.getString("elasticsearch.cluster.hosts.storeserv")
 // private val esRestPort = config.getString("elasticsearch.cluster.port.storeserv").toInt
  private val esHostsList = esHosts.split(",")
  private val esIndex = "employee-index"

  implicit val ec: ExecutionContext = ExecutionContext.global

  def run(): Unit = {
    val es = new Elasticsearch(esHostsList)
    val indexCreated = es.createIndex(esIndex)
    Await.result(indexCreated, 2.minutes)

  }
}
