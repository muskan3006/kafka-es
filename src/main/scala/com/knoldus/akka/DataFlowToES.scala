package com.knoldus.akka

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import akka.{Done, NotUsed}
import com.knoldus.akka.AkkaKafkaConsumer.DeserializationFlowFactory
import com.knoldus.common.utils.{CommonFlows, HasDefaultConfig, ResourceCompanion}
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Format
import spray.json.JsonWriter

import scala.language.implicitConversions

abstract class DataFlowToES( index: String, esHosts: String,
    esRestPort: Int)(implicit val actorSystem: ActorSystem, actorMaterializer: ActorMaterializer,
    dff: DeserializationFlowFactory) extends AkkaKafkaConsumer
  with AkkaElasticsearchProducer with HasDefaultConfig {
  private implicit val log: Logger = LoggerFactory.getLogger(getClass)

  private val esHostsList = esHosts.split(",")
  private implicit val esIndex: String
  = index

  implicit def topicName(topic: String): String = "employee"

  private implicit val client: RestClient = RestClient.builder(esHostsList.map(host => new HttpHost(host, esRestPort)): _*).build()
//
  def r[T](source: Source[T, _], streamName: String): Unit =
    CommonFlows.Log.runIgnoredWithCompletionLogging(
      source = defaultRestartConfig.source(source),
      successMsg = s"Stream completed normally: $streamName",
      failureMsg = s"Stream failed: $streamName"
    )

  def k[T](p: (String, Flow[T, Done, NotUsed]))(implicit format: Format[T]): Unit = {
    val (topic, flow) = p
val streamName = s"backened for $topic"
    r(jsonWithCommits[T](topic, flow),streamName)
  }

  def j[T](p: (String, Flow[T, Done, NotUsed]))(implicit format: Format[T]): Unit =
    k(p._1, p._2)

  def writeDataFlowToES[A](topicName: String, rc: ResourceCompanion[A])(implicit jsonWriter: JsonWriter[A]): (String, Flow[A, Done, NotUsed]) =
    topicName -> writeRCToES[A](rc)

//  def writeSeqDataFlowToES[A](rc: ResourceCompanion[A], idExtractor: A => String)(implicit format: Format[A]): (Flow[Vector[A], Done, NotUsed]) =
//    writeSeqToElasticsearch[A](rc, idExtractor)
//
//  def messageFlowToES[A](rc: ResourceCompanion[A])(implicit firmat: Format[A]): Flow[A, Done, NotUsed] =
//    writeRCToES[A](rc)

}
