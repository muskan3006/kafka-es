package com.knoldus.akka

import akka.actor.ActorSystem
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl.Flow
import akka.{Done, NotUsed}
import com.knoldus.common.utils.{CommonFlows, Play2Spray, ResourceCompanion}
import com.knoldus.elasticsearch.api.ElasticsearchClient.ElasticsearchIndex
import com.typesafe.config.Config
import org.elasticsearch.client.RestClient
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Format
import spray.json.JsonWriter

trait AkkaElasticsearchProducer {
  type WriteFlow[A] = Flow[WriteMessage[A, NotUsed], Seq[WriteResult[A, NotUsed]], NotUsed]
  val defaultMetaExtractor: Any => Map[String, String] = _ => Map.empty[String, String]
  private val log: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a Flow to write objects to Elasticsearch based on the objects' ResourceCompanion.  This is most likely the
   * function you want to use.
   */
  def writeRCToES[A](rc: ResourceCompanion[A])(implicit actorSystem: ActorSystem, log: Logger,
                                               playF: Format[A], client: RestClient,
                                               esIndex: String,
                                               settings: AkkaElasticsearchProducer.Settings = AkkaElasticsearchProducer.Settings.default):
  Flow[A, Done, NotUsed] = {

    implicit val context: AkkaElasticsearchProducer.Context[A] = AkkaElasticsearchProducer.Context.implicitly[A]

    CommonFlows.Log.debug[A](component => s"Writing ${rc.docType} ${rc.id(component)} to ES")
      .via(writeToElasticsearch(rc.docType, rc.id))
  }


  /**
   * Creates a Flow to write objects to Elasticsearch given functions to extract metadata.  The stream will fail if any
   * objects fail to write.
   */
  def writeToElasticsearch[A](docType: String, idExtractor: A => String)(implicit context: AkkaElasticsearchProducer.Context[A]):
  Flow[A, Done, NotUsed] = {

    lazy val esIndex = context.esIndex

    Flow[A]
      .via(toWriteMessageFlow(idExtractor))
      .via(elasticsearchWriteFlow(docType))
      .map { messageResults =>
        messageResults.foreach { result =>
          if (!result.success) {
            log.error(s"Error writing $docType objects to $esIndex: ${result.error.getOrElse("")}")
            throw new Exception(s"Failed to write $docType object to $esIndex")
          }
        }
        Done
      }
  }

  /**
   * Creates a Flow to write objects to Elasticsearch, passing along whether they succeeded or failed.
   */
  def elasticsearchWriteFlow[A](docType: String)(implicit context: AkkaElasticsearchProducer.Context[A]): WriteFlow[A] =
    context.flow(docType)

  private def toWriteMessageFlow[A](
                                     idExtractor: A => String) = {
    Flow[A].map(singleData => WriteMessage.createIndexMessage(idExtractor(singleData), singleData))
  }
}

object AkkaElasticsearchProducer {

  /**
   * Representation of settings that can be passed to an ElasticsearchFlow.
   *
   * @param bufferSize maximum number of messages to send in each bulk request to Elasticsearch; must be positive
   */
  case class Settings(bufferSize: Int) {
    require(bufferSize > 0, "Buffer size must be positive")

    /**
     * A new Settings with a buffer size of a multiple of this buffer size.  Useful if you want to configure
     * streams in terms of a "quantum" buffer size.
     */
    def multiplyBuffer(factor: Int): Settings =
      copy(bufferSize = bufferSize * factor)

    /**
     * Convert these settings to `ElasticsearchWriteSettings`
     */
    def toES: ElasticsearchWriteSettings =
      ElasticsearchWriteSettings().withBufferSize(bufferSize)
  }

  /**
   * A bundle of things passed around implicitly to create an Elasticsearch Flow.
   *
   * @param client   the RestClient to use to connect to Elasticsearch
   * @param esIndex  the index on which to operate
   * @param settings the settings passed to an ElasticSearchFlow
   */
  class Context[A](val client: RestClient, val esIndex: ElasticsearchIndex, val settings: Settings, val jsonWriter: JsonWriter[A]) {
    /**
     * Create an Elasticsearch Flow for this context to write objects to a given type.
     */
    def flow(docType: String): Flow[WriteMessage[A, NotUsed], Seq[WriteResult[A, NotUsed]], NotUsed] =
      ElasticsearchFlow.create[A](
        indexName = esIndex,
        typeName = docType,
        settings = settings.toES
      )(client, jsonWriter)
  }

  object Settings {
    /**
     * The default; corresponds to the default settings in Alpakka's elasticsearch connector
     */
    val default: Settings = Settings(bufferSize = 10)

    /**
     * Read the Settings from standard properties in a Lightbend Config.
     */
    def fromConfig(config: Config): Settings = {
      val bufferSize = config.getInt("elasticsearch.push.batch-size")

      Settings(bufferSize = bufferSize)
    }
  }

  object Context {
    /**
     * Explicitly construct a Context based on values in the implicit scope.  A Spray JSON format will be generated
     * based on the Play JSON format.
     *
     * @param client   a restClient to use to connect to ElasticSearch
     * @param esIndex  index the index on which to operate
     * @param settings settings settings used by ElasticSearchFlow
     * @param playF    implicit play format
     */
    implicit def implicitly[A](implicit client: RestClient, esIndex: String, settings: Settings, playF: Format[A]): Context[A] =
      apply(client, esIndex, settings, Play2Spray(playF))

    def apply[A](client: RestClient, esIndex: String, settings: Settings, jsonWriter: JsonWriter[A]): Context[A] =
      new Context(client, esIndex, settings, jsonWriter)
  }

}
