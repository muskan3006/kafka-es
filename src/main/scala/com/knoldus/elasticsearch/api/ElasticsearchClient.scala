package com.knoldus.elasticsearch.api

import java.io.{PrintWriter, StringWriter}

import com.knoldus.common._
import com.knoldus.common.services.DeleteResponse
import com.knoldus.common.utils.ResourceCompanion
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

object ElasticsearchClient {
//  def createDateAndTimeIndexAndSuffix(prefix: String, separator: String = "_", date: Instant = Instant.now()): (String, String) = {
//    val dateSuffix = dateAndTimeIndexSuffix(date)
//    val backupDistinct = s"backup/$dateSuffix"
//    (s"${prefix}${separator}$dateSuffix", dateSuffix)
//  }
//
//  def createDateTimeFormatter(pattern: String, tz: ZoneId = tzIdUTC, locale: Locale = Locale.US): DateTimeFormatter =
//    new DateTimeFormatterBuilder()
//      .parseCaseInsensitive()
//      .appendPattern(pattern)
//      .toFormatter
//      .withZone(tz)
//      .withLocale(locale)
//      .withResolverStyle(ResolverStyle.LENIENT)
  // ES Index Names require lower case only and no ':', so we can't directly use ISO8601

//  private lazy val esIndexDateAndTimeFormat = createDateTimeFormatter("yyyy-MM-dd't'HH-mm-ss")
//  def dateAndTimeIndexSuffix(date: Instant = Instant.now()): String = esIndexDateAndTimeFormat.format(date)

  import scala.language.implicitConversions

  implicit class ElasticsearchIndex(name: String) {
    override def toString: String = name
  }
  implicit def elasticsearchIndexToString(esi: ElasticsearchIndex): String = esi.toString
}

trait ElasticsearchClient extends AutoCloseable {

  def logger: Logger

  implicit val ec: ExecutionContext

  /**
   * Asynchronous Query to elasticsearch, returning a sequence of JsObjects
   */
//  def queryJson(typ: ResourceCompanion[_], params: QueryParams, withMeta: Boolean = false): Future[QueryResponse[JsObject]]
//
//  /**
//   * Asynchronous Query to elasticsearch, return a sequence of type T
//   */
//  def query[A](typ: ResourceCompanion[A], params: QueryParams)(implicit tjs: Reads[A]): Future[QueryResponse[A]] = {
//    queryJson(typ, params) map { response =>
//      val values: Seq[A] = response.values map { json: JsObject =>
//        json.validate[A].fold(
//          invalid = errors => throw new RuntimeException("Invalid JSON: " + errors.toString),
//          valid = obj => obj
//        )
//      }
//      QueryResponse[A](values = values, total = response.total, offset = response.offset,
//        tookMillis = response.tookMillis, facets = response.facets, aggs = response.aggs)
//    } recover {
//      case err: ESException => throw err
//      case err: Exception => throw clientException(err)
//    }
//  }
//
//  /**
//   * Asynchronous Get to elasticsearch, returning a JsObject
//   */
//  def getJson(typ: ResourceCompanion[_], idVal: String): Future[GetResponse[JsObject]]
//
//  /**
//   * Asynchronous Get to elasticsearch, return an object of type T
//   */
//  def get[A](typ: ResourceCompanion[A], idVal: String)(implicit tjs: Reads[A]): Future[GetResponse[A]] = {
//    getJson(typ, idVal) map { response =>
//      response.value.fold(ifEmpty = GetResponse[A](value = None)) { json =>
//        json.validate[A].fold(
//          invalid = errors => throw new RuntimeException("Invalid JSON: " + errors.toString),
//          valid = obj => GetResponse[A](Some(obj), version = response.version)
//        )
//      }
//    } recover {
//      case err: Exception => throw clientException(err)
//    }
//  }
//
//  /**
//   * Asynchronous Multi Get to elasticsearch, returning a JsObject
//   */
//  def multiGetJson(typ: ResourceCompanion[_], idVals: Vector[String]): Future[MultiGetResponse[JsObject]]
//
//  /**
//   * Asynchronous Multi Get to elasticsearch, return objects of type T
//   */
////  def multiGet[A](typ: ResourceCompanion[A], idVals: Vector[String])(implicit tjs: Reads[A]): Future[MultiGetResponse[A]] = {
////    multiGetJson(typ, idVals) map { response =>
////      val updatesValues = response.values.map { json =>
////        json.validate[A].fold(
////          invalid = errors => throw new RuntimeException("Invalid JSON: " + errors.toString),
////          valid = obj => obj
////        )
////      }
////      MultiGetResponse(updatesValues)
////    } recover {
////      case err: Exception => throw clientException(err)
////    }
////  }
//
//  /**
//   * Asynchronous Insert/Update to elasticsearch
//   */
//  def upsert[A](typ: ResourceCompanion[A], obj: A)(implicit tjs: Writes[A]): Future[UpsertResponse[A]]
//
//  /**
//   * Asynchronous Insert/Update to elasticsearch conditional upon a correct version
//   * For edit if no version provided, then version check is not performed
//   * For edit, if version is provided, then doc is updated only if provided version matches current doc version in ES
//   * For create, version provided should be 0
//   */
//  def upsertConditional[A](typ: ResourceCompanion[A], obj: A, version: Option[Long])(implicit tjs: Writes[A]): Future[UpsertResponse[A]]

  /**
   * Asynchronous Bulk upsert to elasticsearch
   */
//  def bulkUpsert[A](typ: ResourceCompanion[A], objs: Vector[A],
//                    indexOverride: Option[String] = None)(implicit tjs: Writes[A]): Future[BulkUpsertResponse]

  /**
   * Asynchronous Delete to elasticsearch
   */
  def delete(typ: ResourceCompanion[_], idStr: String): Future[DeleteResponse]

  /**
   * Health of the elasticsearch cluster: green, yellow, or red
   */
  def clusterHealthColor: Future[String]

  /**
   * Is the cluster healthy?  I.e. is the cluster not *Red*?
   */
  def isClusterHealthy: Future[Boolean] = clusterHealthColor.map(_ != "red")

  /**
   * Gets a map of indices to the list of aliases that they have
   */
  def getAliasesByIndex: Future[Map[String, List[String]]]

  /**
   * Gets a map of indices to the list of aliases that they have
   */
  def getIndicesByAlias: Future[Map[String, List[String]]] = getAliasesByIndex.map(CommonConversion.invertStringMap)

  /**
   * Get any indices associated with the given alias.
   *
   * @return a Future with a sequence of index names that are associated with the given alias.
   */
  def getIndicesForAlias(aliasName: String): Future[Seq[String]]

  /**
   * Get all indices
   */
  def getAllIndices(): Future[Seq[String]]

  /**
   * Get all indices matching the given string, e.g. "oculus_*"
   */
  def getMatchingIndices(matching: String): Future[Seq[String]]

  /**
   * Update an alias, removing it from a list of old indices and adding it to a list of new indices.
   */
  def updateAlias(aliasName: String, addToIndices: Seq[String], removeFromIndices: Seq[String]): Future[Boolean]

  /**
   * Returns Future with boolean value - does the index exist?
   */
  def indexExists(idx: String): Future[Boolean]

  /**
   * Delete the given index
   */
  def deleteIndex(index: String): Future[Boolean]

  /**
   * Returns Future with true if the index was created, false if it already existed
   */
  def createIndex(index: String): Future[Boolean]

  /**
   * Put resource companion mapping if it exists.
   * This will fail if the index does not exist - use prepareIndex instead
   * to automatically create the index if needed before putting the mappings.
   */
 // def putMapping(typ: ResourceCompanion[_], index: String = ""): Future[Boolean]

  /**
   * Prepare the index for the given resource companion:
   * Create the index if it doesn't exist
   * Put the resource companion's mappings if any
   */
  //def prepareIndex(typ: ResourceCompanion[_]): Future[Boolean]

  /**
   * Create a new index and put mappings
   */
 // def createIndexWithMappings(index: String, typs: GenIterable[ResourceCompanion[_]]): Future[Boolean]

  protected def clientException(err: Throwable): Throwable = err match {
    case e => getDefaultESError(e)
  }

  protected def getDefaultESError(err: Throwable): ESException = {
    val sw = new StringWriter()
    err.printStackTrace(new PrintWriter(sw))
    logger.error("Failed to execute against Elasticsearch - " + err.getClass + ": " + err.getMessage + "\n" + sw.toString)
    ESException("Unknown error connecting to Elasticsearch - " + err.getClass + ": " + err.getMessage + "\n" + sw.toString)
  }

}
