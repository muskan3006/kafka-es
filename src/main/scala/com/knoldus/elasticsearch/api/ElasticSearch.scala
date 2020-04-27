package com.knoldus.elasticsearch.api

import com.knoldus.common.utils.ResourceCompanion
import com.knoldus.common._
import com.knoldus.common.services.{AndFilterList, ChildFilter, DeleteResponse, FacetResponse, FilterTrait, IdsFilter, NestedFilter, NotFilter, OrFilterList, ParentFilter, QueryFilter, QueryParams}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.mappings.FieldType.{DateType, StringType}
import com.sksamuel.elastic4s.source.DocumentSource
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.NoNodeAvailableException
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.engine.{DocumentAlreadyExistsException, VersionConflictEngineException}
import org.elasticsearch.search.aggregations.Aggregation
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.transport.RemoteTransportException
//import org.elasticsearch.search.sort.SortOrder
import org.elasticsearch.cluster.block.ClusterBlockException
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import scala.Option.option2Iterable
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Map Play Json to string source for indexing into Elasticsearch
 *
 * This is unfortunate, but avoids having to configure the Jackson JSON mapper to match the Play Json mapper
 * (e.g. for our Date convention, etc).
 */
case class JsonSource(js: JsValue) extends DocumentSource {
  lazy val json: String = js.toString
}

case class StringSource(json: String) extends DocumentSource

object Elasticsearch {
  val defaultBinaryPort = 9300

  val MaxQueryCount = 1000
  val MaxQueryDepth = 10000

  // If the {} replacements in these strings are changed, it's very likely that every reference to these strings will
  // need to be changed as well
  private[Elasticsearch] val countExceededLogStr = s"""Query exceeds maximum count of $MaxQueryCount: count={} queryparams: {}"""
  private[Elasticsearch] val depthExceededLogStr = s"""Query exceeds maximum depth of $MaxQueryDepth: depth={} queryparams: {}"""
}

class Elasticsearch(
                     esHosts: scala.collection.Seq[String],
                     defaultBinaryPort: Int = Elasticsearch.defaultBinaryPort)
                   (override implicit val ec: ExecutionContext) extends ElasticsearchClient {

  override val logger: Logger = LoggerFactory.getLogger(this.getClass)

 // import Sort._

  private val cfg = ResourceCompanion.config

  val hosts: scala.Seq[(String, Int)] = esHosts.flatMap(_.split(",")).map(_.trim.split(':')).map {
    case Array(hostname, port) => hostname -> Try(port.toInt).toOption.getOrElse(defaultBinaryPort)
    case Array(hostname) => hostname -> defaultBinaryPort
  }
  val hostStrings: scala.Seq[String] = hosts.map(x => x._1 + ":" + x._2)
  logger.info("Elasticsearch client configured with hosts " + hostStrings.mkString(", "))

  val esUri: String = "elasticsearch://" + hostStrings.mkString(",")

  val esTestProp = "elasticsearch.test"

  lazy val client: ElasticClient =
    if (cfg.hasPath(esTestProp) && cfg.getBoolean(esTestProp)) {
      logger.info("Using local elasticsearch for test")
//      val settings = ImmutableSettings.settingsBuilder()
//        .put("gateway.type", "none")
//        .put("index.store.type", "memory")
      val local = ElasticClient.local

//      for (config <- List("elasticsearch.index.default", "elasticsearch.index.users")) {
//        if (cfg.hasPath(config)) {
//          local.execute {
//            val indexName = cfg.getString(config)
//            create index indexName
//          }
//        }
//      }

      Thread.sleep(1000) // Let indexes get created...
      local
    } else {
      ElasticClient.remote(ElasticsearchClientUri(esUri))
    }

  override def close(): Unit = client.close()

  override protected def clientException(err: Throwable): Throwable = err match {
    case _: NoNodeAvailableException => ESException("Cannot connect to Elasticsearch or it is down")
    case x: RemoteTransportException =>
      x.getMostSpecificCause match {
        case (_: VersionConflictEngineException | _: DocumentAlreadyExistsException) => ESException(ESException.conflictMessage, 409)
        case _: ClusterBlockException => ESException(ESException.indexWriteLockedMessage,423)
        case e => getDefaultESError(e)
      }
    case e => getDefaultESError(e)
  }

  /**
   * Convert Sort.Order to Elasticsearch SortOrder
   */
//  def sortOrder(order: Order): SortOrder = order match {
//    case `ascending` => SortOrder.ASC
//    case _ => SortOrder.DESC
//  }

  def buildFilter(f: FilterTrait): FilterDefinition = {
    f match {
      case idf: IdsFilter => idsFilter(idf.ids: _*)
      case qf: QueryFilter => queryFilter(new QueryStringQueryDefinition(qf.f))
      case cf: ChildFilter => hasChildFilter(cf.docType).filter(buildFilter(cf.f))
      case pf: ParentFilter => hasParentFilter(pf.docType).filter(buildFilter(pf.f))
      case nf: NestedFilter => nestedFilter(nf.path).filter(buildFilter(nf.f))
      case not: NotFilter => new BoolFilterDefinition().must().not(buildFilter(not.f))
      case filters: AndFilterList =>
        val (must, mustNot) = filters.list.partition(_.must)
        new BoolFilterDefinition().must(must.map(buildFilter(_)): _*).not(mustNot.map(buildFilter(_)): _*)
      case filters: OrFilterList =>
        val (must, mustNot) = filters.list.partition(_.must)
        new BoolFilterDefinition().should(must.map(buildFilter(_)) ++ mustNot.map(x => new BoolFilterDefinition().must().not(buildFilter(x))): _*)
    }
  }

  /**
   * Extract aggregations from the search response.
   * Note that there is no toXContent method exposed publicly except on the whole search response,
   * so we make the entire search response JSON, then extract "aggregations".
   */
  private def getAggregationsAsJson(searchResponse: SearchResponse): Option[JsObject] = searchResponse.getAggregations.asScala.toList match {
    case Nil => None
    case _: List[Aggregation] =>
      // For unstructured aggregations, we don't parse the results, just pass through
      val builder = XContentFactory.jsonBuilder
      builder.startObject
      searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS)
      builder.endObject
      val jsonStr = builder.string
      val json = Json.parse(jsonStr)
      (json \ "aggregations").asOpt[JsObject].flatMap { aggsObj =>
        aggsObj match {
          case aggsJson: JsObject =>
            Some(aggsJson)
          case _ => None
        }
      }
  }

  def buildSearchDefinition(typ: ResourceCompanion[_], params: QueryParams, withMeta: Boolean): SearchDefinition = {

    val allFilters = params.f ::: params.nf ::: params.filtersFromPost.map {
      List(_)
    }.getOrElse(Nil)

    val queryStart = params.start
    val queryCount = params.count
    val queryDepth = queryStart + queryCount

//     For now we'll just log warnings when we get these queries... we need to figure out if the UI or SSMC
//     are doing this or it's just ad hoc queries
//     If we end up needing to support these we'll have to deal with scan/scroll
    if (queryCount > Elasticsearch.MaxQueryCount) {
      logger.warn(Elasticsearch.countExceededLogStr, queryCount: Any, params: Any)
    }
    if (queryDepth > Elasticsearch.MaxQueryDepth) {
      logger.warn(Elasticsearch.depthExceededLogStr, queryDepth: Any, params: Any)
    }


    var builder = search in typ.indexDoc from queryStart limit queryCount

    if (params.sourceInclude.nonEmpty) {
      builder = builder.sourceInclude(params.sourceInclude: _*)
    }
    if (params.sourceExclude.nonEmpty) {
      builder = builder.sourceExclude(params.sourceExclude: _*)
    }
    params.routing match {
      case Some(routing) => builder = builder.routing(routing)
      case None => ()
    }

    (params.q.nonEmpty, allFilters.nonEmpty) match {
      case (true, false) => // Query, no filter
        builder = builder query params.q
      case (true, true) => // Query and Filter
        builder = builder query {
          filteredQuery.query(new QueryStringQueryDefinition(params.q)).filter(buildFilter(AndFilterList(allFilters)))
        }
      case (false, true) => // Filter, no query
        builder = builder query {
          filteredQuery.query(matchAllQuery).filter(buildFilter(AndFilterList(allFilters)))
        }
      case _ =>
      // Nothing to filter or query on - effectively match all
    }
    if (params.aggsJson.nonEmpty) {
      // Force the "aggregations" value to be the given JSON object...
      builder._builder.setAggregations(params.aggsJson.get.toString.getBytes("UTF-8"))
    } else if (params.facetBy.nonEmpty) {
      params.facetBy.foreach { facetBy =>
        facetBy.split(":", 2).toList match {
          case nPath :: nField :: _ =>
            builder = builder aggs {
              aggregation nested facetBy path nPath aggs {
                aggregation terms facetBy field s"$nPath.$nField" size 200
              }
            }
          case _ =>
            builder = builder aggs {
              aggregation terms facetBy field facetBy size 200
            }
        }
      }
    }
//    if (params.sortBy.nonEmpty) {
//      // E.g. builder sort ( field sort "date" order SortOrder.DESC, by field "author" order SortOrder.ASC )
//      builder = builder sort (params.sortBy.map(p => field sort p._1 order sortOrder(p._2)): _*)
//    }
    if (withMeta) {
      builder.version(true)
    }
    builder
  }

//  def constructJsonReturn(obj: JsObject, hit: SearchHit, withMeta: Boolean): JsObject = {
//    val scoreOpt = Option(hit.getScore).filterNot(_.isNaN)
//    val objWithScore = scoreOpt match {
//      case Some(score) => obj + ("_score" -> JsNumber(score))
//      case None => obj
//    }
//    if (withMeta) {
//      val version: Option[Long] = Option(hit.getVersion)
//
//      version match {
//        case Some(ver) => objWithScore + ("_version" -> JsNumber(ver))
//        case None => objWithScore
//      }
//    } else {
//      objWithScore
//    }
//  }

  def getFacetResponses(response: SearchResponse): Option[List[FacetResponse]] = {
    def toTermsAgg(agg: Aggregation): Option[Terms] = {
      val termsResult = Try(agg.asInstanceOf[Terms]).toOption
      val nestedTermsResult = for {
        nested <- Try(agg.asInstanceOf[Nested]).toOption
        nestedAggs <- Option(nested.getAggregations)
        nestedAgg <- nestedAggs.asScala.headOption // there will only ever be one nested within this nested agg call
        nestedTerms <- Try(nestedAgg.asInstanceOf[Terms]).toOption
      } yield nestedTerms
      termsResult orElse nestedTermsResult
    }

    Option(response.getAggregations).map { esAggs =>
      val aggs = esAggs.asScala.toList
      for {
        agg: Aggregation <- aggs
        termsAgg <- toTermsAgg(agg)
      } yield {
        val termsBucketList: List[Terms.Bucket] = termsAgg.getBuckets.asScala.toList
        val termsCount: List[(String, Long)] = for {
          bucket: Terms.Bucket <- termsBucketList
        } yield {
          bucket.getKeyAsText.string -> bucket.getDocCount
        }
        FacetResponse(fieldName = termsAgg.getName, values = termsCount.toMap)
      }
    } match {
      case Some(resps) if resps.nonEmpty => Some(resps)
      case _ => None
    }
  }

  /**
   * Asynchronous Query to elasticsearch, returning a sequence of JsObjects
   */
//  override def queryJson(typ: ResourceCompanion[_], params: QueryParams, withMeta: Boolean = false): Future[QueryResponse[JsObject]] = {
//    try {
//      val searchDefinition = buildSearchDefinition(typ, params, withMeta)
//      client.execute {
//        searchDefinition
//      } map { response: SearchResponse =>
//        val values: Seq[JsObject] = response.getHits.hits.to[Seq] map { hit =>
//          val jsonStr = hit.getSourceAsString
//          Json.parse(jsonStr) match {
//            case obj: JsObject =>
//              constructJsonReturn(obj, hit, withMeta)
//            case v => throw ESException("query response did not return a JsObject, returned " + v.getClass.toString)
//          }
//        }
//        val aggsResponse: Option[JsObject] = if (params.aggsJson.nonEmpty) getAggregationsAsJson(response) else None
//        val facetsResponse: Option[List[FacetResponse]] = if (params.facetBy.nonEmpty) getFacetResponses(response) else None
//        logger.info(s"Query Response: index=${typ.index}, docType=${typ.docType}, " +
//          s"values=${values.size}, tookMillis=${response.getTookInMillis} for query $searchDefinition")
//        QueryResponse(values = values, total = response.getHits.getTotalHits, offset = params.start,
//          tookMillis = response.getTookInMillis, facets = facetsResponse, aggs = aggsResponse)
//      } recover {
//        case err: Throwable => throw clientException(err)
//      }
//    } catch {
//      case err: Throwable => Future.failed(clientException(err))
//    }
//  }

//  /**
//   * Asynchronous Get to elasticsearch, returning a JsObject
//   */
//  def getJson(typ: ResourceCompanion[_], idVal: String): Future[GetResponse[JsObject]] = {
//    try {
//      client.execute {
//        buildGetDefinition(typ, idVal)
//      } map { response: org.elasticsearch.action.get.GetResponse =>
//        if (!response.isExists) {
//          GetResponse[JsObject](value = None)
//        } else {
//          val jsonStr = response.getSourceAsString
//          Json.parse(jsonStr) match {
//            case obj: JsObject => GetResponse[JsObject](value = Some(obj), version = Some(response.getVersion))
//            case v => throw ESException("get response did not return a JsObject, returned " + v.getClass.toString)
//          }
//        }
//      } recover {
//        case err: Exception => throw clientException(err)
//      }
//    } catch {
//      case err: Throwable => Future.failed(clientException(err))
//    }
//  }

  /**
   * Asynchronous Insert/Update to elasticsearch
   */
//  override def upsert[A](typ: ResourceCompanion[A], obj: A)(implicit tjs: Writes[A]): Future[UpsertResponse[A]] = {
//    doUpsert(typ, obj)
//  }

  /**
   * Asynchronous Insert/Update to elasticsearch conditional upon a correct version
   * For edit if no version provided, then version check is not performed
   * For edit, if version is provided, then doc is updated only if provided version matches current doc version in ES
   * For create, version provided should be 0
   */
//  override def upsertConditional[A](typ: ResourceCompanion[A], obj: A, version: Option[Long])(implicit tjs: Writes[A]): Future[UpsertResponse[A]] = {
//    doUpsert(typ, obj, conditional = true, version)
//  }
//
//  private def doUpsert[A](typ: ResourceCompanion[A], obj: A, conditional: Boolean = false,
//                          version: Option[Long] = None)(implicit tjs: Writes[A]): Future[UpsertResponse[A]] = {
//    val indexDoc = typ.indexDoc
//    val jsonSource = JsonSource(Json.toJson(obj))
//    try {
//      val id = typ.id(obj)
//      var indexCmd = index into indexDoc doc jsonSource id id
//      val parentId = typ.parentId(id)
//      if (parentId.isDefined) {
//        indexCmd = indexCmd.parent(parentId.get)
//      }
//      if (conditional) {
//        indexCmd = version match {
//          case Some(v) if v == 0 => indexCmd.opType(org.elasticsearch.action.index.IndexRequest.OpType.CREATE)
//          case Some(v) => indexCmd.version(v)
//          case None => indexCmd
//        }
//      }
//
//      client.execute {
//        indexCmd
//      } map { indexResponse: org.elasticsearch.action.index.IndexResponse =>
//        val response = UpsertResponse(value = obj, isCreated = indexResponse.isCreated, version = indexResponse.getVersion)
//        if (response.isCreated) {
//          logger.debug("Inserted " + typ.resourceType + "/" + id + " to " + indexDoc + ", version is " + indexResponse.getVersion)
//        } else {
//          logger.debug("Updated " + typ.resourceType + "/" + id + " in " + indexDoc + ", version is now " + indexResponse.getVersion)
//        }
//        response
//      } recover {
//        case err: Exception => throw clientException(err)
//      }
//    } catch {
//      case err: Throwable => Future.failed(clientException(err))
//    }
//  }

  /**
   * Asynchronous Bulk upsert to elasticsearch
   */

//  override def bulkUpsert[A](typ: ResourceCompanion[A], objs: Vector[A],
//                             indexOverride: Option[String] = None)(implicit tjs: Writes[A]): Future[BulkUpsertResponse] = {
//
//    val bulkCmd = objs.map { obj =>
//      val indexName = indexOverride.getOrElse(typ.indexDoc._1)
//      val indexDoc = (indexName, typ.indexDoc._2)
//      val jsonSource = JsonSource(Json.toJson(obj))
//      val id = typ.id(obj)
//      var indexCmd = index into indexDoc doc jsonSource id id
//      val parentId = typ.parentId(id)
//      if (parentId.isDefined) {
//        indexCmd = indexCmd.parent(parentId.get)
//      }
//      indexCmd
//    }
//
//    try {
//      client.execute {
//        bulk(bulkCmd: _*)
//      } map { response =>
//        val (failedDevices, successfulDevices) = response.getItems.partition { x => x.isFailed }
//        val succesfulDeviceCount = successfulDevices.length
//        val successElems = successfulDevices.map { x => BulkUpsertResponseElement(id = x.getId) }
//        val failedDeviceCount = failedDevices.length
//        val failedElems = failedDevices.map { x => BulkUpsertResponseElement(id = x.getId, failure = true, errorMessage = Some(x.getFailureMessage)) }
//        BulkUpsertResponse(succesfulDeviceCount, failedDeviceCount, successElems.toList ++ failedElems.toList)
//      } recover {
//        case err: Exception => throw clientException(err)
//      }
//    } catch {
//      case err: Throwable => Future.failed(clientException(err))
//    }
//
//  }

//  def chunkedUpsert[A](typ: ResourceCompanion[A], objs: Iterator[A], chunkSize: Int = 50000,
//                       indexOverride: Option[String] = None)(implicit tjs: Writes[A]): Future[Vector[BulkUpsertResponse]] = {
//    Future.sequence(objs.grouped(chunkSize).map { chunk =>
//      // You would only be using this if you cared about memory, so we throw this garbage collection in between each chunk jic
//      Runtime.getRuntime.gc()
//      logger.info(s"Upserting chunk of $chunkSize ${typ.docType}")
//      bulkUpsert[A](typ, chunk.toVector, indexOverride)
//    }).map(_.toVector)
//  }

  /**
   * Asynchronous Delete to elasticsearch
   */
  override def delete(typ: ResourceCompanion[_], idStr: String): Future[DeleteResponse] = {
    try {
      client.execute {
        com.sksamuel.elastic4s.ElasticDsl.delete id idStr from typ.indexDoc
      } map {
        response: org.elasticsearch.action.delete.DeleteResponse =>
          if (response.isFound) {
            logger.debug("Deleted " + typ.resourceType + "/" + idStr + " from " + typ.indexDoc + ", version was " + response.getVersion)
          }
          DeleteResponse(id = idStr, isFound = response.isFound, version = response.getVersion)
      } recover {
        case err: Exception => throw clientException(err)
      }
    } catch {
      case err: Throwable => Future.failed(clientException(err))
    }
  }

//  def buildGetDefinition(typ: ResourceCompanion[_], idVal: String): GetDefinition = {
//    val builder = com.sksamuel.elastic4s.ElasticDsl.get id idVal from typ.indexDoc
//    val parentOpt = typ.parentId(idVal)
//    if (parentOpt.isDefined) {
//      // More clearly this should be builder.parent(parentOpt.get) but the parent() method isn't exposed and just sets routing() anyway...
//      builder.routing(parentOpt.get)
//    } else {
//      builder
//    }
//  }

  /**
   * Asynchronous Multi Get to elasticsearch, returning a JsObject
   */
//  override def multiGetJson(typ: ResourceCompanion[_], idVals: Vector[String]): Future[MultiGetResponse[JsObject]] = {
//    try {
//      client.execute {
//        multiget(idVals.map(buildGetDefinition(typ, _)): _*)
//      } map { multiResponse: org.elasticsearch.action.get.MultiGetResponse =>
//
//        val jsObjects = for {
//          response <- multiResponse.getResponses
//          jsonStr <- Option(response.getResponse.getSourceAsString)
//        } yield {
//          Json.parse(jsonStr) match {
//            case obj: JsObject => obj
//            case v => throw ESException("get response did not return a JsObject, returned " + v.getClass.toString)
//          }
//        }
//        MultiGetResponse(jsObjects.toVector)
//      } recover {
//        case err: Exception => throw clientException(err)
//      }
//    } catch {
//      case err: Throwable => Future.failed(clientException(err))
//    }
//  }

  /**
   * Get Cluster Health
   */
  def clusterHealth: Future[ClusterHealthResponse] = client.execute {
    ElasticDsl.clusterHealth
  }

  override def clusterHealthColor: Future[String] = client.execute(ElasticDsl.clusterHealth).map(_.getStatus.name.toLowerCase)

  /**
   * Gets a map of indices to the list of aliases that they have
   */
  override def getAliasesByIndex: Future[Map[String, List[String]]] = {
    import scala.collection.JavaConversions._
    Future {
      val indexToAliasMap = client.client.admin.cluster().prepareState().execute().actionGet().getState.getMetaData.aliases()
      (for {
        indexToAlias <- indexToAliasMap
        indexName = indexToAlias.key
        aliases = indexToAlias.value.map(_.key).toList
      } yield {
        (indexName, aliases)
      }).toMap
    }
  }

  /**
   * Get any indices associated with the given alias.
   *
   * @return a Future with a sequence of index names that are associated with the given alias.
   */
  override def getIndicesForAlias(aliasName: String): Future[Seq[String]] = {
    client.execute({
      ElasticDsl.get alias aliasName
    }).map(_.getAliases.keys.toArray.map(_.asInstanceOf[String]).toVector)
  }

  /**
   * Get all indices
   */
  override def getAllIndices(): Future[Seq[String]] = {
    // Drop down into raw Elastic Java API, wrapping in a Future (bit hacky)
    Future {
      client.client.admin().cluster().prepareState().execute().actionGet().getState.getMetaData.concreteAllIndices().toVector
    }
  }

  /**
   * Get all indices matching the given string, e.g. "oculus_*"
   */
  override def getMatchingIndices(matching: String): Future[Seq[String]] = {
    import org.elasticsearch.action.support.IndicesOptions
    // Drop down into raw Elastic Java API, wrapping in a Future (bit hacky)
    Future {
      client.client.admin().cluster().prepareState().execute().actionGet().getState.getMetaData
        .concreteIndices(IndicesOptions.lenientExpandOpen(), matching).toVector
    }
  }

  /**
   * Update an alias, removing it from a list of old indices and adding it to a list of new indices.
   */
  override def updateAlias(aliasName: String, addToIndices: Seq[String], removeFromIndices: Seq[String]): Future[Boolean] = {
    val removeAliases = removeFromIndices.map(remove alias aliasName on _)
    val addAliases = addToIndices.map(add alias aliasName on _)
    client.execute(aliases(removeAliases ++ addAliases: _*)).map(response => true)
  }

  /**
   * Returns Future with boolean value - does the index exist?
   */
  override def indexExists(idx: String): Future[Boolean] =
    client.execute {
      index exists idx
    }.map {
      _.isExists
    }

  /**
   * Delete the given index
   */
  override def deleteIndex(index: String): Future[Boolean] = {
    client.execute(ElasticDsl.deleteIndex(index)).map(response => response.isAcknowledged)
  }

  /**
   * Returns Future with true if the index was created, false if it already existed
   */
  def createIndex(index: String): Future[Boolean] = {
    indexExists(index) flatMap { exists =>
      if (exists) {
        Future.successful(false)
      } else {
        client.execute
           {
            create index index mappings("employee-details" as(
              "empId" typed StringType,
              "name" typed StringType,
              "doj" typed DateType,
              "email" typed StringType
            ))
          }.map { response => response.isAcknowledged }
      }
    }
  }
//def createDoc(index:String,doctype:String):Future[Boolean] ={
//
//    create index index
//  )
//}
/**
   * Put resource companion mapping if it exists.
   * This will fail if the index does not exist - use prepareIndex instead
   * to automatically create the index if needed before putting the mappings.
   */
//  override def putMapping(typ: ResourceCompanion[_], index: String = ""): Future[Boolean] = {
//    // Largely inlined from lines 238-240, 338-345 of
//    //  https://github.com/sksamuel/elastic4s/blob/41ae427b4fb3241293608668fe697803db79a92c/src/main/scala/com/sksamuel/elastic4s/Client.scala
//    import org.elasticsearch.action.ActionListener
//    import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
//    import scala.concurrent.Promise
//
//    val idx = if (index.nonEmpty) {
//      index
//    } else {
//      typ.index
//    }
//
//    val p = Promise[PutMappingResponse]()
//    val mappings = ResourceCompanionHelper.mappings(typ).get
//    client.admin.indices.preparePutMapping(idx).setType(mappings.`type`).setSource(mappings.build)
//      .execute(new ActionListener[PutMappingResponse] {
//        def onFailure(e: Throwable): Unit = p.failure(e)
//
//        def onResponse(r: PutMappingResponse): Unit = p.success(r)
//      })
//
//    p.future map { _ => true }
//  }

  /**
   * Prepare the index for the given resource companion:
   * Create the index if it doesn't exist
   * Put the resource companion's mappings if any
   */
//  def prepareIndex(typ: ResourceCompanion[_]): Future[Boolean] = {
//    createIndex(typ.index) flatMap { _ =>
//      if (ResourceCompanionHelper.mappings(typ).nonEmpty) {
//        putMapping(typ)
//      } else {
//        Future.successful(false)
//      }
//    }
//  }

  /**
   * Create a new index and put mappings
   */
//  override def createIndexWithMappings(index: String, typs: GenIterable[ResourceCompanion[_]]): Future[Boolean] = {
//    val createIndexFuture = createIndex(index).recover {
//      case NonFatal(err) =>
//        logger.info(s"Failed to create Elasticsearch index $index - $err")
//        throw err
//    }
//
//    typs.foldLeft(createIndexFuture) { (future: Future[Boolean], typ) =>
//      future.flatMap { success =>
//        val futureMapping: Future[Boolean] = putMapping(typ, index).map { created =>
//          logger.info(s"Successfully put mappings for $index/${typ.docType}")
//          success && created
//        } recover {
//          case NonFatal(err) =>
//            logger.error(s"Failed to put mappings for $index/${typ.docType}", err)
//            throw err
//        }
//        futureMapping
//      }
//    }
//  }

}
