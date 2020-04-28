package com.knoldus.common.services

import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.collection.breakOut
import scala.collection.immutable.Map
import scala.concurrent.Future

trait ReadService[A] {

  def get(id: String): Future[GetResponse[A]]

  def multiGet(ids: Vector[String]): Future[MultiGetResponse[A]]

  /**
   * Currently, this is how ReadService getAll is implemented, but it should not be called directly
   */
  def getAll(params: QueryParams): Future[QueryResponse[A]]

  def getAllWithMeta(params: QueryParams): Future[QueryResponse[A]] = throw new java.lang.UnsupportedOperationException()
}

trait WriteService[A] {
  def upsert(obj: A): Future[UpsertResponse[A]]
  def upsertConditional(obj: A, version: Option[Long]): Future[UpsertResponse[A]]

  def bulkUpsert(objs: Vector[A]): Future[BulkUpsertResponse]
}

trait JsonWriteService[A] extends WriteService[A] {
  def upsert(obj: A, tjs: play.api.libs.json.Writes[A]): Future[UpsertResponse[A]]
  def upsertConditional(obj: A, version: Option[Long], tjs: play.api.libs.json.Writes[A]): Future[UpsertResponse[A]]
}

trait DeleteService {
  def delete(id: String): Future[DeleteResponse]
}

case class FacetResponse(
                          fieldName: String,
                          values: Map[String, Long] = Map[String, Long]()
                        ) {

  def length: Int = values.size

  /**
   * Return the value counts for the sum of two FacetResponse objects.
   * Note that the fieldName should be the same between the two, but if not it is taken as the first object's fieldName
   */
  def +(that: FacetResponse): FacetResponse = {
    val allValues = this.values.toSeq ++ that.values.toSeq
    val groupedValues = allValues.groupBy(_._1).map(x => (x._1, x._2 map (_._2)))
    val sumValues = groupedValues.map { tup =>
      val value = tup._1
      val counts = tup._2
      val valueCount = counts.foldLeft(0L)((sum, count) => sum + count)
      (value, valueCount)
    }
    this.copy(values = sumValues)
  }
}

object FacetResponse {
  val jsonReads = new Reads[FacetResponse] {
    override def reads(js: JsValue): JsResult[FacetResponse] = {
      val objOpt = for {
        fieldName <- (js \ "fieldName").asOpt[String]
        valList <- (js \ "values").asOpt[List[JsValue]].orElse(Some(List()))
      } yield {
        val valueSeq = for {
          value <- valList
          term <- (value \ "term").asOpt[String]
          count <- (value \ "count").asOpt[Long]
        } yield {
          (term, count)
        }
        FacetResponse(fieldName = fieldName, values = valueSeq.toMap)
      }
      if (objOpt.isDefined) { JsSuccess(objOpt.get) } else { JsError() }
    }
  }

  val jsonWrites = new Writes[FacetResponse] {
    override def writes(f: FacetResponse): JsValue = {
      Json.obj(
        "fieldName" -> f.fieldName,
        "count" -> f.length,
        "values" -> f.values.toList.sortBy(_._2).map(p => Json.obj("term" -> p._1, "count" -> p._2)).reverse
      )
    }
  }
  implicit val jsonFormat: Format[FacetResponse] = Format(jsonReads, jsonWrites)
}

sealed trait FilterTrait extends Any {
  def must: Boolean = true
}

case class QueryFilter(f: String, override val must: Boolean = true) extends FilterTrait

// Filter by a list of ID's - doesn't require knowing or having the document ID property indexed (e.g. '_id')
case class IdsFilter(ids: Seq[String], override val must: Boolean = true) extends FilterTrait

case class ChildFilter(
                        docType: String,
                        f: FilterTrait,
                        override val must: Boolean = true
                      ) extends FilterTrait

case class ParentFilter(
                         docType: String,
                         f: FilterTrait,
                         override val must: Boolean = true
                       ) extends FilterTrait

case class NestedFilter(
                         path: String,
                         f: FilterTrait,
                         override val must: Boolean = true
                       ) extends FilterTrait

case class AndFilterList(list: List[FilterTrait]) extends AnyVal with FilterTrait

case class OrFilterList(list: List[FilterTrait]) extends AnyVal with FilterTrait

case class NotFilter(f: FilterTrait) extends AnyVal with FilterTrait

case class QueryParams(
                        q: String = "",
                        f: List[FilterTrait] = Nil,
                        filtersFromPost: Option[FilterTrait] = None,
                        count: Int,
                        start: Int = 0,
                        sortBy: Sort.Spec = Sort.NONE,
                        facetBy: List[String] = Nil,
                        nf: List[NestedFilter] = Nil,
                        aggsJson: Option[JsObject] = None,
                        sourceInclude: List[String] = Nil,
                        sourceExclude: List[String] = Nil,
                        routing: Option[String] = None
                      )

case class QueryResponse[T](
                             values: Seq[T] = Seq[T](),
                             facets: Option[List[FacetResponse]] = None,
                             total: Long = 0,
                             offset: Int = 0,
                             tookMillis: Long = -1,
                             aggs: Option[JsObject] = None
                           ) {

  def length: Int = values.size

  /**
   * Append a query response adds more values and updates the total.
   * Note that queries should not be appended when faceting (unclear what the desired behavior would be)
   */
  def ++(that: QueryResponse[T]): QueryResponse[T] = {
    val allValues = this.values ++ that.values
    val facetsList: List[FacetResponse] = this.facets.getOrElse(Nil) ::: that.facets.getOrElse(Nil)
    val groupedFacets = facetsList.groupBy(_.fieldName).toSeq.sortBy(_._1) // Sort for determinism
    val allFacets: List[FacetResponse] = groupedFacets.map { tup: (String, List[FacetResponse]) =>
      val fieldName: String = tup._1
      val facetList: Seq[FacetResponse] = tup._2
      facetList.foldLeft(FacetResponse(fieldName))(_ + _)
    }(breakOut)
    val allFacetsOpt = if (allFacets.nonEmpty) { Some(allFacets) } else { None }
    val allAggsOpt = if (this.aggs.isEmpty && that.aggs.isEmpty) { None } else { Some(this.aggs.getOrElse(Json.obj()) ++ that.aggs.getOrElse(Json.obj())) }
    // tookMillis is < 0 (e.g. -1) if it's not set, so to add two tookMillis values we don't want to sum negative values
    val tookMillis = if (this.tookMillis < 0) that.tookMillis else if (that.tookMillis < 0) this.tookMillis else this.tookMillis + that.tookMillis
    this.copy(total = this.total + that.total, values = allValues, tookMillis = tookMillis, facets = allFacetsOpt, aggs = allAggsOpt)
  }
}

case class GetResponse[T](value: Option[T], version: Option[Long] = None)

case class UpsertResponse[T](value: T, isCreated: Boolean, version: Long = -1)

case class BulkUpsertResponse(successfulDevices: Int, failedDevices: Int, members: List[BulkUpsertResponseElement])

case class BulkUpsertResponseElement(id: String, failure: Boolean = false, errorMessage: Option[String] = None)

case class DeleteResponse(id: String, isFound: Boolean, version: Long = -1)

case class MultiGetResponse[T](values: Vector[T])

/**
 * Enumeration of sort orders "ascending" and "descending"
 */
object Sort extends Enumeration {
  type Order = Value
  val ascending, descending = Value

  /**
   * Short typename for a sequence of field -> order pairs
   */
  type Spec = List[(String, Order)]

  /**
   * Value of empty sort order - or "no sort order"
   */
  val NONE: Spec = List()
}
