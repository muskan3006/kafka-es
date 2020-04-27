//package com.knoldus.common
//
///*
// * Model Metadata is used for the following:
// *   - Describe a model, including information for how API users can query/filter the model
// *   - Automatic generation of Elasticsearch mappings
// *   - Automatic generation of CSV columns, given requested field paths
// *   - Calculation of scores, based on rules
// */
//
//
//import java.time.Instant
//
//import javax.management.relation.Role
//import play.api.libs.json._
//
//sealed trait FieldMeta[A] {
//  def name: String
//  def isOptional: Boolean
//  def isArray: Boolean
//  def roles: Roles
//  def isPersonallyIdentifiable: Boolean
//  def requiresContract: Boolean
//  def requiresApprovalForPartnerAccess: Boolean
//  def isUsefulTrend: Boolean
//
//  /**
//   * Get field by '.' separated field path name
//   */
//  def getField(path: String): Option[FieldMeta[_]] = getField(path.split('.'))
//
//  /**
//   * Get field by sequence of field name paths.
//   */
//  def getField(path: Seq[String]): Option[FieldMeta[_]]
//
////  def isAuthorized(auth: AuthorizedRoles): Boolean = {
////    val fieldRoles = roles.map(_.toString)
////    fieldRoles.isEmpty || fieldRoles.intersect(auth.roles).nonEmpty
////  }
//
//  def setRoles(roles: List[Role]): FieldMeta[_] = this match {
//    case x: ModelFieldMeta[A] => x.copy(roles = roles)
//    case x: NestedFieldMeta[A] => x.copy(roles = roles)
//    case x: StringFieldMeta => x.copy(roles = roles)
//    case x: BooleanFieldMeta => x.copy(roles = roles)
//    case x: IntFieldMeta => x.copy(roles = roles)
//    case x: LongFieldMeta => x.copy(roles = roles)
//    case x: FloatFieldMeta => x.copy(roles = roles)
//    case x: DoubleFieldMeta => x.copy(roles = roles)
//    case x: InstantFieldMeta => x.copy(roles = roles)
//    case x: DateFieldMeta => x.copy(roles = roles)
//    case x: BytesFieldMeta => x.copy(roles = roles)
//  }
//}
//
//sealed trait ScalarMeta { this: FieldMeta[_] =>
//  def getField(path: Seq[String]): Option[FieldMeta[_]] = {
//    // This is the default implementation for primitive types.
//    // Primitive types have no sub-fields, so if any are requested, return None, otherwise just return this.
//    if (path.isEmpty) Some(this) else None
//  }
//}
//sealed trait ObjectMeta { this: FieldMeta[_] =>
//  def meta: ModelMeta
//  override def getField(path: Seq[String]): Option[FieldMeta[_]] = path match {
//    case field +: nextFields => meta.getField(path)
//    case _ => Some(this)
//  }
//  def setMeta(meta: ModelMeta): ObjectMeta with FieldMeta[_] = this match {
//    case x: ModelFieldMeta[_] => x.copy(meta = meta)
//    case x: NestedFieldMeta[_] => x.copy(meta = meta)
//  }
//}
//
///** Supporting class and object to introduce `listToCase` implicit conversion with memoisation logic. */
//case class Roles(rs: List[Role])
//object Roles {
//  import scala.language.implicitConversions
//  // this implicit conversion allows introducing memoization keeping the refactor to a minimum.
//  implicit def listToCase(rs: List[Role]): Roles = {
//    // short-lived instance don't return `r` out of this method! (breaks memoization)
//    val r = Roles(rs)
//    // idempotently add `rs` in the memoizationSet
//    memoizationSet.add(r)
//    // once `rs` is granted to be on the `memoizationSet` we can get the memoised instance
//    memoizationSet.ceiling(r)
//  }
//  implicit def caseToList(roles: Roles): List[Role] = roles.rs
//
//  // internal, global set with memoised instances
//  private val memoizationSet = new java.util.concurrent.ConcurrentSkipListSet[Roles](
//    new java.util.Comparator[Roles]() {
//      override def compare(o1: Roles, o2: Roles): Int = o1.rs.toString.compareTo(o2.rs.toString)
//    }
//  )
//}
//
//case class ModelFieldMeta[A](name: String, meta: ModelMeta, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                             requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                             isUsefulTrend: Boolean = true) extends FieldMeta[A] with ObjectMeta
//case class NestedFieldMeta[A](name: String, meta: ModelMeta, isOptional: Boolean = true, roles: Roles = ModelMeta.Any,
//                              requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                              isUsefulTrend: Boolean = true) extends FieldMeta[A] with ObjectMeta {
//  override def isArray: Boolean = true
//}
//
//case class StringFieldMeta(name: String, isAnalyzed: Boolean = false, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                           requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                           isUsefulTrend: Boolean = true, indexField: Boolean = true) extends FieldMeta[String] with ScalarMeta
//case class BooleanFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                            requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                            isUsefulTrend: Boolean = true) extends FieldMeta[Boolean] with ScalarMeta
//case class IntFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                        requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                        isUsefulTrend: Boolean = true) extends FieldMeta[Int] with ScalarMeta
//case class LongFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                         requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                         isUsefulTrend: Boolean = true) extends FieldMeta[Long] with ScalarMeta
//case class FloatFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                          requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                          isUsefulTrend: Boolean = true) extends FieldMeta[Float] with ScalarMeta
//case class DoubleFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                           requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                           isUsefulTrend: Boolean = true) extends FieldMeta[Double] with ScalarMeta
//case class InstantFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                            requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                            isUsefulTrend: Boolean = true) extends FieldMeta[Instant] with ScalarMeta
//// This is the legacy 'date' type.  It should only be used in 'old' schemas
//case class DateFieldMeta(name: String, isOptional: Boolean = true, isArray: Boolean = false, roles: Roles = ModelMeta.Any,
//                         requiresContract: Boolean = true, requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                         isUsefulTrend: Boolean = true) extends FieldMeta[Instant] with ScalarMeta
//case class BytesFieldMeta(name: String, isOptional: Boolean = true, roles: Roles = ModelMeta.Any, requiresContract: Boolean = true,
//                          requiresApprovalForPartnerAccess: Boolean = true, isPersonallyIdentifiable: Boolean = false,
//                          isUsefulTrend: Boolean = true) extends FieldMeta[Array[Byte]] with ScalarMeta {
//  // isArray=true would mean multiple Array[Byte] or Array[Array[Byte]]. for now force the simple case to prevent confusion
//  override def isArray: Boolean = false
//}
//
//case class ModelMeta(typeName: String, specFields: Seq[FieldMeta[_]]) {
//  val fields: Seq[FieldMeta[_]] = filteredFields(specFields) // Push down roles restrictions to sub-fields
//  val fieldMap: Map[String, FieldMeta[_]] = fields.map { field => (field.name, field) }.toMap
//
//  lazy val toJson: JsObject = _toJson()
//
//  def _toJson(path: List[String] = Nil): JsObject = {
//    val jsFields = fields.foldLeft(JsArray()) {
//      case (jsAry, field) =>
//        val pathName = path ++ List(field.name)
//        val baseJs = Json.obj(
//          "name" -> field.name,
//          "fullName" -> pathName.mkString("."),
//          "type" -> ModelMeta.typeName(field),
//          "isArray" -> field.isArray,
//          "roles" -> field.roles.map(_.toString),
//          "isPersonallyIdentifiable" -> field.isPersonallyIdentifiable,
//          "requiresContract" -> field.requiresContract,
//          "requiresApprovalForPartnerAccess" -> field.requiresApprovalForPartnerAccess
//        )
//        val fieldJs = field match {
//          case scalarField: ScalarMeta =>
//            baseJs ++ (scalarField match {
//              case stringField: StringFieldMeta =>
//                val isIndexed = if (!stringField.indexField) Some(Json.obj("isIndexed" -> stringField.indexField)) else None
//                val analyzed = Json.obj("isAnalyzed" -> stringField.isAnalyzed)
//                isIndexed.map { x => analyzed ++ x }.getOrElse(analyzed)
//              case _ => Json.obj()
//            })
//          case objectField: ObjectMeta =>
//            baseJs ++ Json.obj("objType" -> ModelMeta.objType(objectField)) ++ objectField.meta._toJson(pathName)
//        }
//        jsAry :+ fieldJs
//    }
//    Json.obj("fields" -> jsFields)
//  }
//
//  /**
//   * Push down top level possible roles and prunes fields that are not authorized.
//   */
//  private def filteredFields(fields: Seq[FieldMeta[_]], possibleRoles: Set[Role] = ModelMeta.Any.toSet): Seq[FieldMeta[_]] = {
//    fields.flatMap { field =>
//      val fieldRoles = field.roles.toSet
//      val roles = possibleRoles & fieldRoles
//      if (roles.isEmpty) {
//        None
//      } else {
//        val filteredFieldOpt: Option[FieldMeta[_]] = field match {
//          case _: ScalarMeta => Some(field.setRoles(roles.toList))
//          case objectField: ObjectMeta =>
//            val filteredMeta = objectField.meta.filterRoles(roles)
//            if (filteredMeta.fields.nonEmpty) {
//              Some(objectField.setMeta(filteredMeta).setRoles(roles.toList))
//            } else {
//              None
//            }
//        }
//        filteredFieldOpt
//      }
//    }
//  }
//
//  def filterRoles(possibleRoles: Set[Role] = ModelMeta.Any.toSet): ModelMeta =
//    ModelMeta(typeName, filteredFields(fields, possibleRoles))
//
//  def unknownFields(fields: Seq[String]): Seq[String] = fields.filter { field => getField(field).isEmpty }
//
////  def authorizedFields(auth: AuthorizedRoles, fields: Seq[String]): Seq[String] =
////    fields.filter { field => getField(field).map(_.isAuthorized(auth)).getOrElse(false) }
////
////  def unauthorizedFields(auth: AuthorizedRoles, fields: Seq[String]): Seq[String] =
////    fields.filter { field => getField(field).map(!_.isAuthorized(auth)).getOrElse(false) }
//
//  def getField(path: String): Option[FieldMeta[_]] = getField(path.split('.'))
//  def getField(path: Seq[String]): Option[FieldMeta[_]] = {
//    path match {
//      case field +: nextFields =>
//        fieldMap.get(field).flatMap(_.getField(nextFields))
//      case _ => None
//    }
//  }
//
//  def getValue(path: String, js: JsValue): Option[(FieldMeta[_], JsValue)] = getValue(path.split('.'), js)
//  def getValue(path: Seq[String], js: JsValue): Option[(FieldMeta[_], JsValue)] = {
//    getField(path).flatMap { field =>
//      ModelMeta.getValue(path, js).asOpt.map(field -> _)
//    }
//  }
//}
//
//object ModelMeta {
//  def apply(fields: Seq[FieldMeta[_]]): ModelMeta = apply(typeName = "", fields)
//
//  val Any = List(AdminRole, SalesRole, SupportRole, EngineeringRole, PartnerRole, CustomerRole)
//  val Admin = List(AdminRole)
//  val Sales = List(AdminRole, SalesRole)
//  val Support = List(AdminRole, SalesRole, EngineeringRole, SupportRole)
//  val Internal = Support // Currently authorizing Support authorizes all internal roles
//  val Partner = List(AdminRole, SalesRole, SupportRole, EngineeringRole, PartnerRole)
//  val Customer = List(AdminRole, SalesRole, SupportRole, EngineeringRole, PartnerRole, CustomerRole)
//
//  def typeName(field: FieldMeta[_]): String = field match {
//    case _: ModelFieldMeta[_] => "object"
//    case _: NestedFieldMeta[_] => "object"
//    case _: StringFieldMeta => "string"
//    case _: BooleanFieldMeta => "boolean"
//    case _: IntFieldMeta => "int"
//    case _: LongFieldMeta => "long"
//    case _: FloatFieldMeta => "float"
//    case _: DoubleFieldMeta => "double"
//    case _: InstantFieldMeta => "date"
//    case _: DateFieldMeta => "date"
//    case _: BytesFieldMeta => "bytes"
//  }
//
//  def objType(objField: ObjectMeta): String = objField match {
//    case _: ModelFieldMeta[_] => "model"
//    case _: NestedFieldMeta[_] => "nested"
//  }
//
//  def toJson(field: FieldMeta[_]): JsObject = {
//    val typ = if (field.isArray) s"array[${typeName(field)}]" else typeName(field)
//    val roles: List[String] = field.roles map (_.toString)
//    val fieldJs = field match {
//      case scalarField: ScalarMeta => Json.obj("name" -> field.name, "type" -> typ, "roles" -> roles, "requiresContract" -> field.requiresContract,
//        "requiresApprovalForPartnerAccess" -> field.requiresApprovalForPartnerAccess, "isPersonallyIdentifiable" -> scalarField.isPersonallyIdentifiable)
//      case _ => Json.obj("name" -> field.name, "type" -> typ, "roles" -> roles, "requiresContract" -> field.requiresContract,
//        "requiresApprovalForPartnerAccess" -> field.requiresApprovalForPartnerAccess)
//    }
//    field match {
//      case f: ObjectMeta => fieldJs ++ f.meta.toJson
//      case _ => fieldJs
//    }
//  }
//
//  @scala.annotation.tailrec
//  def getValue(path: Seq[String], js: JsValue): JsResult[JsValue] = path match {
//    case headField +: tailFields =>
//      js match {
//        case jsObj: JsObject => (jsObj \ headField).validate[JsValue] match {
//          case JsSuccess(jsNext, _) => getValue(tailFields, jsNext)
//          case _ => JsError(s"""Field "$headField" is undefined""")
//        }
//        case jsArr: JsArray if tailFields.isEmpty =>
//          val values = for {
//            jsArrVal <- jsArr.value
//            jsObj <- jsArrVal.asOpt[JsObject]
//            jsField <- (jsObj \ headField).validate[JsValue].asOpt
//          } yield jsField
//          if (values.nonEmpty) {
//            JsSuccess(JsArray(values.distinct))
//          } else {
//            JsError(s"""Field "$headField" cannot be accessed either because it is undefined or because you have an unexpected JSON structure""")
//          }
//        case _ => JsError(s"""Field "$headField" cannot be accessed on a ${js.getClass}, only on a JsObject or JsObject under a JsArray""")
//      }
//    case _ => JsSuccess(js)
//  }
//}
//
//object FieldFilterHelper {
//
//  def filterWithMeta(roleName: String, jsObj: JsObject, meta: ModelMeta, hasApprovalForPartner: Boolean, shouldShowPII: Boolean,
//                     shouldShowTrend: Boolean, hasContract: Boolean): JsObject = {
//    jsObj.fields.foldLeft(jsObj) {
//      case (obj, (fieldName, _)) =>
//        meta.getField(fieldName) map { field =>
//          val requiresContract = field.requiresContract
//          val requiresApprovalForPartnerAccess = field.requiresApprovalForPartnerAccess
//          val roles = field.roles map (_.name)
//
//          field match {
//            case model: ObjectMeta =>
//              updateObject(obj = obj, modelOpt = Some(model), roles = roles, roleName = roleName,
//                fieldName = fieldName, shouldShowPII = shouldShowPII, isPII = model.isPersonallyIdentifiable,
//                shouldShowTrend = shouldShowTrend, isUsefulTrend = model.isUsefulTrend, hasApprovalForPartner = hasApprovalForPartner,
//                requiresApprovalForPartnerAccess = requiresApprovalForPartnerAccess, hasContract = hasContract, requiresContract = requiresContract)
//            case scalar: ScalarMeta =>
//              updateObject(obj = obj, modelOpt = None, roles = roles, roleName = roleName,
//                fieldName = fieldName, shouldShowPII = shouldShowPII, isPII = scalar.isPersonallyIdentifiable,
//                shouldShowTrend = shouldShowTrend, isUsefulTrend = scalar.isUsefulTrend, hasApprovalForPartner = hasApprovalForPartner,
//                requiresApprovalForPartnerAccess = requiresApprovalForPartnerAccess, hasContract = hasContract, requiresContract = requiresContract)
//          }
//        } getOrElse obj
//    }
//  }
//
//  def updateObject(obj: JsObject, modelOpt: Option[ObjectMeta] = None, roles: Seq[String], roleName: String,
//                   fieldName: String, shouldShowPII: Boolean, isPII: Boolean, shouldShowTrend: Boolean, isUsefulTrend: Boolean,
//                   hasApprovalForPartner: Boolean, requiresApprovalForPartnerAccess: Boolean, hasContract: Boolean, requiresContract: Boolean): JsObject = {
//    if (roles.contains(roleName) && (shouldShowPII || !isPII) && (isUsefulTrend || !shouldShowTrend) &&
//      (roleName != PartnerRole.name || (!requiresApprovalForPartnerAccess || hasApprovalForPartner)) &&
//      (roleName != CustomerRole.name || (!requiresContract || hasContract)) &&
//      (roleName != PartnerRole.name || (!requiresContract || hasContract))) {
//
//      modelOpt flatMap { model =>
//        (obj \ fieldName).toOption.map {
//          case validObj: JsObject =>
//            val filteredFieldVal = filterWithMeta(roleName, validObj, model.meta, shouldShowPII = shouldShowPII, shouldShowTrend = shouldShowTrend,
//              hasApprovalForPartner = hasApprovalForPartner, hasContract = hasContract)
//            obj - fieldName ++ Json.obj(fieldName -> filteredFieldVal)
//          case validAry: JsArray =>
//            val filteredFieldVal = JsArray(validAry.value.map {
//              case aryObj: JsObject => filterWithMeta(roleName, aryObj, model.meta, shouldShowPII = shouldShowPII, shouldShowTrend = shouldShowTrend,
//                hasApprovalForPartner = hasApprovalForPartner, hasContract = hasContract)
//              case aryVal => aryVal
//            })
//            obj - fieldName ++ Json.obj(fieldName -> filteredFieldVal)
//          case _ => obj - fieldName //Field is of an unexpected type.
//        }
//      } getOrElse obj
//    } else {
//      obj - fieldName
//    }
//  }
//
//}
