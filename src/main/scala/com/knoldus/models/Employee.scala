package com.knoldus.models

import com.knoldus.common.utils.{HasDefaultConfig, ResourceCompanion}
import com.sksamuel.elastic4s.mappings.FieldType.{DateType, StringType}
import play.api.libs.json.{Format, Json}
//create index index mappings("employee-details" as(
//"empId" typed StringType,
//"name" typed StringType,
//"doj" typed DateType,
//"email" typed StringType
//))
//}.map { response => response.isAcknowledged }
case class Address(no: String,
                   area: String,
                   city: String,
                   state: String
                  )

object Address {
  implicit val format: Format[Address] = Json.format[Address]
}

case class Employee(empId: String,
                    name: String,
                    address: Address,
                    doj: String,
                    email: String,
                    mobileNo:Long,
                    designation:String,
                    salary:Int)

object Employee extends ResourceCompanion[Employee] with HasDefaultConfig {
  implicit val format: Format[Employee] = Json.format[Employee]

  implicit val _reads = Json.reads[Employee]

  implicit val defaultWrites = Json.writes[Employee]

  override lazy val index: String = config.getString("elasticsearch.index.default")
  override val resourceType: String = "employee-type"

  override def id(obj: Employee): String = obj.empId

  override def docType: String = resourceType

}