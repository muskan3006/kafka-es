package com.knoldus.models

import com.knoldus.common.utils.{HasDefaultConfig, ResourceCompanion}
import play.api.libs.json.{Format, Json}
import spray.json.{JsObject, JsString, JsonWriter}

//case class Address(houseNo: String, area: String, city: String, state: String)
//object Address{
//  implicit val format: Format[Address] = Json.format[Address]
//}

case class Employee(empId: String,
                     name: String,
                     doj: String,
                     email: String)

object Employee extends ResourceCompanion[Employee] with HasDefaultConfig {
  implicit val format: Format[Employee] = Json.format[Employee]
  implicit val jsonWriter:JsonWriter[Employee] = (employee:Employee) => {
    JsObject(
      "empId" -> JsString(employee.empId),
      "name" -> JsString(employee.name),
      "doj" -> JsString(employee.doj),
    "email" -> JsString(employee.email))
  }
  override lazy val index: String = config.getString("elasticsearch.index.default")
  override val resourceType: String = "employee-details"

  override def id(obj: Employee): String = obj.empId

  override def docType: String = resourceType



  }
