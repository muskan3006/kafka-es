package com.knoldus.models

import com.knoldus.common.utils.{HasDefaultConfig, ResourceCompanion}
import play.api.libs.json.{Format, Json}

case class Company(regNo: String,
                   name: String,
                   address: Address,
                   email: String,
                   phoneNo: Long,
                   ceo: String,
                   revenue: Long
                  )

object Company extends ResourceCompanion[Company] with HasDefaultConfig {
  implicit val format: Format[Company] = Json.format[Company]

  implicit val _reads = Json.reads[Company]

  implicit val defaultWrites = Json.writes[Company]

  override lazy val index: String = config.getString("elasticsearch.index.default")
  override val resourceType: String = "company-type"

  override def id(obj: Company): String = obj.regNo

  override def docType: String = resourceType

}
