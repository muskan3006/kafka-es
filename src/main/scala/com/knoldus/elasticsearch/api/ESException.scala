package com.knoldus.elasticsearch.api

case class ESException(msg: String, statusCode: Int) extends RuntimeException(msg)

object ESException {
  def apply(msg: String): ESException = ESException(msg, 504)

  val conflictMessage = "This data has been modified after it was fetched. Stale data cannot be modified. Please fetch the data and try again."
  val indexWriteLockedMessage = "User data cannot be saved at this time. Please try again in a few minutes"

}
