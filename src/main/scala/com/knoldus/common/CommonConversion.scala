package com.knoldus.common

object CommonConversion {
  def invertStringMap(orig: Map[String, List[String]]): Map[String, List[String]] = {
    val grouped = orig.flatMap {
      case (origKey, origValues) =>
        origValues.map(_ -> origKey)
    } groupBy (_._1)
    grouped.map {
      case (newKey, newKeyMap) =>
        newKey -> newKeyMap.values.toList
    }
}
}
