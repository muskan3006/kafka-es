package com.knoldus.common.utils

import play.api.libs.json.{ Format => PlayFormat, Json => PlayJson }
import spray.json.{ JsValue => SprayJSV, JsonFormat => SprayFormat, JsonParser => SprayParser }

/**
 * A conversion from a Play Format (here PlayFormat) to a Spray JsonFormat (here SprayFormat).
 * The generated SprayFormat is an adapter that defers all serialization/deserialization to the PlayFormat.
 */
object Play2Spray {
  /**
   * Construct a SprayFormat from an implicit PlayFormat.
   * Note that this constructs a new SprayFormat every time, so try to be nice to the GC by either keeping
   * these ephemeral (e.g. short-lived method scope) or keeping them from becoming garbage (e.g. val in an object)
   *
   * @param playFmt the implicit PlayFormat to actually use in place of the SprayFormat
   * @tparam T the type of objects to serialize/deserialize
   * @return a SprayFormat adapter to a PlayFormat
   */
  def apply[T](implicit playFmt: PlayFormat[T]): SprayFormat[T] =
    new SprayFormat[T] {
      // To simplify the logic and maximally rely on Spray and Play being correct/tested, rather than doing a deep AST-transform,
      // we go in both directions by constructing a JSON string.  Correctness before efficiency...
      def read(json: SprayJSV): T =
        PlayJson.parse(json.compactPrint).as[T]

      def write(obj: T): SprayJSV =
        SprayParser(PlayJson.toJson(obj).toString)
    }
}
