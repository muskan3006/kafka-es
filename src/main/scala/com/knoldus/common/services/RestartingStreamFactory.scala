package com.knoldus.common.services

import akka.NotUsed
import akka.stream.scaladsl._
import scala.concurrent.duration.FiniteDuration

/**
 * A convenience factory for creating restarting sources, sinks, and flows
 * See [[akka.stream.scaladsl.RestartSource]], [[akka.stream.scaladsl.RestartSink]], and
 * [[akka.stream.scaladsl.RestartFlow]].
 */
case class RestartingStreamFactory(minBackOff: FiniteDuration, maxBackOff: FiniteDuration, randomFactor: Double,
                                   maxRestarts: Int, onlyOnFailure: Boolean = false) {

  /**
   * Create a restarting flow that wraps `toWrap`.
   * @param toWrap the flow to be wrapped
   */
  def flow[T, S](toWrap: Flow[T, S, _]): Flow[T, S, NotUsed] =
    if (onlyOnFailure) {
      RestartFlow.onFailuresWithBackoff(
        minBackoff = minBackOff,
        maxBackoff = maxBackOff,
        randomFactor = randomFactor,
        maxRestarts = maxRestarts
      ) { () => toWrap }
    } else {
      RestartFlow.withBackoff(
        minBackoff = minBackOff,
        maxBackoff = maxBackOff,
        randomFactor = randomFactor,
        maxRestarts = maxRestarts
      ) { () => toWrap }
    }

  /**
   * Create a restarting sink that wraps `toWrap`.  Note that backoff only on failure is not supported for sinks.
   * @param toWrap the sink to be wrapped
   */
  def sink[T](toWrap: Sink[T, _]): Sink[T, NotUsed] =
    RestartSink.withBackoff(
      minBackoff = minBackOff,
      maxBackoff = maxBackOff,
      randomFactor = randomFactor,
      maxRestarts = maxRestarts
    ) { () => toWrap }

  /**
   * Create a restarting source that wraps `wrap`
   * @param wrap the source to wrap
   */
  def source[T](wrap: Source[T, _]): Source[T, NotUsed] =
    if (onlyOnFailure) {
      RestartSource.onFailuresWithBackoff(
        minBackoff = minBackOff,
        maxBackoff = maxBackOff,
        randomFactor = randomFactor,
        maxRestarts = maxRestarts
      ) { () => wrap }
    } else {
      RestartSource.withBackoff(
        minBackoff = minBackOff,
        maxBackoff = maxBackOff,
        randomFactor = randomFactor,
        maxRestarts = maxRestarts
      ) { () => wrap }
    }
}
