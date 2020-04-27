package com.knoldus.common.utils

import akka.{Done, NotUsed}
import akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition, Sink, Source, Zip}
import org.slf4j.Logger

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object CommonFlows {

  object Log {
    /**
     * Flow which generates a log message for each object in the flow and logs at level DEBUG
     *
     * @tparam T Type of objects in the flow
     * @param msgF function to generate the log message
     * @param log  the Logger through which to log the message
     */
    def debug[T](msgF: T => String)(implicit log: Logger): Flow[T, T, NotUsed] = logMessage(msgF) { m => log.debug(m) }

    /**
     * Flow which generates a log message for each object in the flow and logs at level INFO
     *
     * @tparam T Type of objects in the flow
     * @param msgF function to generate the log message
     * @param log  the Logger through which to log the message
     */
    def info[T](msgF: T => String)(implicit log: Logger): Flow[T, T, NotUsed] = logMessage(msgF) { m => log.info(m) }

    /**
     * Flow which generates a log message for each object in the flow and logs at level WARN
     *
     * @tparam T Type of objects in the flow
     * @param msgF function to generate the log message
     * @param log  the Logger through which to log the message
     */
    def warn[T](msgF: T => String)(implicit log: Logger): Flow[T, T, NotUsed] = logMessage(msgF) { m => log.warn(m) }

    /**
     * Flow which generates a log message for each object in the flow and logs at level ERROR
     *
     * @tparam T Type of objects in the flow
     * @param msgF function to generate the log message
     * @param log  the Logger through which to log the message
     */
    def error[T](msgF: T => String)(implicit log: Logger): Flow[T, T, NotUsed] = logMessage(msgF) { m => log.error(m) }

    /**
     * Flow which generates a log message for each object in the flow and logs at level TRACE
     *
     * @tparam T Type of objects in the flow
     * @param msgF function to generate the log message
     * @param log  the Logger through which to log the message
     */
    def trace[T](msgF: T => String)(implicit log: Logger): Flow[T, T, NotUsed] = logMessage(msgF) { m => log.trace(m) }

    /**
     * Runs the given source, ignores the produced values, and logs a message on completion based on whether the
     * stream succeeded or failed.
     *
     * The logging occurs in the global default ExecutionContext, but since it only runs when a stream completes,
     * that should not be a problem.
     *
     * @tparam T Type of objects produced by the source
     * @param source     the source
     * @param successMsg the message to log on success (by-name)
     * @param failureMsg the preamble to log on failure; the exception information from the failure will be included (by-name)
     * @param log        the Logger through which to log the message
     */
    def runIgnoredWithCompletionLogging[T](source: Source[T, _], successMsg: => String, failureMsg: => String)(implicit log: Logger, mat: Materializer): Unit =
      source.runWith(Sink.ignore).onComplete {
        case Success(Done) => log.warn(successMsg)
        case Failure(t) => log.error(failureMsg, t)
      }(ExecutionContext.global)


    private def logMessage[T](msgF: T => String): (String => Unit) => Flow[T, T, NotUsed] = mapUnitApplyFlow[T, String](t => msgF(t))
  }
  /**
   * Flow which applies a called-for-side-effect function to the items in the flow, passing the items through.
   * Optimized sugar for mapUnitApplyFlow({ t => t }, f)
   *
   * @tparam T Type of objects in the flow
   * @param f the called-for-side-effect function
   */
  def unitApplyFlow[T](f: T => Unit): Flow[T, T, NotUsed] = Flow[T].map {
    item: T =>
      f(item)
      item
  }

  /**
   * Flow which applies a called-for-side-effect function to the result of calling another function to each item in the
   * flow, passing the original items through.  The intent here is to facilitate calling a side-effecting function which
   * requires input of a different type than the objects of the flow.
   *
   * @tparam T Type of objects in the flow
   * @tparam U Result type of convF
   * @param convF function to generate input for f (most often a conversion function)
   * @param f     the called-for-side-effect function
   */
  def mapUnitApplyFlow[T, U](convF: T => U)(f: U => Unit): Flow[T, T, NotUsed] = Flow[T].map {
    item: T =>
      f(convF(item))
      item
  }

  /**
   * Creates a Flow which calls a side effect function passing the items through
   *
   * @param f the called-for-side-effect function
   * @tparam T The type of object in the flow
   * @return the flow
   */
  def sideEffectFlow[T](f: () => Unit): Flow[T, T, NotUsed] = Flow[T].map {
    item: T =>
      f()
      item
  }
  /**
   * Creates a flow to process an Option. If the option is a none, it is sent to the `noneFlow`.  Otherwise it is
   * passed to the `someFlow`.  Both the `someFlow` and `noneFlow` must result in a `Done`
   * @param someFlow flow to handle non-Nones
   * @param noneFlow flow to handle Nones
   * @return the created flow
   */
  def optionalFlow[T](someFlow: Flow[T, Done, _], noneFlow: Flow[None.type, Done, _]): Flow[Option[T], Done, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val somePartition = 0
      val nonePartition = 1

      val partition = b.add(Partition[Option[T]](2, {
        _.map(_ => somePartition).getOrElse(nonePartition)
      }))

      val merge = b.add(Merge[Done](2))

      partition.out(somePartition) ~> Flow[Option[T]].collect { case Some(t) => t } ~> someFlow ~> merge
      partition.out(nonePartition) ~> Flow[Option[T]].collect { case None => None } ~> noneFlow ~> merge

      FlowShape(partition.in, merge.out)
    })
  }

  /**
   * Runs the given flow for a stream, but preserves the original element. The given flow is run only for side
   * effects, and the original input element ends up being output once the given flow completes.
   *
   * @param flow The side-effect-only flow to run
   * @tparam In  Input type for this flow
   * @tparam Out Output type for the side-effect-only flow
   * @return A flow that sends elements through the given flow, but emits the original element when the given flow completes.
   */
  def keepOriginalElementFlow[In, Out](flow: Flow[In, Out, NotUsed]): Flow[In, In, NotUsed] = {
    val keepInputFromZipFlow = Flow[(In, Out)].map { elem => elem._1 }

    Flow.fromGraph[In, (In, Out), NotUsed] {
      GraphDSL.create[FlowShape[In, (In, Out)]]() {
        implicit b =>
          import GraphDSL.Implicits._
          val bcast = b.add(Broadcast[In](2))
          val zip = b.add(Zip[In, Out])

          bcast.out(1) ~> flow ~> zip.in1
          bcast.out(0) ~> zip.in0
          FlowShape(bcast.in, zip.out)
      }
    }.via(keepInputFromZipFlow)
  }

  def manyToOneDoneFlow[In, Out](
                                  individualElementFlow: Flow[In, Out, _],
                                  concurrentFlows: Int = 1
                                )(implicit materializer: Materializer): Flow[Vector[In], Done, NotUsed] = {

    Flow[Vector[In]].mapAsync(concurrentFlows) { iterable =>
      Source(iterable)
        .via(individualElementFlow)
        .runWith(Sink.ignore)
    }
  }

  /**
   * Creates a new identity flow
   */
  def identityFlow[T]: Flow[T, T, NotUsed] = Flow[T].map(identity)

  val doneFlow: Flow[Any, Done, NotUsed] = Flow[Any].map(_ => Done)


}
