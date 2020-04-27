package com.knoldus.akka

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}
import java.util.UUID

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset, CommittableOffsetBatch}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, GraphDSL, Source, Unzip, Zip}
import akka.stream.{FlowShape, OverflowStrategy}
import akka.{Done, NotUsed}
import com.knoldus.common.MDCProvider
import com.knoldus.common.services.RestartingStreamFactory
import com.knoldus.common.utils.CommonFlows
import com.typesafe.config.{Config, ConfigFactory}
import com.knoldus.common.utils.CommonFlows._
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, RetriableCommitFailedException}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{Format, Json, Reads}

import scala.collection.immutable.{Seq, Vector}
import scala.collection.mutable.WrappedArray
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Codec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class KafkaMessage[Key, Value](key: String, value: Value)

trait AkkaKafkaConsumer {

  import AkkaKafkaConsumer._
  import AkkaKafkaConsumer.Implicits._

  private implicit class consumerRecordMDC[K, V](consumerRecord: ConsumerRecord[K, V]) extends MDCProvider {
    override def mdcData: Map[String, String] = Map(
      "kafkaTopic" -> consumerRecord.topic(),
      "kafkaPartition" -> consumerRecord.partition().toString,
      "kafkaOffset" -> consumerRecord.offset().toString
    )
  }

  private val config: Config = ConfigFactory.load()
  private implicit val log: Logger = LoggerFactory.getLogger(getClass)

  protected implicit def actorSystem: ActorSystem

  protected val kafkaBrokers: String = config.getString("akka.kafka.brokers")
  protected val autoOffsetReset: String = config.getString("akka.kafka.consumer.kafka-clients.auto.offset.reset")
  protected val sessionTimeoutMS: String = config.getString("akka.kafka.consumer.kafka-clients.session.timeout.ms")
 protected val sessionTimeoutMSDuration: FiniteDuration = sessionTimeoutMS.toInt.millis

//  private val defaultKafkaConsumerGroup: String = s"${getClass.getSimpleName}-${UUID.randomUUID()}"
//
//  def kafkaConsumerGroup: String = Try(config.getString("akka.kafka.consumer.group")).getOrElse(defaultKafkaConsumerGroup)
protected val kafkaConsumerGroup = "employee-group"
  private def commitOffsetFlow[K, V, MAT](
                                           flow: Flow[ConsumerRecord[K, V], Done, MAT],
                                           parallelism: Int): Flow[(CommittableOffset, ConsumerRecord[K, V]), Done, MAT] = {

    Flow.fromGraph(GraphDSL.create(flow) { implicit builder => flow =>
      import GraphDSL.Implicits._
      val unzip = builder.add(Unzip[CommittableOffset, ConsumerRecord[K, V]])
      val zip = builder.add(Zip[CommittableOffset, Done])
      val committer = {
        val commitFlow = Flow[(CommittableOffset, Done)]
          .groupedWithin(batchingSize, batchingInterval)
          .map(group =>
            group.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) =>
              Try(batch.updated(elem._1))
                .getOrElse(CommittableOffsetBatch.empty.updated(elem._1))
            }
          )
          .via(offsetCommit(parallelism))
        builder.add(commitFlow)
      }
      // To allow the user flow to do its own batching, the offset side of the flow needs to effectively buffer
      // infinitely to give full control of backpressure to the user side of the flow.
      val offsetBuffer = Flow[CommittableOffset].buffer(offsetBufferSize, OverflowStrategy.backpressure)

      unzip.out0 ~> offsetBuffer ~> zip.in0
     unzip.out1 ~> unitApplyFlow[ConsumerRecord[K, V]](_.setMDC()) ~> flow ~> sideEffectFlow[Done](MDCProvider.clear) ~> zip.in1
      zip.out ~> committer.in

      FlowShape(unzip.in, committer.out)
    })
  }

  /**
   * Creates a flow to commit offsets to Kafka.  Emits Dones when either a commit succeeds or if
   * a commit fails but "enough" recent commits have succeeded that a failure is acceptable.  An
   * unacceptable failure will fail the stream.
   *
   * The determination of whether a commit failure is acceptable is based on a success counter which
   * exponentially decays on failures.  The rate of exponential decay is set by the configuration setting
   * "akka.kafka.consumer.offset.commit-failure-decay-factor".  If that configuration setting raised to the
   * power of the cumulative number of failures exceeds the cumulative number of successes, the stream will fail.
   *
   * Note that due to arithmetic overflow, for a "commit-failure-decay-factor" of _b_, after _f_ failures and _s_
   * successes, if _s_ - _b^f_ is greater than about 2 billion, the stream will fail; this cannot happen until
   * at least 2 billion successful commits.
   */
  private def offsetCommit(parallelism: Int): Flow[CommittableOffsetBatch, Done, NotUsed] = {
    require(parallelism > 0, "Negative parallelism doesn't really make sense, does it?")
    val successDone = Success(Done)
    val futDoneToSuccessDone: Done => Try[Done] = _ => successDone
    val ctx = ExecutionContext.global

    // These partial functions ultimately always result in a successful Future of either a successful or failed Try
    // This allows for a mapAsync that does not fail, which in turn means that we can decide stream failure
    // in a statefulMapConcat stage
    def allThrowablesToFailure(commitBatch: CommittableOffsetBatch): PartialFunction[Throwable, Future[Try[Done]]] = {
      case throwable =>
        log.warn(s"Failed to commit offsets for batch=$commitBatch", throwable)
        Future.successful(Failure(throwable))
    }
    def retryIfRetriable(commitBatch: CommittableOffsetBatch): PartialFunction[Throwable, Future[Try[Done]]] = {
      case rcfe: RetriableCommitFailedException =>
        log.info(s"Retrying offset commit", rcfe)
        doCommit(commitBatch).recoverWith(allThrowablesToFailure(commitBatch))(ctx)
    }
    def firstRecovery(commitBatch: CommittableOffsetBatch): PartialFunction[Throwable, Future[Try[Done]]] =
      retryIfRetriable(commitBatch) orElse allThrowablesToFailure(commitBatch)

    def doCommit(commitBatch: CommittableOffsetBatch): Future[Try[Done]] = {
      implicit val _ctx = ctx
      commitBatch.commitScaladsl().map(futDoneToSuccessDone)
    }

    Flow[CommittableOffsetBatch]
      .via(CommonFlows.Log.debug(offsets => s"Committing offsets $offsets"))
      .mapAsync(parallelism) { commitBatch =>
        doCommit(commitBatch).recoverWith(firstRecovery(commitBatch))(ctx)
      }
      .statefulMapConcat { () =>
        var successCount = 1 << 10
        val listDone = List(Done)
        t: Try[Done] =>
          t match {
            case Success(d) =>
              successCount += 1; List(d)
            case Failure(t) =>
              successCount = successCount / commitFailureDecay
              if (successCount > 0) {
                log.warn(s"Failed to commit offsets, but continuing on", t)
                listDone
              } else {
                log.error(s"Too many offset commit failures; failing stream", t)
                throw new RuntimeException(t)
              }
          }
      }
  }

  private def compressedJsonConsumerSettings: ConsumerSettings[String, Array[Byte]] = {

    ConsumerSettings(
      system = actorSystem,
      keyDeserializer = new StringDeserializer,
      valueDeserializer = new ByteArrayDeserializer
    )
      .withBootstrapServers(kafkaBrokers)
      .withGroupId(kafkaConsumerGroup)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  }

  val offsetBufferSize: Int = config.getInt("akka.kafka.consumer.offset.buffer-size")
  val batchingSize: Int = config.getInt("akka.kafka.consumer.offset.commit-batch")
  val batchingInterval: FiniteDuration = 60.seconds
  val commitFailureDecay: Int = config.getInt("akka.kafka.consumer.offset.commit-failure-decay-factor").max(1)
  val wakeupTimeout: FiniteDuration = config.getDuration("akka.kafka.consumer.wakeup-timeout").getSeconds.seconds

  private val defaultMinBackoff = (sessionTimeoutMSDuration - wakeupTimeout + 2.seconds).max(10.seconds)
  private val defaultMaxBackoff = (defaultMinBackoff * 16).min(600.seconds)

  /**
   * A restarting configuration which should be used to restart a stream which reads from Kafka.  It will always wait
   * long enough between restarts such that the old, failed consuming stream will be marked as failed before the new consumer
   * wakes up.
   */
  val defaultRestartConfig = RestartingStreamFactory(
    minBackOff = defaultMinBackoff,
    maxBackOff = defaultMaxBackoff,
    randomFactor = 0.5,
    maxRestarts = -1, // no limit
    onlyOnFailure = false // should never stop unless it fails, but what the heck.
  )

  private def kafkaSourceWithOffsetCommit(
                                           flow: Flow[ConsumerRecord[String, Array[Byte]], Done, _],
                                           topics: Set[String],
                                           sourceFactory: KafkaSourceFactory
                                         ): Source[Done, Consumer.Control] = {
    sourceFactory.source(topics)
      .map(consumerRecord => consumerRecord.committableOffset -> consumerRecord.record).async
     .via(commitOffsetFlow(flow, 1))
  }

  /**
   * Creates a flow that reads objects from Kafka, passes the to a 'business' flow and automatically commits Kafka offsets.
   * The messages in Kafka are JSON encoded and must deserialized by 'format'.  The messages may be raw JSON or GZIP compressed JSON.
   *
   * @param topic the topic to read from
   * @param business the business logic flow
   * @param format Play Json formatter for `A`
   * @param dff A factory for deserialization flows
   * @tparam A The type the JSON will be deserialized to.
   * @return The flow as a `Source`
   */
  def jsonWithCommits[A](
                          topic: String,
                          business: Flow[A, Done, _]
                        )(implicit format: Format[A], dff: DeserializationFlowFactory): Source[Done, Consumer.Control] = {

    sourceWithCommits(
      topic = topic,
      bytesPerSecond = None,
      deser = dff.flow[A],
      business = business
    )
  }


  /**
   * Define a source that reads objects from Kafka, deserializes them, passes them to a business flow and commits Kafka offsets when done.
   *
   * @param topic the topic to read from
   * @param bytesPerSecond optional throttle on values read from Kafka; only applies if defined and positive
   * @param deser flow to deserialize objects; objects which cannot be deserialized will be effectively ignored
   * @param business the business logic to apply to successfully deserialized objects
   * @tparam A the type to deserialize to
   */
  def sourceWithCommits[A](
                            topic: String,
                            bytesPerSecond: Option[Int],
                            deser: DeserializationFlow[A],
                            business: Flow[A, Done, _]): Source[Done, Consumer.Control] = {

    kafkaSourceWithOffsetCommit(
      flow = deser.via(
        optionalFlow(
          someFlow = Flow[KafkaMessage[String, A]]
            .map(_.value)
            .via(business),
          noneFlow = doneFlow
        )),
      topics = Set(topic),
      sourceFactory =
        bytesPerSecond
          .filter(_ > 0)
          .map(throttle => new SizeThrottledKafka(throttle))
          .getOrElse(UnthrottledKafka)
    )
  }

  trait KafkaSourceFactory {
    def source(topics: Set[String]): Source[CommittableMessage[String, Array[Byte]], Consumer.Control]
  }

  object UnthrottledKafka extends KafkaSourceFactory {
    def source(topics: Set[String]): Source[CommittableMessage[String, Array[Byte]], Consumer.Control] =
      Consumer.committableSource(compressedJsonConsumerSettings, Subscriptions.topics(topics))
  }

  class SizeThrottledKafka(val bytesPerSecond: Int) extends KafkaSourceFactory {
    def source(topics: Set[String]): Source[CommittableMessage[String, Array[Byte]], Consumer.Control] =
      UnthrottledKafka.source(topics)
        .throttle(bytesPerSecond, 1.second, _.record.value.length)
  }
}

object AkkaKafkaConsumer {
  type DeserializationFlow[A] = Flow[ConsumerRecord[String, Array[Byte]], Option[KafkaMessage[String, A]], NotUsed]
  type Deserializer[A] = ConsumerRecord[String, Array[Byte]] => Option[KafkaMessage[String, A]]

  object Implicits {
    implicit def playFormat2Deserializer[A](implicit fmt: Format[A], log: Logger): Deserializer[A] = { consumerRecord =>
    parseMaybeCompressedJSONBytes[A](consumerRecord.value).toOption.map(a => KafkaMessage(consumerRecord.key, a))
    }
    /**
     * A basically trivial "deserializer" for byte arrays.
     */
    implicit val rawDeserializer: Deserializer[Array[Byte]] = { consumerRecord =>
      Option(KafkaMessage(consumerRecord.key, consumerRecord.value))
    }
    private lazy val compressorStreamFactory = new CompressorStreamFactory()

    /**
     * Uncompresses a stream if needed.  Supports up to [[maximumCompressionNesting]] nested layers of compression.
     * The compression depth is limited to prevent stack overflow.  If support for more layers is needed
     * for some bizarre reason, calls can be chained without blowing the stack.
     *
     * @param possiblyCompressedStream
     */
    def uncompressStream(possiblyCompressedStream: InputStream): InputStream = {
      @scala.annotation.tailrec
      def iter(bs: BufferedInputStream, depth: Int): InputStream = {
        val cs =
          Try(compressorStreamFactory.createCompressorInputStream(bs))
            .recoverWith {
              // Not a compressed stream
              case _: CompressorException => Success(bs)
              // Some other failure
              case e =>
                bs.close()
                Failure(e)
            }.get // Will throw exceptions that aren't CompressorExceptions
        if (cs eq bs) {
          cs
        } else {
          if (depth > 0) {
            log.warn("Multiple levels of compression detected: {} had at least {} layers of compression", possiblyCompressedStream: Any, depth: Any)
          }
          val nextBS = new BufferedInputStream(cs)
          iter(nextBS, depth + 1)
        }
      }

      iter(new BufferedInputStream(possiblyCompressedStream), 0)
    }

    private val possibleCodecs: Seq[Codec] = Seq(Codec.UTF8, Codec.ISO8859)
    private val log = LoggerFactory.getLogger(this.getClass)
    def readLinesFromCompressedText(compressedText: WrappedArray[Byte], possibleCodecs: Seq[Codec] = possibleCodecs): Vector[String] = {
      possibleCodecs match {
        case codec +: otherCodecs => readLinesFromCompressedText(compressedText, codec).getOrElse(readLinesFromCompressedText(compressedText, otherCodecs))
        case Seq() => {
          log.warn(s"Error reading compressed text - out of codecs to try")
          Vector()
        }
      }
    }

    def readLinesFromCompressedText(compressedText: WrappedArray[Byte], codec: Codec): Option[Vector[String]] = {
      val compressedStream = new ByteArrayInputStream(compressedText.array)
      val uncompressedStream = uncompressStream(compressedStream)
      try {
        val source = scala.io.Source.fromInputStream(uncompressedStream)(codec)
        Some(source.getLines.toVector)
      } catch {
        case NonFatal(e) => None
      }
    }
    def parseMaybeCompressedJSONStream[A: Reads](compressedStream: InputStream): Try[A] = Try {
      val uncompressedStream: InputStream = uncompressStream(compressedStream)
      Json.parse(uncompressedStream).as[A]
    }

    def parseMaybeCompressedJSONBytes[A: Reads](bytes: Array[Byte]): Try[A] = {
      val compressedStream = new ByteArrayInputStream(bytes)
      parseMaybeCompressedJSONStream(compressedStream)
    }
  }

  /**
   * A factory for constructing Kafka deserialization flows given a function to transform a ConsumerRecord into a possible
   * KafkaMessage.
   */
  trait DeserializationFlowFactory {
    /**
     * @param deserializer a function to transform a ConsumerRecord into a possible KafkaMessage
     */
    def flow[A](implicit deserializer: Deserializer[A]): DeserializationFlow[A]
  }

  object DeserializationFlowFactory {
    /**
     * Creates synchronous deserialization flows, conforming to earlier behavior of the AkkaKafkaConsumer.
     *
     * This should only be used if:
     *  - the deserializer is so trivial that the overhead of asynchrony and parallelism is not worth it (e.g. the 'rawDeserializer')
     *  - the stream has low throughput requirements
     */
    object Synchronous extends DeserializationFlowFactory {
      def flow[A](implicit deserializer: Deserializer[A]): DeserializationFlow[A] =
        Flow[ConsumerRecord[String, Array[Byte]]].map(deserializer)
    }

    /**
     * Creates asynchronous deserialization flows.  Depending on the 'parallelism' parameter, the flow may deserialize
     * multiple objects simultaneously (while preserving order).
     *
     * @param parallelism the maximum number of objects to deserialize at once
     * @param ctx the ExecutionContext in which to deserialize
     */
    class Asynchronous(val parallelism: Int, val ctx: ExecutionContext) extends DeserializationFlowFactory {
      require(parallelism >= 1, "Non-positive parallelism doesn't make sense, does it?")

      def flow[A](implicit deserializer: Deserializer[A]): DeserializationFlow[A] =
        Flow[ConsumerRecord[String, Array[Byte]]].mapAsync(parallelism) { record =>
          Future(deserializer(record))(ctx)
        }
    }
  }
}
