package zio.kafka.consumer

import org.apache.kafka.clients.consumer.{ OffsetAndMetadata, OffsetAndTimestamp }
import org.apache.kafka.common.{ Metric, MetricName, PartitionInfo, TopicPartition }
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.kafka.serde.Deserializer
import zio.kafka.consumer.diagnostics.Diagnostics
import zio.kafka.consumer.internal.{ ConsumerAccess, Runloop }
import zio.stream._

import scala.collection.compat._
import scala.jdk.CollectionConverters._

trait Consumer {

  /**
   * Returns the topic-partitions that this consumer is currently assigned.
   *
   * This is subject to consumer rebalancing, unless using a manual subscription.
   */
  def assignment: Task[Set[TopicPartition]]

  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Long]]

  /**
   * Retrieve the last committed offset for the given topic-partitions
   */
  def committed(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Option[OffsetAndMetadata]]]

  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, Long]]

  def listTopics(timeout: Duration = Duration.Infinity): Task[Map[String, List[PartitionInfo]]]

  /**
   * Create a stream that emits chunks whenever new partitions are assigned to this consumer.
   *
   * The top-level stream will emit chunks whenever the consumer rebalances, unless a manual subscription
   * was made. When rebalancing occurs, new topic-partition streams may be emitted and existing
   * streams may be completed.
   *
   * All streams can be completed by calling [[stopConsumption]].
   */
  def partitionedAssignmentStream[R, K, V](
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V]
  ): Stream[Throwable, Chunk[(TopicPartition, ZStream[R, Throwable, CommittableRecord[K, V]])]]

  /**
   * Create a stream with messages on the subscribed topic-partitions by topic-partition
   *
   * The top-level stream will emit new topic-partition streams for each topic-partition that is assigned
   * to this consumer. This is subject to consumer rebalancing, unless a manual subscription
   * was made. When rebalancing occurs, new topic-partition streams may be emitted and existing
   * streams may be completed.
   *
   * All streams can be completed by calling [[stopConsumption]].
   */
  def partitionedStream[R, K, V](
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V]
  ): Stream[Throwable, (TopicPartition, ZStream[R, Throwable, CommittableRecord[K, V]])]

  /**
   * Create a stream with all messages on the subscribed topic-partitions
   *
   * The stream will emit messages from all topic-partitions interleaved. Per-partition
   * record order is guaranteed, but the topic-partition interleaving is non-deterministic.
   *
   * Up to `outputBuffer` chunks may be buffered in memory by this operator.
   *
   * The stream can be completed by calling [[stopConsumption]].
   */
  def plainStream[R, K, V](
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V],
    outputBuffer: Int = 4
  ): ZStream[R, Throwable, CommittableRecord[K, V]]

  /**
   * Stops consumption of data, drains buffered records, and ends the attached
   * streams while still serving commit requests.
   */
  def stopConsumption: UIO[Unit]

  /**
   * See [[Consumer.consumeWith]].
   */
  def consumeWith[R, R1, K, V](
    subscription: Subscription,
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V],
    commitRetryPolicy: Schedule[Clock, Any, Any] = Schedule.exponential(1.second) && Schedule.recurs(3)
  )(
    f: (K, V) => URIO[R1, Unit]
  ): ZIO[R with R1, Throwable, Unit]

  def subscribe(subscripstion: Subscription): Task[Unit]

  def unsubscribe: Task[Unit]

  /**
   * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
   * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
   *
   * The consumer does not have to be assigned the partitions.
   * If no messages exist yet for a partition, it will not exist in the returned map.
   */
  def offsetsForTimes(
    timestamps: Map[TopicPartition, Long],
    timeout: Duration = Duration.Infinity
  ): Task[Map[TopicPartition, OffsetAndTimestamp]]

  def partitionsFor(topic: String, timeout: Duration = Duration.Infinity): Task[List[PartitionInfo]]

  def position(partition: TopicPartition, timeout: Duration = Duration.Infinity): Task[Long]

  def subscribeAnd(subscription: Subscription): SubscribedConsumer

  def subscription: Task[Set[String]]

  /**
   * Expose internal consumer metrics
   */
  def metrics: Task[Map[MetricName, Metric]]
}

object Consumer {

  private final case class Live(
    private val consumer: ConsumerAccess,
    private val settings: ConsumerSettings,
    private val runloop: Runloop,
    private val clock: Clock.Service
  ) extends Consumer {

    override def assignment: Task[Set[TopicPartition]] =
      consumer.withConsumer(_.assignment().asScala.toSet)

    override def beginningOffsets(
      partitions: Set[TopicPartition],
      timeout: Duration = Duration.Infinity
    ): Task[Map[TopicPartition, Long]] =
      consumer.withConsumer(
        _.beginningOffsets(partitions.asJava, timeout.asJava).asScala.view.mapValues(_.longValue()).toMap
      )

    override def committed(
      partitions: Set[TopicPartition],
      timeout: Duration = Duration.Infinity
    ): Task[Map[TopicPartition, Option[OffsetAndMetadata]]] =
      consumer.withConsumer(
        _.committed(partitions.asJava, timeout.asJava).asScala.toMap.view.mapValues(Option.apply).toMap
      )

    override def endOffsets(
      partitions: Set[TopicPartition],
      timeout: Duration = Duration.Infinity
    ): Task[Map[TopicPartition, Long]] =
      consumer.withConsumer { eo =>
        val offs = eo.endOffsets(partitions.asJava, timeout.asJava)
        offs.asScala.view.mapValues(_.longValue()).toMap
      }

    /**
     * Stops consumption of data, drains buffered records, and ends the attached
     * streams while still serving commit requests.
     */
    override def stopConsumption: UIO[Unit] =
      runloop.gracefulShutdown

    override def listTopics(timeout: Duration = Duration.Infinity): Task[Map[String, List[PartitionInfo]]] =
      consumer.withConsumer(_.listTopics(timeout.asJava).asScala.view.mapValues(_.asScala.toList).toMap)

    override def offsetsForTimes(
      timestamps: Map[TopicPartition, Long],
      timeout: Duration = Duration.Infinity
    ): Task[Map[TopicPartition, OffsetAndTimestamp]] =
      consumer.withConsumer(
        _.offsetsForTimes(timestamps.view.mapValues(Long.box).toMap.asJava, timeout.asJava).asScala.toMap
          // If a partition doesn't exist yet, the map will have 'null' as entry.
          // It's more idiomatic scala to then simply not have that map entry.
          .filter(_._2 != null)
      )

    override def partitionedAssignmentStream[R, K, V](
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V]
    ): Stream[Throwable, Chunk[(TopicPartition, ZStream[R, Throwable, CommittableRecord[K, V]])]] =
      ZStream
        .fromQueue(runloop.partitions)
        .map(_.exit)
        .flattenExitOption
        .map {
          _.map { case (tp, partition) =>
            val partitionStream =
              if (settings.perPartitionChunkPrefetch <= 0) partition
              else partition.buffer(settings.perPartitionChunkPrefetch)

            tp -> partitionStream.mapChunksM(_.mapM(_.deserializeWith(keyDeserializer, valueDeserializer)))
          }
        }

    override def partitionedStream[R, K, V](
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V]
    ): ZStream[
      Any,
      Throwable,
      (TopicPartition, ZStream[R, Throwable, CommittableRecord[K, V]])
    ] = partitionedAssignmentStream(keyDeserializer, valueDeserializer).flattenChunks

    override def partitionsFor(
      topic: String,
      timeout: Duration = Duration.Infinity
    ): Task[List[PartitionInfo]] =
      consumer.withConsumer { c =>
        val partitions = c.partitionsFor(topic, timeout.asJava)
        if (partitions eq null) List.empty else partitions.asScala.toList
      }

    override def position(partition: TopicPartition, timeout: Duration = Duration.Infinity): Task[Long] =
      consumer.withConsumer(_.position(partition, timeout.asJava))

    override def plainStream[R, K, V](
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V],
      outputBuffer: Int
    ): ZStream[R, Throwable, CommittableRecord[K, V]] =
      partitionedStream(keyDeserializer, valueDeserializer).flatMapPar(n = Int.MaxValue, outputBuffer = outputBuffer)(
        _._2
      )

    override def subscribeAnd(subscription: Subscription): SubscribedConsumer =
      new SubscribedConsumer(subscribe(subscription).as(this))

    override def subscription: Task[Set[String]] =
      consumer.withConsumer(_.subscription().asScala.toSet)

    override def consumeWith[R, R1, K, V](
      subscription: Subscription,
      keyDeserializer: Deserializer[R, K],
      valueDeserializer: Deserializer[R, V],
      commitRetryPolicy: Schedule[Clock, Any, Any] = Schedule.exponential(1.second) && Schedule.recurs(3)
    )(
      f: (K, V) => URIO[R1, Unit]
    ): ZIO[R with R1, Throwable, Unit] =
      for {
        r <- ZIO.environment[R with R1]
        _ <- ZStream
               .fromEffect(subscribe(subscription))
               .flatMap { _ =>
                 partitionedStream(keyDeserializer, valueDeserializer)
                   .flatMapPar(Int.MaxValue, outputBuffer = settings.perPartitionChunkPrefetch) {
                     case (_, partitionStream) =>
                       partitionStream.mapChunksM(_.mapM { case CommittableRecord(record, offset) =>
                         f(record.key(), record.value()).as(offset)
                       })
                   }
               }
               .provide(r)
               .aggregateAsync(offsetBatches)
               .mapM(_.commitOrRetry(commitRetryPolicy))
               .provide(Has(clock))
               .runDrain
      } yield ()

    override def subscribe(subscription: Subscription): Task[Unit] =
      ZIO.runtime[Any].flatMap { runtime =>
        consumer.withConsumerM { c =>
          subscription match {
            case Subscription.Pattern(pattern)        =>
              ZIO(c.subscribe(pattern.pattern, runloop.rebalanceListener.toKafka(runtime)))
            case Subscription.Topics(topics)          =>
              ZIO(c.subscribe(topics.asJava, runloop.rebalanceListener.toKafka(runtime)))

            // For manual subscriptions we have to do some manual work before starting the run loop
            case Subscription.Manual(topicPartitions) =>
              ZIO(c.assign(topicPartitions.asJava)) *>
                ZIO.foreach(topicPartitions)(runloop.newPartitionStream).flatMap { partitionStreams =>
                  runloop.partitions.offer(
                    Take.chunk(
                      Chunk.fromIterable(partitionStreams.map { case (tp, _, stream) =>
                        tp -> stream
                      })
                    )
                  )
                } *> {
                  settings.offsetRetrieval match {
                    case OffsetRetrieval.Manual(getOffsets) =>
                      getOffsets(topicPartitions).flatMap { offsets =>
                        ZIO.foreach_(offsets) { case (tp, offset) => ZIO(c.seek(tp, offset)) }
                      }
                    case OffsetRetrieval.Auto(_)            => ZIO.unit
                  }
                }
          }
        }
      }

    override def unsubscribe: Task[Unit] =
      consumer.withConsumer(_.unsubscribe())

    override def metrics: Task[Map[MetricName, Metric]] =
      consumer.withConsumer(_.metrics().asScala.toMap)
  }

  val offsetBatches: ZTransducer[Any, Nothing, Offset, OffsetBatch] =
    ZTransducer.foldLeft[Offset, OffsetBatch](OffsetBatch.empty)(_ merge _)

  def live: RLayer[Clock with Blocking with Has[ConsumerSettings] with Has[Diagnostics], Has[Consumer]] =
    (for {
      settings    <- ZManaged.service[ConsumerSettings]
      diagnostics <- ZManaged.service[Diagnostics]
      consumer    <- make(settings, diagnostics)
    } yield consumer).toLayer

  def make(
    settings: ConsumerSettings,
    diagnostics: Diagnostics = Diagnostics.NoOp
  ): RManaged[Clock with Blocking, Consumer] =
    for {
      wrapper <- ConsumerAccess.make(settings)
      runloop <- Runloop(
                   wrapper,
                   settings.pollInterval,
                   settings.pollTimeout,
                   diagnostics,
                   settings.offsetRetrieval
                 )
      clock   <- ZManaged.service[Clock.Service]
    } yield Live(wrapper, settings, runloop, clock)

  /**
   * Accessor method for [[Service.assignment]]
   */
  def assignment: RIO[Has[Consumer], Set[TopicPartition]] =
    ZIO.serviceWith(_.assignment)

  /**
   * Accessor method for [[Service.beginningOffsets]]
   */
  def beginningOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], Map[TopicPartition, Long]] =
    ZIO.serviceWith(_.beginningOffsets(partitions, timeout))

  /**
   * Accessor method for [[Service.committed]]
   */
  def committed(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], Map[TopicPartition, Option[OffsetAndMetadata]]] =
    ZIO.serviceWith(_.committed(partitions, timeout))

  /**
   * Accessor method for [[Service.endOffsets]]
   */
  def endOffsets(
    partitions: Set[TopicPartition],
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], Map[TopicPartition, Long]] =
    ZIO.serviceWith(_.endOffsets(partitions, timeout))

  /**
   * Accessor method for [[Service.listTopics]]
   */
  def listTopics(
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], Map[String, List[PartitionInfo]]] =
    ZIO.serviceWith(_.listTopics(timeout))

  /**
   * Accessor method for [[Service.partitionedStream]]
   */
  def partitionedStream[R, K, V](
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V]
  ): ZStream[
    Has[Consumer],
    Throwable,
    (TopicPartition, ZStream[R, Throwable, CommittableRecord[K, V]])
  ] =
    ZStream.accessStream(_.get[Consumer].partitionedStream(keyDeserializer, valueDeserializer))

  /**
   * Accessor method for [[Service.plainStream]]
   */
  def plainStream[R, K, V](
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V],
    outputBuffer: Int = 4
  ): ZStream[R with Has[Consumer], Throwable, CommittableRecord[K, V]] =
    ZStream.accessStream(_.get[Consumer].plainStream(keyDeserializer, valueDeserializer, outputBuffer))

  /**
   * Accessor method for [[Service.stopConsumption]]
   */
  def stopConsumption: RIO[Has[Consumer], Unit] =
    ZIO.serviceWith(_.stopConsumption)

  /**
   * Execute an effect for each record and commit the offset after processing
   *
   * This method is the easiest way of processing messages on a Kafka topic.
   *
   * Messages on a single partition are processed sequentially, while the processing of
   * multiple partitions happens in parallel.
   *
   * Offsets are committed after execution of the effect. They are batched when a commit action is in progress
   * to avoid backpressuring the stream. When commits fail due to a org.apache.kafka.clients.consumer.RetriableCommitFailedException they are
   * retried according to commitRetryPolicy
   *
   * The effect should absorb any failures. Failures should be handled by retries or ignoring the
   * error, which will result in the Kafka message being skipped.
   *
   * Messages are processed with 'at least once' consistency: it is not guaranteed that every message
   * that is processed by the effect has a corresponding offset commit before stream termination.
   *
   * Usage example:
   *
   * {{{
   * val settings: ConsumerSettings = ???
   * val subscription = Subscription.Topics(Set("my-kafka-topic"))
   *
   * val consumerIO = Consumer.consumeWith(settings, subscription, Serdes.string, Serdes.string) { case (key, value) =>
   *   // Process the received record here
   *   putStrLn(s"Received record: \${key}: \${value}")
   * }
   * }}}
   *
   * @param settings Settings for creating a [[Consumer]]
   * @param subscription Topic subscription parameters
   * @param keyDeserializer Deserializer for the key of the messages
   * @param valueDeserializer Deserializer for the value of the messages
   * @param commitRetryPolicy Retry commits that failed due to a RetriableCommitFailedException according to this schedule
   * @param f Function that returns the effect to execute for each message. It is passed the key and value
   * @tparam R Environment for the consuming effect
   * @tparam R1 Environment for the deserializers
   * @tparam K Type of keys (an implicit `Deserializer` should be in scope)
   * @tparam V Type of values (an implicit `Deserializer` should be in scope)
   * @return Effect that completes with a unit value only when interrupted. May fail when the [[Consumer]] fails.
   */
  def consumeWith[R, R1, K, V](
    settings: ConsumerSettings,
    subscription: Subscription,
    keyDeserializer: Deserializer[R, K],
    valueDeserializer: Deserializer[R, V],
    commitRetryPolicy: Schedule[Clock, Any, Any] = Schedule.exponential(1.second) && Schedule.recurs(3)
  )(f: (K, V) => URIO[R1, Unit]): RIO[R with R1 with Blocking with Clock, Unit] =
    Consumer
      .make(settings)
      .use(_.consumeWith(subscription, keyDeserializer, valueDeserializer, commitRetryPolicy)(f))

  /**
   * Accessor method for [[Service.subscribe]]
   */
  def subscribe(subscription: Subscription): RIO[Has[Consumer], Unit] =
    ZIO.serviceWith(_.subscribe(subscription))

  /**
   * Accessor method for [[Service.unsubscribe]]
   */
  def unsubscribe: RIO[Has[Consumer], Unit] =
    ZIO.serviceWith(_.unsubscribe)

  /**
   * Accessor method for [[Service.offsetsForTimes]]
   */
  def offsetsForTimes(
    timestamps: Map[TopicPartition, Long],
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], Map[TopicPartition, OffsetAndTimestamp]] =
    ZIO.serviceWith(_.offsetsForTimes(timestamps, timeout))

  /**
   * Accessor method for [[Service.partitionsFor]]
   */
  def partitionsFor(
    topic: String,
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], List[PartitionInfo]] =
    ZIO.serviceWith(_.partitionsFor(topic, timeout))

  /**
   * Accessor method for [[Service.position]]
   */
  def position(
    partition: TopicPartition,
    timeout: Duration = Duration.Infinity
  ): RIO[Has[Consumer], Long] =
    ZIO.serviceWith(_.position(partition, timeout))

  /**
   * Accessor method for [[Service.subscribeAnd]]
   */
  def subscribeAnd(
    subscription: Subscription
  ): SubscribedConsumerFromEnvironment =
    new SubscribedConsumerFromEnvironment(
      ZIO.accessM { env =>
        val consumer = env.get[Consumer]
        consumer.subscribe(subscription).as(consumer)
      }
    )

  /**
   * Accessor method for [[Service.subscription]]
   */
  def subscription: RIO[Has[Consumer], Set[String]] =
    ZIO.serviceWith(_.subscription)

  /**
   * Accessor method for [[Service.metrics]]
   */
  def metrics: RIO[Has[Consumer], Map[MetricName, Metric]] =
    ZIO.serviceWith(_.metrics)

  sealed trait OffsetRetrieval

  object OffsetRetrieval {
    final case class Auto(reset: AutoOffsetStrategy = AutoOffsetStrategy.Latest)                extends OffsetRetrieval
    final case class Manual(getOffsets: Set[TopicPartition] => Task[Map[TopicPartition, Long]]) extends OffsetRetrieval
  }

  sealed trait AutoOffsetStrategy { self =>
    def toConfig: String = self match {
      case AutoOffsetStrategy.Earliest => "earliest"
      case AutoOffsetStrategy.Latest   => "latest"
      case AutoOffsetStrategy.None     => "none"
    }
  }

  object AutoOffsetStrategy {
    case object Earliest extends AutoOffsetStrategy
    case object Latest   extends AutoOffsetStrategy
    case object None     extends AutoOffsetStrategy
  }
}
