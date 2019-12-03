package me.yohohaha.demo.flink.functest.watermark

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}

/**
 * created at 2019/12/02 16:45:05
 *
 * @author Yohohaha
 */
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val watermarkConsumer = new FlinkKafkaConsumer[KafkaData]("watermark-test-3", new MyKafkaDataDeserializationSchema, properties)
    watermarkConsumer.setStartFromLatest()

    env.addSource(watermarkConsumer)


    val wm = env.addSource(watermarkConsumer)
      .map(kafkaData => {
        val splits = kafkaData.value.split(",")
        ExtractedData(kafkaData, splits(0), splits(1).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ExtractedData](Time.milliseconds(1)) {
        override def extractTimestamp(element: ExtractedData): Long = {
          println(s"partition=${element.kafkaData.partition} | offset=${element.kafkaData.offset} | value=${element.kafkaData.value} | emit timestamp=${element.timestamp}")
          element.timestamp
        }
      })
      .map(e => {
        (e.value, 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(1))
      .aggregate(new CountAggreate, new ProcessWindowFunction[Long, (String, Long, String), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[(String, Long, String)]): Unit = {
          val element = elements.iterator.next
          out.collect((key, element, s"watermark=${context.currentWatermark} window=[${context.window.getStart}, ${context.window.getEnd - 1}]"))
        }
      }).print("result").setParallelism(1)

    env.execute("watermark test")
  }
}

case class ExtractedData(kafkaData: KafkaData, value: String, timestamp: Long)

case class KafkaData(partition: Int, timestamp: Long, offset: Long, key: String, value: String)

class MyKafkaDataDeserializationSchema extends KafkaDeserializationSchema[KafkaData] {
  override def isEndOfStream(nextElement: KafkaData): Boolean = false

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaData = {
    KafkaData(record.partition, record.timestamp, record.offset, Option(record.key).map(v => new String(v, StandardCharsets.UTF_8)).orNull, new String(record.value, StandardCharsets.UTF_8))
  }

  override def getProducedType: TypeInformation[KafkaData] = TypeInformation.of(new TypeHint[KafkaData] {})
}

class CountAggreate extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = value._2 + accumulator

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
  override def createAccumulator(): (Long, Long) = (0L, 0L)

  override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) =
    (accumulator._1 + value._2, accumulator._2 + 1L)

  override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2

  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) =
    (a._1 + b._1, a._2 + b._2)
}