package me.yohohaha.demo.flink.functest.watermark

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

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
    val watermarkConsumer = new FlinkKafkaConsumer[String]("watermark-test-3", new SimpleStringSchema, properties)
    watermarkConsumer.setStartFromLatest()

    val wm = env.addSource(watermarkConsumer)
      .map(s => {
        val splits = s.split(",")
        (splits(0), splits(1).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.milliseconds(1)) {
        override def extractTimestamp(element: (String, Long)): Long = element._2
      }).setParallelism(1)
    wm.map(e => (e._1, 1L))
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(3), Time.milliseconds(1))
      .aggregate(new CountAggreate, new ProcessWindowFunction[Long, (String, Long, String), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[(String, Long, String)]): Unit = {
          val element = elements.iterator.next
          out.collect((key, element, s"watermark:${context.currentWatermark} window:${context.window.getStart} to ${context.window.getEnd}"))
        }
      }).print("result")

    env.execute("watermark test")
  }
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