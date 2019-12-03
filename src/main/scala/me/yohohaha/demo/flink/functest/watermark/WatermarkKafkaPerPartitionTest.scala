package me.yohohaha.demo.flink.functest.watermark

import java.util.Properties

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
object WatermarkKafkaPerPartitionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val watermarkConsumer = new FlinkKafkaConsumer[ExtractedData]("watermark-test-3", new ExtractedDataDeserializationSchema, properties)
    watermarkConsumer.setStartFromLatest()
    // 在kafka consumer上加了watermark之后，不受env并行度控制
    watermarkConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ExtractedData](Time.milliseconds(1)) {
      override def extractTimestamp(element: ExtractedData): Long = {
        println(s"partition=${element.kafkaData.partition} | offset=${element.kafkaData.offset} | value=${element.kafkaData.value} | emit timestamp=${element.timestamp}")
        element.timestamp
      }
    })

    env.addSource(watermarkConsumer)
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

    env.execute("watermark kafka per partition test")
  }
}
