package me.yohohaha.demo.flink.functest.watermark

import java.nio.charset.StandardCharsets
import java.util.Properties

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * created at 2019/12/02 23:38:46
 *
 * @author Yohohaha
 */
object WatermarkTestKafkaPerPartition {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val watermarkConsumer = new FlinkKafkaConsumer[(String, Long)]("watermark-test-3", new MyTestDataSchema, properties)
    watermarkConsumer.setStartFromLatest()
    watermarkConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[(String, Long)] {
      override def extractAscendingTimestamp(element: (String, Long)): Long = element._2
    })

    env.addSource(watermarkConsumer)
      .map(e => (e._1, 1L))
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


class MyTestDataSchema extends DeserializationSchema[(String, Long)] {
  override def deserialize(message: Array[Byte]): (String, Long) = {
    val splits = new String(message, StandardCharsets.UTF_8).split(",")
    (splits(0), splits(1).toLong)
  }

  override def isEndOfStream(nextElement: (String, Long)): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[(String, Long)] = {
    TypeInformation.of(new TypeHint[(String, Long)] {})
  }
}