package me.yohohaha.demo.flink

import java.util.Properties

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * created at 2019/11/07 14:06:19
 *
 * @author Yohohaha
 */
object SlidingWindowJoin {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val sensorTempratureConsumer = new FlinkKafkaConsumer[String]("sensor-temprature", new SimpleStringSchema, properties)
    val sensorCityConsumer = new FlinkKafkaConsumer[String]("sensor-city", new SimpleStringSchema, properties)
    sensorTempratureConsumer.setStartFromEarliest()
    sensorCityConsumer.setStartFromEarliest()

    val sensorTempratureSource =
      env.addSource(sensorTempratureConsumer)
        .map(value => {
          val splits = value.split(",")
          (splits(0), splits(1).toLong, splits(2).toInt)
        })
        .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long, Int)] {
          override def checkAndGetNextWatermark(lastElement: (String, Long, Int), extractedTimestamp: Long) = new Watermark(extractedTimestamp)

          override def extractTimestamp(element: (String, Long, Int), previousElementTimestamp: Long) = element._2
        })
    sensorTempratureSource.print()

    val sensorCitySource = env.addSource(sensorCityConsumer)
      .map(value => {
        val splits = value.split(",")
        (splits(0), splits(1).toLong, splits(2))
      })
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long, String)] {
        override def checkAndGetNextWatermark(lastElement: (String, Long, String), extractedTimestamp: Long) = new Watermark(extractedTimestamp)

        override def extractTimestamp(element: (String, Long, String), previousElementTimestamp: Long) = element._2
      })

    sensorCitySource.print()


    sensorTempratureSource
      .join(sensorCitySource)
      .where(_._1)
      .equalTo(_._1)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(2), Time.milliseconds(1)))
      .apply(new JoinFunction[(String, Long, Int), (String, Long, String), (String, String, Int)] {
        override def join(first: (String, Long, Int), second: (String, Long, String)) = (first._1, second._3, first._3)
      })
      .print("sensor info")

    env.execute("Sliding window join")
  }

}
