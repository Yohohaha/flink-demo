package me.yohohaha.demo.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.checkpoint.CheckpointFailureManager
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * created at 2019/11/18 16:01:04
 *
 * @author Yohohaha
 */
object WordCount {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(CheckpointFailureManager.UNLIMITED_TOLERABLE_FAILURE_NUMBER)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)


    env.setStateBackend(new FsStateBackend("file:///c:/flink/checkpoints").asInstanceOf[StateBackend])

    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test")
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val wordcountConsumer = new FlinkKafkaConsumer[String]("wordcount", new SimpleStringSchema, properties)
    //wordcountConsumer.setStartFromEarliest()


    val wordSource = env.addSource(wordcountConsumer)

    def t(arr: Array[String]) = {
      (arr(0).toLong, arr(1))
    }

    val result = wordSource.flatMap(_.split(" "))
      .map(_.split(","))
      .map(arr => (arr(0).toLong, arr(1)))
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(Long, String)] {
        override def checkAndGetNextWatermark(lastElement: (Long, String), extractedTimestamp: Long) = new Watermark(extractedTimestamp)

        override def extractTimestamp(element: (Long, String), previousElementTimestamp: Long) = element._1
      })
      .map(_._2)
      .map((_, 1))
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(5))
      .sum(1)
    wordSource.print("words")

    result.print("result")

    env.execute("word count test")
  }
}
