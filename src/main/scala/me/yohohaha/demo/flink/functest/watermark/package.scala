package me.yohohaha.demo.flink.functest

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
 * created by Yohohaha at 2019/12/04 00:16:51
 */
package object watermark {


  case class ExtractedData(kafkaData: KafkaData, value: String, timestamp: Long)

  case class KafkaData(partition: Int, timestamp: Long, offset: Long, key: String, value: String)

  class ExtractedDataDeserializationSchema extends KafkaDeserializationSchema[ExtractedData] {
    override def isEndOfStream(nextElement: ExtractedData): Boolean = false

    override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): ExtractedData = {
      val kafkaData = KafkaData(record.partition, record.timestamp, record.offset, Option(record.key).map(v => new String(v, StandardCharsets.UTF_8)).orNull, new String(record.value, StandardCharsets.UTF_8))
      val splits = kafkaData.value.split(",")
      ExtractedData(kafkaData, splits(0), splits(1).toLong)
    }

    override def getProducedType: TypeInformation[ExtractedData] = TypeInformation.of(new TypeHint[ExtractedData] {})
  }

  class KafkaDataDeserializationSchema extends KafkaDeserializationSchema[KafkaData] {
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

}
