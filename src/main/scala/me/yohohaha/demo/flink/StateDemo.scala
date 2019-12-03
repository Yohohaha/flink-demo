package me.yohohaha.demo.flink

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * created at 2019/11/27 16:59:26
 *
 * @author Yohohaha
 */
object StateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements((1, 3), (1, 5), (1, 7), (1, 4), (1, 2))
      .keyBy(0)
      .flatMap(new RichFlatMapFunction[(Int, Int), (Int, Int)] {
        private var sum: ValueState[(Int, Int)] = _

        override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
          val tmpSum = sum.value()
          val currentSum = if (tmpSum == null) {
            (0, 0)
          } else {
            tmpSum
          }
          val newSum = (currentSum._1 + 1, currentSum._2 + value._2)

          sum.update(newSum)

          if (newSum._1 > 2) {
            out.collect((value._1, (newSum._2 / newSum._1)))
            sum.clear()
          }
        }

        override def open(parameters: Configuration): Unit = {
          sum = getRuntimeContext.getState(
            new ValueStateDescriptor[(Int, Int)]("avg", createTypeInformation[(Int, Int)])
          )
        }
      }).print("result")

    env.execute("state demo")
  }
}
