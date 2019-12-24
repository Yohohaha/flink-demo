package me.yohohaha.demo.flink.cep

import java.util
import java.util.stream.Collectors

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * created at 2019/12/24 15:54:31
 *
 * @author Yohohaha
 */
object ContiguityWithinLoopingPatternsDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.fromElements("a", "b1", "d1", "b2", "d2", "b3", "c")

    val pattern = Pattern
      .begin[String]("start").where(_ startsWith "a")
      .followedByAny("middle").where(_ startsWith "b").timesOrMore(1).allowCombinations()
      .followedBy("end").where(_ startsWith "c")

    val patternStream = CEP.pattern(inputStream, pattern)

    patternStream
      .process(new PatternProcessFunction[String, String] {
        override def processMatch(`match`: util.Map[String, util.List[String]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
          val events = `match`.get("start")
          events.addAll(`match`.get("middle"))
          events.addAll(`match`.get("end"))

          out.collect(events.stream().collect(Collectors.joining(" ")))
        }
      })
      .print("result")


    env.execute("contiguity within looping patterns demo")
  }
}
