package me.yohohaha.demo.flink.cep

import java.util
import java.util.stream.Collectors

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * created at 2019/12/24 17:40:01
 *
 * @author Yohohaha
 */
object ContiguityDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputStream = env.fromElements("a", "c", "b1", "b2")

    val pattern = Pattern
      .begin[String]("start").where(_ startsWith "a")
      .followedBy("end").where(_ startsWith "b")

    val patternStream = CEP.pattern(inputStream, pattern)

    patternStream.process(new PatternProcessFunction[String, String] {
      override def processMatch(`match`: util.Map[String, util.List[String]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
        val events = `match`.get("start")
        events.addAll(`match`.get("end"))

        out.collect(events.stream().collect(Collectors.joining(" ")))
      }

    }).print("result")

    env.execute("contiguity demo")
  }
}
