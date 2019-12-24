package me.yohohaha.demo.flink.cep

import java.util
import java.util.function.Consumer

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * created at 2019/12/04 14:29:49
 *
 * @author Yohohaha
 */
object SimpleCepDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val inputEventStream = env.fromElements("a", "b:sub", "c")
      .map(s => {
        if (s.contains("sub")) {
          new SubEvent(s)
        } else {
          new Event(s)
        }
      })

    inputEventStream.print("input")


    val pattern = Pattern
      .begin[Event]("start").where(_.value startsWith "a")
      .next("middle").subtype(classOf[SubEvent]).where(_.value startsWith "b")
      .followedBy("end").where(_.value startsWith "c")


    val patternStream = CEP.pattern(inputEventStream, pattern)

    patternStream.process(new PatternProcessFunction[Event, Event] {
      override def processMatch(`match`: util.Map[String, util.List[Event]], ctx: PatternProcessFunction.Context, out: Collector[Event]): Unit = {
        val events = `match`.get("start")
        events.addAll(`match`.get("middle"))
        events.addAll(`match`.get("end"))
        events.forEach(new Consumer[Event] {
          override def accept(t: Event): Unit = {
            out.collect(t)
          }
        })
      }
    })
      .print("result")

    env.execute("simple cep demo")
  }
}
