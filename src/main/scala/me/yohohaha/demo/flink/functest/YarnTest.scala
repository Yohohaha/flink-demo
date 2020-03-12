package me.yohohaha.demo.flink.functest

import java.util.Properties

import com.github.yohohaha.java.util.{ExceptionUtils, IOUtils}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

/**
 * created at 2019/11/26 13:58:45
 *
 * @author Yohohaha
 */
object YarnTest {
  private[this] lazy val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val argParas = ParameterTool.fromArgs(args)
    val runConfigurationPath = argParas.get("run.configuration")
    LOG.info(s"runConfigurationPath=$runConfigurationPath")
    val runProperties = try {
      IOUtils.readProperties(runConfigurationPath)
    } catch {
      case e => {
        LOG.warn(ExceptionUtils.getExceptionInfo(e))
        new Properties()
      }
    }
    val paras = ParameterTool.fromMap(runProperties.asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]).mergeWith(argParas)

    val l = paras.getRequired("source.elems").split(",")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new SourceFunction[String] {
      private var isRunning = true

      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        while (isRunning) {
          for (elem <- l) {
            ctx.collect(elem)
            LOG.info(s"source: ${elem}")
            Thread.sleep(3000)
          }
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })
      .map(_.toInt)
      .print()

    env.execute("flink test")
  }
}
