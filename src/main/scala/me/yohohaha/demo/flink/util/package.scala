package me.yohohaha.demo.flink

/**
 * created by Yohohaha at 2019/12/07 14:03:35
 */
package object util {
  def judgeAndDo[T](in: T)(test: T => Boolean)(func: T => Unit) {
    if (test.apply(in)) {
      func(in)
    }
  }
}
