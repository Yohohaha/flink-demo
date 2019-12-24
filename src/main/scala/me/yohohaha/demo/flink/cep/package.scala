package me.yohohaha.demo.flink

/**
 * created by Yohohaha at 2019/12/04 14:31:57
 */
package object cep {

  class Event(v: String) {
    val value: String = v

    override def toString: String = value
  }

  class SubEvent(v: String) extends Event(v: String)

}
