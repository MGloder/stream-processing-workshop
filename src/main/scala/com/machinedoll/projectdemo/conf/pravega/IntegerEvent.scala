package com.machinedoll.projectdemo.conf.pravega

class IntegerEvent(v: Long) extends Serializable {
  val start: Long = -1
  val end: Long = -9999

  private val value = v

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (value ^ (value >>> 32)).asInstanceOf[Int]
    result
  }

  override def equals(obj: Any): Boolean = {
    if (this == obj)
      return true
    if (!obj.isInstanceOf[IntegerEvent]) {
      return false
    }
    val event: IntegerEvent = obj.asInstanceOf[IntegerEvent]

    event.value == value
  }


  override def toString: String = "Integer Event" + value
}
