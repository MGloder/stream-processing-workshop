package com.machinedoll.projectdemo.jobs.gdelt.source

import com.machinedoll.projectdemo.schema.Export
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.util.Try

case class EventSplitter() extends FlatMapFunction[String, Export]{
  def Int(s: String) = Try(s.toInt).toOption

  def String(s: String) = Try(s).toOption

  def Float(s: String) = Try(s.toFloat).toOption

  override def flatMap(in: String, out: Collector[Export]): Unit = {
//    out.collect(new Export(Int("1")))
    out.collect(parseExport(in))
  }

  def parseExport(line: String): Export = {
    val trimString = line.trim
    val t = trimString.split("\\t")
    new Export(Some(t(0).toInt),
      Int(t(1)),
      Int(t(2)),
      Int(t(3)),
      Float(t(4)),
      String(t(5)),
      String(t(6)),
      String(t(7)),
      String(t(8)),
      String(t(9)),
      String(t(10)),
      String(t(11)),
      String(t(12)),
      String(t(13)),
      String(t(14)),
      String(t(15)),
      String(t(16)),
      String(t(17)),
      String(t(18)),
      String(t(19)),
      String(t(20)),
      String(t(21)),
      String(t(22)),
      String(t(23)),
      String(t(24)),
      Int(t(25)),
      String(t(26)),
      String(t(27)),
      String(t(28)),
      Int(t(29)),
      Float(t(30)),
      Int(t(31)),
      Int(t(32)),
      Int(t(33)),
      Float(t(34)),
      Int(t(35)),
      String(t(36)),
      String(t(37)),
      String(t(38)),
      String(t(39)),
      Float(t(40)),
      Float(t(41)),
      String(t(42)),
      Int(t(43)),
      String(t(44)),
      String(t(45)),
      String(t(46)),
      String(t(47)),
      Float(t(48)),
      Float(t(49)),
      String(t(50)),
      Int(t(51)),
      String(t(52)),
      String(t(53)),
      String(t(54)),
      String(t(55)),
      Float(t(56)),
      Float(t(57)),
      String(t(58)),
      Int(t(59)),
      String(t(60))
    )
  }
}
