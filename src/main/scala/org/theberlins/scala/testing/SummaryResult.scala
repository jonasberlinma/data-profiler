package org.theberlins.scala.testing

import java.io.PrintStream


// This class has to be serializable since it will be sent between nodes during aggregation
class SummaryResult extends Serializable with GraphComponent {

  var minValue: Option[String] = None
  var maxValue: Option[String] = None
  var intCount: Long = 0
  var doubleCount: Long = 0
  var stringCount: Long = 0
  var dateCount: Long = 0
  var blankCount: Long = 0
  var totalCount: Long = 0

  var stringMaxLength: Long = 0
  var stringMinLength: Long = Long.MaxValue


  def printAll(): Unit = {
    println("Min " + minValue.getOrElse(""))
    println("Max " + maxValue.getOrElse(""))
    println("Total count " + totalCount)
    println("Int count " + intCount)
    println("Double count " + doubleCount)
    println("String count " + stringCount)
    println("Date count " + dateCount)
    println("String min length " + stringMinLength)
    println("String max length " + stringMaxLength)
    println("Blank count " + blankCount)


  }
  def writeGraph(fieldUUID:String, nodeFile:PrintStream, edgeFile:PrintStream) : Unit = {
    
  }
}