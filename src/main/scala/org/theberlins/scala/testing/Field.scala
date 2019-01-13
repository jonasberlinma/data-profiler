package org.theberlins.scala.testing

import java.io.PrintWriter
import java.io.PrintStream

class Field(val datasetName: String, val fieldName: String, var fieldNumber: Int) extends GraphComponent {

  object AnalysisType extends Enumeration {
    type AnalysisType = Value
    val STRING, NUMBER, DATE, UNKNOWN = Value
  }

  var isSuspectedPrimaryKey: Boolean = false
  var theType: AnalysisType.Value = AnalysisType.UNKNOWN

  val keyFraction: Double = 0.97

  val typeFraction: Double = 0.97

  var fieldStats: SummaryResult = new SummaryResult

  var distinctCount: Long = 0

  var topCounts: Array[(String, Long)] = new Array[(String, Long)](0)
  var topPatterns: Array[(String, Long)] = new Array[(String, Long)](0)

  var distinctPatternCount: Long = 0

  def guesstimateKey(): Unit = {
    if (distinctCount > fieldStats.totalCount * keyFraction) {
      isSuspectedPrimaryKey = true
    }
  }

  def guesstimateType(): Unit = {

    if ((fieldStats.doubleCount + fieldStats.blankCount) > fieldStats.totalCount * typeFraction) {
      theType = AnalysisType.NUMBER
    } else if ((fieldStats.dateCount + fieldStats.blankCount) > fieldStats.totalCount * typeFraction) {
      theType = AnalysisType.DATE
    } else {
      theType = AnalysisType.STRING
    }
  }
  def hasStringSizeOverlap(other: Field): Boolean = {
    var ret: Boolean = true
    if (fieldStats.stringMaxLength < other.fieldStats.stringMinLength ||
      fieldStats.stringMinLength > other.fieldStats.stringMaxLength)
      ret = false
    ret
  }
  def isSameType(other: Field): Boolean = {
    theType.equals(other.theType)
  }
  def hasPatternOverlap(other: Field): Boolean = {
    other.topPatterns.map(_._1) contains topPatterns(0)._1
  }
  def hasNameOverlap(other: Field) : Boolean = {
    fieldName.compareTo(other.fieldName) == 0
  }
  def compareTo(other: Field): Boolean = {
    datasetName.compareTo(other.datasetName) == 0 && fieldName.compareTo(other.fieldName) == 0
  }
  def printAll(): Unit = {
    println("Field name : " + fieldName)
    println("Type : " + theType)
    println("Field number : " + fieldNumber)
    fieldStats.printAll()
    println("Distinct count : " + distinctCount)
    println("Top values with counts")
    topCounts.iterator.foreach(println)
    println("Pattern count : " + distinctPatternCount)
    println("Top patterns with counts")
    topPatterns.iterator.foreach(println)
  }
  def writeGraph(datasetUUID: String, nodeFile: PrintStream, edgeFile: PrintStream): Unit = {
    nodeFile.println(uuid + "," + fieldName + ",F")
    edgeFile.println(uuid + "," + datasetUUID + ",DF,1.0")
    fieldStats.writeGraph(datasetUUID, nodeFile, edgeFile)
  }
}