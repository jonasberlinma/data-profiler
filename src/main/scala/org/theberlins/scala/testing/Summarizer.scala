package org.theberlins.scala.testing

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.Text
import scala.collection.mutable.ArrayBuffer
import datasets.RawDataset
import scala.util.matching.Regex

object Summarizer {

  val datePatterns: Array[Regex] = Array(
    """(\d\d\d\d)[-\/](\d\d)[-\/](\d\d)""".r,
    """(\d\d\d\d)(\d\d)(\d\d)""".r,
    """(\d\d)[-\/](\d\d)[-\/](\d\d\d\d)""".r,
    """(\d\d)(\d\d)(\d\d\d\d)""".r)

  def summarize(dataset: DecoratedDataset): Unit = {

    for (i <- 0 to dataset.fields.size - 1) {
      val field = dataset.fields(i)
      println("Processing stats of field " + dataset.fields(i).fieldName)
      System.out.flush()
      val column = dataset.rdd
        .map(_(i))
        .map(_.trim())
        .persist()

      val result = column.aggregate(new SummaryResult)(
        seqSummarize,
        combSummarize)

      field.distinctCount = column.distinct().count()

      field.fieldStats = result

      field.topCounts = column
        .filter(!_.isEmpty())
        .map((x) => (x, 1L))
        .reduceByKey(_ + _, 1)
        .map(item => item.swap)
        .sortByKey(false, 3)
        .map(item => item.swap).take(10)

      // Find match patterns
      val patterns = column
        .filter(!_.isEmpty())
        .map((x) => (x.replaceAll("[A-Z]", "C")
          .replaceAll("[a-z]", "c")
          .replaceAll("[0-9]", "9")))

      val distinctPatterns = patterns
        .map((x) => (x, 1L))
        .reduceByKey(_ + _, 1)
        .map(item => item.swap)
        .sortByKey(false, 3)
        .map(item => item.swap).collect()
      field.topPatterns = distinctPatterns.take(100)
      field.distinctPatternCount = distinctPatterns.count(!_._1.isEmpty())
      column.unpersist()
    }
  }
  def seqSummarize(x: SummaryResult, value: String): SummaryResult = {
    x.minValue = Some(if (!value.isEmpty() && value.compareTo(x.minValue.getOrElse(value)) < 0) { value } else { x.minValue.getOrElse(value) })
    x.maxValue = Some(if (!value.isEmpty() && value.compareTo(x.maxValue.getOrElse(value)) > 0) { value } else { x.maxValue.getOrElse(value) })

    x.stringMinLength = if (!value.isEmpty() && x.stringMinLength > value.length()) { value.length } else (x.stringMinLength)
    x.stringMaxLength = if (x.stringMaxLength < value.length()) { value.length } else (x.stringMaxLength)

    try {
      val theInt: Int = value.toInt
      x.intCount += 1
    } catch {
      case e: Exception => {}
    }

    try {
      val theDouble: Double = value.toDouble
      x.doubleCount += 1
    } catch {
      case e: Exception => {}
    }

    datePatterns.foreach {
      pattern =>
        value match {
          case pattern(year, month, date) => { x.dateCount += 1 }
          case _ => {}
        }
    }

    if (value.trim().isEmpty()) {
      x.blankCount += 1
    }
    x.stringCount += 1
    x.totalCount += 1

    x
  }
  def combSummarize(x: SummaryResult, y: SummaryResult): SummaryResult = {

    x.minValue = (x.minValue, y.minValue) match {
      case (Some(xmin), Some(ymin)) => if (xmin.compareTo(ymin) < 0) { Some(xmin) } else { Some(ymin) }
      case (Some(xmin), None) => Some(xmin)
      case (None, Some(ymin)) => Some(ymin)
      case (None, None) => None
    }

    x.maxValue = (x.maxValue, y.maxValue) match {
      case (Some(xmax), Some(ymax)) => if (xmax.compareTo(ymax) > 0) { Some(xmax) } else { Some(ymax) }
      case (Some(xmax), None) => Some(xmax)
      case (None, Some(ymax)) => Some(ymax)
      case (None, None) => None
    }

    x.stringMinLength = if (x.stringMinLength < y.stringMinLength) { x.stringMinLength } else (y.stringMinLength)
    x.stringMaxLength = if (x.stringMaxLength > y.stringMaxLength) { x.stringMaxLength } else (y.stringMaxLength)

    x.intCount += y.intCount
    x.doubleCount += y.doubleCount
    x.stringCount += y.stringCount
    x.dateCount += y.dateCount

    x.blankCount += y.blankCount
    x.totalCount += y.totalCount

    x
  }

}