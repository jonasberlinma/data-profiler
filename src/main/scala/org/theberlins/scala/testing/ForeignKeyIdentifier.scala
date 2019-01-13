package org.theberlins.scala.testing

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.HashPartitioner

object ForeignKeyIdentifier {

  val matchTolerance = 0.99

  def findForeignKeys(dataset1: DecoratedDataset, dataset2: DecoratedDataset): Array[FieldRelation] = {
    val relations = new ArrayBuffer[FieldRelation]()

    for (i <- 0 to dataset1.fields.size - 1) {
      val field1 = dataset1.fields(i)
      for (j <- 0 to dataset2.fields.size - 1) {
        val field2 = dataset2.fields(j)

        if (field1.isSuspectedPrimaryKey &&
          field1.isSameType(field2) &&
          field1.hasPatternOverlap(field2) &&
          field1.hasNameOverlap(field2)) {
          // Join the two together

          println("Checking " + dataset1.path + "." + field1.fieldName + "->" + dataset2.path + "." + field2.fieldName)

          val matched = dataset1.rdd
            .map((x) => (if (!x(i).isEmpty()) { x(i) } else { "Missing Left" }, 1))
            .partitionBy(new HashPartitioner(8))
            .fullOuterJoin(dataset2.rdd.map((x) => (if (!x(j).isEmpty()) { x(j) } else { "Missing Right" }, 1))
              .partitionBy(new HashPartitioner(8)))

          val matchCount = matched.filter((x) => (x._2._1, x._2._2) match {
            case (Some(x), Some(y)) => true
            case _ => false
          }).count()
          val missingChildCount = matched.filter((_._2._2 match {
            case None => true
            case _ => false
          })).count()
          val missingParentCount = matched.filter((_._2._1 match {
            case None => true
            case _ => false
          })).distinct().count()
          println("Match stats Match : " + matchCount + " Total : " + field2.fieldStats.totalCount + " Blank :" + field2.fieldStats.blankCount)
          if (matchCount.toDouble > (field2.fieldStats.totalCount.toDouble - field2.fieldStats.blankCount) * matchTolerance) {
            println("Match")
            relations += new FieldRelation(dataset1,
              field1,
              dataset2,
              field2,
              missingChildCount,
              (matchCount.toDouble / (field2.fieldStats.totalCount - field2.fieldStats.blankCount)),
              missingParentCount,
              missingParentCount.toDouble / field1.fieldStats.totalCount)
          }
        }
      }
    }
    relations.toArray
  }
}