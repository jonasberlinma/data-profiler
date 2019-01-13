package org.theberlins.scala.testing

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer

object DatasetCorrelator {

  def findCorrelatedFields(relations: Array[FieldRelation]): Unit = {

    for (relation <- relations) {
      val fromFieldNumber = relation.fromField.fieldNumber
      println("Checking corralation with PK " + relation.fromDataset.path + "." + relation.fromField.fieldName)
      val toFieldNumber = relation.toField.fieldNumber
      val dataset1 = relation.fromDataset
      val dataset2 = relation.toDataset
      val joinedData = dataset1.rdd.map((x) => (x(fromFieldNumber), 1))
        .partitionBy(new HashPartitioner(3))
        .join(dataset2.rdd.map((x) => (x(toFieldNumber), x)))
        .partitionBy(new HashPartitioner(3))
        .mapValues((_._2)).persist()

      // Now walk through each field and check distinct values for each key. (Can't figure out how to do this all in parallel)
      val correlatedFields = new ArrayBuffer[Field]()
      for (field <- dataset2.fields) {
        if (!field.compareTo(relation.toField)) {
          println("Checking correlation PK " + dataset1.path + "." + relation.fromField.fieldName + " to " + dataset2.path + "." + field.fieldName)
          // Find distinct pairs
          val fieldNumber = field.fieldNumber
          val c1 = joinedData.map((x) => (x._1, x._2(fieldNumber))).distinct()
          // Dump the values
          val c2 = c1.map((x) => (x._1))
          // Find number of instance for each value
          val c3 = c2.map((x) => (x, 1))
            .reduceByKey(_ + _, 1)
            .map(item => item.swap)
            .sortByKey(false, 3)
            .map(item => item.swap)

          // Check if all values have one and only one key

          if(c3.filter(_._2 > 1).count() == 0){
            correlatedFields += field
          }
        }
      }
      relation.correlatedFields = Some(correlatedFields.toArray)
    }
  }
}