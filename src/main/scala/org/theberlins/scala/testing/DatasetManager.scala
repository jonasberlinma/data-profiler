package org.theberlins.scala.testing

import java.util.Hashtable
import scala.collection.mutable.ArrayBuffer
import javax.management.relation.Relation
import scala.collection.mutable.HashMap
import java.io.PrintStream
import java.io.File
import org.apache.spark.SparkContext

class DatasetManager {

  val datasets: HashMap[String, DecoratedDataset] = new HashMap[String, DecoratedDataset]()
  val relations: ArrayBuffer[FieldRelation] = new ArrayBuffer[FieldRelation]

  def addDataset(name: String, dataset: DecoratedDataset): Unit = {
    datasets.put(name, dataset)
  }

  def analyzeDependencies(): Unit = {

    for (dataseti <- datasets) {
      for (datasetj <- datasets)
        if (dataseti._2.uuid.compareTo(datasetj._2.uuid) != 0) {
          relations.appendAll(ForeignKeyIdentifier.findForeignKeys(dataseti._2, datasetj._2))
        }
    }
    DatasetCorrelator.findCorrelatedFields(relations.toArray)

  }
  def printAll(): Unit = {
    for (dataset <- datasets) {
      dataset._2.printAll()
    }
    for (relation <- relations)
      relation.printAll()
  }

  def writeGraph(): Unit = {

    val nodeFile = new PrintStream(new File("nodes.txt"))
    val edgeFile = new PrintStream(new File("edges.txt"))
    nodeFile.println("Id,Label,Type")
    edgeFile.println("Source,Target,Label,Weight")
    for (dataset <- datasets) {
      dataset._2.writeGraph(nodeFile, edgeFile)
    }
    for (relation <- relations) {
      relation.writeGraph(nodeFile, edgeFile)
    }
  }
  def readDelimitedFile(sc: SparkContext, fileName: String, separator: String, headerFile: Option[String], headerInFile: Boolean, filter:String, drop:String): DecoratedDataset = {
    val inputFile = sc.textFile(fileName, 3)
    val fieldBuffer = new ArrayBuffer[Field]()
    var skipCount = 0
    val inputFieldsTmp: Array[String] =
      if (headerInFile) {
        skipCount = 1
        inputFile.take(1)(0).toString.split(separator)
      } else {
        sc.textFile(headerFile.getOrElse(""), 1).take(1)(0).toString.split(separator)
      }
    val foo = new ArrayBuffer[String]()

    for (field <- inputFieldsTmp) {
      foo.append(field.replaceAll("\"", "").trim())
    }

    val inputFields = foo.toArray
    val inputFieldsSize = inputFields.size
    val inputData = inputFile
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(skipCount) else iter)
      .map((_.split(separator).padTo(inputFieldsSize, "")))

    for (i <- 0 to inputFields.length - 1) {
      fieldBuffer += new Field(fileName, inputFields(i), i)
    }
    val dataset = new DecoratedDataset(fileName, inputData, fieldBuffer)
    if (!filter.isEmpty()) {
      dataset.filterRecords(filter)
    }
    if (!drop.isEmpty()) {
      dataset.dropFields(drop)
    }

    val summaries = Summarizer.summarize(dataset)

    dataset.guesstimate()
    addDataset(fileName, dataset)
    dataset
  }
  def storeResults(): Unit = {
    
  }
}