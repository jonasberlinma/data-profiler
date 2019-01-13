package org.theberlins.scala.testing

import java.io.PrintStream

class FieldRelation (val fromDataset:DecoratedDataset, val fromField:Field, val toDataset:DecoratedDataset, val toField:Field,
    val missingChildCount:Long, val childCoverage:Double, 
    val missingParentCount:Long, val primaryKeyDefectLevel:Double) extends GraphComponent {
  
  var correlatedFields:Option[Array[Field]] = None

  def printAll():Unit ={
    println(fromDataset.name + "." + fromField.fieldName + "->" + toDataset.name + "." + toField.fieldName)
    println("Child coverage : " + childCoverage)
    println("Missing distinct parents : " + missingParentCount)
    println("Parent key defect Level : " + primaryKeyDefectLevel)
    println("Child fields correlated to assumed primary key:")
    for(field <- correlatedFields.get) println(field.fieldName)
    println()
  }
  def writeGraph(nodeFile:PrintStream, edgeFile: PrintStream): Unit = {
    edgeFile.println(fromField.uuid + "," + toField.uuid + ",PK,.5")
    for(field <- correlatedFields.get){
      edgeFile.println(fromField.uuid + "," + field.uuid +",R,.25")
    }
  }
}

