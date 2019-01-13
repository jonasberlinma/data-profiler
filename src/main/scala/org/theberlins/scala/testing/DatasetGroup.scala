package org.theberlins.scala.testing

import scala.collection.mutable.ArrayBuffer
import java.io.PrintStream

class DatasetGroup extends GraphComponent{

  val datasetGroup:ArrayBuffer[DecoratedDataset] = new ArrayBuffer[ DecoratedDataset]
  
  def addLink(dataset1:DecoratedDataset, dataset2:DecoratedDataset) : Unit = {
    
  }
  
  def writeGraph(nodeFile:PrintStream, edgeFile:PrintStream){
    
  }
}