package org.theberlins.scala.testing

import datasets.RawDataset
import java.security.MessageDigest
import org.apache.commons.codec.binary.Base64
import java.io.PrintWriter
import java.io.PrintStream
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path

class DecoratedDataset(val path: String, var rdd: RawDataset, val fields: ArrayBuffer[Field]) extends GraphComponent {

  var signature: String = ""
  val name: String = new Path(path).getName()

  def guesstimate(): Unit = {
    fields.iterator.foreach(guesstimateFields)
    computeSignature
  }
  private def guesstimateFields(field: Field) = {
    field.guesstimateKey()
    field.guesstimateType()
  }
  def printAll(): Unit = {
    println("Dataset name : " + name)
    println("Signature : " + signature)
    for (field <- fields) field.printAll()
  }
  def writeGraph(nodeFile: PrintStream, edgeFile: PrintStream): Unit = {
    nodeFile.println(uuid + "," + name + ",D")
    for (field <- fields) {
      field.writeGraph(uuid, nodeFile, edgeFile)
    }
  }
  private def computeSignature: Unit = {
    val stringBuffer: StringBuffer = new StringBuffer()
    for (field <- fields) {
      if (field.topPatterns.length > 0) {
        val fieldSignature = MessageDigest.getInstance("MD5").digest(field.topPatterns(0)._1.getBytes())
        val base64Signature = new String(Base64.encodeBase64(fieldSignature))
        stringBuffer.append(base64Signature.substring(0, 2))
      } else {
        stringBuffer.append("==")
      }
    }
    signature = stringBuffer.toString()
  }
  
  def filterRecords(filter:String) : Unit = {
    val conditions = filter.split("&")
    
    for(condition <- conditions){
      val parsedCondition = condition.split("=")
      fields.find(x=>(x.fieldName.compareTo(parsedCondition(0)) == 0)) match {
        case None => println("Warning: While filtering records field " + parsedCondition(0) + " not found in " + path)
        case Some(x) => {
          val fieldNumber = x.fieldNumber
          val constant = parsedCondition(1)
          rdd = rdd.filter(y => (y(fieldNumber).compareTo(constant) == 0))
        }
      }
      
    }
  }
  def dropFields(drop:String) : Unit = {
    
    val dFieldNames = drop.split(",")
    
    for(dFieldName <- dFieldNames){
      fields.find(x => (x.fieldName.compareTo(dFieldName) == 0)) match {
        case None => println("Warning: While dropping fields field " + dFieldName + " not found in " + path)
        case Some(x) => {
          // Remove from RDD
          val fieldNumber = x.fieldNumber
          rdd = rdd.map( y => ({val a = new ArrayBuffer() ++ y;  a.remove(fieldNumber); a.toArray}))
          // Remove from metadata
          fields.remove(fieldNumber)
          // Renumber
          var i = 0
          for(field <- fields){
            field.fieldNumber = i
            i += 1
          }
        } 
      }
      
    }
  }

}