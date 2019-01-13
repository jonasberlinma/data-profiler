package org.theberlins.scala.testing

import scala.Array.canBuildFrom
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Hdfs
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.Path
import scalikejdbc._

object FileStats {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("File Analyzer")
    conf.setMaster("local[2]");
    val sc = new SparkContext(conf)

    val configuration = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(configuration)

    // Turn off the verbose logging
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    var directory: String = ""
    var fileName: String = ""
    var inputSeparator: String = ""
    var headerFile: Option[String] = None
    var headerInFile: Boolean = false
    var filter: String = ""
    var drop: String = ""
    var jdbcString: String = ""
    var user: String = ""
    var pass: String = ""

    var datasetManager: DatasetManager = new DatasetManager()

    val argIterator = args.iterator
    while (argIterator.hasNext) {
      argIterator.next() match {
        case "-directory" => directory = argIterator.next()
        case "-file" => fileName = argIterator.next()
        case "-delimiter" => inputSeparator = argIterator.next()
        case "-headerfile" => headerFile = Some(argIterator.next())
        case "-headerinfile" => headerInFile = true
        case "-filter" => filter = argIterator.next()
        case "-drop" => drop = argIterator.next()
        case "-jdbcstring" => jdbcString = argIterator.next()
        case "-user" => user = argIterator.next()
        case "-pass" => pass = argIterator.next()
        case x => println("Unknown switch " + x); sys.exit(1)
      }
    }

    val delimiter = if (inputSeparator.equals("|")) """\|""" else inputSeparator

    // Split and persist since we are going to iterate over fields
    println("Directory is " + directory)

    val fileStatusListIterator = fileSystem.listFiles(new Path(directory), true)

    val fileList = new ArrayBuffer[String]()

    while (fileStatusListIterator.hasNext()) {
      val fileStatus = fileStatusListIterator.next()
      fileList.append(fileStatus.getPath().toString())
    }

    dbConnect(jdbcString, user, pass)

    println("Processing " + fileList.size + " files")
    for (file <- fileList) {
      println("Processing file " + file)
      val dataset = datasetManager.readDelimitedFile(sc, file, delimiter, headerFile, headerInFile, filter, drop)

    }

    datasetManager.analyzeDependencies()
    datasetManager.printAll()

    datasetManager.writeGraph

    sys.exit()
  }

  def dbConnect(jdbcString: String, user: String, pass: String) {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton(jdbcString, user, pass)
    
    implicit val session = AutoSession

    object Foo extends SQLSyntaxSupport[Foo] {
      override val tableName = "foo"
      def apply(rs: WrappedResultSet) = new Foo(rs.long("bar"))
    }
    case class Foo(bar: Long)
        
    val foos: List[Foo] = sql"select * from filestats.foo".map(rs => Foo(rs)).list.apply()
    println("Result " + foos.toString)
    sys.exit()
  }
}