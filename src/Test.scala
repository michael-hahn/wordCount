/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import org.apache.spark.SparkContext._
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._
import org.apache.spark.delta.DeltaWorkflowManager





class Test extends userTest[(String, Int)] with Serializable {

  def usrTest(inputRDD: RDD[(String, Int)], lm: LogManager, fh: FileHandler): (Boolean, List[(String, Int)]) = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false
    var list : List[(String, Int)] = List()

    /*The rest of the code are for correctness test
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/WordCount_CLDD/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/WordCount1.jar", "WordCount1", "-r", "1", fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[String, Int] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/WordCount_CLDD/output/part-r-00000").getLines()) {
      val token = new StringTokenizer(line)
      val word  = token.nextToken()
      val count = token.nextToken().toInt
      truthList = truthList + (word -> count)
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      val outputWord = tupVal._1
      val outputCount = tupVal._2
      if (!truthList.contains(outputWord)) returnValue = true
      else{
        if (!truthList(outputWord).equals(outputCount)) returnValue = true
        else truthList = truthList - outputWord
      }
    }
    if (!truthList.isEmpty) returnValue = true

    val outputFile = new File("/Users/Michael/IdeaProjects/WordCount_CLDD/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    */

    // use deltaworkflowmanager instead
    val resultRDD = inputRDD.groupByKey()
        .map(pair => {
          val itr = pair._2.toIterator
          var returnedValue = 0
          var size = 0
          while (itr.hasNext) {
            returnedValue += itr.next.asInstanceOf[(Int, Long)]._1
            size += 1
          }
          (pair._1, returnedValue/size)
        })
        //this map marks the faulty result
        .map(pair => {
          var value = pair._2.toString
          if (pair._2 > 30) {
            value += "*"
          }
          (pair._1, value)
        })

    val out = resultRDD.collect()
    for (o <- out) {
      println(o)
      val key = o.asInstanceOf[(String, String)]._1
      var value = o.asInstanceOf[(String, String)]._2
      if (value.substring(value.length - 1).equals("*")) {
        list = (key, value.substring(0, value.length - 1).toInt) :: list
        returnValue = true
      } else list = (key, value.toInt) :: list
    }
    return (returnValue, list)
    //

    /*Use delta workflow instead of recalculation
    inputRDD.collect().foreach(println)
    val finalRdd = DeltaWorkflowManager.generateNewWorkFlow(inputRDD)
    val out = finalRdd.collect()
    for (o <- out) {
      println(o)
      if (o.asInstanceOf[(String, String)]._2.substring(o.asInstanceOf[(String, String)]._2.length - 1).equals("*")) returnValue = true
    }
    return returnValue
    */
  }
}
