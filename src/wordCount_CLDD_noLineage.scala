/**
 * Created by Michael on 4/8/16.
 */
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._

import org.apache.spark.SparkContext._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.Tuple2
import java.util.Calendar
//import java.util.List
import java.util.StringTokenizer

import scala.collection.mutable.MutableList
import scala.reflect.ClassTag

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._
import org.apache.spark.delta.DeltaWorkflowManager


object wordCount_CLDD_noLineage {
  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)


      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[6]")
      sparkConf.setAppName("WordCount_CLDD")
        .set("spark.executor.memory", "2g")

      //set up lineage
//      var lineage = true
//      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
//      if (args.size < 2) {
//        logFile = "test_log"
//        lineage = true
//      } else {
//        lineage = args(0).toBoolean
//        logFile += args(1)
//        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
//      }
      //

      //set up spark context
      val ctx = new SparkContext(sparkConf)


      //set up lineage context
//      val lc = new LineageContext(ctx)
      //

      //Prepare for Hadoop MapReduce - for correctness test only
      /*
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/WordCount1.jar", "WordCount1", "-r", "1", "/Users/Michael/IdeaProjects/InvertedIndex/myLog", "output").!!
      */

      //start recording lineage time
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //spark program starts here
      val lines = ctx.textFile("/Users/Michael/Documents/lambdadelta/angular-seed/README.md", 1)
//      val wordCount = lines.flatMap(line => line.trim().replaceAll(" +", " ").split(" "))
//        .map(word => (word.substring(0, 1), word.length))
//        //.reduceByKey(_ + _) //Use groupByKey to avoid combiners
//        .groupByKey()
//        .map(pair => {
//          val itr = pair._2.toIterator
//          var returnedValue = 0
//          var size = 0
//          while (itr.hasNext) {
//            val num = itr.next()
//            returnedValue += num
//            size += 1
//          }
//          (pair._1, returnedValue/size)
//        })
//        //this map marks the faulty result
//        .map(pair => {
//          var value = pair._2.toString
//          if (pair._2 > 30) {
//            value += "*"
//          }
//          (pair._1, value)
//        })
//
//      val out = wordCount.collectWithId()



      //print out the result for debugging purpose
      //      for (o <- out) {
      //        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      //      }



      //      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/WordCount_CLDD/lineageResult"))



      val mappedRDD = lines.map(s => s)
      mappedRDD.cache()

      //      println("MappedRDD has " + mappedRDD.count() + " records")



      //      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/WordCount_CLDD/lineageResult", 1)
      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/textFile", 1)

      //      val num = lineageResult.count()
      //      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/WordCount_CLDD/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
      //lineageResult.cache()



      //      if (exhaustive == 1) {
      //        val delta_debug = new DD[(String, Int)]
      //        delta_debug.ddgen(mappedRDD, new Test,
      //          new Split, lm, fh)
      //      } else {
      val delta_debug = new DD_NonEx_NonIncr_v2[String]
      val returnedRDD = delta_debug.ddgen(mappedRDD, new Test_NonIncr_v2, new Split_v2, lm, fh)
      //      }
      val ss = returnedRDD.collect
      ss.foreach(println)
      //      println("**************")
      //      for (a <- ss){
      //        println(a._1 + " && " + a._2)
      //      }
      //      println("**************")
      // linRdd.collect.foreach(println)

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")

      //total time
      logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (DeltaDebuggingEndTime - LineageStartTime)/1000 + " microseconds")


      println("Job's DONE!")
      ctx.stop()
    }
  }



}
