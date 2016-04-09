/**
 * Created by Michael on 2/4/16.
 */

import java.sql.Timestamp
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD

import org.apache.spark.api.java.JavaRDD
import java.util.{Calendar, ArrayList}

import scala.reflect.ClassTag


//remove if not needed
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import org.apache.spark.lineage.rdd._

class DD_NonEx[T:ClassTag,K:ClassTag] {
  def split(inputRDD: RDD[(T,K)], numberOfPartitions: Int, splitFunc: userSplit[T,K]): Array[RDD[(T,K)]] = {
    splitFunc.usrSplit(inputRDD, numberOfPartitions)
  }

  def test(inputRDD: RDD[T], testFunc: userTest[T], lm: LogManager, fh: FileHandler): (Boolean, List[(String, Int)]) = {
    testFunc.usrTest(inputRDD, lm, fh)
  }

  def difference(first: List[(String, Int)], second: List[(String, Int)]): List[(String, Int)] = {
    var list: List[(String, Int)] = List()
    for (f <- first) {
      var addf = true
      for (s <- second) {
        if (s._1.equals(f._1)) {
          list = (s._1, f._2 - s._2)::list
          addf = false
        }
      }
      if (addf) {
        list = f::list
      }
    }
    for (s <- second) {
      var adds = true
      for (l <- list) {
        if (s._1.equals(l._1)) adds = false
      }
      if (adds) {
        list = s::list
      }
    }
    list
  }

  private def dd_helper(inputRDD: RDD[(T,K)],
                        numberOfPartitions: Int,
                        testFunc: userTest[T],
                        splitFunc: userSplit[T,K],
                        lm: LogManager,
                        fh: FileHandler
                         ): RDD[(T,K)] = {

    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.addHandler(fh)

    logger.log(Level.INFO, "Running DD_NonEx SCALA")

    var rdd = inputRDD
    var partitions = numberOfPartitions
    var runTime = 1
    var bar_offset = 0
    var rddMap: Map[Int, List[(String, Int)]] = Map()
    val assertResult = test(rdd.map(x=>x._1), testFunc, lm, fh)
    if (!assertResult._1) {
      val endTime = System.nanoTime
      logger.log(Level.INFO, "No need to run delta debugging. The file has no faults")
      return null
    }
    rddMap += (0 -> assertResult._2)
    while (true) {
      val startTimeStampe = new Timestamp(Calendar.getInstance.getTime.getTime)
      val startTime = System.nanoTime

      val sizeRDD = rdd.count

      println("rddMap has key 0")

      if (sizeRDD <= 1) {
        val endTime = System.nanoTime
        logger.log(Level.INFO, "The #" + runTime + " run is done")
        logger.log(Level.INFO, "RDD Only Holds One Line - End of This Branch of Search")
        logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
        rdd.collect().foreach(s=> {
          logger.log(Level.WARNING, s.toString + "\n")
        })
        logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
        return rdd
      }

      val rddList = split(rdd, partitions, splitFunc)

      var rdd_failed = false
      var rddBar_failed = false
      var next_rdd = rdd
      var next_partitions = partitions

      breakable {
        for (i <- 0 until (partitions - 1)) {
          val result = test(rddList(i).map(x => x._1), testFunc, lm, fh)
          if (result._1) {
            rdd_failed = true
            next_rdd = rddList(i)
            next_partitions = 2
            bar_offset = 0
            rddMap += (0 -> result._2)
            break
          }
          else {
            rddMap += ((i + 1) -> result._2)
            println("rddMap has key " + (i+1) )
          }
        }
      }

      if (!rdd_failed) {
        breakable{
          if (partitions > 2) {
            var resultList = difference(rddMap(0), rddMap(1))
            for (k <- 2 until (partitions - 1)) {
              resultList = difference(resultList, rddMap(k))
            }
            var result = false
            for (r <- resultList) {
              if (r._2 > 30) result = true
            }
            if (result) {
              rdd_failed = true
              next_rdd = rddList(partitions - 1)
              next_partitions = 2
              bar_offset = 0
              rddMap += (0 -> resultList)
            }
          }
          if (!rdd_failed) {
            for (j <- 0 until (partitions - 1)) {
              //            val i = (j + bar_offset) % partitions
              //            val rddBar = rdd.subtract(rddList(i))
              //            val result = test(rddBar.map(x=>x._1), testFunc, lm, fh)
              val resultList = difference(rddMap(0), rddMap(j + 1))
              var result = false
              for (r <- resultList) {
                if (r._2 > 30) result = true
              }
              if (result) {
                rddBar_failed = true
                //next_rdd = next_rdd.intersection(rddBar)
                rddMap += (0 -> resultList)
                next_rdd = rdd.subtract(rddList(j))
                next_partitions = next_partitions - 1

                //              bar_offset = i
                break
              }
            }
          }
        }
      }

      if (!rdd_failed && !rddBar_failed) {
//        val rddSiz = rdd.count()
//        if (rddSiz <= 2) {
//          val endTime = System.nanoTime()
//          logger.log(Level.INFO, "The #" + runTime + " run is done")
//          logger.log(Level.INFO, "End of This Branch of Search")
//          logger.log(Level.INFO, "This data size is " + sizeRDD)
//          logger.log(Level.WARNING, "Delta Debugged Error inducing inputs: ")
//          rdd.collect().foreach(s=> {
//            logger.log(Level.WARNING, s.toString + "\n")
//          })
//          logger.log(Level.INFO, "This run takes " + (endTime - startTime)/1000 + " microseconds")
//          return rdd
//        }
        next_partitions = Math.min(sizeRDD.asInstanceOf[Int], partitions * 2)
      }
      val endTime = System.nanoTime()
      logger.log(Level.INFO, "Finish the " + runTime + "th run of Non-exhaustive DD, taking " + (endTime - startTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "This data size is " + sizeRDD)

      rdd = next_rdd
      partitions = next_partitions
      logger.log(Level.INFO, "The next partition is " + partitions)
      runTime = runTime + 1
    }
    null
  }

  def ddgen(inputRDD: RDD[(T,K)], testFunc: userTest[T], splitFunc: userSplit[T,K], lm: LogManager, fh: FileHandler): RDD[(T,K)] = {
    dd_helper(inputRDD, 2, testFunc, splitFunc, lm, fh)
  }


}
