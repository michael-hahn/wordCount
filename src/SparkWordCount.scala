/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "/Users/Michael/Desktop/spark-lineage-1.2.1/README.md"
    if(args.size < 2) {
      //logFile = "biomed"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
//      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      //      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      conf.set("spark.kryo.referenceTracking", "false")
      //      conf.set("spark.kryo.registrationRequired", "true")
      //      conf.registerKryoClasses(Array(
      //        classOf[RoaringBitmap],
      //        classOf[BitmapContainer],
      //        classOf[RoaringArray],
      //        classOf[RoaringArray.Element],
      //        classOf[ArrayContainer],
      //        classOf[Array[RoaringArray.Element]],
      //        classOf[Array[Tuple2[_, _]]],
      //        classOf[Array[Short]],
      //        classOf[Array[Int]],
      //        classOf[Array[Long]],
      //        classOf[Array[Object]]
      //      ))
    }
    conf.setAppName("WordCount-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)

    // Job
    val lines = lc.textFile("/Users/Michael/Documents/lambdadelta/angular-seed/README.md", 1)
//    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))
//    val counts = pairs.groupByKey().map(pair => {
//      val numList = pair._2
//      var numTotal = 0
//      for (n <- numList) {
//        numTotal += n
//      }
//      (pair._1, numTotal)
//    })
    val wordCount = lines.flatMap(line => line.trim().replaceAll(" +", " ").split(" "))
        .map(word => (word.substring(0, 1), word.length))
        //.reduceByKey(_ + _) //Use groupByKey to avoid combiners
        .groupByKey()
        .map(pair => {
          val itr = pair._2.toIterator
          var returnedValue = 0
          var size = 0
          while (itr.hasNext) {
            val num = itr.next()
            returnedValue += num
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

    val results = wordCount.collectWithId()

    lc.setCaptureLineage(false)
    //
    Thread.sleep(1000)

    for (c <- results) {
      println(c._1._1 + "," + c._1._2 + ": " + c._2)
    }
    //    println(counts.collect().mkString("\n"))

    // Step by step full trace backward
        var linRdd = wordCount.getLineage()
        linRdd.collect()
        linRdd = linRdd.filter(l => {
          println("**** =>" + l)
          if (l == 50L) true
          else false
        })
//        linRdd.show
        linRdd = linRdd.goBackAll()
//        linRdd.collect.foreach(println)
        linRdd.show
    linRdd = linRdd.filter(l => {
      println("**** =>" + l)
      if (l.asInstanceOf[((Int, Int), Int)]._1._2 == 100) true
      else false
    })
    //        linRdd.collect.foreach(println)
    linRdd.show
//        linRdd = linRdd.goBack()
//        linRdd.collect.foreach(println)
//        linRdd.show


    // Full trace backward
    //    for(i <- 1 to 10) {
    //      var linRdd = counts.getLineage()
    //      linRdd.collect //.foreach(println)
    //      //    linRdd.show
    // //     linRdd = linRdd.filter(4508) //4508
    //      linRdd = linRdd.goBackAll()
    //      linRdd.collect //.foreach(println)
    //      println("Done")
    //    }
    //    linRdd.show
    //
    //    // Step by step trace backward one record
    //    linRdd = counts.getLineage()
    //    linRdd.collect().foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.filter(262)
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goBack()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goBack()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
    //    // Full trace backward one record
    //    linRdd = counts.getLineage()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.filter(262)
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goBackAll()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
    //    // Step by step full trace forward
//        var linRdd = file.getLineage()
//        linRdd.collect.foreach(println)
//        println("Go forward Result: ***********")
//        linRdd.show
//        linRdd = linRdd.goNext()
//        linRdd.collect.foreach(println)
//        println("Go forward once Result: **********")
//        linRdd.show
//        linRdd = linRdd.goNext()
//        linRdd.collect.foreach(println)
//        println("Go forward twice times Result: ****")
//        linRdd.show
    //
    //    // Full trace forward
    //    linRdd = file.getLineage()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNextAll()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
        // Step by step trace forward one record
//        linRdd = counts.getLineage().goBackAll()
//        linRdd.collect().foreach(println)
//        linRdd.show
//        linRdd = linRdd.filter(2)
//        linRdd.collect.foreach(println)
//        linRdd.show
//        linRdd = linRdd.goNext()
//        linRdd.collect.foreach(println)
//        linRdd.show
//        linRdd = linRdd.goNext()
//        linRdd.collect.foreach(println)
//        linRdd.show

    //    // Full trace forward one record
    //var linRdd = counts.getLineage()
    //    linRdd.collect
    //    linRdd = linRdd.filter(0)
    //    linRdd = linRdd.goBackAll()
    //    linRdd.collect
    //    val value = linRdd.take(1)(0)
    //    println(value)
    ////    sc.unpersistAll(false)
    //    for(i <- 1 to 10) {
    //          var linRdd = file.getLineage().filter(r => (r.asInstanceOf[(Any, Int)] == value))
    //          linRdd.collect()//.foreach(println)
    //      //    linRdd.show
    //          linRdd = linRdd.filter(0)
    //
    //      //    linRdd.collect.foreach(println)
    //      //    linRdd.show
    //          linRdd = linRdd.goNextAll()
    //          linRdd.collect()//.foreach(println)
    //          println("Done")
    //    }
    ////    linRdd.show
    sc.stop()
  }
}
