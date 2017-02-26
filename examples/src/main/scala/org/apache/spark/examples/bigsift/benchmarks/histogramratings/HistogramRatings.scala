package org.apache.spark.examples.bigsift.benchmarks.histogramratings

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  * Seed error with respect to the movie specific rating.
  * When traced back, from the rating count, it would lead to Error in several movies
  *
  *
  *
  */

object HistogramRatings {

  private val division = 0.5f
  private val exhaustive = 1

  def mapFunc(str: String): (Float, Int) = {
    val token = new StringTokenizer(str)
    val bin = token.nextToken().toFloat
    val value = token.nextToken().toInt
    return (bin, value)
  }

  def main(args: Array[String]) {
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
      val sparkConf = new SparkConf()

      var logFile = ""
      var local = 500
      if (args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")
        logFile = "/home/ali/work/temp/git/bigsift/src/benchmarks/histogrammovies/data/file1s.data"
      } else {

        logFile = args(0)
        local = args(1).toInt

      }
      //set up lineage
      var lineage = true
      lineage = true

      val ctx = new SparkContext(sparkConf)

      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)

      //start recording time for lineage
      /** ************************
        * Time Logging
        * *************************/
      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/

      val lines = lc.textFile(logFile, 1)

      //Compute once first to compare to the groundTruth to trace the lineage

      val ratings  = lines.flatMap(s => {
        var ratingMap : Map[Int, Int] =  Map()
        var rating: Int = 0
        var reviewIndex: Int = 0
        var movieIndex: Int = 0
        var reviews: String = new String
        var tok: String = new String
        var ratingStr: String = new String
        var raterStr: String = new String
        var movieStr:String = new String
        movieIndex = s.indexOf(":")
        if (movieIndex > 0) {
          reviews = s.substring(movieIndex + 1)
          movieStr = s.substring(0,movieIndex)
          val token: StringTokenizer = new StringTokenizer(reviews, ",")
          while (token.hasMoreTokens) {
            tok = token.nextToken
            reviewIndex = tok.indexOf("_")
            raterStr = tok.substring(0, reviewIndex)
            ratingStr = tok.substring(reviewIndex + 1)
            rating = ratingStr.toInt
            var rater = raterStr.toLong
            if (rating == 1 || rating == 2) {
              if (rater % 13 != 0) {
                val old_rat = ratingMap.getOrElse(rating, 0)
                ratingMap = ratingMap updated(rating, old_rat+1)
              }
            }
            else {
              if(movieStr.equals("1995670000") && raterStr.equals("2256305") && rating == 5) {
                val old_rat = ratingMap.getOrElse(rating, 0)
                ratingMap = ratingMap updated(rating, old_rat + Int.MinValue)
              }else {
                val old_rat = ratingMap.getOrElse(rating, 0)
                ratingMap = ratingMap updated(rating, old_rat+1)
              }
            }
          }
        }
        ratingMap.toIterable
      })
      val counts  = ratings.groupByKey()
        .map(pair => {
          var total = 0
          for (num <- pair._2) {
            total += num
          }
          (pair._1, total)
        }).filter(s => HistogramRatings.failure(s._2))
      val output = counts.collectWithId()

      /** ************************
        * Time Logging
        * *************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *************************/


      lc.setCaptureLineage(false)
      Thread.sleep(1000)


      var list = List[Long]()
      for (o <- output) {
          //println(o._1._1 + ": " + o._1._2 + " - " + o._2)
          list = o._2 :: list
      }

      /** ************************
        * Time Logging
        * *************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/

      var linRdd = counts.getLineage()
      linRdd.collect
      linRdd = linRdd.filter { l => list.contains(l) }
      linRdd = linRdd.goBackAll()
      val showMeRdd = linRdd.show(false).toRDD

      /** ************************
        * Time Logging
        * *************************/
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *************************/


      /** ************************
        * Time Logging
        * *************************/
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/


      val delta_debug = new DDNonExhaustive[String]
      delta_debug.setMoveToLocalThreshold(local);
      val returnedRDD = delta_debug.ddgen(showMeRdd, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)

      /** ************************
        * Time Logging
        * *************************/
      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")

      /** ************************
        * Time Logging
        * *************************/

      println("Job's DONE!")
      ctx.stop()
    }
  }


  def failure(record:Int): Boolean ={
        record < 0

  }
}