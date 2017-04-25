package org.apache.spark.examples.bigsift.benchmarks.wordcount

/**
 * Created by Michael on 11/12/15.
 */

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.benchmarks.sequencecount.TestSJF
import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object WordCountOverlap {
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
      val sparkConf = new SparkConf()
      var logFile = ""
      var local = 500
      var applySJF = 0
      if(args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("TermVector_LineageDD").set("spark.executor.memory", "2g")
        logFile =  "/home/ali/work/temp/git/bigsift/src/benchmarks/termvector/data/textFile"
      }else{
        applySJF = args(2).toInt
        logFile = args(0)
        local  = args(1).toInt
      }

      //set up spark context
      val ctx = new SparkContext(sparkConf)

      //set up lineage context and start capture lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(true)


      //start recording time for lineage
      /**************************
        Time Logging
        **************************/
      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /**************************
        Time Logging
        **************************/
      val lines = lc.textFile(logFile, 5)

      val sequence = lines.filter(s => WordCount.filterSym(s)).flatMap(s => {
        s.split(" ").map(w  => addFault(s , w) )
      }).reduceByKey(_+_).filter(s => WordCount.failure(s))


      val out = sequence.collectWithId()
      /**Annotating bugs on cluster**/


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
      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purposes
      for (o <- out) {
        println(o._1._1 + ": " + o._1._2 + " - " + o._2)

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




      //list of bad inputs
      var list = List[Tuple2[Long, Long]]() // linID and size
      for (o <- out) {
        var linRdd = sequence.getLineage()
        linRdd.collect
        var size = linRdd.filter { l => o._2 == l }.goBackAll().count()
        list = Tuple2(o._2, size) :: list
      }

      if (applySJF == 1) {
        list = list.sortWith(_._2 < _._2)
      }


      /** ************************
        * Time Logging
        * *************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
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
      val SJFStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val SJFStartTime = System.nanoTime()
      logger.log(Level.INFO, "SJF (unadjusted) time starts at " + SJFStartTimestamp)

      /** ************************
        * Time Logging
        * *************************/
      if (applySJF == 1) {
        while (list.size != 0) {
          if (list.size > 1) {
            logger.log(Level.INFO, s""" Overlap : More than two faults  """)
            val e1 = list(0)
            val e2 = list(1)
            var linRdd = sequence.getLineage()
            linRdd.collect
            val mappedRDD1 = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
            linRdd = sequence.getLineage()
            linRdd.collect
            val mappedRDD2 = linRdd.filter { l => e2._1 == l}.goBackAll().show(false).toRDD
            val mappedRDD = mappedRDD1.intersection(mappedRDD2)
            val fault = runDD(mappedRDD, logger, local, lm, fh)
            if (!fault) {
              logger.log(Level.INFO, s""" Overlap : Contains fault  """)
              runDD(mappedRDD1, logger, local, lm, fh)
              runDD(mappedRDD2, logger, local, lm, fh)
              list = list.drop(2)
            } else {
              logger.log(Level.INFO, s""" Overlap : does not contain faults  """)
              runDD(mappedRDD1.subtract(mappedRDD), logger, local, lm, fh)
              runDD(mappedRDD2.subtract(mappedRDD), logger, local, lm, fh)
              list = list.drop(2)
            }
          } else {
            logger.log(Level.INFO, s""" Overlap : Only one fault """)
            val e1 = list(0)
            list = list.drop(1)
            val linRdd = sequence.getLineage()
            linRdd.collect
            val mappedRDD = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
            runDD(mappedRDD, logger, local, lm, fh)
          }
        }
      }else{
        logger.log(Level.INFO, s""" Overlap : disabled""")
        while (list.size != 0) {
          val e1 = list(0)
          list = list.drop(1)
          val linRdd = sequence.getLineage()
          linRdd.collect
          val mappedRDD = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
          runDD(mappedRDD, logger, local, lm, fh)
        }
      }


      /** ************************
        * Time Logging
        * *************************/
      val SJFEndTime = System.nanoTime()
      val SJFEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "SJF (unadjusted) + L  ends at " + SJFEndTimestamp)
      logger.log(Level.INFO, "SJF (unadjusted) + L takes " + (SJFEndTime - SJFStartTime) / 1000 + " milliseconds")

      /** ************************
        * Time Logging
        * *************************/







      ctx.stop()
    }
  }
  def runDD(maprdd : RDD[String] , logger: Logger , local:Int ,  lm: LogManager, fh: FileHandler): Boolean ={

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
    delta_debug.setMoveToLocalThreshold(local)
    val foundfault = delta_debug.ddgen(maprdd, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
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
    foundfault

  }
  def failure(record:(String, Int)): Boolean = {
    record.asInstanceOf[Tuple2[String , Int]]._2 <= 0
  }

  def filterSym(str:String): Boolean ={
    val sym: Array[String] = Array(">","<" , "*" , "="  , "#" , "+" , "-" , ":" , "{" , "}" , "/","~" , "1" , "2" , "3" ,"4" , "5" , "6" , "7" , "8" , "9" , "0")
    for(i<- sym){
      if(str.contains(i)) {
        return false;
      }
    }
    return true;
  }


  def addFault(str: String, key: String): Tuple2[String, Int] = {
    if (str.contains("Romeo and Juliet") && key.contains("He"))
      return Tuple2(key, Int.MinValue)
    else if (str.contains("coniferous forest and riparian habitat types") && key.contains("are")) // http://scai10.cs.ucla.edu:50075/browseBlock.jsp?blockId=-8388939448631812002&blockSize=67108864&filename=%2Fclash%2Fdatasets%2FWW%2Fwiki-size-80000000000&datanodePort=50010&genstamp=5739306&namenodeInfoPort=50070&chunkSizeToView=32768
      return Tuple2(key, -9999999)
    else if (str.contains("automation harnesses are available for use") && key.contains("so"))
      return Tuple2(key, -9999999)
    else if (str.contains("XMPP extension that allows an application") && key.contains("in"))
      return Tuple2(key,-9999999)
    else if (str.contains("http://www.ntaforum.org/members/index.html") && key.contains("has"))
      return Tuple2(key, -9999999)
    else if (str.contains("http://www.eetimes.com/design/industrial-control/4218488") && key.contains("two"))
      return Tuple2(key,-9999999)
    else if (str.contains("ATPase activity") && key.contains("have")) //http://scai13.cs.ucla.edu:50075/browseBlock.jsp?blockId=-8156531693141357722&blockSize=67108864&filename=%2Fclash%2Fdatasets%2FWW%2Fwiki-size-80000000000&datanodePort=50010&genstamp=5739306&namenodeInfoPort=50070&chunkSizeToView=32768
      return Tuple2(key, -9999999)
    else if (str.contains("most tenacious") && key.contains("one")) // http://scai05.cs.ucla.edu:50075/browseBlock.jsp?blockId=-3751596619526339756&blockSize=67108864&filename=%2Fclash%2Fdatasets%2FWW%2Fwiki-size-80000000000&datanodePort=50010&genstamp=5739306&namenodeInfoPort=50070&chunkSizeToView=32768
      return Tuple2(key, -9999999)
    else if (str.contains("Tranmere Rovers") && key.contains("is"))
      return Tuple2(key, Int.MinValue)
    else
      return Tuple2(key, 1)
  }

}

