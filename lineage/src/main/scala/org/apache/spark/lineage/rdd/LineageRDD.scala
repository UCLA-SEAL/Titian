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

package org.apache.spark.lineage.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark._
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.{Direction, HashAwarePartitioner, LocalityAwarePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import org.roaringbitmap.RoaringBitmap

import scala.reflect._

private[spark]
class LineageRDD(val prev: Lineage[(RecordId, Any)]) extends RDD[Any](prev) with Lineage[Any] {

  def this(prev: => Lineage[(Int, Any)]) = this(prev.map(r => ((Dummy, r._1), r._2)))

  override def lineageContext = prev.lineageContext

  override def ttag = classTag[Any]

  override def getPartitions: Array[Partition] = firstParent[(Any, Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[(Any, Any)].iterator(split, context).map(r => r._1)

  var prevResult = Array[(Int, (Any, Any))]()

  override def collect(): Array[Any] = {
    val result = prev.context.runJob(
      prev, (iter: Iterator[(Any, Any)]) => iter.toArray.distinct
    ).filter(_ != null) // Needed for removing results from empty partitions

    prevResult = Array.concat(result: _*).zipWithIndex.map(_.swap)
    prevResult.map(r => (r._1, r._2._1))
  }

  override def filter(f: (Any) => Boolean): LineageRDD =
    new LineageRDD(firstParent[(Any, Any)].filter(r => f(r._1)).cache())

  def filter(f: Int): LineageRDD = {
    val values = prevResult.filter(r => r._1 == f).map(_._2)
    firstParent[(RecordId, Any)].filter(r => values.contains(r)).cache()
  }

  def goNext(): LineageRDD = {
    val next = prev.lineageContext.getForward
    lineageContext.getCurrentLineagePosition.get match {
      case post: TapPostShuffleLRDD[_] =>
        val part = new HashAwarePartitioner(next.partitions.size)
        val shuffled = new ShuffledLRDD[Int, Any, Any](prev, part)
        rightJoinSuperShort(shuffled, next).map(r => ((Dummy, r._2), r._1)).cache()
      case _ => rightJoinSuperShort(prev, next).map(_.swap).cache()
    }
  }

  def goBack(path: Int = 0): LineageRDD = {
    val next = lineageContext.getBackward(path)
    if (next.isDefined) {
      val shuffled: Lineage[(RecordId, Any)] = lineageContext.getCurrentLineagePosition.get match {
        case _: TapPreShuffleLRDD[_] | _: TapPostCoGroupLRDD[_] | _: FilteredLRDD[_] =>
          val part = new LocalityAwarePartitioner(next.get.partitions.size)
          lineageContext.getLastLineageSeen.get match {
            case group: TapPostShuffleLRDD[_] => new ShuffledLRDD[Any, Any, Any](rightJoin(
              prev.asInstanceOf[Lineage[((Int, Int), Any)]].map(r => ((0L, r._1._2), r._2)),
              lineageContext.getLastLineageSeen.get.asInstanceOf[Lineage[(Int, Any)]].map(r => ((0L, r._1), r._2))
            ).map(r => (r._2.asInstanceOf[(CompactBuffer[Long], Int)], r._1)).flatMap(r => (r._1._1.map(r2 => ((r2, r._1._2), r._2)))), part).setMapSideCombine(false)//.map(r => r.swap)
            case _ => new ShuffledLRDD[Any, Any, Any](prev, part).setMapSideCombine(false)
          }
        case _ => prev
      }

      if (next.get.isInstanceOf[TapPreShuffleLRDD[_]]) {
          rightJoin(shuffled, next.get)
            .flatMap(r => r._2.asInstanceOf[RoaringBitmap].toArray.map(r2 => ((r._1._1, r2), r._1))).map(r => r.swap)
            .asInstanceOf[Lineage[(RecordId, Any)]]
          .cache()
      } else if (lineageContext.getCurrentLineagePosition.get.isInstanceOf[TapParallelCollectionLRDD[_]]) {
        new LineageRDD(rightJoinSuperShort(shuffled.asInstanceOf[Lineage[((Int, Int), Any)]].map(r => (r._1._2, r._2)), next.get.map(r => (r._2.asInstanceOf[Int], r._1)))
          .map(r => (r._2, r._1)).asInstanceOf[Lineage[(RecordId, Any)]]).cache()
      } else if (lineageContext.getCurrentLineagePosition.get.isInstanceOf[TapPostCoGroupLRDD[_]]) {
        new LineageRDD(rightJoinSuperShort(shuffled.asInstanceOf[Lineage[(Any, (Int, Int))]].map(r => r.swap).map(r => (r._1._2, r._2)),
          next.get.map(r => (r._1._2, r._2)))
          .flatMap(r => r._2.asInstanceOf[(CompactBuffer[Long], Int)]._1.toArray.map(r2 => ((r2, r._2.asInstanceOf[(Any, Int)]._2), (r2, r._1)))).map(r => r.swap)).cache()
      } else {
        new LineageRDD(rightJoinSuperShort(shuffled.asInstanceOf[Lineage[(Any, (Int, Int))]]
          .map(_.swap).map(r => (r._1._2, r._2)), next.get.map { r => if(r._1.isInstanceOf[Tuple2[_, _]])(r._1._2, r._2) else r.asInstanceOf[(Int, Any)]} )
          .map(r => ((0L, r._1), r._2))).cache()
      }
    } else {
      new LineageRDD(lineageContext.getCurrentLineagePosition.get match {
        case _: TapPreShuffleLRDD[_] =>
          val part = new LocalityAwarePartitioner(prev.partitions.size)
          val tmp = new ShuffledLRDD[Any, Any, Any](prev.asInstanceOf[Lineage[(Any, (CompactBuffer[Long], Int))]].flatMap(r1 => r1._2._1.map(r2 => ((r2, r1._2._2), (0, r1._1)))), part).setMapSideCombine(false)
          rightJoin(tmp, lineageContext.getCurrentLineagePosition.get).asInstanceOf[Lineage[(Any, RoaringBitmap)]].flatMap(r => r._2.toArray.map(b => (r._1, (0, b)))).asInstanceOf[Lineage[(RecordId, Any)]]
        case _ => prev.map(_.swap).asInstanceOf[Lineage[(RecordId, Any)]]
      }).cache()
    }
  }

  def goBackAll(times: Int = Int.MaxValue) = go(times, Direction.BACKWARD)

  def goNextAll(times: Int = Int.MaxValue) = go(times)

  def show(): ShowRDD = {
    val position = prev.lineageContext.getCurrentLineagePosition
    if(position.isDefined) {
      var result: ShowRDD = null
      position.get match {
        case _: TapHadoopLRDD[_, _] =>
          lineageContext.getLastLineageSeen.get match {
            case _: TapPreShuffleLRDD[_] => result = new ShowRDD(
              rightJoin(prev.map(r => r.swap.asInstanceOf[(Long, (Any))]).map(r => ((r._1, 0), r._2)),
                position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
                  .map(r=> ((r._1.get(), 0), r._2.toString))
              ).cache())
            case _ => result = new ShowRDD(
                            join3Way(
                              prev.map(r => (r._1)).asInstanceOf[Lineage[RecordId]],
                              position.get.asInstanceOf[Lineage[RecordId]],
                position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
                  .map(r=> (r._1.get(), r._2.toString))
              ).asInstanceOf[Lineage[(RecordId, String)]].cache() // Added dummy id. To be removed
            )
          }
        case _: TapParallelCollectionLRDD[_] =>
          result = new ShowRDD(
              position.get.asInstanceOf[Lineage[(RecordId, Int)]]
                .map(r=> ((0L, r._2), (r._1).toString)).cache()
            )
        case _: TapPostCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(
            position.get.getCachedData.setCaptureLineage(true).partitions.size)
          val left = rightJoin(
            prev.asInstanceOf[Lineage[((Int, Int), Any)]].map(r => ((0L, r._1._2), r._2)),
            position.get.asInstanceOf[Lineage[(Int, Any)]].map(r => ((0L, r._1), r._2))
          ).map(r => (r._2.asInstanceOf[(CompactBuffer[Long], Int)], r._1)).flatMap(r => r._1._1.map(r2 => ((r2, r._1._2), r._2)))
          val right =
            position.get.getCachedData.setCaptureLineage(true).map {
              case (v, Array(vs, w1s)) =>
                (
                  (v, vs.asInstanceOf[Iterable[(_, Long)]]),
                  (v, w1s.asInstanceOf[Iterable[(_, Long)]])
                  )
            }.flatMap(
                r => for(v <- r.productIterator) yield v.asInstanceOf[(_, Iterable[(_, Long)])]
              ).flatMap( r => for(v <- r._2) yield((v._2, r._1.hashCode()), ((r._1, v._1), v._2).toString()))

          result = new ShowRDD(rightJoin(
            left, right).cache()
          )
        case _: TapPreCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(prev.partitions.size)
          val right = new ShuffledLRDD[RecordId, Any, Any](position.get.getCachedData.setCaptureLineage(true)
            .asInstanceOf[CoGroupedLRDD[_]].map {
              case (v, Array(vs, w1s)) =>
                ((v, vs.asInstanceOf[Iterable[(_, (_, Long))]]),
                  (v, w1s.asInstanceOf[Iterable[(_, Long)]]))
            }.flatMap(
              r => for(v <- r.productIterator) yield v.asInstanceOf[(_, Iterable[(_, Long)])]
            ).flatMap( r => for(v <- r._2) yield((v._2, r._1.hashCode()), ((r._1, v._1), v._2).toString())), part).setMapSideCombine(false).map(r => ((r._1._1, r._1._2), r._2))

          result = new ShowRDD(rightJoin(
              prev.asInstanceOf[Lineage[(RecordId, Any)]].map(r => ((r._1._1, r._1._2), r._2)), right
            )//.map(r => ((0, r._1), r._2))
            .cache())
        case _: TapPreShuffleLRDD[_] =>
          val current = if(!lineageContext.getlastOperation.isDefined) {
            prev.asInstanceOf[Lineage[(RecordId, Any)]]
          } else {
              val part = new HashPartitioner(prev.partitions.size)
              new ShuffledLRDD[Any, Any, Any](prev.asInstanceOf[Lineage[(RecordId, Any)]].map(r => (r._1._2, r)), part).setMapSideCombine(false)
                .map(r => r._2)
          }

          result = new ShowRDD(rightJoin(
            current,
            position.get.asInstanceOf[TapPreShuffleLRDD[_]]
              .getCachedData
              .asInstanceOf[Lineage[(Any, (Any, Long))]]
              .map { r =>
              val hash = r._1.hashCode()
              ((r._2._2, hash), ((r._1, r._2._1), hash).toString())
            }
          ).cache()
          )
        case _: TapPostShuffleLRDD[_] =>
          val current = if(!prev.lineageContext.getlastOperation.isDefined) {
            prev.map(r => (r._2.asInstanceOf[(CompactBuffer[Long], Int)]._2, r._1))
          } else {
            val tmp = prev.lineageContext.getLastLineageSeen.get match {
              case _: TapPreShuffleLRDD[_] | _: TapPostShuffleLRDD[_] => prev
              case _: TapLRDD[_] => rightJoinSuperShort(prev.asInstanceOf[Lineage[((Int, Int), Any)]].map(r => (r._1._2, r._2)), position.get.asInstanceOf[Lineage[(Int, Any)]])
            }
            tmp.lineageContext.getlastOperation.get match {
              case Direction.FORWARD => tmp.map(r => (r._2, r._1))
              case _ => tmp.map(r => (r._2.asInstanceOf[(CompactBuffer[Long], Int)]._2, r._1))
            }
          }

          result = new ShowRDD(rightJoinSuperShort(
            current.asInstanceOf[Lineage[(Int, Any)]],
              position.get.asInstanceOf[TapPostShuffleLRDD[_]]
                .getCachedData.setCaptureLineage(false)
                .asInstanceOf[Lineage[(Any, Int)]]
                .map(r => (r._1.hashCode, r.toString))
            ).map(r => ((0L, r._1), r._2)).cache() // Added dummy id. To be removed
          )

        case _ => throw new UnsupportedOperationException("what cache are you talking about?")
      }

      result.collect.foreach(println)
      result
    } else {
      throw new UnsupportedOperationException("what position are you talking about?")
    }
  }

  private[spark] def go(times: Int, direction: Direction = Direction.FORWARD): LineageRDD =
  {
    var result = this
    var counter = 0
    try {
      while(counter < times) {
        if(direction == Direction.BACKWARD) {
          result = result.goBack()
        } else {
          result = result.goNext
        }
        counter = counter + 1
      }
    } catch {
      case e: UnsupportedOperationException =>
    } finally {
      if(result == this) {
        throw new UnsupportedOperationException
      }
    }
    // Never reach this but otherwise will not compile
    result
  }
}
