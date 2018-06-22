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

package org.apache.spark.lineage

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache, Ignition}

import scala.collection.mutable.HashSet
import org.apache.spark._
import org.apache.spark.lineage.rdd.{TapHadoopLRDD, TapLRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._
import org.apache.spark.util.PackIntIntoLong

import collection.JavaConverters._

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */

object LineageManager{

  private var underMaterialization = new HashSet[(RDD[_], Int, Int, StorageLevel)]
  private var blockManager: BlockManager = SparkEnv.get.blockManager

  def initMaterialization[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      level: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ): Unit = underMaterialization.synchronized {
      underMaterialization += ((rdd.asInstanceOf[RDD[T]], partition.index, context.stageId, level))
  }

  def materialize(
      split: Int,
      context: TaskContext,
      effectiveStorageLevel: Option[StorageLevel],
      appId: Option[String]): Unit = {
//    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val toMaterialize = underMaterialization.synchronized {
      val rdds = underMaterialization.filter(r => (r._2 == split) && (r._3 == context.stageId))
      rdds.foreach(underMaterialization.remove(_))
      rdds
    }

    toMaterialize.foreach(tap => {
      val t = new Runnable() {
        override def run() {
          val rdd = tap._1.asInstanceOf[RDD[_]]
          val key = RDDBlockId(rdd.id, split)
          val arr = rdd.materializeBuffer
          
          try {
              blockManager.putIterator(key, arr.toIterator, tap._4, true)
              
            // TODO set up key properly
            val ignite = Ignition.ignite()
            var cacheName = s"${appId.get}_${rdd.id}"
            
            val cache: IgniteCache[Long, (Long, Long, Long)] = createOrGetIgniteCache(ignite,
              cacheName)
            val bufferMap: Map[Long, (Long, Long, Long)] = arr.map(r => {
              var rec = r.asInstanceOf[(Long, Long, Long)]
              (rec._1, rec)
            }).toMap
            cache.putAll(bufferMap.asJava)
          } catch {
            case e: Exception => {
              println(s"Error storing ignite data for RDD ${rdd}")
              e.printStackTrace()
            }
          } finally {
            tap._1.releaseBuffer()
          }
        }
      }

      context.asInstanceOf[TaskContextImpl].threadPool.execute(t)
    })

//    val metrics = context.taskMetrics
//    val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
//    metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
  }
  
  // TODO number of cache partitions is currently fixed because the default 1024
  // cannot be overridden globally or changed after creation, but is too high for local
  // development. Using IgniteRDDs will result in one RDD partition per cache
  // partition, and simple operations will end up spawning 1024 tasks.
  def createOrGetIgniteCache(ignite: Ignite, cacheName: String, numPartitionsPerCache: Int = 2) = {
    val cacheConf = new CacheConfiguration[Long, (Long, Long, Long)](cacheName)
      .setAffinity(new RendezvousAffinityFunction(false, numPartitionsPerCache))
    
    val cache: IgniteCache[Long, (Long, Long, Long)] = ignite.getOrCreateCache(cacheConf)
    cache
  }
  
  def finalizeTaskCache(
      rdd: RDD[_],
      split: Int,
      context: TaskContext,
      _blockManager: BlockManager,
      effectiveStorageLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_AND_DISK),
      appId: Option[String] = None): Unit = {
    blockManager = _blockManager
    materialize(split, context, effectiveStorageLevel, appId)
  }

}
