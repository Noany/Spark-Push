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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
* A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
* specified in the ShuffleDependency).
*
* See [[org.apache.spark.scheduler.Task]] for more information.
*
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcast version of of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation])
  extends Task[MapStatus](stageId, partition.index) with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, null, new Partition { override def index = 0 }, null)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    val taskbegin = System.currentTimeMillis()
    // Deserialize the RDD using the broadcast variable.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)

      val pushmode = SparkEnv.get.conf.get("spark.push.mode", "false").toBoolean
      log.debug(s"==============Shuffle Push mode: $pushmode============")

      if(pushmode) {
        log.debug("==================== Entering Push mode ============")
        val arraymode = SparkEnv.get.conf.get("spark.array.mode", "false").toBoolean
        val startTime = System.currentTimeMillis()
        var tmp: RDD[_] = rdd
        var p = List(partition)
        var rdds: List[RDD[_]] = List(rdd)
        var src: Iterator[_] = Iterator.empty

        //get the rdd from which to push, it's either the source data or the recent cached rdd
        var cached = false
        while ((tmp.getParentRDD(p.head).isDefined) && !cached) {
          val iter = tmp.exists(p.head, context)
          if(!iter.isDefined) {
            p = tmp.getParentPartition(p.head) :: p
            tmp = tmp.getParentRDD(partition).get
            rdds = tmp :: rdds
          }else{
            src = iter.get
            cached = true
          }
        }

        log.debug(s"===The time to get parent rdds is ${System.currentTimeMillis() - startTime}===")

        if(arraymode) {

          tmp.setArrayfcs(p(0))
          //get the source data to push and cache it if necessary
          if (!cached)
            src = tmp.iterator(p(0), context)
          if (tmp.getStorageLevel != StorageLevel.NONE) {
            src = SparkEnv.get.cacheManager.put(tmp, p(0), context, tmp.getStorageLevel, src)
          }

          //compute the result and cache the partition when necessary
          var i = 1
          while (i < rdds.size) {
            rdds(i).linkArrayFunc(p(i))
            if (rdds(i).pipelinebreaker) {
              val iter = rdds(i - 1).getMaterializeIter(src, p(i), true)
              src = rdds(i).curifcs(iter)
              rdds(i).setArrayfcs(p(i))
            }
            if (rdds(i).getStorageLevel != StorageLevel.NONE) {
              val iter = rdds(i).getMaterializeIter(src, p(i), true)
              src = SparkEnv.get.cacheManager.put(rdds(i), p(i), context, rdds(i).getStorageLevel, iter)
              rdds(i).setArrayfcs(p(i))
            }
            i += 1
          }
        }else{
          tmp.setSinglefcs(p(0))
          //get the source data to push and cache it if necessary
          if (!cached)
            src = tmp.iterator(p(0), context)
          if (tmp.getStorageLevel != StorageLevel.NONE) {
            src = SparkEnv.get.cacheManager.put(tmp, p(0), context, tmp.getStorageLevel, src)
          }

          //compute the result and cache the partition when necessary
          var i = 1
          while (i < rdds.size) {
            rdds(i).linkSingleFunc(p(i))
            if (rdds(i).pipelinebreaker) {
              val iter = rdds(i - 1).getMaterializeIter(src, p(i), false)
              src = rdds(i).curifcs(iter)
              rdds(i).setSinglefcs(p(i))
            }
            if (rdds(i).getStorageLevel != StorageLevel.NONE) {
              val iter = rdds(i).getMaterializeIter(src, p(i), false)
              src = SparkEnv.get.cacheManager.put(rdds(i), p(i), context, rdds(i).getStorageLevel, iter)
              rdds(i).setSinglefcs(p(i))
            }
            i += 1
          }
        }

        //println("In ResultTask, from rdd %s to push".format(tmp.toString))
        if (rdd.getStorageLevel == StorageLevel.NONE && !rdd.pipelinebreaker) {
          src = rdd.getMaterializeIter(src, partition, arraymode)
        }
        val start = System.currentTimeMillis()
        writer.write(src.asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
        log.debug(s"======The time to write is ${System.currentTimeMillis()-start}======")
      //}
      }else {
        val start = System.currentTimeMillis()
        writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
        log.debug(s"======The time to write is ${System.currentTimeMillis()-start}======")
      }
      log.debug(s"======ShuffleMap Task Running Time: ${System.currentTimeMillis()-taskbegin}======")
      return writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
