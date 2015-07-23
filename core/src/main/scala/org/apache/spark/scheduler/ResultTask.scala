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

import java.io._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.storage.StorageLevel

//zengdan
import scala.reflect.runtime.{universe => ru}
import scala.reflect.{classTag, ClassTag}
import scala.collection.mutable.ArrayBuffer

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient locs: Seq[TaskLocation],
    val outputId: Int)
  extends Task[U](stageId, partition.index) with Serializable with Logging {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    val taskbegin = System.currentTimeMillis()
    // Deserialize the RDD and the func using the broadcast variables.
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    //func(context, rdd.iterator(partition, context))
    val pushmode = SparkEnv.get.conf.get("spark.push.mode", "false").toBoolean
    log.debug(s"===============Result Push mode: $pushmode============")

    if(pushmode) {
      log.debug("==================== Entering Push mode ============")
      val arraymode = SparkEnv.get.conf.get("spark.array.mode", "false").toBoolean
      val startTime = System.currentTimeMillis()
      //zengdan
      var tmp: RDD[_] = rdd
      var p = List(partition)
      var rdds: List[RDD[_]] = List(rdd)
      var src: Iterator[_] = Iterator.empty

      //get the rdd from which to push, it's either the source data or the nearest cached rdd
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
      val result = func(context, src.asInstanceOf[Iterator[T]])
      log.debug(s"======The time to compute is ${System.currentTimeMillis()-start}======")
      log.debug(s"======Result Task Running Time: ${System.currentTimeMillis()-taskbegin}======")
      result
    //}
    } else {
      val start = System.currentTimeMillis()
      val result = func(context, rdd.iterator(partition, context))
      log.debug(s"======The time to compute is ${System.currentTimeMillis()-start}======")
      log.debug(s"======Result Task Running Time: ${System.currentTimeMillis()-taskbegin}======")
      result
    }
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partitionId + ")"
}
