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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

private[spark]
class MappedRDD[U: ClassTag, T: ClassTag](prev: RDD[T], f: T => U)
  extends RDD[U](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).map(f)

  //zengdan
  override def getParent:Option[RDD[_]] = Some(prev)

  override def getParentRDD(s: Partition):Option[RDD[_]] = Some(prev)

  def af(arr: Array[T]):Array[U] = {
    arr.map(f)
  }

  //curf = Option(prev.curf.get.andThen(f))

  override def linkSingleFunc(s: Partition) = {
    //fcs = prev.fcs.andThen(ff)
    //curf = prev.curf.andThen(f2)
    singlef.put(s, prev.singlef.get(s).andThen(f))
  }

  override def linkArrayFunc(s: Partition) = {
    arrayf.put(s, prev.arrayf.get(s).andThen(af))
  }

}
