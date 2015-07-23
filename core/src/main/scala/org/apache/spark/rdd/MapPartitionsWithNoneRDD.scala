package org.apache.spark.rdd

/**
 * Created by zengdan on 14-11-4.
 */

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

private[spark] class MapPartitionsWithNoneRDD[U: ClassTag, T: ClassTag](
  prev: RDD[T],
  f: Iterator[T] => Iterator[U],  // (TaskContext, partition index, iterator)
  preservesPartitioning: Boolean = false,
  pipebreaker: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    f(firstParent[T].iterator(split, context))

  //zengdan
  pipelinebreaker = pipebreaker
  curifcs = f.asInstanceOf[Iterator[_] => Iterator[U]]
  override def getParent:Option[RDD[_]] = Some(prev)

  override def getParentRDD(s: Partition):Option[RDD[_]] = Some(prev)


  def af(arr: Array[T]):Array[U] = {
    f(arr.toIterator).toArray[U]
  }

  def sf(x:T):U = {
    val iter = new Iterator[T]{
      private var hdDefined = false
      override def hasNext() = !hdDefined
      override def next() = {
        hdDefined = true
        x
      }
    }
    f(iter).next()
    //result
  }

  override def linkSingleFunc(s: Partition) = {
    //fcs = prev.fcs.andThen(ff)
    //curf = prev.curf.andThen(f2)
    singlef.put(s, prev.singlef.get(s).andThen(sf))
  }

  override def linkArrayFunc(s: Partition) = {
    arrayf.put(s, prev.arrayf.get(s).andThen(af))
  }
}
