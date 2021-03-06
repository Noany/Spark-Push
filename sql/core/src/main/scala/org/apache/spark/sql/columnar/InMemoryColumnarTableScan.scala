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

package org.apache.spark.sql.columnar

import java.nio.ByteBuffer

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LeafNode, SparkPlan}
import org.apache.spark.storage.StorageLevel
//zengdan 无cacheTable类似接口，rdd＝InMemoryRelation(...)  rdd.collect()类似
private[sql] object InMemoryRelation {
  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel, child)()
}

private[sql] case class CachedBatch(buffers: Array[Array[Byte]], stats: Row)

private[sql] case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    child: SparkPlan)
    (private var _cachedColumnBuffers: RDD[CachedBatch] = null)
  extends LogicalPlan with MultiInstanceRelation {

  override lazy val statistics =
    Statistics(sizeInBytes = child.sqlContext.defaultSizeInBytes)

  val partitionStatistics = new PartitionStatistics(output)

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    buildBuffers()
  }

  def recache() = {
    _cachedColumnBuffers.unpersist()
    _cachedColumnBuffers = null
    buildBuffers()
  }

  private def buildBuffers(): Unit = {
    val output = child.output
    val cached = child.execute().mapPartitions { rowIterator =>
      new Iterator[CachedBatch] {
        def next() = {
          val columnBuilders = output.map { attribute =>
            val columnType = ColumnType(attribute.dataType)
            val initialBufferSize = columnType.defaultSize * batchSize
            ColumnBuilder(columnType.typeId, initialBufferSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          while (rowIterator.hasNext && rowCount < batchSize) {
            val row = rowIterator.next()
            var i = 0
            while (i < row.length) {
              columnBuilders(i).appendFrom(row, i)
              i += 1
            }
            rowCount += 1
          }

          val stats = Row.fromSeq(
            columnBuilders.map(_.columnStats.collectedStatistics).foldLeft(Seq.empty[Any])(_ ++ _))

          //stats: GenericRow(key.lower, key.upper, key.nullcount, value.lower, value.upper, value.nullcount)

          CachedBatch(columnBuilders.map(_.build().array()), stats)
        }

        def hasNext = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(child.toString)
    _cachedColumnBuffers = cached
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(
      newOutput, useCompression, batchSize, storageLevel, child)(_cachedColumnBuffers)
  }

  override def children = Seq.empty

  override def newInstance() = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child)(
      _cachedColumnBuffers).asInstanceOf[this.type]
  }

  def cachedColumnBuffers = _cachedColumnBuffers
}

private[sql] case class InMemoryColumnarTableScan(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    relation: InMemoryRelation)
  extends LeafNode {

  @transient override val sqlContext = relation.child.sqlContext

  override def output: Seq[Attribute] = attributes

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) && buildFilter(rhs)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, l: Literal) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      aStats.lowerBound <= l && l <= aStats.upperBound

    case EqualTo(l: Literal, a: AttributeReference) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      aStats.lowerBound <= l && l <= aStats.upperBound

    case LessThan(a: AttributeReference, l: Literal) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      aStats.lowerBound < l

    case LessThan(l: Literal, a: AttributeReference) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      l < aStats.upperBound

    case LessThanOrEqual(a: AttributeReference, l: Literal) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      aStats.lowerBound <= l

    case LessThanOrEqual(l: Literal, a: AttributeReference) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      l <= aStats.upperBound

    case GreaterThan(a: AttributeReference, l: Literal) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      l < aStats.upperBound

    case GreaterThan(l: Literal, a: AttributeReference) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      aStats.lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, l: Literal) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      l <= aStats.upperBound

    case GreaterThanOrEqual(l: Literal, a: AttributeReference) =>
      val aStats = relation.partitionStatistics.forAttribute(a)
      aStats.lowerBound <= l
  }

  /*
     key<20 and value>40
     According to buildFilter, we get key.lowerBound < 20 and value.upperBound > 40
     By bindReference function, we get further tranfrom it to input[0] < 20 and input[4] > 40.
     The index of input refers to the postition in relation.partitionStatistics.schema
   */
  val partitionFilters = {  //@zengdan 找到predicate中涉及列存在统计信息的，返回对应的filter，过滤规则在buildFilter中定义
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            relation.partitionStatistics.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  // Accumulators used for testing purposes
  val readPartitions = sparkContext.accumulator(0)
  val readBatches = sparkContext.accumulator(0)

  private val inMemoryPartitionPruningEnabled = sqlContext.inMemoryPartitionPruning

  override def execute() = {
    readPartitions.setValue(0)
    readBatches.setValue(0)

    relation.cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        relation.partitionStatistics.schema)
      //@zengdan
      //a where condition corresponds to a partitionFilter, when there exists more than one where condition,
      // we should combine them with "And".
      //Then bind the attribute in partitionFilters with relation.partitionStatistics.schema again,
      //this time i think it does so to get the columntype in order to evaluate the expression??

      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (attributes.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =
          relation.output.zipWithIndex.map { case (a, ordinal) =>
            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        attributes.map { a =>
          relation.output.indexWhere(_.exprId == a.exprId) -> a.dataType
        }.unzip
      }

      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)

      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]) = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.map { batch =>
            ColumnAccessor(ByteBuffer.wrap(cachedBatch.buffers(batch)))  //括号内为取出第batch－1列的buffer，即Array[Byte]
          }

          // Extract rows via column accessors
          new Iterator[Row] {
            override def next() = {
              var i = 0
              while (i < nextRow.length) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              nextRow
            }

            override def hasNext = columnAccessors(0).hasNext
          }
        }

        if (rows.hasNext) {
          readPartitions += 1
        }

        rows
      }

      // Do partition batch pruning if enabled
      val cachedBatchesToScan =
        if (inMemoryPartitionPruningEnabled) {
          cachedBatchIterator.filter { cachedBatch =>
            if (!partitionFilter(cachedBatch.stats)) {
              def statsString = relation.partitionStatistics.schema
                .zip(cachedBatch.stats)
                .map { case (a, s) => s"${a.name}: $s" }
                .mkString(", ")
              logInfo(s"Skipping partition based on stats $statsString")
              false
            } else {
              readBatches += 1
              true
            }
          }
        } else {
          cachedBatchIterator
        }

      cachedBatchesToRows(cachedBatchesToScan)
    }
  }
}
