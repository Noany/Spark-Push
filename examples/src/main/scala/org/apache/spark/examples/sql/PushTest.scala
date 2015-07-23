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

package org.apache.spark.examples.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.expressions.Row

case class Lineitem(L_ORDERKEY: Int, L_PARTKEY: Int, L_SUPPKEY: Int, L_LINENUMBER: Int,
                    L_QUANTITY: Double, L_EXTENDEDPRICE: Double, L_DISCOUNT: Double, L_TAX: Double,
                    L_RETURNFLAG: String, L_LINESTATUS: String, L_SHIPDATE: String, L_COMMITDATE: String,
                    L_RECEIPTDATE: String, L_SHIPINSTRUCT: String, L_SHIPMODE: String, L_COMMENT: String)

case class Orders(O_ORDERKEY: Int, O_CUSTKEY: Int, O_ORDERSTATUS: String, O_TOTALPRICE: Double,
                   O_ORDERDATE: String, O_ORDERPRIORITY: String, O_CLERK: String, O_SHIPPRIORITY: Int,
                   O_COMMENT: String)

object PushTest {

  final val MS_PER_SECOND = 1000

  def main(args: Array[String]) {

    var start = System.nanoTime
    System.setProperty("spark.push.mode", args(1));
    System.setProperty("spark.array.mode", args(2))
    val sparkConf = new SparkConf()
    //sparkConf.set("spark.push.mode", args(1))

    val sc = new SparkContext(sparkConf.setAppName("SQL Testing"))

    val sqlContext = new SQLContext(sc)

    import sqlContext._

    val file1 = sqlContext.sparkContext.textFile(args(0))

    val table_lineitem = file1.map(_.split('|')).map(l => Lineitem(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toInt,
      l(4).toDouble, l(5).toDouble, l(6).toDouble, l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15)))

    //val table_orders = file2.map(_.split("\\|")).map(l => Orders(l(0).toInt, l(1).toInt, l(2), l(3).toDouble,
    //  l(4), l(5), l(6), l(7).toInt, l(8)))

    table_lineitem.registerTempTable("lineitem")
    //table_orders.registerTempTable("orders")

    val rdd = sql("SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), " +
      "SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)), " +
      "AVG(L_QUANTITY), AVG(L_EXTENDEDPRICE), AVG(L_DISCOUNT), COUNT(1) FROM lineitem " +
      "WHERE L_SHIPDATE < '1993-06-27' GROUP BY L_RETURNFLAG, L_LINESTATUS")

    println("====Time used before sql is %f s====".format((System.nanoTime - start) / 1e9))


    start = System.nanoTime()

    rdd.collect().foreach(println)
    //println(s"COUNT(*): $count")
    println("====Time used running sql is %f s====".format((System.nanoTime - start) / 1e9))


    /*
    Q4
    INSERT OVERWRITE TABLE q4_order_priority_tmp
      select
    DISTINCT l_orderkey
      from
    lineitem
    where
    l_commitdate < l_receiptdate;
    INSERT OVERWRITE TABLE q4_order_priority
      select o_orderpriority, count(1) as order_count
    from
    orders o join q4_order_priority_tmp t
    on
    o.o_orderkey = t.o_orderkey and o.o_orderdate >= '1993-07-01' and o.o_orderdate < '1993-10-01'
    group by o_orderpriority
    order by o_orderpriority;
    */

    /*
    Q1
    SELECT
    L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)), AVG(L_QUANTITY), AVG(L_EXTENDEDPRICE), AVG(L_DISCOUNT), COUNT(1)
    FROM
    lineitem
    WHERE
    L_SHIPDATE<='1998-09-02'
    GROUP BY L_RETURNFLAG, L_LINESTATUS
    ORDER BY L_RETURNFLAG, L_LINESTATUS

    select
    sum(l_extendedprice*l_discount) as revenue
    from
    lineitem
    where
    l_shipdate >= '1994-01-01'
    and l_shipdate < '1995-01-01'
    and l_discount >= 0.05 and l_discount <= 0.07
    and l_quantity < 24;
    */
  }

}