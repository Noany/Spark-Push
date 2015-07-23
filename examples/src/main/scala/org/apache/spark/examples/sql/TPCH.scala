package org.apache.spark.examples.sql

/**
 * Created by zengdan on 14-12-10.
 */
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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SchemaRDD}

/*
case class Lineitem(L_ORDERKEY: Int, L_PARTKEY: Int, L_SUPPKEY: Int, L_LINENUMBER: Int,
                    L_QUANTITY: Double, L_EXTENDEDPRICE: Double, L_DISCOUNT: Double, L_TAX: Double,
                    L_RETURNFLAG: String, L_LINESTATUS: String, L_SHIPDATE: String, L_COMMITDATE: String,
                    L_RECEIPTDATE: String, L_SHIPINSTRUCT: String, L_SHIPMODE: String, L_COMMENT: String)

case class Orders(O_ORDERKEY: Int, O_CUSTKEY: Int, O_ORDERSTATUS: String, O_TOTALPRICE: Double,
                  O_ORDERDATE: String, O_ORDERPRIORITY: String, O_CLERK: String, O_SHIPPRIORITY: Int,
                  O_COMMENT: String)
                  */

case class Part(P_PARTKEY: Int, P_NAME: String, P_MFGR: String, P_BRAND: String, P_TYPE: String, P_SIZE: Int,
                P_CONTAINER: String, P_RETAILPRICE: Double, P_COMMENT: String)

case class Supplier(S_SUPPKEY: Int, S_NAME: String, S_ADDRESS: String, S_NATIONKEY: Int, S_PHONE: String,
                    S_ACCTBAL: Double, S_COMMENT: String)

case class Partsupp(PS_PARTKEY: Int, PS_SUPPKEY: Int, PS_AVAILQTY: Int, PS_SUPPLYCOST: Double, PS_COMMENT: String)

case class Nation(N_NATIONKEY: Int, N_NAME: String, N_REGIONKEY: Int, N_COMMENT: String)

case class Region(R_REGIONKEY: Int, R_NAME: String, R_COMMENT: String)

case class Customer(C_CUSTKEY: Int, C_NAME: String, C_ADDRESS: String, C_NATIONKEY: Int, C_PHONE: String,
                    C_ACCTBAL: Double, C_MKTSEGMENT: String, C_COMMENT: String)

object TPCH {

  def main(args: Array[String]) {

    var start = System.nanoTime
    //System.setProperty("spark.push.mode", args(1));
    //System.setProperty("spark.array.mode", args(2))
    val sparkConf = new SparkConf()
    //sparkConf.set("spark.push.mode", args(1))

    val sc = new SparkContext(sparkConf.setAppName("SQL Testing"))

    val sqlContext = new SQLContext(sc)

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    import sqlContext._

    val file1 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/lineitem/lineitem.tbl")
    val file2 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/orders/orders.tbl")
    val file3 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/part/part.tbl")
    val file4 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/supplier/supplier.tbl")
    val file5 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/partsupp/partsupp.tbl")
    val file6 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/nation/nation.tbl")
    val file7 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/region/region.tbl")
    val file8 = sqlContext.sparkContext.textFile("hdfs://localhost:9000/user/zengdan/customer/customer.tbl")
    //val file2 = sqlContext.sparkContext.textFile(args(1))
    //file1.cache()

    val table_lineitem = file1.map(_.split('|')).map(l => Lineitem(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toInt,
      l(4).toDouble, l(5).toDouble, l(6).toDouble, l(7).toDouble, l(8), l(9), l(10), l(11), l(12), l(13), l(14), l(15)))

    val table_orders = file2.map(_.split('|')).map(l => Orders(l(0).toInt, l(1).toInt, l(2), l(3).toDouble,
      l(4), l(5), l(6), l(7).toInt, l(8)))

    val table_part = file3.map(_.split('|')).map(l => Part(l(0).toInt, l(1), l(2), l(3), l(4), l(5).toInt,
      l(6), l(7).toDouble, l(8)))

    val table_supplier = file4.map(_.split('|')).map(l => Supplier(l(0).toInt, l(1), l(2), l(3).toInt,l(4),
      l(5).toDouble, l(6)))

    val table_partsupp = file5.map(_.split('|')).map(l => Partsupp(l(0).toInt, l(1).toInt, l(2).toInt, l(3).toDouble,l(4)))

    val table_nation = file6.map(_.split('|')).map(l => Nation(l(0).toInt, l(1), l(2).toInt, l(3)))

    val table_region = file7.map(_.split('|')).map(l => Region(l(0).toInt, l(1), l(2)))

    val table_customer = file8.map(_.split('|')).map(l => Customer(l(0).toInt, l(1), l(2), l(3).toInt, l(4),
      l(5).toDouble, l(6), l(7)))


    table_lineitem.registerTempTable("lineitem")
    table_orders.registerTempTable("orders")
    table_part.registerTempTable("part")
    table_supplier.registerTempTable("supplier")
    table_partsupp.registerTempTable("partsupp")
    table_nation.registerTempTable("nation")
    table_region.registerTempTable("region")
    table_customer.registerTempTable("customer")

    hiveContext.sql("Create external table if not exists lineitem (L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, " +
      "L_QUANTITY DOUBLE, L_EXTENDEDPRICE DOUBLE, L_DISCOUNT DOUBLE, L_TAX DOUBLE, L_RETURNFLAG STRING, L_LINESTATUS STRING, " +
      "L_SHIPDATE STRING, L_COMMITDATE STRING, L_RECEIPTDATE STRING, L_SHIPINSTRUCT STRING, L_SHIPMODE STRING, L_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/lineitem'")
    hiveContext.sql("create external table if NOT EXISTS orders (O_ORDERKEY INT, O_CUSTKEY INT, O_ORDERSTATUS STRING, " +
      "O_TOTALPRICE DOUBLE, O_ORDERDATE STRING, O_ORDERPRIORITY STRING, O_CLERK STRING, O_SHIPPRIORITY INT, O_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/orders'")
    hiveContext.sql("create external table if not exists part (P_PARTKEY INT, P_NAME STRING, P_MFGR STRING, " +
      "P_BRAND STRING, P_TYPE STRING, P_SIZE INT, P_CONTAINER STRING, P_RETAILPRICE DOUBLE, P_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/part'")
    hiveContext.sql("create external table if not exists supplier (S_SUPPKEY INT, S_NAME STRING, S_ADDRESS STRING, " +
      "S_NATIONKEY INT, S_PHONE STRING, S_ACCTBAL DOUBLE, S_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/supplier'")
    hiveContext.sql("create external table if not exists partsupp (PS_PARTKEY INT, PS_SUPPKEY INT, PS_AVAILQTY INT, " +
      "PS_SUPPLYCOST DOUBLE, PS_COMMENT STRING) location 'hdfs://localhost:9000/user/zengdan/partsupp'")
    hiveContext.sql("create external table if not exists nation (N_NATIONKEY INT, N_NAME STRING, N_REGIONKEY INT, N_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/nation'")
    hiveContext.sql("create external table if not exists region (R_REGIONKEY INT, R_NAME STRING, R_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/region'")
    hiveContext.sql("create external table if NOT EXISTS customer (C_CUSTKEY INT, C_NAME STRING, C_ADDRESS STRING, " +
      "C_NATIONKEY INT, C_PHONE STRING, C_ACCTBAL DOUBLE, C_MKTSEGMENT STRING, C_COMMENT STRING)" +
      " location 'hdfs://localhost:9000/user/zengdan/customer'")




    val rdds = new Array[SchemaRDD](22)


    rdds(0) = sql("SELECT L_RETURNFLAG, L_LINESTATUS, SUM(L_QUANTITY), SUM(L_EXTENDEDPRICE), " +
      "SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)), SUM(L_EXTENDEDPRICE*(1-L_DISCOUNT)*(1+L_TAX)), " +
      "AVG(L_QUANTITY), AVG(L_EXTENDEDPRICE), AVG(L_DISCOUNT), COUNT(1) FROM lineitem " +
      "GROUP BY L_RETURNFLAG, L_LINESTATUS ")
    //WHERE L_SHIPDATE<='1998-09-02'  ORDER BY L_RETURNFLAG, L_LINESTATUS

    ///*
    hiveContext.sql("create table if not exists q2_minimum_cost_supplier_tmp1 (s_acctbal double, s_name string, n_name string, " +
      "p_partkey int, ps_supplycost double, p_mfgr string, s_address string, s_phone string, s_comment string)" +
      " location 'hdfs://localhost:9000/user/zengdan/q2_minimum_cost_supplier_tmp1'")
    hiveContext.sql("create table if not exists q2_minimum_cost_supplier_tmp2 (p_partkey int, ps_min_supplycost double)" +
      " location 'hdfs://localhost:9000/user/zengdan/q2_minimum_cost_supplier_tmp2'")
    hiveContext.sql("create table if not exists q2_minimum_cost_supplier (s_acctbal double, s_name string, n_name string, " +
      "p_partkey int, p_mfgr string, s_address string, s_phone string, s_comment string)" +
      " location 'hdfs://localhost:9000/user/zengdan/q2_minimum_cost_supplier'")


    rdds(1) = hiveContext.sql("insert overwrite table q2_minimum_cost_supplier_tmp1 select  s.s_acctbal, s.s_name, n.n_name, " +
      "p.p_partkey, ps.ps_supplycost, p.p_mfgr, s.s_address, s.s_phone, s.s_comment from  nation n join region r  on " +
      "n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE' join supplier s on s.s_nationkey = n.n_nationkey join partsupp ps" +
      " on s.s_suppkey = ps.ps_suppkey  join part p  on p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS'")

    /*
    rdds(1) = hiveContext.sql("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, " +
      "supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and " +
      "p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'" +
      //"ps_supplycost = (select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey" +
      //"and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE')" +
      "order by s_acctbal desc, n_name, s_name, p_partkey")
    */


    rdds(0).queryExecution.executedPlan.foreach(println)
  }

}