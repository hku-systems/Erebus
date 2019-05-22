package edu.hku.dp

import edu.hku.cs.dp.dpread
import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object TPCH13DP {

  def special(x: String): Boolean = {
    x.matches(".*special.*requests.*")
  }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen"

    val customer_input = new dpread(spark.sparkContext.textFile(inputDir + "/customer.tbl*"))
      .mapDP(_.split('|'))
      .mapDPKV(p =>
      (p(0).trim.toLong,1))

    val order_input = new dpread(spark.sparkContext.textFile(inputDir + "/orders.tbl*"))
      .mapDP(_.split('|'))
      .mapDPKV(p =>
      (p(1).trim.toLong, p(0).trim.toLong))

    val final_result = customer_input
      .joinDP(order_input)
      .mapDPKV(p => ((p._1,p._2._2),p._2._1))
      .reduceByKeyDP((a,b) => a + b)
//      .map(p => (p._2,1))
//      .reduceByKey((a,b) => a + b)

    final_result.addnoiseQ41()

  }
}