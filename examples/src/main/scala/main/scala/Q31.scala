package main.scala

import org.apache.spark.sql.SparkSession

/**
 * TPC-H Query 1
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
object Q31 {

  def decrease(x: Double, y: Double): Double = {
    x * (1 - y)
  }

  def increase(x: Double, y: Double): Double = {
    x * (1 + y)
  }

  def removenan(r: Double): Double = {if (r.isNaN) Double.MinValue else r }

  def main(args: Array[String]): Unit = {
    // this is used to implicitly convert an RDD to a DataFrame.
    val spark = SparkSession
      .builder
      .appName("TpchQuery1")
      .getOrCreate()
    val inputDir = "/home/john/tpch-spark/dbgen/ground-truth"
//    schemaProvider.lineitem.filter($"l_shipdate" <= "1998-09-02")
//      .groupBy($"l_returnflag", $"l_linestatus")
//      .agg(sum($"l_quantity"), sum($"l_extendedprice"),
//        sum(decrease($"l_extendedprice", $"l_discount")),
//        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
//        avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
//      .sort($"l_returnflag", $"l_linestatus")

    val filtered_result = spark.sparkContext.textFile(inputDir + "/lineitem" + args(0) + ".ftbl*").map(_.split('|')).map(p =>
      (p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim))
//      .map(case (l_orderkey: Long, l_partkey: Long, l_suppkey: Long, l_linenumber: Long, l_quantity: Double, l_extendedprice: Double, l_discount:Double, l_tax:Double, l_returnflag:String, l_linestatus:String, l_shipdate:String, l_commitdate:String, l_receiptdate:String, l_shipinstruct:String, l_shipmode:String, l_comment:String))
      .filter(_._11 < "1998-09-02")
      .map(p => {
        val inter = decrease(p._6,p._7)
        ((p._9,p._10),(p._5,p._6, p._7, inter,increase(inter,p._8),1))
      })
      .reduceByKey((a,b) => {
      val x = decrease(a._2,a._3)
      val y = decrease(b._2,b._3)
      (a._1 + b._1, a._2 + b._2, a._3 + b._3 , a._4 + b._4, a._5 + b._5, a._6 + b._6)
    })

    filtered_result.collect().foreach(p => print(args(0) + ":" + p._1._1 + "," + p._1._2 + ":" + p._2._1 + "," + p._2._2 + "," + p._2._3 + "," + p._2._4 + "," + p._2._5 + "," + p._2._6 + "\n"))

  }
}