package edu.hku.cs.dp

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}

import scala.reflect.ClassTag


/**
  * Created by lionon on 10/22/18.
  */
class dpread_c[T: ClassTag](
  var rdd1 : RDD[String])
{
  var main = rdd1.map(p => {
    val s = p.split(";/;")
    (s(0),s(1).trim.toInt)
  })

  def mapDP[U: ClassTag](f: String => U, rate: Int): dpobject_c[U]= {
    val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
    val t1 = System.nanoTime
    var advance_sampling = main
    var sampling = main
    if (parameters(2).toInt == 1) {
      advance_sampling = main.sample(false,parameters(4).toDouble,1)
      sampling = main.sample(false,parameters(4).toDouble,1)
    }
    else {
      advance_sampling = main.sparkContext.parallelize(main.take(rate + parameters(0).toInt))
      sampling = main.sparkContext.parallelize(main.take(rate + parameters(0).toInt))
    }
//    val sampling = main.sparkContext.parallelize(main.take(rate))
      val a = sampling.map(p => (f(p._1),p._2))
      val b = advance_sampling.map(p => f(p._1))
      if(parameters(2).toInt == 1)// 1 means tune accuracy
      {
          val duration = (System.nanoTime - t1) / 1e9d
          println("Sampling execution time: " + duration)
          new dpobject_c(a,b,main.subtract(sampling).map(p => (f(p._1),p._2)))
      }
      else
      {
        val duration = (System.nanoTime - t1) / 1e9d
        println("Sampling execution time: " + duration)
        new dpobject_c(a,b,main.map(p => (f(p._1),p._2)))
      }
  }


    def mapDPKV[K: ClassTag,V: ClassTag](f: String => (K,V), rate: Int): dpobjectKV_c[K,V]= {
      val parameters = scala.io.Source.fromFile("security.csv").mkString.split(",")
      val t1 = System.nanoTime
      var advance_sampling = main
      var sampling = main
      if (parameters(2).toInt == 1) {
        advance_sampling = main.sample(false,parameters(4).toDouble,1)
        sampling = main.sample(false,parameters(4).toDouble,1)
      }
      else {
        advance_sampling = main.sparkContext.parallelize(main.take(rate + parameters(0).toInt))
        sampling = main.sparkContext.parallelize(main.take(rate + parameters(0).toInt))
      }
//      val sampling = main.sparkContext.parallelize(main.take(rate))
        val a = sampling.map(p => (f(p._1),p._2))
        val b = advance_sampling.map(p => f(p._1))
      if(parameters(2).toInt == 1)// 1 means tune accuracy
      {
        val duration = (System.nanoTime - t1) / 1e9d
        println("Sampling execution time: " + duration)
        new dpobjectKV_c(a,b,main.subtract(sampling).map(p => (f(p._1),p._2)))
      }
      else
      {
        val duration = (System.nanoTime - t1) / 1e9d
        println("Sampling execution time: " + duration)
        new dpobjectKV_c(a,b,main.map(p => (f(p._1),p._2)))
      }
    }

//  def mapfilter[U: ClassTag](f: String => U, rate: Int): dpfilter[U]= {
//    new dpfilter(main.map(f))
//  }
}
