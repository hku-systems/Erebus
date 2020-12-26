package edu.hku.cs.dp

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import breeze.linalg.{DenseVector, Vector}
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.math.{exp, pow}
import scala.reflect.ClassTag

/**
  * Created by lionon on 10/28/18.
  */
class dpobjectKV[K, V](var inputsample: RDD[(K, V)], var inputsample_advance: RDD[(K, V)], var inputoriginal: RDD[(K, V)])
                      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends Logging with Serializable
{
  var sample = inputsample
  var sample_advance = inputsample_advance
  var original = inputoriginal
  val epsilon = 0.1
  val delta = pow(10,-8)
  val k_distance_double = 1/epsilon
  val k_distance = k_distance_double.toInt
  val beta = epsilon / (2*scala.math.log(2/delta))

  def filterDPKV(f: ((K,V)) => Boolean) : dpobjectKV[K, V] = {

    val t1 = System.nanoTime
    val r1 = inputsample.filter(f)
    val r2 = inputsample_advance.filter(f)
    val r3 = inputoriginal.filter(f)
    val duration = (System.nanoTime - t1) / 1e9d
    println("filter: " + duration)
    new dpobjectKV(r1,r2,r3)
  }
  //********************Join****************************
  def joinDP[W](otherDP: RDD[(K, W)]): dpobjectArray[((K, (W, V)))] = {

    //No need to care about sample2 join sample1
    val t1 = System.nanoTime

    val joinresult = original.join(otherDP).map(p => (p._1,(p._2._2,p._2._1)))

    val advance_original = sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._2,p._2._1._1)),p._2._1._2))

    val with_sample = sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._2,p._2._1._1)),p._2._1._2))

    val duration = (System.nanoTime - t1) / 1e9d
    println("join: " + duration)

    new dpobjectArray(with_sample,advance_original,joinresult)
  }

  def joinDP_original[W](otherDP: RDD[(K, W)]): dpobjectArray[((K, (V, W)))] = {

    //No need to care about sample2 join sample1
    val t1 = System.nanoTime

    val joinresult = original.join(otherDP).map(q => (q._1,(q._2._1,q._2._2)))

    val advance_original = sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    val with_sample = sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    val duration = (System.nanoTime - t1) / 1e9d
    println("join: " + duration)

    new dpobjectArray(with_sample,advance_original,joinresult)
  }

  def joinDP[W](otherDP: dpobjectKV[K, W]): dpobjectArray[((K, (V, W)))] = {

    //No need to care about sample2 join sample1
    val t1 = System.nanoTime

    val input2 = otherDP.original
    val input2_sample = otherDP.original
    val joinresult = original.join(otherDP.original)

    val zipin_advance = otherDP.sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
    val original_advance = original
      .join(zipin_advance)
      .map(p => ((p._1,(p._2._1,p._2._2._1)),p._2._2._2))

    val advance_original = sample_advance
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(otherDP.original)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))
    //        val advance_advance = sample_advance.join(otherDP.sample_advance)


    val with_sample = sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
      .join(input2)
      .map(p => ((p._1,(p._2._1._1,p._2._2)),p._2._1._2))

    val zipin = otherDP.sample
      .zipWithIndex()
      .map(p => (p._1._1,(p._1._2,p._2)))
    val with_input2_sample = original
      .join(zipin)
      .map(p => ((p._1,(p._2._1,p._2._2._1)),p._2._2._2))

    //        val samples_join = sample.join(input2_sample)

    //        print("array1: ")
    //        with_sample.union(with_input2_sample).union(samples_join).groupByKey.collect().foreach(println)
    //        print("array2: ")
    //          original_advance.union(advance_original).union(advance_advance).groupByKey.collect().foreach(println)

    //This is final original result because there is no inter key
    //or intra key combination for join i.e., no over lapping scenario
    //within or between keys
    val duration = (System.nanoTime - t1) / 1e9d
    println("join: " + duration)
    new dpobjectArray(with_sample ++ with_input2_sample,original_advance ++ advance_original,joinresult)

  }
}