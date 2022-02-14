package graphx

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.util.Random

object demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd1 = sc.makeRDD(Array(("1","Spark"),("2","Hadoop"),("3","Scala"),("4","Java")),2)
    val rdd3 = sc.makeRDD(Array(("Spark","1"),("Hadoop","2"),("Scala","3"),("Java","4")),2)
    val rdd2 = sc.makeRDD(Array(("1","30K"),("2","15K"),("3","25K"),("5","10K")),2)

//    rdd1.join(rdd2).collect.foreach(println)
//    rdd1.join(rdd3).collect.foreach(println)
    val map1 = Map(2 -> 3, 0 -> 2, 5 -> 2)
    val map2 = Map(0 -> 3, 2 -> 2, 6 -> 2)
    val map3 = Map("a" -> 3, "b" -> 2, "c" -> 4, "d" -> 4)
    println(map1.keySet--map2.keySet)

//    val value: RDD[(Int, Int)] = sc.makeRDD(Array((1, 2), (2, 2), (3, 2), (4, 3), (3, 3)))
//    value.groupBy(_._2).mapValues(p=>{
//      p.toList(Random.nextInt(p.size))
//    }).foreach(println(_))
    val tuple: (String, Int) = map3.maxBy(_._2)
    println(tuple)
  }

}
