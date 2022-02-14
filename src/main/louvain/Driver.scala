package louvain

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object Driver {

  def main(args: Array[String]): Unit ={

    val config = LouvainConfig(
      "./data/sample.txt",
      "./data/output/",
      20,
      2000,
      1,
      ",")


    //单机版时需要指定，否则在saveAsTextFile时会报nullpoint错误 或者设置hadoop环境变量
    System.setProperty("hadoop.home.dir", "E:\\hadoop-2.7.7")

    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    // deleteOutputDir(config)

    val louvain = new Louvain()
    louvain.run(sc, config)

  }
}
