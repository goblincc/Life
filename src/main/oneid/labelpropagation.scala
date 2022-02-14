package oneid

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import utils.TimeUtils

object labelpropagation {
  def main(args: Array[String]): Unit = {
    val dt = args(0)
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .config("spark.executor.memoryOverhead", "2G")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    val sc = spark.sparkContext

    val sql_vertext =
      s"""
         |select hdid as vertices,"hdid" as types from persona.hl_idmapping_uid_hdid_active_90_filter where dt = '${dt}' and cnt >= 2
         |union
         |select uid as vertices,"uid" as types from persona.hl_idmapping_uid_hdid_active_90_filter where dt = '${dt}' and cnt >= 2
         |""".stripMargin
    println("sql_vertext:",sql_vertext)
    val verticesDataFrame = spark.sql(sql_vertext).persist(StorageLevel.MEMORY_AND_DISK)
    verticesDataFrame.createOrReplaceTempView("table_vertices")

    val verticesRdd: RDD[(String, Long)] = verticesDataFrame.rdd.map(p => {
      p.getAs[String]("vertices")
    }).zipWithUniqueId().persist(StorageLevel.MEMORY_AND_DISK)

    verticesRdd.toDF("vertices", "id").createOrReplaceTempView("table_index")

    //节点存储到表
    spark.sql(
      s"""
         |insert overwrite table persona.hl_oneid_table_index partition(dt='${dt}')
         |select * from table_index
       """.stripMargin)

    val vertices: RDD[(VertexId, String)] = verticesDataFrame.rdd.map(p => {
      (p.getString(0), p.getString(1))
    }).join(verticesRdd, 1000).map(p => {
      (p._2._2, p._2._1)
    })

    val sql_edge =
      s"""
         |select hdid, uid, cnt as conn from persona.hl_idmapping_uid_hdid_active_90_filter where dt = '${dt}' and cnt >= 2
         |""".stripMargin

    spark.sql(sql_edge).toDF("src", "dst","conn").createOrReplaceTempView("table_edge")

    val relationships: RDD[Edge[Long]] = spark.sql(
      s"""
         |SELECT srcid,
         |       id AS dstid,
         |       conn
         |FROM
         |  (SELECT id AS srcid,
         |          dst,
         |          conn
         |   FROM table_edge AS a
         |   INNER JOIN table_index AS b ON a.src = b.vertices) AS a
         |INNER JOIN table_index AS b ON a.dst = b.vertices
       """.stripMargin).rdd.map(p=>{
      Edge(p.getLong(0), p.getLong(1), p.getLong(2))
    })

    //构图
    val graph = Graph(vertices, relationships)
    val graph_community = LPARevolution.run(graph,20)
    graph_community.toDF("verticesid","categoryid").createOrReplaceTempView("table_connect")

    val result = spark.sql(
      s"""
         |insert overwrite table persona.oneid_groups partition(dt='${dt}')
         |SELECT b.vertices,
         |       types,
         |       categoryid
         |FROM table_connect AS a
         |INNER JOIN table_index AS b ON a.verticesid = b.id
         |INNER JOIN table_vertices AS c ON b.vertices = c.vertices
       """.stripMargin)
    spark.close()

  }
}
