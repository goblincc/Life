package graphx

import graphx.PregelDemo2.mergeMsg
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.TimeUtils

object GraphConnect {
  def main(args: Array[String]): Unit = {
    val dt = TimeUtils.changFormat(args(0))
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .config("spark.executor.memoryOverhead", "2G")
//      .config("spark.speculation", value = false)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlTxt =
      s"""
         |select uid as vertices, "uid" as types from persona.yylive_uid_mobile_info where dt = '${dt}'
         |union
         |select mobile as vertices, "mobile" as types from persona.yylive_uid_mobile_info where dt = '${dt}'
         |union
         |select uid as vertices, "uid" as types from persona.yylive_uid_idnum_info where dt = '${dt}'
         |union
         |select idnum as vertices, "idnum" as types from persona.yylive_uid_idnum_info where dt = '${dt}'
       """.stripMargin

    val verticesDataFrame = spark.sql(sqlTxt).persist(StorageLevel.MEMORY_AND_DISK)
    verticesDataFrame.createOrReplaceTempView("table_vertices")

    val verticesRdd: RDD[(String, Long)] = verticesDataFrame.rdd.map(p => {
      p.getAs[String]("vertices")
    }).zipWithUniqueId().persist(StorageLevel.MEMORY_AND_DISK)

    verticesRdd.toDF("vertices", "id").createOrReplaceTempView("table_index")

    spark.sql(
      s"""
         |insert overwrite table persona.yylive_uid_table_index partition(dt='${dt}')
         |select * from table_index
       """.stripMargin)

    val vertices: RDD[(VertexId, String)] = verticesDataFrame.rdd.map(p => {
      (p.getString(0), p.getString(1))
    }).join(verticesRdd, 500).map(p => {
      (p._2._2, p._2._1)
    })

    val sqlTxt2 =
      s"""
         |select mobile, uid, "mobile_conn" as conn from persona.yylive_uid_mobile_info where dt = '${dt}'
         |union
         |select idnum, uid, "idnum_conn" as conn from persona.yylive_uid_idnum_info where dt = '${dt}'
       """.stripMargin

    spark.sql(sqlTxt2).toDF("src", "dst", "conn").createOrReplaceTempView("table_edge")

    val relationships: RDD[Edge[String]] = spark.sql(
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
      Edge(p.getLong(0), p.getLong(1), p.getString(2))
    })

    val graph = Graph(vertices, relationships)
    val connectVertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    connectVertices.toDF("verticesid","categoryid").createOrReplaceTempView("table_connect")

    val result = spark.sql(
      s"""
         |insert overwrite table persona.yylive_uid_risk_groups partition(dt='${dt}')
         |SELECT b.vertices,
         |       types,
         |       categoryid
         |FROM table_connect AS a
         |INNER JOIN table_index AS b ON a.verticesid = b.id
         |INNER JOIN table_vertices AS c ON b.vertices = c.vertices
       """.stripMargin)

    getNdegree(4, graph, vertices, spark, dt)

    spark.close()
  }

  /**
    *
    * @param n  定义n度关系
    * @param graph  图网络
    * @param spark
    * @param dt 日期
    * @return
    */
  def getNdegree(n: Int, graph: Graph[String, String],vertices: RDD[(VertexId, String)]
                 , spark: SparkSession, dt: String)={
    import spark.implicits._
    val newG: Graph[Map[VertexId, (Int, String)], String] = graph.mapVertices((vid, attr) => Map[VertexId, (Int, String)](vid -> (n, attr)))
      .pregel(Map[VertexId, (Int, String)](), n, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)

    newG.vertices.mapValues(_.filter(p => p._2._1 <= 2 && p._2._2 == "uid" ).keys)
      .filter(_._2 != Set()).join(vertices)
      .map(p=>(p._1, p._2._1, p._2._2))
      .filter(_._3 == "uid")
      .map(p=>(p._1, p._2.mkString(",")))
      .toDF("item", "nlist")
      .createOrReplaceTempView("ndegree")

    spark.sql(
      s"""
         |insert overwrite table persona.yylive_uid_ndgree_tmp partition(dt='${dt}')
         |select * from ndegree
       """.stripMargin)

  }

  /**
    * 更新节点数据，vdata为本身数据，message为消息数据
    */
  def vprog(vid: VertexId, vdata: Map[VertexId, (Int, String)], message: Map[VertexId, (Int, String)])
  : Map[VertexId, (Int, String)] = {
    mergeMsg(vdata, message)
  }

  /**
    * 节点更新数据发送消息
    */
  def sendMsg(e: EdgeTriplet[Map[VertexId, (Int, String)], String]) = {
    val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k)._1 - 1, e.dstAttr(k)._2) }.toMap
    val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k)._1 - 1, e.srcAttr(k)._2) }.toMap
    if (srcMap.isEmpty && dstMap.isEmpty)
      Iterator.empty
    else
      Iterator((e.dstId, dstMap), (e.srcId, srcMap))
  }

  /**
    * 对于交集的点的处理，取msg1和msg2中最小的值
    */
  def mergeMsg(msg1: Map[VertexId, (Int, String)], msg2: Map[VertexId, (Int, String)]): Map[VertexId, (Int, String)] =
    (msg1.keySet ++ msg2.keySet).map {
      k =>{
        val s1 = msg1.getOrElse(k, (Int.MaxValue,""))
        val s2 = msg2.getOrElse(k, (Int.MaxValue, ""))
        if(s1._1 <= s2._1){
          k->(s1._1, s1._2)
        }else{
          k->(s2._1, s2._2)
        }
      }
    }.toMap

}
