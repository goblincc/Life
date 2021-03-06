package graphx

import graphx.GraphConnect.getNdegree
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import utils.TimeUtils

object TextGraph {
  def main(args: Array[String]): Unit = {
    val dt = TimeUtils.changFormat(args(0))
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .config("spark.executor.memoryOverhead", "2G")
      .config("spark.rdd.compress","true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    val sc = spark.sparkContext
    val sqlTxt =
      s"""
         |select uid as vertices, "uid" as types  from  persona.yylive_nick_target_d where dt = '2021-11-16'
         |union
         |select nick as vertices, "nick" as types from persona.yylive_nick_target_d where dt = '2021-11-16'
       """.stripMargin

    val verticesDataFrame = spark.sql(sqlTxt).cache()
    verticesDataFrame.show(10, false)

    val verticesRdd: RDD[(String, Long)] = verticesDataFrame.rdd.map(p => {
      p.getAs[String]("vertices")
    }).zipWithUniqueId()

    verticesRdd.toDF("vertices", "id").createOrReplaceTempView("table_index")

    spark.sql(
      s"""
         |insert overwrite table persona.yylive_uid_nick_table_index partition(dt='${dt}')
         |select * from table_index
       """.stripMargin)

    val vertices: RDD[(VertexId, String)] = verticesDataFrame.rdd.map(p => {
      (p.getString(0), p.getString(1))
    }).join(verticesRdd, 500).map(p => {
      (p._2._2, p._2._1)
    }).repartition(500)

    val sqlTxt2 =
      s"""
         |select nick as src, uid as dst, "nick_conn" as conn from persona.yylive_nick_target_d where dt = '2021-11-16' group by nick, uid, conn
       """.stripMargin

    spark.sql(sqlTxt2).createOrReplaceTempView("table_edge")

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
    }).repartition(500)

    val graph = Graph(vertices, relationships)

    getNdegree(4, graph, vertices, spark, dt)

    spark.close()
  }

  /**
    *
    * @param n  ??????n?????????
    * @param graph  ?????????
    * @param spark
    * @param dt ??????
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
         |insert overwrite table persona.yylive_uid_nick_ndgree_tmp partition(dt='${dt}')
         |select * from ndegree
       """.stripMargin)

  }

  /**
    * ?????????????????????vdata??????????????????message???????????????
    */
  def vprog(vid: VertexId, vdata: Map[VertexId, (Int, String)], message: Map[VertexId, (Int, String)])
  : Map[VertexId, (Int, String)] = {
    mergeMsg(vdata, message)
  }

  /**
    * ??????????????????????????????
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
    * ?????????????????????????????????msg1???msg2???????????????
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
