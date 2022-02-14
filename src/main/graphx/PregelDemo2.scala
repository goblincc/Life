package graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PregelDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .master("local[1]")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    val sc = spark.sparkContext
    val users: RDD[(VertexId, String)] =
      sc.parallelize(Array(
        (3L, "uid"),
        (7L, "uid"),
        (5L, "ip"),
        (2L, "ip"),
        (4L, "uid"),
        (10L, "ip"),
        (16L, "ip"),
        (0L, "ip")
      ))

    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(
        Edge(3L, 7L, "collab"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "pi"),
        Edge(4L, 10L, "student"),
        Edge(5L, 0L, "colleague"),
        Edge(0L, 2L, "colleague"),
        Edge(7L, 16L, "colleague")
      ))

    val graph = Graph(users, relationships)

    //定义n度关系
    val n = 4

    //每个节点开始的时候只存储了（该节点编号，n）这一个键值对
    val newG = graph.mapVertices((vid, attr) => Map[VertexId, (Int, String)](vid -> (n, attr)))
      .pregel(Map[VertexId, (Int, String)](), n, EdgeDirection.Out)(vprog, sendMsg, mergeMsg)

    newG.vertices.mapValues(_.filter(p =>p._2._1 <= 2 && p._2._2 == "ip" ).keys)
      .filter(p=>p._2 != Set()).join(users)
      .map(p=>(p._1, p._2._1, p._2._2))
      .filter(_._3 == "uid")
      .map(p=>p._1 + "节点的"+ n + "度关系节点-->" + p._2.mkString(","))
      .foreach(println(_))
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
