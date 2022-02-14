package oneid

import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object LPARevolution {
  def sendMessage(e: EdgeTriplet[VertexId, Long]): Iterator[(VertexId, Map[VertexId, Long])] = {
    Iterator((e.srcId, Map(e.dstAttr -> e.attr)), (e.dstId, Map(e.srcAttr -> e.attr)))
  }

  def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
  : Map[VertexId, Long] = {
    (count1.keySet ++ count2.keySet).map { i =>
      val count1Val = count1.getOrElse(i, 0L)
      val count2Val = count2.getOrElse(i, 0L)
      i -> (count1Val + count2Val)
    }(collection.breakOut)
  }

  // 更新点属性
  def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
    if (message.isEmpty) {
      attr
    }
    else {
//                  print(vid)
//                  println(" 接收到的消息：   ")
//                  println(message)
//                  println("最终选择的是：")
//                  println(message.maxBy(_._2)._1)
      message.maxBy(_._2)._1 // 按照计数排序，然后取第一个
    }
  }

  def run[VD](graph: org.apache.spark.graphx.Graph[VD, Long], maxSteps: scala.Int): RDD[(VertexId, VertexId)] = {
    // 图初始化
    val initGraph = graph.mapVertices { case (vid, attr) => vid }

    // 初始化msg
    val initialMessage = Map[VertexId, Long]()

    println("迭代结果：")

    // 分水岭，开始解决社区震荡&孤立点问题
    // ----------------------------------  迭代多轮  -------------------------------------
    val cluster1 = Pregel(initGraph, initialMessage, maxIterations = maxSteps, activeDirection = EdgeDirection.Either)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    println("第二轮迭代：")

    // =====================================================================
    // 优雅代码的核心部分，基于前面的结果初始化新的图
    // 利用前面迭代结果重新初始化图
    // 以此结果作为基础，后续在此基础上继续迭代
    val users_trans = cluster1.vertices
    val graph_trans = Graph(vertices = users_trans, edges = graph.edges)
    val initGraph_trans = graph_trans.mapVertices { case (vid, attr) => attr }
    // ======================================================================

    // 在基础数据上，额外迭代的轮数
    val cluster2 = Pregel(initGraph_trans, initialMessage, maxIterations = 1, activeDirection = EdgeDirection.Either)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    val cluster3 = Pregel(initGraph_trans, initialMessage, maxIterations = 2, activeDirection = EdgeDirection.Either)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    val cluster4 = Pregel(initGraph_trans, initialMessage, maxIterations = 3, activeDirection = EdgeDirection.Either)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)

    val clusters = users_trans.union(cluster2.vertices).union(cluster3.vertices).union(cluster4.vertices)
    val result = clusters.groupBy(_._1).map(x => {
      val map = mutable.Map[VertexId, Int]()
      x._2.foreach(y => {
        if (map.contains(y._2)) {
          map(y._2) += 1
        } else {
          map(y._2) = 1
        }
      })
      val gid = map.maxBy(a => (a._2, a._1))._1
      (x._1, gid)
    })
    result
//    cluster1.vertices
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .master("local[1]")
      .getOrCreate() //      .enableHiveSupport()
    spark.sparkContext.setLogLevel("error")
    val sc = spark.sparkContext

    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Seq((1L, ("rxin", "student")), (2L, ("jgonzal", "postdoc")), (3L, ("hello", "xxxx")), (4L, ("hello", "xxxx")), (5L, ("hello", "xxxx"))
      ,(6L, ("hello", "xxxx")), (7L, ("hello", "xxxx")), (8L, ("hello", "xxxx"))))

    val edges: RDD[Edge[Long]] = sc.parallelize(Seq(Edge(1L, 2L, 1L), Edge(2L, 3L, 1L),
      Edge(2L, 4L, 1L), Edge(2L, 5L, 1L), Edge(5L, 6L, 1L), Edge(5L, 8L, 1L),
      Edge(7L, 8L, 1L), Edge(6L, 7L, 1L)))

    val graph = Graph(vertices = users, edges = edges)
    val result = run(graph, 5)
    println("最终结果：")
    result.foreach(println(_))
    sc.stop()
  }

}
