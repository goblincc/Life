package analysis

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TextLSH {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("error")
    import spark.implicits._
    val sqlTxt =
      s"""
         |select uid, newnick from yy_channel.nick_histroy_dt where dt >= '20210601' and length(newnick) >= 2 group by uid, newnick
       """.stripMargin
    val data = spark.sql(sqlTxt)

    val keyWordTxt =
      s"""
         |select keys, value from persona.tb_ks_keywd_reflect_keyword
         |union
         |select keys, value from persona.tb_ks_keywd_reflect_lettnum
       """.stripMargin

    val keyWordMap: collection.Map[String, String] = spark.sql(keyWordTxt).rdd.map(p => {
      (p.getAs[String](0), p.getAs[String](1))
    }).collectAsMap()

    val keyWordMaps: Broadcast[collection.Map[String, String]] = sc.broadcast(keyWordMap)

    val datas = data.rdd.map(p=>{
      val keyword: collection.Map[String, String] = keyWordMaps.value
      val uid = p.getAs[Long](0)
      val nick = p.getAs[String](1)
      (uid, nick.toCharArray.map(p=>keyword.getOrElse(p.toString, p)).mkString("|").split("\\|"))
    }).toDF("uid", "nick")
    datas.show(5, false)

    val word2Vec = new Word2Vec()
      .setInputCol("nick")
      .setOutputCol("nickVec")
      .setVectorSize(5)
      .setMinCount(0)
    val word2VecModel = word2Vec.fit(datas)
    val idfData = word2VecModel.transform(datas)

    idfData.show(10, false)

    val brp = new BucketedRandomProjectionLSH()
      .setBucketLength(0.2)
      .setNumHashTables(3)
      .setInputCol("nickVec")
      .setOutputCol("bucketId")

    val model = brp.fit(idfData)

    // Feature Transformation
    println("The hashed dataset where hashed values are stored in the column 'hashes':")
    model.transform(idfData).show(20, false)

    val candidate_sql =
      s"""
         |  SELECT uid
         |   FROM zbasezx.tb_tw_text_punish_hist_dt
         |   WHERE biz_code IN (999980240,999980194)
         |     AND audit_status='S02'
         |     AND dt >= '20210601'
         |   GROUP BY uid
       """.stripMargin
    val candidates = spark.sql(candidate_sql)
    val candidate = candidates.join(idfData,"uid").select("uid", "nickVec", "nick")

    val result = model.approxSimilarityJoin(candidate, candidate, 10, "EuclideanDistance")
      .select(col("datasetA.nick").alias("ida"),
        col("datasetA.uid").alias("uid1"),
        col("datasetB.nick").alias("idb"),
        col("datasetB.uid").alias("uid2"),
        col("EuclideanDistance").alias("dist"))
    result.show(5, false)

    result.createOrReplaceTempView("result_db")
    spark.sql(
      s"""
         |insert overwrite table persona.yylive_txt_dist partition(dt='2021-11-09')
         |	select ida,uid1,idb, uid2, dist from result_db
       """.stripMargin)
  }
}
