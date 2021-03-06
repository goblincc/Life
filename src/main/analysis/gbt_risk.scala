package analysis

import analysis.gbtTrain_active.tf_idf
import analysis.gbt_risk_predict.saveData2hive
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.TimeUtils

import scala.collection.mutable.ListBuffer

object gbt_risk {
  def main(args: Array[String]): Unit = {
    val dts = args(0)
    val dt = TimeUtils.changFormat(dts)
    val delta_dt = TimeUtils.addDate(dts, -90)
    val spark = SparkSession.builder()
      .config("spark.hadoop.validateOutputSpecs", value = false)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("error")

    val sql_txt =
      s"""
         |SELECT
         |    label,
         |    uid,
         |    statuscode_list,
         |    appid_list,
         |    is_nick_modifys,
         |    has_idnumber,
         |    is_nick_modifys,
         |    has_idnumber,
         |    cnt_90,
         |    day_90,
         |    sid_90,
         |    sid_all_90,
         |    avg_sid_cnt_90,
         |    avg_sid_90,
         |    sub_cnt,
         |    cont_d_90,
         |    cont_all_90,
         |    avg_cont_90,
         |    gift_cnt_90,
         |    sum_90,
         |    stddev_90,
         |    avg_90,
         |    alldt_90,
         |    amount_90,
         |    avg_amount_90,
         |    stddev_amount_90,
         |    chid_90,
         |    paymethod_90,
         |    userip_90,
         |    max_cnt_90,
         |    avg_cnt_90,
         |    no_active_90,
         |    no_active_pay_90,
         |    max_cnt,
         |    avg_cnt,
         |    avg_delta_time,
         |    avg_all_90,
         |    stddev_all_90,
         |    max_device_cnt,
         |    avg_device_cnt,
         |    stdev_device_cnt,
         |    appid_cnt,
         |    buyerid_cnt,
         |    hdid_cnt,
         |    apporderid_cnt,
         |    reg_day,
         |    fail_rate,
         |    max_ip_cnt as max_ip_cnts,
         |    avg_ip_cnt as avg_ip_cnts,
         |    stdev_ip_cnt as stdev_ip_cnts,
         |    nvl(chid_90/alldt_90,0) AS avg_chid_times_90,
         |    nvl(paymethod_90/alldt_90,0) AS avg_method_times_90,
         |    nvl(buyerid_cnt/alldt_90,0) AS buyer_pay_ratio
         |   FROM
         |  persona.yylive_uid_feature_info_label
         |   WHERE dt='${dt}' and cast(uid as bigint) > 0
         |union
         |SELECT
         |label,
         |uid,
         |statuscode_list,
         |    appid_list,
         |    is_nick_modifys,
         |    has_idnumber,
         |    is_nick_modifys,
         |    has_idnumber,
         |    cnt_90,
         |    day_90,
         |    sid_90,
         |    sid_all_90,
         |    avg_sid_cnt_90,
         |    avg_sid_90,
         |    sub_cnt,
         |    cont_d_90,
         |    cont_all_90,
         |    avg_cont_90,
         |    gift_cnt_90,
         |    sum_90,
         |    stddev_90,
         |    avg_90,
         |    alldt_90,
         |    amount_90,
         |    avg_amount_90,
         |    stddev_amount_90,
         |    chid_90,
         |    paymethod_90,
         |    userip_90,
         |    max_cnt_90,
         |    avg_cnt_90,
         |    no_active_90,
         |    no_active_pay_90,
         |    max_cnt,
         |    avg_cnt,
         |    avg_delta_time,
         |    avg_all_90,
         |    stddev_all_90,
         |    max_device_cnt,
         |    avg_device_cnt,
         |    stdev_device_cnt,
         |    appid_cnt,
         |    buyerid_cnt,
         |    hdid_cnt,
         |    apporderid_cnt,
         |    reg_day,
         |    fail_rate,
         |    max_ip_cnt as max_ip_cnts,
         |    avg_ip_cnt as avg_ip_cnts,
         |    stdev_ip_cnt as stdev_ip_cnts,
         |    nvl(chid_90/alldt_90,0) AS avg_chid_times_90,
         |    nvl(paymethod_90/alldt_90,0) AS avg_method_times_90,
         |    nvl(buyerid_cnt/alldt_90,0) AS buyer_pay_ratio
         |   FROM
         |  persona.yylive_uid_feature_info_label
         |   WHERE dt='${delta_dt}' and cast(uid as bigint) > 0 and label = 1
       """.stripMargin
    println("sql_txt:", sql_txt)
    val df = spark.sql(sql_txt).persist(StorageLevel.MEMORY_AND_DISK)

    val splits = df.randomSplit(Array(0.7, 0.3), seed = 11L)

    //??????????????????
    val training = sampleData(splits(0))
    println("train_pos:"+ training.where("label = 1.0").count())
    println("train_neg:"+ training.where("label = 0.0").count())

    //???????????????????????????
    val test = splits(1)
    println("test_pos:"+ test.where("label = 1.0").count())
    println("test_neg:"+ test.where("label = 0.0").count())

    val num_features = Array(
      "avg_chid_times_90", "avg_method_times_90","buyer_pay_ratio",
      "cnt_90", "day_90", "sid_90" ,
      "sid_all_90", "avg_sid_cnt_90", "avg_sid_90",
      "sub_cnt", "cont_d_90", "cont_all_90", "avg_cont_90",
      "gift_cnt_90", "sum_90", "stddev_90", "avg_90",
      "alldt_90", "amount_90", "avg_amount_90",
      "stddev_amount_90", "chid_90", "paymethod_90",
      "userip_90", "max_cnt_90", "avg_cnt_90", "no_active_90",
      "no_active_pay_90",  "max_cnt", "avg_cnt",
      "avg_delta_time", "avg_all_90",
      "stddev_all_90", "max_device_cnt", "avg_device_cnt",
      "stdev_device_cnt", "max_ip_cnts", "avg_ip_cnts",
      "stdev_ip_cnts", "appid_cnt", "buyerid_cnt",
      "hdid_cnt", "apporderid_cnt", "reg_day", "fail_rate"
    )

    val stagesArray = new ListBuffer[PipelineStage]()

    val formula =
      s"""
         |label ~ is_nick_modifys + has_idnumber
       """.stripMargin

    val rformula = new RFormula()
      .setFormula(formula)
      .setFeaturesCol("catVec")
      .setLabelCol("label")
      .setHandleInvalid("skip")

    stagesArray.append(rformula)

//    tf_idf_stage(stagesArray, "events_list_90", 20)
//    tf_idf_stage(stagesArray, "events_list_60", 20)
//    tf_idf_stage(stagesArray, "events_list_30", 20)
    tf_idf_stage(stagesArray, "statuscode_list", 10)
    tf_idf_stage(stagesArray, "appid_list", 10)

    val assembler = new VectorAssembler()
      .setInputCols(num_features ++ Array("catVec","statuscode_list_vec", "appid_list_vec"))
      .setOutputCol("assemble")
      .setHandleInvalid("skip")
    stagesArray.append(assembler)

    val trainer = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("assemble")
      .setMaxIter(20)
    stagesArray.append(trainer)

    val pipeline = new Pipeline()
      .setStages(stagesArray.toArray)

    val model = pipeline.fit(training)

    val output = "hdfs://yycluster02/hive_warehouse/persona_client.db/chenchang/risk"
    model.write.overwrite().save(output + s"/piperisk_${dts}")

    val traindf = model.transform(training)
    traindf.select("label", "prediction")
      .createOrReplaceTempView("traindf")
    getIndicators(spark, "traindf")

    val testpred = model.transform(test)
    val testdf = testpred.cache()
    testdf.select("label", "prediction")
      .createOrReplaceTempView("testdf")
    getIndicators(spark, "testdf")

    import spark.implicits._

    //????????????
    testdf.select("label", "prediction", "probability").rdd.map(p => {
      val label = p.getAs[Int](0)
      val prediction = if (p.getAs[DenseVector](2)(1) >= 0.7) 1.0 else 0.0
      (label.toDouble, prediction)
    }).toDF("label", "prediction")
      .createOrReplaceTempView("testDF_7")
    getIndicators(spark, "testDF_7")

    //????????????2
    testdf.select("label", "prediction", "probability").rdd.map(p => {
      val label = p.getAs[Int](0)
      val prediction = if (p.getAs[DenseVector](2)(1) >= 0.8) 1.0 else 0.0
      (label.toDouble, prediction)
    }).toDF("label", "prediction")
      .createOrReplaceTempView("testDF_8")
    getIndicators(spark, "testDF_8")


    //????????????3
    testdf.select("label", "prediction", "probability").rdd.map(p => {
      val label = p.getAs[Int](0)
      val prediction = if (p.getAs[DenseVector](2)(1) >= 0.9) 1.0 else 0.0
      (label.toDouble, prediction)
    }).toDF("label", "prediction")
      .createOrReplaceTempView("testDF_9")
    getIndicators(spark, "testDF_9")

    val evaluator = new BinaryClassificationEvaluator()
    evaluator.setMetricName("areaUnderROC")
    val testAuc= evaluator.evaluate(testdf)
    println(" test auc:" + testAuc)

    val gbtModel = model.stages(8).asInstanceOf[GBTClassificationModel]
    println("importance:"+ gbtModel.featureImportances)

    val predict = model.transform(df)
    saveData2hive(spark, dt, predict)

  }

  def getIndicators(sparkSession: SparkSession, table: String): Unit ={
    val matrix = sparkSession.sql(
      s"""
        select
            '${table}' as type
            ,predict_cnt
            ,(TP + FN) as real_cnt
            ,(TP + FP) as predict_real_cnt
            ,(TP + TN)/ predict_cnt as accuarcy
            ,TP/(TP + FP) as precise
            ,TP/(TP + FN) as recall
            ,TP
            ,FP
            ,TN
            ,FN
        from (
            select
                count(*) as predict_cnt
                ,count(if(label = 1.0 and prediction= 1.0, 1, null)) as TP
                ,count(if(label = 0.0 and prediction= 1.0, 1, null)) as FP
                ,count(if(label = 0.0 and prediction= 0.0, 1, null)) as TN
                ,count(if(label = 1.0 and prediction= 0.0, 1, null)) as FN
            from ${table}
        )b
        """
    )
    matrix.show()
  }

  def sampleData(data: DataFrame): DataFrame ={
    val pos_data = data.where("label = 1.0")
    val neg_data = data.where("label = 0.0")
    val ratio = pos_data.count() * 1.0/neg_data.count()
    println("pos_data", pos_data.count())
    println("neg_data", neg_data.count())
    val dataFrame = pos_data.union(neg_data.sample(false, ratio * 200))
    dataFrame
  }


  def tf_idf_stage(stagesArray:ListBuffer[PipelineStage], input: String, numFeatures: Int): Unit ={
    val tokenizer = new RegexTokenizer()
      .setInputCol(input)
      .setOutputCol(input + "_token")
      .setPattern(",")
    stagesArray.append(tokenizer)

    val hashingTF = new HashingTF()
      .setInputCol(input + "_token").setOutputCol(input + "_tf").setNumFeatures(numFeatures)
    stagesArray.append(hashingTF)

    val idf = new IDF().setInputCol(input + "_tf").setOutputCol(input + "_vec")
    stagesArray.append(idf)

  }

  def saveData2hive(spark:SparkSession, dt: String, dataFrame: DataFrame): Unit ={
    val structFields = Array(
      StructField("uid",StringType,true),
      StructField("prediction",DoubleType,true),
      StructField("probability",DoubleType,true)
    )
    val structType = DataTypes.createStructType(structFields)
    val row: RDD[Row] = dataFrame.select("uid", "prediction", "probability").rdd.map(p => {
      val uid = p.getString(0)
      val prediction = p.getDouble(1)
      val probability = p.getAs[DenseVector](2)(1)
      Row(uid, prediction, probability)
    })

    spark.createDataFrame(row,structType).createOrReplaceTempView("tb_save")

    spark.sql(
      s"""
         |insert overwrite table persona.yylive_uid_feature_train_predict partition(dt='${dt}')
         |	select * from tb_save
       """.stripMargin)
  }
}
