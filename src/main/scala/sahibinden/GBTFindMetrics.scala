package sahibinden

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

object GBTFindMetrics {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark=SparkSession.builder()
      .appName("GbtRegressionPipelineRsquare2")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println()
    val df=spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferschema","true")
      .load("D:\\sahibinden_records/sahibinden_clean_data_merkezXXX.csv")

    var newCols = Array("m2","oda","il","ilce","label")
    val df2 = df.toDF(newCols:_*)

    //train ve test data ayırma
    val Array(trainingData,testData) = df2.randomSplit(Array(0.9,0.1))


    val pipModel=PipelineModel.load("gbtRegression.modelPipelineFinal")
    val gbtdf=pipModel.transform(testData)

    gbtdf.select("m2","oda","il","ilce","label","prediction").show(30)
    import org.apache.spark.sql.functions.rand
    val shuffledDF = gbtdf.orderBy(rand())
    shuffledDF.select("m2","oda","il","ilce","label","prediction").show(30)

    import org.apache.spark.ml.evaluation.RegressionEvaluator

    //Root Mean Squared Error
    val evaluatorRMSE = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
    val rmse = evaluatorRMSE.evaluate(gbtdf)

    //Mean Squared Error
    val evaluatorMSE = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mse")
    val mse = evaluatorMSE.evaluate(gbtdf)

    //Regression through the origin
    val evaluatorR2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")
    val r2 = evaluatorR2.evaluate(gbtdf)

    //Mean absolute error
    val evaluatorMAE = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("mae")
    val mae = evaluatorMAE.evaluate(gbtdf)

    println("Regression through the origin(R2) = " + r2)
    println("Root Mean Squared Error(RMSE)= " + rmse)
    println("Mean squared error(MSE) = " + mse)
    println("Mean absolute error(MAE)= " + mae)

  }
}
