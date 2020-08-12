package sahibinden

import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{Normalizer, OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.SparkSession

object GBTPipeline {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark=SparkSession.builder()
      .appName("GbtRegressionPipelineY")
      .master("local")
      .getOrCreate()
    import spark.implicits._

    val df=spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferschema","true")
      .load("D:\\sahibinden_records/sahibinden_clean_data_merkez2.csv")


    var newCols = Array("m2","oda","il","ilce","label")


    //df.show()
    val df2 = df.toDF(newCols:_*)

    df2.printSchema()
    df2.describe().show()



    //String indexer
    val odaStrIndexer=new StringIndexer()
      .setInputCol("oda")
      .setOutputCol("odaIndex")

    val ilStrIndexer=new StringIndexer()
      .setInputCol("il")
      .setOutputCol("ilIndex")

    val ilceStrIndexer=new StringIndexer()
      .setInputCol("ilce")
      .setOutputCol("ilceIndex")



    //OneHotEncoder
    val encoder=new OneHotEncoderEstimator()
      .setInputCols(Array[String]("odaIndex","ilIndex","ilceIndex"))
      .setOutputCols(Array[String]("odaIndexEncoded","ilIndexEncoded","ilceIndexEncoded"))



    //VectorAssembler
    val vectorAssembler=new VectorAssembler()
      .setInputCols(Array[String]("m2","odaIndexEncoded","ilIndexEncoded","ilceIndexEncoded"))
      .setOutputCol("vectorFeatures")


    //standardScaler
    val standardScaler=new StandardScaler()
      .setInputCol("vectorFeatures")
      .setOutputCol("scalerfeatures")

    //normalizer
    val normalizer =new Normalizer()
      .setInputCol("scalerfeatures")
      .setOutputCol("features")


    //train and test data split
    val Array(trainingData, testData) = df2.randomSplit(Array(0.9,0.1),1234)

    val starttime = Calendar.getInstance().getTime()

    val gbtRegressionObj= new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(150)
      .setMaxBins(50)


    //Pipeline Model
    val pipelineObj=new Pipeline()
      .setStages(Array(odaStrIndexer,ilStrIndexer,ilceStrIndexer,encoder,vectorAssembler,standardScaler,normalizer,gbtRegressionObj))

    val pipelineModel=pipelineObj.fit(trainingData)
    val pipelineModelDF= pipelineModel.transform(testData).select("m2","oda","il","ilce","label","prediction")

    pipelineModelDF.show()
    val predictDF= Seq((150,"2+1","ankara","mamak")).toDF("m2","oda","il","ilce")
    pipelineModel.transform(predictDF).select("m2","oda","il","ilce","prediction").show()


    pipelineModel.write.overwrite().save("gbtRegression.modelPipelineFinal2")


    //EVALUATE THE MODEL ON TEST DATA
    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")
    val r2 = evaluator.evaluate(pipelineModelDF)
    println("R-sqr on test data = " + r2)

    val evaluator2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
    val mAccuracy = evaluator2.evaluate(pipelineModelDF)
    println("Test set rmse = " + mAccuracy)


    import java.util.Calendar
    val endtime = Calendar.getInstance.getTime
    val elapsedtime = ((endtime.getTime - starttime.getTime) / 1000).toString
    println("Time taken to run the above cell: " + elapsedtime + " seconds.")



    /*
    R-sqr on test data = 0.6303795787763278
    Time taken to run the above cell: 11295 seconds.
     */

    spark.stop()
  }
}
