package sahibinden

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

object GBTPipelineTahmin {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession.builder()
      .appName("GbtRegressionPipelineElleTahmin2")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val pipelineModel=PipelineModel.load("gbtRegression.modelPipelineFinal")

    val predictDF= Seq((150,"3+1","ankara","akyurt")).toDF("m2","oda","il","ilce")
    pipelineModel.transform(predictDF).select("m2","oda","il","ilce","prediction").show()

  }
}
