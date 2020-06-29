package sahibinden

import java.util.Calendar

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



    println("-------------------------------------------------")
    val starttime1 = Calendar.getInstance().getTime()
    val predictDF1= Seq((150,"3+1","ankara","akyurt")).toDF("m2","oda","il","ilce")
    val dfx=pipelineModel.transform(predictDF1).select("m2","oda","il","ilce","prediction")

    dfx.show()
    val x=dfx.select("prediction").as[Double].collect()(0)

    println(x.toInt)

    import java.util.Calendar
    val endtime1 = Calendar.getInstance.getTime
    val elapsedtime1 = ((endtime1.getTime - starttime1.getTime) / 1000).toString
    println("ilk tahmin süresi " + elapsedtime1 + " seconds.")



    val starttime = Calendar.getInstance().getTime()
    val predictDF= Seq((75,"2+1","bayburt","demirözü")).toDF("m2","oda","il","ilce")
    pipelineModel.transform(predictDF).select("m2","oda","il","ilce","prediction").show()

    import java.util.Calendar
    val endtime = Calendar.getInstance.getTime
    val elapsedtime = ((endtime.getTime - starttime.getTime) / 1000).toString
    println("ikinci tahmin süresi " + elapsedtime + " seconds.")
  }
}
