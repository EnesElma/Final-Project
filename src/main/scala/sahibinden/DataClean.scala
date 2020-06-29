package sahibinden

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, lower, trim, when}

object DataClean {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark= SparkSession.builder()
      .appName("SahibindenProject")
      .master("local")
      .getOrCreate()



    val myDF=spark.read.format("csv")
      .option("header",true)
      .option("sep",",")
      .option("inferschema",true)
      .load("D:\\sahibinden_records/sahibinden_dirty_data.csv")


    import spark.implicits._
    /*
        val x= myDF.groupBy($"binaYasi")
          .agg(count($"*").as("sayi"))
          .orderBy(desc("binaYasi"))

        x.show(x.count.toInt)

        myDF.orderBy(desc("binaYasi")).show(100,false)

        myDF.filter(row => row.getAs[String]("metreKare").matches("""\d+""")).show(5)
        myDF.filter($"metreKare".contains("180000")).show(false)
        println(myDF5.filter($"metreKare".gt(400) && $"metreKare".lt(1000)).count())
        val myDF6=myDF5.filter($"metreKare".lt(500))
    */

    //id kolonu silindi:
    val myDF2=myDF.drop("_id")


    // text boşluklarını temizle:
    val myDF3 = myDF2
      .withColumn("oda", trim(($"oda")))
      .withColumn("il", trim(($"il")))
      .withColumn("ilce", trim(($"ilce")))


    //null olan veriler silindi:
    val myDF4=myDF3.na.drop()       //na = not available
    /*  Veya:
    val myDF4 = myDF3.filter($"fiyat".isNotNull)
      .filter($"oda".isNotNull)
      .filter($"metreKare".isNotNull)
      .filter($"il".isNotNull)
      .filter($"ilce".isNotNull)
     */





    //Oda kategorileri birleştirme(merged):
    val myDF5=myDF4
      .withColumn("oda",
        when(col("oda").isin("1+1","1.5+1"),"1+1")
          .when(col("oda").isin("2+1","2.5+1","2+2","2+0"),"2+1")
          .when(col("oda").isin("3+1","3.5+1"),"3+1")
          .when(col("oda").isin("4+2","4.5+1","4+3","4+4"),"4+2")
          .when(col("oda").isin("5+2","5+3","5+4"),"5+2")
          .when(col("oda").isin("6+1","6+2","6+3"),"6+1")
          .otherwise(col("oda"))
      )


    val x= myDF5.groupBy($"oda")
      .agg(count($"*").as("sayi"))
      .orderBy(desc("oda"))

    x.show(x.count.toInt)


    //metreKare de ki tutarsız verilerin düzenlenmesi ve birleştirme(merged):
    val myDF6=myDF5
      .filter(col("fiyat").lt(2000001))

    myDF6.describe().show()


    //il,ilçe ve mahalle kolonlarını küçük harf yapma:
    val myDF7=myDF6
      .withColumn("il",lower($"il"))
      .withColumn("ilce",lower($"ilce"))

    val myDF8=myDF7.select("m2","oda","il","ilce","fiyat")



    import org.apache.spark.sql.functions.udf
    def myFun= udf(
      (x : String,y:String) =>  {
        x+"-"+y
      }
    )
    val myDF9=myDF8.withColumn("ilce", when($"ilce" === "merkez", myFun($"il",$"ilce"))
      .otherwise($"ilce"))



    val count_df = myDF9.select($"ilce").groupBy($"ilce").count()
    val join_df = myDF9.join(count_df, myDF9.col("ilce") === count_df.col("ilce")).drop(count_df.col("ilce"))
    val new_df = join_df.select("m2","oda","il","ilce","fiyat").filter($"count" >= 50)

    new_df.show(30)



    val ilce= new_df.groupBy($"ilce")
      .agg(count($"*").as("sayi"))
      .orderBy(desc("sayi"))

    ilce.show(ilce.count.toInt)


    //yeni dataframe diske kaydetme:
    new_df
      .coalesce(1)         // tek parça olması için
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("D:\\sahibinden_records/sahibinden_clean_data_merkezXXX.csv")


  }
}
