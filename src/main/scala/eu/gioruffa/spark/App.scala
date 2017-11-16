package eu.gioruffa.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
 * @author ${user.name}
 */
object App {
  

  def main(args : Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Exercise 2")
      .master("local[4]") //can put with vm option -Dspark.master=local in run configuration
      .config("spark.executor.memory","4g")
      .getOrCreate()

//    WOOOOOOOOW!
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._ //inject functions in regular scala objects, like toDF()

    case class  Page(project : String, page : String, numReq : Long, size : Long)
//    val df = spark.read.text("/data/UniData/BigData/exercises/exercise1/pagecounts-20100806-030000")
    var df = spark.read
      .option("delimiter"," ")
      .option("inferSchema", true)
      .csv("/data/UniData/BigData/exercises/exercise1/pagecounts-20100806-030000")
      .withColumnRenamed("_c0", "project") //set the name of the columns
      .withColumnRenamed("_c2", "numReq")
      .withColumn("numReq",col("numReq").cast("long")) //col is in the import, the way to cast without infer

    df = df.toDF(Array("project", "name", "numReq", "length"):_*) //":_*" array to args
    df.printSchema()
    df.show(10)

    val totalRecords = df.count()
    val uniqueNames = df.select("name").distinct// .groupBy("name").count().select("name")

    //print all of them
    uniqueNames.collectAsList() //returns a list of rows

    //alternative accessing every row inside the rdd
    //lo sta facendo per averli tutto
    //altrimenti con take() prima devi fare il count -> costa
    uniqueNames.rdd.map(_.getString(0)).collect()

//    ?? Why not a user defined function?


    df.filter(col("project") === "en") //get only english

    println(s"total $totalRecords, unique ${uniqueNames.count()}")
    val onlyEng =df.where("project like  'en'")
    onlyEng.cache()
    onlyEng.select("project","length").groupBy("project").sum("length").show(10)
    //a posto di ".sum" puoi usare ".agg(sum("length"))"
    //ma e' meglio perche' puoi fare
    //".agg(sum("length").as("nuovonomecolonna"))"
    onlyEng.select("name","numReq").groupBy("name").sum("numReq").sort($"sum(numReq)".desc).show(5)

    val projectAverage = df.groupBy("project")
      .mean("numReq")
        .withColumnRenamed("avg(numReq)","avgreq")
      .sort($"avgreq".desc)
    //.sort(col("avgreq").desc) //modo piu' esplicito
    projectAverage.show(10)


    df.join(projectAverage,"project")
      .where("numReq > avgreq")
      .show(100)

    //Now with SQL
    df.createOrReplaceTempView("myFakeTable") //give it a name in sql session
    spark.sql("select distinct project from myFakeTable").show()
    //the problem is that with SQL there is no linter or compiler for verification
    //and maybe the query fail after 2 hours of runs
    //using the API you have compile time verification and linting

    //la funzione .count del dataframe ritorna un numero perche' conta gli elementi dell'rdd
    //la funzione .count del grouped element ritorna un df perche' e' una aggregazione sui gruppi


    df.groupBy("project")
      .agg(
        count("name").as("totpages"), //every one is a column object
        sum("length").as("totlength"),
        avg("numReq").as("avgreq")
      ) //SUPER COOOL! multiple aggregations! Do not need

  }

}
