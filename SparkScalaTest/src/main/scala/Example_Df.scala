


import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.streaming.{Seconds, StreamingContext}





object Example_Df extends App {

  val sConf = new SparkConf().setMaster("local[*]").setAppName("test1")
  val sc = new SparkContext(sConf)

  val ss = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()
  import ss.implicits._
  val macollection=List((1,"Judith",41),(36,"Bertrand",25),(12,"Diana",45))
  //val maDf =  macollection.toDF("Id","Prenom","age")

  //val sqlContext=new SQLContext(sc)
  val rowsRDD= sc.parallelize(Seq(Row("John",17),Row("Franck",40),Row("Eloïse",30)))
  val schema=new StructType().add(StructField("Nom",StringType,true))
    .add(StructField("Age",IntegerType,true))

  val df=ss.createDataFrame(rowsRDD,schema)
  df.createGlobalTempView("equipe")

  val dfSQL= ss.sql("select * from global_temp.equipe where age > 20")


  val dataf= List((0,"Hello"),(1,"World")).toDF("Id","Text")
  val upperUDF = udf {s:String=>s.toUpperCase}
  dataf.withColumn("upper",upperUDF('Text)).show()

  // lecture CSV

  val maDf= ss.read.format("csv").option("header","true").option("delimiter",",")
    .load("/home/wilder/Téléchargements/titanic.csv")
  //maDf.show()

  //lecture Json

  val dfJson= ss.read.format("json").load("/opt/spark/examples/src/main/resources/people.json")

  //dfJson.show()

  //lecture Parquet

  val dfParquet= ss.read.format("parquet")
    .load("/opt/spark/examples/src/main/resources/users.parquet")

  //dfParquet.show()

  // lecture mySQL

  val dfmySQL= ss.read.format("jdbc")
    .option("url","jdbc:mysql://localhost:3306/test4")
    .option("driver","com.mysql.jdbc.Driver")
    .option("dbtable", "cars")
    .option("user","root")
    .option("password","root").load()

  dfmySQL.show()

  val maDf2 = List((1001,"BMW","Turbo",2001,"AZE56VEBH")).toDF("id","constructeur","modele"
  ,"year","immatriculation")

  maDf2.write.mode(org.apache.spark.sql.SaveMode.Append).format("jdbc")
    .option("url","jdbc:mysql://localhost:3306/test4")
    //.option("driver","com.mysql.jdbc.Driver")
    .option("dbtable", "cars")
    .option("user","root")
    .option("password","root")
    .save()
}


object Example_Df2 extends App {
  val sConf = new SparkConf().setMaster("local[*]").setAppName("test1")
  val sc = new SparkContext(sConf)

  val ss = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()
  import ss.implicits._

  // exercice section 7
  val maDf= ss.read.format("csv").option("header","true").option("delimiter",",")
    .load("/home/wilder/Documents/dataset/scores.csv")

  maDf.show()

  val lensup10 = udf {s:String=>s.length>10}
  maDf.withColumn("lensup10",lensup10('nom)).show()



}


object Example_Streaming extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    .set("spark.driver.allowMultipleContexts","true")


  val ssc = new StreamingContext(conf,Seconds(1))


  // wordcount

  val lines=ssc.socketTextStream("localhost",5555)

  val word=lines.flatMap(_.split(" "))
  val pairs=word.map(x=>(x,1))
  val wordCounts= pairs.reduceByKeyAndWindow((x:Int,y:Int)=> x+y, Seconds(6),Seconds(4))

  wordCounts.print()

  ssc.start()
  ssc.awaitTermination()



}

object Exercice_Streaming extends App {
  val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    .set("spark.driver.allowMultipleContexts","true")


  val ssc = new StreamingContext(conf,Seconds(5))




  val file=ssc.textFileStream("/home/wilder/Projets/Spark/Exo_Streaming_source/")

  file.foreachRDD(t => {
    val test= t.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    if (test.take(1).length !=0)  {
      println("coucou")
    test.saveAsTextFile("/home/wilder/Projets/Spark/Exo_Streaming_destination"+
      java.time.LocalDateTime.now.toString)
    }
  }
  )
  ssc.start()
  ssc.awaitTermination()



}