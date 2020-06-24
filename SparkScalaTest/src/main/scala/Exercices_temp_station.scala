
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object Exerc extends App {



  val sConf = new SparkConf().setMaster("local[*]").setAppName("test1")
  val sc = new SparkContext(sConf)

  val rdd1 = sc.textFile("/home/wilder/Téléchargements/original.csv")
  val rdd2=rdd1.filter(x=>x.contains("MAX"))
  val rdd3=rdd2.sortBy(x=>x.split(",")(3).toInt,false).map(x=> x.split(",")).map(x=>(x(0),x(3)))
  //rdd3.take(1).foreach(println)

  val rdd4=rdd2.map(x=>(x.split(",")(0),x.split(",")(3).toInt))
  val rdd5=rdd4.reduceByKey((x,y)=>math.min(x,y))
  rdd5.collect().foreach(println)
}



