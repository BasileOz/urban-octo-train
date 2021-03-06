import org.apache.spark.{SparkConf, SparkContext}

object SparkContextOld extends App{

  val conf = new SparkConf().setAppName("sparkbyexamples.com").setMaster("local[1]")
  val sparkContext = new SparkContext(conf)
  val rdd = sparkContext.textFile("C:\\Program Files\\Java\\jre1.8.0_241\\COPYRIGHT")

  sparkContext.setLogLevel("ERROR")

  println("First SparkContext:")
  println("APP Name :"+sparkContext.appName);
  println("Deploy Mode :"+sparkContext.deployMode);
  println("Master :"+sparkContext.master);
  sparkContext.stop()

  val conf2 = new SparkConf().setAppName("sparkbyexamples.com-2").setMaster("local[1]")
  val sparkContext2 = new SparkContext(conf2)

  println("Second SparkContext:")
  println("APP Name :"+sparkContext2.appName);
  println("Deploy Mode :"+sparkContext2.deployMode);
  println("Master :"+sparkContext2.master);

}