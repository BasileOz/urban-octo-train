object readcsv extends App {


  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate;

  val df = spark.read
    .option("header", "true") //first line in file has headers
    .option("delimiter",";")
    .csv("file:///C:/Users/Administrator/OneDrive/Documents/Reykjavik University/Big_Data/Projet2/db_csv_files - Copy/blak-domarar.csv")
    .persist()
  ;
  df.show(df.count.toInt,false);

}


