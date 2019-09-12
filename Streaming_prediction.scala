import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.Encoders
import org.apache.spark.ml._
case class Feature(feature:String)
object BikeSharingPrediction {
 def main(args: Array[String]) {
 val conf = new SparkConf().setAppName("BikeSharingPrediction")
 val ssc = new StreamingContext(conf, Seconds(10))
 val lines = ssc.textFileStream("tmp/kafka/bikesharingfeatures")
 lines.foreachRDD { rdd =>
val spark=SparkSession.builder().getOrCreate()
import spark.implicits._
val rawRdd = rdd.map(Feature(_))
val raw = spark.createDataFrame(rawRdd)
val pipeline = PipelineModel.read.load("bike_sharing_gbt_file.model")
val predictions = pipeline.transform(raw)
val prop = new java.util.Properties
prop.put("driver", "com.mysql.jdbc.Driver");
prop.put("url", "jdbc:mysql://mysqldb.edu.cloudlab.com/use_cases");
prop.put("user", "labuser");
prop.put("password", "edureka");
predictions.select("feature","prediction").write.mode("append").jdbc(
 prop.getProperty("url"), "bike_sharing_feature", prop)
 }
 ssc.start()
 ssc.awaitTermination()
 }