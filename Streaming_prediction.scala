//Import the dependencies 
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.sql.Encoders
import org.apache.spark.ml._

// creat case class template 
case class Bike(datetime:String,season:Int, holiday:Int, workingday:Int, weather:Int, temp:Double,
atemp:Double,humidity:Double, windspeed:Double)

object BikeStreaming {
def main(args: Array[String]) {
//spark instance created
val conf = new SparkConf().setAppName("BikeStreaming")
val ssc = new StreamingContext(conf, Seconds(10))
val lines = ssc.textFileStream(“use_cases/bike_sharing/flume")
lines.foreachRDD { rdd =>
val spark=SparkSession.builder().getOrCreate()
import spark.implicits._
val rawRdd = rdd.map(_.split(",")).
map(d=>Bike(d(0).toString,d(1).toInt, d(2).toInt, d(3).toInt, d(4).toInt,d(5).toDouble,d(6).toDouble, d(7).toDouble, d(8).toDouble))
val raw = spark.createDataFrame(rawRdd)
//processing the inputs
val casted = raw.withColumn("humidity", col("humidity").cast(DoubleType))
val df = casted.withColumn("year",year(col("datetime"))).withColumn("month",month(col("datetime"))).
withColumn("hour",hour(col("datetime")))

val pipeline = PipelineModel.read.load("bike_sharing_gbt_file.model")
val predictions = pipeline.transform(df)
val prop = new java.util.Properties
//saving the predictions to RDBMS
prop.put("driver", "com.mysql.jdbc.Driver");
prop.put("url", "jdbc:mysql://mysqldb.edu.cloudlab.com/use_cases");
prop.put("user", "labuser");
prop.put("password", "edureka");
predictions.drop("features").write.mode("append").jdbc(
prop.getProperty("url"), "bike_sharing", prop)
}
ssc.start()
ssc.awaitTermination()
}
}