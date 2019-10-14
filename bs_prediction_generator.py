import pyspark.sql.functions as func
from pyspark.sql.types import *
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from functools import reduce
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext, DataFrame
# from camp_revamp import turingBatch as tb
from pyspark.sql.functions import rand,when

##################################################### Function definitions #####################################################

try:
                ##################################################### Define variables #####################################################
                sc = SparkContext()
                sqlContext = HiveContext(sc)
                bs_df = sqlContext.read.load("test.csv",
                                  format='com.databricks.spark.csv',
                                  header='true',
                                  inferSchema='true')
                print("Test data features")
                bs_df.show()
                print(bs_df.printSchema())

                def valueToCategory(value, encoding_index):
                   if(value == encoding_index):
                     return 1
                   else:
                     return 0
                #Explode season column into separate columns such as season_<val> and drop season
                from pyspark.sql.functions import udf
                from pyspark.sql.functions import lit
                from pyspark.sql.types import *
                from pyspark.sql.functions import col
                udfValueToCategory = udf(valueToCategory, IntegerType())
                bs_df_encoded = (bs_df.withColumn("season_1", udfValueToCategory(col('season'),lit(1)))
                                     .withColumn("season_2", udfValueToCategory(col('season'),lit(2)))
                                     .withColumn("season_3", udfValueToCategory(col('season'),lit(3)))
                                     .withColumn("season_4", udfValueToCategory(col('season'),lit(4))))
                bs_df_encoded = bs_df_encoded.drop('season')
                #https://stackoverflow.com/questions/40161879/pyspark-withcolumn-with-two-conditions-and-three-outcomes

                bs_df_encoded = (bs_df_encoded.withColumn("weather_1", udfValueToCategory(col('weather'),lit(1)))
                     .withColumn("weather_2", udfValueToCategory(col('weather'),lit(2)))
                     .withColumn("weather_3", udfValueToCategory(col('weather'),lit(3)))
                     .withColumn("weather_4", udfValueToCategory(col('weather'),lit(4))))
                bs_df_encoded = bs_df_encoded.drop('weather')

                #  hour, day, month, year
                from pyspark.sql.functions import split
                from pyspark.sql.functions import *
                from pyspark.sql.types import *
                bs_df_encoded = bs_df_encoded.withColumn('hour',  split(split(bs_df_encoded['datetime'], ' ')[1], ':')[0].cast('int'))
                bs_df_encoded = bs_df_encoded.withColumn('year', split(split(bs_df_encoded['datetime'], ' ')[0], '-')[0].cast('int'))
                bs_df_encoded = bs_df_encoded.withColumn('month', split(split(bs_df_encoded['datetime'], ' ')[0], '-')[1].cast('int'))
                bs_df_encoded = bs_df_encoded.withColumn('day', split(split(bs_df_encoded['datetime'], ' ')[0], '-')[2].cast('int'))
                print("Test data features encoded")
                bs_df_encoded.show(10)

                bs_df_encoded = bs_df_encoded.drop('datetime')
                # bs_df_encoded = bs_df_encoded.withColumnRenamed("count", "label")

                #Split the dataset into train and train_test
                from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
                #train, test = bs_df_encoded.randomSplit([0.9, 0.1], seed=12345)

                from pyspark.ml.linalg import Vectors
                from pyspark.ml.feature import VectorAssembler

                assembler = VectorAssembler(inputCols=["holiday","workingday","temp","atemp","humidity","windspeed","season_1","season_2","season_3","season_4","weather_1","weather_2","weather_3","weather_4", "hour", "year", "month", "day"],outputCol="features")

                output = assembler.transform(bs_df_encoded)
                output.show(truncate=False)#.select("features", "clicked")
                print(output.count())
                test_features = output.na.drop()
                print(test_features.count())

                # test_output = assembler.transform(test)
                # print(test_output.count())
                # train_output = test_output.na.drop()
                # print(test_output.count())
                # print("Assembled columns 'hour', 'minute' .. to vector column 'features'")
                # test_output.show(truncate=False)#.select("features", "clicked")

                from pyspark.ml.regression import GBTRegressor, GBTRegressionModel
                # gbt = GBTRegressor(featuresCol="features", maxIter=10)
                path = "bike_sharing_gbt_file.model"

                gbt_model = GBTRegressionModel.load(path)
                # Make predictions.

                print("Before model creation")
                predictions = gbt_model.transform(test_features)
                print("After model creation")
                predictions.printSchema()
                predictions.show()
                # gbt_model.write().overwrite().save(path)
                # Select example rows to display.
                from pyspark.sql.functions import col, lit, concat
                bs_df.show()
                # predictions = predictions.withColumn("datetime", bs_df.select("datetime"))
                predictions = predictions.withColumn("datetime",concat(col("year"),lit("-"),col("month"),lit("-"),col("day"),lit(" "),col("hour"),lit(":00:00")))
                predictions.show()

                pred_file =  predictions.select("prediction", "datetime")
                # spark_df.write.fxormat('com.databricks.spark.csv') \
                pred_file.coalesce(1).write.mode("overwrite").csv("prediction.csv")
                print("file saved")
                mode = "overwrite"
                url = "jdbc:mysql://mysqldb.edu.cloudlab.com/use_cases"
                properties = {
                                "user": "labuser",
                                "password": "edureka"
                             }
                #CREATE TABLE bike_sharing_preditions( prediction VARCHAR(50), datetime DOUBLE);
                pred_file.df.jdbc(url=url, table="bike_sharing_preditions", mode=mode, properties=properties)
                # Select (prediction, true label) and compute test error

                # from pyspark.ml.evaluation import RegressionEvaluator
                # evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
                # rmse = evaluator.evaluate(predictions)
                # print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
except Exception as e:
        print(e)
