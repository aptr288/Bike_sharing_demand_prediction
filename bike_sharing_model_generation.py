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
                bs_df = sqlContext.read.load("train.csv",
                                  format='com.databricks.spark.csv',
                                  header='true',
                                  inferSchema='true')
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
                bs_df_encoded.show(10)

                bs_df_encoded = bs_df_encoded.drop('datetime')
                bs_df_encoded = bs_df_encoded.withColumnRenamed("count", "label")

                #Split the dataset into train and train_test
                from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
                train, test = bs_df_encoded.randomSplit([0.9, 0.1], seed=12345)

                from pyspark.ml.linalg import Vectors
                from pyspark.ml.feature import VectorAssembler
                assembler = VectorAssembler(inputCols=["holiday","workingday","temp","atemp","humidity","windspeed","label","season_1","season_2","season_3","season_4","weather_1","weather_2","weather_3","weather_4", "hour", "year", "month", "day"],outputCol="features")

                # assembler = VectorAssembler(inputCols=["holiday","workingday","temp","atemp","humidity","windspeed","casual","registered","label","season_1","season_2","season_3","season_4","weather_1","weather_2","weather_3","weather_4", "hour", "month", "day", "year"],outputCol="features")

                output = assembler.transform(train)
                print("Assembled columns 'hour', 'minute' .. to vector column 'features'")
                output.show(truncate=False)#.select("features", "clicked")
                print(output.count())
                train_output = output.na.drop()
                print(train_output.count())

                test_output = assembler.transform(test)
                print(test_output.count())
                train_output = test_output.na.drop()
                print(test_output.count())
                print("Assembled columns 'hour', 'minute' .. to vector column 'features'")
                test_output.show(truncate=False)#.select("features", "clicked")

                from pyspark.ml.regression import GBTRegressor
                gbt = GBTRegressor(featuresCol="features", maxIter=10)

                gbt_model = gbt.fit(train_output)
                # Make predictions.
                predictions = gbt_model.transform(test_output)
                path = "bike_sharing_gbt_file.model"
                gbt_model.write().overwrite().save(path)
                # Select example rows to display.
                predictions.select("prediction", "label", "features").show(5)

                # Select (prediction, true label) and compute test error

                from pyspark.ml.evaluation import RegressionEvaluator
                evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
                rmse = evaluator.evaluate(predictions)
                print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
except Exception as e:
        print(e)
