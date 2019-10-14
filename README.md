<<<<<<< HEAD
# SparkML

This repository contains Bike sharing demand project using Spark framework, to so solve machine learning challengs with huge datasets with pyspark, flume and sparkstreaming.

* **Bike_sharing_demand_project.ipynb** <br/>
Initial Data Exploration and Transformation is done in data bricks platform and have
this codebase as Ipython Notebook which explicitly displays each intermediate result and model
performances.

* **bike_sharing_model_generation.py** <br/>
This file is used to give a trained model taking training files as input by cleaning them and using
one of the algorithms which gave best results **GBTRegressor** which gave least root mean
square error. This pyspark code gives trained model files as **bike_sharing_gbt_file.model**


* **bike_sharing_prediction.py** <br/>
This file is uses the model generated out of previous file and predicts the bike sharing demand
on the test files given. Then finally outputs the predictions as csv file with name **predictions.csv**
and also saves them in RDBMS. 

* **Streaming_prediction.scala** <br/>
The streaming prediction is done in scala it uses existing model generated and flume to stream
the features and predict the demand then finally push them to RDBMS.

Screenshots of results pushed to RDBMS are are shown in **Project Report Bicycle Sharing Demand Prediction.pdf**

* **Instructions to run flume and spark job** <br/>
Command to run flume <br/>
```
flume-ng agent -n bs_agent -c conf -f bs_flume.conf - Dflume.root.logger=INFO,console
```
Compile and run the spark streaming program <br/>

```
spark2-submit --jzars mysql-connector-java-8.0.12.jar --class bikesharing.BikeStreaming --deploy-mode client target/scala-2.11/sparkme-project_2.11-1.0.jar
=======
# SparkML

This repository contains Bike sharing demand project using Spark framework, to so solve machine learning challengs with huge datasets with pyspark, flume and sparkstreaming.

* **Bike_sharing_demand_project.ipynb** <br/>
Initial Data Exploration and Transformation is done in data bricks platform and have
this codebase as Ipython Notebook which explicitly displays each intermediate result and model
performances.

* **bike_sharing_model_generation.py** <br/>
This file is used to give a trained model taking training files as input by cleaning them and using
one of the algorithms which gave best results **GBTRegressor** which gave least root mean
square error. This pyspark code gives trained model files as **bike_sharing_gbt_file.model**


* **bike_sharing_prediction.py** <br/>
This file is uses the model generated out of previous file and predicts the bike sharing demand
on the test files given. Then finally outputs the predictions as csv file with name **predictions.csv**
and also saves them in RDBMS. 

* **Streaming_prediction.scala** <br/>
The streaming prediction is done in scala it uses existing model generated and flume to stream
the features and predict the demand then finally push them to RDBMS.

Screenshots of results pushed to RDBMS are are shown in **Project Report Bicycle Sharing Demand Prediction.pdf**

* **Instructions to run flume and spark job** <br/>
Command to run flume <br/>
```
flume-ng agent -n bs_agent -c conf -f bs_flume.conf - Dflume.root.logger=INFO,console
```
Compile and run the spark streaming program <br/>

```
spark2-submit --jzars mysql-connector-java-8.0.12.jar --class bikesharing.BikeStreaming --deploy-mode client target/scala-2.11/sparkme-project_2.11-1.0.jar
>>>>>>> ecda965ed7d38f5c011c590f3c3fcc5bd6697ef2
```