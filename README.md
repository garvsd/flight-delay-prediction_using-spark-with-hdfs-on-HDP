# flight-delay-prediction_using-spark-with-hdfs-on-HDP

Flight delay prediction using Spark on HDFS.

Copy the data files in appropriate paths in HDP using the following commands.

A sample enviornment to perform this POC could be HDP (Horton works data platform).

Launch Spark jobs with spark submit and check sprak history dashboard for time taken to process a new prediction.
Use below commands to launch spark job.

sbt package
spark-submit --class SparkApp --master local[4] sparkml-flight-delay_2.10-1.0.jar

With URLs below you can access sandboxed dashboards.
Spark Dashboard - http://127.0.0.1:4040/jobs/

Spark History - http://127.0.0.1:18080/

Zippelin - http://127.0.0.1:9995/

HDFS Web - http://127.0.0.1:50070/explorer.html

HDP Dashboard - http://127.0.0.1:8888/

Ambari - http://127.0.0.1:8080/

