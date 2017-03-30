import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.model.DecisionTreeModel

/**
  * Created.
  */
object SparkApp extends App {

  val conf = new SparkConf().setAppName("sparkml-flight-delay").setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def parseFlight(str: String) = {
    val r = str.split(",")
    Flight(
      r(0).toInt - 1, r(1).toInt - 1, r(2), r(3), r(4).toInt, r(5), r(6),
      r(7), r(8), r(9).toDouble, r(10).toDouble, r(11).toDouble, r(12).toDouble,
      r(13).toDouble, r(14).toDouble, r(15).toDouble, r(16).toInt
    )
  }

  /* Get single row data file from HDFS */
  val rdd = sc.textFile("/user/root/demodata/flights_jan_2014_1.csv")
  val flights = rdd.map(parseFlight).cache()

  /*Kafka part of processing the job */
  /*
  val ssc: StreamingContext = ???
  val kafkaParams: Map[String, String] = Map("group.id" -> "terran", ...)

  val numDStreams = 5
  val topics = Map("flights" -> 1)
  val kafkaDStreams = (1 to numDStreams).map { _ =>
    KafkaUtils.createStream(ssc, kafkaParams, topics, ...)
	kafkaDStreams.foreachRDD(Process_and_stream)
  }
  */
  
  val carrierIndex =
    flights
      .map(_.carrier).distinct
      .collect
      .zipWithIndex
      .toMap

  val originIndex =
    flights
      .map(_.origin).distinct
      .collect
      .zipWithIndex
      .toMap

  val destinationIndex =
    flights
      .map(_.destination).distinct
      .collect
      .zipWithIndex
      .toMap

  val features = flights map { flight =>
    val dayOfMonth = flight.dayOfMonth.toDouble
    val dayOfWeek = flight.dayOfWeek.toDouble
    val departureTime = flight.crsDepartureTime
    val arrivalTime = flight.crsArrivalTime
    val carrier = carrierIndex(flight.carrier)
    val elapsedTime = flight.crsElapsedTime
    val origin = originIndex(flight.origin).toDouble
    val destination = destinationIndex(flight.destination).toDouble
    val delayed = if (flight.departureDelayMinutes > 40) 1.0 else 0.0
    Array(delayed, dayOfMonth, dayOfWeek, departureTime, arrivalTime, carrier, elapsedTime, origin, destination)
  }

  val labeled = features map { x =>
    LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
  }
  val delayed = labeled.filter(_.label == 0).randomSplit(Array(0.90, 0.10))(1)
  val notDelayed = labeled.filter(_.label != 0)
  val all = delayed ++ notDelayed
    
  val model = DecisionTreeModel.load(sc,"/user/root/demodata/prediction/model")

  /*
  println(model.toDebugString)
  */
  val predictions = all map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }

  predictions.saveAsTextFile("/user/root/demodata/prediction/val")
  
  sc.stop()

}

case class Flight(dayOfMonth: Int,
                  dayOfWeek: Int,
                  carrier: String,
                  tailNumber: String,
                  flightNumber: Int,
                  originId: String,
                  origin: String,
                  destinationId: String,
                  destination: String,
                  crsDepartureTime: Double, // crs - central reservation system
                  actualDepartureTime: Double,
                  departureDelayMinutes: Double,
                  crsArrivalTime: Double,
                  actualArrivalTime: Double,
                  arrivalDelayMinutes: Double,
                  crsElapsedTime: Double,
                  distance: Int)