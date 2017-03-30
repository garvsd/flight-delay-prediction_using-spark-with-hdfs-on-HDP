package controllers

//import com.gigaspaces.document.SpaceDocument
//import com.j_spaces.core.client.SQLQuery
import model.grid.Flight
//import org.openspaces.core.GigaSpaceConfigurer
//import org.openspaces.core.space.SpaceProxyConfigurer
import play.api.libs.json._
import play.api.mvc._

object FlightEndpoint extends Controller {

  val STREAMED = "1"
  val SUBMITTED = "0"

  implicit val orderStatusWrites = new Writes[Flight] {
    override def writes(f: Flight): JsValue = JsString(f.getClass.getSimpleName)
  }

  val flightsWriter = Json.writes[Flight]
  val flightsListWriter = Writes.list[Flight](flightsWriter)

//  val grid = {
//    val spaceConfigurer = new SpaceProxyConfigurer("insightedge-space").lookupGroups("insightedge").lookupLocators("127.0.0.1:4174")
//    new GigaSpaceConfigurer(spaceConfigurer).create()
//  }

  def getLastFlights(streamedRowId: String, submittedRowId: String) = Action { implicit request =>
    //val parquetFileDF = spark.read.parquet("FlightWithPrediction")
    //parquetFileDF.createOrReplaceTempView("parquetFile")
    //val flights = spark.sql("SELECT * FROM parquetFile")
	//namesDF.map(attributes => "Name: " + attributes(0)).show()
    //val query = new SQLQuery[SpaceDocument]("FlightWithPrediction", "(row_id > ? AND streamed = ?) OR (row_id > ? AND streamed = ?)")
    //query.setParameter(1, streamedRowId)
    //query.setParameter(2, STREAMED)
    //query.setParameter(3, submittedRowId)
    //query.setParameter(4, SUBMITTED)
    //val flights = grid.readMultiple(query).map(toFlight).toList

    //Ok(Json.toJson(flights)(flightsListWriter))
  }
  
//   def getLastFlights(streamedRowId: String, submittedRowId: String) = Action { implicit request =>
//    val query = new SQLQuery[SpaceDocument]("FlightWithPrediction", "(row_id > ? AND streamed = ?) OR (row_id > ? AND streamed = ?)")
//    query.setParameter(1, streamedRowId)
//    query.setParameter(2, STREAMED)
//    query.setParameter(3, submittedRowId)
//    query.setParameter(4, SUBMITTED)
//    val flights = grid.readMultiple(query).map(toFlight).toList
//
//    Ok(Json.toJson(flights)(flightsListWriter))
//  }

//  def toFlight(sd: SpaceDocument): Flight = {
//    Flight(
//      sd.getProperty[Long]("row_id"),
//      sd.getProperty[String]("streamed"),
//      sd.getProperty[String]("day_of_month"),
//      sd.getProperty[String]("day_of_week"),
//      sd.getProperty[String]("carrier"),
//      sd.getProperty[String]("origin"),
//      sd.getProperty[String]("destination"),
//      sd.getProperty[String]("scheduled_departure_time"),
//      sd.getProperty[String]("departure_delay_minutes"),
//      sd.getProperty[String]("scheduled_arrival_time"),
//      sd.getProperty[String]("crs_elapsed_flight_minutes"),
//      sd.getProperty[Double]("prediction")
//    )
//  }

}