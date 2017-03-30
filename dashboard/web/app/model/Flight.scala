package model

import model.web.SubmittedFlight

/**
  * @author Danylo_Hurin.
  */
object grid {

  case class Flight(
                     rowId: Long,
                     streamed: String,
                     dayOfMonth: String,
                     dayOfWeek: String,
                     carrier: String,
                     origin: String,
                     destination: String,
                     scheduledDepartureTime: String,
                     departureDelayMinutes: String,
                     scheduledArrivalTime: String,
                     crsElapsedFlightMinutes: String,
                     prediction: Double
                   )

}

object kafka {

  case class FlightEvent(
                          rowId: Int,
                          dayOfMonth: String,
                          dayOfWeek: String,
                          carrier: String,
                          origin: String,
                          destination: String,
                          scheduledDepartureTime: String,
                          scheduledArrivalTime: String,
                          crsElapsedFlightMinutes: String
                        ) {
    override def toString: String = {
      s"$rowId," +
        s"0," + //streamed
        s"$dayOfMonth," +
        s"$dayOfWeek," +
        s"$carrier," +
        s"," + //tailNumber
        s"," + //flightNumber
        s"," + //originId
        s"$origin," +
        s"," + //destinationId
        s"$destination," +
        s"$scheduledDepartureTime," +
        s"," + //actual_departure_time
        s"-1," + //departure_delay_minutes
        s"$scheduledArrivalTime," +
        s"," + //actual_arrival_time
        s"," + //arrival_delay_minutes
        s"$crsElapsedFlightMinutes," +
        s"" //distance
    }

  }

  object FlightEvent {
    def apply(rowId: Int, flight: SubmittedFlight): FlightEvent = {
      FlightEvent(
        rowId,
        flight.dayOfMonth,
        flight.dayOfWeek,
        flight.carrier,
        flight.origin,
        flight.destination,
        flight.scheduledDepartureTime,
        flight.scheduledArrivalTime,
        flight.crsElapsedFlightMinutes
      )
    }
  }

}

object web {

  case class SubmittedFlight(
                              dayOfMonth: String,
                              dayOfWeek: String,
                              carrier: String,
                              origin: String,
                              destination: String,
                              scheduledDepartureTime: String,
                              scheduledArrivalTime: String,
                              crsElapsedFlightMinutes: String
                            )

}