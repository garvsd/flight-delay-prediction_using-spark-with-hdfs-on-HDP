$(function () {
    $.demo = {};
    $.demo.refreshRate = 2000;
    $.demo.submittedLastRowId = 0;
    $.demo.submittedCount = 0;
    $.demo.streamedLastRowId = 0;
    $.demo.streamedCount = 0;
    $.demo.delayedMinutes = 40;
    $.demo.debug = true;
});

function initialLoad() {
    var flights = jsRoutes.controllers.FlightEndpoint.getLastFlights(0, 0);

    $.getJSON(flights.url, function(data) {
        $.each(data, function(index, flight) {
            if (flight.streamed == "1") {
                insertStreamedFlight(flight)
            } else {
                if (flight.rowId > $.demo.submittedLastRowId) {
                    $.demo.submittedLastRowId = flight.rowId
                }
                insertSubmittedFlight(flight)
            }
            $('#submittedCount').text($.demo.submittedCount)
            $('#streamedCount').text($.demo.streamedCount)
        });
        log("Last streamed: " + $.demo.streamedLastRowId);
        log("Last submitted: " + $.demo.submittedLastRowId);
    })

    log("Initial load complete")
}

function insertStreamedFlight(flight) {
    if (flight.rowId > $.demo.streamedLastRowId) {
        $.demo.streamedLastRowId = flight.rowId
    }
    var row = toStreamedRow(flight)
    $('#streamedFlightsTable tr:last').after(row);
}

function insertSubmittedFlight(flight) {
    row = toSubmittedRow(flight)
    if ($.demo.submittedCount > 0) {
        $('#submittedFlightsTable tr:first').after(row);
    } else {
        $('#submittedFlightsTable tr:last').after(row);
    }
    $.demo.submittedCount += 1
}

function getFlights() {
    var flights = jsRoutes.controllers.FlightEndpoint.getLastFlights($.demo.streamedLastRowId, $.demo.submittedLastRowId);

    $.getJSON(flights.url, function(data) {
        $.each(data, function(index, flight) {
            var tableToInsert = ""
            if (flight.streamed == "1") {
                insertStreamedFlight(flight)
                $.demo.streamedCount += 1;
            } else {
                if (flight.rowId > $.demo.submittedLastRowId) {
                    $.demo.submittedLastRowId = flight.rowId
                }
                changeFlightPrediction(flight.rowId, flight.prediction)
            }
            $('#submittedCount').text($.demo.submittedCount)
            $('#streamedCount').text($.demo.streamedCount)
        });
        log("Last streamed: " + $.demo.streamedLastRowId);
        log("Last submitted: " + $.demo.submittedLastRowId);
    })

    setTimeout(function() {getFlights();}, $.demo.refreshRate);
}

function toStreamedRow(flight) {
    var row = [];
    row.push('<tr>')
    row.push('<td>');     row.push(flight.rowId);       row.push('</td>');
    row.push('<td>Jan '); row.push(flight.dayOfMonth);  row.push('</td>');
    row.push('<td>');     row.push(flight.carrier);     row.push('</td>');
    row.push('<td>');     row.push(flight.origin);      row.push('</td>');
    row.push('<td>');     row.push(flight.destination); row.push('</td>');
    row.push('<td>');
        row.push(addColon(flight.scheduledDepartureTime));
    row.push('</td>');
    row.push('<td>');
        row.push(addColon(flight.scheduledArrivalTime));
    row.push('</td>');
    row.push('<td>');     row.push(flight.departureDelayMinutes);   row.push('</td>');
    if ((flight.prediction == "0" && flight.departureDelayMinutes <= $.demo.delayedMinutes) ||
        (flight.prediction == "1" && flight.departureDelayMinutes > $.demo.delayedMinutes)) {
        row.push('<td class="success center-text">Correct');
    } else {
        row.push('<td class="warning center-text">Incorrect');
    }
    row.push('</td>');
    row.push('</tr>');
    var combinedRow = row.join("");
    return combinedRow;
}

function toSubmittedRow(flight) {
    var row = [];
    row.push('<tr>')
    row.push('<td>'); row.push(flight.rowId);       row.push('</td>');
    row.push('<td>Jan '); row.push(flight.dayOfMonth);  row.push('</td>');
    row.push('<td>'); row.push(flight.carrier);     row.push('</td>');
    row.push('<td>'); row.push(flight.origin);      row.push('</td>');
    row.push('<td>'); row.push(flight.destination); row.push('</td>');
    row.push('<td>');
        row.push(addColon(flight.scheduledDepartureTime));
    row.push('</td>');
    row.push('<td>');
        row.push(addColon(flight.scheduledArrivalTime));
     row.push('</td>');
    if (!flight.hasOwnProperty('prediction')) {
        id = 'submitted_fight_' + flight.rowId
        row.push('<td class="center-text" id=' + id + '>Pending...');
    } else if (flight.prediction == "0") {
        row.push('<td class="success center-text">On time');
    } else {
        row.push('<td class="warning center-text">Delay');
    }
    row.push('</td>');
    row.push('</tr>')
    var combinedRow = row.join("");
    return combinedRow;
}

function changeFlightPrediction(rowId, prediction) {
        if (prediction == "0") {
            $("#submitted_fight_" + rowId).addClass("success").text("On time")
        } else {
            $("#submitted_fight_" + rowId).addClass("warning").text("Delay")
        }
}

function submitFlight() {
    var route = jsRoutes.controllers.KafkaEndpoint.submitFlight();
    var flightToSubmit = JSON.stringify({
                                         dayOfMonth: $('#mday').val(),
                                         dayOfWeek: $('#wday').val(),
                                         carrier: $('#carrier').val(),
                                         origin: $('#origin').val(),
                                         destination: $('#destination').val(),
                                         scheduledDepartureTime: $('#departureTime').val(),
                                         scheduledArrivalTime: $('#arrivalTime').val(),
                                         crsElapsedFlightMinutes: $('#crsFlightTime').val()
                                        });
    var flight = JSON.parse(flightToSubmit)
    $.ajax({
        url: route.url,
        type: route.type,
        data: flightToSubmit,
        contentType: "application/json",
        success: function(newId) {
            flight.rowId = newId
            insertSubmittedFlight(flight)
        }
    });
    log("Flight was submitted: " + flightToSubmit);
}

function addColon(str) {
    var index = str.length - 2
    return str.substr(0, index) + ':' + str.substr(index);
}

function log(msg) {
    if ($.demo.debug) {
        console.log(msg);
    }
}

$(document).ready(initialLoad);
$(document).ready(getFlights);