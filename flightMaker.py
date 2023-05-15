import mysql.connector
from datetime import datetime, date, timedelta
import csv

def main():
    compileFlightIntoFlightPath()

def compileFlightIntoFlightPath():
    db = mysql.connector.connect(
        host= "",
        port= ,
        user = "",
        passwd = "",
        database = ""
    )

    mycursor = db.cursor()
    
    # Equivalent to being within 2 miles, just under the distance between the closest airports in the world (0.38 miles)
    flightLatitudeRange = 0.03

    # Equivalent to being within 2 miles
    flightLongitudeRange = 0.036

    # Longest commercial flight in the world is 18 hours and 50 minutes 
    # (to allow for signals to be possibly on the ground before take off and after landing we are contraining all flights
    #  to be within 19 hours)
    flightDurationConstraint = timedelta(hours=19)

    # Constraint to get flights from hobbyists but also disclude flights resulting from missing data
    flightDurationSameAirport = timedelta(hours=1)

    # Constraint for helicopter flight
    flightDurationHelicopter = timedelta(hours=2)

    mycursor.execute("CREATE TABLE IF NOT EXISTS Flight ( flight_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, \
                     icao24 VARCHAR(24) NOT NULL, callsign VARCHAR(20) NOT NULL, completed BOOL NOT NULL, origin_country VARCHAR(56), \
                     departure_airport VARCHAR(80) NOT NULL, arrival_airport VARCHAR(80) NOT NULL, departure_datetime DATETIME NOT NULL, \
                     arrival_datetime DATETIME NOT NULL, flight_duration INT UNSIGNED NOT NULL, \
                     airline VARCHAR(50), aircraft_type VARCHAR(75), registration VARCHAR(20), squawk VARCHAR(4), spi BOOLEAN, \
                     category INT NOT NULL);")

    mycursor.execute("CREATE TABLE IF NOT EXISTS Waypoint ( flight_id INT UNSIGNED NOT NULL REFERENCES Flight(flight_id), \
                     latitude DECIMAL(23,20) NOT NULL, longitude DECIMAL(23,20) NOT NULL, altitude DECIMAL(25,20) NOT NULL, offset_ms INT UNSIGNED NOT NULL, \
                     PRIMARY KEY (flight_id, offset_ms));")

    ## Ability to use today or a selected date
    #today = date.today()
    #now = datetime.now()
    
    # Date and time of completing Customized
    ##today = date(2023, 3, 31)
    now = datetime(2023, 3, 31, 5, 0, 0, 0)

    target = now - timedelta(hours = 24)
    targetTime = target.strftime("%H:%M:%S")

    # The date that is a set amount of hours ago
    ##targetDate = today - timedelta(days = 1.5)
    ##targetDateString = date.strftime(targetDate, '20%y-%m-%d')
    targetDateString = target.strftime('%Y-%m-%d')
    currentDate = now.strftime('%Y-%m-%d')
    currentTime = now.strftime('%H:%M:%S')

    mycursor.execute("SELECT icao24, callsign, origin_country, received_date, received_time, latitude, longitude, squawk, spi, category FROM StateVector WHERE baro_altitude < 500 AND (state_vector_id IN (Select state_vector_id FROM StateVector WHERE received_date = %s AND received_time >= %s) OR state_vector_id IN (Select state_vector_id FROM StateVector WHERE received_date = %s AND received_time <= %s )) ORDER BY received_date, received_time LIMIT 50000", (targetDateString, targetTime, currentDate, currentTime))
    stateVectors = mycursor.fetchall()
    for (icao24, callsign, origin_country, received_date, received_time, latitude, longitude, squawk, spi, category) in stateVectors:
        
        d = received_date
        t = received_time
        startLat = latitude
        startLong = longitude

        # If a flight started within 15 minutes of this point we are assuming the flight has already been completed
        dt = datetime.combine(d,(datetime.min + t).time())
        
        #tBefore = t - timedelta(minutes=15)
        tAfter = t + timedelta(minutes=15)

        dtBefore = dt - timedelta(minutes=15)
        dtAfter = dt + timedelta(minutes=15)

        dayAfter = dtAfter.date()

        # Checking if the start point already exists
        mycursor.execute("SELECT * FROM Flight WHERE icao24 = %s AND departure_datetime >= %s \
                                    AND departure_datetime <= %s", (icao24, dtBefore, dtAfter))
        
        # Ensures an empty list if none
        startflights = mycursor.fetchall()

        mycursor.execute("SELECT * FROM Flight WHERE icao24 = %s AND arrival_datetime >= %s \
                                    AND arrival_datetime <= %s", (icao24, dtBefore, dtAfter))
        
        # Ensures an empty list if none
        endflights = mycursor.fetchall()

        # Ensure a completed flight doesn't already exist for this start point
        if len(startflights) == 0 and len(endflights) == 0:
            mycursor.execute("SELECT icao24, received_date, received_time, latitude, longitude FROM StateVector WHERE baro_altitude < 500 AND icao24 = %s AND (received_date = %s AND received_time >= %s)  LIMIT 3", (icao24, dayAfter, tAfter))
            possibleEndPoints = mycursor.fetchall()
            # Ensure an endpoint exists, if it does use the first end point found for the landing.
            if len(possibleEndPoints) != 0:
                # Checking if the starting point and end point have an associated airport
                startingAirport = None
                endingAirport = None
                endingTime = None
                endingDate = None
                lowerLat = latitude - flightLatitudeRange
                upperLat = latitude + flightLatitudeRange
                lowerLong = longitude - flightLongitudeRange
                upperLong = longitude + flightLongitudeRange

                with open('./airportsClosedDeleted.csv', newline='') as csvfile:
                    airportReader = csv.DictReader(csvfile)
                    for airport in airportReader:
                        if float(airport['latitude_deg']) >= lowerLat and float(airport['latitude_deg']) <= upperLat and float(airport['longitude_deg']) >= lowerLong and float(airport['longitude_deg']) <= upperLong:
                            startingAirport = airport['name']
                            break

                if startingAirport == None:
                    continue

                

                for (icao24, received_date, received_time, latitude, longitude) in possibleEndPoints:
                    if endingAirport != None:
                        break
                    
                    lowerLat = latitude - flightLatitudeRange
                    upperLat = latitude + flightLatitudeRange
                    lowerLong = longitude - flightLongitudeRange
                    upperLong = longitude + flightLongitudeRange

                    with open('./airportsClosedDeleted.csv', newline='') as csvfile2:
                        airportReader2 = csv.DictReader(csvfile2)
                        for airport2 in airportReader2:
                            if float(airport2['latitude_deg']) >= lowerLat and float(airport2["latitude_deg"]) <= upperLat and float(airport2["longitude_deg"]) >= lowerLong and float(airport2["longitude_deg"]) <= upperLong:
                                endingAirport = airport2["name"]
                                endingTime = received_time
                                endingDate = received_date
                                break
                            
                    if endingAirport == None:
                        break
                    

                if endingAirport == None:
                    continue
                    
                # Final Check ensure the plane leaves the ground inbetween these two points (proving it is in fact a start point)

                # case where departure date and arrival date are the same
                if d == endingDate:
                    mycursor.execute("SELECT latitude, longitude FROM StateVector WHERE baro_altitude > 1000 AND icao24 = %s AND received_date >= %s AND received_date <= %s AND received_time >= %s AND received_time <= %s", (icao24, d, endingDate, t, endingTime))
                    itTakesFlight = mycursor.fetchall()
                else:
                    # Case where the flight spans across 2 dates
                    mycursor.execute("SELECT latitude, longitude FROM StateVector WHERE state_vector_id IN ( SELECT state_vector_id FROM StateVector WHERE baro_altitude > 1000 AND icao24 = %s AND received_date = %s AND received_time >= %s) OR state_vector_id IN ( SELECT state_vector_id FROM StateVector WHERE baro_altitude > 1000 AND icao24 = %s AND received_date = %s AND received_time <= %s)", (icao24, d, t, icao24, endingDate, endingTime))
                    itTakesFlight = mycursor.fetchall()

                if len(itTakesFlight) == 0:
                    continue

                # Checking to see if the aircraft moves lat/long
                moves = False
                for (latitude, longitude) in itTakesFlight:
                    if latitude >= startLat+1 or latitude <= startLat-1:
                        moves=True
                        break
                    if longitude >= startLong+1 or longitude <= startLong-1:
                        moves=True
                        break

                if not moves:
                    continue

                # All criteria met (this is a new flight)
                dt = datetime.combine(d,(datetime.min + t).time())
                enddt = datetime.combine(endingDate, (datetime.min + endingTime).time())
                flightDuration = enddt - dt

                if flightDuration > flightDurationConstraint:
                    continue
                
                if startingAirport == endingAirport and flightDuration > flightDurationSameAirport:
                    continue

                if "Heli" in startingAirport and flightDuration > flightDurationHelicopter:
                    continue

                if "Heli" in endingAirport and flightDuration > flightDurationHelicopter:
                    continue

                print(startingAirport)
                print(endingAirport)
                print(flightDuration)

                flightDurationSeconds = flightDuration.total_seconds()
                TheFlightIsSmoothed = False
                departureDT = datetime.combine(d,(datetime.min + t).time())
                arrivalDT = datetime.combine(endingDate,(datetime.min + endingTime).time())

                mycursor.execute("INSERT INTO Flight (icao24, callsign, completed, origin_country, departure_airport, \
                                 arrival_airport, departure_datetime, arrival_datetime, flight_duration, \
                                 squawk, spi, category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                                 (icao24, callsign, TheFlightIsSmoothed, origin_country, startingAirport, endingAirport, departureDT, arrivalDT, 
                                  flightDurationSeconds, squawk, spi, category))
                db.commit()

                mycursor.execute("SELECT flight_id, icao24 FROM Flight WHERE icao24 = %s AND departure_datetime = %s AND arrival_datetime = %s", (icao24, departureDT, arrivalDT))
                flightID = mycursor.fetchall()

                for (flight_id, icao24) in flightID:
                    if d == endingDate:
                        mycursor.execute("SELECT latitude, longitude, baro_altitude, received_time FROM StateVector WHERE icao24 = %s AND \
                                        received_date = %s AND received_time >= %s AND received_time <= %s",
                                        (icao24, d, t, endingTime))
                        flightWaypoints = mycursor.fetchall()
                        for (latitude, longitude, baro_altitude, received_time) in flightWaypoints:
                            offset = datetime.combine(date.min,(datetime.min + received_time).time()) - datetime.combine(date.min,(datetime.min + t).time())
                            offset_ms = offset.total_seconds() * 1000
                            mycursor.execute("INSERT INTO Waypoint (flight_id, latitude, longitude, altitude, offset_ms) \
                                             VALUES (%s,%s,%s,%s,%s)", (flight_id, latitude, longitude, baro_altitude, offset_ms))
                            db.commit()
                    else:
                        mycursor.execute("SELECT latitude, longitude, baro_altitude, received_date, received_time FROM StateVector WHERE state_vector_id IN (icao24 = %s AND \
                                        received_date = %s AND received_time >= %s) OR state_vector_id IN (icao24 = %s AND \
                                        received_date = %s AND received_time <= %s)",
                                        (icao24, d, t, icao24, endingDate, endingTime))
                        flightWaypoints = mycursor.fetchall()
                        for (latitude, longitude, baro_altitude, received_date, received_time) in flightWaypoints:
                            offset = datetime.combine(received_date,(datetime.min + received_time).time()) - departureDT
                            offset_ms = offset.total_seconds() * 1000
                            mycursor.execute("INSERT INTO Waypoint (flight_id, latitude, longitude, altitude, offset_ms) \
                                             VALUES (%s,%s,%s,%s,%s)", (flight_id, latitude, longitude, baro_altitude, offset_ms))
                            db.commit()
                    break




    # Could be returned for use in the flight smoother
    return datetime.combine(targetDateString, targetTime)


main()


    
