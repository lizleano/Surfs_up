import datetime
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from dateutil.relativedelta import relativedelta

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
# Establish Connection to MySQL
# DB_CONFIG_DICT = {
#         'user': 'root',
#         'password': 'Bernice1!',
#         'host': 'localhost',
#         'port': 3306,
#     }

# DB_CONN_FORMAT = "mysql://{user}:{password}@{host}:{port}/{database}"
# DB_NAME = "hawaii_sqllite"
# DB_CONN_URI_DEFAULT = (DB_CONN_FORMAT.format(
#     database=DB_NAME,
#     **DB_CONFIG_DICT))

# Establish Connection to sqlite
DB_CONN_URI_DEFAULT = 'sqlite:///hawaii.sqlite'
engine = create_engine(DB_CONN_URI_DEFAULT)

# Establish Connection to sqlite
# engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurements
Station = Base.classes.stations

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Functions
#################################################
def get_precipitation_records(enddate):
    results = []

    if (enddate == "max"):
        enddate = session.query(Measurement.date).\
            order_by(Measurement.date.desc()).\
            first()[0].strftime('%Y-%m-%d')
    else:
        try:
            datetime.datetime.strptime(enddate, '%Y-%m-%d')
        except ValueError:
            error_dict = {
                "error": "Incorrect enddate format, should be YYYY-MM-DD"
            }
            results.append(error_dict)

    #  no error in date format
    if len(results) == 0:
        datetime_object = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        enddate = datetime_object     
    
        startdate = enddate - relativedelta(years=1)

        prcpresult = session.query(Measurement.station,  Measurement.date, Measurement.prcp).\
                filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).\
                order_by(Measurement.station, Measurement.date).\
                all()
            
        stationName = ""
        prcp_results = []
        for p in prcpresult:
            # print(temp.date)
            if stationName != p.station:
                if len(stationName) > 0:
                    results.append({
                        "station": stationName,
                        "precipitation": prcp_results
                        })
                
                print(p.station)    
                stationName = p.station          
                prcp_results = []

            datetime_string = datetime.datetime.strftime(p.date, '%Y-%m-%d')
            prcp_dict = {
               datetime_string: p.prcp
            }
            prcp_results.append(prcp_dict)

        # print last prcp results
        if len(prcp_results) > 0:
            results.append({
                "station": stationName,
                "precipitation": prcp_results
                })

    return results

def get_temperature_records(enddate):
    results=[]
    
    if (enddate == "max"):
        enddate = session.query(Measurement.date).\
            order_by(Measurement.date.desc()).\
            first()[0].strftime('%Y-%m-%d')
    else:
        try:
            datetime.datetime.strptime(enddate, '%Y-%m-%d')
        except ValueError:
            error_dict = {
                "error": "Incorrect enddate format, should be YYYY-MM-DD"
            }
            results.append(error_dict)


   #  no error in date format
    if len(results) == 0:
        datetime_object = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        enddate = datetime_object     
    
        startdate = enddate - relativedelta(years=1)
    
        tobsresult = session.query(Measurement.station,  Measurement.date, Measurement.tobs).\
            filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).\
            order_by(Measurement.station, Measurement.date).\
            all()
        
        stationName = ""
        tobs_results = []
        for t in tobsresult:
            # print(temp.date)
            if stationName != t.station:
                if len(stationName) > 0:
                    results.append({
                        "station": t.station,
                        "tobs": tobs_results
                        })
                    
                stationName = t.station          
                tobs_results = []

            datetime_string = datetime.datetime.strftime(t.date, '%Y-%m-%d')
            tobs_dict = {
               datetime_string: t.tobs
            }
            tobs_results.append(tobs_dict)

        # print last prcp results
        if len(tobs_results) > 0:
            results.append({
                "station": stationName,
                "tobs": tobs_results
                })

    return results


def calc_temperature(startdate, enddate):
    result = []
        
    try:
        datetime.datetime.strptime(startdate, '%Y-%m-%d')
    except ValueError:
        result.append({
                "error": "Incorrect startdate format, should be YYYY-MM-DD"
            })

    if (enddate == "max"):       
        enddate = session.query(Measurement.date).\
                order_by(Measurement.date.desc()).\
                    first()[0]

    else:
        try:
            datetime.datetime.strptime(enddate, '%Y-%m-%d')
        except ValueError:
            result.append({
                "error": "Incorrect enddate format, should be YYYY-MM-DD"
            })

    if (len(result) == 0):
        # datetime_object = datetime.datetime.strptime(enddate, '%Y-%m-%d')
        # startdate = datetime_object - relativedelta(months=12)
            
        minimum = session.query(func.min(Measurement.tobs)).\
            filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).scalar()
           
        maximum = session.query(func.max(Measurement.tobs)).\
            filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).scalar()
        
        average = session.query(func.avg(Measurement.tobs)).\
            filter(Measurement.date >= startdate).filter(Measurement.date <= enddate).scalar()

        if isinstance(startdate, datetime.date):
            startdate = startdate.strftime('%y-%m-%d')

        if isinstance(enddate, datetime.date):
            enddate = enddate.strftime('%Y-%m-%d')


        result.append({"date": {"startdate": startdate,"enddate": enddate}, 
                       "temperature": {"minimum": minimum, "average": average, "maximum": maximum}
                       })
    
    return result

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"        
        f"<br/>"
        f"Search precipitation<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/precipitation/enddate/<br/>"
        f"<br/>"
        f"Search Temperature<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/precipitation/enddate/<br/>"
        f"<br/>"
        f"Search Temperature (min, maximum, average)<br/>"
        f"/api/v1.0/startdate<br/>"
        f"/api/v1.0/precipitation/startdate/enddate/<br/>"
    )

@app.route("/api/v1.0/precipitation")
def get_prcp_last12():
    """Return a list of all dates and temperature from the last 12 months"""
    results = []

    results = get_precipitation_records("max")

    return jsonify(results)
   
            
@app.route("/api/v1.0/precipitation/<enddate>")
def get_prcp(enddate):
    """Return a list of all dates and temperature for the last year from the date entered"""
    results = []    
    
    results = get_precipitation_records(enddate)
            
    return jsonify(results)


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations """
    # Query stations
    results = session.query(Station.station, Station.name).all()

    # Create a dictionary from the row data and append to a list of all_passengers
    stations = []
    for station in results:
        print(station.name)
        station_dict = {}
        station_dict["station"] = station.station
        station_dict["name"] = station.name
        stations.append(station_dict)

    return jsonify(stations)

@app.route("/api/v1.0/tobs")
def get_tobs_last12():
    """Return a list of all dates and temperature from the last year from the date entered"""
    results = []    
        
    results = get_temperature_records("max")
            
    return jsonify(results)

@app.route("/api/v1.0/tobs/<enddate>")
def get_tobs(enddate):
    """Return a list of all dates and temperature from the last year from the date entered"""
    results = []    
            
    results = get_temperature_records(enddate)
            
    return jsonify(results)


@app.route("/api/v1.0/<startdate>")
def get_min_avg_max(startdate):
    """Return a list of all dates and mi max and avg from the last year from the date entered"""
    results = []
    
    results = calc_temperature(startdate, "max")
            
    return jsonify(results)


@app.route("/api/v1.0/<startdate>/<enddate>")
def get_min_avg_max_range(startdate, enddate):
    """Return a list of all dates and mi max and avg from the last year from the date entered"""
    results = []
    
    results = calc_temperature(startdate, enddate)
            
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
