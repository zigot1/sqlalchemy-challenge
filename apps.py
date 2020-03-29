import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import Column, Integer, String, Float, Text

from flask import Flask, jsonify


engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Create a "Metadata" Layer That Abstracts our SQL Database
# ----------------------------------
# Create (if not already in existence) the tables associated with our classes.
Base.metadata.create_all(engine)

m1 = Base.classes.measurement
s1 = Base.classes.station
Base.classes.keys()
# Create our session (link) from Python to the DB
session = Session(bind=engine)
Base.metadata.tables
#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    return (
        f"Hawaii - Climate Analysis API!<br/>"
        f"Calls:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the precipitation data for the last year"""
    
    period = dt.date(2018, 7, 1) - dt.timedelta(days=365)

    #all_time = session.query(m1.date, m1.prcp, m1.station, m1.tobs).all()
    one_year = session.query(m1.date, m1.prcp).filter(m1.date >= period).all()

    h_df = pd.DataFrame(one_year, columns=['date', 'precipitation'])
    h_df.set_index(h_df['date'], inplace = True)
    h_df=h_df.sort_index()

    # Dict with date as the key and prcp as the value
    prcp_out = {date: prcp for date, prcp in one_year}
    #prcp_out = h_df.to_json(orient = 'date')
    return jsonify(prcp_out)
   


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations."""
    results = session.query(s1.station).all()

    # Unravel results into a 1D array and convert to a list
    stations = list(np.ravel(results))
    return jsonify(stations)


@app.route("/api/v1.0/tobs")
def temp_monthly():
    """Return the temperature observations (tobs) for previous year."""
    # Calculate the date 1 year ago from last date in database
    #prev_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    st_na = session.query(func.count(s1.station)).all()
    st_na
    s_all = session.query(s1.station, s1.name, s1.elevation, s1.longitude, s1.latitude).all()
    s_df = pd.DataFrame(s_all, columns=['station','name', 'elevation', 'Longitude', 'Latitude'])
    all_df = pd.DataFrame(all_time, columns=['date', 'precipitation','station','tobs'])

    st_list = []
    st_list2 = []
    for row  in s_df.iterrows():
        st_list.append(list(all_df.station).count(row[1].station))

    s_df['frequency'] = st_list
    sorted_s_df = s_df.sort_values(by='frequency')
    sorted_s_df.head(9)
    maxf_st = sorted_s_df['frequency'].max()
    row_maxf_st = sorted_s_df[sorted_s_df['frequency']==maxf_st]
    station_name = row_maxf_st['station'].tolist()
    session.query(func.min(m1.tobs), func.max(m1.tobs), func.avg(m1.tobs)).\
        filter(m1.station == station_name[0]).all()

    r1 = session.query(m1.tobs).\
            filter(m1.station == station_name[0]).\
            filter(m1.date > period).all()
    new_df = pd.DataFrame(r1, columns=['tabs'])

    # Query the primary station for all tobs from the last year
    # results = session.query(m1.tobs).\
    #     filter(m1.station == 'USC00519281').\
    #     filter(m1.date >= prev_year).all()




    # Unravel results into a 1D array and convert to a list
    xx = list(np.ravel(r1))

    # Return the results
    return jsonify(xx)




if __name__ == '__main__':
    app.run()
