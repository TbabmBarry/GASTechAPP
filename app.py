import os
import json
import geojson
import pandas as pd
from random import randint
from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from core.db_config import SQLALCHEMY_DATABASE_URI

from dateutil.parser import parse
from datetime import datetime

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})
db_conn = SQLALCHEMY_DATABASE_URI


@app.route("/fetch_map")
def fetch_map():
    with open("./static/Abila.geojson") as map_file:
        map = json.load(map_file)
    return map

@app.route("/fetch_person", methods=["GET"])
def fetch_person():
    engine = create_engine(db_conn)
    Base = automap_base()
    Base.prepare(engine, reflect = True)
    db = sessionmaker(bind = engine)()
    GPS = Base.classes.gps
    CAR_ASGMT = Base.classes.car_assignments
    result_list = db.query(CAR_ASGMT).filter(CAR_ASGMT.CarID.isnot(None))
    person_list = []
    for result in result_list:
        person = {
            "carid": result.CarID,
            "firstname" : result.FirstName,
            "lastname" : result.LastName,
            "color": '#%06X' % randint(0, 0xFFFFFF)}
        person_list.append(person)
    return jsonify(person_list)

@app.route("/fetch_gps", methods=["GET","POST"])
def fetch_gps():
    car_id = request.args.get("id")
    time_start = request.args.get("time_start")
    time_end = request.args.get("time_end")
    time_start = datetime.strptime(time_start, "%y-%m-%d %H:%M")
    time_end = datetime.strptime(time_end, "%y-%m-%d %H:%M")
    engine = create_engine(db_conn)
    Base = automap_base()
    Base.prepare(engine, reflect = True)
    db = sessionmaker(bind = engine)()
    GPS = Base.classes.gps
    result_list = db.query(GPS).filter(GPS.id == car_id).filter(GPS.Timestamp.between(time_start, time_end))
    gps_paths = pd.read_sql(result_list.statement, db.bind)
    gps_paths["Timestamp"] = pd.to_datetime(gps_paths.Timestamp)
    gps_paths["path_id"] = gps_paths.Timestamp.diff().dt.seconds.ge(120).cumsum()
    gps_paths["Timestamp"] = gps_paths.Timestamp.dt.strftime('%Y-%m-%d %H:%M:%S')
    features = []
    gps_paths_groupby = gps_paths.groupby("path_id")
    for path_id, path_group in gps_paths_groupby:
        features.append(geojson.Feature(
            geometry=geojson.LineString(
                coordinates=path_group.reset_index()[['long', 'lat']].values.astype(float).tolist()),
            properties={"path_id": path_id}))
    feature_collection = geojson.FeatureCollection(features)
    return jsonify(feature_collection)
    
    # return jsonify(gps_paths)

if __name__ == '__main__':
    app.run()