import os
import json
import pandas as pd
from random import randint
from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from core.db_config import SQLALCHEMY_DATABASE_URI
from utils.path_parser import routeParser
from utils.freq_parser import freqByHourParser
from utils.freq_parser import freqByDayParser
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
    feature_collection = routeParser(gps_paths)
    return jsonify(feature_collection)


@app.route("/fetch_heatmap", methods=["GET"])
def fetch_heatmap():
    cc_df = pd.read_csv("./static/cc_data.csv")
    loyalty_df = pd.read_csv("./static/loyalty.csv")
    res_cc = freqByHourParser(cc_df)
    res_loyalty = freqByDayParser(loyalty_df)
    return jsonify({
        "cc": res_cc,
        "loyalty": res_loyalty
    })

if __name__ == '__main__':
    app.run()