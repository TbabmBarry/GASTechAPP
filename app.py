from random import randint
import pandas as pd
from flask import Flask, jsonify, request
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from core.db_config import SQLALCHEMY_DATABASE_URI


from dateutil.parser import parse
from datetime import datetime

app = Flask(__name__)

db_conn = SQLALCHEMY_DATABASE_URI

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
    # gps_res = []
    # for result in result_list:
    #     gps_record = {
    #         "timestamp": result.Timestamp,
    #         "car_id": result.id,
    #         "latitude": str(result.lat),
    #         "longitude": str(result.long),
    #     }
    #     gps_res.append(gps_record)
    gps_paths = pd.read_sql(result_list.statement, db.bind)
    result = []
    for _, row in gps_paths.iterrows():
        result.append(dict(row))
    return jsonify(result)
    
    # return jsonify(gps_paths)

if __name__ == '__main__':
    app.run()