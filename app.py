from flask import Flask, jsonify, request
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

app = Flask(__name__)

gps_conn = "mysql+pymysql://root:367176@127.0.0.1:3306/vast"

@app.route("/init_gps", methods=['GET','POST'])
def search_gps():
    # car_id = request.args.get("id")
    # timestamp = request.args.get("timestamp")
    engine = create_engine(gps_conn)
    Base = automap_base()
    Base.prepare(engine, reflect = True)
    db = sessionmaker(bind = engine)()
    GPS = Base.classes.gps
    result_list = db.query(GPS).filter(GPS.car_id == 35)
    gps_res = []
    for result in result_list:
        gps_record = {
            "timestamp": result.timestamp,
            "car_id": result.car_id,
            "latitude": str(result.lat_),
            "longitude": str(result.long_),
        }
        gps_res.append(gps_record)
    return jsonify(gps_res)
if __name__ == '__main__':
    app.run()