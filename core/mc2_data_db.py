from flask_sqlalchemy import SQLAlchemy
from flask import Flask
import pymysql
from datetime import datetime
from dateutil.parser import parse
import db_config

# connection with mysql database
app = Flask(__name__)
app.config.from_object(db_config)
db = SQLAlchemy(app)


class CC(db.Model):
    __tablename__ = 'cc_data'
    new_id = db.Column(db.Integer, primary_key = True)
    last4ccnum = db.Column(db.Integer)
    timestamp = db.Column(db.String(255))
    location = db.Column(db.String(255))
    price = db.Column(db.String(255))

class CAR_ASGMT(db.Model):
    __tablename__ = 'car_assignments'
    new_id = db.Column(db.Integer, primary_key = True)
    lastname = db.Column(db.String(255))
    firstname = db.Column(db.String(255))
    carid = db.Column(db.String(255))
    currentemploymenttype = db.Column(db.String(255))
    currentemploymenttitle = db.Column(db.String(255))

class GPS(db.Model):
    __tablename__ = 'gps'
    new_id = db.Column(db.Integer, primary_key = True)
    timestamp = db.Column(db.String(255))
    id = db.Column(db.String(255))
    lat = db.Column(db.String(255))
    long = db.Column(db.String(255))

class LOYALTY(db.Model):
    __tablename__ = 'loyalty'
    new_id = db.Column(db.Integer, primary_key = True)
    loyaltynum = db.Column(db.String(255))
    timestamp = db.Column(db.String(255))
    location = db.Column(db.String(255))
    price = db.Column(db.String(255))

db.create_all()


class VastDBInit():
    def __init__(self):
        pass

    def select_cc(self):
        # Open database connection
        db = pymysql.connect(user = db_config.USERNAME , password = db_config.PASSWORD,
                            host = db_config.HOSTNAME,
                            database = db_config.DATABASE )
        # prepare a cursor object using cursor() method
        cursor = db.cursor()
        sql = """select * from cc_data"""
        cursor.execute(sql)
        creditcard_record_list = cursor.fetchall()
        db.close()
        return creditcard_record_list

    def select_car(self):
        # Open database connection
        db = pymysql.connect(user = db_config.USERNAME , password = db_config.PASSWORD,
                            host = db_config.HOSTNAME,
                            database = db_config.DATABASE )
        # prepare a cursor object using cursor() method
        cursor = db.cursor()
        sql = """select * from car_assignments"""
        cursor.execute(sql)
        car_record_list = cursor.fetchall()
        db.close()
        return car_record_list

    def select_gps(self):
        # Open database connection
        db = pymysql.connect(user = db_config.USERNAME , password = db_config.PASSWORD,
                            host = db_config.HOSTNAME,
                            database = db_config.DATABASE )
        # prepare a cursor object using cursor() method
        cursor = db.cursor()
        sql = """select * from gps"""
        cursor.execute(sql)
        gps_record_list = cursor.fetchall()
        db.close()
        return gps_record_list

    def select_loyalty(self):
        # Open database connection
        db = pymysql.connect(user = db_config.USERNAME , password = db_config.PASSWORD,
                            host = db_config.HOSTNAME,
                            database = db_config.DATABASE )
        # prepare a cursor object using cursor() method
        cursor = db.cursor()
        sql = """select * from loyalty"""
        cursor.execute(sql)
        loyalty_record_list = cursor.fetchall()
        db.close()
        return loyalty_record_list


def init_cc_data():
    cc_record_list = VastDBInit().select_cc()
    for cc_record in cc_record_list:
        timestamp = parse(cc_record[1])
        cc_data = CC(
                    timestamp = timestamp,
                    location = cc_record[2],
                    price = cc_record[3],
                    last4ccnum = cc_record[4])
        db.session.add(cc_data)
        db.session.commit()

def init_car_assigments():
    car_record_list = VastDBInit().select_car()
    for car_record in car_record_list:
        car_assigments = CAR_ASGMT(
                    lastname = car_record[1],
                    firstname = car_record[2],
                    carid = car_record[3],
                    currentemploymenttype = car_record[4],
                    currentemploymenttitle = car_record[5])
        db.session.add(car_assigments)
        db.session.commit()

def init_gps():
    gps_record_list = VastDBInit().select_gps()
    for gps_record in gps_record_list:
        timestamp = parse(gps_record[1])
        gps = GPS(
                    timestamp = timestamp,
                    id = gps_record[2],
                    lat = gps_record[3],
                    long = gps_record[4])
        db.session.add(gps)
        db.session.commit()

def init_loyalty_data():
    loyalty_record_list = VastDBInit().select_loyalty()
    for loyalty_record in loyalty_record_list:
        timestamp = parse(loyalty_record[1])
        loyalty_data = LOYALTY(
                    timestamp = timestamp,
                    location = loyalty_record[2],
                    price = loyalty_record[3],
                    loyaltynum = loyalty_record[4])
        db.session.add(loyalty_data)
        db.session.commit()


if __name__ == "__main__":
    init_cc_data()
    init_car_assigments()
    init_gps()
    init_loyalty_data()