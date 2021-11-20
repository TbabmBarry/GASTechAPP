from urllib.parse import quote

USERNAME = 'root'
PASSWORD = '367176'
HOSTNAME = '127.0.0.1'
DATABASE = 'vast'
DB_URI = 'mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(USERNAME, quote(PASSWORD),
                                                           HOSTNAME, DATABASE)

SQLALCHEMY_DATABASE_URI = DB_URI
SQLALCHEMY_TRACK_MODIFICATIONS = True
