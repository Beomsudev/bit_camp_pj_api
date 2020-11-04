from flask import Flask
from flask_restful import Api
from flask_cors import CORS
from com_dayoung_api.ext.db import url, db
from com_dayoung_api.ext.routes import initialize_routes
from com_dayoung_api.resources.user import UserDao
from com_dayoung_api.resources.movie import MovieDao
from com_dayoung_api.resources.reco_movie import RecoMovieDao


print('========== 1 ==========')
app = Flask(__name__)
CORS(app, resources={r'/api/*': {"origins": "*"}})

app.config['SQLALCHEMY_DATABASE_URI'] = url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
api = Api(app)

with app.app_context():
    db.create_all()
with app.app_context():
    print('데이터 DB 삽입')
    count_user = UserDao.count()
    count_movie = MovieDao.count()
    count_reco_movie = RecoMovieDao.count()
    print(f'Users Total Count is {count_user}')
    print(f'Movies Total Count is {count_movie}')
    print(f'Reco_Movies Total Count is {count_reco_movie[0]}')

    if count_user == 0:
        UserDao.insert_many()
        
    if count_movie == 0:
        MovieDao.insert_many()

    if count_reco_movie[0] == 0:
        RecoMovieDao.bulk()


# @app.before_first_request
# def create_tables():
#     db.create_all()

initialize_routes(api)