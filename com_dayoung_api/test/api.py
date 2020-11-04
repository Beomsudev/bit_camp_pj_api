from typing import List
from flask import request
from flask_restful import Resource, reqparse
from com_dayoung_api.movie.dto import MovieDto
from com_dayoung_api.movie.dao import MovieDao, MovieVo
import json
from flask import jsonify

parser = reqparse.RequestParser()  # only allow price changes, no name changes allowed
parser.add_argument('movieid', type=str, required=True,
                                        help='This field should be a movieid')


class Movie(Resource):
    @staticmethod
    def post():
        args = parser.parse_args()
        print(f'Movie {args["id"]} added ')
        params = json.loads(request.get_data(), encoding='utf-8')
        if len(params) == 0:

            return 'No parameter'

        params_str = ''
        for key in params.keys():
            params_str += 'key: {}, value: {}<br>'.format(key, params[key])
        return {'code':0, 'message': 'SUCCESS'}, 200
    @staticmethod
    def get(id):
        print(f'Movie {id} added ')
        try:
            movie = MovieDao.find_by_id(id)
            if movie:
                return movie.json()
        except Exception as e:
            return {'message': 'Movie not found'}, 404

    @staticmethod
    def update():
        args = parser.parse_args()
        print(f'Movie {args["id"]} updated ')
        return {'code':0, 'message': 'SUCCESS'}, 200

    @staticmethod
    def delete():
        args = parser.parse_args()
        print(f'Movie {args["id"]} deleted')
        return {'code' : 0, 'message' : 'SUCCESS'}, 200

    
    
class Movies(Resource):
    
    def post(self):
        print('aaaaaaa')
        md = MovieDao()
        md.insert_many('movies')

    def get(self):
        print('========== movie10 ==========')
        data = MovieDao.find_all()
        return data, 200

class Auth(Resource):

    def post(self):
        body = request.get_json()
        movie = MovieDto(**body)
        MovieDao.save(movie)
        id = movie.movieid
        
        return {'id': str(id)}, 200 


class Access(Resource):
    def __init__(self):
        print('========== movie5 ==========')
    def post(self):
        print('========== movie6 ==========')
        args = parser.parse_args()
        movie = MovieVo()
        movie.movieid = args.movieid
        movie.password = args.password
        print(movie.movieid)
        print(movie.password)
        data = MovieDao.login(movie)
        return data[0], 200    