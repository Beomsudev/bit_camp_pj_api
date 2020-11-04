import logging
from flask import Blueprint
from flask_restful import Api

movie = Blueprint('movie', __name__, url_prefix='/api/movie')
movies = Blueprint('movies', __name__, url_prefix='/api/movies')

api = Api(movie)
api = Api(movies)
print('========== movie3 ==========')
print('========== movie4 ==========')
@movie.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during movie request. %s' % str(e))
    return 'An internal error occurred.', 500