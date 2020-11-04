import logging
from flask import Blueprint
from flask_restful import Api
from com_dayoung_api.resources.home import Home
from com_dayoung_api.resources.movie import Movie, Movies, MovieSearch, MovieDel
from com_dayoung_api.resources.reco_movie import RecoMovie, RecoMovies, RecoMovieSearch, RecoMovieDel
from com_dayoung_api.resources.review import Review, Reviews
# from com_dayoung_api.resources.actor import Actor, Actors
from com_dayoung_api.resources.user import User, Users, Auth, Access
from com_dayoung_api.resources.item import Item, Items

home = Blueprint('home', __name__, url_prefix='/api')
user = Blueprint('user', __name__, url_prefix='/api/user')
users = Blueprint('users', __name__, url_prefix='/api/users')
auth = Blueprint('auth', __name__, url_prefix='/api/auth')
access = Blueprint('access', __name__, url_prefix='/api/access')
movie = Blueprint('movie', __name__, url_prefix='/api/movie')
movies = Blueprint('movies', __name__, url_prefix='/api/movies')
moviesearch = Blueprint('moviesearch', __name__, url_prefix='/api/moviesearch')
moviedel = Blueprint('moviedel', __name__, url_prefix='/api/moviedel')
recomovie = Blueprint('recomovie', __name__, url_prefix='/api/recomovie')
recomovies = Blueprint('recomovies', __name__, url_prefix='/api/recomovies')
recomoviesearch = Blueprint('recomoviesearch', __name__, url_prefix='/api/recomoviesearch')
recomoviedel = Blueprint('recomoviedel', __name__, url_prefix='/api/recomoviedel')
review = Blueprint('review', __name__, url_prefix='/api/review')
reviews = Blueprint('reviews', __name__, url_prefix='/api/reviews')
actor = Blueprint('actor', __name__, url_prefix='/api/actor')
actors = Blueprint('actors', __name__, url_prefix='/api/actors')

api = Api(home)
api = Api(user)
api = Api(users)
api = Api(auth)
api = Api(access)
api = Api(movie)
api = Api(movies)
api = Api(moviesearch)
api = Api(moviedel)
api = Api(recomovie)
api = Api(recomovies)
api = Api(recomoviesearch)
api = Api(recomoviedel)
api = Api(review)
api = Api(reviews)
# api = Api(actor)
# api = Api(actors)

def initialize_routes(api):
    print('========== 2 ==========')
    api.add_resource(Home, '/api')
    api.add_resource(Movie, '/api/movie')
    api.add_resource(Movies, '/api/movies')
    api.add_resource(MovieSearch, '/api/moviesearch/<string:title>')
    api.add_resource(MovieDel, '/api/moviedel')
    api.add_resource(RecoMovie, '/api/recomovie')
    api.add_resource(RecoMovies, '/api/recomovies')
    api.add_resource(RecoMovieSearch, '/api/recomoviesearch/<string:title>')
    api.add_resource(RecoMovieDel, '/api/recomoviedel')
    api.add_resource(Review, '/api/review/<string:id>')
    api.add_resource(Reviews, '/api/reviews')
    # api.add_resource(Actor, '/api/actor<string:id>')
    # api.add_resource(Actors, '/api/actors')
    api.add_resource(User, '/api/user/<string:id>')
    api.add_resource(Users, '/api/users')
    api.add_resource(Auth, '/api/auth')
    api.add_resource(Access, '/api/access')
    api.add_resource(Item, '/api/item/<string:id>')
    api.add_resource(Items,'/api/items')



@user.errorhandler(500)
def user_api_error(e):
    logging.exception('An error occurred during user request. %s' % str(e))
    return 'An internal error occurred.', 500

@home.errorhandler(500)
def home_api_error(e):
    logging.exception('An error occurred during home request. %s' % str(e))
    return 'An internal error occurred.', 500

@review.errorhandler(500)
def review_api_error(e):
    logging.exception('An error occurred during review request. %s' % str(e))
    return 'An internal error occurred.', 500

@movie.errorhandler(500)
def movie_api_error(e):
    logging.exception('An error occurred during movie request. %s' % str(e))
    return 'An internal error occurred.', 500

@movie.errorhandler(500)
def reco_movie_api_error(e):
    logging.exception('An error occurred during reco_movie request. %s' % str(e))
    return 'An internal error occurred.', 500