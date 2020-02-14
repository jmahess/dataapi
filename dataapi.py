#!/usr/bin/env python3

# use the flask framework to build the web apis
from flask import Flask, jsonify, g, request
from flask_api import status # for http status codes
import iso8601 # used to verify ISO 8601 timestamp format

# database access - sqllite
from sqlite3 import dbapi2 as sqlite3
# the name of the database
DATABASE = './db/test.db'
app = Flask(__name__)

# method to get access to the database and create the tables if they do not already exist
def get_db():
	db = getattr(g, '_database', None)
	if db is None:
		# connect to the database
		db = g._database = sqlite3.connect(DATABASE)
		# creat the tables if it is the first time accessing the database
		db.execute('CREATE TABLE IF NOT EXISTS users (userid TEXT, username TEXT, password_hash TEXT, time TEXT)')
		db.row_factory = sqlite3.Row
	return db

# close the database connection when we are done
@app.teardown_appcontext
def close_connection(exception):
	db = getattr(g, '_database', None)
	if db is not None: db.close()

# def query_db(query, args=(), one=False):
# 	cur = get_db().execute(query, args)
# 	rv = cur.fetchall()
# 	cur.close()
# 	return (rv[0] if rv else None) if one else rv

# initialize the database
# def init_db():
# 	with app.app_context():
# 		db = get_db()
# 		with app.open_resource('schema.sql', mode='r') as f:
# 			db.cursor().executescript(f.read())
# 		db.commit()

def add_user_to_db(username='test', password_hash='PASSWORDHASH', time='2013-02-04T22:44:30.652Z'):
	# users is the name of the table in the sqldb

	# TODO don's use string insertion - use sql cleansing to avoid sql injection
	sql = "INSERT INTO users (username, password_hash, time) VALUES('%s', '%s', '%s')" %(username, password_hash, time)
	print(sql)
	db = get_db()
	db.execute(sql)
	res = db.commit()
	return res

def find_user_from_db(username=''):
	# TODO don's use string insertion - use sql cleansing to avoid sql injection
	# rowid is the index of the row. It starts a 1 (not zero)
	sql = "select rowid, * from users where username = '%s' limit 1" %(username)
	print(sql)
	db = get_db()
	rv = db.execute(sql)
	res = rv.fetchall()
	rv.close()
	return res[0]

# TODO - remove this before submitting
@app.route('/')
def users():
	return jsonify(hello='world')



# implementing the users endpoint POST functionality to add users
@app.route('/users',methods=['POST'])
def add_user():
	# check if we have all the variables
	username = request.args.get("username")
	timestamp = request.args.get("timestamp")
	password_hash = request.args.get("password_hash")

	# if we dont have the username then return as a bad request
	if username is None or timestamp is None or password_hash is None or len(request.args) != 3:
		# have invalid number or arguents, or invalid argument names
		return '', status.HTTP_400_BAD_REQUEST

	# now verify that we have a valid timestamp format
	try:
		# see if we can succesfully parse the timestamp
	    result = iso8601.parse_date(timestamp)
	except Exception as e: # if not the catch the exception
		# log an error message and the exception
		print('Invalid timestamp format: %s' %(timestamp))
		print(e)
		# return bad request due to invalid date format
		return '', status.HTTP_400_BAD_REQUEST


	# TODO - fix sql injection issue - use better string insertion
	sql = "SELECT * from users WHERE username='%s'" %(username)
	db = get_db()
	cursor=db.cursor()
	cursor.execute(sql)

	exist = cursor.fetchone()
	if exist is None:
		print(add_user_to_db(username=username, time=timestamp, password_hash=password_hash))

		# return jsonify(userid=str(userid)), status.HTTP_200_OK
		return '', status.HTTP_200_OK
	else:
		return jsonify(error="username is already in use"), status.HTTP_409_CONFLICT


# implementing the users endpoint GET functionality to look up users
@app.route('/users',methods=['GET'])
def find_user_by_name():
	# check that we have one argument and that is the username
	username = request.args.get('username')

	if username is None or len(request.args) != 1:
		# have invalid number or arguents, or invalid argument names
		return '', status.HTTP_400_BAD_REQUEST

	user = find_user_from_db(username)
	return jsonify(userid=user['rowid'], username=user['username'], timestamp=user['time'], password_hash=user['password_hash']), status.HTTP_200_OK

# run the application
if __name__ == '__main__' : app.run(debug=True)















