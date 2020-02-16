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
		# create the tables if it is the first time accessing the database
		db.execute('CREATE TABLE IF NOT EXISTS users (userid TEXT, username TEXT, password_hash TEXT, timestamp TEXT)')
		db.row_factory = sqlite3.Row
	return db

# close the database connection when we are done
@app.teardown_appcontext
def close_connection(exception):
	db = getattr(g, '_database', None)
	if db is not None: db.close()

# method to query the database safely using args instead of direct string insertion. This
# protects against sql injection attack
# query = the sql query to execute
# args = the arguments to insert into the query
# one = do we just want the first result or do we want all the results
def query_db(query, args=(), one=False):
	db = get_db()
	cur = db.execute(query, args)
	rv = cur.fetchall()
	db.commit() # need to commit in case we are inserting new values eg users or messages
	cur.close()
	return (rv[0] if rv else None) if one else rv

# initialize the database
# def init_db():
# 	with app.app_context():
# 		db = get_db()
# 		with app.open_resource('schema.sql', mode='r') as f:
# 			db.cursor().executescript(f.read())
# 		db.commit()

def add_user_to_db(username='test', password_hash='PASSWORDHASH', timestamp='2013-02-04T22:44:30.652Z'):
	# users is the name of the table in the sqldb
	query = 'INSERT INTO users (username, password_hash, timestamp) VALUES (?, ?, ?)'
	args = [username, password_hash, timestamp] # the arguments for the query
	return query_db(query, args, False)

# this method finds one specific user from the database, it does NOT find a list of users
# TODO - functionality to look up a list of multiple users
def find_user_from_db(username=''):
	# rowid is the index of the row. It starts a 1 (not zero)
	query = 'select rowid, * from users where username = (?) limit 1'
	args = [username]
	got = query_db(query, args, True)
	print("Got: %s" %(got))
	return got


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

	# query the db to check if the username is already in use
	query = 'select * from users where username = (?)'
	args = [username]
	got = query_db(query, args, True)

	# if the username is not already taken then add it to the database
	if got is None:
		print(add_user_to_db(username=username, timestamp=timestamp, password_hash=password_hash))
		# now get the userid to return
		got = find_user_from_db(username)

		return jsonify(userid=got['rowid']), status.HTTP_200_OK
		return '', status.HTTP_200_OK
	else:
		return jsonify(error="username is already in use"), status.HTTP_409_CONFLICT



# returns an array of users matching certain criteria
@app.route('/users',methods=['GET'])
def get_user_array():
	# get the variables from the request arguments
	index = request.args.get('index')
	vector = request.args.get('vector')
	sort = request.args.get('sort')

	# set default if index was not provided
	if index is None:
		# get the max row in the DB and set the index to that rowID minus 1 (index is zero indexed, rowid is one indexed)
		# this is the default value for index
		index = getNumberOfUsers() - 1

	# set default if index was not provided
	if vector is None:
		# default vector value
		vector = -10

	# TODO ASK - how to handle empty variables when there is a default?
	if sort is None or len(sort) == 0:
		sort = 'username'
	
	# convert sort to lowercase
	# TODO ASK ben if we need to do this or if it should be case sensitive
	sort = sort.lower()

	# print variables for debugging
	print("Index: %s" %(index))
	print("Vector: %s" %(vector))
	print("Sort: %s" %(sort))

	# check that index and vector are both integers
	try:
		index = int(index)
		vector = int(vector)
	except:
		print("Index and vector must be integers")		
		return '', status.HTTP_400_BAD_REQUEST

	# check that the index is in the valid range (the number of rows)
	if index >= getNumberOfUsers():
		print("Index must be less than the total number of users")
		return '', status.HTTP_400_BAD_REQUEST

	if index < 0:
		# bad request so return
		print("Index must be non negative")		
		return '', status.HTTP_400_BAD_REQUEST

	if sort != 'username' and sort != 'timestamp':
		# bad request so return
		print("Sort must be 'username' or 'timestamp'")				
		return '', status.HTTP_400_BAD_REQUEST

	# now we have the valid variables let's query the db for the information
	query = 'select rowid, * from users order by ?'
	args = [sort]
	got = query_db(query, args, False)	


	# TODO DO NOT USE ROW ID AS USER ID AS IT CHANGES BASED ON OUTPUT !!!! ASSIGN ONE MANUALLY

	print("Selected rows: %s" %(got))
	for row in got:
		print("Got row: %s" %(list(row)))

	# now take the section according to the vector and index


	# return the total length of the underlying array along with the array of data


	# TODO - return the actual list of users
	return '', status.HTTP_200_OK

def getNumberOfUsers():
	query = 'SELECT COUNT(*) FROM users' # count how many users we have
	args = [] # no arguments for this query
	got = query_db(query, args, False)
	numberOfUsers = int(got[0][0]) # get the number of users out of the record
	return numberOfUsers


	
# run the application
if __name__ == '__main__' : app.run(debug=True)








# TODO this is used for TESTING ONLY - remove prior to submission
# implementing the users endpoint GET functionality to look up users
@app.route('/getuser',methods=['GET'])
def find_user_by_name():
	# check that we have one argument and that is the username
	username = request.args.get('username')

	if username is None or len(request.args) != 1:
		# have invalid number or arguents, or invalid argument names
		return '', status.HTTP_400_BAD_REQUEST

	user = find_user_from_db(username)
	return jsonify(userid=user['rowid'], username=user['username'], timestamp=user['timestamp'], password_hash=user['password_hash']), status.HTTP_200_OK

# run the application
if __name__ == '__main__' : app.run(debug=True)















