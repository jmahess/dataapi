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
		# create the tables if it is the first time accessing the database, auto increment the primary key
		# note: in a product system these statements would be removed as the tables would be known to exist
		# I have left the statements in here in case the test.db file is deleted, this allows the tables
		# to be recreated without any manual setup
		db.execute('CREATE TABLE IF NOT EXISTS users (userid INTEGER PRIMARY KEY, username TEXT, password_hash TEXT, timestamp TEXT)')
		db.execute('CREATE TABLE IF NOT EXISTS messages (msgid INTEGER PRIMARY KEY, text TEXT, author_id TEXT, timestamp TEXT)')
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

# implementing the users endpoint POST functionality to add users
@app.route('/users',methods=['POST'])
def add_user():
	# check if we have all the variables
	username = request.args.get("username")
	password_hash = request.args.get("password_hash")
	timestamp = request.args.get("timestamp")
	# now call the add item method to check
	return add_item('users', username, password_hash, timestamp)

# implementing the messages endpoint POST functionality to add messages
@app.route('/messages',methods=['POST'])
def add_message():
	# check if we have all the variables
	text = request.args.get("text")
	author_id = request.args.get("author_id")
	timestamp = request.args.get("timestamp")
	# now call the add item method to check
	return add_item('messages', text, author_id, timestamp)

# do validation of fields and then insert to database
def add_item(table='users', firstString='', secondString='', timestamp=''):
	# if we dont have the firstString then return as a bad request
	if firstString is None or timestamp is None or secondString is None or len(request.args) != 3:
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

	# users does not allow us to reuse usernames so check for this
	if table == 'users':
		# query the db to check if the username is already in use
		query = 'select * from users where username = (?)'
		args = [firstString]
		got = query_db(query, args, True)

		# if the username is not already taken then add it to the database
		if not (got is None):
			return jsonify(error="username is already in use"), status.HTTP_409_CONFLICT

	# add the item to the relevant database
	add_item_to_db(table, firstString, secondString, timestamp)

	# handle different ID functionality - users does not allow duplicates but messages does
	if table == 'users':
		query = 'select * from users where username = (?) limit 1'
		args = [firstString]
		got = query_db(query, args, True)
	elif table == 'messages':
		query = 'SELECT MAX(msgid) FROM messages'
		args = []
		got = query_db(query, args, True)

	# now we get the ID to return back in the response		
	if table == 'users':
		# now get the id to return
		return jsonify(id=got['userid']), status.HTTP_200_OK
	elif table == 'messages':
		return jsonify(id=got[0]), status.HTTP_200_OK
	else:
		print("Invalid table")
		return '', status.HTTP_400_BAD_REQUEST

# # helper method to add items to db
def add_item_to_db(table='users', firstString='', secondString='', timestamp=''):
	args = [firstString, secondString, timestamp] # the arguments for the query
	# handle the two tables
	if table == 'users':
		query = 'INSERT INTO users (username, password_hash, timestamp) VALUES (?, ?, ?)'
		return query_db(query, args, False)
	elif table == 'messages':
		query = 'INSERT INTO messages (text, author_id, timestamp) VALUES (?, ?, ?)'
		return query_db(query, args, False)

# this helper method finds one specific user from the database
def find_user_from_db(username=''):
	query = 'select * from users where username = (?) limit 1'
	args = [username]
	got = query_db(query, args, True)
	return got

# returns an array of users matching certain criteria
@app.route('/users',methods=['GET'])
def get_user_array():
	# get the variables from the request arguments
	index = request.args.get('index')
	vector = request.args.get('vector')
	sort = request.args.get('sort')

	return get_item_array('users', index, vector, sort)

# returns an array of messages matching certain criteria
@app.route('/messages',methods=['GET'])
def get_messages_array():
	# get the variables from the request arguments
	index = request.args.get('index')
	vector = request.args.get('vector')

	return get_item_array('messages', index, vector, 'timestamp')

# this method helps looking up an array of elements from a table based on
# the index and vector pagination scheme
def get_item_array(table='users', index='0', vector='-10', sort="timestamp"):
	total = 0
	if table == 'users':
		total = getNumberOfUsers()
	elif table == 'messages':
		total = getNumberOfMessages()

	# set default if index was not provided
	if index is None:
		# get the max row in the DB and set the index to the total minus 1 
		# (index is zero indexed)
		# this is the default value for index
		index = total - 1

	# set default if index was not provided
	if vector is None:
		# default vector value
		vector = -10

	# handle default sort value
	if sort is None or len(sort) == 0:
		sort = 'username'
	
	# convert sort to lowercase to ignore the case
	sort = sort.lower()

	# check that index and vector are both integers
	try:
		index = int(index)
		vector = int(vector)
	except:
		print("Index and vector must be integers")		
		return '', status.HTTP_400_BAD_REQUEST

	# check that the index is in the valid range (the number of rows)
	if index >= total:
		print("Index must be less than the total number of users")
		return '', status.HTTP_400_BAD_REQUEST

	# check that the index is non negative
	if index < 0:
		# bad request so return
		print("Index must be non negative")		
		return '', status.HTTP_400_BAD_REQUEST

	# check that vector is non zero (zero vector would yield no results)
	if vector == 0:
		# bad request so return
		print("Vector must be non zero")		
		return '', status.HTTP_400_BAD_REQUEST

	# verify that we have a valid sorting
	if sort != 'username' and sort != 'timestamp':
		# bad request so return
		print("Sort must be 'username' or 'timestamp'")				
		return '', status.HTTP_400_BAD_REQUEST

	# set the correct query based on which table and sorting we are looking for
	if sort == 'username':
		query = 'select * from users order by username'
	elif sort == 'timestamp':
		if table == 'users':
			query = 'select * from users order by timestamp'
		elif table == 'messages':
			query = 'select * from messages order by timestamp'
	else:
		print("Invalid sort value: %s" %(sort))		
		return '', status.HTTP_400_BAD_REQUEST

	# now query the database
	got = query_db(query, [], False)	

	# this will hold the rows before we pick the ones to return
	allRows = list()	

	# iterate over the rows and add them to allrows
	for row in got:
		lrow = list(row)
		allRows.append(lrow)

	# get the start and endpoint for the subarray
	start = 0
	end = 0
	# first case is looking at elements up to index
	if vector < 0:
		end = index
		# move the start index
		if not (index + vector < 0):
			start = index + vector + 1
	# second case is looking at elements from index onwards			
	else:
		start = index
		if index + vector >= len(allRows):
			end = len(allRows) - 1
		else:
			end = index + vector - 1

	# now build the output 
	output = list()
	for i in range(start, end + 1):
		# format the row correctly with labels
		gotRow = dict()
		if table == 'users':
			# add the labels for the users table fields
			gotRow['id'] = allRows[i][0]
			gotRow['username'] = allRows[i][1]
			gotRow['password_hash'] = allRows[i][2]
			gotRow['timestamp'] = allRows[i][3]
		elif table == 'messages':
			# add the labels for the messages table fields
			gotRow['id'] = allRows[i][0]
			gotRow['text'] = allRows[i][1]
			gotRow['author_id'] = allRows[i][2]
			gotRow['timestamp'] = allRows[i][3]			
		output.append(gotRow)
	# return the total length of the underlying array along with the array of data
	return jsonify(total_length=len(allRows), array=output), status.HTTP_200_OK

# method to get the total number of users from the database
def getNumberOfUsers():
	query = 'SELECT COUNT(*) FROM users' # count how many users we have
	args = [] # no arguments for this query
	got = query_db(query, args, False)
	numberOfUsers = int(got[0][0]) # get the number of users out of the record
	return numberOfUsers

# method to get the total number of messages from the database
# could consolidate with getNumberOfUsers but since it is a short method I left it separately
def getNumberOfMessages():
	query = 'SELECT COUNT(*) FROM messages' # count how many users we have
	args = [] # no arguments for this query
	got = query_db(query, args, False)
	numberOfMessages = int(got[0][0]) # get the number of messages out of the record
	return numberOfMessages
	
# run the application
if __name__ == '__main__' : app.run(debug=True)











