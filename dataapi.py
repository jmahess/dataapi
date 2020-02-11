#!/usr/bin/env python3

from flask import Flask, jsonify, g, request
from sqlite3 import dbapi2 as sqlite3
DATABASE = './db/test.db'
app = Flask(__name__)

def get_db():
	db = getattr(g, '_database', None)
	if db is None:
		db = g._database = sqlite3.connect(DATABASE)
		db.execute('CREATE TABLE IF NOT EXISTS users (name TEXT, sex TEXT, age INT)')

		db.row_factory = sqlite3.Row
	return db

@app.teardown_appcontext
def close_connection(exception):
	db = getattr(g, '_database', None)
	if db is not None: db.close()

def query_db(query, args=(), one=False):
	cur = get_db().execute(query, args)
	rv = cur.fetchall()
	cur.close()
	return (rv[0] if rv else None) if one else rv

def init_db():
	with app.app_context():
		db = get_db()
		with app.open_resource('schema.sql', mode='r') as f:
			db.cursor().executescript(f.read())
		db.commit()

def add_user_to_db(name='test', age=10, sex='male'):
	# users is the name of the table in the sqldb
	sql = "INSERT INTO users (name, sex, age) VALUES('%s', '%s', %d)" %(name, sex, int(age))
	print(sql)
	db = get_db()
	db.execute(sql)
	res = db.commit()
	return res

def find_user_from_db(name=''):
	sql = "select * from users where name = '%s' limit 1" %(name)
	print(sql)
	db = get_db()
	rv = db.execute(sql)
	res = rv.fetchall()
	rv.close()
	return res[0]


@app.route('/')
def users():
	return jsonify(hello='world')

@app.route('/add',methods=['POST'])
def add_user():
	print(add_user_to_db(name=request.form['name'], age=request.form['age'], sex=request.form['sex']))
	return ''

@app.route('/add',methods=['GET'])
def find_user_by_name():
	name = request.args.get('name', '')
	user = find_user_from_db(name)
	return jsonify(name=user['name'], age=user['age'], sex=user['sex'])

if __name__ == '__main__' : app.run(debug=True)