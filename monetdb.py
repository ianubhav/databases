import monetdblite,csv,pandas,time
import numpy as np
from itertools import islice

#Creating database

def create_db(c):
	a = []
	try:
		c.execute('DROP TABLE status;')
	except monetdblite.exceptions.DatabaseError:
		pass
	c.execute('CREATE TABLE status (id int,timestamp bigint,status text);')
	with open('data.csv') as f:
		reader = csv.reader(f)
		print('READER')
		for i in islice(reader,0,10000):
			a.append(i)
		nr = np.asarray(a)
		print('numpy')
	import pdb
	# pdb.set_trace()
	c.insert('status', {'id':nr[:,0].astype('int64'),'timestamp': nr[:,1].astype('datetime64').astype('int64'),'status':nr[:,2].astype('str')})
	c.execute('''CREATE INDEX ts_idx ON status ("timestamp");''')
	print('done here')

def get_time(c):
	sum = 0
	a = time.time()
	c.execute(''' SELECT timestamp FROM status WHERE status='connected' ORDER BY timestamp ASC LIMIT 1''')
	minima = c.fetchall()[0][0]
	while 1==1:
		c.execute(''' SELECT timestamp FROM status WHERE status='disconnected' AND timestamp>%d ORDER BY timestamp ASC LIMIT 1''' %(minima) )
		upper = c.fetchall()[0][0]
		if upper != None:
			sum += upper - minima
		else:
			break
		c.execute(''' SELECT timestamp FROM status WHERE status='connected' AND timestamp>%d ORDER BY timestamp ASC LIMIT 1''' %(upper))
		try:
			minima = c.fetchall()[0][0]
		except:
			break 
	print(time.time()-a)         
	print(sum) 
	return sum






conn = monetdblite.connect('/Users/viper/Desktop/vayve/databases/performance/monetdblite')

c = conn.cursor()

create_db(c
get_time(c)