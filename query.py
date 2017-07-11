import sqlalchemy
from sqlalchemy import create_engine ,asc
from sqlalchemy.ext.declarative import declarative_base
from create_data import Base,Status
from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.orm import sessionmaker
import random
from datetime import datetime, timedelta
import time
from sqlalchemy.sql import func

engine = create_engine("postgresql://localhost/vayve")
Base.metadata.bind = engine
DBSession = sessionmaker()
DBSession.bind = engine
session = DBSession()

fmt = '%Y-%m-%d %H:%M:%S'
a = time.time()
minima = session.query(Status.timestamp).filter(Status.status == 'connected').order_by(Status.timestamp.asc()).first()
sum = 0
x=0
while 1==1 and minima:
    upper = session.query(Status.timestamp).filter(Status.status == 'disconnected',Status.timestamp > minima).order_by(Status.timestamp.asc()).first()
    # print(upper[0])
    if upper[0] == None:
        break
    else:
        sum = sum + (upper[0] - minima[0]).total_seconds()
    minima = session.query(Status.timestamp).filter(Status.status == 'connected',Status.timestamp > upper).order_by(Status.timestamp.asc()).first()    
    # 
print(time.time()-a)         
print(sum) 