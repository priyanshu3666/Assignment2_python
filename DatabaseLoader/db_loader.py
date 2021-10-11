import json
from sqlalchemy import MetaData,Table,VARCHAR,Column,Integer,create_engine, engine
from sqlalchemy.sql.schema import Constraint, ForeignKey, ForeignKeyConstraint
from sqlalchemy.sql.sqltypes import BIGINT
import pandas as pd

meta =MetaData()
engine = create_engine("mysql+mysqlconnector://shukla:a123456@0.0.0.0:3306/sample",echo = True)
connection = engine.connect()
Table(
    'student', meta,
    Column('stu_id', VARCHAR(10), primary_key=True),
    Column('firstName', VARCHAR(50)),
    Column('lastName', VARCHAR(50)),
    Column('gender', VARCHAR(11)),
    Column('age', Integer),
    Column('streetAddress', VARCHAR(30)),
    Column('city', VARCHAR(20)),
    Column('state', VARCHAR(20)),
    Column('postalCode',Integer()),
    
)
Table(
    
    'student_contact', meta,
    Column('cont_id', Integer, primary_key=True, autoincrement=True),
    Column('stu_id', VARCHAR(10),ForeignKey("student.stu_id")),
    Column('type', VARCHAR(10)),
    Column('number', BIGINT())
)

meta.create_all(engine)


try:
    last_insert = connection.execute("select max(stu_id) from student")
    last_id = last_insert.all()[0][0]
except ConnectionError:
    print("connection not established with database ")
    
stu_data = pd.read_csv('student.csv', index_col=False, delimiter = ',')
stu_data.head()
for num,row in stu_data.iterrows():
    sql_query = f"INSERT INTO student VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    connection.execute(sql_query, tuple(row))
        
    
stu_contact_data = pd.read_csv('student_contact.csv', index_col=False, delimiter = ',')
stu_contact_data.head()
for num,row in stu_contact_data.iterrows():
    cont_insert = connection.execute("select max(cont_id) from student_contact")
    cont_id = cont_insert.all()[0][0]
    if cont_id is None:
        cont_id  = 1
        
    else:
        cont_id +=1
    sql_query = f"INSERT INTO student_contact  VALUES ({cont_id},%s,%s,%s)"
    connection.execute(sql_query, tuple(row))

    

