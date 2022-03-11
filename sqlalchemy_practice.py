import sqlalchemy
from pprint import pprint
import os
from dotenv import load_dotenv

load_dotenv()

PASSWORD = os.getenv("PASSWORD")

engine = sqlalchemy.create_engine(f"mysql+pymysql://root:{PASSWORD}@localhost/sakila")

connection = engine.connect()
metadata = sqlalchemy.MetaData() # needed when we are creating table object
actor = sqlalchemy.Table('actor', metadata, autoload=True, autoload_with=engine) # creating table objects
film = sqlalchemy.Table('film', metadata, autoload=True, autoload_with=engine) 
film_actor = sqlalchemy.Table("film_actor", metadata, autoload=True, autoload_with=engine)
# print(actor.columns.keys())
# print(repr(metadata.tables['actor']))

#SELECT
# query = sqlalchemy.select([actor])


#FILTERING DATA

###HWERE###
query = sqlalchemy.select([actor]).where(actor.columns.first_name == 'PENELOPE')


###IN###
query = sqlalchemy.select([actor]).where(actor.columns.first_name.in_(["PENELOPE", "JOHN", "UMA"]))


###AN, OR, NOT###
query = sqlalchemy.select([film]).where(sqlalchemy.and_(film.columns.length > 60, film.columns.rating == "PG"))


###JOIN###
join_actor_and_actor_film = actor.join(film_actor, film_actor.columns.actor_id == actor.columns.actor_id).join(film, film.columns.film_id == film_actor.columns.film_id)

query = sqlalchemy.select([film.columns.film_id, film.columns.title, actor.columns.first_name, actor.columns.last_name]).select_from(join_actor_and_actor_film)

#To get result print out - 2 steps
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall() #fetchall()
# result_set = result_proxy.fetchmany(5) #fetchmany()
# result_set = result_proxy.fetchone() #fetchone()
# print(result_set) #normal print
# pprint(result_set) #pprint


####CREATING A TABLE###

newTable = sqlalchemy.Table(
    "newTable", 
    metadata,
    sqlalchemy.Column("ID", sqlalchemy.Integer()),
    sqlalchemy.Column("name", sqlalchemy.String(255), nullable=False),
    sqlalchemy.Column("salary", sqlalchemy.Float(), default=100.0),
    sqlalchemy.Column("active", sqlalchemy.Boolean(), default=True)
)

# metadata.create_all(engine)


###INSERT###

#Insert key value pair to above newly created NewTable

#Inserting one record
# newTable = sqlalchemy.Table("newTable", metadata, autoload=True, autoload_with=engine)
# query = sqlalchemy.insert(newTable).values(ID=1, name="Software Engineer", salary=60000.00, active=True)
# result_proxy = connection.execute(query)

#Inserting multiple records
# query = sqlalchemy.insert(newTable)
# new_records = [{"ID": "2", "name": "Ai Oakenfull", "salary": 1000000, "active": False},
#             {"ID": "3", "name": "Ramon Oakenfull", "salary": 800000, "active": True}]
# result_proxy = connection.execute(query, new_records)


###UPDATE###

# query = sqlalchemy.update(newTable).values(salary=1400000).where(newTable.columns.ID == 1)
# result = connection.execute(query)


###DELETE###

query = sqlalchemy.delete(newTable).where(newTable.columns.salary == 1400000)
results = connection.execute(query)





