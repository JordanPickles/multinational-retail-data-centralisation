import pandas as pd
import numpy as np
import yaml

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        pass #Take in yaml and return a dict

    def init_db_engine(self):
        pass # Take in the db_creds output and initialise and return an sql_alchemy database engine

    def list_db_tables(self):
        pass # Using the engine from init_db_engine() list all the tables in the database so you know whhich tables you can extract data from

    def upload_to_db(self):
        pass #this method will take in a pd df and a table name to upload to as an argument


    #Once extracted and cleaned use the upload_to_db method to store the data in your Sales_Data database in a table named dim_users



