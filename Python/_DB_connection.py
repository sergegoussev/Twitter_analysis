# -*- coding: utf-8 -*-
"""
DB_conn v1.0
release date: 2018-01-07
author: @SergeGoussev
"""
import MySQLdb, sys
	
class DB:
    '''
    The DB oject creates a connection to the MySQL server database, verifies db 
    connection based on db name specified, and executes prepared statemetns. 
    
    Acts as a single interface for I/O to the DB server
    '''
    def __init__(self, db_name):
        if db_name is not "":
            try:
                self.connect(db_name)
                print("Successfully connected to {} database".format(db_name))
            except:
                print('No such database exists, please check spelling and try again')
                sys.exit()
        else:
            print("No DB selected, please specify DB and try again")
            sys.exit()

    def connect(self, db_chosen):
        self.conn = MySQLdb.connect(host="*******",
            user="*******",
            password="*******!",
            db=db_chosen,
            charset="*******")

    def query(self, sql_query, values=None, q_type="SELECT"):
        '''
        This is the function that passes in the query, with 3 options:
            
        If a SELECT query, a result is returned (no extra input necessary)
        If an INSERT query (nothing returned):
            - need q_type = 'INSERT'
            - values - the values that will be committed
            - NOTE: REPLACE works similarly if specified 
        If a CREATE query (nothing returned):
            - need q_type = 'CREATE'
            - values = None (or skip)

        The error handling is done so as to re-create the cursor in case it 
        expires through lack of use.
        '''
        #cursor error handler:
        try:
            c = self.conn.cursor()
            c.execute(sql_query, values)
    
        #suppress the duplicate entry error (but not others)
        except MySQLdb.IntegrityError as e:
            if e.args[0] == 1062:
                pass
            else:
                raise e
                
        #re-establish the cursor if expired
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            c = self.conn.cursor()
            c.execute(sql_query, values)

        #return function based on input type
        if q_type is 'INSERT' or q_type is 'REPLACE':
            self.conn.commit()
        if q_type is 'SELECT':
            return c.fetchall()
        
if __name__ == '__main__':
    pass