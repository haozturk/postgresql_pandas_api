import psycopg2
import psycopg2.extras
import numpy as np
import pandas as pd
import io
from typing import Iterator, Optional, Any
import time

connect_str = "dbname='algosis_db' user='tony' host='localhost' password='tony'"

# Creates a table with the name of the frame, if the table does not exist
def create_table(frame):
    #get table name
    table_name = frame.name
    row_count = frame.shape[0]

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        cursor.execute("select * from information_schema.tables where table_name=%s", (table_name.lower(),))
        
        doesTableExist = bool(cursor.rowcount)

        if(not doesTableExist):
            print("-----------Table creation------------")


            query_s = 'CREATE TABLE ' + \
                      table_name + \
                      ' ' + \
                      '( ' + \
                      'id serial PRIMARY KEY, '

            for column in frame.columns:

                if frame.dtypes[column] == np.object:
                    query_s += str(column) + " TEXT,"
                elif frame.dtypes[column] == np.int64:
                    query_s += str(column) + " INTEGER,"
                elif frame.dtypes[column] == np.float64:
                    query_s += str(column) + " FLOAT,"
                elif frame.dtypes[column] == '<M8[ns]':
                    query_s += str(column) + " TIMESTAMP,"

       

            # Delete last comma
            query_s = query_s[:-1]
            query_s += ");"

            cursor.execute(query_s)
            conn.commit() # <--- makes sure the change is shown in the database        
        
        cursor.close()
        conn.close()
    except Exception as e:

        print("Create Table Error!")
        print(e)


def insert(frame):

    #get table name
    table_name = frame.name

    row_count = frame.shape[0]

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Create table if it does not exist
        create_table(frame) 

        # Insert row by row
        values = get_values_s(frame);
        for idx in range(row_count):
            query_s = 'INSERT INTO ' + table_name +  ' ' + \
                                   get_column_names(frame) + ' ' + \
                                  'VALUES' + ' ' + \
                                   values[idx] + ';'

            cursor.execute(query_s)
            conn.commit() # <--- makes sure the change is shown in the database
        
        
        cursor.close()
        conn.close()
    except Exception as e:

        print("Insert Error!")
        print(e)

def insert_many(frame):

    #get table name
    table_name = frame.name

    row_count = frame.shape[0]

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Create table if it does not exist
        create_table(frame) 
      
        query_s = 'INSERT INTO ' + table_name +  ' ' + \
                                   get_column_names(frame) + ' ' + \
                                  'VALUES' + ' ' + \
                                  '('

        for i in range(len(frame.columns)):
            query_s += '%s,'

        query_s = query_s[:-1]
        query_s += ');'

        
        start = time.perf_counter()
        
        #variables = get_values_l(frame); #prev bottleneck
        variables = get_values_efficient(frame); 

        elapsed = time.perf_counter() - start
        print(f'Get values in many: Time {elapsed:0.4} seconds')

        cursor.executemany(query_s,variables)
        conn.commit() # <--- makes sure the change is shown in the database
        
        
        cursor.close()
        conn.close()
    except Exception as e:

        print("Insert Error!")
        print(e)

def insert_batch(frame):

    #get table name
    table_name = frame.name

    row_count = frame.shape[0]

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Create table if it does not exist
        create_table(frame)        

        query_s = 'INSERT INTO ' + table_name +  ' ' + \
                                   get_column_names(frame) + ' ' + \
                                  'VALUES' + ' ' + \
                                  '('

        for i in range(len(frame.columns)):
            query_s += '%s,'

        query_s = query_s[:-1]
        query_s += ');'


        start = time.perf_counter()
        
        #variables = get_values_l(frame); #prev bottleneck
        variables = get_values_efficient(frame); 

        elapsed = time.perf_counter() - start
        print(f'Get values in many: Time {elapsed:0.4} seconds')

        psycopg2.extras.execute_batch(cursor, query_s, variables)

        conn.commit() # <--- makes sure the change is shown in the database
        
        cursor.close()
        conn.close()
    except Exception as e:

        print("Insert Error!")
        print(e)


def insert_batch_page(frame, page_size):

    #get table name
    table_name = frame.name

    row_count = frame.shape[0]

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Create table if it does not exist
        create_table(frame)
        

        query_s = 'INSERT INTO ' + table_name +  ' ' + \
                                   get_column_names(frame) + ' ' + \
                                  'VALUES' + ' ' + \
                                  '('

        for i in range(len(frame.columns)):
            query_s += '%s,'

        query_s = query_s[:-1]
        query_s += ');'

        start = time.perf_counter()
        
        #variables = get_values_l(frame); #prev bottleneck
        variables = get_values_efficient(frame); 

        elapsed = time.perf_counter() - start
        print(f'Get values in batch: Time {elapsed:0.4} seconds')

        psycopg2.extras.execute_batch(cursor, query_s, variables, page_size=page_size)

        conn.commit() # <--- makes sure the change is shown in the database
        
        cursor.close()
        conn.close()
    except Exception as e:

        print("Insert Error!")
        print(e)


def insert_sqlalchemy(frame):
    
    try:
        from sqlalchemy import create_engine
        engine = create_engine('postgresql+psycopg2://tony:tony@localhost/algosis_db')
    
        table_name = frame.name

        create_table(frame)

        frame.to_sql(table_name, engine,if_exists='append',index=False)

    except Exception as e:
        print(e)

def insert_copy(frame):

    #get table name
    table_name = frame.name

    row_count = frame.shape[0]

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        create_table(frame)
        
        start = time.perf_counter()
        
        variables = get_values_efficient(frame);

        elapsed = time.perf_counter() - start
        print(f'Get values in batch: Time {elapsed:0.4}')

        csv_file_like_object = io.StringIO()

        csv_file_like_object.write('|'.join(map(clean_csv_value,(tuple(tuple(variables))))) + '\n')

        csv_file_like_object.seek(0)

        conn.commit() # <--- makes sure the change is shown in the database

    
        cursor.copy_from(csv_file_like_object, table_name, sep='|')

        cursor.close()
        conn.close()
    except Exception as e:

        print("Insert Error!")
        print(e)




def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def search():

    try:
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # Fetch all the tables in the database
        cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")

        result = []
        for table in cursor.fetchall():
            result.append(table)

        cursor.close()
        conn.close()

        #return with its name
        return pd.DataFrame.from_records(result)
        
    except Exception as e:

        print("Rename Error!")
        print(e)



def rename(current_table_name, new_table_name):


    try:

        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        s = "ALTER TABLE IF EXISTS " + current_table_name + " RENAME TO " + new_table_name + ";"

        cursor.execute(s)

        print("SQL Query: ")
        print(s)

        conn.commit() # <--- makes sure the change is shown in the database

        cursor.close()
        conn.close()
        
    except Exception as e:

        print("Rename Error!")
        print(e)

# ! Be careful with the frame name
def get(table_name):

    try:

        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        df = pd.read_sql_query('select * from ' + table_name,con=conn)


        conn.close()

        return df

    except Exception as e:

        print("Get Table Error!")
        print(e)

def drop_table(table_name):

    try:

        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)

        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()

        # create a new table with a single column called "name"
        cursor.execute('DROP TABLE ' + table_name + ';')


        conn.commit() # <--- makes sure the change is shown in the database
        
        cursor.close()
        conn.close()
    except Exception as e:

        print("Drop Table Error!")
        print(e)


def get_column_names(frame):
    column_s = '('
    for col in frame.columns:
        column_s += col + ','

    #remove last comma
    column_s = column_s[:-1]
    column_s += ')'

    #print("Column_s: " + column_s)
    return column_s 

def get_values_s(frame):

    result = {}
    for index, row in frame.iterrows():

        values_s = '('
        for i in range(len(frame.columns)):

            if frame.dtypes[frame.columns[i]] == np.object:
                values_s += '\'' + str(row[frame.columns[i]]) + '\'' + ',' # If string, put ''
            elif frame.dtypes[frame.columns[i]] == np.int64:
                values_s +=  str(row[frame.columns[i]]) + ','
            elif frame.dtypes[frame.columns[i]] == np.float64:
                values_s +=  str(row[frame.columns[i]]) + ','
            else:
                values_s += '\'' + str(row[frame.columns[i]]) + '\'' + ',' # If string, put ''
            #elif frame.dtypes[frame.columns[i]] == pd.to_datetime(frame.columns[i]):  # Look at here!
            #values_s +=  str(row[frame.columns[i]]) + ','

        # Delete last comma
        values_s = values_s[:-1]
        values_s += ')'
        
        result[index] = values_s

    return result

def get_values_l(frame):

    result = []
    for index, row in frame.iterrows():

        current_item = []
        for i in range(len(frame.columns)):

            if frame.dtypes[frame.columns[i]] == np.object:
                current_item.append('\'' + str(row[frame.columns[i]]) + '\'' + ',')
            elif frame.dtypes[frame.columns[i]] == np.int64:
                current_item.append(row[frame.columns[i]])
            elif frame.dtypes[frame.columns[i]] == np.float64:
                current_item.append(row[frame.columns[i]])
            else:
                current_item.append(row[frame.columns[i]])

        
        #result.append(tuple(current_item))
        result.append(current_item)

    return 

def get_values_efficient(frame):

    return frame.values.tolist()

