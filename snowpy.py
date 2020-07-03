def run_SQL(SQL_dir_path,SQL_file_name,to_csv=False,col_names_upper=False,append=False):
    """
    Run SQL on Snowflake and return data in a python dictionary or CSV file

    IMPORTANT: 
            in the "from" clause of SQL, you need to include warehouse, 
            schema and table names
            e.g. ODS_APAC_PROD.RD.AT_PAGES

            "login.txt" must be in the same directory as .py file
            in the login.txt file:
                - firstline  -->  username
                - secondline -->  password
                - thirdline  -->  account (e.g. johnpaul_us.us-east-1)

    SQL_dir_path:     
        the path of the folder which contains SQL files

    SQL_file_name:    
        file name of the SQL to be executed

    to_csv:
        optional, if true, saves data returned to a CSV file 
        in the directory of the .py file
        default: False

    col_names_upper:  
        optional, if true and to_csv = True, 
        caplitalises column header in CSV file
        default: False

    append:  
        optional, if true and to_csv = True, 
        append data to existing "data.csv" file
        default: False   
    """                      
    import os

    class Error(Exception):
        """Base class for other exceptions"""
        pass

    class SQL_File_Not_Found(Error):
        """Raised when path of SQL file is not valid"""
        pass

    class Login_File_Not_Found(Error):
        """Raised when path of login details file is not valid"""
        pass

    login_file = 'login.txt'
    
    dir_path = os.path.realpath(__file__)
    dir_path = os.path.dirname(dir_path)
    login_file_path = os.path.join(dir_path,login_file)

    try:
        if not os.path.isfile(os.path.join(SQL_dir_path,SQL_file_name)):
            raise SQL_File_Not_Found
        if not os.path.isfile(login_file_path):
            raise Login_File_Not_Found
    except SQL_File_Not_Found:
        print('Path to SQL file is NOT valid!')
    except Login_File_Not_Found:
        print('Path to login file is NOT valid!')
    else:
        def read_sql(dir_path,file_name):
            dir_path = dir_path
            file_name = file_name
            file_path = os.path.join(dir_path,file_name)
            with open(file_path,'r') as f_file:
                reader = f_file.read()
            return reader

        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine
        import snowflake.connector
        import logging
        import pandas as pd
        import datetime as dt
        
        logging_file = 'snowflake_python_connector_log.txt'
        logging_file_path = os.path.join(dir_path,logging_file)
        logging_file_in_dir = os.path.isfile(logging_file_path)

        if logging_file_in_dir:
            with open(logging_file_path,'w'):
                pass

        logging.basicConfig(
            filename = logging_file_path,
            level = logging.DEBUG)
        time_now = pd.Timestamp.now('GMT')
        logging.debug(f'-------- Started (GMT: {time_now} --------')

        with open(login_file_path,'r') as login:
            reader = login.read().splitlines()

        engine = create_engine(URL(
            user = reader[0],
            password = reader[1],
            account = reader[2],
            database = 'DATA',
            autocommit = False
        ))
        # IMPORTANT: in the "from" clause of SQL, you need to include warehouse, schema and table names
        #            e.g. ODS_APAC_PROD.RD.AT_PAGES

        SQL = read_sql(SQL_dir_path,SQL_file_name)

        connection = engine.connect()

        df = pd.DataFrame()

        try:
            time_start = pd.Timestamp.now()
            df = pd.read_sql_query(SQL, engine)
            time_end = pd.Timestamp.now()
            timer = pd.Timedelta(time_end-time_start).microseconds/1000
            if __name__ == '__main__': 
                print(timer)
        except snowflake.connector.errors.ProgrammingError as e:
            connection.rollback()
            print(e)
        except snowflake.connector.errors.ForbiddenError:
            connection.rollback()
            print('Incorrect account name!')
        except snowflake.connector.errors.DatabaseError as e1:
            connection.rollback()
            print(e1)
        else:
            connection.commit()
        finally:
            connection.close()
            engine.dispose()
            logging.debug('-------- Finished --------' )
            if to_csv:
                col_names = df.columns.tolist()
                if col_names_upper:
                    col_names = [x.upper() for x in col_names]
                csv_file_name = 'data.csv'
                csv_path = os.path.join(dir_path,csv_file_name)
                if append:
                    mode='a'
                else:
                    mode='w'
                df.to_csv(csv_path,index=False, mode=mode, header=col_names)
                return None
            else:
                return df

# -----------------------------------------------------------------------------------------------------------

def run_SQL_to_feather(SQL_dir_path,SQL_file_name,feather_file_name_path='data.feather',col_names_upper=True):
    """
    Run SQL on Snowflake and return data in a feather file

    IMPORTANT: 
            in the "from" clause of SQL, you need to include warehouse, 
            schema and table names
            e.g. ODS_APAC_PROD.RD.AT_PAGES

            "login.txt" must be in the same directory as .py file
            in the login.txt file:
                - firstline  -->  username
                - secondline -->  password
                - thirdline  -->  account (e.g. johnpaul_us.us-east-1)

    SQL_dir_path:     
        the path of the folder which contains SQL files

    SQL_file_name:    
        file name of the SQL to be executed

    feather_file_name_path:
        optional, set feather file name
        default: 'data.feather'

    col_names_upper:  
        optional, caplitalises column headers in feather file
        default: True  
    """                      
    import os

    class Error(Exception):
        """Base class for other exceptions"""
        pass

    class SQL_File_Not_Found(Error):
        """Raised when path of SQL file is not valid"""
        pass

    class Login_File_Not_Found(Error):
        """Raised when path of login details file is not valid"""
        pass

    login_file = 'login.txt'
    
    dir_path = os.path.realpath(__file__)
    dir_path = os.path.dirname(dir_path)
    login_file_path = os.path.join(dir_path,login_file)

    try:
        if not os.path.isfile(os.path.join(SQL_dir_path,SQL_file_name)):
            raise SQL_File_Not_Found
        if not os.path.isfile(login_file_path):
            raise Login_File_Not_Found
    except SQL_File_Not_Found:
        print('Path to SQL file is NOT valid!')
    except Login_File_Not_Found:
        print('Path to login file is NOT valid!')
    else:
        def read_sql(dir_path,file_name):
            dir_path = dir_path
            file_name = file_name
            file_path = os.path.join(dir_path,file_name)
            with open(file_path,'r') as f_file:
                reader = f_file.read()
            return reader

        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine
        import snowflake.connector
        import logging
        import pandas as pd
        import datetime as dt
        
        logging_file = 'snowflake_python_connector_log.txt'
        logging_file_path = os.path.join(dir_path,logging_file)
        logging_file_in_dir = os.path.isfile(logging_file_path)

        if logging_file_in_dir:
            with open(logging_file_path,'w'):
                pass

        logging.basicConfig(
            filename = logging_file_path,
            level = logging.DEBUG)
        time_now = pd.Timestamp.now('GMT')
        logging.debug(f'-------- Started (GMT: {time_now} --------')

        with open(login_file_path,'r') as login:
            reader = login.read().splitlines()

        engine = create_engine(URL(
            user = reader[0],
            password = reader[1],
            account = reader[2],
            database = 'DATA',
            autocommit = False
        ))
        # IMPORTANT: in the "from" clause of SQL, you need to include warehouse, schema and table names
        #            e.g. ODS_APAC_PROD.RD.AT_PAGES

        SQL = read_sql(SQL_dir_path,SQL_file_name)

        connection = engine.connect()

        df = pd.DataFrame()

        try:
            time_start = pd.Timestamp.now()
            df = pd.read_sql_query(SQL, engine)
            time_end = pd.Timestamp.now()
            timer = pd.Timedelta(time_end-time_start).microseconds/1000
            if __name__ == '__main__':
                print(timer)
        except snowflake.connector.errors.ProgrammingError as e:
            connection.rollback()
            print(e)
        except snowflake.connector.errors.ForbiddenError:
            connection.rollback()
            print('Incorrect account name!')
        except snowflake.connector.errors.DatabaseError as e1:
            connection.rollback()
            print(e1)
        else:
            connection.commit()
        finally:
            connection.close()
            engine.dispose()
            logging.debug('-------- Finished --------' )

            if not df.empty:
                if col_names_upper:
                    df.columns = [c.upper() for c in df.columns]
                feather_path = os.path.join(dir_path,feather_file_name_path)
                df.to_feather(feather_path)
                return None
            else:
                return None