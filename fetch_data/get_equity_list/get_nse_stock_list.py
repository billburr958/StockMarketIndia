import mysql.connector
from datetime import datetime
from nselib import capital_market
import logging
import os
import config
from logging.handlers import TimedRotatingFileHandler


class EquityListToMySQL:


    def __init__(self, db_config):
        
        self.db = mysql.connector.connect(**db_config)
        self.cursor = self.db.cursor()
        self.table =config.table_name
        self.date_input_format = config.date_input_format
        self.date_output_format = config.date_output_format


    
    def convert_date_format(self, input_date_string ):

        """
        Convert the date from one format to another.

        Parameters:
        - input_date_string: The input date string.
        - input_format: The format of the input date string.
        - output_format: The desired output format.

        Returns:
        A string representing the date in the desired output format.
        """

        # Convert to datetime object
        date_object = datetime.strptime(input_date_string, self.date_input_format)

        # Format as output format
        formatted_date = date_object.strftime(self.date_output_format)

        return formatted_date


    def create_equity_list_table(self):

        # Create table if not exists
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.table} (
        SYMBOL VARCHAR(255) PRIMARY KEY, 
        NAME VARCHAR(255) NOT NULL,
        SERIES VARCHAR(255),
        LISTING_DATE  VARCHAR(255),
       FACE_VALUE  VARCHAR(255)
        )
        '''
        self.cursor.execute(create_table_query)
        self.db.commit()

    def fetch_equity_df(self):

        """
       
         Function to fetch equity list from NSE's website
        
        """
        
        equity_list_df = capital_market.equity_list()
        equity_list_df.columns = [i.strip() for i in equity_list_df.columns] 
        equity_list_df['DATE OF LISTING'] = equity_list_df['DATE OF LISTING'].apply(lambda x: self.convert_date_format(x))
        return equity_list_df
        
    def symbol_exists(self, symbol):

        """
        
         Function to check if the symbol exists in the table or not.
        
        """

        # Check if symbol exists in the table
        check_query = f'SELECT COUNT(*) FROM {self.table} WHERE symbol = %s'
        self.cursor.execute(check_query, (symbol,))
        result = self.cursor.fetchone()[0]
        return result > 0


    def insert_equity_list_to_db(self, equity_list):


        # Insert equity list into the table
        insert_query = f'INSERT IGNORE INTO {self.table} (symbol, name, series, listing_date, face_value) VALUES (%s, %s, %s, %s, %s)'
        for symbol, name, series, listing_date, face_value in list(equity_list.itertuples(index=False, name=None)):
            if not self.symbol_exists(symbol):
                self.cursor.execute(insert_query, (symbol, name, series, listing_date, face_value))
        self.db.commit()

    def close_connection(self):

        # Close database connection
        self.cursor.close()
        self.db.close()

if __name__=='__main__':

    if not os.path.exists('logs'):
            os.makedirs('logs')

    # Set up logging configuration with TimedRotatingFileHandler
    log_formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    log_handler = TimedRotatingFileHandler(filename='logs/equity_list_scheduler.log', when='midnight', interval=1, backupCount=7)
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)


    try:
    
        db_config = {
            'host': config.host,
            'user': config.user,
            'password': config.password,
            'database': config.database,
        }


        equity_to_mysql = EquityListToMySQL(db_config)
        equity_to_mysql.create_equity_list_table()

        # Fetch equity list from NSE
        equity_df = equity_to_mysql.fetch_equity_df()

        # Insert equity list into MySQL table
        equity_to_mysql.insert_equity_list_to_db(equity_df)

        # Close the database connection
        equity_to_mysql.close_connection()

        # Log the job execution
        logging.info(f"Job executed at {datetime.now()}")
    

    except Exception as e:

        logging.error(f"Error because of  : {e}")
    
        
        
       

