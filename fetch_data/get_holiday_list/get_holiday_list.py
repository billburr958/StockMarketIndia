import mysql.connector
from datetime import datetime
import logging
import nselib
import os
import config
from logging.handlers import TimedRotatingFileHandler



class HolidayListToMySQL:


    def __init__(self, db_config):

        self.db = mysql.connector.connect(**db_config)
        self.cursor = self.db.cursor()
        self.table =config.table_name
        self.date_input_format = config.date_input_format
        self.date_output_format = config.date_output_format


    def create_holiday_list_table(self):

        # Create table if not exists
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS {self.table} (
        PRODUCT VARCHAR(255),
        TRADING_DATE VARCHAR(255) NOT NULL,
        WEEKDAY VARCHAR(255),
        DESCRIPTION  VARCHAR(255),
        SR_NO  INT PRIMARY KEY AUTO_INCREMENT
        )
        '''
        self.cursor.execute(create_table_query)
        self.db.commit()


    def convert_date_format(self, input_date_string):

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
        date_object = datetime.strptime(input_date_string, self.input_date_fomat)

        # Format as output format
        formatted_date = date_object.strftime(self.output_date_format)

        return formatted_date



    def fetch_holiday_df(self):

        """
       
         Function to fetch holiday list from NSE's website
        
        """
        
        holiday_list_df = nselib.trading_holiday_calendar()
        holiday_list_df = holiday_list_df.loc[:,['Product',  'tradingDate','weekDay','description']]
        return holiday_list_df
        
    def date_exists(self, date):

        """
        
         Function to check if the symbol exists in the table or not.
        
        """

        # Check if symbol exists in the table
        check_query = f'SELECT COUNT(*) FROM {self.table} WHERE TRADING_DATE = %s'
        self.cursor.execute(check_query, (date,))
        result = self.cursor.fetchone()[0]
        return result > 0


    def insert_holiday_list_to_db(self, equity_list):


        # Insert equity list into the table
        insert_query = f'INSERT IGNORE INTO {self.table} (PRODUCT, TRADING_DATE, WEEKDAY, DESCRIPTION) VALUES (%s, %s, %s, %s)'
        for product, trading_date, week_day, description in list(equity_list.itertuples(index=False, name=None)):
            if not self.date_exists(trading_date):
                self.cursor.execute(insert_query, (product, trading_date, week_day, description))
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
    log_handler = TimedRotatingFileHandler(filename='logs/holiday_list_scheduler.log', when='midnight', interval=1, backupCount=7)
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


        holiday_to_mysql = HolidayListToMySQL(db_config)
        holiday_to_mysql.create_holiday_list_table()

        # Fetch holiday list from NSE
        equity_df = holiday_to_mysql.fetch_holiday_df()

        # Insert holiday list into MySQL table
        holiday_to_mysql.insert_holiday_list_to_db(equity_df)

        # Close the database connection
        holiday_to_mysql.close_connection()

        # Log the job execution
        logging.info(f"Job executed at {datetime.now()}")
    

    except Exception as e:

        logging.error(f"Error because of  : {e}")

    