from get_equity_data import StockDataToDatabase
import os
import sys
from datetime import datetime, timedelta
sys.path.append('../config')
sys.path.append('../get_equity_list')
import config
import mysql.connector
from tqdm import tqdm
import logging
from logging.handlers import TimedRotatingFileHandler



class EquityListFetcher:



    def __init__(self, config):
        self.connection_params = {
            'host': config.host,
            'user': config.user,
            'password': config.password,
            'database': config.database
        }
        self.table_name = config.equity_list_table_name
        self.column_name = config.symbol_column_name


    def fetch_equity_list(self):

        try:
            # Connect to the database
            connection = mysql.connector.connect(**self.connection_params)
            cursor = connection.cursor()

            # Execute SQL query to get unique values from the specified column
            query = f"SELECT DISTINCT {self.column_name} FROM {self.table_name}"
            cursor.execute(query)

            # Fetch all unique values
            unique_values = [value[0] for value in cursor.fetchall()]

            return unique_values

        except mysql.connector.Error as err:
            return None

        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()



if __name__=='__main__':

    if not os.path.exists('logs'):
            os.makedirs('logs')

    # Set up logging configuration with TimedRotatingFileHandler
    logger = logging.getLogger("my_logger")
    logger.setLevel(logging.DEBUG)  # Set the logging level

    # Configure the formatter
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    # Create a TimedRotatingFileHandler
    file_handler = TimedRotatingFileHandler("logs/delivery_volume_fetcher_scheduler.log", when="midnight", interval=1, backupCount=5)
    file_handler.setLevel(logging.DEBUG)  # Set the logging level for the handler
    file_handler.setFormatter(log_formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    try:

        equity_list_fetcher = EquityListFetcher(config=config)
        symbol_values = equity_list_fetcher.fetch_equity_list()


        stock_data_manager = StockDataToDatabase(config=config)
            
        # Get the current date
        current_date = datetime.now()

        # Calculate the previous date by subtracting one day
        previous_date = current_date - timedelta(days=20)

        formatted_previous_date = previous_date.strftime('%d-%m-%Y')
        formatted_current_date = current_date.strftime('%d-%m-%Y')

        logger.info(f"Job execution started at : {datetime.now()}")
        for symbol in tqdm(symbol_values):

            try:

                stock_data_manager.create_or_append_to_table(symbol, formatted_previous_date, formatted_current_date)
                csvs = [file for file in os.listdir('.') if file.lower().endswith('.csv')]
                for csv in csvs:
                    os.remove(csv)
                    
            except Exception as e:
                logger.error(f"Error for stock {symbol} because of  : {e}",exc_info=True)

        csvs = [file for file in os.listdir('.') if file.lower().endswith('.csv')]
        logger.info(f"Job executed at : {datetime.now()}")

    except Exception as e:
        logger.error(f"Error because of : {e}",exc_info=True)


