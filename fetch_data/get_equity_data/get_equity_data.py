from nselib import capital_market
import pandas as pd
import mysql.connector
from tqdm import tqdm
import sys
sys.path.append('../config')
import config
from datetime import datetime, timedelta



class StockDataToDatabase:
    def __init__(self, config):
        
        
        self.connection_params = {
            'host': config.host,
            'user': config.user,
            'password': config.password,
            'database': config.database
        }
        
        self.table_name = config.delivery_daily_table_name
        self.date_format = config.date_output_format
        self.connection = mysql.connector.connect(**self.connection_params)
        self.cursor = self.connection.cursor()

    def clean_column_names(self, df):
        
        # Remove special characters from column names
        
        df.columns = ["".join(c if c.isalnum() else "_" for c in str(col)) for col in df.columns]
        return df

    def fetch_data(self, symbol, start_date, end_date):
        
        # Fetch data using the provided library
        
        df = capital_market.price_volume_and_deliverable_position_data(symbol, start_date, end_date)
        return self.clean_column_names(df)

    def create_or_append_to_table(self, symbol, start_date, end_date):
        
        # Fetch data from the library
        
        df = self.fetch_data(symbol, start_date, end_date)
        
        # Format the date in 'dd-mm-yyyy' format
        df['Date']= df['Date'].apply(lambda x : datetime.strptime(x, "%d-%b-%Y").strftime(self.date_format))

        # Check if the table exists
        self.cursor.execute("SHOW TABLES LIKE %s", (self.table_name,))
        table_exists = bool(self.cursor.fetchone())

        if not table_exists:
            
            # If the table does not exist, create it
            self.create_table(df)
        else:
        
            # If the table exists, fetch existing data
            existing_data = pd.read_sql(f"SELECT * FROM {self.table_name} WHERE Symbol = '{symbol}'", self.connection)

            # Filter new data to append (based on Date column)
            if existing_data.shape[0]!=0:
                new_data = df[~df['Date'].isin(existing_data['Date'])]
                print ('cond 1')
            
            else:
                new_data = df
                print ('cond2')


            # Append new data to the existing table row by row
            for index, row in new_data.iterrows():
                self.insert_row(row)

    def create_table(self, df):
        
        # Create a table with cleaned column names and their data types for MySQL
        columns_and_types = ', '.join([f"`{col}` VARCHAR(255)" for col in df.columns])
        create_table_query = f"CREATE TABLE {self.table_name} ({columns_and_types});"
        self.cursor.execute(create_table_query)

        # Commit changes
        self.connection.commit()

    def insert_row(self, row):
        
        # Insert a row into the table
        columns = ', '.join([f"`{col}`" for col in row.index])
        values = ', '.join([f"'{str(val)}'" for val in row.values])
        insert_query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values});"
        self.cursor.execute(insert_query)

        # Commit changes
        self.connection.commit()

    def close_connection(self):
        
        # Close cursor and connection
        self.cursor.close()
        self.connection.close()



if __name__=='__main__':
    
    stock_data_manager = StockDataToDatabase(config=config)
    
    # Get the current date
    current_date = datetime.now()

    # Calculate the previous date by subtracting one day
    previous_date = current_date - timedelta(days=20)

    formatted_previous_date = previous_date.strftime('%d-%m-%Y')
    formatted_current_date = current_date.strftime('%d-%m-%Y')

    stock_data_manager.create_or_append_to_table('TCS', formatted_previous_date, formatted_current_date)






 
