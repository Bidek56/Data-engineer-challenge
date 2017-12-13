
"""
Created on Monday Dec 4th 2017
Purpose: Parse Friendly Vendor feed and match their data with our data
  Steps: Call HTML pages and parse them
         Call vendor JSON api and parse the output
         Merge the above data sources using Pandas
         Connect to MySql DB and download sample set
         Merge MySql data with vendor data
         Upload merged data to S3
         Copy meerged data from S3 to Redshift
"""

import os
import urllib.request
import json
from datetime import datetime
import requests
import logging

from   bs4 import BeautifulSoup
import pandas as pd
import pymysql

# Pandas options
pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# logging options
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

# create logger with
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

fh = logging.FileHandler('vendor_parser.log')
fh.setLevel(logging.INFO)
# add the handlers to the logger
logger.addHandler(fh)

# Utils class to handle database password
class Utils():
    @staticmethod
    def readMySQLpass():
        with open(os.path.expanduser('~/.mypass'),'r') as f:
            for line in f.read().splitlines():
                return line.split(':')

# Class to handle vendor data downloading and parsing
class Parser():

    @classmethod
    def api2Df(self, api_endpoint):
        json_obj = requests.get(api_endpoint).json()
        users = json_obj["users"]

        output_dataframe = pd.DataFrame(columns=['firstname', 'ID', 'last_active_date',
                                                'lastname', 'practice_location', 'specialty', 'user_type_classification'])
        for i in range(len(users)):
            list_to_append = []
            list_to_append.append( users[i]['firstname'])
            list_to_append.append( str(int(users[i]['id'])))
            list_to_append.append( users[i]['last_active_date'])
            list_to_append.append( users[i]['lastname'])
            list_to_append.append( users[i]['practice_location'])
            list_to_append.append( users[i]['specialty'])
            list_to_append.append( users[i]['user_type_classification'])
            output_dataframe.loc[i] = list_to_append

        # output_dataframe = pd.read_json(first_list["users"], orient='list')

        return output_dataframe

    @classmethod
    def line_parse(self, line_of_data):
        list_of_data = []
        list_of_soup = BeautifulSoup(line_of_data, "html.parser").find_all("td")
        for i in list_of_soup:
            list_of_data.append(i.string)
        return list_of_data

    # parse HTML table toDF
    @classmethod
    def htmlTable2Df(self, url):
        html = urllib.request.urlopen(url)
        input_soup = BeautifulSoup(html.read(), "html.parser")
        data = input_soup.find_all("table")
        header = str(data).split("<tr id=")[1]
        header_soup = BeautifulSoup(header, "html.parser")
        header_names = header_soup.find_all("th")
        header_list = []
        for h in header_names:
            column_name = h.string
            header_list.append(column_name)
        df = pd.DataFrame(columns=header_list)
        list_of_raw_data = str(data).split("<tr id=")[2:]
        for i in range(len(list_of_raw_data)):
            list_to_append = self.line_parse(list_of_raw_data[i])
            df.loc[i] = list_to_append
        return df

    # Read html and Json and merge it together
    @classmethod
    def merge(self, page):

        logger.info(f'Working on page: {page}')

        url = f"http://de-tech-challenge-api.herokuapp.com/user_activity?page={page}"
        html_df = self.htmlTable2Df(url)
        logger.debug(html_df.head(2))

        # Rename a column
        html_df = html_df.rename(columns={ 'Firstname': 'firstname', 'Lastname': 'lastname' })

        api_endpoint = f"http://de-tech-challenge-api.herokuapp.com/api/v1/users?page={page}"
        api_df = self.api2Df(api_endpoint)
        logger.debug(api_df.head(2))

        api_df = api_df.rename(columns={ 'last_active_date': 'vendor_last_active_date' })

        logger.debug(api_df.head(2))

        # Merge 2 DFs
        merged_df = html_df.merge(api_df, on=['ID', 'firstname', 'lastname'], how="inner" )

        merged_df = merged_df.rename(columns={ 'ID': 'Vendor_ID' })

        logger.info( merged_df.head(10) )

        return merged_df

class Combiner():

    total_db_rows = 0
    total_matched_rows = 0

    # Function to match vendor data with MySql data
    @classmethod
    def matcher(self, web_df, page ):
        logger.debug( f'web df shape: { web_df.shape }' )

        first_last_name = web_df['lastname'].min()
        last_last_name = web_df['lastname'].max()

        logger.debug( f'First: {first_last_name} Last: {last_last_name}' )

        db_df = self.retrieveUsers4Db( first_last_name, last_last_name )

        logger.debug( db_df.iloc[1:5,] )

        merged_df = web_df.merge(db_df, on=[ 'firstname', 'lastname', 'specialty' ], how="left")

        db_rows = len( db_df )

        self.total_db_rows = self.total_db_rows + db_rows

        # find number of rows matched
        matched_rows = len( merged_df[ merged_df.vendor_last_active_date.notnull() ] )

        # keep trac of mached totals
        self.total_matched_rows = self.total_matched_rows + matched_rows

        logger.debug( f"Total DB rows: {db_rows}" )
        logger.debug( f"Total matched rows: {matched_rows}" )

        logger.debug( merged_df.iloc[1:5,] )

        # write sample 10 rows
        if page == 1:
            sampleFileName = f"c:/temp/vendor_page{page}.json"

            merged_df.iloc[0:10,].to_json( sampleFileName, orient="records" )

        fileName = f"c:/temp/vendor_page{page}.txt"

        logger.debug( f'Writing to: {fileName}' )
        # merged_df.to_csv( fileName, sep='|', index=False, compression='gzip' )

        self.upload2S3( fileName )

        self.copy2Redshift( fileName )

        try:
            os.remove( fileName )
        except OSError:
            pass
        
        return True

    @classmethod
    def retrieveUsers4Db(self, start, end):

        host, port, user, password = Utils.readMySQLpass()

        conn = pymysql.connect(host=host, port=int(port), user=user, passwd=password )

        sql = f"select * from data_engineer.`user` where lastname between '{start}' and '{end}' order by lastname"
        
        sql_df = pd.read_sql( sql, conn )

        conn.close()

        return sql_df

    @classmethod
    def upload2S3( self, fileName ):
        logger.debug( f'Uploading {fileName} to S3' )
    
    @classmethod
    def copy2Redshift( self, fileName ):
        logger.debug( f'Coping {fileName} to Redshift' )

if __name__ == "__main__":

    startTime = datetime.now()

    parser = Parser()
    combo = Combiner()

    # Loop thru pages
    for page in range(1, 150, 1):
        merged_df = parser.merge( page )
        combo.matcher( merged_df, page )

    script_runtime = datetime.now() - startTime
    logger.info( f"Elapsed Time: {script_runtime}" )
    logger.info( f"Total matched rows: { combo.total_matched_rows }")
