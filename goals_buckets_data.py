import mysql.connector
import pandas as pd
import json
from datetime import datetime, timedelta
import boto3
import re
import logging
import time
import warnings 
warnings.filterwarnings('ignore')
class goals_ETL:
    def __init__(self):
        self.access_key = "YOUR_ACCESS_KEY_VARIABLE"
        self.secret_key = "YOUR_SECRET_KEY_VARIABLE"
        # Specify the bucket name
        self.bucket_name = "dashboard-goals"
        self.goals = []
        self.names_of_files = []
        self.file_verf = None
        self.site_id = None
        self.date_str = None
        self.data_df = pd.DataFrame()
        self.failed_etl = []
        self.etl_log_goals = []
        self.max_goals_id = None
        self.max_batch_id = None
        self.schema = None
        
        # database credential
        self.cursor = None
        self.connection = None
        self.host="51.158.56.32"
        self.port=1564
        self.user="Site"
        self.password="515B]_nP0;<|=pJOh35I"
    
    def s3_client(self):
        print('Goals ETL')
        # Create an S3 client with the specified endpoint URL
        bucket_name = "dashboard-goals"
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url="https://s3.nl-ams.scw.cloud",
        )
        
        # List all objects in the bucket
        response = s3.list_objects_v2(Bucket=bucket_name)
        for index, obj in enumerate(response.get("Contents", [])):
            file_key = obj["Key"]
            local_file_name = f"file{index}.json"

            file_name = str(file_key)
            self.names_of_files.append(file_name)
        # Assume df is your DataFrame containing the data
        try:
            if len(self.names_of_files) != 0:

                df = pd.DataFrame({
                    "Filename": self.names_of_files
                })

                # Extract the Site ID and Date from the Filename
                df[["Site ID", "Date"]] = df["Filename"].str.extract(r"site-(\d+)-user-\d+-goals-(\d{4}-\d{2}-\d{2})T")

                # Convert the Date column to datetime type
                df["Date"] = pd.to_datetime(df["Date"])

                # Sort the DataFrame by Site ID, Date, and Time
                df_sorted = df.sort_values(by=["Site ID", "Date", "Filename"], ascending=[True, False, True])


                # Find the index of the maximum row for each Site ID
                max_index = df_sorted.groupby("Site ID")["Date"].idxmax()

                # Get the corresponding file names for the maximum rows
                max_files = df_sorted.loc[max_index, "Filename"].tolist()
                for i in max_files:
                    self.truncate()
                    file_key = i
                     # Extract the Site ID  
                    self.site_id = re.search(r"site-(\d+)", file_key).group(1)
                    print('Site id: ',self.site_id)
                    # Extract the date
                    self.date_str = re.search(r"(\d{4}-\d{2}-\d{2})", file_key).group(1)

                    # Read the file contents
                    try:
                        response = s3.get_object(Bucket=bucket_name, Key=file_key)
                        file_content = response["Body"].read().decode("utf-8")
                        json_data = json.loads(file_content)
                        # Get the current year
                        current_year = datetime.now().year

                        # Create a list of dates from January 1st to December 31st
                        start_date = datetime(current_year, 1, 1)
                        end_date = datetime(current_year, 12, 31)
                        date_range = pd.date_range(start=start_date, end=end_date)

                        # Create a DataFrame with the dates
                        df = pd.DataFrame({'Date': date_range})

                        # Optional: Extract additional columns (Week, Month, Quarter) from the Date column
                        df['Week'] = df['Date'].dt.week
                        df['Month'] = df['Date'].dt.month
                        df['Quarter'] = df['Date'].dt.quarter
                        df['CTA_per_day'] = ''
                        df['Pageviews_per_day'] = ''
                        df['Visits_per_day'] = ''
                        df['Min_pageviews'] = json_data['q41']
                        df['Min_CTA'] = json_data['q51']
                        df['Site_ID'] = self.site_id
                        df['batch_id'] = self.max_batch_id

                        quarter_mapping = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
                        df['Quarter'] = df['Date'].dt.quarter.map(quarter_mapping)

                        # Assign the values to the DataFrame
                        df.loc[df['Quarter'] == 'Q1', 'CTA_per_day'] = json_data['q11']
                        df.loc[df['Quarter'] == 'Q1', 'Pageviews_per_day'] = json_data['q21']
                        df.loc[df['Quarter'] == 'Q1', 'Visits_per_day'] = json_data['q31']

                        df.loc[df['Quarter'] == 'Q2', 'CTA_per_day'] = json_data['q12']
                        df.loc[df['Quarter'] == 'Q2', 'Pageviews_per_day'] = json_data['q22']
                        df.loc[df['Quarter'] == 'Q2', 'Visits_per_day'] = json_data['q32']

                        df.loc[df['Quarter'] == 'Q3', 'CTA_per_day'] = json_data['q13']
                        df.loc[df['Quarter'] == 'Q3', 'Pageviews_per_day'] = json_data['q23']
                        df.loc[df['Quarter'] == 'Q3', 'Visits_per_day'] = json_data['q33']

                        df.loc[df['Quarter'] == 'Q4', 'CTA_per_day'] = json_data['q14']
                        df.loc[df['Quarter'] == 'Q4', 'Pageviews_per_day'] = json_data['q24']
                        df.loc[df['Quarter'] == 'Q4', 'Visits_per_day'] = json_data['q34']
                        
                        df = df[df['Date'] >= self.date_str]
                        total = df.shape[0]
                        count = df.shape[0]
                        failed = 0
                        table = 'goals'


                        # Insert the data into the table
                        
                        self.connection_db()
                        query = """
                        INSERT INTO pre_stage.stage (Date, Week, Month, Quarter, CTA_per_day, Pageviews_per_day, Visits_per_day,
                                                     Min_pageviews, Min_CTA, Site_ID, batch_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        # Prepare the data as a list of tuples
                        f_data = [tuple(row) for _, row in df.iterrows()]
                        # Execute the query using executemany
                        self.cursor.executemany(query, f_data)
                        self.connection.commit()
                        print('data insertion is complete pre_stage')
                        count = len(f_data)
                        
                        self.etl_log_goals.append([self.site_id, total,count, failed, table,self.date_str, self.max_batch_id])
                        
                        query_stage = f"""
                        INSERT INTO stage.goals (Date, Week, Month, Quarter, CTA_per_day, Pageviews_per_day, Visits_per_day, Min_pageviews, Min_CTA, Site_ID, batch_id)
                        SELECT Date, Week, Month, Quarter, CTA_per_day, Pageviews_per_day, Visits_per_day, Min_pageviews, Min_CTA, Site_ID, batch_id
                        FROM pre_stage.goals;
                        """
                        # Execute the pages query1
                        self.cursor.execute(query_stage)
                        # Commit the changes to the database
                        self.connection.commit()
                        print("Complete insertion in stage")
                        site_id_value = self.site_id
                        date_value = self.date_str
                        print('Delete from production previous data')
                        query_del = f"DELETE FROM prod.goals WHERE site_id = {site_id_value} AND date >= '{date_value}'"
                        print(query_del)
                        # Assuming you have the values for siteid_ and st_date
                        # Execute the query
                        self.cursor.execute(query_del)
                        self.connection.commit()
                        
                        # sql query for aggregate of table
                        query_prod = f"""
                        INSERT INTO prod.goals (Date, Week, Month, Quarter, CTA_per_day, Pageviews_per_day, Visits_per_day, Min_pageviews, Min_CTA, Site_ID, batch_id)
                        SELECT Date, Week, Month, Quarter, CTA_per_day, Pageviews_per_day, Visits_per_day, Min_pageviews, Min_CTA, Site_ID, batch_id
                        FROM stage.goals;
                        """
                        # Execute the pages query1
                        self.cursor.execute(query_prod)
                        # Commit the changes to the database
                        self.connection.commit()
                        self.cursor.close()
                        self.connection.close()
                        print("Complete insertion in prod")
                        
                    except Exception as e:
                        print("Error:", str(e))
                        break
            print('Completed!!!!!!!!!')
                        
        except Exception as e:
            print("No new file is available on s3 --> (Error) :", str(e))
            
    #  this function create the connection of database
    def connection_db(self):
        try:
            # Connect to the MySQL database
            self.connection = mysql.connector.connect(
                host = self.host,
                port = self.port,
                user = self.user,
                password = self.password
            )
            self.cursor = self.connection.cursor()

            print(' Database Connection successfull')
            
        except mysql.connector.Error as error:
            print("Database Connection Error:", error)
            
            
    def start_execution(self):
        
#         self.connection_db()
#         self.truncate()
#         self.connection_db()
        self.batch_insert()
        self.s3_client()
        
        if (len(self.names_of_files) > 0):
            self.buckets_file_archive()
#         self.etl_log_insert()
#         self.insert_data_into_stage()

#         self.insert_data_into_prod()
    
    # its create and add the batch details 
    def batch_insert(self):
        print('batch_insert')
        try:
            self.connection_db()
            query = "INSERT INTO stage.batch_name (Type, status) VALUES (%s, %s)"
            values = ('Goals', 'initialize')
            self.cursor.execute(query, values)
            self.connection.commit()
            
            # Define the SQL query
            query = "SELECT MAX(id) AS max_id FROM stage.batch_name;"

            # Execute the query
            self.cursor.execute(query)

            # Fetch the result
            result = self.cursor.fetchone()

            # Access the maximum id value
            self.max_batch_id = result[0]
            self.cursor.close()
            self.connection.close()

        except mysql.connector.Error as error:
            print("Error inserting data into MySQL database:", error)
            self.cursor.close()
            self.connection.close()

    # goals_etl_log data
    def etl_log_insert(self):
        print('etl_log_insert')
        print('start...',self.etl_log_goals)
        for row in self.etl_log_goals:
            query = "INSERT INTO stage.goals_etl_log (siteid, total_records, inserted, fail_record, table_name, file_date, batch_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            self.cursor.execute(query, values)
            self.connection.commit()
        print("Data inserted successfully into MySQL database.")
        query = "SELECT MAX(id) AS max_id FROM stage.goals_etl_log;"

        # Execute the query
        self.cursor.execute(query)

        # Fetch the result
        result = self.cursor.fetchone()

        # Access the maximum id value
        self.max_goals_id = result[0]
    def truncate(self):
        print('start')
        self.connection_db()
        query1 = """
        TRUNCATE TABLE pre_stage.goals;
        TRUNCATE TABLE stage.goals;
        """
#         TRUNCATE TABLE stage.goals;
        self.cursor.execute(query1)
        time.sleep(3)
                # close the database connection  
        self.cursor.close()
        self.connection.close()
        print('Pre_stage and stage table truncated!!')

    def buckets_file_archive(self):
        # Specify the source and destination bucket names
        source_bucket_name = "dashboard-goals"
        destination_bucket_name = "archive-dashboard-goals"

        # Create an S3 client with the specified endpoint URL
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url="https://s3.nl-ams.scw.cloud",
        )

        # Create the new bucket
#         s3.create_bucket(Bucket=destination_bucket_name)

        # List objects in the source bucket
        response = s3.list_objects_v2(Bucket=source_bucket_name)

        # Move objects from the source bucket to the destination bucket
        for obj in response['Contents']:
            # Get the object key
            object_key = obj['Key']

            # Copy the object to the destination bucket
            s3.copy_object(
                CopySource={'Bucket': source_bucket_name, 'Key': object_key},
                Bucket=destination_bucket_name,
                Key=object_key
            )

            # Delete the object from the source bucket
            s3.delete_object(Bucket=source_bucket_name, Key=object_key)

        print("Files moved to the new bucket successfully!")
def dev_etl():
    etl = goals_ETL()
    etl.start_execution()
