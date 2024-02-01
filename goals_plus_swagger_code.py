import mysql.connector
import pandas as pd
import json
from datetime import datetime, timedelta
import boto3
import re
import logging
import time
import os
import yaml
import requests
import time
import subprocess
import traceback
import ssl
import smtplib
import mail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import warnings 
warnings.filterwarnings('ignore')
class goals_ETL:
    def __init__(self):
        self.access_key = "SCWSHP5FCYZTXQ55XHBH"
        self.secret_key = "f943ad23-f89c-4bea-ae91-6bc5cb716c4d"
        # Specify the bucket name
        self.bucket_name = " "
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

                file_name_list = df_sorted['Filename'].tolist()
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

                        site_id_value = self.site_id
                        date_value = self.date_str
                        # Insert the data into the table
                        
                        self.connection_db()
                        query = """
                        INSERT INTO stage.goals(Date,Week,Month,Quarter,CTA_per_day,Pageviews_per_day,Visits_per_day,Min_pageviews,Min_CTA,Site_ID,batch_id)
                        select Date,Week,Month,Quarter,CTA_per_day,Pageviews_per_day,Visits_per_day,Min_pageviews,Min_CTA,Site_ID,batch_id from prod.goals;
                        """
                        self.cursor.execute(query)
                        self.connection.commit()
                        
                        current_year  = datetime.now().year
                        year_c = str(current_year)
                        print('start')
                        #  q1
                        #print(query_up)
                        d1 = year_c + '-01-01'
                        d2 = year_c + '-03-31'
                        # Assign the values to the DataFrame
                        try: 
                            if int(json_data['q11']) >= 0:
                                va = json_data['q11']
                                query = f"UPDATE prod.goals SET CTA_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q21']) >= 0:
                                va = json_data['q21']
                                query = f"UPDATE prod.goals SET Pageviews_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q31']) >= 0:

                                va = json_data['q31']
                                query = f"UPDATE prod.goals SET Visits_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        #  q2
                        d1 = year_c + '-04-01'
                        d2 = year_c + '-06-30'
                        try:
                            if int(json_data['q12']) >= 0:
                                va = json_data['q12']
                                query = f"UPDATE prod.goals SET CTA_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q22']) >= 0:
                                va = json_data['q22']
                                query = f"UPDATE prod.goals SET Pageviews_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q32']) >= 0:  
                                # df.loc[df['Quarter'] == 'Q2', 'Visits_per_day'] = json_data['q32']
                                va = json_data['q32']
                                query = f"UPDATE prod.goals SET Visits_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                            
                        #  q3
                        d1 = year_c + '-07-01'
                        d2 = year_c + '-09-30'
                        try:
                            if int(json_data['q13']) >= 0: 
                                # df.loc[df['Quarter'] == 'Q3', 'CTA_per_day'] = json_data['q13']
                                va = json_data['q13']
                                query = f"UPDATE prod.goals SET CTA_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q23']) >= 0: 
                                # df.loc[df['Quarter'] == 'Q3', 'Pageviews_per_day'] = json_data['q23']
                                va = json_data['q23']
                                query = f"UPDATE prod.goals SET Pageviews_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q33']) >= 0:  
                                # df.loc[df['Quarter'] == 'Q3', 'Visits_per_day'] = json_data['q33']
                                va = json_data['q33']
                                query = f"UPDATE prod.goals SET Visits_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        #  q4
                        d1 = year_c + '-10-01'
                        d2 = year_c + '-12-31'
                        try:
                            if int(json_data['q14']) >= 0:
                                # df.loc[df['Quarter'] == 'Q4', 'CTA_per_day'] = json_data['q14']
                                va = json_data['q14']
                                query = f"UPDATE prod.goals SET CTA_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q24']) >= 0: 
                                # df.loc[df['Quarter'] == 'Q4', 'Pageviews_per_day'] = json_data['q24']
                                va = json_data['q24']
                                query = f"UPDATE prod.goals SET Pageviews_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q34']) >= 0:
                                # df.loc[df['Quarter'] == 'Q4', 'Visits_per_day'] = json_data['q34']
                                va = json_data['q34']
                                query = f"UPDATE prod.goals SET Visits_per_day = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        
                        d1 = year_c + '-01-01'
                        d2 = year_c + '-12-31'
                        try:
                            if int(json_data['q41']) >= 0:
                                va = json_data['q41']
                                query = f"UPDATE prod.goals SET Min_pageviews = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        try:
                            if int(json_data['q51']) >= 0:
                                va = json_data['q51']
                                query = f"UPDATE prod.goals SET Min_CTA = '{va}' WHERE site_id = {site_id_value} AND date >= '{d1}' AND date <= '{d2}';"
                                self.cursor.execute(query)
                                self.connection.commit()
                        except Exception as e:
                            print("Error:", str(e))
                        self.cursor.close()
                        self.connection.close()
                        print('ending')
                        site_id_value = int(site_id_value)
                        # data post
                        
                        
                        res = self.start_execution_data_post(site_id_value)
                       
                        print("Complete insertion in prod")
                    except Exception as e:
                        print("Error:", str(e))
                self.buckets_file_archive(file_name_list)
            print('Completed!!!!!!!!!')
            return res      
        except Exception as e:
            self.connection.close()
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
            
            
    def start_execution_goals(self):
        

        self.batch_insert()
        res = self.s3_client()
        return res
            
#         self.etl_log_insert()
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


    def truncate(self):
        print('start')
        try:
            self.connection_db()
            query1 = """
            TRUNCATE TABLE stage.goals;
            """
            self.cursor.execute(query1)
            time.sleep(3)
                    # close the database connection  
            self.cursor.close()
            self.connection.close()
            print('Pre_stage and stage table truncated!!')
        except mysql.connector.Error as error:
            print(error)
            self.cursor.close()
            self.connection.close()

    def buckets_file_archive(self,file_name_list):
        try:
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
            # s3.create_bucket(Bucket=destination_bucket_name)

            # List objects in the source bucket
            response = s3.list_objects_v2(Bucket=source_bucket_name)

            # Move objects from the source bucket to the destination bucket
            for obj in response['Contents']:
                # Get the object key
                object_key = obj['Key']

                if object_key in file_name_list:
                    # Copy the object to the destination bucket
                    s3.copy_object(
                        CopySource={'Bucket': source_bucket_name, 'Key': object_key},
                        Bucket=destination_bucket_name,
                        Key=object_key
                    )

                    # Delete the object from the source bucket
                    s3.delete_object(Bucket=source_bucket_name, Key=object_key)

            print("Files moved to the new bucket successfully!")
        except mysql.connector.Error as error:
            print("File not move to archive bucket:", error)



    def post(self, token,url,json_data_list):
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            print('len: ',len(json_data_list))
            c = 0
            for json_data in json_data_list:
                response = requests.post(url, headers=headers, json=json_data)
                if response.status_code == 200:
                    c=c+1
                    print('POST request successful.',c)
                else:
                    print(f'Error: {response.status_code} - {response.text}')
        except Exception as e:
            print(e)
            
    def compare_json_format(self, json1, json2):
        try:
            json2 = json.dumps(json2)
            # Load JSON strings into Python dictionaries
            data1 = json.loads(json1)
            data2 = json.loads(json2)

            # Extract keys from the dictionaries
            keys1 = set(data1.keys())
            keys2 = set(data2.keys())

            # Compare the keys
            if keys1 == keys2:
                logging.info("=====================The JSON formats have the same keys========================")
                return True
            else:
                logging.info("The JSON formats have different keys.")
                return False
        except Exception as e:
            print(e)
            return False
    
    def start_execution_data_post(self, site__id):
        # Select the  values of the credentials column
        try:
                # Select the  values of the credentials column
            self.connection_db()
            select_query = f"select * from prod.credentials_detail where status ='active' and siteid= {site__id};"
            self.cursor.execute(select_query)
            cred = self.cursor.fetchall()
            print(cred)
            self.cursor.close()
            self.connection.close()
            res = self.yaml_DQ(site__id)
        
        except Exception as e:
            print("Error :", e)
            self.cursor.execute(select_query)
            cred = self.cursor.fetchall()
            print(cred)
            self.cursor.close()
            self.connection.close()
            res = self.yaml_DQ(site__id)
            
        return res
    
    def yaml_DQ(self,siteid):
        try:
            # ---------------------YAML FILE READ---------------------
            yaml_file_path = r'Data_Quality.yaml'
            try:
                with open(yaml_file_path, 'r') as file:
                    config = yaml.safe_load(file)
                    results = "YAML file opened successfully."
                    print(results)
            except FileNotFoundError:
                results = "YAML file not found."
                print(results)
                config = None
            except Exception as e:
                results = "Error opening YAML file"
                print("Error opening YAML file:", e)
                config = None

            # -----------------Data Quality start ---------------------
            new_where_clause = str(siteid)
            if config is not None:
                dq_checks = config.get('dq_checks')

                if dq_checks and isinstance(dq_checks, list):
                    count = 0
                    for dq_check in dq_checks:

                        check_number = ''.join(filter(str.isdigit, dq_check.get('name')))
                        if not check_number or int(check_number) == siteid:
                            # get the name, query in yaml
                            name = dq_check.get('name')
                            sql_query = dq_check.get('sql_query')
                            # replace the where cluse
                            sql_query = sql_query.replace("siteid_value", new_where_clause)
                            stop_on_failure = dq_check.get('stop_on_failure', False)
                            target = 0  # Initialize target value
                            if sql_query:
                                try:
                                    try:  
                                        self.connection_db()
                                        self.cursor.execute(sql_query)
                                        print(sql_query)
                                        result = self.cursor.fetchone()
                                        print(result)
                                        self.cursor.close()
                                        self.connection.close()
                                        result_value =0
                                    except Exception as e:
                                        print(e)
                                        self.connection_db()
                                        self.cursor.execute(sql_query)
                                        print(sql_query)
                                        result = self.cursor.fetchone()
                                        self.cursor.close()
                                        self.connection.close()

                                    if result is not None and result[0] is not None and result[0] > 0:
                                        print(f"Query result for '{name}': {result[0]}")
                                        result_value = True if result[0] > 0 and stop_on_failure else False
                                        try:
                                            count =1

                                            swagger = "Swagger Data Post"  # Modify this with your actual Swagger link
                                            # Insert the result into the prod.dq_checks table
                                            subject = f'[ISSUE][DQ_failed][DQ_name : {name}][Siteid : {siteid}] Data Quality'
                                            message = f"Hello ! "
                                            message = str(message)
                                            email_template = f"""
                                                <!DOCTYPE html>
                                                <html>
                                                <head>
                                                    <style>
                                                        .failed-list {{
                                                            border-collapse: collapse;
                                                            width: 100%;
                                                            border: 1px solid black;
                                                        }}

                                                        .failed-list th, .failed-list td {{
                                                            border: 1px solid black;
                                                            padding: 8px;
                                                            text-align: left;
                                                        }}

                                                        .failed-list th {{
                                                            background-color: #f2f2f2;
                                                        }}
                                                    </style>
                                                </head>
                                                <body>
                                                    <div class="email-container">
                                                        <div class="header">Data Quality Check Report: Failed Entries</div>

                                                        <div class="details">
                                                            Dear Team,
                                                            <p>We are writing to inform you about recent data quality check failures that require your attention. Our data quality monitoring process has identified issues in the following entries: An important update the Data Quality check {name} for Site ID {siteid} has failed. Immediate action is needed to resolve this issue.</p>
                                                        </div>

                                                        <table class="failed-list">
                                                            <tr>
                                                                <th>siteId</th>
                                                                <th>name</th>
                                                                <th>value</th>
                                                                <th>target</th>
                                                                <th>result</th>
                                                                <th>Stop on failure</th>
                                                                <th>source</th>

                                                            </tr>
                                                            <tr>
                                                                <td><strong>{siteid}</strong></td>
                                                                <td>{name}</td>
                                                                <td>{result[0]}</td>
                                                                <td>{target}</td>
                                                                <td>{result_value}</td>
                                                                <td>{stop_on_failure}</td>
                                                                <td>{swagger}</td>

                                                            </tr>

                                                            <!-- Add more failed entries as needed -->
                                                        </table>
                                                        <br>

                                                        <hr>
                                                        <p><strong><b>Server Resources</b></strong></p>
                                                        <table class="failed-list">
                                                           

                                                            <!-- Add more failed entries as needed -->
                                                        </table>

                                                        <div class="details">
                                                            <p>We kindly request that you review and address these issues promptly to ensure the accuracy and reliability of our data. If you have any questions or need further assistance, please don't hesitate to reach out to our support team.</p>

                                                            <p>Thank you for your attention to this matter.</p>

                                                            <p>Sincerely,
                                                            <br>[Gulraiz]
                                                            <br>[Dotlabs]
                                                            <br>[https://sindri.media/]</p>
                                                        </div>
                                                    </div>
                                                </body>
                                                </html>
                                                """
                                            insert_query = """
                                            INSERT INTO prod.dq_checks_status (value, name, stop_on_failure, target, result, source, siteid) 
                                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                                            """
                                            values = (result[0], name, stop_on_failure, target, result_value, swagger,siteid)
                                            try:
                                                self.connection_db()
                                                self.cursor.execute(insert_query, values)
                                                self.connection.commit()
                                                self.connection.close()
                                                try:
                                                    mail.mailConnection(subject, message,email_template) 
                                                except Exception as e:
                                                    mail.mailConnection(subject, message,email_template)
                                                    print(e)

                                            except Exception as e:
                                                print(e)
                                                self.connection_db()
                                                self.cursor.execute(insert_query, values)
                                                self.connection.commit()
                                                self.connection.close()
                                                try:
                                                    mail.mailConnection(subject, message,email_template) 
                                                except Exception as e:
                                                    mail.mailConnection(subject, message,email_template)
                                                    print(e)
                                            if stop_on_failure:
                                                print(f"Aborting further checks due to failure in '{name}'.")
                                                d = "DQ is fail"
                                                return d
                                        except Exception as e:
                                            print(e)
                                            # Stop checking other jobs
                                except Exception as e:
                                    print(e)
                if count == 0:               
                    insert_query = """
                    INSERT INTO prod.dq_checks_status (value, name, stop_on_failure, target, result, source, siteid) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    values = (0, 0, 0, 0, 0,  "Swagger Data DQ complete",siteid)
                    try:

                        self.connection_db()
                        self.cursor.execute(insert_query, values)
                        self.connection.commit()
                        self.connection.close()

                    except Exception as e:
                        print(e)
                        self.connection_db()
                        self.cursor.execute(insert_query, values)
                        self.connection.commit()
                        self.connection.close()
                    self.start_endpoint_to_post(siteid)
                    results = 'Completed'
                    return results
            else:
                results = "yaml file is not opened"
                print('yaml file is not opened')
                return results
            
        except Exception as e:
            print(e)
    
    def start_endpoint_to_post(self,siteid):
        try:

            print('start_data')
            subject = f"[COMPLETED]Data Posted Completed via Swagger: Site ID {siteid}"
            message = f"HY"
            message = str(message)

            # ---------------------ENDPOINT and Function name get-------------------

            select_query = f"""call prod.SelectINputBasedOnSiteId({siteid});"""
            #  database connection handle
            try:
                # Select the  values of the credentials column
                self.connection_db()
                self.cursor.execute(select_query)
                endp = self.cursor.fetchall()
            except mysql.connector.errors.OperationalError as op_err:
                if "is locked" in str(op_err):
                    print("Database is busy. Please try again later.")
                    self.connection_db()
                    self.cursor.execute(select_query)
                    endp = self.cursor.fetchall()
                elif "Can't connect" in str(op_err):
                    print("Unable to connect to the database.")
                    self.connection_db()
                    self.cursor.execute(select_query)
                    endp = self.cursor.fetchall()
                else:
                    print("An unexpected operational error occurred:", op_err)
                    self.connection_db()
                    self.cursor.execute(select_query)
                    endp = self.cursor.fetchall()
            except mysql.connector.errors.IntegrityError as int_err:
                print("Integrity error occurred:", int_err)
            except mysql.connector.errors.ProgrammingError as prog_err:
                print("Programming error occurred:", prog_err)
            except mysql.connector.errors.InterfaceError as intf_err:
                print("Interface error occurred:", intf_err)
            except Exception as e:
                print(e)
                self.connection_db()
                self.cursor.execute(select_query)
                endp = self.cursor.fetchall()


            self.connection.close()
            self.cursor.close()
            print('siteid : ',siteid)
            try:
                url = 'https://sindri.media/strapi/api/auth/local'
                payload = {
                    'identifier': 'import@dashboardapp.com',
                    'password': 'JSyzkFtR9Cw5v5t'
                }
                auth_response = requests.post(url, data=payload)

                # Assuming the JWT token is returned in the 'token' field of the response JSON
                token = auth_response.json().get('jwt')
                print('Token:', token)

            except requests.exceptions.RequestException as req_err:
                print("Request error occurred:", req_err)
                auth_response = requests.post(url, data=payload)
                token = auth_response.json().get('jwt')
            except Exception as ex:
                print("An unexpected exception occurred:", ex)
                auth_response = requests.post(url, data=payload)
                token = auth_response.json().get('jwt')


                # ------------------- Query starting-----------
            for i in endp:
                try:
                    json_data = 0
                    json_data_list = []
                    # print('start')
                    req_json_format = i[3]
                    parameter = str(i[2])
                    print('\n',i[0])
                    label_val = None
                    hint_val = None
                    event_action_val = None
                    series = None
                    category = None
                    ty = False
                    # -------------- label and hints get in the json input column---------
                    if i[7] is not None and len(i[7]) > 0:
                        data_dict = json.loads(i[7])
                        word_to_find = "table"
                        # Check if the word is present in the string
                        if i[1].lower().find(word_to_find.lower()) != -1:
                            if 'label' in data_dict:
                                label_val = data_dict["label"]
                            ty = True
                        else:
                            if 'label' in data_dict:
                                label_val = data_dict["label"]
                            if 'hint' in data_dict:
                                hint_val = data_dict["hint"]


                    # -------------- query specific input ---------
                    if i[5] is not None and len(i[5]) > 0:
                        data_dict = json.loads(i[5])
                        for key, val in data_dict.items():
                            if key == "event_action":
                                event_action_val = val
                            elif key == "series":
                                series = val
                                series_val1 = series[0]
                                series_val2 = series[1]
                            elif key == "category":
                                category = val
                            else:
                                print('No find')
                    # ---------------query , SP , Parameter , input
                    if (event_action_val is not None) and (series is not None):
                        query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' , '{event_action_val}', '{series_val1}' , '{series_val2}');"
                    # Check if at least one of the variables is not None
                    #     elif event_action is not None or series is not None:
                        # At least one variable is not None

                        # Now, you can check which variables are not None and perform your desired action for each
                    elif event_action_val is not None:
                        if (label_val is not None) and (hint_val is not None):
                            query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' , '{event_action_val}' );"
                        elif ty == True:
                            query = f"call prod.{i[1]}({siteid},  '{event_action_val}', '{label_val}' );"
                        else:
                            query = f"call prod.{i[1]}({siteid},  '{event_action_val}' );"
                    elif series is not None:

                        query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' ,  '{series_val1}' , '{series_val2}');"

                    elif category is not None: # categories query
                        query = f"call prod.{i[1]}({siteid}, '''{label_val}''' ,  '''{hint_val}''' , '{category}' );"
                    elif ty == True:  # table queries
                        query = f"call prod.{i[1]}({siteid}, '{label_val}'  );"
                    elif (label_val is not None) and (hint_val is not None):
                        query = f"call prod.{i[1]}({siteid}, '{label_val}' ,  '{hint_val}' );"
                    else:
                        query = f"call prod.{i[1]}({siteid});"
                    print(query)
                    try:
                        self.connection_db()
                        self.cursor.execute(query)
                        # Retry parameters

                    except mysql.connector.errors.OperationalError as op_err:
                        if "is locked" in str(op_err):
                            print("Database is busy. Please try again later.")
                            self.connection_db()
                            self.cursor.execute(query)
                        elif "Can't connect" in str(op_err):
                            print("Unable to connect to the database.")
                            self.connection_db()
                            self.cursor.execute(query)

                        else:
                            print("An unexpected operational error occurred:", op_err)
                            self.connection_db()
                            self.cursor.execute(query)

                    except mysql.connector.errors.IntegrityError as int_err:
                        print("Integrity error occurred:", int_err)
                    except mysql.connector.errors.ProgrammingError as prog_err:
                        print("Programming error occurred:", prog_err)
                    except mysql.connector.errors.InterfaceError as intf_err:
                        print("Interface error occurred:", intf_err)
                    except Exception as e:
                        print(e)
                        self.connection_db()
                        self.cursor.execute(query)
                    results = self.cursor.fetchall()
                    self.connection.close()
                    self.cursor.close()
    #                 print(results)
                    if len(results)  == 0:
                        print('Blank result of this query: ',query)
                        json_data =  json.loads(i[3])
                        json_data['site'] = siteid
                        json_data_list.append(json_data)
                    # Process the results
                    elif None in results[0]:
                        json_data =  json.loads(i[3])
                        json_data['site'] = siteid
                        json_data_list.append(json_data)
                    else:
                        # print('result: ',results)
                        for j in results:
                            json_data = json.loads(j[0])
                            json_data_list.append(json_data)
    #                 print(query)
                    url = 'https://sindri.media' + parameter
                    print(url)
                    print(json_data)
                    my_for = json_data
                    if my_for != 0:
                        verf = self.compare_json_format(req_json_format,my_for)
                        verf =True
                        print('verf',verf)
                        if verf == True:
                            self.post(token,url,json_data_list)

                        else:
                            print(f'this query {query} of json not match in Swagger json: ',url)
                    else:
                        print(f'this query {query} result is empty')
                except Exception as e:
                    print(e)
            try:
                try:
                    result_server = self.server_resour()
                except Exception as e:
                    result_server =[e,e,e]
                    print(e)
                # HTML email template
                email_template = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                        }}
                        .email-container {{
                            width: 80%;
                            margin: auto;
                            padding: 20px;
                            border: 1px solid #ccc;
                        }}
                        .header {{
                            font-size: 24px;
                            margin-bottom: 10px;
                        }}
                        .details {{
                            font-size: 16px;
                            margin-top: 20px;
                        }}
                        .data-section {{
                            margin-top: 15px;
                        }}
                        .data-item {{
                            margin-bottom: 5px;
                        }}
                    </style>
                </head>
                <body>
                    <div class="email-container">
                        <div class="header">Data Posted Completed via Swagger: Site ID {siteid}</div>
                        <div class="details">
                            <p>The following data has been successfully posted to the Swagger (sindri.media) :</p>
                        </div>
                        <div class="data-section">
                            <ul>
                                <li class="data-item"><strong>Site ID:</strong> {siteid}</li>
                                <!-- Add more data items here -->
                            </ul>
                        </div>
                        <br>

                            <hr>
                            <p><strong><b>Server Resources</b></strong></p>
                            <table class="failed-list">
                                <tr>
                                    <th>Memory</th>
                                    <th>Storage</th>
                                    <th>CPU</th>
                                </tr>
                                <tr>
                                    <td><strong>{result_server[0]}</strong></td>
                                    <td>{result_server[1]}</td>
                                    <td>{result_server[2]}</td>
                                </tr>

                                <!-- Add more failed entries as needed -->
                            </table>
                        <div class="details">
                            <p>Thank you. <br>
                             If you have any questions or need further assistance, please don't hesitate to contact us.</p>
                            <p>Sincerely,
                            <br>Gulraiz
                            <br>Dotlabs.ai</p>
                        </div>
                    </div>
                </body>
                </html>
                """
                mail.mailConnection(subject, message,email_template)
            except Exception as e:
                print(e) 
        except Exception as e:
            print(e)

def dev_etl():
    etl = goals_ETL()
    etl.start_execution_goals()

