import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm
import sys
import logging
from google.cloud import bigquery_datatransfer
from dotenv import load_dotenv
import os
load_dotenv()

project_id = os.getenv('project_id')

def get_bq_queries():
    try:
        # Name the file directory where you want to save your backup.log
        logging.basicConfig(filename='backup.log', encoding='utf-8', level=logging.DEBUG)
        orig_stdout = sys.stdout

        # Provide your bigquery secret directory here
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/aakash/mp360/credentials/dev.json"

        # Initializing the clients
        client = bigquery.Client()
        transfer_client = bigquery_datatransfer.DataTransferServiceClient()

        # Creating the directory it is not already present
        if not os.path.exists('./projects/'):
            os.makedirs('./projects/')

        # Changing directory to Projects folder
        os.chdir('./projects/')

        # Initializing Project id from BQ

        datasets = client.list_datasets(project_id)

        print("Datasets contained in '{}':".format(project_id))

        df_datasets = pd.DataFrame()
        i = 0
        print(project_id+' : Extraction in Progress')

        # Iterating each dataset present in project_id
        for dataset in datasets:
            df_datasets.loc[i,'project_id'] = dataset.project
            df_datasets.loc[i,'dataset_id'] = dataset.dataset_id
            i = i + 1

            # creating folder if it doesnot exsist for project 
            if not os.path.exists(project_id):
                os.makedirs(project_id)
            root_path = './'+project_id+'/datasets/'
            path = root_path+dataset.dataset_id
            if not os.path.exists(path):
                os.makedirs(path)

            # saving Dataset Lists with './<project_id>/datasets/dataset_lists.csv'
            df_datasets.to_csv(root_path+'dataset_lists.csv', index=False)


        # Iterating each dataset from df_datasets
        for i, row in tqdm(df_datasets.iterrows()):
            dataset_id = row['project_id']+'.'+row['dataset_id']
            tables = client.list_tables(dataset_id)
            df_tables = pd.DataFrame()
            i = 0

            # Iterating each tables from dataset
            for table in tables:

                # DONE: Added additional details - https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.TableListItem
                df_tables.loc[i,'dataset_id'] = table.dataset_id
                df_tables.loc[i,'table_id'] = table.table_id
                df_tables.loc[i,'table_type'] = str(table.table_type).lower()
                df_tables.loc[i,'clustering_fields'] = table.clustering_fields
                df_tables.loc[i,'created'] = table.created
                df_tables.loc[i,'expires'] = table.expires
                df_tables.loc[i,'friendly_name'] = table.friendly_name
                df_tables.loc[i,'full_table_id'] = table.full_table_id
                df_tables.loc[i,'partition_expiration'] = table.partition_expiration
                df_tables.loc[i,'partitioning_type'] = table.partitioning_type
                df_tables.loc[i,'path'] = table.path
                df_tables.loc[i,'project'] = table.project
                df_tables.loc[i,'reference'] = table.reference
                df_tables.loc[i,'reference'] = table.reference
                df_tables.loc[i,'time_partitioning'] = table.time_partitioning
                df_tables.loc[i,'view_use_legacy_sql'] = table.view_use_legacy_sql
                i = i + 1
            
            summary_path = root_path+row['dataset_id']+'/'+row['dataset_id']+'_summary.csv'
            
            # saving tables Lists with for dataset
            df_tables.to_csv(summary_path,index=False)
            logging.info('Table summary is added at : '+summary_path)
            path_dataset = root_path+row['dataset_id']

            # creating folder if it doesnot exsist for dataset 
            if not os.path.exists(path_dataset):
                os.makedirs(path_dataset)

            # Iterating each tables from df_tables
            for i, row in tqdm(df_tables.iterrows(), desc =row['dataset_id']+' >> Extraction in Progress : '):

                table_id = dataset_id+'.'+row['table_id']
                table = client.get_table(table_id)
                view_or_table = 'table'

                # Check for weather table_id corresponds to a view or not
                if table.view_query != None:
                    view_or_table = 'view'
                
                path_table = path_dataset + '/' + view_or_table + '/' + row['table_id']
                save_path_label = path_table + '/' + row['table_id']


                # creating folder if it doesnot exsist for table 
                if not os.path.exists(path_table):
                    os.makedirs(path_table)
                
                # Saving query for table_id if it is a view
                if table.view_query != None:
                    file = open(save_path_label + "_view_query.txt","w")
                    sys.stdout = file
                    print(table.view_query)
                    sys.stdout = orig_stdout
                    file.close()
                    logging.info('View Query is added at : '+save_path_label + "_view_query.txt")

                schema = table.schema
                df_schema = pd.DataFrame()
                i = 0
                for field in schema:
                    df_schema.loc[i,'field_name'] = field.name
                    df_schema.loc[i,'type'] = field.field_type
                    df_schema.loc[i,'mode'] = field.mode
                    i = i + 1
                
                path_schema = path_table+'/'+row['table_id']
                # Saving schema for table_id
                df_schema.to_csv(save_path_label + '_schema.csv',index=False)
                logging.info('Schema is added at : '+save_path_label + "_schema.txt")
                
                query = """SELECT * FROM `{}` limit 10""".format(table_id)
                try:
                    data = client.query(query).result()
                    data_df = pd.DataFrame(data)
                    data_list = []
                    for i in range(len(data_df)):
                        data_list.append(list(data_df.iloc[i,0]))
                    col_headers = [field.name for field in data.schema]
                    df_bq = pd.DataFrame(data_list, columns=col_headers)
                    # Saving sample of table_id of size 10
                    df_bq.to_csv(save_path_label+'_sample.csv', index=False)
                except:
                    logging.error(table_id+' could not save the sample file.')
                if 'analytics_308497106' in row['dataset_id'].lower():
                    break

        os.chdir(project_id)
        parent_ = 'projects/{}/locations/{}'.format(project_id,'asia-south1')

        # Setting transfer client setup
        configs = transfer_client.list_transfer_configs(parent=parent_)

        for ele in configs:
            job_details = {
                'job_name' : ele.name,
                'destination_dataset_id' : ele.destination_dataset_id,
                'display_name' : ele.display_name,
                'data_source_id' : ele.data_source_id,
                'schedule' : ele.schedule,
                'schedule_options__start_time' : ele.schedule_options.start_time,
                'schedule_options_update_time' : str(ele.update_time),
                'schedule_options_next_run_time' : str(ele.next_run_time),
                'state' : ele.state,
                'user_id' : ele.user_id,
                'dataset_region' : ele.dataset_region,
                'email_preferences' : ele.email_preferences.enable_failure_email,
            }

            root_path = './'+'transfers/'
            df_params = pd.DataFrame()
            i = 0
            
            # creating folder if it doesnot exsist for datasource 
            path_dataset_source = root_path + job_details['data_source_id']
            if not os.path.exists(path_dataset_source):
                os.makedirs(path_dataset_source)
            
            # creating folder if it doesnot exsist for datatransfer 
            path_data_transfer = path_dataset_source + '/' + job_details['display_name']
            if not os.path.exists(path_data_transfer):
                os.makedirs(path_data_transfer)
            

            df_job = pd.DataFrame()

            job_details_path = path_data_transfer + '/' + job_details['display_name'] 

            i = 0
            for key in job_details.keys():
                df_job.loc[i,'parameter'] = key
                df_job.loc[i,'value'] = job_details[key]
                i = i + 1
            
            # Saving job_details for Transfer/Scheduled query
            df_job.to_csv(job_details_path + '__info.csv', index=False)
            logging.info('Job info is added at : ' + job_details_path + '__info.csv')

            for param in ele.params:
                # Saving query for Scheduled query
                if param == 'query':
                    file = open(job_details_path + "__scheduled_query.txt","w")
                    sys.stdout = file
                    print(ele.params[param])
                    sys.stdout = orig_stdout
                    file.close()
                    logging.info('Scheduled query is added at : ' + job_details_path + "__scheduled_query.txt")

                df_params.loc[i,'parameter'] = param
                df_params.loc[i,'value'] = ele.params[param]
                i = i + 1

            # Saving job_params for Transfer/Scheduled query
            df_params.to_csv(job_details_path + '__params.csv', index=False)
            logging.info('Job params added at : ' + job_details_path + '__params.csv')

    
    except Exception as e:
        logging.info("some error occurred.")
        sys.exit(1)