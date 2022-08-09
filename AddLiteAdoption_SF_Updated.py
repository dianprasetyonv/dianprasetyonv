'''
This code fetches the adoption data from dbo.objects table in NL SQL DB and upserts into Salesforce.
@author: Sravani

Updated: Dian
Updated for the database name used where it was previously outdated.
Update how to get NL user from the MSSQL table where previously it only captures novade lite leads.
It is updated to capture novade lite leads and leads that are not yet converted.
Update done to function "get_SF_NL_Leads".
'''
import sys
import json
import pandas as pd
import pyodbc

from datetime import datetime, date

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

sys.path.append('/home/DataPipeline/airflow/Py_functions_DAGS/DB_tools')
from simple_salesforce import Salesforce
import Sf_tools as st
# import Lite_functions as lf

SF_sandbox = False

DAG_ID = 'AddLiteAdoption_SF_Updated'
start_date = datetime(2022, 5, 4)
default_args = {
    'owner': 'Dian',
    'depends_on_past': False,
    'start_date': start_date
    }
dag = DAG(dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='@daily',
    tags=['Novade Lite', 'Salesforce', 'Adoption'])


def connect_mssql_NovadeLite():
    '''     
    This function connects to the NovadeLite SQL database on azure
    '''
    mssql_connection = BaseHook.get_connection('azure_sql_database_credentials')
    driver= '{ODBC Driver 17 for SQL Server}'
    clientID = str(mssql_connection.login)
    client_secret = str(mssql_connection.password)
    server = str(mssql_connection.host)
    database_name = 'NovadeLite'
    connection_string = 'DRIVER=' + driver + \
                        ';SERVER=' + server + \
                        ';PORT=1433' + \
                        ';DATABASE=' + database_name + \
                        ';UID=' + clientID + \
                        ';PWD=' + client_secret + \
                        ';CHARSET=UTF8' + \
                        ';Encrypt=yes'
    mssql_conn = pyodbc.connect(connection_string)
    
    return mssql_conn

#### LOAD SF CONNECTION ####
def get_SF_connection(SF_sandbox, **kwargs):
    '''     
    This function connects to the Salesforce UAT/Production
    '''
    if SF_sandbox:
        SF_connection = BaseHook.get_connection("Salesforce_Test")
    else:
        SF_connection = BaseHook.get_connection("Salesforce_Prod")
    
    SF_login = str(SF_connection.login)
    SF_pw = str(SF_connection.password)
    SF_extra = json.loads(SF_connection.extra)

    ## SANDBOX
    if SF_sandbox:
        sf = Salesforce(username = SF_login, password = SF_pw, organizationId = SF_extra['organizationId'],
                        instance_url = SF_extra['instance_url_test'], domain = 'test')
    ## PRODUCTION
    else:
        sf = Salesforce(username = SF_login, password = SF_pw, security_token = SF_extra['security_token'],
                        instance_url = SF_extra['instance_url_prod'])
    return sf

def gen_db_table_df(conn, table, columns=None, where=None):
    '''     
    This function connects to sql database table given the connection, returns the table in df format
    '''
    if columns:
        cols = ','.join(columns)
        query = " SELECT " + cols + " FROM \"dbo\".\"" + table + "\""
    else:
        query = " SELECT * FROM \"dbo\".\"" + table + "\""
    if where:
        query = query + " WHERE "+ where
    df = pd.read_sql_query(query, conn)
    return df


def get_SF_NL_Leads(**kwargs):
    ''' This function connects and fetches all NL leads
    '''
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids="get_SF_connection")
    lead_cols = ["Id", "Email", "Workspace_Name__c", "NL_Lead_Unique_Identifier__c", "NL_Adoption__c","NL_User__c","IsConverted"]
    sf_leads = st.gen_table_df(sf, lead_cols, "Lead", where="NL_User__c=True")
    sf_leads = sf_leads[sf_leads['IsConverted'] == False] 
    sf_leads.drop(["NL_User__c","IsConverted"], axis=1, inplace = True)
    sf_leads = sf_leads.drop_duplicates()
    print("Total number of NL leads in SF: ", sf_leads.shape[0])
    print(sf_leads.iloc[0])
    return sf_leads

def get_SF_NL_Contacts(**kwargs):
    ''' This function connects and fetches all NL contacts
    '''
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids="get_SF_connection")
    contact_cols = ["Id", "Email", "Workspace_Name__c", "NL_Lead_Unique_Identifier__c", "NL_Adoption__c"]
    sf_contacts = st.gen_table_df(sf, contact_cols, "Contact", where="NL_User__c=True")
    print("Total number of contacts in SF: ", sf_contacts.shape[0])
    print(sf_contacts.iloc[0])
    return sf_contacts

def get_NL_adoption_df(**kwargs):
    ''' This function counts the total adoption for all workspaces
    '''
    ti = kwargs['ti']
    mssql_conn = connect_mssql_NovadeLite()
    adoption_df = gen_db_table_df(mssql_conn, "objects", ["connection","id","date","workspaceID","objectType"], None)
    adoption_df = adoption_df.groupby(["connection","workspaceID"])["id"].count().reset_index()
    adoption_df.rename(columns={"workspaceID":"databaseID","id":"NL_Adoption__c"}, inplace=True)

    print("Number of workspaces with adoption: ", adoption_df.shape[0])
    print(adoption_df.iloc[0])
    return adoption_df


def get_NL_users(**kwargs): ## clean up leads
    ''' This function gets NL users and joins with adoption df
    '''
    ti = kwargs['ti']
    mssql_conn = connect_mssql_NovadeLite()
    users_df = gen_db_table_df(mssql_conn, "users", ["connection","id","email"], "isCreator=1 AND isAcceptedTnC=1")
    dbaccess_df = gen_db_table_df(mssql_conn, "databaseaccesses", ["connection","userID","workspaceID","status2"])
    dbaccess_df = dbaccess_df[dbaccess_df['status2'] == 'active']
    dbaccess_df.drop(['status2'], axis=1, inplace = True)
    dbaccess_df.rename(columns={'workspaceID':'databaseID','userID':'id'}, inplace=True)
    workspace_df = gen_db_table_df(mssql_conn, "workspaces", ["connection","id","name","customerID"])
    workspace_df.rename(columns={"id":"databaseID","name":"Workspace_Name__c"}, inplace=True)

    users_df = pd.merge(users_df, dbaccess_df, how = "left", on =["connection","id"])
    users_df = pd.merge(users_df, workspace_df, how = "left", on =["connection","databaseID"])
    users_df = users_df.loc[~users_df.databaseID.isnull()]
    users_df.id = users_df['id'].astype(str)
    users_df.customerID = users_df['customerID'].astype(str)
    users_df.databaseID = users_df['databaseID'].astype(str)
    users_df["NL_Lead_Unique_Identifier__c"] = users_df.apply(lambda x: x["id"]+'|'+x["customerID"]+'|'+x["databaseID"], axis=1)

    print('users_df.columns = ', users_df.columns)
    print(users_df.iloc[0])
    return users_df

def get_users_with_adoption(**kwargs):
    ''' This functions matches all workspace adoption to be updated
    '''
    ti = kwargs['ti']
    adoption_df = ti.xcom_pull(key=None, task_ids="get_NL_adoption_df")
    users_df = ti.xcom_pull(key=None, task_ids="get_NL_users")
    final_adoption_df = pd.merge(adoption_df, users_df, how='left', on=['connection','databaseID'])
    final_adoption_df = final_adoption_df[['NL_Lead_Unique_Identifier__c','NL_Adoption__c']]
    print("Combined adoption df shape: ", final_adoption_df.shape)
    print(final_adoption_df.iloc[0])
    return final_adoption_df

def update_adoption_Leads(**kwargs):
    ''' This function updates changes to existing SF NL Leads
    '''
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids="get_SF_connection")
    sf_leads = ti.xcom_pull(key=None, task_ids="get_SF_NL_Leads")
    final_adoption_df = ti.xcom_pull(key=None, task_ids="get_users_with_adoption")
    toupdate_df = pd.merge(final_adoption_df,sf_leads[['Id','NL_Lead_Unique_Identifier__c']], how='left', on = 'NL_Lead_Unique_Identifier__c')
    toupdate_df = toupdate_df.loc[~toupdate_df.Id.isnull()]

    if (len(toupdate_df)>0):
        print("Number of records to update: ", len(toupdate_df))
        list_to_update = toupdate_df[["Id","NL_Adoption__c"]].to_dict(orient="records")
        err_list = []
        gen_error = False
        for record in list_to_update:
            print(record)
            record_id = record['Id']
            record_line = {k: v for k, v in record.items() if v}
            record_line.pop('Id', None)
            try:
                sf.Lead.update(record_id, record_line)
                print("ADDED ", record_id, record_line)
            except:
                err_list.append(record_line)
                gen_error = True
        if gen_error:
            print("ERROR for the following records: ", err_list)
            raise ValueError('Error in returned list.')
    else:
        print("No existing records to update")
    return None

def update_adoption_Contacts(**kwargs):
    ''' This function updates changes to existing SF NL Contacts
    '''
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids="get_SF_connection")
    sf_contacts = ti.xcom_pull(key=None, task_ids="get_SF_NL_Contacts")
    final_adoption_df = ti.xcom_pull(key=None, task_ids="get_users_with_adoption")
    toupdate_df = pd.merge(final_adoption_df,sf_contacts[['Id','NL_Lead_Unique_Identifier__c']], how='left', on = 'NL_Lead_Unique_Identifier__c')
    toupdate_df = toupdate_df.loc[~toupdate_df.Id.isnull()]

    if (len(toupdate_df)>0):
        print("Number of records to update: ", len(toupdate_df))
        list_to_update = toupdate_df[["Id","NL_Adoption__c"]].to_dict(orient="records")
        err_list = []
        gen_error = False
        for record in list_to_update:
            record_id = record['Id']
            record_line = {k: v for k, v in record.items() if v}
            record_line.pop('Id', None)
            try:
                sf.Contact.update(record_id, record_line)
                print("ADDED ", record_id, record_line)
            except:
                err_list.append(record_line)
                gen_error = True
        if gen_error:
            print("ERROR for the following records: ", err_list)
            raise ValueError('Error in returned list.')
    else:
        print("No existing records to update")
    return None


getSFConnection = PythonOperator(
    task_id='get_SF_connection',
    provide_context=True,
    python_callable=get_SF_connection,
    op_kwargs={'SF_sandbox': SF_sandbox},
    dag=dag
)

getSFNLLeads = PythonOperator(
    task_id='get_SF_NL_Leads',
    provide_context=True,
    python_callable=get_SF_NL_Leads,
    dag=dag
)

getSFNLContacts = PythonOperator(
    task_id='get_SF_NL_Contacts',
    provide_context=True,
    python_callable=get_SF_NL_Contacts,
    dag=dag
)

getNLAdoption = PythonOperator(
    task_id='get_NL_adoption_df',
    provide_context=True,
    python_callable=get_NL_adoption_df,
    dag=dag
)

getNLUsers = PythonOperator(
    task_id='get_NL_users',
    provide_context=True,
    python_callable=get_NL_users,
    dag=dag
)

getUsersWithAdoption = PythonOperator(
    task_id='get_users_with_adoption',
    provide_context=True,
    python_callable=get_users_with_adoption,
    dag=dag
)

updateNLAdoptionLeads = PythonOperator(
    task_id='update_adoption_Leads',
    provide_context=True,
    python_callable=update_adoption_Leads,
    dag=dag
)

updateNLAdoptionContacts = PythonOperator(
    task_id='update_adoption_Contacts',
    provide_context=True,
    python_callable=update_adoption_Contacts,
    dag=dag
)



[getNLAdoption, getNLUsers] >> getUsersWithAdoption
getSFConnection >> [getSFNLLeads, getSFNLContacts] >> getUsersWithAdoption
getUsersWithAdoption >> [updateNLAdoptionLeads, updateNLAdoptionContacts]