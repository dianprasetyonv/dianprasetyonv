import pandas as pd
from pandas.io.json import json_normalize
import json
from datetime import datetime
from datetime import timedelta
from simple_salesforce import Salesforce, format_soql
from dateutil.relativedelta import relativedelta
from xero import Xero
from xero.auth import OAuth2Credentials
import requests
import sys
import logging
import ast

#airflow imports
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow import DAG

sys.path.append('/home/DataPipeline/airflow/Py_functions_DAGS/DB_tools/')
import sf_xero_cleaning as sxc
import data_cleaning_xero as dcx

'''Updated DAG from XeroToSF
    Modification is done because the previous DAG file is not able to refresh automatically.
    Modification is done to the get_xero_credentials where it is updated from only fetching the credentials to also get payments from Xero.
    Added two functions to automatically connect and refresh connections to Xero:
    def get_xero_connection(**kwargs)
    def check_xero_connection(**kwargs)
'''

storage_path = '/home/storage/airflow_storage/'

logger = logging.getLogger(__name__)

DAG_ID = 'XeroToSF_Updated'
start_date = datetime(2022, 4, 21)
default_args = {
    'owner': 'Dian',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 2
    }

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='00 16 * * *'
    )

SF_sandbox = False

#### LOAD SF CONNECTION ### #
def get_SF_connection(**kwargs):
    if SF_sandbox:
        SF_connection = BaseHook.get_connection("Salesforce_Test")
    else:
        SF_connection = BaseHook.get_connection("Salesforce_Prod")
    
    SF_login = str(SF_connection.login)
    SF_pw = str(SF_connection.password)
    SF_extra = json.loads(SF_connection.extra)

    ## SANDBOX
    if SF_sandbox:
        sf = Salesforce(username = SF_login,
                        password = SF_pw,
                        organizationId = SF_extra['organizationId'],
                        instance_url = SF_extra['instance_url_test'],
                        domain = 'test')
    ## PRODUCTION
    else:
        sf = Salesforce(username = SF_login,
                        password = SF_pw,
                        security_token = SF_extra['security_token'],
                        instance_url = SF_extra['instance_url_prod'])
        
    
    return sf

#connect to xero and return Xero instance
def get_xero_credentials_payments(**kwargs):
    '''This function is used to get the credentials from XERO_CREDS and if connection succesffull, then get 
        the payments from Xero   
    '''
    ti = kwargs['ti']
    connection_status = ti.xcom_pull(key=None, task_ids='get_xero_connection')
    xero_creds = ast.literal_eval(Variable.get('XERO_CREDS'))
    print("xero_creds_1 ",xero_creds)

    if connection_status == 200:
        ACCESS_TOKEN = Variable.get('XERO_ACCESS_TOKEN')
        REFRESH_TOKEN = Variable.get('XERO_REFRESH_TOKEN')
        xero_creds["token"]["access_token"] = ACCESS_TOKEN
        xero_creds["token"]["refresh_token"] = REFRESH_TOKEN
        print("xero_creds_2 ",xero_creds)
        credentials = OAuth2Credentials(**xero_creds)
        xero = Xero(credentials)
        dt_object = datetime(2021, 12, 2)
        # dt_object = datetime.now() - relativedelta(years=1)
        payments_df = pd.json_normalize(xero.payments.filter(since= dt_object))
        #only save payments for invoices that start with S-
        payments_df_1 = payments_df[payments_df['Invoice.InvoiceNumber'].str.startswith('S-')]
        payments_df_2 = payments_df[payments_df['Invoice.InvoiceNumber'].str.startswith('SL-')]
        payments_df = pd.concat([payments_df_1, payments_df_2])
    else:
        connection = BaseHook.get_connection("crm_xero")
        CLIENT_ID = str(connection.login)
        CLIENT_SECRET = str(connection.password)
        extra = json.loads(connection.extra)
        REFRESH_TOKEN = Variable.get('XERO_REFRESH_TOKEN')

        tokens = dcx.getRefreshAccessTokens(extra['TOKEN_URL'], CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        print("tokens: ",tokens)
        Variable.set("XERO_ACCESS_TOKEN", tokens['access_token'])
        Variable.set("XERO_REFRESH_TOKEN", tokens['refresh_token'])
        ACCESS_TOKEN = tokens['access_token']
        REFRESH_TOKEN = tokens['refresh_token']
        print("Updated new refresh and access tokens")
        xero_creds["token"]["access_token"] = ACCESS_TOKEN
        xero_creds["token"]["refresh_token"] = REFRESH_TOKEN
        print("xero_creds_2 ",xero_creds)
        credentials = OAuth2Credentials(**xero_creds)
        xero = Xero(credentials)
        dt_object = datetime(2021, 12, 2)
        # dt_object = datetime.now() - relativedelta(years=1)
        payments_df = pd.json_normalize(xero.payments.filter(since= dt_object))
        #only save payments for invoices that start with S-
        payments_df_1 = payments_df[payments_df['Invoice.InvoiceNumber'].str.startswith('S-')]
        payments_df_2 = payments_df[payments_df['Invoice.InvoiceNumber'].str.startswith('SL-')]
        payments_df = pd.concat([payments_df_1, payments_df_2])

    return payments_df

def get_xero_connection(**kwargs):
    ''' This function makes a GET connection request to Xero and returns the response
    The request response will contain the xero tenantId in case required
    '''
    connection = BaseHook.get_connection("crm_xero")
    extra = json.loads(connection.extra)
    ACCESS_TOKEN = Variable.get('XERO_ACCESS_TOKEN')
    response = requests.get(extra['CONNECTION_URL'], headers={'Authorization': 'Bearer ' + ACCESS_TOKEN}, verify=False)
    print("Response: ", response)
    if (response.status_code):
        return (response.status_code)
    else:
        return (401)

def check_xero_connection(**kwargs):
    '''This function uses the XERO app credentials and access token to call the API. In case of unsuccessful connection,
    new refresh tokens and access tokens are generated and stored in environment variables for later use     
    '''
    ti = kwargs['ti']
    connection_status = ti.xcom_pull(key=None, task_ids='get_xero_connection')

    connection = BaseHook.get_connection("crm_xero")
    CLIENT_ID = str(connection.login)
    CLIENT_SECRET = str(connection.password)
    extra = json.loads(connection.extra)
    REFRESH_TOKEN = Variable.get('XERO_REFRESH_TOKEN')

    if connection_status != 200:
        tokens = dcx.getRefreshAccessTokens(extra['TOKEN_URL'], CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN)
        print("tokens: ",tokens)
        Variable.set("XERO_ACCESS_TOKEN", tokens['access_token'])
        Variable.set("XERO_REFRESH_TOKEN", tokens['refresh_token'])
        Variable.set("Xero_Access", tokens['access_token'])
        Variable.set("Xero_Refresh", tokens['refresh_token'])
        print("Updated new refresh and access tokens")
        new_connection_status = ti.xcom_pull(key=None, task_ids='get_xero_connection')
        if new_connection_status != 200:
            print("Failed to establish connection to Xero")
        else:
            print("Connection established successfully")
    else:
        print("Connection established successfully")

def get_sf_invoicenumbers(**kwargs):

    #conect to salesfroce.
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids='get_SF_connection')

    payments_df = ti.xcom_pull(key=None, task_ids = 'get_xero_credentials_payments')

    if len(payments_df) > 0:
        invoice_numbers = payments_df['Invoice.InvoiceNumber'].unique().tolist()
    else:
        return None

    invoiceid_num_df = pd.DataFrame(
    sf.query_all(
        format_soql("SELECT Id, Invoiced_Amount_Tax_Inc__c, Invoice_Name__c FROM Invoice__c WHERE Invoice_Name__c IN {invoiceNumbers}", invoiceNumbers = invoice_numbers)

    )['records'])
    
    print('InvoiceNumbers paid in Xero: ')
    print(invoice_numbers)


    #send invoicenum_df and payments df to merge. 
    if len(invoiceid_num_df)  == 0:
        return None
    merged_df = sxc.formatPaymentsDF(payments_df, invoiceid_num_df)

    return merged_df

def send_payments_df(**kwargs):

    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids='get_SF_connection')
    merged_df = ti.xcom_pull(key=None, task_ids='get_sf_invoicenumbers')
    if merged_df is None:
        return
    merged_df['Payment_Type__c'] = 'Bank Transfer'

    #check if payments already exist on salesforce.
    xero_paymentids_list = merged_df.External_ID__c.unique().tolist()
    existing_payments_df = pd.DataFrame(
        sf.query_all(
            format_soql("SELECT External_ID__c FROM Payment__c WHERE External_ID__c IN {paymentIds}", paymentIds = xero_paymentids_list)

        )['records'])
  
    if len(existing_payments_df) > 0:
        existing_payments_list = existing_payments_df.External_ID__c.unique().tolist()
    else:
        existing_payments_list = []

    #send only payments that don't already exist in salesforce
    payments_df = merged_df[~merged_df.External_ID__c.isin(existing_payments_list)]

    #SEND PAYMENTS TO SF.
    successCount = 0
    failedCount = 0
    for row in payments_df.to_dict('records'):
        res = sf.Payment__c.create(row)
        if res:
            print('Payment Suceeded for: ' + str(row['Description__c']))
            successCount += 1
        else:
            print('Payment failed for: ' + str(row['Description__c']))
            failedCount += 1
            
    print('Total Payments sent: ' + str(successCount))
    print('Total Payments failed: ' + str(failedCount))


getSFConnection = PythonOperator(
    task_id='get_SF_connection',
    provide_context=True,
    python_callable=get_SF_connection,
    pool='adoption_pool',
    dag=dag
)

getXeroCredentialsPayments = PythonOperator(
    task_id='get_xero_credentials_payments',
    provide_context=True,
    python_callable=get_xero_credentials_payments,
    pool='adoption_pool',
    dag=dag
)

checkXeroConnection = PythonOperator(
    task_id = 'check_xero_connection',
    provide_context = True,
    python_callable = check_xero_connection,
    retries = 2,
    dag = dag
)

getXeroConnection = PythonOperator(
    task_id='get_xero_connection',
    provide_context=True,
    python_callable=get_xero_connection,
    pool='adoption_pool',
    retries = 2,
    dag=dag
)

getSFInvoiceNumbers = PythonOperator(
    task_id='get_sf_invoicenumbers',
    provide_context=True,
    python_callable=get_sf_invoicenumbers,
    pool='adoption_pool',
    dag=dag
)

sendPayments = PythonOperator(
    task_id='send_payments_df',
    provide_context=True,
    python_callable=send_payments_df,
    pool='adoption_pool',
    dag=dag
)

((getSFConnection), 
    (getXeroConnection >> checkXeroConnection>> getXeroCredentialsPayments)) >> getSFInvoiceNumbers >> sendPayments