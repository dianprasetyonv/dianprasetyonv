import pandas as pd
from pandas.io.json import json_normalize
import json
from datetime import datetime
from datetime import timedelta
from datetime import date
from simple_salesforce import Salesforce, format_soql
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

'''Updated DAG from SFToXero
    Modification is done because the previous DAG file is not able to refresh automatically.
    Modification is done to the get_xero_credentials where it is updated from only fetching the credentials to also get invoice from Xero.
    Added two functions to automatically connect and refresh connections to Xero:
    def get_xero_connection(**kwargs)
    def check_xero_connection(**kwargs)
'''

storage_path = '/home/storage/airflow_storage/'

logger = logging.getLogger(__name__)

DAG_ID = 'SFToXero_Updated'
start_date = datetime(2022, 4, 24)
default_args = {
    'owner': 'Dian',
    'depends_on_past': False,
    'start_date': start_date,
    'retries': 2
    }

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='00 15 * * *'
    )


SF_sandbox = False

#### LOAD SF CONNECTION ####
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

#connect to xero and return Xero instance
def get_xero_credentials_invoices(**kwargs):
    '''This function is used to get the credentials from XERO_CREDS and if connection succesffull, then get 
        the payments from Xero   
    '''
    ti = kwargs['ti']
    connection_status = ti.xcom_pull(key=None, task_ids='get_xero_connection')
    xero_creds = ast.literal_eval(Variable.get('XERO_CREDS'))
    sf_invoice_df = ti.xcom_pull(key=None, task_ids='get_full_invoice_df_with_invoice_items')
    print("xero_creds_1 ",xero_creds)

    if connection_status == 200:
        ACCESS_TOKEN = Variable.get('XERO_ACCESS_TOKEN')
        REFRESH_TOKEN = Variable.get('XERO_REFRESH_TOKEN')
        xero_creds["token"]["access_token"] = ACCESS_TOKEN
        xero_creds["token"]["refresh_token"] = REFRESH_TOKEN
        print("xero_creds_2 ",xero_creds)
        credentials = OAuth2Credentials(**xero_creds)
        xero = Xero(credentials)
        xero_invoices = pd.DataFrame(xero.invoices.filter(InvoiceNumber__startswith='S-'))
        xero_invoices_l = pd.DataFrame(xero.invoices.filter(InvoiceNumber__startswith='SL-'))
        xero_invoices = pd.concat([xero_invoices, xero_invoices_l])
        invoicenames_in_xero = []
        for index, row in sf_invoice_df.iterrows():
            return_df = xero_invoices[xero_invoices['InvoiceNumber'] == row['InvoiceNumber']]
        if len(return_df) > 0:
            return_df = return_df[return_df['Status'] != 'DELETED']
        if len(return_df) > 0:
            invoicenames_in_xero.append(row['InvoiceNumber'])    #returns list of invoices out of the invoices being sent to xero. 
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
        xero_invoices = pd.DataFrame(xero.invoices.filter(InvoiceNumber__startswith='S-'))
        xero_invoices_l = pd.DataFrame(xero.invoices.filter(InvoiceNumber__startswith='SL-'))
        xero_invoices = pd.concat([xero_invoices, xero_invoices_l])
        invoicenames_in_xero = []
        for index, row in sf_invoice_df.iterrows():
          return_df = xero_invoices[xero_invoices['InvoiceNumber'] == row['InvoiceNumber']]
          if len(return_df) > 0:
            return_df = return_df[return_df['Status'] != 'DELETED']
            if len(return_df) > 0:
                invoicenames_in_xero.append(row['InvoiceNumber'])    #returns list of invoices out of the invoices being sent to xero. 
    return invoicenames_in_xero

def get_sf_invoice_df(**kwargs):
    '''
    * gets all invoices with names like 'S-%' in Salesforce.
    '''
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids='get_SF_connection')

    sf_invoice_df = pd.DataFrame(
      sf.query_all(
         "SELECT Account__r.Name, BillTo__r.Name, OverrideBillTo__c, Invoice_Name__c, Sent_Date__c, Due_Date__c, CurrencyIsoCode, Reseller__r.Name  \
        FROM Invoice__c \
        Where Sent_Date__c >= LAST_N_DAYS:365 and (Invoice_Name__c LIke 'SL-%' or Invoice_Name__c LIke 'S-%')"

    )['records'])
    
    sf_invoice_df = sxc.format_sf_invoice_df(sf_invoice_df)
    
    return sf_invoice_df


def get_full_invoice_df_with_invoice_items(**kwargs):
    '''
    * Gets the invoice items correlating to the invoices needed to be sent, adds them to the respective invoices
    * Formatted for Xero API post request
    '''
    
    ti = kwargs['ti']
    sf = ti.xcom_pull(key=None, task_ids='get_SF_connection')
    
    sf_invoice_df = ti.xcom_pull(key=None, task_ids='get_sf_invoice_df')
    sf_invoice_names = sf_invoice_df["InvoiceNumber"].values.tolist()
    print(sf_invoice_names)

    #get all invoice items related to sf invoices
    sf_invoice_items_df = pd.DataFrame(
      sf.query_all(
         format_soql("SELECT Invoice__r.Order__r.Order_Amount_Tax_Exc__c, Invoice__r.Account__r.BillingCountry,Invoice__r.Special_Tax_Criteria__c, Invoice__r.Invoice_Name__c, Invoice__r.Tax_Rate__c, Order_Product_Name__c, Total_Amount__c FROM Invoice_Item__c WHERE Invoice__r.Invoice_Name__c IN {names}", names=sf_invoice_names)

    )["records"])

    sf_invoice_df = sxc.format_sf_invoice_with_items_df(sf_invoice_df, sf_invoice_items_df)

    return sf_invoice_df

def post_to_xero(**kwargs):

    #     # Filter out invoices already there in Xero
    ti = kwargs['ti']
    sf_invoice_df = ti.xcom_pull(key=None, task_ids='get_full_invoice_df_with_invoice_items')
    xero_inv_names = ti.xcom_pull(key=None, task_ids='get_xero_credentials_invoices')

    xero_creds = ast.literal_eval(Variable.get('XERO_CREDS'))

    ACCESS_TOKEN = Variable.get('XERO_ACCESS_TOKEN')
    TENANT_ID = xero_creds['tenant_id']
    API_CALL_HEADERS = {'Authorization': 'Bearer ' + ACCESS_TOKEN, 'xero-tenant-id': TENANT_ID, 
    'Accept':'application/json', 'Content-Type':'application/json'}
    INVOICE_API_URL = "https://api.xero.com/api.xro/2.0/Invoices"

    print("Number of Invoices already in Xero: ", len(xero_inv_names))
    print(sf_invoice_df)
    sf_invoice_df = sf_invoice_df[~sf_invoice_df.InvoiceNumber.isin(xero_inv_names)]
    print("Number of Invoices to send to Xero", sf_invoice_df.shape[0])

    successful_invoices_count = 0
    failed_invoices_count = 0
    for i in sf_invoice_df.index:
        json_dump = sf_invoice_df.loc[sf_invoice_df.index==i].to_json(orient='records')[1:-1]
        print(json_dump)
        json_data = json.loads(json_dump)
        response = requests.post(INVOICE_API_URL, headers=API_CALL_HEADERS, json = json_data)
        print(sf_invoice_df.loc[sf_invoice_df.index==i,'InvoiceNumber'].values[0], "POST status: ", response.status_code)
        if (response.status_code != 200):
            failed_invoices_count += 1
            print(response.text)
        else:
            successful_invoices_count += 1

    print("Number of invoices successfully sent: "  + str(successful_invoices_count))
    print("Number of invoices failed to send: " + str(failed_invoices_count))

getSFConnection = PythonOperator(
    task_id='get_SF_connection',
    provide_context=True,
    python_callable=get_SF_connection,
    pool='adoption_pool',
    retries = 2,
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

getXeroCredentialsInvoices = PythonOperator(
    task_id='get_xero_credentials_invoices',
    provide_context=True,
    python_callable=get_xero_credentials_invoices,
    retries = 2,
    dag=dag
)

getSFInvoices = PythonOperator(
    task_id='get_sf_invoice_df',
    provide_context=True,
    python_callable=get_sf_invoice_df,
    pool='adoption_pool',
    retries = 2,
    dag=dag
)

getFullSFInvoices = PythonOperator(
    task_id='get_full_invoice_df_with_invoice_items',
    provide_context=True,
    python_callable=get_full_invoice_df_with_invoice_items,
    pool='adoption_pool',
    retries = 2,
    dag=dag
)

postToXero = PythonOperator(
    task_id='post_to_xero',
    provide_context=True,
    python_callable=post_to_xero,
    pool='adoption_pool',
    retries = 2,
    dag=dag
)

(
    (getSFConnection >> getSFInvoices >> getFullSFInvoices),
    (getXeroConnection >> checkXeroConnection)
) >> getXeroCredentialsInvoices >> postToXero