import pandas as pd
import numpy as np
import operator as op

##For payments
def formatPaymentsDF(payments_df, invoiceid_num_df):

    invoiceid_num_df = invoiceid_num_df[['Id', 'Invoice_Name__c', 'Invoiced_Amount_Tax_Inc__c']]
    invoiceid_num_df = invoiceid_num_df.rename(columns={'Invoice_Name__c':'Invoice.InvoiceNumber'})

    #get payments where invoice number starts with S-
    payments_df = payments_df[['PaymentID', 'Date', 'Amount', 'Invoice.CurrencyCode', 'Invoice.InvoiceNumber']]
    #merge id to payment df
    merged_df = payments_df.merge(invoiceid_num_df, on='Invoice.InvoiceNumber', how='right') 
    #set description for full vs partial payments.
    for index, row in merged_df.iterrows():
        
        description = ""
        
        if row['Amount'] == row['Invoiced_Amount_Tax_Inc__c']:
            description = 'Full payment for ' + row['Invoice.InvoiceNumber']
        else:
            description = 'Partial payment for ' + row['Invoice.InvoiceNumber']

        merged_df.loc[index, 'Description'] = description

    #rename columns to match salesfroce names
    merged_df = merged_df.rename(columns={'PaymentID':'External_ID__c', 'Date':'Date_Paid__c','Amount':'Amount_Paid__c',\
                                         'Invoice.CurrencyCode':'CurrencyIsoCode','Id':'Invoice__c', 'Description':'Description__c'})
    merged_df = merged_df.drop('Invoice.InvoiceNumber', 1)
    merged_df['Date_Paid__c'] = formatSFDateCol(merged_df['Date_Paid__c'])
    merged_df = merged_df.drop('Invoiced_Amount_Tax_Inc__c', axis = 1)

    return merged_df



    
def addADS_SFInvoiceItemsDF(sf_invoice_items_df):
     
    new_invoice_item_df = sf_invoice_items_df
    
    added_ads_invoicenumberlist = []
    ads_invoice_items = sf_invoice_items_df[sf_invoice_items_df["SpecialTaxCriteria"] != ""]

    for index, row in ads_invoice_items.iterrows():
        if row["InvoiceNumber"] in added_ads_invoicenumberlist:
            continue

        sum_amount = ads_invoice_items[ads_invoice_items["InvoiceNumber"] == row["InvoiceNumber"]]["LineAmount"].agg('sum')
        order_amount = row['OrderAmount']
        tax_rate_decimal = row['taxrate'] /100

        if row["SpecialTaxCriteria"] == "Apply 100% Tax":
            unit_amount = (order_amount * tax_rate_decimal) - (tax_rate_decimal*sum_amount) 
            value = {"InvoiceNumber": row["InvoiceNumber"],"Description": "Add: GST not bill to IMDA", "AccountCode": 24000, "UnitAmount": unit_amount, "LineAmount": unit_amount,
                "Quantity": 1, "TaxType": "NONE"}
            new_invoice_item_df = new_invoice_item_df.append(value, ignore_index=True)


        elif row["SpecialTaxCriteria"] == "No Tax Applied":
            unit_amount = -1*(tax_rate_decimal * sum_amount)
            value = {"InvoiceNumber": row["InvoiceNumber"],"Description": "Less: GST not bill to IMDA", "AccountCode": 24000, "UnitAmount": unit_amount, "LineAmount": unit_amount,
                "Quantity": 1, "TaxType": "NONE"}
            new_invoice_item_df = new_invoice_item_df.append(value, ignore_index=True)

        new_invoice_item_df = new_invoice_item_df.astype(object).replace([np.nan], "")

        added_ads_invoicenumberlist.append(row["InvoiceNumber"])
        
    return new_invoice_item_df

def addWithHoldingTaxItems_SfInvoiceItemDF(sf_invoice_items_df):
    
    new_invoice_item_df = sf_invoice_items_df
       
    #get dataframe where tax
    for index, row in sf_invoice_items_df.iterrows():
    
        if row['taxrate']<0:
            value = {"InvoiceNumber": row["InvoiceNumber"],"Description": "Withholding Tax", "AccountCode": 50810, "UnitAmount": row['UnitAmount']*row['taxrate']/100, "LineAmount": row['UnitAmount']*row['taxrate']/100,
            "Quantity": 1, "TaxType": "NONE"}
            new_invoice_item_df = new_invoice_item_df.append(value, ignore_index=True)
        elif (row['taxrate']!=0 and row['taxrate']!=7):
            value = {"InvoiceNumber": row["InvoiceNumber"],"Description": "Suspense Tax", "AccountCode": 28100, "UnitAmount": row['UnitAmount']*row['taxrate']/100, "UnitAmount": row['UnitAmount']*row['taxrate']/100,
            "Quantity": 1, "TaxType": "NONE"}
            new_invoice_item_df = new_invoice_item_df.append(value, ignore_index=True)
            
        new_invoice_item_df = new_invoice_item_df.astype(object).replace([np.nan], "")

        
    return new_invoice_item_df

def formatSFDateCol(col):
    '''
    * Create a date string from given date from salesforce in the format "2020-02-28T00:00:00" to import in xero
    * col: The date columns in the dataframe
    '''
    col = pd.to_datetime(col)
    col = col + pd.DateOffset(hours=8) # Adding 8 hours to reflect Singapore timezone
    col = col.dt.strftime('%Y-%m-%dT00:00:00')
    return col

def addSFAccountCode(row):
    
    productName = "" if pd.isnull(row['Description']) else row['Description']
    
    if "Subscription" in productName:
        row['AccountCode'] = 23100
    elif "Services" in productName:
        row['AccountCode'] = 23200
    elif "Hardware" in productName:
        row['AccountCode'] = 23250
    else:
        row['AccountCode'] = ''
    
    return row

def addLineItemsToInvoice(row, invoice_items_df):
    
    invoice_num = row["InvoiceNumber"]
    items_in_invoice = invoice_items_df[invoice_items_df["InvoiceNumber"] == invoice_num]
    row["LineItems"] = items_in_invoice[['Description','Quantity','UnitAmount','AccountCode','TaxType','DiscountRate','Tracking']]

    return row

def addLineItemsToCN(row, invoice_items_df):
    
    CN_num = row["CreditNoteNumber"]
    items_in_invoice = invoice_items_df[invoice_items_df["CreditNoteNumber"] == CN_num]
    row['CreditNote'] = {"CreditNoteNumber": CN_num, "LineItems": items_in_invoice[['Description','Quantity','UnitAmount','AccountCode','TaxType','DiscountRate','Tracking']]}

    return row

def addLineItemsToIv(row, invoice_items_df):
    
    invoice_num = row["InvoiceNumber"]
    items_in_invoice = invoice_items_df[invoice_items_df["InvoiceNumber"] == invoice_num]
    # row["LineItems"] = items_in_invoice[['Description','Quantity','UnitAmount','AccountCode','TaxType','DiscountRate','Tracking']]
    row['Invoice'] = {"InvoiceID": "5f4f53cd-9bbc-4238-8d4f-e58773e4373f","InvoiceNumber": invoice_num, "LineItems": items_in_invoice[['Description','Quantity','UnitAmount','AccountCode','TaxType','DiscountRate','Tracking']]}

    return row

def addTrackingCol (row):
    '''
    * Add the tracking column for country of the client.  
    * Maximum two tracking items can be added. Format:
    "Tracking": {"Name": "Geography", "Option": "Thailand"},
    '''
    value = {"Name": "Geography", "Option": row['country']}
    row['Tracking'] = [value]
    return row

def addTaxType(row, col):
    '''
    * Map the taxrate with the Tax types - OUTPUT, ZERORATEDOUTPUT or None
    * For negative tax rates, only the withholding tax lineitem has Tax type assigned as NONE in formatLineItem function,
    rest all is ZERORATEDOUTPUT
    * col: taxrate column of invoice table
    '''
    if row[col]== 7:
        row['TaxType'] = 'OUTPUT'
    elif row[col]== 0:
        row['TaxType'] = 'ZERORATEDOUTPUT'
    elif row[col]< 0:
        row['TaxType'] = 'ZERORATEDOUTPUT'
    else:
        row['TaxType'] = 'ZERORATEDOUTPUT'
    return row

##for invoices
def addContactCol (row):
    '''
    * Return the contact for the Xero - in case of resller, the contact is reseller else it is the client.  
    * Only adding the compulsory field "Name", options to add more info if required. Format:
    "Contact": {"Name": "VR Digital Company Limited"},
    '''
    contact = row['contactname']
    bill = row['billto']
    override = row['OverrideBillTo__c']
    if override == False:
        row['Contact'] = {"Name": contact}
    else:
        row['Contact'] = {"Name": bill}
    return row

def format_sf_invoice_df(sf_invoice_df):
    
    sf_invoice_df['clientname'] = sf_invoice_df['Account__r'].map(op.itemgetter('Name'))
    sf_invoice_df['billto'] = sf_invoice_df['BillTo__r'].map(op.itemgetter('Name'), na_action='ignore')
    sf_invoice_df['resellername'] = sf_invoice_df['Reseller__r'].map(op.itemgetter('Name'))
    sf_invoice_df['contactname'] = sf_invoice_df.apply(lambda x: x['resellername'] if x['resellername']!="Novade SG" else x['clientname'], axis=1)
    sf_invoice_df['Reference'] = sf_invoice_df.apply(lambda x: x['resellername'] if x['resellername']=="Novade SG" else x['clientname'], axis=1)

    #add contact col
    sf_invoice_df = sf_invoice_df.apply(lambda x: addContactCol(x), axis = 1)
    
    #rename columns
    sf_invoice_df = sf_invoice_df.rename(columns={'Invoice_Name__c': 'InvoiceNumber', 'Sent_Date__c': 'Date', 'Due_Date__c': 'DueDate', 'CurrencyIsoCode': 'CurrencyCode'})
    
    #drop attributes column
    sf_invoice_df = sf_invoice_df.drop(['attributes', 'Account__r', 'BillTo__r','Reseller__r', 'resellername', 'clientname', 'contactname', 'billto', 'OverrideBillTo__c'], axis = 1)
    #set static attributes
    sf_invoice_df['Type'] = 'ACCREC'
    sf_invoice_df['Status'] = 'AUTHORISED'    
    sf_invoice_df['LineAmountTypes'] = 'Exclusive'
    # sf_invoice_df['CurrencyRate'] = 1
    
    #format dates
    sf_invoice_df['Date'] = formatSFDateCol(sf_invoice_df['Date'])
    sf_invoice_df['DueDate'] = formatSFDateCol(sf_invoice_df['DueDate'])

    return sf_invoice_df


def format_sf_invoice_with_items_df(sf_invoice_df, sf_invoice_items_df):

    #columns needed to handle ADS invoices
    sf_invoice_items_df['OrderAmount'] = sf_invoice_items_df['Invoice__r'].map(op.itemgetter('Order__r')).map(op.itemgetter('Order_Amount_Tax_Exc__c'))
    sf_invoice_items_df['SpecialTaxCriteria'] = sf_invoice_items_df['Invoice__r'].map(op.itemgetter('Special_Tax_Criteria__c'))

    #set country for tracking
    sf_invoice_items_df['country'] = sf_invoice_items_df['Invoice__r'].map(op.itemgetter('Account__r')).map(op.itemgetter('BillingCountry'))
    sf_invoice_items_df = sf_invoice_items_df.apply(lambda x: addTrackingCol(x), axis = 1)

    #set invoice number
    sf_invoice_items_df['InvoiceNumber'] = sf_invoice_items_df['Invoice__r'].map(op.itemgetter('Invoice_Name__c'))
    
    #set tax rate to be used to add tax type
    sf_invoice_items_df['taxrate'] = sf_invoice_items_df['Invoice__r'].map(op.itemgetter('Tax_Rate__c'))
    sf_invoice_items_df = sf_invoice_items_df.apply(lambda x: addTaxType(x, 'taxrate'), axis = 1)

    #set discountrate to be 0(discount is already reflected in amount from SF)
    sf_invoice_items_df['DiscountRate']  = 0

    #rename columns accordingly
    sf_invoice_items_df = sf_invoice_items_df.rename(columns={'Order_Product_Name__c': 'Description', 'Total_Amount__c':'LineAmount'})
    sf_invoice_items_df["UnitAmount"] = sf_invoice_items_df["LineAmount"]
    
    #set description, account code -> hardcoded based on order product name, total amount (unit amount), quantity = 1
    sf_invoice_items_df['Quantity'] = 1
    sf_invoice_items_df = sf_invoice_items_df.apply(lambda x: addSFAccountCode(x), axis = 1)
    
    sf_invoice_items_df = addWithHoldingTaxItems_SfInvoiceItemDF(sf_invoice_items_df)
    sf_invoice_items_df = addADS_SFInvoiceItemsDF(sf_invoice_items_df)
    
    ###sf dataframe should not contain none values, in this case we dropna.
    sf_invoice_items_df = sf_invoice_items_df[sf_invoice_items_df["Description"].notna()]
    
    #drop unwanted columns
    sf_invoice_items_df = sf_invoice_items_df.drop(['attributes', 'Invoice__r', 'taxrate', 'country', 'SpecialTaxCriteria'], axis = 1)
    
    #convert all amount values to string.
    #convert all float types to str
    sf_invoice_items_df['LineAmount'] = sf_invoice_items_df['LineAmount'].astype(str)
    sf_invoice_items_df['OrderAmount'] = sf_invoice_items_df['OrderAmount'].astype(str)
    sf_invoice_items_df['DiscountRate'] = sf_invoice_items_df['DiscountRate'].astype(str)
    sf_invoice_items_df['Quantity'] = sf_invoice_items_df['Quantity'].astype(str)
    sf_invoice_items_df['UnitAmount'] = sf_invoice_items_df['UnitAmount'].astype(str)
    sf_invoice_items_df['AccountCode'] = sf_invoice_items_df['AccountCode'].astype(str)

        
    #add the line items to the respective invoice
    sf_invoice_df = sf_invoice_df.apply(lambda x: addLineItemsToInvoice(x, sf_invoice_items_df), axis = 1)
    
    return sf_invoice_df

def format_sf_CN_df(sf_invoice_df):
    
    sf_invoice_df['clientname'] = sf_invoice_df['Invoice__r'].map(op.itemgetter('Account__r')).map(op.itemgetter('Name'))
    sf_invoice_df['Invoice_Name__c'] = sf_invoice_df['Invoice__r'].map(op.itemgetter('Invoice_Name__c'))
    sf_invoice_df['resellername'] = sf_invoice_df['Invoice__r'].map(op.itemgetter('Reseller__r')).map(op.itemgetter('Name'))
    sf_invoice_df['contactname'] = sf_invoice_df.apply(lambda x: x['resellername'] if x['resellername']!="Novade SG" else x['clientname'], axis=1)
    sf_invoice_df['Reference'] = sf_invoice_df.apply(lambda x: x['resellername'] if x['resellername']=="Novade SG" else x['clientname'], axis=1)

    #add contact col
    sf_invoice_df = sf_invoice_df.apply(lambda x: addContactCol(x), axis = 1)
    
    #rename columns
    sf_invoice_df = sf_invoice_df.rename(columns={'Name__c':'CreditNoteNumber','Invoice_Name__c': 'InvoiceNumber', 'Issued_Date__c': 'Date', 'CurrencyIsoCode': 'CurrencyCode',
                                                    'Amount_Tax_Exc__c':'Amount'})
    
    #drop attributes column
    sf_invoice_df = sf_invoice_df.drop(['attributes', 'Invoice__r', 'resellername', 'clientname', 'contactname'], axis = 1)
    #set static attributes
    sf_invoice_df['Type'] = 'ACCRECCREDIT'
    sf_invoice_df['Status'] = 'AUTHORISED'    
    sf_invoice_df['LineAmountTypes'] = 'Exclusive'
    # sf_invoice_df['CurrencyRate'] = 1
    
    #format dates
    sf_invoice_df['Date'] = formatSFDateCol(sf_invoice_df['Date'])

    return sf_invoice_df

def format_sf_CN_with_items_df(sf_invoice_df, sf_invoice_items_df):

    #columns needed to handle ADS invoices
    sf_invoice_items_df['OrderAmount'] = sf_invoice_items_df['Order__r'].map(op.itemgetter('Order_Amount_Tax_Exc__c'))
    sf_invoice_items_df['SpecialTaxCriteria'] = sf_invoice_items_df['Credit_Note__r'].map(op.itemgetter('Special_Tax_Criteria__c'))

    #set country for tracking
    sf_invoice_items_df['country'] = sf_invoice_items_df['Credit_Note__r'].map(op.itemgetter('Invoice__r')).map(op.itemgetter('Account__r')).map(op.itemgetter('BillingCountry'))
    sf_invoice_items_df = sf_invoice_items_df.apply(lambda x: addTrackingCol(x), axis = 1)

    #set invoice number
    sf_invoice_items_df['InvoiceNumber'] = sf_invoice_items_df['Credit_Note__r'].map(op.itemgetter('Invoice__r')).map(op.itemgetter('Invoice_Name__c'))
    
    #set tax rate to be used to add tax type
    sf_invoice_items_df['taxrate'] = sf_invoice_items_df['Credit_Note__r'].map(op.itemgetter('Tax_Rate__c'))
    sf_invoice_items_df = sf_invoice_items_df.apply(lambda x: addTaxType(x, 'taxrate'), axis = 1)

    #set CreditNote Number
    sf_invoice_items_df['CreditNoteNumber'] = sf_invoice_items_df['Credit_Note__r'].map(op.itemgetter('Name__c'))

    #set discountrate to be 0(discount is already reflected in amount from SF)
    sf_invoice_items_df['DiscountRate']  = 0

    #rename columns accordingly
    sf_invoice_items_df = sf_invoice_items_df.rename(columns={'Order_Product_Name__c': 'Description', 'Total_Amount__c':'LineAmount'})
    sf_invoice_items_df["UnitAmount"] = sf_invoice_items_df["LineAmount"]
    
    #set description, account code -> hardcoded based on order product name, total amount (unit amount), quantity = 1
    sf_invoice_items_df['Quantity'] = 1
    sf_invoice_items_df = sf_invoice_items_df.apply(lambda x: addSFAccountCode(x), axis = 1)
    
    sf_invoice_items_df = addWithHoldingTaxItems_SfInvoiceItemDF(sf_invoice_items_df)
    sf_invoice_items_df = addADS_SFInvoiceItemsDF(sf_invoice_items_df)
    
    ###sf dataframe should not contain none values, in this case we dropna.
    sf_invoice_items_df = sf_invoice_items_df[sf_invoice_items_df["Description"].notna()]
    
    #drop unwanted columns
    sf_invoice_items_df = sf_invoice_items_df.drop(['attributes', 'Order__r','Credit_Note__r','taxrate', 'country', 'SpecialTaxCriteria'], axis = 1)
    
    #convert all amount values to string.
    #convert all float types to str
    sf_invoice_items_df['LineAmount'] = sf_invoice_items_df['LineAmount'].astype(str)
    sf_invoice_items_df['OrderAmount'] = sf_invoice_items_df['OrderAmount'].astype(str)
    sf_invoice_items_df['DiscountRate'] = sf_invoice_items_df['DiscountRate'].astype(str)
    sf_invoice_items_df['Quantity'] = sf_invoice_items_df['Quantity'].astype(str)
    sf_invoice_items_df['UnitAmount'] = sf_invoice_items_df['UnitAmount'].astype(str)
    sf_invoice_items_df['AccountCode'] = sf_invoice_items_df['AccountCode'].astype(str)

    #add the line items to the respective invoice
    sf_invoice_df = sf_invoice_df.apply(lambda x: addLineItemsToInvoice(x, sf_invoice_items_df), axis = 1)

    # #add the line items to the respective Credit Note
    sf_invoice_df = sf_invoice_df.apply(lambda x: addLineItemsToIv(x, sf_invoice_items_df), axis = 1)

    #add the line items to the respective Credit Note
    sf_invoice_df = sf_invoice_df.apply(lambda x: addLineItemsToCN(x, sf_invoice_items_df), axis = 1)

    return sf_invoice_df