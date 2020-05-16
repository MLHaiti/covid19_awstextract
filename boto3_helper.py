import boto3
import time
import pandas as pd
from datetime import datetime
from io import StringIO
from trp import Document, Cell, Table
import json


def startJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_analysis(
    DocumentLocation={
        'S3Object': {
            'Bucket': s3BucketName,
            'Name': objectName
        },},
     FeatureTypes=["TABLES"])

    return response["JobId"]

def isJobComplete(jobId):
    # For production use cases, use SNS based notification
    # Details at: https://docs.aws.amazon.com/textract/latest/dg/api-async.html
    time.sleep(5)
    client = boto3.client('textract',)
    response = client.get_document_analysis(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_analysis(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def getJobResults(jobId):

    pages = []

    client = boto3.client('textract')
    response = client.get_document_analysis(JobId=jobId)
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):

        response = client.get_document_analysis(JobId=jobId, NextToken=nextToken)

        pages.append(response)
        print("Resultset page recieved: {}".format(len(pages)))
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']
    # print(pages)
    # f = open("test_{}.json".format(jobId), "w")
    # f.write(json.loads(pages))
    # f.close()
    return pages

def generate_table_csv(table_result, blocks_map, table_index):
    #rows = Table(table_result,blocks_map).rows
    rows = get_rows_columns_map(table_result, blocks_map)

    csv = ""
    for row_index, cols in rows.items():

        for col_index, text in cols.items():
            csv += '{}'.format(text) + ","
        csv += '\n'

    csv += '\n\n\n'
    return csv

def get_rows_columns_map(table_result, blocks_map):
    rows = {}
    if('Relationships' in table_result and table_result['Relationships']):
        for relationship in table_result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    cell = blocks_map[child_id]
                    if cell['BlockType'] == 'CELL':
                        row_index = cell['RowIndex']
                        col_index = cell['ColumnIndex']
                        if row_index not in rows:
                            # create new row
                            rows[row_index] = {}

                        # get the text value
                        rows[row_index][col_index] = get_text(cell, blocks_map)
    return rows


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] =='SELECTED':
                            text +=  'X '
    return text


def get_table_responses(response):
    response_item = response

    # Get the text blocks
    blocks=response_item['Blocks']
    #pprint.pprint(blocks)

    blocks_map = {}
    table_blocks = []
    for block in blocks:
        blocks_map[block['Id']] = block
        if block['BlockType'] == "TABLE":
            table_blocks.append(block)

    if len(table_blocks) <= 0:
        print("<b> NO Table FOUND </b>")

    csvs = []
    for index, table in enumerate(table_blocks):
        csvs.append(generate_table_csv(table, blocks_map, index +1))
        #csv += '\n\n'

    return csvs
def generate_csv_from_table(table):
    csv = ""
    for r, row in enumerate(table.rows):
        for c, cell in enumerate(row.cells):
            #Replace , by '' because 1026 is written like 1,026
            #That can cause a problem to csv file
            csv += '{}'.format(cell.text.replace(',','')) + ","
        csv += '\n'
    csv += '\n\n\n'
    return csv

def get_tables_from_pdf(s3BucketName,documentName):
    jobId = startJob(s3BucketName, documentName)
    print("Started job with id: {}".format(jobId))
    if(isJobComplete(jobId)):
        response = getJobResults(jobId)
    doc = Document(response)
    csv_tables =[]
    for page in doc.pages:
        for table in page.tables:
            csv_tables.append(generate_csv_from_table(table))
    #tables = get_table_responses(response[0]) # Get first item in response
    return csv_tables

from datetime import date
import re

def find_column(col,regex):
    if re.match(regex,col) and (re.match(regex,col)!=None):
        match_col = re.match(regex,col).groups(1)[0]
    else:
        match_col = None
    return match_col


def get_column_name(cols,regex):
    col_list = [find_column(col,regex) for col in cols]
    match_index = [key for key, val in enumerate(col_list) if val !=None]
    if len(match_index)>0:
        print("match index",match_index)
        match_index = match_index[0]
        column_name = col_list[match_index]
        return column_name
    else:
        return None

def get_mspp_covid_data(s3BucketName,documentName):
    # Get the data from the mspp pdf
    tables = get_tables_from_pdf(s3BucketName=s3BucketName,documentName=documentName)
    # Read in the string as a CSV
    df = pd.read_csv(StringIO(tables[0]))
    # Subselect the desired columns
    # Find the columns with the matching names
    try:
        dept_col  = get_column_name(df.columns.values,r'(.*Departem.*)')
        suspect_col = get_column_name(df.columns.values,r'(.*suspect.*)')
        confirme_col =get_column_name(df.columns.values,r'(.*Confirmes.*|.*Cas Cumules.*)')
        deces_col = get_column_name(df.columns.values,r'(.*Deces.*)')
        letalite_col = get_column_name(df.columns.values,r'(.*letalite.*)')
        columns =[]
        str_columns =[]
        missing_columns =[]
        col_search = {'departement':dept_col,'cas_suspects':suspect_col,'cas_confirmes':confirme_col,'deces':deces_col,'taux_de_letalite':letalite_col}
        for col_str, _val in col_search.items():
            if _val!=None:
                columns.append(_val)
                str_columns.append(col_str)
            else:
                missing_columns.append(col_str)

        print("columns", columns)
        print("columns srt", str_columns)
        print("missign columns srt", missing_columns)


        # df = df.loc[:,[dept_col,suspect_col,confirme_col,deces_col,letalite_col]]
        # df.columns = ['departement','cas_suspects','cas_confirmes','deces','taux_de_letalite']
        df = df.loc[:,columns]
        df.columns = str_columns

        # remove null rows
        df.dropna(inplace=True)
        # print("extracted_df" , df)
        # Clean the data
        #df['date'] = data_date
        # replace 'O' with 0 and cast column to an int
        for el in missing_columns:
            df[el]=None
        if df['cas_confirmes'].dtype == object:
            df['cas_confirmes'] = pd.to_numeric(df['cas_confirmes'].str.replace('O','0'), errors="coerce")
            #remove null row
            df = df[df["cas_confirmes"].notnull()]
            df["cas_confirmes"]=df["cas_confirmes"].astype(int)

        # replace 'O' with 0 and cast column to an int
        if df['deces'].dtype == object:
            df['deces'] = df['deces'].str.replace('O','0').astype(int)

        # Remove the '%' and fill empty values with 0 then convert to a float
        # We divide by 100 in the end to convert it to the percentage
        #print("letalite",df['taux_de_letalite'].notnull().sum())
        if df['taux_de_letalite'].dtype == object:
            df['taux_de_letalite'] = df['taux_de_letalite'].str.extract(r'([\d\w\.]*)%?').replace('O','0').replace('','0').astype(float)/100

        #df[['cas_confirmes','deces','taux_de_letalite']] = pd.to_numeric(df[['cas_confirmes','deces','taux_de_letalite']], errors="coerce")
        return df
    except Exception as e :
        if hasattr(e, 'message'):
            print("message", e.message)
        else:
            print("e" ,e)
        print("The following document was not loaded correctly:",documentName)
        return None

# s3BucketName='mlhaiti-data'
# documentName='Sitrep COVID 19 10 05 2020.pdf'
# df = get_mspp_covid_data(s3BucketName,documentName)