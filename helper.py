import re
from urllib.parse import quote_plus,quote
from urllib.request import urlopen
from bs4 import BeautifulSoup
import requests
import pandas as pd
import dateparser
from boto3_helper import get_mspp_covid_data
import timeit
from db import get_posgres_connection , is_table_exist
import os

import logging
from botocore.exceptions import ClientError

import boto3

def download_file(download_url,download_name="document.pdf"):
    response = requests.get(download_url)
    file = open(download_name, 'wb')
    file.write(response.content)
    file.close()
    print("Completed")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_pdf_file_links(mpspp_page='https://mspp.gouv.ht/newsite/documentation.php'):
    web =  requests.get(mpspp_page)
    web_data = web.text

    soup = BeautifulSoup(web_data,'lxml')

    pdf_links = []
    for a_tag in soup.findAll('a'):
        link = a_tag.get('href')
        if link:
            if re.match(r'^.*\.pdf$',link):
                quote_href = quote(link[2:])
                # url_path = quote_href
                url_path = link

                # Get Tag Description
                # tag_contents = a_tag.contents[0]
                if a_tag.parent.name == "p":
                    card = a_tag.parent.parent
                    tag_contents = card.contents[3].text
                # Check if the contents is related to COVID
                    if re.match(r'.*(la surveillance du nouveau Coronavirus).*',tag_contents):
                        file_date = re.match(r'^Bulletin du (\d*\w?\w? \w* \d{4}).*',tag_contents).groups(1)[0]
                        pdf_links.append((url_path,tag_contents,file_date))
        else:
            pass

    return pdf_links

def get_all_mspp_pdf_file_links():
    mspp_pages = []
    for page_num in range(6):
        mspp_documentation_page_template = f"https://mspp.gouv.ht/documentation.php?start={page_num}&categorie=10"
        mspp_pages.append(mspp_documentation_page_template)

    mspp_pdfs = []

    for mspp_page in mspp_pages:
        mspp_pdfs += get_pdf_file_links(mspp_page)

    mspp_df = pd.DataFrame(mspp_pdfs)
    mspp_df.columns = ['url','document_description','document_date']
    mspp_df['document_date'] = mspp_df['document_date'].str.replace('1er','1')
    mspp_df['document_date'] = mspp_df['document_date'].apply(lambda x: dateparser.parse(x))
    mspp_df = mspp_df.sort_values(by=["document_date"])
    if( is_table_exist('mspp_covid19_links') ):
        df = pd.read_sql_table('mspp_covid19_links',con=get_posgres_connection(), schema='public')
        start_date = df['document_date'].max()
        mspp_df =mspp_df.loc[mspp_df['document_date']>start_date,:]

    return mspp_df


def get_mspp_data(mspp_df):
    s3BucketName=os.getenv("AWS_S3_BUCKET")
    # Start the iteration at 12 since I couldn't parse 11
    for index, data in mspp_df.iterrows():
        __mspp_df = mspp_df.loc[[index],:]
        file_date = data['document_date'].date()
        print("starting to load ",file_date)
        local_file = f'MSPP_COVID19_data_{file_date}.pdf'
        document_name = f"public-data/mspp/covid19-updates/{local_file}"
        download_file(data['url'],local_file)
        __mspp_df['local_file'] = local_file
        upload_file(local_file, s3BucketName, document_name)
        __mspp_df['document_name'] = document_name
        __mspp_df['bucket_name'] = s3BucketName
        __mspp_df['message'] = None
        try:
            start_time = timeit.default_timer()
            mspp_data = get_mspp_covid_data(s3BucketName,document_name)
            elapsed = timeit.default_timer() - start_time
            print('Function "{name}" took {time} seconds to complete.'.format(name=file_date, time=elapsed))
            if isinstance(mspp_data, pd.DataFrame):
                if(mspp_data.empty):
                    __mspp_df["message"]="The first table results are not compatible"
                mspp_data['document_date'] = data['document_date']
                mspp_data.to_sql("mspp_covid19_cases",index=False,schema='public',con=get_posgres_connection(),if_exists='append')
                __mspp_df.to_sql("mspp_covid19_links",index=False,schema='public',con=get_posgres_connection(),if_exists='append')
            elif ("message" in mspp_data):
                __mspp_df["message"]=mspp_data["message"]
                __mspp_df.to_sql("mspp_covid19_links",index=False,schema='public',con=get_posgres_connection(),if_exists='append')
            else:
                print(data['document_date']," was not loaded")
        except Exception as e :
            if hasattr(e, 'message'):
                print("message", e.message)
                __mspp_df["message"]=e.message
            else:
                print("e" ,e)
                __mspp_df["message"]=str(e)
            print(file_date," was not loaded")
            __mspp_df.to_sql("mspp_covid19_links",index=False,schema='public',con=get_posgres_connection(),if_exists='append')
            pass
