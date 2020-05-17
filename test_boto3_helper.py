import unittest
from boto3_helper import get_mspp_covid_data
from helper import get_document_name
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


class TestBoto3Helper(unittest.TestCase):
    def setUp(self):
        self.s3BucketName=os.getenv("AWS_S3_BUCKET")

    def test_get_mspp_covid_data(self):
        file_date1="2020-04-25"
        file_date2="2020-05-05"

        document_name1 = get_document_name(file_date1)
        document_name2 = get_document_name(file_date2)
        print(document_name2)
        df1 = get_mspp_covid_data(self.s3BucketName,document_name1)
        print("df1",df1)
        df2 = get_mspp_covid_data(self.s3BucketName,document_name2)
        print("df2",df2)
        self.assertFalse(isinstance(df1, pd.DataFrame))
        self.assertTrue(isinstance(df2, pd.DataFrame))

if __name__ == '__main__':
    unittest.main()