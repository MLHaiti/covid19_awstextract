import unittest
from boto3_helper import get_mspp_covid_data
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()


class TestBoto3Helper(unittest.TestCase):
    def test_get_mspp_covid_data(self):
        file_date="2020-05-10"
        local_file = f'MSPP_COVID19_data_{file_date}.pdf'
        document_name = f"public-data/mspp/covid19-updates/{local_file}"
        s3BucketName = os.getenv("AWS_S3_BUCKET")
        df = get_mspp_covid_data(s3BucketName,document_name)
        print(df)
        self.assertTrue(isinstance(df, pd.DataFrame))

if __name__ == '__main__':
    unittest.main()