from helper import get_all_mspp_pdf_file_links , get_mspp_data
from db import get_posgres_connection

con = get_posgres_connection()


mspp_df= get_all_mspp_pdf_file_links()

df = get_mspp_data(mspp_df)
