import re
from urllib.parse import quote_plus,quote
from bs4 import BeautifulSoup
import requests
import pandas as pd
import dateparser


def download_file(download_url,download_name="document.pdf"):
    response = urlopen(download_url)
    file = open(download_name, 'wb')
    file.write(response.read())
    file.close()
    print("Completed")

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
                url_path = "https://mspp.gouv.ht" + quote_href

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
    mspp_pages = ['https://mspp.gouv.ht/documentation.php']
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
    return mspp_df