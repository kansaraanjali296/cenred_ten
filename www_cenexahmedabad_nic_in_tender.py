import os
import sqlite3
import warnings
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import logging
import pyodbc
import re

from webdriver_manager.chrome import ChromeDriverManager

warnings.simplefilter("ignore")
driver = webdriver.Chrome(ChromeDriverManager().install())


def generate_file_name(bstdc_url):
    try:
        #----------------------------------------------url get file_name -------------------------------------------------#
        file_name = bstdc_url.split('https://')[1].split("/")[0].replace(".", "_") + '_tender'
        return file_name
    except Exception as e:
        print(e)


def create_db(file_name):
    try:
        #----------------------------------------------database created using sqlite -------------------------------------------------#
        conn = sqlite3.connect(file_name + '.db')
        Cursor = conn.cursor()

        create_db_query = """CREATE TABLE www_cenexahmedabad_nic_in_tender(Id INTEGER PRIMARY KEY,
                                                                            Tender_Notice_No TEXT,
                                                                            Tender_Summery TEXT,
                                                                            Tender_Details TEXT,
                                                                            Bid_deadline_2 TEXT,
                                                                            Documents_2 TEXT,
                                                                            TenderListing_key TEXT,
                                                                            Notice_Type TEXT,
                                                                            Competition TEXT,
                                                                            Purchaser_Name TEXT,
                                                                            Pur_Add TEXT,
                                                                            Pur_State TEXT,
                                                                            Pur_City TEXT,
                                                                            Pur_Country TEXT,
                                                                            Pur_Email TEXT,
                                                                            Pur_URL TEXT,
                                                                            Bid_Deadline_1 TEXT,
                                                                            Financier_Name TEXT,
                                                                            CPV TEXT,
                                                                            scannedImage TEXT,
                                                                            Documents_1 TEXT,
                                                                            Documents_3 TEXT,
                                                                            Documents_4 TEXT,
                                                                            Documents_5 TEXT,
                                                                            currency TEXT,
                                                                            actualvalue TEXT,
                                                                            TenderFor TEXT,
                                                                            TenderType TEXT,
                                                                            SiteName TEXT,
                                                                            createdOn TEXT,
                                                                            updateOn TEXT,
                                                                            Content TEXT,
                                                                            Content1 TEXT,
                                                                            Content2 TEXT,
                                                                            Content3 TEXT,
                                                                            DocFees TEXT,
                                                                            EMD TEXT,
                                                                            OpeningDate TEXT,
                                                                            Tender_No TEXT,
                                                                            Flag TEXT)"""

        #----------------------------------------------check table exists or not -------------------------------------------------#
        tb_exists = "SELECT name FROM sqlite_master WHERE type='table' AND name='www_cenexahmedabad_nic_in_tender'"
        if not Cursor.execute(tb_exists).fetchone():
            Cursor.execute(create_db_query)
        # else:
        #     print('Table Already Exists! Now, You Can Insert Date.')
        return conn

    except Exception as e:
        print(e)


def download_pdf(links, count):
    
    #----------------------------------------------download pdf-------------------------------------------------#
    response = requests.get(links, verify=False)    #------ verify-use openssl's error 
    fullname = os.path.join(files_dir, datetime.now().strftime(f"%d%m%Y_%H%M%S%f") + "." + links.rsplit('.', 1)[-1])
    if response.status_code == 404 or response.status_code == 400:
        fullname = None
    else:
        count = count + 1 
        pdf = open(fullname, 'wb')
        pdf.write(response.content)
        pdf.close()
        logging.info("File Downloaded")
    
        print(count)
        print("------------------------------------------------------------------------")

    return fullname


def sqlite_and_sql_server_db(Page_Data):
    try:
        #----------------------------------------------insert data in sqlite table -------------------------------------------------#
        que = "INSERT INTO www_cenexahmedabad_nic_in_tender(Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2, Flag) VALUES (?, ?, ?, ?, 1)"
        conn.executemany(que, Page_Data)
        conn.commit()
        logging.info('SQLite Data Inserted')

        #----------------------------------------------select sqlite data for insert in sql server -------------------------------------------------#
        sqlite_rows = conn.execute("SELECT Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2 FROM www_cenexahmedabad_nic_in_tender WHERE Flag = 1").fetchall()
        if len(sqlite_rows) > 0:
            sql_conn = pyodbc.connect('Driver={SQL Server};'
                                      'Server=192.168.100.153;'
                                      'Database=CrawlingDB;'
                                      'UID=anjali;'
                                      'PWD=anjali@123;')
            sql_cursor = sql_conn.cursor()
            sql_cursor.execute("""IF NOT EXISTS (SELECT name FROM sysobjects WHERE name='www_cenexahmedabad_nic_in_tender' AND xtype='U') CREATE TABLE www_cenexahmedabad_nic_in_tender (id int IDENTITY(1,1) PRIMARY KEY, 
                                                                                                                                                                                        Tender_Notice_No TEXT,                                                                                                                                
                                                                                                                                                                                        Tender_Summery TEXT,
                                                                                                                                                                                        Tender_Details TEXT,
                                                                                                                                                                                        Bid_deadline_2 TEXT,
                                                                                                                                                                                        Documents_2 TEXT,
                                                                                                                                                                                        TenderListing_key TEXT,
                                                                                                                                                                                        Notice_Type TEXT,
                                                                                                                                                                                        Competition TEXT,
                                                                                                                                                                                        Purchaser_Name TEXT,
                                                                                                                                                                                        Pur_Add TEXT,
                                                                                                                                                                                        Pur_State TEXT,
                                                                                                                                                                                        Pur_City TEXT,
                                                                                                                                                                                        Pur_Country TEXT,
                                                                                                                                                                                        Pur_Email TEXT,
                                                                                                                                                                                        Pur_URL TEXT,
                                                                                                                                                                                        Bid_Deadline_1 TEXT,
                                                                                                                                                                                        Financier_Name TEXT,
                                                                                                                                                                                        CPV TEXT,
                                                                                                                                                                                        scannedImage TEXT,
                                                                                                                                                                                        Documents_1 TEXT,
                                                                                                                                                                                        Documents_3 TEXT,
                                                                                                                                                                                        Documents_4 TEXT,
                                                                                                                                                                                        Documents_5 TEXT,
                                                                                                                                                                                        currency TEXT,
                                                                                                                                                                                        actualvalue TEXT,
                                                                                                                                                                                        TenderFor TEXT,
                                                                                                                                                                                        TenderType TEXT,
                                                                                                                                                                                        SiteName TEXT,
                                                                                                                                                                                        createdOn TEXT,
                                                                                                                                                                                        updateOn TEXT,
                                                                                                                                                                                        Content TEXT,
                                                                                                                                                                                        Content1 TEXT,
                                                                                                                                                                                        Content2 TEXT,
                                                                                                                                                                                        Content3 TEXT,
                                                                                                                                                                                        DocFees TEXT,
                                                                                                                                                                                        EMD TEXT,
                                                                                                                                                                                        OpeningDate TEXT,
                                                                                                                                                                                        Tender_No TEXT)""")

            #----------------------------------------------insert data in sql server db -------------------------------------------------#
            que1 = "INSERT INTO www_cenexahmedabad_nic_in_tender(Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2) VALUES (?, ?, ?, ?)"
            sql_cursor.executemany(que1, sqlite_rows)
            sql_conn.commit()
            logging.info('SQL Server Data Inserted')
            sql_conn.close()

            #----------------------------------------------update sqlite db for flag 1-------------------------------------------------#
            conn.execute("UPDATE www_cenexahmedabad_nic_in_tender SET Flag = 0 WHERE Flag = 1")
            conn.commit()

    except Exception as e:
        print(e)


try:

    bstdc_url = 'https://www.cenexahmedabad.nic.in/tender.html'
    driver.get(bstdc_url)

    file_name = generate_file_name(bstdc_url)
    conn = create_db(file_name)

    logging.basicConfig(filename=file_name + '.log', filemode='a', level=logging.DEBUG, format='%(asctime)s %(message)s')
    logging.info('Started Web Scraping')
    logging.info('Program Start')

    files_dir = os.path.expanduser('~') + "\\Documents\\" + "PythonFile\\" + bstdc_url.split('https://')[1].split("/")[0] + "py_tender\\" + "File"
    if os.path.exists(files_dir):
        pass
    else:
        os.makedirs(files_dir)

    tr_all_data = driver.find_elements(By.XPATH, value='//*[@class="contecttable"]//tbody//tr')

    count = 0
    Page_Data = []
    del tr_all_data[0]
    for tr_data in tr_all_data:

        #---------------------------------------------- check blank row in table -------------------------------------------------#
        if (tr_data.find_element(By.XPATH, value='.//td[1]').text.replace("&nbsp;", "").replace(" ", "") == ""):
            continue
        
        sr_no = tr_data.find_element(By.XPATH, value='./td[1]').text.strip()
        print(sr_no)

        try:
            links = tr_data.find_element(By.XPATH, value='.//td[2]/a').get_attribute('href')
            if '../docs/tender/appoint auction.PDF' in links:
                links = ''
        except:
            links = ''
        
        Tender_Summery = tr_data.find_element(By.XPATH, value='./td[2]').text.strip()
        logging.info(f'{Tender_Summery}')

        OpeningDate = tr_data.find_element(By.XPATH, value='./td[3]').text.replace(".", "/").strip()

        Bid_deadline_2 = tr_data.find_element(By.XPATH, value='./td[4]').text.replace(".", "/").strip()

        # ---------------------------------------------- dublicate checking -------------------------------------------------#
        row_exists = "SELECT * FROM www_cenexahmedabad_nic_in_tender WHERE Tender_Summery='" + Tender_Summery + "' AND OpeningDate='" + OpeningDate + "' AND Bid_deadline_2='" + Bid_deadline_2 + "'"
        if not conn.execute(row_exists).fetchone():
            logging.info("Fresh")

            try:
                Documents_2 = download_pdf(links, count)
            except Exception as e:
                print(e)
                Documents_2 = ''

            Row_data = [Tender_Summery, OpeningDate, Bid_deadline_2, Documents_2]
            Page_Data.append(Row_data)

        else:
            print("This Tender_Summery, OpeningDate, Bid_deadline_2 is already exist.")
            logging.info("Duplicated")

    sqlite_and_sql_server_db(Page_Data)

    conn.close()
    driver.close()
    logging.info('Proceed Successfully Done')
    print("Completed")

except Exception as e:
    print(e)

