import scrapy
import pandas as pd
import datetime
import re
import json
from cleantext import clean
from urllib.parse import urlparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, defer
from google.cloud import storage
import json
import os
import sys

#Directory of this python code
BASE_DIR = os.path.dirname(os.path.abspath('__file__'))

#Directories of the input,Exception and output folder through OS
input_dir = os.path.join(BASE_DIR + "\\Input")
error_dir = os.path.join(BASE_DIR + "\\Exception")
output_dir = os.path.join(BASE_DIR + "\\OutputData")
No_filter_dir = os.path.join(BASE_DIR+'\\NoFilteredData')


#Base seller URL and Product URL
main_url_seller = "https://www.ajio.com"
main_url = "https://www.ajio.com/p/"


#Global variables to store the data
Glob_Exception_ID = []
Glob_Exception_URL = []
Exception_url = []
Data_without_filtered = []
Main_Data_list =[]
Non_MandatoryID = []
batch = 'Ajio'

#The function gets the response from the requests and extracts all the specified data
def parse(response):
    #this try block will initially clear all the error Id's if there are some

    try: 
        if(len(pd.read_excel(r"{}\\AjioPDP_Exception_ProductID1.xlsx".format(error_dir)))>0):
            pd.DataFrame({"Exception_Id":[]}).to_excel(r"{}\\AjioPDP_Exception_ProductID1.xlsx".format(error_dir))
    except: pass

    #Product ID is captured
    p_id = response.meta['ProductId']
    p_idupdate = p_id.split("/")[4]
    print("-----------------------",p_id,p_idupdate)
    page_source = str(response.body)
    
    # Using String Index and slicing for Json conversion
    start = '''{"wishlist"'''
    end = '''"unRatedProducts":'''
    start_index = page_source.find(start)   
    end_index = page_source.find(end)
    product_data = page_source[start_index:end_index+len(end)]
    product_str_data = product_data+'''""}}'''
    # Reamoving Unwanted Characters from the Json String
    product_str_data = clean(product_str_data, no_emoji=True,lower=False)
    product_str_data = product_str_data.replace("\'","'")
    # Storing Json String as TXT file for Our Reference
    with open('AjioPDP.txt','w') as f:
            f.write(product_str_data)
    try:
        # Converting Json String format to Perfect Json
        json_data = json.loads(product_str_data)
        with open("AjioPDP.txt","w") as f:
            f.write(str(json_data))
        json_main_data = json_data['product']
        json_data = json_data['product']['productDetails']
    
        # Columns Name list for Output Data, if extra column needs to be added we can specify column name here below
        product_col = ['Product ID', 'Brand', 'Title', 'MRP', 'Selling Price', 'Discount', 'Division','Category', 'Sub-Category', 'No of Sizes',
                    'Total Sizes', 'No of Available Sizes', 'Available Sizes', 'No of Non-Available Sizes', 'Non-Available Sizes', 
                    'No. of Offers', 'Offers','Product Details','Fabric', 'No of Colors', 'Total Colors', 'No of Images','Image URL','Seller URL',
                    'In Stock']
        
        # Empty Dict for storing  each page data 
        temp_dict = {}

        temp_dict.update({"Date": datetime.date.today().strftime("%Y-%m-%d")})
        today = datetime.date.today()
        temp_dict.update({"Week": today.isocalendar()[1]})
        temp_dict.update({"Marketplace": urlparse(response.url).netloc.replace('www.','').replace('.com','').title()})
        temp_dict.update({"Product URL": p_id})
        # Looping for get Each columns at a time
        for key in product_col:
            # Global Try Except for Error Handling
            try:
                # These all conditions to get perticular  data from the Json 
                if(key=='Product ID'): val = p_idupdate
                elif(key=='Brand'): val = json_data['categories'][0]['code'].replace('-',' ').title()
                elif(key=='Title'): val = json_data['baseOptions'][0]['options'][0]['modelImage']['altText']
                elif(key=='MRP'): val = json_data['wasPriceData']['value']
                elif(key=='Selling Price'): val = json_data['baseOptions'][0]['options'][0]['priceData']['value']
                elif(key=='Discount'): val = json_data['baseOptions'][0]['options'][0]['priceData']['discountValue']
                elif(key=='Division'): val = json_data['rilfnlBreadCrumbList']['rilfnlBreadCrumb'][0]['name']
                elif(key=='Category'): val = json_data['rilfnlBreadCrumbList']['rilfnlBreadCrumb'][1]['name']
                elif(key=='Sub-Category'): val = json_data['rilfnlBreadCrumbList']['rilfnlBreadCrumb'][2]['name']
                elif(key=='No of Sizes'): val = len([data['scDisplaySize'] for data in json_data['variantOptions']])
                elif(key=='Total Sizes'): val = [[data['scDisplaySize'] for data in json_data['variantOptions']]]
                elif(key=='No of Available Sizes'): 
                    val = []
                    for size in json_data['variantOptions']: 
                        if(size['stock']['stockLevelStatus']=='inStock' or size['stock']['stockLevelStatus']=='lowStock'):
                            val.append(size['scDisplaySize'])
                    val = len(val)
                elif(key=='Available Sizes'):
                    val = []
                    for size in json_data['variantOptions']: 
                        if(size['stock']['stockLevelStatus']=='inStock' or size['stock']['stockLevelStatus']=='lowStock'): 
                            val.append(size['scDisplaySize'])
                    val = [val]
                elif(key=='No of Non-Available Sizes'): 
                    val = []
                    for size in json_data['variantOptions']: 
                        if(size['stock']['stockLevelStatus']=='outOfStock'):
                          val.append(size['scDisplaySize'])
                    val = len(val)
                elif(key=='Non-Available Sizes'):
                    val = []
                    for size in json_data['variantOptions']: 
                        if(size['stock']['stockLevelStatus']=='outOfStock'):
                            val.append(size['scDisplaySize'])
                    val = [val]
                elif(key=='No. of Offers'): val = len([offers for offers in json_data['potentialPromotions']])
                elif(key=='Offers'): 
                    val = {}
                    [val.update({offers['title'].replace('<br>',''):offers['description'][:offers['description'].find('<a')]})for offers in json_data['potentialPromotions']]
                elif(key=='Product Details'):
                    val = {}
                    [val.update({detail['name']: detail['featureValues'][0]['value']}) for detail in json_data['featureData']]
                elif(key=='Fabric'): val = temp_dict['Product Details']['Fabric Detail']
                elif(key=='No of Colors'): val = len([colors['color'] for colors in json_data['baseOptions'][0]['options']])
                elif(key=='Total Colors'): val = [[colors['color'] for colors in json_data['baseOptions'][0]['options']]]
                elif(key=='No of Images'): val = len([image['url'] for image in json_data['images'] if(image['format']=='cartIcon')])
                elif(key=='Image URL'): val = json_data['baseOptions'][0]['options'][0]['modelImage']['url']
                elif(key=='Seller URL'): val = main_url_seller+ json_data['fnlColorVariantData']['categoryUrl']
                elif(key=='In Stock'): 
                    stocks = []
                    for availability in json_data['variantOptions']:
                        stocks.append(availability['stock']['stockLevelStatus'])
                    print("===========",stocks)
                    if(('inStock' in stocks) or ('lowStock' in stocks)):
                        val = 'Yes'
                    else:
                        val = 'No'
                elif(key == 'New Column'):
                    val = json_data['Key we found in backend data']
            #If there is no data for specific column then empty data will be appended
            except: val = ''
            
            # Dynamically Storing Each Key, Value pair and storing to the perticular Dictionary
            temp_dict.update({key: val})
            try: temp_dict['Discount'] = round(temp_dict['Discount'],0)#This convert large floating value to smaller one
            except: pass
            val = ''
        
        # Not present in the page (But these all columns  also required)
        not_available_col = ['Product Rating','Count of Ratings','Count of Reviews','Current_Size','Bestseller Rank',
                                'Rank Detail','Ques','COD','Product Type','Description']
        # Storing value as empty (non available columns)
        for empty_col in not_available_col: temp_dict.update({empty_col: ''})
        # Filtering Data
        if(temp_dict['MRP']==temp_dict['Selling Price']): temp_dict['Discount'] = ''

        #Separation of mandatory data with non mandatory data
        if(temp_dict['Brand']!='' and temp_dict['Title']!='' and temp_dict['Selling Price']!=''
           and temp_dict['MRP']!='' and temp_dict['In Stock']!='No' and temp_dict['Image URL']!=''):
            Main_Data_list.append(temp_dict)
            pass
            # print("\n\n",temp_dict,"\n\n")
        else:
            #Error IDs generation
            Glob_Exception_URL.append(response.url)
            Glob_Exception_ID.append(response.url.split(main_url)[1].split('?')[0])
        
        #All data will be Generated 
        Data_without_filtered.append(temp_dict)
        return temp_dict
    

    except:
        Glob_Exception_URL.append(response.url)
        Glob_Exception_ID.append(response.url.split(main_url)[1].split('?')[0])


#First iteration for getting the data and error ID's
class AjiopdpSpider(scrapy.Spider):
    name = "ajiopdp"
    custom_settings = {
        'DOWNLOAD_DELAY': 1,
        'AUTOTHROTTLE_ENABLED': True,
    }

    allowed_domains = ["ajio.com"]
    name = 'ajiopdp'
    #Input file to be declared below along with sheetname
    productId_df = pd.read_excel(r"{}\\AJIO_Scraping_Input.xlsx".format(input_dir), sheet_name= "Myntra")

    #Presenting the Column name below
    productId_df['Input'] = main_url+productId_df['Input'].astype(str)
    all_urls = list(productId_df['Input'])

    #Function to hit the URL and gets the response to the parse function
    def start_requests(self):
        try: 
            if(len(pd.read_excel(r"{}\\AjioPDP_Exception_ProductID.xlsx".format(error_dir)))>0): pd.DataFrame({"Exception_Id":[]}).to_excel(r"{}\\AjioPDP_Exception_ProductID1.xlsx".format(error_dir))
        except: pass
        #One on one URLs will be parsed
        for url in self.all_urls: 
           
            yield scrapy.Request(url=url, callback=parse, meta={'handle_httpstatus_all': True, "ProductId":url})        
        
#Crawler Procecss
process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"AjioPDP_OutputData.csv": {'format': 'csv', 'overwrite': True}} # For Storing Parse Return Data
                                   })
process.crawl(AjiopdpSpider)
Glob_Exception_ID =[]

#Second iteration which takes input as Error ID generated from First itteration
class Exception_AjioPDP(scrapy.Spider):
    name = "Exception_AjioPDP"
    def start_requests(self):
        global Glob_Exception_ID, Glob_Exception_URL
        #Iteration of Error ID

        for try_url in Glob_Exception_URL:
            main_lis_len = len(Main_Data_list)
            #Number of Itteration is sepcified in range(Num_of_Itteration)

            for looping in range(2):
               if(len(Main_Data_list)>main_lis_len):break
               yield scrapy.Request(try_url, callback=parse,meta={'handle_httpstatus_all': True, "ProductId":try_url})

#Scrapy crawling process for the second itteration                      
Excecption_looping = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG',
                                   'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                                   'FEEDS': {"AjioPDP_Exception_Output.csv": {'format': 'csv', 'overwrite': True}}
                                   })
Excecption_looping.crawl(Exception_AjioPDP)


# For Remove ReactModule Error In Scrapy
configure_logging()
runner = CrawlerRunner()
@defer.inlineCallbacks
#Calls the first itteration AjiopdpSpider class and then the second one Exception_AjioPDP

def crawl():
    yield runner.crawl(AjiopdpSpider)
    yield runner.crawl(Exception_AjioPDP)
    reactor.stop()
crawl()
reactor.run()

#Generates the error ID's

try:
    original_df = pd.DataFrame(Main_Data_list)
    output_product_id = list(original_df['Product ID'])
    main_exp_id = []
    #if the Input Product IDs are not in Output list then those IDs are considered as Error IDs

    for exp_id in set(Glob_Exception_ID):
        Glob_Exception_ID = []
        if exp_id not in output_product_id:
            main_exp_id.append(exp_id)
    Exception_Df = pd.DataFrame({"Exception_ProductID":list(set(main_exp_id))}).to_excel(r"{}\\AjioPDP_Exception_ProductID1.xlsx".format(error_dir))

except:
    Exception_Df = pd.DataFrame({"Exception_ProductID":list(set(Glob_Exception_ID))}).to_excel(r"{}\\AjioPDP_Exception_ProductID1.xlsx".format(error_dir))


try:
    all_data = pd.DataFrame(Main_Data_list)
    all_data.drop_duplicates(subset=["Product ID"], inplace=True)
    #mandatory data wiil be saved
    all_data.to_excel(r"{0}\\Ajio_PDPOutputData_mandatory.xlsx".format(output_dir), index=False)
except:pass 

try:
    data_no_filtered = pd.DataFrame(Data_without_filtered)
    data_no_filtered.drop_duplicates(subset=['Product ID'], inplace=True)
    only_nonMandatory = data_no_filtered
    #All data will be saved in below file name
    data_no_filtered.to_excel(r"{0}\\Ajio_AllData.xlsx".format(output_dir), index=False)
    data_no_filtered.to_excel(r"{0}\\AjioData_NoFiltered.xlsx".format(No_filter_dir), index=False)


    #It saves the unique non mandatory data (All_data-Mandatory_Data)
    for prdId in all_data['Product ID']:
        only_nonMandatory = only_nonMandatory[only_nonMandatory['Product ID']!=prdId]
    only_nonMandatory.to_excel(r"{}\\Ajio_Unique_NonMandatoryData.xlsx".format(output_dir), index=False)
except:
    pass


#Function to push the data into GCP cloud
def gcp_push(data, f_type):
    try:

        #Gets the current date format.

        current_date = datetime.datetime.now().date()
        current_month = current_date.strftime("%m")
        current_date = datetime.datetime.now().date()
        current_day = current_date.strftime("%d")
        #Credentials are read from the credentials-python-storage.json file placed in the folder

        client = storage.Client.from_service_account_json(
            json_credentials_path=r'credentials-python-storage.json')
        bucket = client.get_bucket('tmrw_scraping_data')
        #File name with mandatory or non mandatory type will be saved

        filename = f"Ajio_{f_type}.xlsx"
        excel_buffer = pd.ExcelWriter(filename, engine='xlsxwriter')
        data.to_excel(excel_buffer, index=False)
        excel_buffer.close()
        #Given below is the folder path in GCP

        blob = bucket.blob(
            f'pdp/ajio/2023/{current_month}/{current_day}/{filename}')
        with open(filename, 'rb') as file:
            blob.upload_from_file(
                file, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        print('Batch data uploaded to GCP.')
    except Exception as e:
        print(f"Failed to push the data to GCP: {e}")


#This will push the mandatory data to the GCP
gcp_push(all_data,f_type="Mandatory")

#The below line of code pushes the Non-mandatory data to the GCP.
gcp_push(only_nonMandatory, f_type = "Non_mandatory")

