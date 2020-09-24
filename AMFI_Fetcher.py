import bs4
import requests
import pandas as pd
import time
import argparse
import tqdm

class safety_backup:
    def __init__(self):
        self.name = "safety backup"
        self.failed_cities = set()
        
    def failed_city_add(self, name):
        self.failed_cities.add(name)
        
    def fetch_failed_city(self):
        return self.failed_cities         
        
        
class data_store:
    def __init__(self, file_name="AMFI_SEP_2020.csv"):
        self.file_name = file_name
        self.req_column_list = ['Sr No','ARN',"ARN Holder's Name",'Address','Pin','Email','City', 
                                'Telephone (R)','Telephone (O)','ARN Valid Till','ARN Valid From',
                                'KYD Compliant','EUIN']
        self.data_frame = ""
        
    def create_dataframe(self):
        self.data_frame = pd.DataFrame(columns=self.req_column_list)
            
    def insert_in_dataframe(self, data):
        data = pd.read_html(data)[0]
        if len(data) == 0:
            return
        else:
            self.data_frame = self.data_frame.append(data, ignore_index=False)
            return "Done"
        
    def save_to_csv(self):
        self.data_frame.to_csv(self.file_name)
            

def content_load(url=None):
    if url == None:
        return "No input Provided"
    else:
        data = requests.get(url)
        datas = data.content
        data.close()
        return datas
    
def html_preetyfier(data=None):
    if data == None:
        return "No Data Provided"
    else:
        data = bs4.BeautifulSoup(data, "html.parser")
        return data

def selection_items_load(data):
    item = set()
    content = data.find_all("div", attrs={"class":"ui-widget auto-select"})

    for i in content[0].strings:
        if i == "\n":
            continue
            
        elif i.isalpha():
            item.add(i)
            
        else:
            continue
    
    return item


def safety_feature(failed_city=None, do_now = False):
    #Only call drivers are loaded successfully
    if failed_city != None:
        safe_back.failed_city_add(failed_city)
    
    if do_now == True:
        city = safe_back.fetch_failed_city()
        
        if len(city) == 0:
            print("No Failed Attempts Detected!")
            pass
        
        else:
            try:
                for i in city:
                    data = amfi_post_request(i).decode()
                    store.insert_in_dataframe(data)
            except:
                pass
            
def amfi_post_request(city_name):
    req_url = 'https://www.amfiindia.com/modules/NearestFinancialAdvisorsDetails'
    post_data = {"nfaType":"All","nfaARN":"","nfaARNName":"","nfaAddress":"","nfaCity":city_name,"nfaPin":""}
    req_post = requests.post(req_url, post_data)
    
    if req_post.status_code == 200:
        data = req_post.content
        req_post.close()
        return data
    
    else:
        req_post.close()
        return None
    
    
def script_controller_req(main_url = "https://www.amfiindia.com/locate-your-nearest-mutual-fund-distributor-details"):

    print("AMFI FETCHER VERSION 1.0")
    print("\n")
    
    script_begin = time.time()
    store.create_dataframe()
    
    city_name = selection_items_load(html_preetyfier(content_load(main_url)))
    
    for i in tqdm.tqdm(city_name):
        
        try:
            data = amfi_post_request(i).decode()
                
            if data != None:
                store.insert_in_dataframe(data)
        
        except:
            safety_feature(i, False)
            continue

    safety_feature(None, True)
    store.save_to_csv()
    
    print("Total Time Taken by Script: {} Seconds for: {} cities".format(int(time.time() - script_begin),len(city_name)))
        
    
if __name__ == "__main__":    
    #Global Driver code for thread a:
    store = data_store()
    safe_back = safety_backup()
    #https://www.amfiindia.com/locate-your-nearest-mutual-fund-distributor-details
    cli = argparse.ArgumentParser()
    cli.add_argument("-st", "--Start", required=True, help="To start the Script, give 'y'")
    inputs = vars(cli.parse_args())

    if inputs["Start"] == 'y':
        script_controller_req()
    else:
        print("Give -h to see available options")
