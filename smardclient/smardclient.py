import requests
import pandas as pd
from io import StringIO
import json
from datetime import datetime
import json
from io import BytesIO
from zipfile import ZipFile

_debug_mode = False

_smard_download_url = "https://www.smard.de/nip-download-manager/nip/download/market-data"

class SMARDRequest(object):
    
    def __init__(self,format = 'CSV',moduleIds = [],region="DE",
                 timestamp_from = "01.01.2018" ,timestamp_to = datetime.now(),
                 type = "discrete",language="de"):
        self.format = format
        self.moduleIds = moduleIds
        self.region = region
        self.timestamp_from = self._convert_to_unix(timestamp_from)
        self.timestamp_to = self._convert_to_unix(timestamp_to)
        self.type = type
        self.language = language
 
        
    def _convert_to_unix(self, date):
        try:
            return str(int(datetime.strptime( date , "%d.%m.%Y").timestamp()*1000))
        except ValueError: 
            return str(int(date.timestamp())*1000)
        except TypeError:
            return str(int(date.timestamp())*1000)
            
    def toJSON(self):
        return json.dumps(self, default=lambda o: {'request_form':[o.__dict__]}, indent=4)

    def download_data(self):
        params = {"Content-Type":"application/json;charset=UTF-8"}
        res  = requests.post(_smard_download_url, params=params, data = self.toJSON())

        if _debug_mode:
            print("request url: ",res.url)
            print("request body: ",self.toJSON())
            #print(res.content)
            print(res.headers)
            print("status code: ",res.status_code)
        
        data_bytes = _download_extract_zip(res)

        if _debug_mode:
            print(type(data_bytes))
            print(dir(data_bytes))
            #print(type(data_bytes.read()))
            
        
        data = pd.read_csv(StringIO(data_bytes.decode('utf-8')),sep=';',parse_dates=[['Datum','Uhrzeit']],dayfirst=True, na_values="-",thousands=".",decimal=",")
        data = data.set_index('Datum_Uhrzeit')
        data = data.rename(columns = lambda c : (c+"_"+self.region).replace(' ','_') )
        data = data.tz_localize("Europe/Berlin", ambiguous='infer')
        return data
    
def get_realized_power_supply(format = 'CSV',region="DE",
                 timestamp_from = "01.01.2018" ,timestamp_to = datetime.now(),
                 type = "discrete",language="de"):
    
    ids = SMARDIds["realized_power_supply"][region]
    
    return SMARDRequest(format = format,
                        region = region,
                        timestamp_from = timestamp_from,
                        timestamp_to = timestamp_to,
                        type = type,
                        language = language,
                        moduleIds = ids).download_data()


def get_forecasted_power_supply(format = 'CSV',region="DE",
                 timestamp_from = "01.01.2018" ,timestamp_to = datetime.now(),
                 type = "discrete",language="de"):
    
    ids = SMARDIds["forecasted_power_supply"][region]
    
    
    return SMARDRequest(format = format,
                        region = region,
                        timestamp_from = timestamp_from,
                        timestamp_to = timestamp_to,
                        type = type,
                        language = language,
                        moduleIds = ids ).download_data()


def get_forecasted_power_demand(format = 'CSV',region="DE",
                 timestamp_from = "01.01.2018" ,timestamp_to = datetime.now(),
                 type = "discrete",language="de"):
    
    ids = SMARDIds["forecasted_power_demand"][region]
    
    
    return SMARDRequest(format = format,
                        region = region,
                        timestamp_from = timestamp_from,
                        timestamp_to = timestamp_to,
                        type = type,
                        language = language,
                        moduleIds = ids ).download_data()


def get_physical_power_flow(format = 'CSV',region="DE",
                 timestamp_from = "01.01.2018" ,timestamp_to = datetime.now(),
                 type = "discrete",language="de"):
    
    ids = SMARDIds["physical_power_flow"][region]
    
    
    return SMARDRequest(format = format,
                        region = region,
                        timestamp_from = timestamp_from,
                        timestamp_to = timestamp_to,
                        type = type,
                        language = language,
                        moduleIds = ids ).download_data()

def _download_extract_zip(response):
    """
    from https://techoverflow.net/2018/01/16/downloading-reading-a-zip-file-in-memory-using-python/
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    with ZipFile(BytesIO(response.content)) as thezip:
        if _debug_mode:
            print(thezip.infolist())
        for zipinfo in thezip.infolist():
            #print(pd.read_csv(thezip.open(zipinfo)))
            with thezip.open(zipinfo) as thefile:
                return thefile.read()


SMARDIds = {"forecasted_power_supply":{
                                    "DE":[2000122, 2000715, 2000125, 2003791, 2000123],
                                    "AT":[2000122, 2000715, 2000125, 2000123],
                                    "LU":[2000122, 2000715, 2000125, 2000123],
                                    "50Hertz":[2000122, 2000715, 2000125, 2003791, 2000123],
                                    "Amprion":[2000122, 2000715, 2000125, 2000123],
                                    "TenneT" :[2000122, 2000715, 2000125, 2003791, 2000123],
                                    "TransnetBW":[2000122, 2000715, 2000125, 2000123],
                                    "APG":[2000122, 2000715, 2000125, 2000123],
                                    "Creos":[2000122, 2000715, 2000125, 2000123]},
            "realized_power_supply":{
                                    "DE":[1001224, 1004066, 1004067, 1004068, 1001223, 
                                          1004069, 1004071, 1004070, 1001226, 1001228, 
                                          1001227, 1001225],
                                    "AT":[1004066, 1004067, 1004068, 1004069, 1004071, 
                                          1004070, 1001226, 1001228, 1001227],
                                    "50Hertz":[1004066, 1004067, 1004068, 1001223, 1004069, 
                                               1004071, 1004070, 1001226, 1001228, 1001227, 
                                               1001225],
                                    "Amprion":[1001224, 1004066, 1004067, 1004068, 1001223, 
                                               1004069, 1004071, 1004070, 1001226, 1001228, 
                                               1001227],
                                    "TenneT" :[1001224, 1004066, 1004067, 1004068, 1001223, 
                                               1004069, 1004071, 1004070, 1001226, 1001228, 
                                               1001227, 1001225],
                                    "TransnetBW":[1001224, 1004066, 1004067, 1004068, 1004069, 
                                                  1004071, 1004070, 1001226, 1001228, 1001227],
                                    "APG":[1004066, 1004067, 1004068, 1004069, 1004071, 1004070, 
                                           1001226, 1001228, 1001227]},
            "forecasted_power_demand":{
                                    "DE":[6000411]
            },
            "physical_power_flow":{
                                    "DE":[31000714,31000140,31000569,31000145,31000574,31000570,
                                    31000139,31000568,31000138,31000567,31000146,31000575,31000144,
                                    31000573,31000142,31000571,31000143,31000572,31000141]
            }}





    

