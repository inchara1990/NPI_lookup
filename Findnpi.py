import requests
import pandas as pd
import json

file_read = pd.read_csv("providers_master_6k.csv")

#api_url = "https://npiregistry.cms.hhs.gov/api"
api_url = "https://208.83.149.111/api"
df = pd.DataFrame()

for index,row in file_read.iterrows():
    print("processing ",index," document")
    reqd_params = dict()
    reqd_params['first_name'] = row["*First Name"]
    reqd_params['last_name'] = row["*Last Name"]
    reqd_params["state"] = row["State"]
    payload_str = "&".join("%s=%s" % (k ,v) for k ,v in reqd_params.items())
    complete_urls = '?'.join([api_url, payload_str])

    try:
        result = requests.get(complete_urls,verify=False)
        print('Completed URL:', result.url, result.status_code)

        address_Count = json.loads(result.content)['results']
        list_len = len(address_Count)
        #first level match :only on state- if no other adresses present
        if(list_len == 1):
            NPI = json.loads(result.content)['results'][0]['number']
            print(NPI)
            row["NPI_Code"] = NPI
            df = df.append(row)
        # second level match : on state and city
        elif (list_len > 1):
            reqd_params["city"] = row["City"]
            payload_str = "&".join("%s=%s" % (k, v) for k, v in reqd_params.items())
            complete_urls = '?'.join([api_url, payload_str])
            result = requests.get(complete_urls, verify=False)
            NPI_mod = json.loads(result.content)['results'][0]['number']
            row["NPI_Code"] = NPI_mod
            print(NPI_mod)
            df = df.append(row)
        #if no results
        else:
            row['NPI_Code'] = "No results"
            df = df.append(row)

    except:
        row['NPI_Code'] = "NO NPI"
        df = df.append(row)



df.to_csv("providers_master_6k_modified_v2.csv")



