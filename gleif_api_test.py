import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
import requests
from pygleif import PyGleif
def company_info(companyname):
        #api_url = "https://api.gleif.org/api/v1/fuzzycompletions?field=entity.legalName&q=ABB E-mobility"
        #api_url="https://api.gleif.org/api/v1/fuzzycompletions?field=entity.legalName&q="+companyname
        api_url="https://api.gleif.org/api/v1/lei-records?filter[fulltext]=" + companyname 
        response = requests.get(api_url)
        #print(response.json())
        #print(response.status_code)
        json_format = json.loads(response.text)
        #print(json_format['assets'][0]['network_ports'][0]['version'])
        print(json_format['data'][0]["relationships"]["lei-records"]["data"]["id"])
        # print(json_format)

        for i in json_format['data']:
                #print (i['attributes']['value'])
                key='relationships'
                x = list(i.keys())
                if(x.count(key) == 1):
                        #print (i['relationships']['lei-records']['links']['related'])
                        api_url1=i['relationships']['lei-records']['links']['related']
                        response1=requests.get(api_url1)
                        #print(response1.status_code)
                        json_format1 = json.loads(response1.text)
                        print("Company name : ",json_format1['data']['attributes']['entity']['legalName']['name'])
                        legalAddress_details=""
                        for j in json_format1['data']['attributes']['entity']['legalAddress']['addressLines']:
                                legalAddress_details=str(legalAddress_details) + str(j)
                        #print("aaaaaa", legalAddress_details)
                        legalAddress=str(legalAddress_details)+" "+str(json_format1['data']['attributes']['entity']['legalAddress']['city'])+ " "+str(json_format1['data']['attributes']['entity']['legalAddress']['region'])+ " "+str(json_format1['data']['attributes']['entity']['legalAddress']['country'])
                        #legalAddress=str(json_format1['data']['attributes']['entity']['legalAddress']['addressLines'])+" "+str(json_format1['data']['attributes']['entity']['legalAddress']['city'])+ " "+str(json_format1['data']['attributes']['entity']['legalAddress']['region'])+ " "+str(json_format1['data']['attributes']['entity']['legalAddress']['country'])
                        #print("legalAddress",json_format1['data']['attributes']['entity']['legalAddress']['addressLines'])
                        print("legalAddress : ",legalAddress)
                        print("legaladdress Country name :",json_format1['data']['attributes']['entity']['legalAddress']['country'])

                        HQAddress_details=""
                        for j in json_format1['data']['attributes']['entity']['headquartersAddress']['addressLines']:
                                HQAddress_details=str(HQAddress_details) + str(j)
                        
                        headquarterAddress=str(HQAddress_details)+" "+str(json_format1['data']['attributes']['entity']['headquartersAddress']['city'])+ " "+str(json_format1['data']['attributes']['entity']['headquartersAddress']['region'])+ " "+str(json_format1['data']['attributes']['entity']['headquartersAddress']['country'])
                        #headquarterAddress=str(json_format1['data']['attributes']['entity']['headquartersAddress']['addressLines'])+" "+str(json_format1['data']['attributes']['entity']['headquartersAddress']['city'])+ " "+str(json_format1['data']['attributes']['entity']['headquartersAddress']['region'])+ " "+str(json_format1['data']['attributes']['entity']['headquartersAddress']['country'])
                        print("headquarterAddress : ",headquarterAddress)


def company_info1(companyname,iso):
        #api_url="https://api.gleif.org/api/v1/fuzzycompletions?field=entity.legalName&q="+str(companyname)
        api_url="https://api.gleif.org/api/v1/lei-records?filter[fulltext]=" + companyname
        response = requests.get(api_url)
        json_format = json.loads(response.text)
        res_obj = {"company_name":None,"legalAddress":None,"officeAddress":None,"country":None,"LEI":None}
        objects = []
        #print("json format", json_format)
        try:
                        for i in json_format['data']:

                                #print ("inside  keys in Json ........", i )
                                res_obj["company_name"] = i['attributes']['entity']['legalName']['name']
                                print("company name", res_obj["company_name"] )
                                legalAddress_details=""
                                for j in i ['attributes']['entity']['legalAddress']['addressLines']:
                                        legalAddress_details=str(legalAddress_details) + str(j)
                                legalAddress=str(legalAddress_details)+" "+str(i['attributes']['entity']['legalAddress']['city'])+ " "+str(i['attributes']['entity']['legalAddress']['region'])+ " "+str(i['attributes']['entity']['legalAddress']['country'])
                                res_obj["legalAddress"] = legalAddress

                                print ("legaladdress", legalAddress) 
                                res_obj["country"] = i['attributes']['entity']['legalAddress']['country']
                                HQAddress_details = ""
                                for j in i['attributes']['entity']['headquartersAddress']['addressLines']:
                                        HQAddress_details=str(HQAddress_details) + str(j)
                                
                                headquarterAddress = str(HQAddress_details) + " " + str(i['attributes']['entity']['headquartersAddress']['city']) + " "+ str(i['attributes']['entity']['headquartersAddress']['region'])+ " "+str(i['attributes']['entity']['headquartersAddress']['country'])
                                res_obj["officeAddress"] = headquarterAddress
                                res_obj["LEI"] = i["id"]

                                print("res_obji headquarter  address...", res_obj["officeAddress"]) 
                                print ("lei ", res_obj["LEI"])
                                if iso in headquarterAddress.strip().split():
                                        objects.append(res_obj)
                                        print("country matches .....")
        except:
                return res_obj
        if len(objects) == 0:
                return {"company_name":None,"legalAddress":None,"officeAddress":None,"country":None,"LEI":None}
        return objects[0] 
def get_jurisdiction_info(lei):
        try:
                gleif = PyGleif(lei)
                return gleif.response.data.attributes.entity.jurisdiction
        except:
                return ""
        
def generate_final_file(df):
        la,oa,lei = [],[],[]
        for i,row in df.iterrows():
                company = row["Companies"]
                if "," in row["Companies"]:
                        company = row["Companies"].strip().split(",")[0]
                print(company)
                word_length = len(company.split())
                if word_length > 2:
                        company = " ".join(company.strip().split()[:-1])
                data = company_info1(company,row["Country"].strip())
                if row["Country"] == data["country"]:
                        la.append(data["legalAddress"])
                        oa.append(data["officeAddress"])
                        lei.append(data["LEI"])
                else:
                        la.append("")
                        oa.append("")
                        lei.append("")
        df["LegalAddress"] = la
        df["OfficeAddress"] = oa
        df["LEI"] = lei
        df.to_csv("FinalFile.csv")
        return df


if __name__ == "__main__":
        
        df = pd.read_excel("11-01-2023.xlsx")
        #generate_final_file(df)
        #company_info1("bookmyshow", "IN")
        #company_info1("Security Bank Corporation.","PH")
        company_info1("Bounty Agro Ventures, Inc.","PH")
        #print(df["LegalAddress"])
        #print(df["OfficeAddress"])
        # get_company_info("DIVGI TORQTRANSFER SYSTEMS LIMITED","IN")
        # "335800N9OHIPOMBP7C30"
        # print(company_info1("Dar Al Arkan","UAE"))
