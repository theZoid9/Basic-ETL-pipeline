import pandas as pd 
import json


'''
Step 1 : Extract 
'''

def extract(file_path):
    data = pd.read_csv(file_path)
    return data


'''
Step 2 : Transform
'''

def transform(data):
    cleaned_data = data.dropna()
    cleaned_data["name"] = cleaned_data["name"].str.title()
    cleaned_data["city"] =  cleaned_data["city"].str.title()
    return cleaned_data

'''
Step 3 : Load
'''

def load(data, output_file):
    data.to_json(output_file, orient="records", lines=True)
    print(f"Data loaded into {output_file}")
    
    
'''
Pipeline function
'''

def etl_pipeline(input_file,out_file):
    # Extract
    data = extract(input_file)
    # Transform
    cleaned_data = transform(data)
    # load
    load(cleaned_data,out_file)


input_file = "users.csv"
output_file = "users.json"
etl_pipeline(input_file,output_file)
    