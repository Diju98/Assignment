#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing the required packages and libraries
from bs4 import BeautifulSoup
import requests


# In[2]:


# Importing logging to create a well maintained log file
import logging

logging.basicConfig(filename = 'demo.log', level = logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')


# In[3]:


# Using the requests module for collecting the data from the URL
url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
xml = requests.get(url)
logging.debug('Data has been collected from the URL using the reqquests module and stored in the variable xml')


# In[4]:


"""Our next task is to parse through the data in the URL and find the first download link which contains our 
data which we will use to work on with the assignment
"""
soup = BeautifulSoup(xml.content, 'lxml')    # Using BeautifulSoup for scraping and parsing through the XML file in the URL
xml_tag = soup.find()    # Finding all the tags present in the XML file
for tag in soup.find('str', {'name':'download_link'}):    #Parsing the XML file to find the first download link
    print(tag)


# In[5]:


"""The download link found is a zip file, we will now use the wget module to download the zip file"""
import wget
url = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
xml_zip = wget.download(url)
logging.debug('Zip file which contains our xml file has been downloaded')


# In[6]:


"""We have downloaded the zip file using the wget module. Now we need to extract the XML file contained in the zip file.
For doing that we will zipfile module
"""

# importing required modules
from zipfile import ZipFile

# specifying the zip file name
file_name = "DLTINS_20210117_01of01.zip"

# opening the zip file in read mode
with ZipFile(file_name,'r') as zip:
    # printing all the contents of the zip file
    zip.printdir()
    
    #extracting all the files
    zip.extractall()
    logging.debug('XML extracted successfully')


# In[7]:


"""Our next task is to convert the XML file into a CSV. For that we will use the xml.etree.ElementTree module and parse
the XML file to find the root node. After finding the root node we will implement DFS algorithm to find all the nodes
present. After finding the nodes, we will collect the data from the elements and then append the data to form a CSV
"""
import xml.etree.ElementTree as ET
import pandas as pd

tree = ET.parse('DLTINS_20210117_01of01.xml')
root = tree.getroot()

Id = []
FullNm = []
ClssfctnTp = []
CmmdtyDerivInd = []
NtnlCcy = []
Issr = []

def dfs(root):
    """We are using Depth First Search to get all the nodes of the tree
    
    
    Parameter:
    root: It is the root element of the tree
    
    """
    
    if root == None:
        return
    
    if (root.tag[48:]) == 'Id':
        try:
            if len(root.text) == 12:
                Id.append(root.text)     
        except:
            pass
        
    if (root.tag[48:]) == 'FullNm':
        FullNm.append(root.text)
                
    if (root.tag[48:]) == 'ClssfctnTp':
        ClssfctnTp.append(root.text)

    if (root.tag[48:]) == 'CmmdtyDerivInd':
        CmmdtyDerivInd.append(root.text)
                        
    if (root.tag[48:]) == 'NtnlCcy':
        NtnlCcy.append(root.text)
        
    if (root.tag[48:]) == 'Issr':
        Issr.append(root.text)
        
    else:    
        for i in root:
            dfs(i)

dfs(root)

output=pd.DataFrame(
        {"Id":Id,
         "Fullnm":FullNm,
         "ClssfctnTp":ClssfctnTp,
         "CmmdtyDerivInd":CmmdtyDerivInd,
         "NtnlCcy":NtnlCcy,
         "Issr":Issr})


output.to_csv(r'test.csv')
logging.debug('All the data has been appended successfuly and converted into a csv')


# In[11]:


"""After forming the CSV we will now upload the CSV in an AWS S3 bucket.
We will be using the boto3 module. The boto3 module allows us to create, 
configure, and manage AWS services using AWS APIs.
"""

import boto3  
s3=boto3.resource(
        service_name = 's3',
        region_name = 'us-east-1',
        aws_access_key_id = 'AKIAVGM72J3JWJCD2C5F',
        aws_secret_access_key = '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')
#uploading files to s3 bucket
s3.Bucket('dhyantest1').upload_file(Filename = 'test.csv', Key = 'test.csv')

logging.debug('CSV uploaded to AWS S3 bucket')


# In[ ]:




