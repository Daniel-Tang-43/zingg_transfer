#!/usr/bin/env python
# coding: utf-8

# Import lib's

# In[67]:


import pandas as pd
import numpy as np
import math
import re
from fuzzywuzzy import fuzz
from collections import Counter
import csv
from google.cloud import bigquery
from pandas.io import gbq
from google.oauth2 import service_account


# Load Brand List from csv file

# In[68]:


fields = ['Brand cleaned']
df_brand = pd.read_csv('Brand and Line data.csv', skipinitialspace=True, usecols=fields).dropna()
df_brand = df_brand.reset_index()
df_brand = df_brand.drop(['index'], axis = 1)
df_brand["id"] = df_brand.index + 1


# Initialize df

# In[69]:


df_cigar_af = pd.DataFrame(columns=['row_id', 'title_unfiltered', 'title_filtered', 'binder', 'wrapper', 'filler', 'size', 'country',
                               'length_fraction', 'length_float', 'ring_gauge', 'strength', 'rating', 'extract_vitola',
                               'extract_seed', 'extract_color', 'extract_region'])

df_cigar_af = df_cigar_af.applymap(str)

df_cigar_geek = pd.DataFrame(columns=['row_id', 'title_unfiltered', 'title_filtered', 'binder', 'wrapper', 'filler', 'country',
                               'length_float', 'ring_gauge', 'color', 'strength', 'size', 'rating', 'extract_vitola',
                               'extract_seed', 'extract_color', 'extract_region'])
                            
df_cigar_geek = df_cigar_geek.applymap(str)


df_cigar_scanner = pd.DataFrame(columns=['row_id', 'title_unfiltered', 'title_filtered', 'binder', 'wrapper', 'filler', 'manufacturer', 'country',
                               'length_fraction', 'length_float', 'ring_gauge', 'strength', 'size', 'rating', 'extract_vitola',
                               'extract_seed', 'extract_color', 'extract_region'])
                               

df_cigar_scanner = df_cigar_scanner.applymap(str)


# Connect to Big Query

# In[70]:


credentials = service_account.Credentials.from_service_account_file(
'coastal-set-346210-7412c02a4f9f.json')

project_id = 'coastal-set-346210'

client = bigquery.Client(credentials= credentials,project=project_id)

# table_id = 'coastal-set-346210.Cigar_Review_DataSet.cigar_af_transform'
# table_id2 = 'coastal-set-346210.Cigar_Review_DataSet.cigar_geek_transform'
# table_id3 = 'coastal-set-346210.Cigar_Review_DataSet.cigar_scanner_transform'

# client.delete_table(table_id, not_found_ok=True)  # Make an API request.
# print("Deleted table '{}'.".format(table_id))

# client.delete_table(table_id2, not_found_ok=True)  # Make an API request.
# print("Deleted table '{}'.".format(table_id2))

# client.delete_table(table_id3, not_found_ok=True)  # Make an API request.
# print("Deleted table '{}'.".format(table_id3))

query_job = client.query("""
   SELECT *
   FROM Cigar_Review_DataSet.dbt_cigar_af_transform""")

df = query_job.result()

query_job2 = client.query("""
   SELECT *
   FROM Cigar_Review_DataSet.dbt_cigar_geek_transform""")

df1 = query_job2.result()

query_job4 = client.query("""
   SELECT *
   FROM Cigar_Review_DataSet.dbt_cigar_scanner_transform""")

df2 = query_job4.result()


# Cigar_af

# In[71]:


for row in df:

    cigar_af_dict = {
        'row_id': row[0],
        'title_unfiltered': row[1],
        'title_filtered': row[2],
        'binder': row[3],
        'wrapper': row[4],
        'filler': row[5],
        'size': row[6],
        'country': row[7],
        'length_fraction': row[8],
        'length_float': row[9],
        'ring_gauge': row[10],
        'strength': row[11],
        'rating': row[12],
        'extract_vitola': row[13],
        'extract_seed': row[14],
        'extract_color': row[15],
        'extract_region': row[16]
    }

    df_new_row = pd.DataFrame(cigar_af_dict, index=[0])
    df_cigar_af = pd.concat([df_cigar_af, df_new_row])

df_cigar_af = df_cigar_af.reset_index()
df_cigar_af = df_cigar_af.drop(['index'], axis = 1)
df_cigar_af["extract_brand"] = ""
df_cigar_af["extract_line"] = ""
df_cigar_af["extract_size"] = ""


# Cigar_geek

# In[72]:


for row in df1:

    cigar_geek_dict = {
        'row_id': row[0],
        'title_unfiltered': row[1],
        'title_filtered': row[2],
        'binder': row[3],
        'wrapper': row[4],
        'filler': row[5],
        'country': row[6],
        'length_float': row[7],
        'ring_gauge': row[8],
        'color': row[10],
        'strength': row[11],
        'size': row[12],
        'rating': row[13],
        'extract_vitola': row[14],
        'extract_seed': row[15],
        'extract_color': row[16],
        'extract_region': row[17]
    }

    df_new_row = pd.DataFrame(cigar_geek_dict, index=[0])
    df_cigar_geek = pd.concat([df_cigar_geek, df_new_row])

df_cigar_geek = df_cigar_geek.reset_index()
df_cigar_geek = df_cigar_geek.drop(['index'], axis = 1)
df_cigar_geek["extract_brand"] = ""
df_cigar_geek["extract_line"] = ""
df_cigar_geek["extract_size"] = ""


# Cigar Scanner

# In[73]:


for row in df2:

    cigar_scanner_dict = {
        'row_id': row[0],
        'title_unfiltered': row[1],
        'title_filtered': row[2],
        'binder': row[3],
        'wrapper': row[4],
        'filler': row[5],
        'manufacturer': row[6],
        'country': row[7],
        'length_fraction': row[8],
        'length_float': row[9],
        'ring_gauge': row[10],
        'strength': row[11],
        'size': row[13],
        'rating': row[14],
        'extract_vitola': row[15],
        'extract_seed': row[16],
        'extract_color': row[17],
        'extract_region': row[18]
    }

    df_new_row = pd.DataFrame(cigar_scanner_dict, index=[0])
    df_cigar_scanner = pd.concat([df_cigar_scanner, df_new_row])

df_cigar_scanner = df_cigar_scanner.reset_index()
df_cigar_scanner = df_cigar_scanner.drop(['index'], axis = 1)
df_cigar_scanner["extract_brand"] = ""
df_cigar_scanner["extract_line"] = ""
df_cigar_scanner["extract_size"] = ""


# cigar_af_extract_brand_line_size

# In[74]:


# extract brand by comparing cigar af data frame records with brand list

for index1, row1 in df_cigar_af.iterrows():
    for index2, row2 in df_brand.iterrows():
        if row1['title_unfiltered'] is not None:
            word_list_1 = row1['title_unfiltered'].split()
            number_of_words_1 = len(word_list_1)
            word_list_2 = row2['Brand cleaned'].split()
            number_of_words_2 = len(word_list_2)
            
            # if the number of words in cigar_af record is > number of words in brand list record
            # then slice the cigar_af title column to the size of brand list record and compare
            # if the ratio is > 90, then the brand is similar. So extract the brand and save in the 
            # new column 

            if(number_of_words_1 > number_of_words_2):
                title_list_cigar_af = row1['title_unfiltered'].split()[:number_of_words_2]
                title_cigar_af = ' '.join(title_list_cigar_af)
            
            if(fuzz.ratio(title_cigar_af.lower(),row2['Brand cleaned'].lower()) > 90):
                df_cigar_af.loc[df_cigar_af.index[index1], 'extract_brand'] = row2['Brand cleaned'].lower()
                df_cigar_af.loc[df_cigar_af.index[index1], 'title_unfiltered'] = df_cigar_af.loc[df_cigar_af.index[index1], 'title_unfiltered'].lower().replace(title_cigar_af.lower(), row2['Brand cleaned'].lower()).strip()
                df_cigar_af.loc[df_cigar_af.index[index1], 'title_filtered'] = df_cigar_af.loc[df_cigar_af.index[index1], 'title_filtered'].lower().replace(title_cigar_af.lower(), row2['Brand cleaned'].lower()).strip()


# In[75]:


# extract line and size of cigar_af

# compare each brand with the cigar_af extract_brand column. If it is a match,
# get the line and size by replacing brand value to '' and add the result to a set
# so only unique line and size value will be available 

for index1, row1 in df_brand.iterrows():
    unique_line_set = set()
    unique_line_set_final = set()
    for index2, row2 in df_cigar_af.iterrows():
        if(row2['extract_brand'].lower() == row1['Brand cleaned'].lower()):
            x = row2['title_unfiltered'].lower().replace(row2['extract_brand'].lower(), "").strip()
            if(x != "" and len(x.split()) > 1):
                unique_line_set.add(x)

    # get the line value alone by comparing each record of same brand and extract a line list

    if (len(unique_line_set) != 0):
        for line_size_1 in unique_line_set:
            for line_size_2 in unique_line_set:
                if(len(line_size_1.split()) <= len(line_size_2.split())):
                    if(line_size_1 != line_size_2):
                        word_list_1 = line_size_1.split()
                        word_list_2 = line_size_2.split()
                        concat_word_list = ""
                        for ind in range(len(line_size_1.split())):
                            if(word_list_1[ind] == word_list_2[ind]):
                                concat_word_list = concat_word_list + " " + word_list_1[ind]
                            else:
                                break
                        concat_word_list = concat_word_list.strip()
                        unique_line_set_final.add(concat_word_list)
    if "" in unique_line_set_final:
        unique_line_set_final.remove('')

    # follow the same procedure as how the brand was extracted in the previous step to extract
    # line. Once the line is extracted, the remaining string will be the size.

    for index2, row2 in df_cigar_af.iterrows():
        if(row2['extract_brand'].lower() == row1['Brand cleaned'].lower()):
            x = row2['title_unfiltered'].lower().replace(row2['extract_brand'].lower(), "").strip()
            for value in unique_line_set_final:
                if x is not None:
                    word_list_1 = x.split()
                    number_of_words_1 = len(word_list_1)
                    word_list_2 = value.split()
                    number_of_words_2 = len(word_list_2)
            
                    if(number_of_words_1 > number_of_words_2):
                        title_list_cigar_af = x.split()[:number_of_words_2]
                        title_cigar_af = ' '.join(title_list_cigar_af)
            
                    if(title_cigar_af.lower() == value.lower()):
                        df_cigar_af.loc[df_cigar_af.index[index2], 'extract_line'] = value.lower()
                        y = x.lower().replace(value.lower(), "").strip()
                        df_cigar_af.loc[df_cigar_af.index[index2], 'extract_size'] = y.lower()


# In[76]:


df_cigar_af.to_csv('cigar_af_transform.csv', header=True, index=False)


# cigar_geek_extract_brand_line_size

# In[77]:


# extract brand by comparing cigar geek data frame records with brand list

for index1, row1 in df_cigar_geek.iterrows():
    for index2, row2 in df_brand.iterrows():
        if row1['title_unfiltered'] is not None:
            word_list_1 = row1['title_unfiltered'].split()
            number_of_words_1 = len(word_list_1)
            word_list_2 = row2['Brand cleaned'].split()
            number_of_words_2 = len(word_list_2)
            
            # if the number of words in cigar_geek record is > number of words in brand list record
            # then slice the cigar_geek title column to the size of brand list record and compare
            # if the ratio is > 90, then the brand is similar. So extract the brand and save in the 
            # new column 

            if(number_of_words_1 > number_of_words_2):
                title_list_cigar_geek = row1['title_unfiltered'].split()[:number_of_words_2]
                title_cigar_geek = ' '.join(title_list_cigar_geek)
            
            if(fuzz.ratio(title_cigar_geek.lower(),row2['Brand cleaned'].lower()) > 90):
                df_cigar_geek.loc[df_cigar_geek.index[index1], 'extract_brand'] = title_cigar_geek.lower()
                df_cigar_geek.loc[df_cigar_geek.index[index1], 'title_unfiltered'] = df_cigar_geek.loc[df_cigar_geek.index[index1], 'title_unfiltered'].lower().replace(title_cigar_geek.lower(), row2['Brand cleaned'].lower()).strip()


# In[78]:


# extract line and size of cigar_geek

# compare each brand with the cigar_geek extract_brand column. If it is a match,
# get the line and size by replacing brand value to '' and add the result to a set
# so only unique line and size value will be available 

for index1, row1 in df_brand.iterrows():
    unique_line_set = set()
    unique_line_set_final = set()
    for index2, row2 in df_cigar_geek.iterrows():
        if(row2['extract_brand'].lower() == row1['Brand cleaned'].lower()):
            x = row2['title_unfiltered'].lower().replace(row2['extract_brand'].lower(), "").strip()
            if(x != "" and len(x.split()) > 1):
                unique_line_set.add(x)

    # get the line value alone by comparing each record of same brand and extract a line list

    if (len(unique_line_set) != 0):
        for line_size_1 in unique_line_set:
            for line_size_2 in unique_line_set:
                if(len(line_size_1.split()) <= len(line_size_2.split())):
                    if(line_size_1 != line_size_2):
                        word_list_1 = line_size_1.split()
                        word_list_2 = line_size_2.split()
                        concat_word_list = ""
                        for ind in range(len(line_size_1.split())):
                            if(word_list_1[ind] == word_list_2[ind]):
                                concat_word_list = concat_word_list + " " + word_list_1[ind]
                            else:
                                break
                        concat_word_list = concat_word_list.strip()
                        unique_line_set_final.add(concat_word_list)
    if "" in unique_line_set_final:
        unique_line_set_final.remove('')

    # follow the same procedure as how the brand was extracted in the previous step to extract
    # line. Once the line is extracted, the remaining string will be the size.

    for index2, row2 in df_cigar_geek.iterrows():
        if(row2['extract_brand'].lower() == row1['Brand cleaned'].lower()):
            x = row2['title_unfiltered'].lower().replace(row2['extract_brand'].lower(), "").strip()
            for value in unique_line_set_final:
                if x is not None:
                    word_list_1 = x.split()
                    number_of_words_1 = len(word_list_1)
                    word_list_2 = value.split()
                    number_of_words_2 = len(word_list_2)
            
                    if(number_of_words_1 > number_of_words_2):
                        title_list_cigar_geek = x.split()[:number_of_words_2]
                        title_cigar_geek = ' '.join(title_list_cigar_geek)
            
                    if(title_cigar_geek.lower() == value.lower()):
                        df_cigar_geek.loc[df_cigar_geek.index[index2], 'extract_line'] = value.lower()
                        y = x.lower().replace(value.lower(), "").strip()
                        df_cigar_geek.loc[df_cigar_geek.index[index2], 'extract_size'] = y.lower()


# In[ ]:


df_cigar_geek.to_csv('cigar_geek_transform.csv', header=True, index=False)


# cigar_scanner_extract_brand_line_size

# In[ ]:


# extract brand by comparing cigar scanner data frame records with brand list

for index1, row1 in df_cigar_scanner.iterrows():
    for index2, row2 in df_brand.iterrows():
        if row1['title_unfiltered'] is not None:
            word_list_1 = row1['title_unfiltered'].split()
            number_of_words_1 = len(word_list_1)
            word_list_2 = row2['Brand cleaned'].split()
            number_of_words_2 = len(word_list_2)
            
            # if the number of words in cigar_scanner record is > number of words in brand list record
            # then slice the cigar_scanner title column to the size of brand list record and compare
            # if the ratio is > 90, then the brand is similar. So extract the brand and save in the 
            # new column 

            if(number_of_words_1 > number_of_words_2):
                title_list_cigar_scanner = row1['title_unfiltered'].split()[:number_of_words_2]
                title_cigar_scanner = ' '.join(title_list_cigar_scanner)
            
            if(fuzz.ratio(title_cigar_scanner.lower(),row2['Brand cleaned'].lower()) > 90):
                df_cigar_scanner.loc[df_cigar_scanner.index[index1], 'extract_brand'] = title_cigar_scanner.lower()
                df_cigar_scanner.loc[df_cigar_scanner.index[index1], 'title_unfiltered'] = df_cigar_scanner.loc[df_cigar_scanner.index[index1], 'title_unfiltered'].lower().replace(title_cigar_scanner.lower(), row2['Brand cleaned'].lower()).strip()
                df_cigar_scanner.loc[df_cigar_scanner.index[index1], 'title_filtered'] = df_cigar_scanner.loc[df_cigar_scanner.index[index1], 'title_filtered'].lower().replace(title_cigar_scanner.lower(), row2['Brand cleaned'].lower()).strip()


# In[ ]:


# extract line and size of cigar_scanner

# compare each brand with the cigar_scanner extract_brand column. If it is a match,
# get the line and size by replacing brand value to '' and add the result to a set
# so only unique line and size value will be available 

for index1, row1 in df_brand.iterrows():
    unique_line_set = set()
    unique_line_set_final = set()
    for index2, row2 in df_cigar_scanner.iterrows():
        if(row2['extract_brand'].lower() == row1['Brand cleaned'].lower()):
            x = row2['title_unfiltered'].lower().replace(row2['extract_brand'].lower(), "").strip()
            if(x != "" and len(x.split()) > 1):
                unique_line_set.add(x)

    # get the line value alone by comparing each record of same brand and extract a line list

    if (len(unique_line_set) != 0):
        for line_size_1 in unique_line_set:
            for line_size_2 in unique_line_set:
                if(len(line_size_1.split()) <= len(line_size_2.split())):
                    if(line_size_1 != line_size_2):
                        word_list_1 = line_size_1.split()
                        word_list_2 = line_size_2.split()
                        concat_word_list = ""
                        for ind in range(len(line_size_1.split())):
                            if(word_list_1[ind] == word_list_2[ind]):
                                concat_word_list = concat_word_list + " " + word_list_1[ind]
                            else:
                                break
                        concat_word_list = concat_word_list.strip()
                        unique_line_set_final.add(concat_word_list)
    if "" in unique_line_set_final:
        unique_line_set_final.remove('')

    # follow the same procedure as how the brand was extracted in the previous step to extract
    # line. Once the line is extracted, the remaining string will be the size.

    for index2, row2 in df_cigar_scanner.iterrows():
        if(row2['extract_brand'].lower() == row1['Brand cleaned'].lower()):
            x = row2['title_unfiltered'].lower().replace(row2['extract_brand'].lower(), "").strip()
            for value in unique_line_set_final:
                if x is not None:
                    word_list_1 = x.split()
                    number_of_words_1 = len(word_list_1)
                    word_list_2 = value.split()
                    number_of_words_2 = len(word_list_2)
            
                    if(number_of_words_1 > number_of_words_2):
                        title_list_cigar_scanner = x.split()[:number_of_words_2]
                        title_cigar_scanner = ' '.join(title_list_cigar_scanner)
            
                    if(title_cigar_scanner.lower() == value.lower()):
                        df_cigar_scanner.loc[df_cigar_scanner.index[index2], 'extract_line'] = value.lower()
                        y = x.lower().replace(value.lower(), "").strip()
                        df_cigar_scanner.loc[df_cigar_scanner.index[index2], 'extract_size'] = y.lower()


# In[ ]:


df_cigar_scanner.to_csv('cigar_scanner_transform.csv', header=True, index=False)


# In[47]:


# df_brand.to_csv('df_brand.csv', header=True, index=False)


# DF To Big Query Table

# In[ ]:



# cred = service_account.Credentials.from_service_account_file(
# 'coastal-set-346210-7412c02a4f9f.json')

# table_id = 'coastal-set-346210.Cigar_Review_DataSet.cigar_af_transform'
# table_id2 = 'coastal-set-346210.Cigar_Review_DataSet.cigar_geek_transform'
# table_id3 = 'coastal-set-346210.Cigar_Review_DataSet.cigar_scanner_transform'

# df_cigar_af = df_cigar_af.astype(str)
# df_cigar_geek = df_cigar_geek.astype(str)
# df_cigar_scanner = df_cigar_scanner.astype(str)

# df_cigar_af.to_gbq(destination_table=table_id,     project_id='coastal-set-346210', if_exists='fail', credentials=cred)

# df_cigar_geek.to_gbq(destination_table=table_id2,     project_id='coastal-set-346210', if_exists='fail', credentials=cred)

# df_cigar_scanner.to_gbq(destination_table=table_id3,     project_id='coastal-set-346210', if_exists='fail', credentials=cred)    

