from datetime import datetime

import pandas as pd
import numpy as np

import asyncio
import zipfile
import time
import os

from openehr.excel import append_df_to_excel

"""
Defines the age range categories in which the ages have been aggregated in the fact table
"""
age_range_categories = {
    'Infant': (0, 2),
    'Child': (2, 13),
    'Adolescent': (13, 18),
    'Young Adult': (18, 25),
    'Adult': (25, 45),
    'Middle-aged': (45, 65),
    'Aged (65-79 years)': (65, 80),
    'Aged (>80 years)': (80, 150)
}


def add_missing_diseases(bbmri_dir_file, disease_types):
    #Add * to directory excel file
    asterisk_row=["*", "Any", "", "", "", "", "", "", ""]
    missing_diseases_bbmri_df = pd.DataFrame([asterisk_row])
    append_df_to_excel(bbmri_dir_file, missing_diseases_bbmri_df, sheet_name='eu_bbmri_eric_disease_types',
                       index=False)  # Append another "df" to the sheet "Cool" starting from col 5


    #Add * to directory csv file
    asterisk_row=[np.nan,"*", "Any", np.nan, np.nan, "*", np.nan, np.nan, np.nan, np.nan,np.nan]
    disease_types.loc[len(disease_types.index)] = asterisk_row
    
    return disease_types

def add_missing_material_types(bbmri_dir_file,material_types):
    #Add * to directory excel file
    asterisk_row=["*", "Any", "", ""]
    missing_materials_bbmri_df = pd.DataFrame([asterisk_row])
    append_df_to_excel(bbmri_dir_file, missing_materials_bbmri_df, sheet_name='eu_bbmri_eric_material_types',
                       index=False)  # Append another "df" to the sheet "Cool" starting from col 5

    #Add * to directory csv file
    asterisk_row=[np.nan,"*", "Any", np.nan, np.nan,"*",np.nan,np.nan]
    material_types.loc[len(material_types.index)] = asterisk_row
    return material_types 

def add_missing_sex_types(bbmri_dir_file,sex_types):
    #Add asterisk to directory excel file
    asterisk_row=["*", "Any", "", "", ""]
    missing_sex_bbmri_df = pd.DataFrame([asterisk_row])
    append_df_to_excel(bbmri_dir_file, missing_sex_bbmri_df, sheet_name='eu_bbmri_eric_sex_types',
                       index=False)  

    #Add asterisk to directory csv file
    asterisk_row=[np.nan,"*", "Any", np.nan, np.nan,"*",np.nan,np.nan]
    sex_types.loc[len(sex_types.index)] = asterisk_row 
    return sex_types 

def add_missing_age_ranges(bbmri_dir_file,age_ranges):
    #Add asterisk to directory excel file
    asterisk_row=["*", "Any", "", "", ""]
    missing_age_ranges_bbmri_df = pd.DataFrame([asterisk_row])
    append_df_to_excel(bbmri_dir_file, missing_age_ranges_bbmri_df, sheet_name='eu_bbmri_eric_AgeRanges',
                       index=False)  

    #Add asterisk to directory csv file
    asterisk_row=[np.nan,"*", "Any", np.nan, np.nan,"*",np.nan,np.nan]
    age_ranges.loc[len(age_ranges.index)] = asterisk_row 
    return age_ranges 



def compute_array_age_ranges_df(df):
    """
    Creates an array containing for each age_range (plus the undefined and the unknown):
    -dataframe subset of input df
    -age range name for fact cell
    -age range name for fact id
    :param df: original dataframe
    :return array of array with dataframe subset of df in position 0, age range name in position 1,
    and age range name for fact id in position 2
    """
    rdage = []
    # NaN
    rd = df[df['sample_age'].isnull()]
    rdage.append([rd, 'Unknown', 'unknown'])
    # -1
    rd = df[df['sample_age'] == -1]
    rdage.append([rd, 'Undefined', 'undefined'])
    # age_range_categories
    for k in age_range_categories:
        (r1, r2) = age_range_categories[k]
        rd = df[df['sample_age'].between(r1, r2 - 1, inclusive='both')]
        forid = k.replace("(", "").replace(")", "").replace("years", "").strip().replace(' ', '').lower()
        rdage.append([rd, k, forid])
    return rdage


def compute_last_update():
    now = datetime.now()
    return datetime.strftime(now, '%Y-%m-%d')

def generate_csv(outdir,filename,data_df):
    data_df.to_csv(outdir+'/'+filename,index=False)

async def zip_files(outdir,myzipfile,listoffiles):
    zip_file = zipfile.ZipFile(outdir+'/'+myzipfile, 'w')
    for f in listoffiles:
        zip_file.write(outdir+'/'+f,f)
    zip_file.close()
    print(f'Zip file {myzipfile} with files {listoffiles} created successfully in dir {outdir}.')

def wait_for_file(filepath, timeout=None):
    start_time = time.time()
    while True:
        if os.path.exists(filepath):
            current_size = os.stat(filepath).st_size
            time.sleep(1)  # Adjust sleep time based on your requirement
            new_size = os.stat(filepath).st_size
            if current_size == new_size:
                print(f"File '{filepath}' has been fully written.")
                break
        if timeout is not None and (time.time() - start_time) > timeout:
            print("Timeout exceeded. File not fully written.")
            break
