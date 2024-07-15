import os
import shutil

import pandas as pd
import yaml

import asyncio

import json

from openehr.client import openEHRClient
from openehr.process import process_patient_data
from openehr.destinations.bbmri_directory.fact import generate_fact_table
from openehr.destinations.bbmri_directory.utils import add_missing_diseases, add_missing_material_types,add_missing_sex_types, add_missing_age_ranges,generate_csv,zip_files,wait_for_file
#from openehr.queries import get_unique_crcc_diseases_codes, get_unique_crcc_material_types


def main():
    with open('config.yaml') as f:
        cfg = yaml.load(f, Loader=yaml.FullLoader)

    hostname = cfg['openehr_server']['hostname']
    port = cfg['openehr_server']['port']
    username = cfg['openehr_server']['user']
    password = cfg['openehr_server']['password']

    openehrclient = openEHRClient(hostname, port , username, password)

    openehrclient.create_session()

    bbmri_directory_data = cfg['bbmri']['directory_data']
    csv_in_dir = cfg['bbmri']['csv_input_dir']
    csv_out_dir = cfg['bbmri']['csv_output_dir']
    out_dir = cfg['bbmri']['fact_output_dir']
    bbmri_directory_with_fact_data = f'{out_dir}/{cfg["bbmri"]["directory_with_fact_file_name"]}'

    #read bbmri dimensions ontology
    age_ranges=pd.read_csv(csv_in_dir+'/AgeRanges.csv')
    # ar=age_ranges.shape[0]
    disease_types=pd.read_csv(csv_in_dir+'/DiseaseTypes.csv')
    # dt=disease_types.shape[0]
    sex_types=pd.read_csv(csv_in_dir+'/SexTypes.csv')
    # st=sex_types.shape[0]
    material_types=pd.read_csv(csv_in_dir+'/MaterialTypes.csv')
    # mt=material_types.shape[0]

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    
    if not os.path.exists(csv_out_dir):
        os.makedirs(csv_out_dir)

    # BBMRI_OUTPUT = './rd_connect/destinations/bbmri_directory/output/eu_bbmri_eric_directory_acceptance_server_with_fact.xlsx'

    shutil.copy(bbmri_directory_data, bbmri_directory_with_fact_data)

    n_asterisks=int(cfg['fact']['number_of_asterisks'])
    if n_asterisks==0:
        print(f'run with no asterisks')
    elif n_asterisks==34:
        print(f'run with 3 and 4 asterisks')
    elif n_asterisks==1234:
        print(f'run with 1,2,3 and 4 asterisks')
    else:
        print(f'value allowed for asterisks are \n0->no asterisk \n34->3 and 4 asterisks \n1234->1,2,3, and 4 asterisks')
        exit(0)

# get openEHR data per patient
    print('querying data from openEHR server')
    crcc_patient_data=openehrclient.get_patient_data()

    # print(type(patient_data))
    # with open('pippo','w') as f:
    #     f.write(json.dumps(patient_data))

    print('processing data')
    crcc_samples_df=process_patient_data(crcc_patient_data)
    generate_csv(csv_out_dir,'Samples.csv',crcc_samples_df)

    if n_asterisks>0:
        print("Add asterisk to disease types")
        disease_types=add_missing_diseases(bbmri_directory_with_fact_data,disease_types)

        print("Add asterisk to material types")
        material_types=add_missing_material_types(bbmri_directory_with_fact_data, material_types)
        
        print("Add asterisk to sex types")
        sex_types=add_missing_sex_types(bbmri_directory_with_fact_data, sex_types)

        print("Add asterisk to age ranges")
        age_ranges=add_missing_age_ranges(bbmri_directory_with_fact_data, age_ranges)
    
        #generate csv for dimensions ontologies in output
        print('generating csv for dimensions ontologies')
        generate_csv(csv_out_dir,'DiseaseTypes.csv',disease_types)
        generate_csv(csv_out_dir,'MaterialTypes.csv',material_types)
        generate_csv(csv_out_dir,'SexTypes.csv',sex_types)
        generate_csv(csv_out_dir,'AgeRanges.csv',age_ranges)
        listcsv=['DiseaseTypes.csv','MaterialTypes.csv','SexTypes.csv','AgeRanges.csv']
        print(f'Writing zip file Ontologies.zip with {listcsv} inside') 
        asyncio.run(zip_files(csv_out_dir,'Ontologies.zip',listcsv))

    print("Creating fact table")
    fact_table=generate_fact_table(bbmri_directory_with_fact_data, crcc_samples_df, n_asterisks)
    generate_csv(csv_out_dir,'CollectionFacts.csv',fact_table)

    #create fact zip file
    print('Waiting for fact file to be written')
    wait_for_file(csv_out_dir+'/CollectionFacts.csv')
    asyncio.run(zip_files(csv_out_dir,'Fact.zip',['CollectionFacts.csv']))


if __name__ == "__main__":
    main()
