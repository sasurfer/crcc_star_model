
import pandas as pd
import sys

def get_sample_id(sample_id,patient_id):
    newsample_id=sample_id
    if sample_id==None:
            newsample_id=patient_id+'_1'
    return newsample_id

def findcode(location):
    return location.split('-')[0].replace(' ','')

def surgery_consistency_check(surgery_location,surgery_type):
    loc=findcode(surgery_location)
    stype=surgery_type

    #controllare tutti i possibili surgery_type che abbiamo nel file per vedere se coincidono con i test che stiamo facendo

    if loc == "C18.0" or loc == "C18.1" or loc == "C18.2":
        if stype == "Right hemicolectomy" or stype == "Other":
            return True
        elif stype == "Pan-procto colectomy" or stype == "Total colectomy":#suspect
            return False
        else:#invalid
            return False
    elif loc == "C18.3":
        if stype == "Right hemicolectomy" or stype == "Other":
            return True
        elif stype == "Pan-procto colectomy" or stype == "Total colectomy" or stype == "Transverse colectomy":#suspect
             return False
        else:#invalid
             return False
    elif loc == "C18.4":
        if stype == "Transverse colectomy" or stype == "Other":
            return True
        elif stype == "Left hemicolectomy" or stype == "Pan-procto colectomy" or stype == "Right hemicolectomy" or stype == "Total colectomy":#suspect
            return False
        else:#invalid
            return False
    elif loc == "C18.5":
        if stype == "Left hemicolectomy" or stype == "Other":
            return True
        elif stype == "Abdomino-perineal resection" or stype == "Pan-procto colectomy" or stype == "Total colectomy" or stype == "Sigmoid colectomy" or stype == "Transverse colectomy":#suspect
            return False
        else:#invalid
            return False
    elif loc == "C18.6":
        if stype == "Left hemicolectomy" or stype == "Other":
            return True
        elif stype == "Abdomino-perineal resection" or stype == "Pan-procto colectomy" or stype == "Total colectomy" or stype == "Sigmoid colectomy":#suspect
            return False
        else:#invalid
            return False
    elif loc == "C18.7": 
        if stype == "Sigmoid colectomy" or stype == "Left hemicolectomy" or stype == "Other":
            return True
        elif stype == "Abdomino-perineal resection" or stype == "Low anterior colon resection" or stype == "Pan-procto colectomy" or stype == "Total colectomy":#suspect
            return False
        else:#invalid
            return False 
        # this shoud not occur in CRC-cohort but we keep it for sake of completeness
    elif loc == "C18.8":
        # all is permitted
        return True
    elif loc == "C18.9":
        if stype == "Left hemicolectomy" or stype == "Pan-procto colectomy" or stype == "Right hemicolectomy" or stype == "Sigmoid colectomy" or stype == "Total colectomy" or stype == "Transverse colectomy" or stype == "Other":
            return True
        elif stype == "Abdomino-perineal resection" or stype == "Low anterior colon resection":#suspect
            return False 
        else:#invalid
            return False
    elif loc == "C19" or loc == "C19.9":
        if stype == "Anterior resection of rectum" or stype == "Endo-rectal tumor resection" or stype == "Low anterior colon resection" or stype == "Sigmoid colectomy" or stype == "Other":
            return True
        elif stype == "Abdomino-perineal resection" or stype == "Left hemicolectomy" or stype == "Pan-procto colectomy" or stype == "Total colectomy":#suspect
            return False
        else:#invalid
            return False
    elif loc == "C20" or loc == "C20.9":
        if stype == "Abdomino-perineal resection" or stype == "Anterior resection of rectum" or stype == "Endo-rectal tumor resection" or stype == "Low anterior colon resection" or stype == "Other":
            return True
        elif stype == "Left hemicolectomy" or stype == "Pan-procto colectomy" or stype == "Total colectomy":#suspect
            return False
        else:#invalid
            return False


def get_sample_disease(localization,surgery_location,surgery_type):
    if localization != None:
            code=findcode(localization)          
            return "urn:miriam:icd:"+code
    else:
        if surgery_location!=None:
            if surgery_consistency_check(surgery_location,surgery_type):
                    code=findcode(surgery_location)
                    return "urn:miriam:icd:"+code
            else:
                return "urn:miriam:icd:C18.9"
        return "urn:miriam:icd:C18.9"
        

def get_sample_material(sample_material,sample_preservation):
    if sample_material==None or sample_preservation==None:
        return 'NAV'
    elif sample_material=='Other specimen type' or sample_preservation=='Other':
        return 'OTHER'
    elif sample_preservation=='Cryopreservation':
        return 'TISSUE_FROZEN'
    elif sample_preservation=='FFPE':
        return 'TISSUE_PARAFFIN_EMBEDDED'
    else:
        print(f'unexpected values: sample_material={sample_material} sample_preservation={sample_preservation}')


def get_sex(sex):
    if sex==None:
        return 'Not available'
    elif sex=='MALE':
        return 'MALE'
    elif sex=='FEMALE':
        return 'FEMALE'
    elif sex=='OTHER':
        return 'UNKNOWN'

def get_age(age):
    if age != None:
        if age.endswith('Y'):
            return age[1:-1]
        elif age.endswith('M'):
            return int(int(age[1:-1])/12.)
        elif  age.endswith('W'):
            return int(int(age[1:-1])/52.)
        elif age.endswith('D'):
            return int(int(age[1:-1])/365.)    
        return -1
    return -1


def process_patient_data(patient_data):
    #from biobank,patient_id,sample_id,sample_material,sample_preservation,surgery_location,surgery_type,localization_of_primary_tumor,age_at_primary_diagnosis,sex
    #to dataframe with columns
    #columns=('sample_id', 'sample_disease', 'sample_material_type', 'sample_sex', 'sample_age', 'sample_biobank','sample_participant')


    biobank="bbmri-eric:ID:EU_BBMRI-ERIC"
    collection="bbmri-eric:ID:EU_BBMRI-ERIC:collection:CRC-Cohort"

    sample_data=[]
    null=None


    for p in patient_data:
        p_p_id=p[1]
        p_s_id=p[2]
        p_s_mat=p[3]
        p_s_pre=p[4]
        p_s_loc=p[5]
        p_s_type=p[6]
        p_localiz=p[7]
        p_age=p[8]
        p_sex=p[9]
        if p_age == None and p_sex == None:
            print(f'patient {p_p_id} ignored. No sex nor age defined')
            continue
        sample_id=get_sample_id(p_s_id,p_p_id)
        sample_disease=get_sample_disease(p_localiz,p_s_loc,p_s_type)
        sample_material_type=get_sample_material(p_s_mat,p_s_pre)
        sample_sex=get_sex(p_sex)
        sample_age=get_age(p_age)
        sample_biobank=biobank
        sample_collection=collection
        sample_participant=p_p_id
        sample_data.append([sample_id,sample_disease,sample_material_type,sample_sex,sample_age,sample_biobank,sample_participant,sample_collection])

    #create df from array sample_data
    df = pd.DataFrame([s for s in sample_data],
                        columns=['sample_id', 'sample_disease', 'sample_material_type','sample_sex', 'sample_age', 'sample_biobank','sample_participant','sample_collection'])
    df['sample_age'] = df['sample_age'].astype('int',errors="ignore")
    return df

