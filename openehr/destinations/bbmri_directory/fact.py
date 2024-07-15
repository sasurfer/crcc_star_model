import pandas as pd

from openehr.destinations.bbmri_directory.utils import compute_array_age_ranges_df, compute_last_update
from openehr.excel import append_df_to_excel


# columns= ('sample_id', 'sample_disease', 'sample_material_type', 'sample_sex', 'sample_age', 'sample_biobank',
#                  'sample_participant')
def generate_fact_table(bbmri_dir_file, samples_df, n_asterisks):
    fact_table_rows = list()
    last_update = compute_last_update()
    #withdrawn = False  # TODO: verify if it can be false for all rows
    fact_table_counter = 1
    bb_filtered_df = samples_df
    biobank_diseases = bb_filtered_df['sample_disease'].unique()
    bbmri_biobank_id = bb_filtered_df.loc[0]['sample_biobank']
    bbmri_collection_id = bb_filtered_df.loc[0]['sample_collection']
    national_node = 'EU'

    fact_table_id_part=bbmri_collection_id.replace("bbmri-eric:ID", "bbmri-eric:factID")
    
    ### ****
    if n_asterisks > 0:
        nsamples = len(bb_filtered_df)
        ndonors = len(bb_filtered_df['sample_participant'].unique())
        fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
        fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', '*', '*', '*',
                                        nsamples, ndonors, last_update,
                                        national_node])
        fact_table_counter += 1

    # 1.disease
    for disease in biobank_diseases:
        disease_filtered_df = bb_filtered_df.query(f'sample_disease=="{disease}"')

        ### D***
        if n_asterisks > 0:
            nsamples = len(disease_filtered_df)
            ndonors = len(disease_filtered_df['sample_participant'].unique())
            fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
            fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', '*', '*',
                                    disease, nsamples, ndonors, last_update,
                        national_node])
            fact_table_counter += 1

        # 2. Age range
        rdage = compute_array_age_ranges_df(disease_filtered_df)
        for rda in rdage:
            age_at_sampling_filtered_df = rda[0]
            assigned_ages = rda[1]
            age_at_sampling = rda[2]

            if n_asterisks > 34:
                ### DA**
                nsamples = len(age_at_sampling_filtered_df)
                ndonors = len(age_at_sampling_filtered_df['sample_participant'].unique())
                fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', assigned_ages, '*',
                            disease, nsamples, ndonors, last_update,
                            national_node])
                fact_table_counter += 1

            # 3. sex
            sex_val = age_at_sampling_filtered_df['sample_sex'].unique()
            for s in sex_val:
                sex_filtered_df = age_at_sampling_filtered_df.query(f'sample_sex=="{s}"')
                sex = s

                if n_asterisks > 34:
                    ### DAS*
                    nsamples = len(sex_filtered_df)
                    ndonors = len(sex_filtered_df['sample_participant'].unique())
                    fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                    fact_table_rows.append([fact_table_id, bbmri_collection_id, sex, assigned_ages, '*',
                                disease, nsamples, ndonors, last_update,
                                national_node])
                    fact_table_counter += 1

                # 4. material type
                # determine current_material_type
                material_types = sex_filtered_df['sample_material_type'].unique()
                # now drill down also with material
                for material in material_types:
                    material_filtered_df = sex_filtered_df.query(f'sample_material_type=="{material}"')

                    fact_material_type = material
                    # number of samples: it is simply the dimension of the biobank_disease_filtered_df
                    number_of_samples = len(material_filtered_df)

                    # calculate the number of donors: before, calculate the unique donors for every biobank
                    unique_donors = material_filtered_df['sample_participant'].unique()
                    number_of_donors = len(unique_donors)

                    # generate facts line and add it to df
                    fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                    fact_table_rows.append([fact_table_id, bbmri_collection_id, sex, assigned_ages,
                                            fact_material_type, disease,
                                            number_of_samples, number_of_donors, last_update,
                                            national_node])
                    fact_table_counter += 1
                                
            material_types4 = age_at_sampling_filtered_df['sample_material_type'].unique()
            for material4 in material_types4:
                material_filtered_df4 = age_at_sampling_filtered_df.query(
                    f'sample_material_type=="{material4}"')

                if n_asterisks > 34:
                    ## DA*M
                    nsamples = len(material_filtered_df4)
                    ndonors = len(material_filtered_df4['sample_participant'].unique())
                    fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                    fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', assigned_ages, material4,
                                disease, nsamples, ndonors, last_update,
                                national_node])
                    fact_table_counter += 1

        sex_val2 = disease_filtered_df['sample_sex'].unique()
        for s2 in sex_val2:
            sex_filtered_df2 = disease_filtered_df.query(f'sample_sex=="{s2}"')
            sex2 = s2

            if n_asterisks > 34:
                ### D*S*
                nsamples = len(sex_filtered_df2)
                ndonors = len(sex_filtered_df2['sample_participant'].unique())
                fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                fact_table_rows.append([fact_table_id, bbmri_collection_id, sex2, '*', '*',
                            disease, nsamples, ndonors, last_update,
                            national_node])
                fact_table_counter += 1

            material_types2 = sex_filtered_df2['sample_material_type'].unique()
            for material2 in material_types2:
                material_filtered_df2 = sex_filtered_df2.query(f'sample_material_type=="{material2}"')

                if n_asterisks > 34:
                    ## D*SM
                    nsamples = len(material_filtered_df2)
                    ndonors = len(material_filtered_df2['sample_participant'].unique())
                    fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                    fact_table_rows.append([fact_table_id, bbmri_collection_id, sex2, '*', material2,
                                disease, nsamples, ndonors, last_update,
                                national_node])
                    fact_table_counter += 1

        material_types3 = disease_filtered_df['sample_material_type'].unique()
        for material3 in material_types3:
            material_filtered_df3 = disease_filtered_df.query(f'sample_material_type=="{material3}"')

            if n_asterisks > 34:
                ## D**M
                nsamples = len(material_filtered_df3)
                ndonors = len(material_filtered_df3['sample_participant'].unique())
                fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', '*', material3,
                            disease, nsamples, ndonors, last_update,
                            national_node])
                fact_table_counter += 1

    rdage2 = compute_array_age_ranges_df(bb_filtered_df)
    for rda2 in rdage2:
        age_at_sampling_filtered_df2 = rda2[0]
        assigned_ages = rda2[1]
        age_at_sampling = rda2[2]
        ### *A**
        if n_asterisks > 0:
            nsamples = len(age_at_sampling_filtered_df2)
            ndonors = len(age_at_sampling_filtered_df2['sample_participant'].unique())
            fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
            fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', assigned_ages, '*', '*', nsamples, ndonors,
                        last_update, national_node])
            fact_table_counter += 1

        sex_val3 = age_at_sampling_filtered_df2['sample_sex'].unique()
        for s3 in sex_val3:
            sex_filtered_df3 = age_at_sampling_filtered_df2.query(f'sample_sex=="{s3}"')
            sex3 = s3

            if n_asterisks > 34:
                ## *AS*
                nsamples = len(sex_filtered_df3)
                ndonors = len(sex_filtered_df3['sample_participant'].unique())
                fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                fact_table_rows.append([fact_table_id, bbmri_collection_id, sex3, assigned_ages, '*', '*', nsamples, ndonors,
                            last_update, national_node])
                fact_table_counter += 1

            material_types5 = sex_filtered_df3['sample_material_type'].unique()
            for material5 in material_types5:
                material_filtered_df5 = sex_filtered_df3.query(f'sample_material_type=="{material5}"')

                if n_asterisks > 34:
                    ## *ASM
                    nsamples = len(material_filtered_df5)
                    ndonors = len(material_filtered_df5['sample_participant'].unique())
                    fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                    fact_table_rows.append([fact_table_id, bbmri_collection_id, sex3, assigned_ages, material5, '*', nsamples,
                                ndonors, last_update, national_node])
                    fact_table_counter += 1

        material_types6 = age_at_sampling_filtered_df2['sample_material_type'].unique()
        for material6 in material_types6:
            material_filtered_df6 = age_at_sampling_filtered_df2.query(f'sample_material_type=="{material6}"')

            if n_asterisks > 34:
                ## *A*M
                nsamples = len(material_filtered_df6)
                ndonors = len(material_filtered_df6['sample_participant'].unique())
                fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', assigned_ages, material6, '*', nsamples, ndonors,
                            last_update, national_node])
                fact_table_counter += 1

    sex_val4 = bb_filtered_df['sample_sex'].unique()
    for s4 in sex_val4:
        sex_filtered_df4 = bb_filtered_df.query(f'sample_sex=="{s4}"')
        sex4 = s4

        if n_asterisks > 0:
            ### **S*
            nsamples = len(sex_filtered_df4)
            ndonors = len(sex_filtered_df4['sample_participant'].unique())
            fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
            fact_table_rows.append([fact_table_id, bbmri_collection_id, sex4, '*', '*', '*', nsamples, ndonors, last_update,
                        national_node])
            fact_table_counter += 1

        material_types7 = sex_filtered_df4['sample_material_type'].unique()
        for material7 in material_types7:
            material_filtered_df7 = sex_filtered_df4.query(f'sample_material_type=="{material7}"')

            if n_asterisks > 34:
                ## **SM
                nsamples = len(material_filtered_df7)
                ndonors = len(material_filtered_df7['sample_participant'].unique())
                fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
                fact_table_rows.append([fact_table_id, bbmri_collection_id, sex4, '*', material7, '*', nsamples, ndonors,
                            last_update, national_node])
                fact_table_counter += 1

    material_types8 = bb_filtered_df['sample_material_type'].unique()
    for material8 in material_types8:
        material_filtered_df8 = bb_filtered_df.query(f'sample_material_type=="{material8}"')

        if n_asterisks > 0:
            ### ***M
            nsamples = len(material_filtered_df8)
            ndonors = len(material_filtered_df8['sample_participant'].unique())
            fact_table_id = f'{fact_table_id_part}:{fact_table_counter}'
            fact_table_rows.append([fact_table_id, bbmri_collection_id, '*', '*', material8, '*', nsamples, ndonors, last_update,
                        national_node])
            fact_table_counter += 1



    fact_table_df = create_fact_table_df(fact_table_rows)
    append_df_to_excel(bbmri_dir_file, fact_table_df, sheet_name='eu_bbmri_eric_facts',
                       index=False)
    return fact_table_df


def create_fact_table_df(fact_table_rows):
    return pd.DataFrame([f for f in fact_table_rows],
                        columns=['id',
                                 'collection',
                                 'sex',
                                 'age_range',
                                 'sample_type',
                                 'disease',
                                 'number_of_samples',
                                 'number_of_donors',
                                 'last_update',
                                 'national_node'
                                 ])
