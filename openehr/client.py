import requests
import base64
import json


# biobank, patient_id, sample_id, sample_material, sample_prevervation_mode,surgery_location, surgery_type, localization_of_primary_tumor,age_at_primary_diagnosis,sex
test_query="SELECT count(c/uid/value) from EHR e contains COMPOSITION c"


patient_query = """ 
SELECT
c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.organisation.v1, 'Biobank']/items[at0001]/value/value AS biobank, 

c/context/other_context[at0001]/items[openEHR-EHR-CLUSTER.case_identification.v0]/items[at0001]/value/value AS patient_id,

c/content[openEHR-EHR-SECTION.adhoc.v1,'Sample']/items[openEHR-EHR-EVALUATION.specimen_summary.v1]/data[at0001]/items[at0002, 'Sample ID']/value/id as sample_id,

c/content[openEHR-EHR-SECTION.adhoc.v1,'Sample']/items[openEHR-EHR-EVALUATION.specimen_summary.v1]/data[at0001]/items[openEHR-EHR-CLUSTER.specimen.v1]/items[at0029]/value/value as sample_material,

c/content[openEHR-EHR-SECTION.adhoc.v1, 'Sample']/items[openEHR-EHR-EVALUATION.specimen_summary.v1]/data[at0001]/items[openEHR-EHR-CLUSTER.specimen.v1]/items[at0079, 'Preservation mode']/value/value as sample_preservation,

c/content[openEHR-EHR-SECTION.adhoc.v1, 'Surgery']/items[openEHR-EHR-ACTION.procedure.v1, 'Surgery']/description[at0001]/items[at0063, 'Location of the tumor']/value/value as surgery_location,

c/content[openEHR-EHR-SECTION.adhoc.v1, 'Surgery']/items[openEHR-EHR-ACTION.procedure.v1, 'Surgery']/description[at0001]/items[at0002, 'Surgery type']/value/value as surgery_type,

c/content[openEHR-EHR-SECTION.result_details.v0, 'Histopathology']/items[at0002]/items[openEHR-EHR-EVALUATION.problem_diagnosis.v1, 'Cancer diagnosis']/data[at0001]/items[at0012, 'Localization of primary tumor']/value/value as localization_of_primary_tumor,

c/content[openEHR-EHR-SECTION.demographics_rcp.v1, 'Patient data']/items[openEHR-EHR-EVALUATION.problem_diagnosis.v1, 'Primary diagnosis']/data[at0001]/items[openEHR-EHR-CLUSTER.timing_nondaily.v1, 'Diagnosis timing']/items[at0006, 'Primary diagnosis']/items[at0009, 'Age at diagnosis']/value/value AS age_at_primary_diagnosis,

c/content[openEHR-EHR-SECTION.demographics_rcp.v1, 'Patient data']/items[openEHR-EHR-EVALUATION.gender.v1]/data[at0002]/items[at0019, 'Biological sex']/value/value AS sex
FROM EHR e contains COMPOSITION c
"""

EHRBASE_QUERY_URL = "{base_url}/query/aql"

class EHRBaseException(Exception):
    pass

def getauth(username,password):
    message=username+":"+password
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    auth="Basic "+base64_message
    return auth


class openEHRClient:

    def __init__(self, hostname, port, username, password):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.url="http://"+hostname+":"+str(port)+"/ehrbase/rest/openehr/v1"
        self.client=None
        self.auth=getauth(self.username,self.password)

    def create_session(self):
        self.client=requests.Session()
        self.client.auth = (self.username,self.password)

    def get_patient_data(self):
        data={'q': patient_query.replace('\n',' ')}
        headers = {'Content-Type': 'application/json','Authorization':self.auth}
        try:
            response = self.client.post(EHRBASE_QUERY_URL.format(base_url=self.url),
                                     headers=headers,
                                     data=json.dumps(data) )
        except requests.exceptions.ConnectionError:
            raise EHRBaseException('Cannot connect to OpenEHR CDR')

        if response.status_code >= 200 and response.status_code <210:
            results = response.json()
        else:
            raise EHRBaseException(f'Error retrieving data openEHR data (code: {response.status_code})')
        return results['rows']

    def __repr__(self):
        return self.url
