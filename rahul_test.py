# import pyarrow.parquet as pq
import pandas as pd

# tc_data = pq.read_table(source='teachers.parquet').to_pandas()
tc_data = pd.read_parquet('teachers.parquet', engine='pyarrow')
st_data = pd.read_csv('students.csv')


st_data['cid'] = st_data['id_fname_lname_email_ssn_address_cid'].map(lambda x:x.split('_')[-1])

st_data['fname'] = st_data['id_fname_lname_email_ssn_address_cid'].map(lambda x:x.split('_')[1])
st_data['lname'] = st_data['id_fname_lname_email_ssn_address_cid'].map(lambda x:x.split('_')[1])


st_data['name'] = st_data['fname'] + '_' + st_data['lname']

tc_data['name'] = tc_data['fname'] + '_' + tc_data['lname']


tc_data['name'] = tc_data['fname'] + '_' + tc_data['lname']

json_data = {}
for index, row in st_data.iterrows():
    for tc_index, tc_row in tc_data.iterrows():
        if row['cid'] == tc_row['cid']:
            json_data[row['name']] = {
                "class": row['cid'],
                "teacher_name": tc_row['name'],
                "teacher_id": tc_row['id']
            }

import json
with open('json_data.json', 'w') as fp:
    json.dump(json_data, fp)




