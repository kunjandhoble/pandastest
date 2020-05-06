# import pyarrow.parquet as pq
import pandas as pd

# tc_data = pq.read_table(source='teachers.parquet').to_pandas()
tc_data = pd.read_parquet('teachers.parquet', engine='pyarrow')
st_data = pd.read_csv('students.csv')

new_st_data = st_data[st_data.columns[0]].str.split("_", n = 6, expand=True)
# st_data['cid'] = st_data['id_fname_lname_email_ssn_address_cid'].map(lambda x:x.split('_')[-1])

# print(st_data)
# st_data['fname'] = st_data['id_fname_lname_email_ssn_address_cid'].map(lambda x:x.split('_')[1])
# st_data['lname'] = st_data['id_fname_lname_email_ssn_address_cid'].map(lambda x:x.split('_')[1])
#
#
new_st_data['name'] = new_st_data[1] + '_' + new_st_data[2]

tc_data['name'] = tc_data['fname'] + '_' + tc_data['lname']

json_data = {}
for index, row in new_st_data.iterrows():
    for tc_index, tc_row in tc_data.iterrows():
        if row[6] == tc_row['cid']:
            json_data[row['name']] = {
                "class": row[6],
                "teacher_name": tc_row['name'],
                "teacher_id": tc_row['id']
            }

import json
with open('json_data.json', 'w') as fp:
    json.dump(json_data, fp)




