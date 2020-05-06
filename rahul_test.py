import pandas as pd
import json


tc_data = pd.read_parquet('teachers.parquet', engine='pyarrow')
st_data = pd.read_csv('students.csv')

new_st_data = st_data[st_data.columns[0]].str.split("_", n=6, expand=True)
new_st_data['name'] = new_st_data[1] + '_' + new_st_data[2]
new_st_data['cid'] = new_st_data[6]

tc_data['name'] = tc_data['fname'] + '_' + tc_data['lname']

json_data = {}
for index, row in new_st_data.iterrows():
    for tc_index, tc_row in tc_data.iterrows():
        if row['cid'] == tc_row['cid']:
            if row['name'] not in json_data:
                json_data[row['name']] = [{
                    "class": row['cid'],
                    "teacher_name": tc_row['name'],
                    "teacher_id": tc_row['id']
                }]
            else:
                json_data[row['name']].append({
                    "class": row['cid'],
                    "teacher_name": tc_row['name'],
                    "teacher_id": tc_row['id']
                })

with open('report.json', 'w') as fp:
    report = {"Report_Data": json_data}
    json.dump(report, fp)
