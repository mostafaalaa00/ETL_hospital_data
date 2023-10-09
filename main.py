import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text

# read both data and create dataframe to each one
data_discovery = pd.read_csv('Clinical Data_Discovery_Cohort.csv')
data_valid = pd.read_excel('Clinical_Data_Validation_Cohort.xlsx')
df_dis = pd.DataFrame(data_discovery)
df_val = pd.DataFrame(data_valid)


# clean and enhance Data_Discovery_Cohort to marge with Data_Validation_Cohort
# print(df_dis.head)
# add ditinct id with the same format with validation id
id_range = []
for i in range(df_dis.shape[0]):
    id_range.append(int(140000+i))
df_dis['PatientID'] = id_range

#count servival time (Days)
df_dis['Date of Last Follow Up'] = pd.to_datetime(df_dis['Date of Last Follow Up'], format='mixed')
df_dis['Specimen date'] = pd.to_datetime(df_dis['Specimen date'], format='mixed')
df_dis['servival days'] = (df_dis['Date of Last Follow Up'] - df_dis['Specimen date']).dt.days

# match death or alive with validation csv file
event = []
for i in df_dis['Dead or Alive']:
    if i == 'Dead':
        event.append(1)
    if i == 'Alive':
        event.append(0)
df_dis['Dead or Alive'] = event

# match gender type with validation csv file
sex = []
for i in df_dis['sex']:
    if i == 'F':
        sex.append('Female')
    if i == 'M':
        sex.append('Male')
df_dis['sex'] = sex

# after made prepocessing in csv column arrange column to be the same order
# unmatched column
df_dis['Date of Death'] = 0
df_dis['Date of Last Follow Up'] = 0
df_dis['Time'] = 0
df_dis['Event'] = 0
df_dis['tmep1'] = 0
df_dis['tmep2'] = 0
df_dis['tmep3'] = 0
df_dis['tmep4'] = 0
current_columns = df_dis.columns.tolist()
new_columns = current_columns[:1] + current_columns[10:11] + current_columns[2:3] + current_columns[3:4] + current_columns[4:5]+current_columns[7:8]+current_columns[8:9]+current_columns[5:6]+current_columns[6:7]+current_columns[9:10]+current_columns[11:16]
#print(new_columns)
new_dis = df_dis[new_columns]


# remove (p) letter form the patientID
ids = []
for i in df_val['Patient ID']:
    ids.append(int(i[1:]))
df_val['Patient ID'] = ids

# rename Data_Discovery_Cohort column to  Data_Validation_Cohort column
tempx = 0
for i in df_val.columns:
    new_dis = new_dis.rename(columns={new_columns[tempx]: i})
    tempx += 1
column_index = df_val.columns

# Concatenate the dataframe vertically
df = pd.concat([df_val, new_dis[column_index]], axis=0)
#print(df.head())

# Data Cleaning
#df.dropna(inplace=True)
df.drop_duplicates(inplace=True)
df.fillna(0, inplace=True)
df.drop(columns=['Type.Adjuvant', 'Pack per year', 'batch'], axis=1, inplace=True)
#print(df.shape)
#print(df.head)
#print(df.isnull().sum())
#print(df.duplicated().sum())


# Data Visualization
#let's take a look about all dataframe histogram first
#print(df.hist(figsize=(12,10),bins=8))

#number of dead or alive
'''
color = ['blue','red']
state = df['Event (death: 1, alive: 0)'].value_counts().tolist()
plt.pie(state,labels=['Died','Alive'],colors=color,autopct='%1.1f%%')
plt.title = 'pataint dead or alive'
plt.legend(state)
plt.show()
#from the chart we see that their is no big differance in result but dead is more than alive

'''

# which gender survive more..
'''
df.groupby('Sex')['Survival time (days)'].mean().plot(kind='bar')
plt.show()
# female is survive more than male
'''

# the relationship between number of survival days and death
# (if he/she survive more than 1000 days for example he/she have more probability to live)
'''
df.plot(x='Event (death: 1, alive: 0)', y='Survival time (days)', kind='scatter')
plt.legend(['dead', 'alive'])
plt.show()
# survival days can not determine if the pationt have more probability to live (No Relationship)
'''
#df.to_excel('allData.xlsx', index=False)

#now after data cleaning and visualization convert this data into tables and add it to database

engine = create_engine('mysql+mysqlconnector://root:12345@localhost:3306/hospital_cancar')

# create first table
patient_columns = ['Patient ID', 'Sex', 'Age', 'Event (death: 1, alive: 0)']
patient_table = df[patient_columns]

# pid as a forign key
df['PID'] = df['Patient ID']
# create second table
hospital_record_columns = ['PID','Survival time (days)','Tumor size (cm)', 'Grade', 'Stage (TNM 8th edition)', 'Cigarette', 'EGFR', 'KRAS']
hospital_record_table = df[hospital_record_columns]

# Write first table to the database with specified data types
df[patient_columns].to_sql('patient', engine, index=False, if_exists='replace')

# Write second table to the database with specified data types
df[hospital_record_columns].to_sql('hospital_record', engine, index=False, if_exists='replace')

# try to make a join between two inserted two tables and get survival time with pid
query = text("select `Patient ID`, `Survival time (days)` from hospital_record join patient on hospital_record.PID = patient.`Patient ID` limit 20")
with engine.connect() as con:
    results = con.execute(query)
    for rows in results:
        print("PID: " + str(rows[0]) + "  survival Days: " + str(rows[1]))
