"""@bruin 
name: ingestion.state_df
type: python
engine: python

connection: bigquery-default


materialization:
    type: table
    strategy: create+replace
    dataset: state_dataset
    table: state_data
image: python:3.12

columns:
  - name: 'State_ID'
    type: string

  - name: 'Code'
    type: string

  - name: 'State_name'
    type: string

@bruin """


import pandas as pd

def materialize():
    ids=[1,2,4,5,6,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,
     32,33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,53,54,55,56,99]
    
    code=["AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA",
      "ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR",
      "PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","ERROR"]

    states=["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","District of Columbia","Florida",
        "Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts",
        "Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York",
        "North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee",
        "Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming","Error"]

    dicc_states={"State_ID":ids,"Code":code,"State_name":states}

    return pd.DataFrame(dicc_states)