"""@bruin 
name: ingestion.poverty_data
type: python
engine: python

connection: bigquery-default


materialization:
    type: table
    strategy: append
    dataset: poverty_dataset
    table: poverty_data
image: python:3.12

columns:
  - name: 'FILEDATE'
    type: datetime

  - name: 'serialno'
    type: string

  - name: 'sporder'
    type: integer

  - name: 'st'
    type: integer

  - name: 'OFFPoor'
    type: integer

  - name: 'Age'
    type: integer

  - name: 'Mar'
    type: integer

  - name: 'Sex'
    type: integer

  - name: 'Education'
    type: integer

  - name: 'Race'
    type: integer

  - name: 'Hispanic'
    type: integer

  - name: 'AGI'
    type: float

  - name: 'Tax_unit'
    type: float

  - name: 'SPM_NumPer'
    type: integer

  - name: 'SPM_NumKids'
    type: integer

  - name: 'SPM_NumAdults'
    type: integer

  - name: 'SPM_TenMortStatus'
    type: integer

  - name: 'SPM_Resources'
    type: float

  - name: 'SPM_Totval'
    type: float

  - name: 'SPM_WkXpns'
    type: float

  - name: 'SPM_ChildcareXpns'
    type: float

  - name: 'SPM_wCohabit'
    type: integer

  - name: 'SPM_wUI_LT15'
    type: integer

  - name: 'extracted_at'
    type: datetime


@bruin """

#Import libraries
import os
import tzdata
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
os.environ["TZDIR"] = "UTC"
import json
from datetime import datetime
from typing import List, Tuple
import io
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

#Link to the data, we will append the year and extension to it
url='https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spm_'

#Iteration throght the dates provides
def _iter_year_starts(start_date, end_date):
    cur = start_date.replace(day=1)
    last = end_date.replace(day=1)
    while cur <= last:
        yield cur
        cur = (cur + relativedelta(years=1)).replace(day=1)


def materialize():
    start_date_s = os.environ.get("BRUIN_START_DATE")
    end_date_s = os.environ.get("BRUIN_END_DATE")

    if not start_date_s or not end_date_s:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be provided")

    start_date = datetime.strptime(start_date_s, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_s, "%Y-%m-%d").date()

    #Only there's data from 2009 to 2023, except 2020. So we restricted the dates 
    MIN_YEAR = 2009
    MAX_YEAR = 2023
    
    #When the dates provide is less the minimun year, it takes the minimun year
    if start_date.year < MIN_YEAR:
        start_date = datetime(MIN_YEAR, 1, 1).date()
        
    #When the dates provide is over the maximun year, it takes the maximun year
    if end_date.year > MAX_YEAR:
        end_date = datetime(MAX_YEAR, 12, 31).date()
    
    if start_date > end_date:
        print(f"[ingestion.data] Out of bounds range (2009-2023). Nothing to download.")
        return pd.DataFrame()

    #Creating the final_url and the dataframes union
    dfs = []
    ext='dta'
    for year_start in _iter_year_starts(start_date, end_date):
        year = year_start.year
        url_ext=f"{url}{year}_pu.{ext}"
        
        try:
            resp = requests.get(url_ext, timeout=520)
            if resp.status_code != 200:
                print(f"[ingestion.poverty_data] WARNING: {url_ext} returned {resp.status_code}")
                continue

            df = pd.read_stata(io.BytesIO(resp.content))
            
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
            
            #Loading like string the data variables, because I had a problem with tzdata library 
            df["extracted_at"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            dfs.append(df)
            print(f"[ingestion.poverty_data] fetched, rows={len(df)}")
        except Exception as e:
            print(f"[ingestion.poverty_data] ERROR fetching {url_ext}: {e}")
            continue

    if not dfs:
        return pd.DataFrame()
    
    #Taking only the interest variables
    result = pd.concat(dfs, ignore_index=True)
    result['FILEDATE'] = pd.to_datetime(result['FILEDATE'], errors='coerce').dt.strftime('%Y-%m-%d')
    result=result[['FILEDATE', 'serialno', 'sporder', 'st', 'OFFPoor','Age','Mar', 'Sex',
                 'Education', 'Race', 'Hispanic', 'AGI', 'Tax_unit','SPM_NumPer', 'SPM_NumKids',
                 'SPM_NumAdults', 'SPM_TenMortStatus', 'SPM_Resources', 'SPM_Totval',
                 'SPM_WkXpns', 'SPM_ChildcareXpns','SPM_wCohabit','SPM_wUI_LT15',"extracted_at"]]


    return result