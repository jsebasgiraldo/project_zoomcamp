/* @bruin

name: staging.poverty_transform
type: bq.sql
connection: bigquery-default

depends: 
    - ingestion.poverty_data
    - ingestion.state_df
materialization: 
  type: table
  partition_by: "FILEDATE"
  cluster_by: ["State_name", "poverty_status"]

custom_checks:
  - name: table_not_empty
    query: SELECT COUNT(*) > 0 FROM staging.poverty_transform
    value: 1

 @bruin */

-- Casting all the columns to it's proper format
-- Join with states table
-- Transforming numeric codes following the database documentation

select cast(poverty.FILEDATE as DATE) as FILEDATE,
cast(poverty.serialno as STRING ) as serialno,
cast(poverty.sporder as INTEGER) as sporder,
state.Code as code_state,
state.State_name,
case when poverty.off_poor=1 then "Not in poverty"
     when poverty.off_poor=0 then "In poverty"
     else "Unknown" end as poverty_status,
cast(poverty.Age as INTEGER) as Age,
case when poverty.Mar=1 then "Married"
     when poverty.Mar=2 then "Widowed"
     when poverty.Mar=3 then "Divorced"
     when poverty.Mar=4 then "Separated"
     when poverty.Mar=5 then "Never married"
     else "Unknown" end as Marital_status,
case when poverty.Sex=1 then "Male"
     when poverty.Sex=2 then "Female"
     else "Unknown" end as Sex,
case when poverty.Education=0 then "Under age 25"
     when poverty.Education=1 then "Less than a high school degree"
     when poverty.Education=2 then "High school degree"
     when poverty.Education=3 then "Some college education"
     when poverty.Education=4 then "College degree"
     else "Unknown" end as Education,
case when poverty.Race=1 then "White alone"
     when poverty.Race=2 then "Black alone"
     when poverty.Race=3 then "Asian alone"
     when poverty.Race=4 then "Other"
     else "Unknown" end as Race,
case when poverty.Hispanic=1 then "Hispanic"
     when poverty.Hispanic=0 then "Not Hispanic"
     else "Unknown" end as Hispanic,
poverty.AGI as gross_income,
poverty.Tax_unit,
cast(poverty.spm_num_per as INTEGER) as num_person,
cast(poverty.spm_num_kids as INTEGER) as num_kids,
cast(poverty.spm_num_adults as INTEGER) as num_adults,
case when poverty.spm_ten_mort_status=1 then "Owner with mortgage"
     when poverty.spm_ten_mort_status=0 then "Owner without mortgage or rent-free"
     when poverty.spm_ten_mort_status=3 then "Renter"
     else "Unknown" end as Mortgage_Status,
poverty.SPM_Resources,
poverty.SPM_Totval as cash_income,
poverty.spm_wk_xpns as work_expenses,
poverty.spm_childcare_xpns as childcare_expenses,
case when poverty.spm_w_cohabit=1 then "No cohabiting couple"
     when poverty.spm_w_cohabit=0 then "Has cohabiting couple"
     else "Unknown" end as Cohabit,
case when poverty.spm_w_ui_lt15=1 then "Has UI under 15"
     when poverty.spm_w_ui_lt15=0 then "No UI under 15"
     else "Unknown" end as Under_UI_15,
cast(poverty.extracted_at as DATETIME) as extracted_at

from ingestion.poverty_data as poverty
LEFT JOIN ingestion.state_df as state
ON poverty.st = state.State_ID
where state.State_ID <> 99
