/* @bruin

name: reports.families_in_poverty
type: bq.sql
connection: bigquery-default

depends: 
    - staging.poverty_transform
materialization: 
    type: table
    cluster_by: ["year", "State_name"]

custom_checks:
  - name: table_not_empty
    query: SELECT COUNT(*) > 0 FROM reports.families_in_poverty
    value: 1

 @bruin */

 Select State_name,
 poverty_status,
 Cohabit,
 extract( year from FILEDATE) as year,
 count(distinct serialno) as num_families,
 max(gross_income) as gross_income,
 max(num_person) as num_person,
 max(num_kids) as num_kids,
 max(num_adults) as num_adults,
 max(cash_income) as cash_income,
 max(work_expenses) as work_expenses,
 max(childcare_expenses)  as childcare_expenses
 from staging.poverty_transform
 group by State_name,
 poverty_status,
 Cohabit,
 extract( year from FILEDATE)
