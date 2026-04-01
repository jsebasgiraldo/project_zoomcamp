/* @bruin

name: reports.population_characteristics
type: bq.sql
connection: bigquery-default

depends: 
    - staging.poverty_transform
materialization: 
    type: table
    cluster_by: ["year", "State_name"]

custom_checks:
  - name: table_not_empty
    query: SELECT COUNT(*) > 0 FROM reports.population_characteristics
    value: 1

 @bruin */

--Grouping the population and the poverty indicator by each demographic characteristic
 Select State_name,
 poverty_status,
 extract( year from FILEDATE) as year,
 Age,
 Marital_status,
 Sex,
 Education,
 Race,
 Hispanic,
 Mortgage_status,
 count(1) as Population_num
 from staging.poverty_transform
 group by State_name,
 poverty_status,
 extract( year from FILEDATE),
 Age,
 Marital_status,
 Sex,
 Education,
 Race,
 Hispanic,
 Mortgage_status