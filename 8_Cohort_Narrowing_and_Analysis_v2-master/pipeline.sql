

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3a6446b1-1dc7-48b3-b059-f0fc02177725"),
    ch_analytic_winf=Input(rid="ri.foundry.main.dataset.ff5bcbcb-e1a3-464f-b526-d8dfd2058f11")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Summarizing eligibility criteria in a cascading way (i.e. so each new criteria includes all previous criteria plus new criteria) to summarize for CONSORT diagram

Inputs:         'ch_analytic_winf'
Outputs:        'CONSORT_Step1'

Revisions:      
==============================================================================*/

SELECT count(macrovisit_id) as covid_hosps,year,
/*count(CASE WHEN year_of_hosp2020_2021=true THEN 1 END) as crit1,*/ 
count(CASE WHEN age_at_pos is not null THEN 1 END) as crit2,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 THEN 1 END) as crit3,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' THEN 1 END) as crit4,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and zip_code is not null THEN 1 END) as crit5a,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null THEN 1 END) as crit5b,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_21d_valid>17) THEN 1 END) as crit6a,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_5d_valid=5) THEN 1 END) as crit6b,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_2d_valid=2) THEN 1 END) as crit6c,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_21d_valid>17) AND MeanTemp_F is not null THEN 1 END) as crit7,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_21d_valid>17) AND MeanTemp_F is not null AND new_cases_7d is not null THEN 1 END) as crit8

FROM ch_analytic_winf
where case=1 or case is NULL
group by year

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.26910d9c-5fda-4830-88e1-f85d19a084e9"),
    ch_elig_simple=Input(rid="ri.foundry.main.dataset.ea9f5726-53a2-4377-9081-081211ea431c")
)
SELECT count(*) as covid_hosps,
count(CASE WHEN year(macrovisit_start_date)=2020 THEN 1 END) as crit1,
/*count(CASE WHEN year_of_hosp2020_2021=true THEN 1 END) as crit1,*/ 
count(CASE WHEN age_at_pos is not null THEN 1 END) as crit2,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 THEN 1 END) as crit3,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' THEN 1 END ) as crit4,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null THEN 1 END)  as crit5,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and pm25_21d_valid>17 THEN 1 END) as crit6a,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and pm25_5d_valid=5 THEN 1 END) as crit6b,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and pm25_2d_valid=2 THEN 1 END) as crit6c,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and pm25_21d_valid>17 AND MeanTemp_F is not null THEN 1 END) as crit7,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_21d_valid>17) AND MeanTemp_F is not null AND new_cases_7d is not null THEN 1 END) as crit8,
count(CASE WHEN age_at_pos is not null and age_at_pos>=18 and shift_date_yn!='Y' and ZCTA is not null and (pm25_21d_valid=5) AND MeanTemp_F is not null AND new_cases_7d is not null THEN 1 END) as crit9

FROM ch_elig_simple

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6fa82a85-d2db-40cb-85cd-858c07cf8712"),
    CONSORT_Step1=Input(rid="ri.foundry.main.dataset.3a6446b1-1dc7-48b3-b059-f0fc02177725")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Cleaning up and labelling CONSORT numbers

Inputs:         'CONSORT_Step1'
Outputs:        'CONSORT_Step2'

Revisions:      
==============================================================================*/

SELECT 1 as order, year, covid_hosps as Criteria, 'Covid Hospitalizations' as NAME FROM CONSORT_Step1
UNION
/*SELECT crit1 as Criteria, 'Year 2020 or 2021' as NAME FROM CONSORT_Step1
UNION*/
SELECT 2 as order, year,crit2 as Criteria, 'Valid age' as NAME FROM CONSORT_Step1
UNION
SELECT 3 as order, year,crit3 as Criteria, 'Age>=18' as NAME FROM CONSORT_Step1
UNION
SELECT 4 as order, year,crit4 as Criteria, 'No shift date' as NAME FROM CONSORT_Step1
UNION
SELECT 5 as order, year,crit5a as Criteria, 'Has zip code' as NAME FROM CONSORT_Step1
UNION
SELECT 5.5 as order, year,crit5b as Criteria, 'Has ZCTA' as NAME FROM CONSORT_Step1
UNION
SELECT 6 as order, year,crit6a as Criteria, 'Has PM2.5 data (21-day)' as NAME FROM CONSORT_Step1
UNION
SELECT 7 as order, year,crit6b as Criteria, 'Has PM2.5 data (5-day)' as NAME FROM CONSORT_Step1
UNION
SELECT 8 as order, year,crit6c as Criteria, 'Has PM2.5 data (2-day)' as NAME FROM CONSORT_Step1
UNION
SELECT 9 as order, year,crit7 as Criteria, 'Has climate data' as NAME FROM CONSORT_Step1
UNION
SELECT 10 as order, year,crit8 as Criteria, 'County level covid counts' as NAME FROM CONSORT_Step1

ORDER by Criteria, year DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cc1d7a4e-ab05-4bb7-959a-346f9694ed36"),
    covid_pos_dedup=Input(rid="ri.foundry.main.dataset.b2d2aec9-32ed-4f41-9f91-549a004b046c"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
SELECT distinct a.*,b.location_id,c.zip,b.data_partner_id
FROM covid_pos_dedup a LEFT JOIN person b on a.person_id=b.person_id
LEFT JOIN location c on b.location_id=c.location_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6bd98be1-31ac-41c7-905a-a6a35c7063cf"),
    ch_analytic=Input(rid="ri.foundry.main.dataset.00cce7c1-05eb-46ca-96ab-91cc090ab1e0")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        12/21/2022
Last modified:  12/21/2022
Purpose:        Generate count of final analytic observations per CBSA to use later to filter CBSA subgroups to only those with substantial numbers (n>=100 cases)

Inputs:         'ch_analytic'
Outputs:        'cbsa_analytic_counts'
==============================================================================*/
SELECT CBSA_CODE, count(*) as cbsa_obs
FROM ch_analytic

WHERE year=2020 AND pm25 IS NOT NULL AND Precip IS NOT NULL and case=1 and CBSA_CODE is not null
group by CBSA_CODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.40562261-a9b5-4df1-b4e4-ce57f9b7bc69"),
    cbsa_covid_counts=Input(rid="ri.foundry.main.dataset.645667b9-f3ed-4c7d-af29-7ab37d982a5c")
)
SELECT CBSA_CODE,CBSA_Name, case when covid_count<20 then -20 else covid_count end as covid_count_obscure
FROM cbsa_covid_counts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.645667b9-f3ed-4c7d-af29-7ab37d982a5c"),
    covid_pos_w_location=Input(rid="ri.foundry.main.dataset.eedd643d-b678-4776-94af-57d6aa72d201")
)
SELECT CBSA_CODE, CBSA_Name, count(*) as covid_count 
FROM covid_pos_w_location
group by CBSA_CODE,CBSA_Name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.56352f2c-4cb6-4964-b894-f48e2f87c0f8"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
/* ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        3/15/2023
# Last modified:  4/27/2023
# Purpose:        Create CBSA-level Table 1 type summary statistics (this program creates the base dataset, subsequent programs obscure cases with small cell sizes)

# Inputs:         'ch_analytic_wmeans'
# Outputs:        cbsa_summary_stats_final
#
# Revisions:	  4/27/2023 - Adding 2- and 5-day PM2.5
#                 6/07/2023 - Adding ZCTA level education variables
#==============================================================================*/

SELECT
	CBSA_CODE,first_case_date_cbsa,
	sum(case when race_categ = 'AMERICAN INDIAN OR ALASKA NATIVE' then 1 else 0 END) as race_categ_aian,
	sum(case when race_categ = 'ASIAN OR PACIFIC ISLANDER' then 1 else 0 END) as race_categ_api,
	sum(case when race_categ = 'BLACK' then 1 else 0 END ) as race_categ_black,
	sum(case when race_categ = 'OTHER OR MULTIPLE RACES' then 1 else 0 END) as race_categ_othermult,
	sum(case when race_categ = 'WHITE' then 1 else 0 END) as race_categ_white,
	sum(case when race_categ is NULL or race_categ = 'UNKNOWN' then 1 else 0 END) as race_categ_na,
	sum(case when ethnicity_concept_name = 'Hispanic or Latino' then 1 else 0 END) as ethnicity_hisp,
	sum(case when ethnicity_concept_name = 'Not Hispanic or Latino' then 1 else 0 END) as ethnicity_nonhisp,
	sum(case when ethnicity_concept_name = 'Other/Unknown' or ethnicity_concept_name is NULL then 1 else 0 END) as ethnicity_otherna,
	sum(case when gender_concept_name = 'MALE' then 1 else 0 END) as gender_male,
	sum(case when gender_concept_name = 'FEMALE' then 1 else 0 END) as gender_female,
	sum(case when gender_concept_name in ('No matching concept','UNKNOWN','Gender unknown') or gender_concept_name is NULL  then 1 else 0 END) as gender_na,
	sum(case when CVD=0 then 1 else 0 END) as CVD0,
	sum(CVD)  as CVD1,
	sum(case when MD=0 then 1 else 0 END) as MD0,
	sum(MD)  as MD1,
	sum(case when RD=0 then 1 else 0 END)  as RD0,
	sum(RD) as RD1,
	sum(case when age_gte65=0 then 1 else 0 END) as age_gte650,
	sum(age_gte65) as age_gte651,
	mean(poverty_status) as poverty_status_mean,
	stddev(poverty_status) as poverty_status_stddev,
	MIN(poverty_status) poverty_status_Minimum,
	MAX(CASE WHEN poverty_status_Quartile = 1 THEN poverty_status END) poverty_status_Q1,
	MAX(CASE WHEN poverty_status_Quartile = 2 THEN poverty_status END) poverty_status_Median,
	MAX(CASE WHEN poverty_status_Quartile = 3 THEN poverty_status END) poverty_status_Q3,
	MAX(poverty_status) poverty_status_Maximum,
    mean(age_at_pos) as age_at_pos_mean,
    stddev(age_at_pos) as age_at_pos_stddev,
    MIN(age_at_pos) age_at_pos_Minimum,
	MAX(CASE WHEN age_at_pos_Quartile = 1 THEN age_at_pos END) age_at_pos_Q1,
	MAX(CASE WHEN age_at_pos_Quartile = 2 THEN age_at_pos END) age_at_pos_Median,
	MAX(CASE WHEN age_at_pos_Quartile = 3 THEN age_at_pos END) age_at_pos_Q3,
	MAX(age_at_pos) age_at_pos_Maximum,
    mean(MDs) as MDs_mean,
    stddev(MDs) as MDs_stddev,
    MIN(MDs) MDs_Minimum,
	MAX(CASE WHEN MDs_Quartile = 1 THEN MDs END) MDs_Q1,
	MAX(CASE WHEN MDs_Quartile = 2 THEN MDs END) MDs_Median,
	MAX(CASE WHEN MDs_Quartile = 3 THEN MDs END) MDs_Q3,
	MAX(MDs) MDs_Maximum,
    mean(pm25) as pm25_mean,
    stddev(pm25) as pm25_stddev,
    MIN(pm25) pm25_Minimum,
	MAX(CASE WHEN pm25_Quartile = 1 THEN pm25 END) pm25_Q1,
	MAX(CASE WHEN pm25_Quartile = 2 THEN pm25 END) pm25_Median,
	MAX(CASE WHEN pm25_Quartile = 3 THEN pm25 END) pm25_Q3,
	MAX(pm25) pm25_Maximum,
    mean(pm25_2d) as pm25_2d_mean,
    stddev(pm25_2d) as pm25_2d_stddev,
    MIN(pm25_2d) pm25_2d_Minimum,
	MAX(CASE WHEN pm25_2d_Quartile = 1 THEN pm25_2d END) pm25_2d_Q1,
	MAX(CASE WHEN pm25_2d_Quartile = 2 THEN pm25_2d END) pm25_2d_Median,
	MAX(CASE WHEN pm25_2d_Quartile = 3 THEN pm25_2d END) pm25_2d_Q3,
	MAX(pm25_2d) pm25_2d_Maximum, 
    mean(pm25_5d) as pm25_5d_mean,
    stddev(pm25_5d) as pm25_5d_stddev,
    MIN(pm25_5d) pm25_5d_Minimum,
	MAX(CASE WHEN pm25_5d_Quartile = 1 THEN pm25_5d END) pm25_5d_Q1,
	MAX(CASE WHEN pm25_5d_Quartile = 2 THEN pm25_5d END) pm25_5d_Median,
	MAX(CASE WHEN pm25_5d_Quartile = 3 THEN pm25_5d END) pm25_5d_Q3,
	MAX(pm25_5d) pm25_5d_Maximum, 
    mean(pm25_7d) as pm25_7d_mean,
    stddev(pm25_7d) as pm25_7d_stddev,
    MIN(pm25_7d) pm25_7d_Minimum,
	MAX(CASE WHEN pm25_7d_Quartile = 1 THEN pm25_7d END) pm25_7d_Q1,
	MAX(CASE WHEN pm25_7d_Quartile = 2 THEN pm25_7d END) pm25_7d_Median,
	MAX(CASE WHEN pm25_7d_Quartile = 3 THEN pm25_7d END) pm25_7d_Q3,
	MAX(pm25_7d) pm25_7d_Maximum, 
    mean(pm25_21d) as pm25_21d_mean,
    stddev(pm25_21d) as pm25_21d_stddev,
    MIN(pm25_21d) pm25_21d_Minimum,
	MAX(CASE WHEN pm25_21d_Quartile = 1 THEN pm25_21d END) pm25_21d_Q1,
	MAX(CASE WHEN pm25_21d_Quartile = 2 THEN pm25_21d END) pm25_21d_Median,
	MAX(CASE WHEN pm25_21d_Quartile = 3 THEN pm25_21d END) pm25_21d_Q3,
	MAX(pm25_21d) pm25_21d_Maximum,   
    mean(new_cases_7d) as new_cases_7d_mean,
    stddev(new_cases_7d) as new_cases_7d_stddev,
    MIN(new_cases_7d) new_cases_7d_Minimum,
	MAX(CASE WHEN new_cases_7d_Quartile = 1 THEN new_cases_7d END) new_cases_7d_Q1,
	MAX(CASE WHEN new_cases_7d_Quartile = 2 THEN new_cases_7d END) new_cases_7d_Median,
	MAX(CASE WHEN new_cases_7d_Quartile = 3 THEN new_cases_7d END) new_cases_7d_Q3,
	MAX(new_cases_7d) new_cases_7d_Maximum,
    mean(days_from_1st_case_cbsa) as days_from_1st_case_cbsa_mean,
    stddev(days_from_1st_case_cbsa) as days_from_1st_case_cbsa_stddev,
    MIN(days_from_1st_case_cbsa) days_from_1st_case_cbsa_Minimum,
	MAX(CASE WHEN days_from_1st_case_cbsa_Quartile = 1 THEN days_from_1st_case_cbsa END) days_from_1st_case_cbsa_Q1,
	MAX(CASE WHEN days_from_1st_case_cbsa_Quartile = 2 THEN days_from_1st_case_cbsa END) days_from_1st_case_cbsa_Median,
	MAX(CASE WHEN days_from_1st_case_cbsa_Quartile = 3 THEN days_from_1st_case_cbsa END) days_from_1st_case_cbsa_Q4,
	MAX(days_from_1st_case_cbsa) days_from_1st_case_cbsa_Maximum,
  mean(education_lt_hs_diploma) as education_lt_hs_diploma_mean,
    stddev(education_lt_hs_diploma) as education_lt_hs_diploma_stddev,
    MIN(education_lt_hs_diploma) education_lt_hs_diploma_Minimum,
	MAX(CASE WHEN education_lt_hs_diploma_Quartile = 1 THEN education_lt_hs_diploma END) education_lt_hs_diploma_Q1,
	MAX(CASE WHEN education_lt_hs_diploma_Quartile = 2 THEN education_lt_hs_diploma END) education_lt_hs_diploma_Median,
	MAX(CASE WHEN education_lt_hs_diploma_Quartile = 3 THEN education_lt_hs_diploma END) education_lt_hs_diploma_Q4,
	MAX(education_lt_hs_diploma) education_lt_hs_diploma_Maximum,
 mean(education_hs_diploma) as education_hs_diploma_mean,
    stddev(education_hs_diploma) as education_hs_diploma_stddev,
    MIN(education_hs_diploma) education_hs_diploma_Minimum,
	MAX(CASE WHEN education_hs_diploma_Quartile = 1 THEN education_hs_diploma END) education_hs_diploma_Q1,
	MAX(CASE WHEN education_hs_diploma_Quartile = 2 THEN education_hs_diploma END) education_hs_diploma_Median,
	MAX(CASE WHEN education_hs_diploma_Quartile = 3 THEN education_hs_diploma END) education_hs_diploma_Q4,
	MAX(education_hs_diploma) education_hs_diploma_Maximum,
mean(education_some_college) as education_some_college_mean,
    stddev(education_some_college) as education_some_college_stddev,
    MIN(education_some_college) education_some_college_Minimum,
	MAX(CASE WHEN education_some_college_Quartile = 1 THEN education_some_college END) education_some_college_Q1,
	MAX(CASE WHEN education_some_college_Quartile = 2 THEN education_some_college END) education_some_college_Median,
	MAX(CASE WHEN education_some_college_Quartile = 3 THEN education_some_college END) education_some_college_Q4,
	MAX(education_some_college) education_some_college_Maximum,
mean(education_associate_degree) as education_associate_degree_mean,
    stddev(education_associate_degree) as education_associate_degree_stddev,
    MIN(education_associate_degree) education_associate_degree_Minimum,
	MAX(CASE WHEN education_associate_degree_Quartile = 1 THEN education_associate_degree END) education_associate_degree_Q1,
	MAX(CASE WHEN education_associate_degree_Quartile = 2 THEN education_associate_degree END) education_associate_degree_Median,
	MAX(CASE WHEN education_associate_degree_Quartile = 3 THEN education_associate_degree END) education_associate_degree_Q4,
	MAX(education_associate_degree) education_associate_degree_Maximum,
mean(education_bachelors_degree) as education_bachelors_degree_mean,
    stddev(education_bachelors_degree) as education_bachelors_degree_stddev,
    MIN(education_bachelors_degree) education_bachelors_degree_Minimum,
	MAX(CASE WHEN education_bachelors_degree_Quartile = 1 THEN education_bachelors_degree END) education_bachelors_degree_Q1,
	MAX(CASE WHEN education_bachelors_degree_Quartile = 2 THEN education_bachelors_degree END) education_bachelors_degree_Median,
	MAX(CASE WHEN education_bachelors_degree_Quartile = 3 THEN education_bachelors_degree END) education_bachelors_degree_Q4,
	MAX(education_bachelors_degree) education_bachelors_degree_Maximum,
mean(education_graduate_or_professional_degree) as education_graduate_or_professional_degree_mean,
    stddev(education_graduate_or_professional_degree) as education_graduate_or_professional_degree_stddev,
    MIN(education_graduate_or_professional_degree) education_graduate_or_professional_degree_Minimum,
	MAX(CASE WHEN education_graduate_or_professional_degree_Quartile = 1 THEN education_graduate_or_professional_degree END) education_graduate_or_professional_degree_Q1,
	MAX(CASE WHEN education_graduate_or_professional_degree_Quartile = 2 THEN education_graduate_or_professional_degree END) education_graduate_or_professional_degree_Median,
	MAX(CASE WHEN education_graduate_or_professional_degree_Quartile = 3 THEN education_graduate_or_professional_degree END) education_graduate_or_professional_degree_Q4,
	MAX(education_graduate_or_professional_degree) education_graduate_or_professional_degree_Maximum,
mean(education_HSplus) as education_HSplus_mean,
    stddev(education_HSplus) as education_HSplus_stddev,
    MIN(education_HSplus) education_HSplus_Minimum,
	MAX(CASE WHEN education_HSplus_Quartile = 1 THEN education_HSplus END) education_HSplus_Q1,
	MAX(CASE WHEN education_HSplus_Quartile = 2 THEN education_HSplus END) education_HSplus_Median,
	MAX(CASE WHEN education_HSplus_Quartile = 3 THEN education_HSplus END) education_HSplus_Q4,
	MAX(education_HSplus) education_HSplus_Maximum,
mean(education_BAplus) as education_BAplus_mean,
    stddev(education_BAplus) as education_BAplus_stddev,
    MIN(education_BAplus) education_BAplus_Minimum,
	MAX(CASE WHEN education_BAplus_Quartile = 1 THEN education_BAplus END) education_BAplus_Q1,
	MAX(CASE WHEN education_BAplus_Quartile = 2 THEN education_BAplus END) education_BAplus_Median,
	MAX(CASE WHEN education_BAplus_Quartile = 3 THEN education_BAplus END) education_BAplus_Q4,
	MAX(education_BAplus) education_BAplus_Maximum,  
	COUNT(poverty_status_Quartile) AS poverty_status_Count,
    COUNT(age_at_pos_Quartile) AS age_at_pos_Count,
    COUNT(MDs_Quartile) AS MDs_Count,
    COUNT(pm25_Quartile) AS pm25_Count,
    COUNT(pm25_2d_Quartile) AS pm25_2d_Count,
    COUNT(pm25_5d_Quartile) AS pm25_5d_Count,
    COUNT(pm25_7d_Quartile) AS pm25_7d_Count,
    COUNT(pm25_21d_Quartile) AS pm25_21d_Count,
    COUNT(new_cases_7d_Quartile) AS new_cases_7d_Count,
    COUNT(days_from_1st_case_cbsa_Quartile) AS days_from_1st_case_cbsa_Count,
    COUNT(education_lt_hs_diploma_Quartile) AS education_lt_hs_diploma_Count,
	COUNT(education_hs_diploma_Quartile) AS education_hs_diploma_Count,
	COUNT(education_some_college_Quartile) AS education_some_college_Count,
	COUNT(education_associate_degree_Quartile) AS education_associate_degree_Count,
	COUNT(education_bachelors_degree_Quartile) AS education_bachelors_degree_Count,
	COUNT(education_graduate_or_professional_degree_Quartile) AS education_graduate_or_professional_degree_Count,
	COUNT(education_HSplus_Quartile) AS education_HSplus_Count,
	COUNT(education_BAplus_Quartile) AS education_BAplus_Count
FROM (
	SELECT
		ch_analytic_wmeans.CBSA_CODE,
		poverty_status,age_at_pos,MDs,race_categ,pm25,pm25_2d,pm25_5d,pm25_7d,pm25_21d,new_cases_7d,days_from_1st_case_cbsa,education_lt_hs_diploma,education_hs_diploma,education_some_college,education_associate_degree,education_bachelors_degree,education_graduate_or_professional_degree, education_HSplus,education_BAplus,first_case_date_cbsa,ethnicity_concept_name,gender_concept_name,CVD,MD,RD,age_gte65,
		NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY poverty_status) AS poverty_status_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY age_at_pos) AS age_at_pos_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY MDs) AS MDs_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY pm25) AS pm25_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY pm25_2d) AS pm25_2d_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY pm25_5d) AS pm25_5d_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY pm25_7d) AS pm25_7d_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY pm25_21d) AS pm25_21d_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY new_cases_7d) AS new_cases_7d_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY days_from_1st_case_cbsa) AS days_from_1st_case_cbsa_Quartile,
        NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_lt_hs_diploma) AS education_lt_hs_diploma_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_hs_diploma) AS education_hs_diploma_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_some_college) AS education_some_college_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_associate_degree) AS education_associate_degree_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_bachelors_degree) AS education_bachelors_degree_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_graduate_or_professional_degree) AS education_graduate_or_professional_degree_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_HSplus) AS education_HSplus_Quartile,
	    NTILE(4) OVER (PARTITION BY ch_analytic_wmeans.CBSA_CODE ORDER BY education_BAplus) AS education_BAplus_Quartile
	FROM
		ch_analytic_wmeans
        WHERE case=1 and pm25_21d IS NOT NULL and pm25_21d_valid>17 AND Precip IS NOT NULL AND new_cases_7d IS NOT NULL and CBSA_CODE IS NOT NULL
) Vals
GROUP BY
	CBSA_CODE,first_case_date_cbsa
ORDER BY
	CBSA_CODE,first_case_date_cbsa

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.17e0104c-adb1-4c12-a519-8bb3ddc713a8"),
    cbsa_summary_stats_final=Input(rid="ri.foundry.main.dataset.56352f2c-4cb6-4964-b894-f48e2f87c0f8")
)
/* ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        3/15/2023
# Last modified:  4/27/2023
# Purpose:        Create CBSA-level Table 1 type summary statistics. This program limits CBSA summaries to those in which there are at least 50 valid pm25 21-day averages; with 'valid' defined as 18+ days non-missing PM2.5 data. 

# Inputs:         'cbsa_summary_stats_final'
# Outputs:        'cbsa_summary_stats_final_sub'
#
#==============================================================================*/

SELECT *
FROM cbsa_summary_stats_final
where pm25_21d_Count>=50

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fbeb5202-ffaa-43db-80d6-5a53df2a4ee0"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        12/19/2022
Last modified:  12/19/2022
Purpose:        Generate aggregate counts of complete cases within CBSA to link back to dataset such that I can limit CBSA subanalyses to CBSA that have at least X cases

Inputs:         'ch_analytic_wmeans'
Outputs:         'cbsacounts'
==============================================================================*/

SELECT CBSA_CODE, count(*) as cbsa_obs
FROM ch_analytic_wmeans

WHERE year=2020 AND pm25_21d IS NOT NULL AND Precip IS NOT NULL and case=1 and  CBSA_CODE is not null
group by CBSA_CODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.00cce7c1-05eb-46ca-96ab-91cc090ab1e0"),
    Pm25_distance_agg_wlag=Input(rid="ri.foundry.main.dataset.37a67b1f-6f0f-4c08-bff2-1f4e4c8f67ad"),
    ch_final_elig_date=Input(rid="ri.foundry.main.dataset.daf82c61-8c55-4f56-ac8b-1976a06b5da4")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        09/16/2022
Last modified:  04/27/2023
Purpose:        Creation of basic analytic dataset, linking PM2.5 case and control
                PM2.5 measurements

Inputs:         'ch_final_elig', 'Pm25_distance_agg_wlag'
Outputs:         ch_analytic

Revisions:       01/26/2023 - Adding NO2 and Ozone
                 04/27/2023 - Revising PM variables to be missing when they do not meet criteria for number of valid values
==============================================================================*/

SELECT distinct a.*, 
/*b.pm25, b.pm25_2d,b.pm25_2d_valid,b.pm25_5d,b.pm25_5d_valid,b.pm25_7d,b.pm25_7d_valid,b.pm25_21d,b.pm25_21d_valid, */
b.pm25, b.pm25_2d_valid,b.pm25_5d_valid,b.pm25_7d_valid,b.pm25_21d_valid,
CASE WHEN b.pm25_2d_valid=2 then b.pm25_2d ELSE NULL END as pm25_2d,
CASE WHEN b.pm25_5d_valid=5 then b.pm25_5d ELSE NULL END as pm25_5d,
CASE WHEN b.pm25_7d_valid>5 then b.pm25_7d ELSE NULL END as pm25_7d,
CASE WHEN b.pm25_21d_valid>17 then b.pm25_21d ELSE NULL END as pm25_21d,
b.date as pm_date,
/*c.DASYPOP, c.Area, c.Precip, c.MeanTemp as MeanTemp_C, (c.MeanTemp*1.8)+32 as MeanTemp_F,*/
NTILE(3) OVER ( ORDER BY poverty_status) AS poverty_tertile,
NTILE(3) OVER ( ORDER BY MDs) AS MD_tertile,
CAST(b.date as date), 
CASE    WHEN a.macrovisit_start_date = b.date then 1 
        WHEN a.macrovisit_start_date!=b.date and b.date is not null then 0
        ELSE NULL END AS case
FROM ch_final_elig_date a 
LEFT JOIN Pm25_distance_agg_wlag b on a.year=b.year AND a.weekday=b.weekday AND a.month=b.month AND a.ZCTA=b.ZCTA

/*LEFT JOIN No2_distance_agg_wlag c on a.year=c.year AND a.weekday=c.weekday AND a.month=c.month AND a.ZCTA=c.ZCTA
LEFT JOIN Ozone_distance_agg_wlag d on a.year=d.year AND a.weekday=d.weekday AND a.month=d.month AND a.ZCTA=d.ZCTA*/

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.19697fec-0a26-4d5f-a38f-f1c2bf3ef78e"),
    CBSA_obsc=Input(rid="ri.foundry.main.dataset.1875f16e-8584-43ea-bdb4-4cfb8e91128e"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        02/07/2024
Last modified:  
Purpose:        Assigning obscured codes to CBSAs for data privacy. Note this is only because for some reason the 'ch_analytic_wmeans' program would not let me add this code

Inputs:         'ch_analytic_wmeans'
Outputs:        'ch_analytic_for_graphs'

Revisions:      
==============================================================================*/

SELECT a.*, b.rnk as cbsa_obsc
FROM ch_analytic_wmeans a LEFT JOIN CBSA_obsc b on a.CBSA_Name=b.CBSA_Name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ff5bcbcb-e1a3-464f-b526-d8dfd2058f11"),
    Date_first_case_final=Input(rid="ri.foundry.main.dataset.0487bd03-e1d9-47f3-be58-dfda80d883e1"),
    ch_analytic_wpr=Input(rid="ri.foundry.main.dataset.5d78bb83-c2b6-4052-8884-4f2c556acf26")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Adding in local infection data for case and control dates (note: I also define "weekends" in this dataset but don't end up using it)

Inputs:         'ch_analytic_wpr', 'Date_first_case_final'
Outputs:        'ch_analytic_winf'

Revisions:       
==============================================================================*/

SELECT distinct a.*,
CASE WHEN a.weekday in (5,6) THEN 1
     WHEN a.weekday in (0,1,2,3,4) THEN 0
     END as weekend,
b.covid19_total_cases,b.covid19_new_cases,b.drvd_new_cases,b.moving_average as new_cases_7d, b.days_from_1st_case_county,b.days_from_1st_case_cbsa,b.first_case_date_cbsa
FROM ch_analytic_wpr a LEFT JOIN Date_first_case_final b on a.date = DATE_SUB(b.date,16) AND a.Preferred_county=b.local_code

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052"),
    ch_analytic_winf=Input(rid="ri.foundry.main.dataset.ff5bcbcb-e1a3-464f-b526-d8dfd2058f11")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  05/04/2023
Purpose:        Basic narrowing down of dataset to final analytic sample prior to running analyses, excluding criteria around PM2.5 data availability because I need that to be flexible to the type of PM2.5 measure I'm using (e.g. for 21-day average need >17 valid days, for 2-day need both days valid, for 5 days need all 5 days valid)

Inputs:         'ch_analytic_winf'
Outputs:        'ch_analytic_wmeans'

Revisions:      05/04/2023 - Omitting PM2.5 eligibility criteria from this program so that criteria can be flexibility in analysis
#               09/06/2023 - Adding race as a series of binary variables
==============================================================================*/

SELECT *,substring(ZCTA,1,3) as ZCTA3,
    case when race_categ = 'WHITE' then 1 else 0 end as race_white,
    case when race_categ = 'BLACK' then 1 else 0 end as race_black,
    case when race_categ = 'ASIAN OR PACIFIC ISLANDER' then 1 else 0 end as race_api,
    case when race_categ = 'AMERICAN INDIAN OR ALASKA NATIVE' then 1 else 0 end as race_aian,
    case when race_categ = 'OTHER OR MULTIPLE RACES' then 1 else 0 end as race_other,
    case when race_categ is NULL then 1 else 0 end as race_null,
    case when gender_3categ='Male' then 1 else 0 end as gender_male,
    case when gender_3categ='Female' then 1 else 0 end as gender_female,
    case when gender_3categ='No matching concept, missing, or unknown' then 1 else 0 end as gender_unk
FROM ch_analytic_winf  
where year=2020 and /*pm25_21d_valid>17 and*/  shift_date_yn !='Y' and zip_code is not null and ZCTA is not null and age_at_pos>=18 and MeanTemp_C is not null and days_from_1st_case_county is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5d78bb83-c2b6-4052-8884-4f2c556acf26"),
    Prism_2019_2021=Input(rid="ri.foundry.main.dataset.da8670fe-c033-4df9-82ea-8d195260e1db"),
    ch_analytic=Input(rid="ri.foundry.main.dataset.00cce7c1-05eb-46ca-96ab-91cc090ab1e0")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Adding PRISM data in for case and control dates (also minor cleaning task to create a Fahrenheit version of average temperature)

Inputs:         'ch_analytic', 'Prism_2019_2021'
Outputs:        'ch_analytic_wpr'

Revisions:       
==============================================================================*/

SELECT a.*,b.DASYPOP, b.Area, b.Precip, b.MeanTemp as MeanTemp_C, (b.MeanTemp*1.8)+32 as MeanTemp_F,b.DewPoint
FROM ch_analytic a LEFT JOIN Prism_2019_2021 b on a.zip_code=b.POSTCODE and a.pm_date=b.Date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3c842350-39c7-4601-a6c7-528b6b2b0408"),
    ch_final_elig_date=Input(rid="ri.foundry.main.dataset.daf82c61-8c55-4f56-ac8b-1976a06b5da4")
)
SELECT *
FROM ch_final_elig_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ea9f5726-53a2-4377-9081-081211ea431c"),
    Date_first_case_final=Input(rid="ri.foundry.main.dataset.0487bd03-e1d9-47f3-be58-dfda80d883e1"),
    Pm25_distance_agg_wlag=Input(rid="ri.foundry.main.dataset.37a67b1f-6f0f-4c08-bff2-1f4e4c8f67ad"),
    ch_final_elig=Input(rid="ri.foundry.main.dataset.c6321f32-bcbd-4b7c-a28b-a4f7db41d8e2")
)
SELECT distinct a.*,case when year(a.poslab_date)=2020 and year(a.macrovisit_start_date)=2021 then 'exclude' else 'include' end as flag,b.pm25_2d_valid, b.pm25_5d_valid,b.pm25_21d_valid,c.moving_average as new_cases_7d
FROM ch_final_elig a LEFT JOIN Pm25_distance_agg_wlag b on a.ZCTA=b.ZCTA and a.macrovisit_start_date=b.date
LEFT JOIN Date_first_case_final c on a.Preferred_county=c.local_code and a.macrovisit_start_date=DATE_SUB(c.date,16)
where year(a.poslab_date)=2020

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c6321f32-bcbd-4b7c-a28b-a4f7db41d8e2"),
    ch_final=Input(rid="ri.foundry.main.dataset.f059513c-0de1-4029-9f1c-3d5ea8bd79d5")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  9/16/2022
Purpose:        Implement some of the pre-cleaning needed to provide descriptive statistics (ethnicity, gender, and age groups), and limit cohort to only inpatient hospital encounters

Inputs:         'ch_final'
Outputs:        'ch_final_elig'
==============================================================================*/

SELECT DISTINCT *, CASE WHEN ethnicity_concept_name = 'Not Hispanic or Latino' then 'Not Hispanic or Latino'
            WHEN ethnicity_concept_name = 'Hispanic or Latino' then 'Hispanic or Latino'
            WHEN ethnicity_concept_name in ('No matching concept','Other/Unknown','Unknown','No information','Other') or ethnicity_concept_name is NULL  then 'No matching concept, Other, or Unknown' END AS ethnicity_3categ,
        CASE    WHEN gender_concept_name = 'MALE' then 'Male'
                WHEN gender_concept_name = 'FEMALE' then 'Female'
                WHEN gender_concept_name in ('No matching concept','UNKNOWN','Gender unknown') or gender_concept_name IS NULL then 'No matching concept, missing, or unknown' END as gender_3categ,
                CASE    WHEN age_at_pos>=18 AND age_at_pos<30 then '18-29'
                        WHEN age_at_pos>=30 AND age_at_pos<50 then '30-49'
                        WHEN age_at_pos>=50 AND age_at_pos<65 then '50-64'
                        WHEN age_at_pos>=65 AND age_at_pos<76 then '65-74'
                        WHEN age_at_pos>=75 then '75+' ELSE 'Age missing/invalid' end as age_categ
FROM ch_final
WHERE covid_associated_hosp=1 /*AND age_at_pos>=18 AND year_of_hosp=2020 AND zip_code is NOT NULL AND shift_date_yn='N'*/ /*AND (pm25 IS NOT NULL or no2 is NOT NULL or ozone is not null)*/

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.daf82c61-8c55-4f56-ac8b-1976a06b5da4"),
    ch_final_elig=Input(rid="ri.foundry.main.dataset.c6321f32-bcbd-4b7c-a28b-a4f7db41d8e2")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  9/16/2022
Purpose:        Quick add of strata linking variables (weekday, month, year)
                in preparation for merge with PM2.5 data

Inputs:         'ch_final_elig'
Outputs:         ch_final_elig_date
==============================================================================*/
SELECT person_id,macrovisit_id, macrovisit_start_date,macrovisit_end_date,poslab_date,age_at_pos, ethnicity_concept_name,ethnicity_3categ,
        race_categ,race_concept_name,race_source_value, gender_concept_name,gender_3categ,CVD,RD,MD, CASE WHEN age_at_pos>=65 THEN 1 ELSE 0 END as age_gte65,age_categ,Precip as old_Precip, DASYPOP as old_DASYPOP, Area as old_Area, MeanTemp_C as old_MeanTemp_C, MeanTemp_F as old_MeanTemp_F,mean_pm25_2020, pm25_2020_gt12,pm25_avg_tert_2020,died_during_hosp,
        DewPoint as old_DewPoint,ZCTA,poverty_status,MDs,education_lt_hs_diploma,education_hs_diploma,education_some_college,education_associate_degree,education_bachelors_degree,education_graduate_or_professional_degree,education_HSplus,education_BAplus, data_partner_id,Preferred_county,CBSA_CODE, CBSA_Name,CENSUS_REGION,CENSUS_REGION_LABEL,RUCA1, RUCA1_label,RUCA2, RUCA2_label,corr_shift_date_yn as shift_date_yn,zip_code,
    weekday(macrovisit_start_date) as weekday,
    month(macrovisit_start_date) as month,
    year(macrovisit_start_date) as year
FROM ch_final_elig

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03a7738d-7803-489b-8baa-9a241d485d31"),
    ch_final=Input(rid="ri.foundry.main.dataset.f059513c-0de1-4029-9f1c-3d5ea8bd79d5")
)
/*Test dataset: count number of patients with covid-related hospitalizations in 2020 vs 2021 where covid positive test was is 2020*/

SELECT distinct person_id, macrovisit_start_date,covid_associated_hosp,poslab_date,
case when year(poslab_date)=2020 and year(macrovisit_start_date)=2021 then 'exclude' else 'include' end as flag
FROM ch_final
where covid_associated_hosp=1 and year(poslab_date)=2020

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.82567231-b83a-41f0-b4aa-7ce952cd653f"),
    ch_analytic_winf=Input(rid="ri.foundry.main.dataset.ff5bcbcb-e1a3-464f-b526-d8dfd2058f11")
)
/*Test dataset: count number of unique encounters in 2020*/

SELECT *
FROM ch_analytic_winf
where year(macrovisit_start_date)=2020 and (case=1 or ZCTA is NULL )

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aac15dce-f480-426d-832b-1e49d61d5150"),
    condition_era=Input(rid="ri.foundry.main.dataset.cbe7cbcd-4abb-4213-96de-b588c6bb3ba5")
)
SELECT distinct person_id, condition_era_start_date,condition_era_end_date,condition_concept_name
FROM condition_era
where year(condition_era_start_date)=2020 AND condition_concept_id = 37311061

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7c7a7648-dc67-4b76-afe4-1708f422c6d0"),
    PATIENT_COVID_POS_DATES=Input(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce"),
    condition_era2020=Input(rid="ri.foundry.main.dataset.aac15dce-f480-426d-832b-1e49d61d5150")
)
SELECT a.*, b.covid_date, case when b.covid_date IS NOT NULL THEN 1 ELSE 0 END AS pos_test
FROM condition_era2020 a LEFT JOIN PATIENT_COVID_POS_DATES b
ON a.person_id=b.person_id and b.covid_date between a.condition_era_start_date and a.condition_era_end_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0172d29c-e5bc-42dd-ab1f-cf129a45ea46"),
    test_x_era=Input(rid="ri.foundry.main.dataset.0ffb3d94-b305-485a-82c5-9523a0a401a0")
)
SELECT person_id, count(*) as pos_count
FROM test_x_era
group by person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ac7a3e6-c72d-4eea-bc14-44bf020371e3"),
    covid_pos_dedup=Input(rid="ri.foundry.main.dataset.b2d2aec9-32ed-4f41-9f91-549a004b046c")
)
SELECT person_id,count(*) as pos_count
FROM covid_pos_dedup
group by person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.983c4582-147f-456a-8d5e-db603cf7c442"),
    ch_final=Input(rid="ri.foundry.main.dataset.f059513c-0de1-4029-9f1c-3d5ea8bd79d5"),
    covid_pos_dedup=Input(rid="ri.foundry.main.dataset.b2d2aec9-32ed-4f41-9f91-549a004b046c")
)
SELECT *
FROM covid_pos_dedup

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b2d2aec9-32ed-4f41-9f91-549a004b046c"),
    PATIENT_COVID_POS_DATES=Input(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce")
)
SELECT *
FROM PATIENT_COVID_POS_DATES
where year(covid_date)=2020 and (days_since_pos_pcr_antigen is NULL or days_since_pos_pcr_antigen>90)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.eedd643d-b678-4776-94af-57d6aa72d201"),
    BU_SC_SDOH_N3C_2018_20201130=Input(rid="ri.foundry.main.dataset.f9fb2781-bed3-421e-bb57-6eaa24ddd85d"),
    covid_pos_wzip_zcta=Input(rid="ri.foundry.main.dataset.a2f1fb1a-7fc6-40e0-8542-d028baa4bd71"),
    ds_1_1_ZIP_COUNTY_HUD_file=Input(rid="ri.foundry.main.dataset.9ac4d85e-8d54-45b0-ab0c-da4180067c3e")
)
SELECT distinct a.*,b.Preferred_county,c.CBSA_CODE,c.CBSA_Name
FROM covid_pos_wzip_zcta a LEFT JOIN ds_1_1_ZIP_COUNTY_HUD_file b on a.zip=b.ZIP
LEFT JOIN BU_SC_SDOH_N3C_2018_20201130 c on b.Preferred_county=c.FIPS_CODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a2f1fb1a-7fc6-40e0-8542-d028baa4bd71"),
    Covid_pos_wzip=Input(rid="ri.foundry.main.dataset.cc1d7a4e-ab05-4bb7-959a-346f9694ed36"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
SELECT a.*,b.ZCTA
FROM Covid_pos_wzip a LEFT JOIN ZiptoZcta_Crosswalk_2021_ziptozcta2020 b
on a.zip=b.ZIP_CODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3f963728-c514-4ac7-878a-52e068f53b4c"),
    ch_final=Input(rid="ri.foundry.main.dataset.f059513c-0de1-4029-9f1c-3d5ea8bd79d5")
)
/*Test dataset: count number of unique people in original 'ch_final' dataset that did not have a covid-related hospitalization*/

SELECT DISTINCT(person_id)
FROM ch_final
WHERE poslab_date IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f26e7e58-cbb4-47b8-8296-af5fdcfeea3c"),
    ch_final=Input(rid="ri.foundry.main.dataset.f059513c-0de1-4029-9f1c-3d5ea8bd79d5")
)
SELECT *
FROM ch_final
WHERE age_at_hosp > 18

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.119b2b71-7e0c-411c-93fe-0122ceba9c22"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
/*Very specific purpose for this program - count the number of people included in overall (non-sub) analysis*/
SELECT distinct macrovisit_id,person_id
FROM ch_analytic_wmeans
where pm25_21d is not null and MeanTemp_C is not null and Precip is not null and DewPoint is not null and new_cases_7d is not null and days_from_1st_case_cbsa is not null and case=1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fd672be0-d084-457e-9950-383baec7d016"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
SELECT distinct a.CBSA_CODE,a.year,b.CBSA_Name,b.pm25_cbsa_avg2020,b.valid_obs
FROM ch_analytic_wmeans a LEFT JOIN Pm25_agg_cbsa2020 b
on a.CBSA_CODE=b.CBSA_CODE and a.year=b.year

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.32c2804c-c35f-4d93-b96e-4c9cd0e813f7"),
    table_1_prep=Input(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:       Compute medians for 2-, 5-, and 21-day PM2.5 (note that this was program was added on to accomplish a very specific task based on a reviewer request)

Inputs:         'table_1_prep'
Outputs:        'Table1 output allchars non normal1'
==============================================================================*/

SELECT DISTINCT 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm25_2d) OVER() AS Median_pm25_2d,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm25_5d) OVER() AS Median_pm25_5d,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm25_21d) OVER() AS Median_pm25_21d
FROM table_1_prep

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c"),
    Date_first_case_final=Input(rid="ri.foundry.main.dataset.0487bd03-e1d9-47f3-be58-dfda80d883e1"),
    Pm25_distance_agg_wlag=Input(rid="ri.foundry.main.dataset.37a67b1f-6f0f-4c08-bff2-1f4e4c8f67ad"),
    ch_final_elig_date=Input(rid="ri.foundry.main.dataset.daf82c61-8c55-4f56-ac8b-1976a06b5da4")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Preparatory work to creating a Table 1 for the analysis, merge in county-level infections and pollution data (which come later in the process for the regression analysis since the regression analysis uses a case-control panel dataset) in order (1) Define who is in vs who is out of the dataset and why, (2) To be able to identify who is in the final sample. Note that I am defining everything including missingness in relation to cases only

Inputs:         'ch_final_elig_date', 'Date_first_case_final','Pm25_distance_agg_wlag'
Outputs:        'table_1_prep'
==============================================================================*/

SELECT distinct a.*,
b.pm25, b.pm25_2d_valid,b.pm25_5d_valid, b.pm25_7d_valid,b.pm25_21d_valid, 
CASE WHEN b.pm25_2d_valid=2 then b.pm25_2d ELSE NULL END as pm25_2d,
CASE WHEN b.pm25_5d_valid=5 then b.pm25_5d ELSE NULL END as pm25_5d,
CASE WHEN b.pm25_7d_valid>5 then b.pm25_7d ELSE NULL END as pm25_7d,
CASE WHEN b.pm25_21d_valid>17 then b.pm25_21d ELSE NULL END as pm25_21d,
 c.covid19_total_cases,c.covid19_new_cases,c.drvd_new_cases,c.moving_average as new_cases_7d, c.days_from_1st_case_county,c.days_from_1st_case_cbsa, a.old_DASYPOP as DASYPOP , a.old_Area as Area, a.old_Precip as Precip,a.old_DewPoint as DewPoint  , a.old_MeanTemp_C as MeanTemp_C, a.old_MeanTemp_F as MeanTemp_F,
 CASE WHEN a.shift_date_yn!='Y' and a.ZCTA is not null and b.pm25_21d_valid>17 and c.moving_average is not null and a.old_MeanTemp_C is not null then 1 ELSE 0 end as final_sample,
    CASE    WHEN a.RUCA1 = 1 then 'Metropolitan area core'
            WHEN a.RUCA1 = 2 then 'Metropolitan area high commuting'
            WHEN a.RUCA1 = 3 then 'Metropolitan area low commuting'
            WHEN a.RUCA1 in (4,5,6) then 'Micropolitan core, high/low commuting'
            WHEN a.RUCA1 in (7,8,9) then 'Small town core, high/low commuting'
            WHEN a.RUCA1 = 10 then 'Rural areas'
            WHEN a.RUCA1 = 99 then NULL ELSE NULL end as RUCA1_5categ,
        CASE WHEN a.ZCTA is NOT NULL then 'Has ZCTA' ELSE 'No ZCTA' END AS has_ZCTA,
        CASE WHEN b.pm25_21d_valid>17 then 'Has PM2.5 data' ELSE 'No PM2.5 data' END as has_pollution_data,
        CASE WHEN a.ZCTA is NOT NULL then 1 ELSE 0 END AS alt_has_ZCTA,
        CASE WHEN b.pm25_21d_valid>17 then 1 ELSE 0 END as alt_has_pollution_data
FROM ch_final_elig_date a LEFT JOIN Pm25_distance_agg_wlag b on a.macrovisit_start_date=b.date AND a.ZCTA=b.ZCTA
LEFT JOIN Date_first_case_final c on a.macrovisit_start_date = DATE_SUB(c.date,16) AND a.Preferred_county=c.local_code
where a.year=2020 and a.age_at_pos>=18  

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0ffb3d94-b305-485a-82c5-9523a0a401a0"),
    PATIENT_COVID_POS_DATES=Input(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce"),
    condition_era_2020_wtest=Input(rid="ri.foundry.main.dataset.7c7a7648-dc67-4b76-afe4-1708f422c6d0")
)
SELECT distinct a.*,b.condition_era_start_date,b.condition_era_end_date,b.condition_concept_name
FROM condition_era_2020_wtest b 
RIGHT JOIN PATIENT_COVID_POS_DATES a 
on a.covid_date between b.condition_era_start_date and b.condition_era_end_date and a.person_id=b.person_id
where year(a.covid_date)=2020 and (a.days_since_pos_pcr_antigen is NULL or a.days_since_pos_pcr_antigen>90)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a9a5c229-d94a-4821-9e53-afb3c059b3a7"),
    table_1_prep=Input(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c")
)
SELECT distinct zip_code
FROM table_1_prep
WHERE shift_date_yn='N' and age_at_pos>=18 and ZCTA is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.41639a8d-b20a-4b1b-889e-e45ca5579284"),
    table_1_prep=Input(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c")
)
SELECT distinct zip_code
FROM table_1_prep
WHERE shift_date_yn='N' and age_at_pos>=18 and ZCTA is not null and pm25_21d_valid>17

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e4b1ecec-7a6a-4cf3-815a-ee4aa678b151"),
    people=Input(rid="ri.foundry.main.dataset.119b2b71-7e0c-411c-93fe-0122ceba9c22")
)
SELECT distinct person_id 
FROM people

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.83b9e0bb-1fbb-4dfb-89c3-f7623cb2d352"),
    table_1_prep=Input(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c")
)
SELECT distinct macrovisit_id
FROM table_1_prep
WHERE shift_date_yn='N' and age_at_pos>=18 and ZCTA is not null and pm25_21d_valid>17 and Precip is NOT NULL and covid19_total_cases is NOT NULL

