

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.11d706f0-587c-40ed-a9da-601a12b3b4bd"),
    county_daily_w_cbsa=Input(rid="ri.foundry.main.dataset.2e71c00e-d2d7-47da-a4a8-367d28eaadad")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Create a 7-day moving average count of new covid cases at the county level

Inputs: 'county_daily_w_cbsa'
Output: 'county_daily_ma'
                
==============================================================================*/

SELECT
   region_id,mapbox_geoid,state_name,state_abbr,date,local_code,covid19_total_cases,CBSA_CODE,
   covid19_new_cases,drvd_new_cases,
   AVG(drvd_new_cases)
         OVER(PARTITION BY local_code ORDER BY local_code, date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS moving_average
FROM county_daily_w_cbsa

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2e71c00e-d2d7-47da-a4a8-367d28eaadad"),
    BU_SC_SDOH_N3C_2018_20201130=Input(rid="ri.foundry.main.dataset.f9fb2781-bed3-421e-bb57-6eaa24ddd85d"),
    county_daily_status=Input(rid="ri.foundry.main.dataset.99814779-5924-4e29-8df1-5d80b4da5cf7")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        Linking county-level covid statistics to CBSA name and CBSA code to help summarize at aggregate CBSA

Inputs: 'county_daily_status','BU_SC_SDOH_N3C_2018_20201130'
Output: 'county_daily_w_cbsa'
                
==============================================================================*/

SELECT a.*, CASE WHEN covid19_new_cases<0 then NULL else covid19_new_cases end as drvd_new_cases, b.CBSA_CODE, b.CBSA_Name
FROM county_daily_status a LEFT JOIN BU_SC_SDOH_N3C_2018_20201130 b
ON a.local_code = b.FIPS_CODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0487bd03-e1d9-47f3-be58-dfda80d883e1"),
    date_first_case=Input(rid="ri.foundry.main.dataset.27a0e619-0e4a-4dd8-b06f-6d7e763da771")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        
Last modified:  
Purpose:        For each possible date, create a variable that provides number of days since first COVID-19 case in the county

Inputs: 'date_first_case'
Output: 'date_first_case_final'
                
==============================================================================*/

SELECT *, DATEDIFF(date , first_case_date_county) as days_from_1st_case_county, DATEDIFF( date , first_case_date_cbsa) as days_from_1st_case_cbsa
FROM date_first_case

