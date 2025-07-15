

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9d042220-5299-44d0-a593-5b585bd26889"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810"),
    person=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
/*Simple program that adds race_source_value back into dataset so that race can be better categorized*/
SELECT distinct a.*,b.race_source_value,b.gender_source_value,b.ethnicity_source_value
FROM COVID_POS_PERSON_FACT a LEFT JOIN person b on a.person_id=b.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8034f0f3-6888-44cf-9c04-f7621dbbd6df"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7"),
    ds_2020_Gaz_zcta_national=Input(rid="ri.foundry.main.dataset.d1d07dc1-4096-463e-b585-8b2747aabff9")
)
SELECT a.*,b.INTPTLAT as LATITUDE, b.INTPTLONG as LONGITUDE, STATE as ZCTA_STATE
FROM ZiptoZcta_Crosswalk_2021_ziptozcta2020 a LEFT JOIN ds_2020_Gaz_zcta_national b
ON a.ZCTA=b.GEOID

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ced625e4-6a88-4ec5-a823-a05ee304797d"),
    No2_distance_agg=Input(rid="ri.foundry.main.dataset.e918f691-9c9e-4d73-b2a3-3b6d138f3632")
)
SELECT ZCTA,CAST(date as date), measurement_avg, num_monitors
FROM No2_distance_agg

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3b11b335-b43c-407a-9b58-4904e65c8b2a"),
    Ozone_distance_agg=Input(rid="ri.foundry.main.dataset.4d558095-990c-4bfc-a9ae-2dc839194ff1")
)
SELECT ZCTA,CAST(date as date), measurement_avg, num_monitors
FROM Ozone_distance_agg

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7aa4e50a-1a3e-412e-b8e1-b669eb8a4fd1"),
    Pm25_distance_agg=Input(rid="ri.foundry.main.dataset.3d739e51-4f74-4c50-89e6-3b347dc3fdd5")
)
SELECT ZCTA,CAST(date as date), measurement_avg, num_monitors
FROM Pm25_distance_agg

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3f31782b-fb0f-41aa-83ad-17364d956219"),
    ZCTA_Monitor_Pairs_Within_or_Within20km=Input(rid="ri.foundry.main.dataset.198610c4-b080-4c4c-b315-416055315094")
)
/*  Project: N3C Case-Crossover Study
    Author:  Alyssa Platt
    Created: 9/21/2022
    Updated: 9/21/2022
    Purpose: Create file such that I have an indicator for each ZCTA that contains at least one Monitor_ID
    
    Inputs: 'ZCTA_Monitor_Pairs_Within_or_Within20km'
    Outputs: 'ZCTA_Monitors_Within'*/
SELECT distinct ZCTA, max(WithinZCTA) as has_monitor
FROM ZCTA_Monitor_Pairs_Within_or_Within20km 
GROUP BY ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c3c26169-44ca-4873-a75e-a9b7474b8688"),
    ch_hosp_facts_wr=Input(rid="ri.foundry.main.dataset.b5e0213d-eda8-459e-a3e2-c7a718a8bdd5"),
    pay_plan_collapse=Input(rid="ri.foundry.main.dataset.bd566ee8-feea-40df-9726-71c0d36daeb7")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/6/2022
Last modified:  10/6/2022
Purpose:        add insurance variables into build of cohort dataset
==============================================================================*/

SELECT a.*,b.payer_null,
b.payer_NMC,
b.payer_medicare,
b.payer_private,
b.payer_medicaid,
b.payer_none_listed,
b.payer_gov_other,
b.payer_dod,
b.payer_misc,
b.payer_doc
FROM ch_hosp_facts_wr a LEFT JOIN pay_plan_collapse b 
ON a.person_id=b.person_id AND a.macrovisit_start_date=b.macrovisit_start_date 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a8b42791-c770-4ed3-bdf5-a8df609cb1ea"),
    COVID_PATIENT_COMORBIDITIES=Input(rid="ri.foundry.main.dataset.203392f0-b875-453c-88c5-77ca5223739e"),
    ch_facts_ins=Input(rid="ri.foundry.main.dataset.c3c26169-44ca-4873-a75e-a9b7474b8688")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/6/2022
Last modified:  10/6/2022
Purpose:        Use the start date of condition X and assess whether patient had that comorbidity identified at the time of the hospitalization and add these as dummies to the growing analytic dataset
==============================================================================*/
SELECT a.*,
    CASE WHEN b.CARDIOMYOPATHIES_start<=poslab_date OR b.CEREBROVASCULAR_DISEASE_start<=poslab_date OR b.CONGESTIVE_HEART_FAILURE_start<=poslab_date OR b.CORONARY_ARTERY_DISEASE_start<=poslab_date OR b.HEART_FAILURE_start<=poslab_date OR b.HYPERTENSION_start<=poslab_date OR b.MYOCARDIAL_INFARCTION_start<=poslab_date OR b.PERIPHERAL_VASCULAR_DISEASE_start<=poslab_date THEN 1 ELSE 0 END AS CVD,
    CASE    WHEN b.CHRONIC_LUNG_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as RD,
    CASE WHEN b.DIABETES_COMPLICATED_start<=poslab_date OR b.DIABETES_UNCOMPLICATED_start<=poslab_date OR b.OBESITY_start<=poslab_date THEN 1 ELSE 0 END AS MD,

    CASE    WHEN b.CARDIOMYOPATHIES_start<=poslab_date THEN 1 ELSE 0
            END as CARDIOMYOPATHIES,
    CASE    WHEN b.CEREBROVASCULAR_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as CEREBROVASCULAR_DISEASE,
    CASE    WHEN b.CHRONIC_LUNG_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as CHRONIC_LUNG_DISEASE,
    CASE   WHEN b.CONGESTIVE_HEART_FAILURE_start<=poslab_date THEN 1 ELSE 0
           END as CONGESTIVE_HEART_FAILURE,  
    CASE    WHEN b.CORONARY_ARTERY_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as CORONARY_ARTERY_DISEASE,    
    CASE    WHEN b.DEMENTIA_start<=poslab_date THEN 1 ELSE 0
            END as DEMENTIA,       
    CASE    WHEN b.DEPRESSION_start<=poslab_date THEN 1 ELSE 0
            END as DEPRESSION,     
    CASE    WHEN b.DIABETES_COMPLICATED_start<=poslab_date THEN 1 ELSE 0
            END as DIABETES_COMPLICATED,  
    CASE    WHEN b.DIABETES_UNCOMPLICATED_start<=poslab_date THEN 1 ELSE 0
            END as DIABETES_UNCOMPLICATED,  
    CASE    WHEN b.DOWN_SYNDROME_start<=poslab_date THEN 1 ELSE 0
            END as DOWN_SYNDROME,  
    CASE    WHEN b.HEART_FAILURE_start<=poslab_date THEN 1 ELSE 0
            END as HEART_FAILURE, 
    CASE   WHEN b.HEMIPLEGIA_OR_PARAPLEGIA_start<=poslab_date THEN 1 ELSE 0
           END as HEMIPLEGIA_OR_PARAPLEGIA, 
    CASE    WHEN b.HIV_INFECTION_start<=poslab_date THEN 1 ELSE 0
            END as HIV_INFECTION, 
    CASE    WHEN b.HYPERTENSION_start<=poslab_date THEN 1 ELSE 0
            END as HYPERTENSION, 
    CASE    WHEN b.KIDNEY_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as KIDNEY_DISEASE, 
    CASE    WHEN b.MALIGNANT_CANCER_start<=poslab_date THEN 1 ELSE 0
            END as MALIGNANT_CANCER, 
    CASE    WHEN b.METASTATIC_SOLID_TUMOR_CANCERS_start<=poslab_date THEN 1 ELSE 0
            END as METASTATIC_SOLID_TUMOR_CANCERS, 
    CASE    WHEN b.MILD_LIVER_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as MILD_LIVER_DISEASE, 
    CASE    WHEN b.MODERATE_OR_SEVERE_LIVER_DISEASE_start<=poslab_date THEN 0 ELSE 0
            END as MODERATE_OR_SEVERE_LIVER_DISEASE, 
    CASE    WHEN b.MYOCARDIAL_INFARCTION_start<=poslab_date THEN 1 ELSE 0
            END as MYOCARDIAL_INFARCTION, 
    CASE    WHEN b.OBESITY_start<=poslab_date THEN 1 ELSE 0
            END as OBESITY, 
    CASE    WHEN b.PEPTIC_ULCER_start<=poslab_date THEN 1 ELSE 0
            END as PEPTIC_ULCER, 
    CASE    WHEN b.PERIPHERAL_VASCULAR_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as PERIPHERAL_VASCULAR_DISEASE, 
    CASE    WHEN b.PSYCHOSIS_start<=poslab_date THEN 1 ELSE 0
            END as PSYCHOSIS, 
    CASE    WHEN b.RHEUMATOLOGIC_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as RHEUMATOLOGIC_DISEASE, 
    CASE    WHEN b.SICKLE_CELL_DISEASE_start<=poslab_date THEN 1 ELSE 0
            END as SICKLE_CELL_DISEASE, 
    CASE    WHEN b.SUBSTANCE_ABUSE_start<=poslab_date THEN 1 ELSE 0
            END as SUBSTANCE_ABUSE, 
    CASE    WHEN b.THALASSEMIA_start<=poslab_date THEN 1 ELSE 0
            END as THALASSEMIA, 
/*    CASE    WHEN b.TOBACCO_SMOKER_start<=macrovisit_start_date THEN 'TRUE' ELSE 'FALSE'
            END as TOBACCO_SMOKER, */
    CASE    WHEN b.TUBERCULOSIS_start<=poslab_date THEN 1 ELSE 0
            END as TUBERCULOSIS, 
b.CARDIOMYOPATHIES_start,
b.CEREBROVASCULAR_DISEASE_start,
b.CHRONIC_LUNG_DISEASE_start,
b.CONGESTIVE_HEART_FAILURE_start,
b.CORONARY_ARTERY_DISEASE_start,
b.DEMENTIA_start,
b.DEPRESSION_start,
b.DIABETES_COMPLICATED_start,
b.DIABETES_UNCOMPLICATED_start,
b.DOWN_SYNDROME_start,
b.HEART_FAILURE_start,
b.HEMIPLEGIA_OR_PARAPLEGIA_start,
b.HIV_INFECTION_start,
b.HYPERTENSION_start,
b.KIDNEY_DISEASE_start,
b.MALIGNANT_CANCER_start,
b.METASTATIC_SOLID_TUMOR_CANCERS_start,
b.MILD_LIVER_DISEASE_start,
b.MODERATE_OR_SEVERE_LIVER_DISEASE_start,
b.MYOCARDIAL_INFARCTION_start,
b.OBESITY_start,
b.PEPTIC_ULCER_start,
b.PERIPHERAL_VASCULAR_DISEASE_start,
b.PSYCHOSIS_start,
b.RHEUMATOLOGIC_DISEASE_start,
b.SICKLE_CELL_DISEASE_start,
b.SUBSTANCE_ABUSE_start,
b.THALASSEMIA_start,
/*b.TOBACCO_SMOKER_start,*/
b.TUBERCULOSIS_start

FROM ch_facts_ins a LEFT JOIN COVID_PATIENT_COMORBIDITIES b 
ON a.person_id=b.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f059513c-0de1-4029-9f1c-3d5ea8bd79d5"),
    ZCTA_Monitors_Within=Input(rid="ri.foundry.main.dataset.3f31782b-fb0f-41aa-83ad-17364d956219"),
    ch_wmanifest=Input(rid="ri.foundry.main.dataset.9ca7d8b2-0c66-447f-a51f-3488186dffd9")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  10/16/2022
Purpose:        Add dummy for having a monitor in the zip code area to analytic dataset (final step)

Inputs:         'ch_sdoh', 'ZCTA_Monitors_Within'
Outputs:        'ch_final'
==============================================================================*/

SELECT distinct a.*,b.has_monitor,
    CASE    WHEN SUBSTRING(a.Preferred_county,1,2) in ('09','2','25','33','34','36','42','44','50','23') THEN 1
            WHEN SUBSTRING(a.Preferred_county,1,2) in ('17','18','19','20','26','27','29','31','38','39','46','55') THEN 2
            WHEN SUBSTRING(a.Preferred_county,1,2) in ('01','05','10','11','12','13','21','22','24','28','37','40','45','47','48','51','54') THEN 3
            WHEN SUBSTRING(a.Preferred_county,1,2) in ('02','04','06','08','15','16','30','32','35','41','49','53','56') THEN 4 END AS CENSUS_REGION,
    CASE    WHEN SUBSTRING(a.Preferred_county,1,2) in ('09','2','25','33','34','36','42','44','50','23') THEN 'Northeast'
            WHEN SUBSTRING(a.Preferred_county,1,2) in ('17','18','19','20','26','27','29','31','38','39','46','55') THEN 'Midwest'
            WHEN SUBSTRING(a.Preferred_county,1,2) in ('01','05','10','11','12','13','21','22','24','28','37','40','45','47','48','51','54') THEN 'South'
            WHEN SUBSTRING(a.Preferred_county,1,2) in ('02','04','06','08','15','16','30','32','35','41','49','53','56') THEN 'West' END AS CENSUS_REGION_LABEL           
FROM ch_wmanifest a LEFT JOIN ZCTA_Monitors_Within b 
ON a.ZCTA=b.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.621b33c5-d410-4143-9b60-c7f9b9924da8"),
    COHORT_HOSPITALIZATIONS=Input(rid="ri.foundry.main.dataset.986e7e7e-b533-48b0-aa61-92a77c10e8c5"),
    COVID_POS_PERSON_FACT_wOR=Input(rid="ri.foundry.main.dataset.9d042220-5299-44d0-a593-5b585bd26889")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/14/2022
Last modified:  9/14/2022
Purpose:        Link cohort hospitalizations (created by Elliott) to person facts
                such that I have enough information to employ eligibility criteria
                for the analytic cohort and to that end create necessary dummies
                with criteria, including:
                1. Hospitalization for covid (previously defined, dummy already created)
                2. Hospitalization in year 2020-2021 (one overall dummy and separate dummies by year)
                3. Age 18+
                4. Has zip code
                5. Zip code links to ZCTA
                6. Non-date shifting health system
==============================================================================*/
SELECT COHORT_HOSPITALIZATIONS.*,
DATE_ADD(COVID_POS_PERSON_FACT_wOR.date_of_birth,14) as date_of_birth, 
CASE WHEN (DATEDIFF(COHORT_HOSPITALIZATIONS.macrovisit_start_date,DATE_ADD(COVID_POS_PERSON_FACT_wOR.date_of_birth,14)))/365 BETWEEN 0 and 110 then (DATEDIFF(COHORT_HOSPITALIZATIONS.macrovisit_start_date,DATE_ADD(COVID_POS_PERSON_FACT_wOR.date_of_birth,14)))/365 ELSE NULL END as age_at_hosp,
CASE WHEN (DATEDIFF(COHORT_HOSPITALIZATIONS.poslab_date,DATE_ADD(COVID_POS_PERSON_FACT_wOR.date_of_birth,14)))/365 BETWEEN 0 and 110 then (DATEDIFF(COHORT_HOSPITALIZATIONS.poslab_date,DATE_ADD(COVID_POS_PERSON_FACT_wOR.date_of_birth,14)))/365 ELSE NULL END as age_at_pos,
year(macrovisit_start_date) as year_of_hosp,
year(macrovisit_start_date)=2020 as year_of_hosp2020,
year(macrovisit_start_date)=2021 as year_of_hosp2021,
year(macrovisit_start_date) in (2020,2021) as year_of_hosp2020_2021,
COVID_POS_PERSON_FACT_wOR.ethnicity_concept_name,
COVID_POS_PERSON_FACT_wOR.ethnicity_concept_name in ('Not Hispanic or Latino','Hispanic or Latino') as valid_ethnicity,
COVID_POS_PERSON_FACT_wOR.race_concept_name,
COVID_POS_PERSON_FACT_wOR.race_concept_name !in ('No matching concept', 'null','No information','Unknown','Refuse to answer','Unknown racial group') as valid_race,
COVID_POS_PERSON_FACT_wOR.race_source_value,
COVID_POS_PERSON_FACT_wOR.gender_concept_name,
COVID_POS_PERSON_FACT_wOR.gender_concept_name in ('Male','Female') as gender_binary,
COVID_POS_PERSON_FACT_wOR.gender_source_value,
COVID_POS_PERSON_FACT_wOR.ethnicity_source_value,
COVID_POS_PERSON_FACT_wOR.zip_code, 
COVID_POS_PERSON_FACT_wOR.city as pf_city,
COVID_POS_PERSON_FACT_wOR.state as pf_state,
COVID_POS_PERSON_FACT_wOR.shift_date_yn,
COVID_POS_PERSON_FACT_wOR.death_date,
CASE WHEN COVID_POS_PERSON_FACT_wOR.death_date>=COHORT_HOSPITALIZATIONS.macrovisit_start_date and COVID_POS_PERSON_FACT_wOR.death_date<=COHORT_HOSPITALIZATIONS.macrovisit_end_date THEN 1 ELSE 0 END as died_during_hosp,
COVID_POS_PERSON_FACT_wOR.death_recorded
FROM COHORT_HOSPITALIZATIONS LEFT JOIN COVID_POS_PERSON_FACT_wOR on COHORT_HOSPITALIZATIONS.person_id= COVID_POS_PERSON_FACT_wOR.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b5e0213d-eda8-459e-a3e2-c7a718a8bdd5"),
    ch_hosp_facts=Input(rid="ri.foundry.main.dataset.621b33c5-d410-4143-9b60-c7f9b9924da8")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/6/2022
Last modified:  10/6/2022
Purpose:        Reclassify racial categories based on frequency
 
Revisions       02/21/2023 - add reclassification of gender and ethnicity into clean categories
==============================================================================*/

SELECT *, 
CASE 
    WHEN race_concept_name in ('Black','African American','Black or African American','Haitian','African','Jamaican','Dominica Islander','Barbadian') THEN 'BLACK'
    WHEN race_concept_name='White' THEN 'WHITE'
    WHEN race_concept_name in ('Asian','Asian or Pacific islander','Other Pacific Islander','Filipino','Chinese','Chinese','Vietnamese','Japanese','Bangladeshi','Pakistani','West Indian','Laotian','Taiwanese','Thai','Sri Lankan','Burmese','Okinawan','Cambodian','Bhutanese','Singaporean','Hmong','Malaysian','Indonesian','Korean','Nepalese','Maldivian','Asian Indian') THEN 'ASIAN OR PACIFIC ISLANDER'
    WHEN    race_concept_name LIKE '%Pacific%' OR 
            race_concept_name LIKE '%Polynesian%' OR
            race_concept_name LIKE '%Native Hawaiian%' OR
            race_concept_name LIKE '%Micronesian%' OR
            race_concept_name LIKE '%Melanesian%' OR
            race_concept_name LIKE '%Melanesian%' THEN 'ASIAN OR PACIFIC ISLANDER'
    WHEN race_concept_name in ('Trinidadian','Other', 'Other Race','Multiple race','Multiple races','More than one race') THEN 'OTHER OR MULTIPLE RACES'
    WHEN race_concept_name = 'American Indian or Alaska Native' THEN 'AMERICAN INDIAN OR ALASKA NATIVE'
    WHEN race_concept_name in ('Unknown','Unknown racial group','No information','Refuse to answer') OR race_concept_name is NULL THEN 'UNKNOWN'
    /*WHEN race_concept_name is null then 'UNKNOWN' */
    WHEN /*race_concept_name is 'No matching concept' AND*/ upper(race_source_value) in ('OT','R9 OTHER','OTHER COMBINATIONS NOT DESCRIBED', 'OTHER RACE','OTHER','MULTIPLE RACE','.OTHER','OT\Y','O','MULTIRACIAL',
'MULTI-RACIAL','B,W','W,O','DECLINED/OTHER','OT\N','O,W','W,B','BLACK | WHITE','A,W','I,W','WHITE~BLACK OR AFRICAN AMERICAN','ASIAN | WHITE','W,A','MULTIPLE','WHITE | ASIAN', 'B,O','A,O','B,A','O,B','DECLINED | MULTIRACIAL','WHITE | OTHER PACIFIC ISLANDER','NATIVE HAWAIIAN | OTHER PACIFIC ISLANDER | WHITE','WHITE | MULTIRACIAL','SOME OTHER RACE','WHITE~WHITE~BLACK OR AFRICAN AMERICAN','W,I','O,A','OT\R','BLACK OR AFRICAN AMERICAN~WHITE','OTHER PACIFIC ISLANDER | WHITE','MULTI RACIAL','BLACK | MULTIRACIAL','WHITE | BLACK | AMERICAN INDIAN/ALASKA NATIVE','AMERICAN INDIAN/ALASKA NATIVE | BLACK | WHITE','ASIAN | BLACK','MULTIRACIAL | DECLINED','AMERICAN INDIAN/ALASKA NATIVE | BLACK','MULTIRACIAL | DECLINED','AMERICAN INDIAN/ALASKA NATIVE | BLACK','OT\UN','WHITE | BLACK','NATIVE HAWAIIAN OR PACIFIC ISLANDER~WHITE','ASIAN~WHITE','WHITE~BLACK OR AFRICAN AMERICAN~PATIENT REFUSES OR','OTHER/YES','W,A,O','O,I','WHITE | NATIVE HAWAIIAN','I,B,W','I,O','OTHER NOT DESCRIBED','B,W,O','W,P','BLACK | OTHER PACIFIC ISLANDER','B,A,W','A,W,O','NATIVE HAWAIIAN | WHITE','BLACK | WHITE | AMERICAN INDIAN/ALASKA NATIVE','B,I','O,B,W','ASIAN | NATIVE HAWAIIAN | WHITE','WHITE\E\R\E\BLACK/AFRICAN AMERIC','W,B,O','UNKNOWN | MULTIRACIAL','A,B','W,B,A','W,O,B','OTHER/NO') THEN 'OTHER OR MULTIPLE RACES' ELSE 'UNKNOWN'
    END AS race_categ 
FROM ch_hosp_facts

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b739ea82-cb2c-4870-8fbf-e996b275c4b7"),
    CW_ZIP_ZCTA_wcoord=Input(rid="ri.foundry.main.dataset.8034f0f3-6888-44cf-9c04-f7621dbbd6df"),
    ch_facts_ins_co=Input(rid="ri.foundry.main.dataset.a8b42791-c770-4ed3-bdf5-a8df609cb1ea"),
    zip_county_cbsa=Input(rid="ri.foundry.main.dataset.aa35f8bf-f1f6-435b-a017-f752a265acf6")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/6/2022
Last modified:  12/16/2022
Purpose:        Add ZCTAs, counties (FIPS), and CBSA codes to the analytic dataset
==============================================================================*/

SELECT a.*,b.ZCTA,b.STATE,b.LATITUDE,b.LONGITUDE, c.Preferred_county,c.CBSA_CODE, c.CBSA_Name
FROM ch_facts_ins_co a LEFT JOIN CW_ZIP_ZCTA_wcoord b on trim(a.zip_code) = trim(b.ZIP_CODE)
LEFT JOIN zip_county_cbsa c on a.zip_code=c.ZIP

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.33ccb708-5f92-4260-b3cb-cb8c474d0490"),
    Pm25_distance_agg1=Input(rid="ri.foundry.main.dataset.7aa4e50a-1a3e-412e-b8e1-b669eb8a4fd1"),
    ch_hosp_facts_zcta=Input(rid="ri.foundry.main.dataset.b739ea82-cb2c-4870-8fbf-e996b275c4b7")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  9/16/2022
Purpose:        Link cohort hospitalization data with dummy indicators to PM2.5
                data such that I can describe availability of pollution/climate data
                when I describe the cohort. For purpose of simple sample description I 
                am only linking to the hospitalization case day to determine basic data availability

Inputs:         'ch_hosp_facts_zcta', 'Pm25_distance_agg1'
Outputs:         ch_pm25
==============================================================================*/

SELECT ch_hosp_facts_zcta.*,CAST(ch_hosp_facts_zcta.ZCTA AS INTEGER) as ZCTA_num,
    Pm25_distance_agg1.measurement_avg as pm25
FROM ch_hosp_facts_zcta LEFT JOIN Pm25_distance_agg1 on ch_hosp_facts_zcta.macrovisit_start_date = Pm25_distance_agg1.date AND ch_hosp_facts_zcta.ZCTA=Pm25_distance_agg1.ZCTA
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.de628a17-49aa-4e55-85e1-ff00bc060033"),
    Pm25_avg_tert_2020=Input(rid="ri.foundry.main.dataset.18d88a94-5c20-455b-ad2b-9f0c8c7babd1"),
    ch_prism=Input(rid="ri.foundry.main.dataset.88467fd4-090b-4905-a95a-f86709a5ce4a")
)
SELECT a.*,b.mean_pm25_2020, b.pm25_2020_gt12, pm25_avg_tert_2020
FROM ch_prism a LEFT JOIN Pm25_avg_tert_2020 b
on a.ZCTA_num=b.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.96e7f9e5-a9b7-4e83-b548-32877155b224"),
    No2_distance_agg1=Input(rid="ri.foundry.main.dataset.ced625e4-6a88-4ec5-a823-a05ee304797d"),
    ch_pm25=Input(rid="ri.foundry.main.dataset.33ccb708-5f92-4260-b3cb-cb8c474d0490")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  9/16/2022
Purpose:        Link cohort hospitalization data with dummy indicators to No2
                data such that I can describe availability of pollution/climate data
                when I describe the cohort. For purpose of simple sample description I 
                am only linking to the hospitalization case day to determine basic data availability

Inputs:         'ch_pm25', 'No2_distance_agg'
Outputs:         ch_pm25_no2
==============================================================================*/

SELECT ch_pm25.*,
    l0.measurement_avg as no2
    
FROM ch_pm25 
    LEFT JOIN No2_distance_agg1 l0 on ch_pm25.macrovisit_start_date = l0.date AND ch_pm25.ZCTA=l0.ZCTA
        

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.16c70318-cb81-4920-901b-48e12c0815a6"),
    Ozone_distance_agg1=Input(rid="ri.foundry.main.dataset.3b11b335-b43c-407a-9b58-4904e65c8b2a"),
    ch_pm25_no2=Input(rid="ri.foundry.main.dataset.96e7f9e5-a9b7-4e83-b548-32877155b224")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  9/16/2022
Purpose:        Link cohort hospitalization data with dummy indicators to No2
                data such that I can describe availability of pollution/climate data
                when I describe the cohort. For purpose of simple sample description I 
                am only linking to the hospitalization case day to determine basic data availability

Inputs:         'ch_pm25_no2', 'ozone_distance_agg'
Outputs:         ch_pm25_no2_ozone
==============================================================================*/

SELECT ch_pm25_no2.*,
    l0.measurement_avg as ozone
FROM ch_pm25_no2 
    LEFT JOIN Ozone_distance_agg1 l0 on ch_pm25_no2.macrovisit_start_date = l0.date AND ch_pm25_no2.ZCTA=l0.ZCTA
 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.88467fd4-090b-4905-a95a-f86709a5ce4a"),
    ch_pm25_no2_ozone=Input(rid="ri.foundry.main.dataset.16c70318-cb81-4920-901b-48e12c0815a6"),
    prism_2019_2021=Input(rid="ri.foundry.main.dataset.da8670fe-c033-4df9-82ea-8d195260e1db")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  10/16/2022
Purpose:        Add PRISM data to the analytic dataset

Inputs:         'prism_2019_2021', 'ch_pm25_no2_ozone'
Outputs:         'ch_pm25_no2_ozone'
Revisions:       02/14/2023 - Adding PM2.5 2020 average as another join (to round out pollution info)
==============================================================================*/
SELECT ch_pm25_no2_ozone.*,
prism_2019_2021.DASYPOP,
prism_2019_2021.Area,
prism_2019_2021.Precip,
prism_2019_2021.MeanTemp as MeanTemp_C,
(prism_2019_2021.MeanTemp*1.8)+32 as MeanTemp_F,
prism_2019_2021.DewPoint
FROM ch_pm25_no2_ozone LEFT JOIN prism_2019_2021
ON  ch_pm25_no2_ozone.macrovisit_start_date=prism_2019_2021.Date and 
    ch_pm25_no2_ozone.zip_code=prism_2019_2021.POSTCODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.984c4847-1626-4a6f-8247-abd08729ac9c"),
    COVID_PATIENT_SDOH=Input(rid="ri.foundry.main.dataset.eb277a38-7a8c-4002-9f36-7f633894b37a"),
    RUCA2020zipcode=Input(rid="ri.foundry.main.dataset.0b1fe3a9-ec4c-4cdb-a26c-96ca0a255ec1"),
    ch_pm25_2020=Input(rid="ri.foundry.main.dataset.de628a17-49aa-4e55-85e1-ff00bc060033")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        9/16/2022
Last modified:  05/04/2023
Purpose:        Add social determinants of health (SDoH) variables (only those agreed upon) to the analytic dataset

Inputs:         'ch_prism', 'COVID_PATIENT_SDOH'
Outputs:         'ch_sdoh'

Revisions:      05/04/2023 - Addition of RUCA codes in another left join
==============================================================================*/

SELECT a.*, b.poverty_status, b.MDs,100-(b.education_hs_diploma+b.education_some_college+b.education_associate_degree+b.education_bachelors_degree+b.education_graduate_or_professional_degree) as education_lt_hs_diploma,b.education_hs_diploma,b.education_some_college,b.education_associate_degree,b.education_bachelors_degree,b.education_graduate_or_professional_degree,(education_hs_diploma+b.education_some_college+b.education_associate_degree+b.education_bachelors_degree+b.education_graduate_or_professional_degree) as education_HSplus,(b.education_bachelors_degree+b.education_graduate_or_professional_degree) as education_BAplus,c.RUCA1,c.RUCA2,
    case    when c.RUCA1 = 1 then 'Metropolitan area core: primary flow within an urbanized area (UA)'
            when c.RUCA1 = 2 then 'Metropolitan area high commuting: primary flow 30% or more to a UA'
            when c.RUCA1 = 3 then  'Metropolitan area low commuting: primary flow 10% to 30% to a UA'
            when c.RUCA1 = 4 then 'Micropolitan area core: primary flow within an Urban Cluster of 10,000 to 49,999 (large UC)'
            when c.RUCA1 = 5 then  'Micropolitan high commuting: primary flow 30% or more to a large UC'
            when c.RUCA1 = 6 then  'Micropolitan low commuting: primary flow 10% to 30% to a large UC'
            when c.RUCA1 = 7 then  'Small town core: primary flow within an Urban Cluster of 2,500 to 9,999 (small UC)'
            when c.RUCA1 = 8 then  'Small town high commuting: primary flow 30% or more to a small UC'
            when c.RUCA1 = 9 then  'Rural areas: primary flow to a tract outside a UA or UC'
            when c.RUCA1 = 10 then  'Rural areas: primary flow to a tract outside a UA or UC'
            when c.RUCA1 = 99 then  'Not coded: Census tract has zero population and no rural-urban identifier information' END AS RUCA1_label,
    CASE    WHEN round(c.RUCA2,1) in (1.0,2.0,3.0,1.1,2.1,4.1,5.1,7.1,8.1,10.1) then 'Urban'
            WHEN round(c.RUCA2,1) in (4.0,5.0,6.0) then 'Suburban'
            WHEN round(c.RUCA2,1) in (7.0,8.0,9.0,10.0,7.2,8.2,10.2,10.3) then 'Rural' end as RUCA2_label
FROM ch_pm25_2020 a 
LEFT JOIN COVID_PATIENT_SDOH b on a.person_id=b.person_id
LEFT JOIN RUCA2020zipcode c on a.zip_code=c.ZIP_CODE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9ca7d8b2-0c66-447f-a51f-3488186dffd9"),
    ch_sdoh=Input(rid="ri.foundry.main.dataset.984c4847-1626-4a6f-8247-abd08729ac9c"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f")
)
SELECT a.*,b.shift_date_yn as corr_shift_date_yn
FROM ch_sdoh a LEFT JOIN manifest b on a.data_partner_id=b.data_partner_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d294d017-a840-4842-946c-8be8183ebbaa"),
    zip_county_cbsa=Input(rid="ri.foundry.main.dataset.aa35f8bf-f1f6-435b-a017-f752a265acf6")
)
/*check that county to CBSA relationship is unique*/
SELECT distinct Preferred_county,CBSA_CODE
FROM zip_county_cbsa

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.623e88a2-4e4c-4eb2-be46-6a3804db7bda"),
    county_daily_status=Input(rid="ri.foundry.main.dataset.99814779-5924-4e29-8df1-5d80b4da5cf7")
)
SELECT
   region_id,mapbox_geoid,state_name,state_abbr,date,local_code,covid19_total_cases,
   covid19_new_cases,
   AVG(covid19_new_cases)
         OVER(PARTITION BY local_code ORDER BY local_code, date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS moving_average
FROM county_daily_status

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8ed896c0-552f-4653-8211-914ff5be968e"),
    BU_SC_SDOH_N3C_2018_20201130=Input(rid="ri.foundry.main.dataset.f9fb2781-bed3-421e-bb57-6eaa24ddd85d"),
    county_daily_status=Input(rid="ri.foundry.main.dataset.99814779-5924-4e29-8df1-5d80b4da5cf7")
)
SELECT *
FROM county_daily_status

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6a135c5a-44cf-4791-ae57-1268dd61428a"),
    county_daily_status=Input(rid="ri.foundry.main.dataset.99814779-5924-4e29-8df1-5d80b4da5cf7"),
    date_first_case=Input(rid="ri.foundry.main.dataset.df27149e-2b90-4558-8ca8-26306cad299a")
)
SELECT date_first_case.*,unique_zip_county.ZIP 
FROM date_first_case LEFT JOIN unique_zip_county
ON date_first_case.local_code = unique_zip_county.Preferred_county

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f78bd4ee-a61d-4eb3-b482-e54677dd047d"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318")
)
SELECT location.*,unique_zip_county.Preferred_county as FIPS_county
FROM location LEFT JOIN unique_zip_county on location.zip = unique_zip_county.ZIP

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bd566ee8-feea-40df-9726-71c0d36daeb7"),
    payer_plan_dummies=Input(rid="ri.foundry.main.dataset.9e2376dc-466e-4d05-bb96-30f8b7355f58")
)
/*
Project: N3C Case Crossover
Author: Alyssa Platt
Created: 9/16/2022
Updated: 9/16/2022
Purpose: Insurance plans are not mutually exclusive (i.e. there can be multiple insurance plans assigned to a partcular date), therefore create dummies to indicate that the patient ever had a particular insurance type on that date

Inputs: 'payer_plan_dummies'
Outputs: 'payer_plan_collapse'
*/    
    select distinct person_id, macrovisit_start_date,
    max(payer_null) as payer_null,
    max(payer_NMC) as payer_NMC,
    max(payer_medicare) as payer_medicare,
    max(payer_private) as payer_private,
    max(payer_medicaid) as payer_medicaid,
    max(payer_none_listed) as payer_none_listed,
    max(payer_gov_other) as payer_gov_other,
    max(payer_dod) as payer_dod,
    max(payer_misc) as payer_misc,
    max(payer_doc) as payer_doc
    from payer_plan_dummies
    group by person_id,macrovisit_start_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9e2376dc-466e-4d05-bb96-30f8b7355f58"),
    ch_hosp_facts=Input(rid="ri.foundry.main.dataset.621b33c5-d410-4143-9b60-c7f9b9924da8"),
    payer_plan_period=Input(rid="ri.foundry.main.dataset.2210599f-758d-467e-82ae-9c350422b2ab")
)
/*
Project: N3C Case Crossover
Author: Alyssa Platt
Created: 9/16/2022
Updated: 9/16/2022
Purpose: Create dummies for each type of insurance to link more accurated with hospitalizations (multiple insurances may be listed for the same period of time, e.g. Medicare and Medicaid) such that a merge does not produce multiple records per hospitalizations

Inputs: 'payer_plan_period'
Outputs: 'payer_plan_dummies'
*/
SELECT a.person_id,a.macrovisit_start_date,
b.payer_concept_name=null as payer_null,
b.payer_concept_name='No matching concept' as payer_NMC,
b.payer_concept_name='Medicare' as payer_medicare,
b.payer_concept_name='Private Health Insurance' as payer_private,
b.payer_concept_name='Medicaid' as payer_medicaid,
b.payer_concept_name='No Payment from an Organization/Agency/Program/Private Payer Listed' as payer_none_listed,
b.payer_concept_name='Other Government Federal/State/Local, excluding Department of Corrections' as payer_gov_other,
b.payer_concept_name='Department of Defense' as payer_dod,
b.payer_concept_name='Miscellaneous Program' as payer_misc,
b.payer_concept_name='Department of Corrections' as payer_doc
FROM ch_hosp_facts a LEFT JOIN payer_plan_period b 
  ON (a.person_id=b.person_id)
    AND (a.macrovisit_start_date >= b.payer_plan_period_start_date) AND (a.macrovisit_start_date<=b.payer_plan_period_end_date)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.da8670fe-c033-4df9-82ea-8d195260e1db"),
    ds_2021_EQI_Prism=Input(rid="ri.foundry.main.dataset.cfc9cd85-36f7-4a36-a8a4-8d3012717881"),
    prism_2019_2020=Input(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9")
)
SELECT *
FROM ds_2021_EQI_Prism
UNION
SELECT *
FROM prism_2019_2020

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.aa35f8bf-f1f6-435b-a017-f752a265acf6"),
    BU_SC_SDOH_N3C_2018_20201130=Input(rid="ri.foundry.main.dataset.f9fb2781-bed3-421e-bb57-6eaa24ddd85d"),
    ds_1_1_ZIP_COUNTY_HUD_file=Input(rid="ri.foundry.main.dataset.9ac4d85e-8d54-45b0-ab0c-da4180067c3e")
)
SELECT a.*,b.CBSA_CODE,b.CBSA_Name
FROM ds_1_1_ZIP_COUNTY_HUD_file a LEFT JOIN BU_SC_SDOH_N3C_2018_20201130 b on
a.Preferred_county = b.FIPS_CODE

