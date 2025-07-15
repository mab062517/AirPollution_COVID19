from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.986e7e7e-b533-48b0-aa61-92a77c10e8c5"),
    cohort_hosp_covid_flag=Input(rid="ri.foundry.main.dataset.4ed8c7f9-0371-4a0b-bc20-bf95f1983f48")
)
def COHORT_HOSPITALIZATIONS(cohort_hosp_covid_flag):

    df = cohort_hosp_covid_flag

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.67e1fd1f-2a94-4e20-9523-88717a231e66"),
    cohort_non_hosp_visits=Input(rid="ri.foundry.main.dataset.2b4facce-75bf-491a-a9e9-e637e3137b73")
)
def COHORT_VISITS(cohort_non_hosp_visits):
    
    df = cohort_non_hosp_visits

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.96ec25c2-64f3-4301-a06e-4d191281fa21"),
    PATIENT_COVID_POS_DATES=Input(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-22

Description:
Subset microvisit_to_macrovisit_lds to contain only cohort visits
Note: There appears to be some cohort patients not in the 
      microvisit_to_macrovisit_lds table

Input:
1.  microvisit_to_macrovisit_lds:
    Visits table plus indicators of hospitalizations
================================================================================
"""
def cohort_all_visits(microvisit_to_macrovisit_lds, PATIENT_COVID_POS_DATES):

    distinct_patients_df = PATIENT_COVID_POS_DATES.select('person_id').dropDuplicates()
    
    visits_df = microvisit_to_macrovisit_lds.join(distinct_patients_df, 'person_id', 'inner')

    return visits_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.58fc6419-f17a-4eb6-9a43-c45fe734c677"),
    cohort_all_visits=Input(rid="ri.foundry.main.dataset.96ec25c2-64f3-4301-a06e-4d191281fa21")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-22

Description:
Subset Cohort Visits to Cohort Hospitalizations (macrovisit_id != null)

Input:
1.  cohort_all_visits:
    Visits with our patients only
================================================================================
"""
def cohort_hosp(cohort_all_visits):

    cohort_hosp_df = (
        cohort_all_visits
        .select('person_id','data_partner_id', 'macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
    ).dropDuplicates()

    return cohort_hosp_df 
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ed8c7f9-0371-4a0b-bc20-bf95f1983f48"),
    PATIENT_COVID_POS_DATES=Input(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce"),
    cohort_hosp=Input(rid="ri.foundry.main.dataset.58fc6419-f17a-4eb6-9a43-c45fe734c677")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-22

Description:
All patient hospitalizations with covid-associated hospitalizations indicated. 

Input:
1.  cohort_hosp:
    All patient hospitalizations
================================================================================
"""
def cohort_hosp_covid_flag(cohort_hosp, PATIENT_COVID_POS_DATES):
    '''
    Hospitalization start date  (macrovisit_start_date) must start:
        days_before_visit_start_date positive poslab_date
        and    
        days_after_visit_start_date positive poslab_date
    example:
    If poslab_date        = June 10
    macrovisit_start_date = [June 9, June 26]    
    ''' 

    days_after_visit_start_date     = 1
    days_before_visit_start_date    = 16

    # only need these columns
    p1_poslab = (
        PATIENT_COVID_POS_DATES
        .select('person_id', F.col('covid_date').alias('poslab_date'))  
    )
    
    # COVID-associated hospitalizations
    all_poslab_hosp_df = (
        cohort_hosp
        .join(p1_poslab, 'person_id', 'left')
        .where( (F.col('macrovisit_start_date') >= F.col('poslab_date') - days_after_visit_start_date   ) & 
                (F.col('macrovisit_start_date') <= F.col('poslab_date') + days_before_visit_start_date  ) &
                (F.col('poslab_date')           <= F.col('macrovisit_end_date')                         ) )
    )
    # Order by macrovisit_id, poslab_date
    w = Window.partitionBy('macrovisit_id').orderBy('poslab_date')
    # Keep only the first poslab_date for each hospitalization period.
    # Drop duplicates to get rid of instances when more than one  
    # positive lab results occur on the first poslab_date. 
    first_poslab_per_hosp_df = (
        all_poslab_hosp_df
        .withColumn('poslab_date', F.first('poslab_date').over(w))         
        .select('macrovisit_id', 'poslab_date')
        .dropDuplicates()
    )
    
    # All hospitalizations (including COVID-associated)
    # Creates flags for hospitalization visits and covid-associated hospitalizations
    all_hosp_df = (
        cohort_hosp
        .join(first_poslab_per_hosp_df, 'macrovisit_id', 'left')
        .withColumn('covid_associated_hosp' , F.when(F.col('poslab_date').isNotNull(), F.lit(1) ).otherwise(0) )
        .select('person_id'             , 
                'macrovisit_id'         , 
                'covid_associated_hosp' , 
                'poslab_date'           , 
                'macrovisit_start_date' , 
                'macrovisit_end_date'   , 
                'data_partner_id'
        )
    )

    return all_hosp_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2b4facce-75bf-491a-a9e9-e637e3137b73"),
    cohort_all_visits=Input(rid="ri.foundry.main.dataset.96ec25c2-64f3-4301-a06e-4d191281fa21")
)
'''
Keeps only cohort non-hospitalization visits 
'''
def cohort_non_hosp_visits(cohort_all_visits):

    df = (
        cohort_all_visits
        .filter(F.col('macrovisit_id').isNull())
        .drop('macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
    )
    
    return df
    

