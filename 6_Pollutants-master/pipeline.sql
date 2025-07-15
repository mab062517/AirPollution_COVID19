

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dcb57862-a033-4a44-a41f-56eafdabc02f"),
    ZCTA_Monitor_Pairs_Within_or_Within50km=Input(rid="ri.foundry.main.dataset.18a66ac9-5a53-48cd-b695-b48fc706fd0a"),
    pm25_aggregations=Input(rid="ri.foundry.main.dataset.6504028c-c02d-4696-8d6f-0c9104f4798f")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Reduce dimensionality such that relationships are only kept when distance is <=20km
*/
SELECT *, Distance_m/1000 as Distance_km,
CASE WHEN (Distance_m/1000)<=20 THEN 1 ELSE 0 END AS within_20km
FROM ZCTA_Monitor_Pairs_Within_or_Within50km
WHERE (Distance_m/1000)<=20

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a8e0b897-e347-4db9-9a38-8673fa75adf0"),
    monitors_20km=Input(rid="ri.foundry.main.dataset.dcb57862-a033-4a44-a41f-56eafdabc02f"),
    no2_aggregations=Input(rid="ri.foundry.main.dataset.07ddc2e9-d6e9-4993-88a1-aa08762ab698")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Link N02 measurements with monitor distance data in preparation for aggregation by 20km Distance
    Inputs: 'monitors_20km', 'no2_aggregations'
    Outputs: 'no2_distance'
*/
SELECT monitors_20km.*, no2_aggregations.*
FROM monitors_20km RIGHT JOIN no2_aggregations on monitors_20km.Monitor_ID=no2_aggregations.aqs_site_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e918f691-9c9e-4d73-b2a3-3b6d138f3632"),
    no2_distance=Input(rid="ri.foundry.main.dataset.a8e0b897-e347-4db9-9a38-8673fa75adf0")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Collapse data to level of ZCTA/day averaging N02 values for all monitors within 20km
*/
SELECT distinct ZCTA, date, mean(measurement) as measurement_avg, count(Monitor_ID) as num_monitors
FROM no2_distance
group by ZCTA, date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b85ac1d3-4eb1-4e43-a9df-38b7ebdd97db"),
    no2_distance_agg=Input(rid="ri.foundry.main.dataset.e918f691-9c9e-4d73-b2a3-3b6d138f3632")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/24/2022
Last modified:  10/24/2022
Purpose:        Embed lag variables in original pollution files so I can more easily
                create analytic data with controls. Add day and month variables as
                well for linking to controls
                

Inputs:         'no2_distance_agg'
Outputs:         no2_distance_agg_wlag
==============================================================================*/

SELECT no2_distance_agg.*, 
weekday(no2_distance_agg.date) as weekday,
month(no2_distance_agg.date) as month,
year(no2_distance_agg.date) as year,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS no2_7d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS no2_7d_valid,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
         AS no2_21d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
         AS no2_21d_valid,
    l0.measurement_avg as no2,
    l1.measurement_avg as l1_no2,
    l2.measurement_avg as l2_no2,
    l3.measurement_avg as l3_no2,
    l4.measurement_avg as l4_no2,
    l5.measurement_avg as l5_no2,
    l6.measurement_avg as l6_no2,
    l7.measurement_avg as l7_no2,
    l8.measurement_avg as l8_no2,
    l9.measurement_avg as l9_no2,
    l10.measurement_avg as l10_no2,
    l11.measurement_avg as l11_no2,
    l12.measurement_avg as l12_no2,
    l13.measurement_avg as l13_no2,
    l14.measurement_avg as l14_no2,
    l15.measurement_avg as l15_no2,
    l16.measurement_avg as l16_no2,
    l17.measurement_avg as l17_no2,
    l18.measurement_avg as l18_no2,
    l19.measurement_avg as l19_no2,
    l20.measurement_avg as l20_no2,
    l21.measurement_avg as l21_no2
FROM no2_distance_agg 
LEFT JOIN no2_distance_agg l0 on no2_distance_agg.date = l0.date AND no2_distance_agg.ZCTA=l0.ZCTA
LEFT JOIN no2_distance_agg l1 on DATE_SUB(no2_distance_agg.date,1) = l1.date AND no2_distance_agg.ZCTA=l1.ZCTA
LEFT JOIN no2_distance_agg l2 on DATE_SUB(no2_distance_agg.date,2) = l2.date AND no2_distance_agg.ZCTA=l2.ZCTA
LEFT JOIN no2_distance_agg l3 on DATE_SUB(no2_distance_agg.date,3) = l3.date AND no2_distance_agg.ZCTA=l3.ZCTA
LEFT JOIN no2_distance_agg l4 on DATE_SUB(no2_distance_agg.date,4) = l4.date AND no2_distance_agg.ZCTA=l4.ZCTA
LEFT JOIN no2_distance_agg l5 on DATE_SUB(no2_distance_agg.date,5) = l5.date AND no2_distance_agg.ZCTA=l5.ZCTA
LEFT JOIN no2_distance_agg l6 on DATE_SUB(no2_distance_agg.date,6) = l6.date AND no2_distance_agg.ZCTA=l6.ZCTA
LEFT JOIN no2_distance_agg l7 on DATE_SUB(no2_distance_agg.date,7) = l7.date AND no2_distance_agg.ZCTA=l7.ZCTA
LEFT JOIN no2_distance_agg l8 on DATE_SUB(no2_distance_agg.date,8) = l8.date AND no2_distance_agg.ZCTA=l8.ZCTA
LEFT JOIN no2_distance_agg l9 on DATE_SUB(no2_distance_agg.date,9) = l9.date AND no2_distance_agg.ZCTA=l9.ZCTA
LEFT JOIN no2_distance_agg l10 on DATE_SUB(no2_distance_agg.date,10) = l10.date AND no2_distance_agg.ZCTA=l10.ZCTA
LEFT JOIN no2_distance_agg l11 on DATE_SUB(no2_distance_agg.date,11) = l11.date AND no2_distance_agg.ZCTA=l11.ZCTA
LEFT JOIN no2_distance_agg l12 on DATE_SUB(no2_distance_agg.date,12) = l12.date AND no2_distance_agg.ZCTA=l12.ZCTA
LEFT JOIN no2_distance_agg l13 on DATE_SUB(no2_distance_agg.date,13) = l13.date AND no2_distance_agg.ZCTA=l13.ZCTA
LEFT JOIN no2_distance_agg l14 on DATE_SUB(no2_distance_agg.date,14) = l14.date AND no2_distance_agg.ZCTA=l14.ZCTA
LEFT JOIN no2_distance_agg l15 on DATE_SUB(no2_distance_agg.date,15) = l15.date AND no2_distance_agg.ZCTA=l15.ZCTA
LEFT JOIN no2_distance_agg l16 on DATE_SUB(no2_distance_agg.date,16) = l16.date AND no2_distance_agg.ZCTA=l16.ZCTA
LEFT JOIN no2_distance_agg l17 on DATE_SUB(no2_distance_agg.date,17) = l17.date AND no2_distance_agg.ZCTA=l17.ZCTA
LEFT JOIN no2_distance_agg l18 on DATE_SUB(no2_distance_agg.date,18) = l18.date AND no2_distance_agg.ZCTA=l18.ZCTA
LEFT JOIN no2_distance_agg l19 on DATE_SUB(no2_distance_agg.date,19) = l19.date AND no2_distance_agg.ZCTA=l19.ZCTA
LEFT JOIN no2_distance_agg l20 on DATE_SUB(no2_distance_agg.date,20) = l20.date AND no2_distance_agg.ZCTA=l20.ZCTA
LEFT JOIN no2_distance_agg l21 on DATE_SUB(no2_distance_agg.date,21) = l21.date AND no2_distance_agg.ZCTA=l21.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.eabf4871-be0f-4d22-a909-b12970ebc572"),
    monitors_20km=Input(rid="ri.foundry.main.dataset.dcb57862-a033-4a44-a41f-56eafdabc02f"),
    ozone_aggregations=Input(rid="ri.foundry.main.dataset.d5a62ea1-186b-421d-a73b-0fb0157287dc")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Link ozone measurements with monitor distance data in preparation for aggregation by 20km Distance
    Inputs: 'monitors_20km', 'ozone_aggregations'
    Outputs: 'ozone_distance'
*/
SELECT monitors_20km.*, ozone_aggregations.*
FROM monitors_20km RIGHT JOIN ozone_aggregations on monitors_20km.Monitor_ID=ozone_aggregations.aqs_site_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4d558095-990c-4bfc-a9ae-2dc839194ff1"),
    ozone_distance=Input(rid="ri.foundry.main.dataset.eabf4871-be0f-4d22-a909-b12970ebc572")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Collapse data to level of ZCTA/day averaging ozone values for all monitors within 20km
*/
SELECT distinct ZCTA, date, mean(measurement) as measurement_avg, count(Monitor_ID) as num_monitors
FROM ozone_distance
group by ZCTA, date
HAVING ZCTA IS NOT NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.edf6b2e6-d267-45d2-8562-b563ac1db393"),
    ozone_distance_agg=Input(rid="ri.foundry.main.dataset.4d558095-990c-4bfc-a9ae-2dc839194ff1")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/24/2022
Last modified:  10/24/2022
Purpose:        Embed lag variables in original pollution files so I can more easily
                create analytic data with controls. Add day and month variables as
                well for linking to controls
                

Inputs:         'ozone_distance_agg'
Outputs:         ozone_distance_agg_wlag
==============================================================================*/

SELECT ozone_distance_agg.*, 
weekday(ozone_distance_agg.date) as weekday,
month(ozone_distance_agg.date) as month,
year(ozone_distance_agg.date) as year,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS ozone_7d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS ozone_7d_valid,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
         AS ozone_21d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
         AS ozone_21d_valid,
    l0.measurement_avg as ozone,
    l1.measurement_avg as l1_ozone,
    l2.measurement_avg as l2_ozone,
    l3.measurement_avg as l3_ozone,
    l4.measurement_avg as l4_ozone,
    l5.measurement_avg as l5_ozone,
    l6.measurement_avg as l6_ozone,
    l7.measurement_avg as l7_ozone,
    l8.measurement_avg as l8_ozone,
    l9.measurement_avg as l9_ozone,
    l10.measurement_avg as l10_ozone,
    l11.measurement_avg as l11_ozone,
    l12.measurement_avg as l12_ozone,
    l13.measurement_avg as l13_ozone,
    l14.measurement_avg as l14_ozone,
    l15.measurement_avg as l15_ozone,
    l16.measurement_avg as l16_ozone,
    l17.measurement_avg as l17_ozone,
    l18.measurement_avg as l18_ozone,
    l19.measurement_avg as l19_ozone,
    l20.measurement_avg as l20_ozone,
    l21.measurement_avg as l21_ozone
FROM ozone_distance_agg 
LEFT JOIN ozone_distance_agg l0 on ozone_distance_agg.date = l0.date AND ozone_distance_agg.ZCTA=l0.ZCTA
LEFT JOIN ozone_distance_agg l1 on DATE_SUB(ozone_distance_agg.date,1) = l1.date AND ozone_distance_agg.ZCTA=l1.ZCTA
LEFT JOIN ozone_distance_agg l2 on DATE_SUB(ozone_distance_agg.date,2) = l2.date AND ozone_distance_agg.ZCTA=l2.ZCTA
LEFT JOIN ozone_distance_agg l3 on DATE_SUB(ozone_distance_agg.date,3) = l3.date AND ozone_distance_agg.ZCTA=l3.ZCTA
LEFT JOIN ozone_distance_agg l4 on DATE_SUB(ozone_distance_agg.date,4) = l4.date AND ozone_distance_agg.ZCTA=l4.ZCTA
LEFT JOIN ozone_distance_agg l5 on DATE_SUB(ozone_distance_agg.date,5) = l5.date AND ozone_distance_agg.ZCTA=l5.ZCTA
LEFT JOIN ozone_distance_agg l6 on DATE_SUB(ozone_distance_agg.date,6) = l6.date AND ozone_distance_agg.ZCTA=l6.ZCTA
LEFT JOIN ozone_distance_agg l7 on DATE_SUB(ozone_distance_agg.date,7) = l7.date AND ozone_distance_agg.ZCTA=l7.ZCTA
LEFT JOIN ozone_distance_agg l8 on DATE_SUB(ozone_distance_agg.date,8) = l8.date AND ozone_distance_agg.ZCTA=l8.ZCTA
LEFT JOIN ozone_distance_agg l9 on DATE_SUB(ozone_distance_agg.date,9) = l9.date AND ozone_distance_agg.ZCTA=l9.ZCTA
LEFT JOIN ozone_distance_agg l10 on DATE_SUB(ozone_distance_agg.date,10) = l10.date AND ozone_distance_agg.ZCTA=l10.ZCTA
LEFT JOIN ozone_distance_agg l11 on DATE_SUB(ozone_distance_agg.date,11) = l11.date AND ozone_distance_agg.ZCTA=l11.ZCTA
LEFT JOIN ozone_distance_agg l12 on DATE_SUB(ozone_distance_agg.date,12) = l12.date AND ozone_distance_agg.ZCTA=l12.ZCTA
LEFT JOIN ozone_distance_agg l13 on DATE_SUB(ozone_distance_agg.date,13) = l13.date AND ozone_distance_agg.ZCTA=l13.ZCTA
LEFT JOIN ozone_distance_agg l14 on DATE_SUB(ozone_distance_agg.date,14) = l14.date AND ozone_distance_agg.ZCTA=l14.ZCTA
LEFT JOIN ozone_distance_agg l15 on DATE_SUB(ozone_distance_agg.date,15) = l15.date AND ozone_distance_agg.ZCTA=l15.ZCTA
LEFT JOIN ozone_distance_agg l16 on DATE_SUB(ozone_distance_agg.date,16) = l16.date AND ozone_distance_agg.ZCTA=l16.ZCTA
LEFT JOIN ozone_distance_agg l17 on DATE_SUB(ozone_distance_agg.date,17) = l17.date AND ozone_distance_agg.ZCTA=l17.ZCTA
LEFT JOIN ozone_distance_agg l18 on DATE_SUB(ozone_distance_agg.date,18) = l18.date AND ozone_distance_agg.ZCTA=l18.ZCTA
LEFT JOIN ozone_distance_agg l19 on DATE_SUB(ozone_distance_agg.date,19) = l19.date AND ozone_distance_agg.ZCTA=l19.ZCTA
LEFT JOIN ozone_distance_agg l20 on DATE_SUB(ozone_distance_agg.date,20) = l20.date AND ozone_distance_agg.ZCTA=l20.ZCTA
LEFT JOIN ozone_distance_agg l21 on DATE_SUB(ozone_distance_agg.date,21) = l21.date AND ozone_distance_agg.ZCTA=l21.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.47ed14ec-083c-4727-a244-4f14de5462e2"),
    pm25_zcta_cbsa=Input(rid="ri.foundry.main.dataset.40d2b521-a3d0-4935-ba45-52704e7e0788")
)
SELECT date, CBSA_CODE, CBSA_Name,
MEAN(measurement_avg) as pm25_cbsa_avg,year(date) as year,
SUM(CASE WHEN measurement_avg is not null then 1 else 0 END) as  pm25_cbsa_avg_valid 
FROM pm25_zcta_cbsa
where CBSA_CODE is not null
group by CBSA_CODE, CBSA_Name, date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e5772aa3-41ca-4db6-9365-8c4159b57aff"),
    pm25_agg_cbsa=Input(rid="ri.foundry.main.dataset.47ed14ec-083c-4727-a244-4f14de5462e2")
)
SELECT distinct CBSA_CODE, CBSA_Name, year,
MEAN(pm25_cbsa_avg) as pm25_cbsa_avg2020,
SUM(CASE WHEN pm25_cbsa_avg is not null then 1 ELSE 0 END) as valid_obs
FROM pm25_agg_cbsa
GROUP BY CBSA_CODE, CBSA_Name,  year

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.99e302f2-3848-43ba-8c84-eac1570342fd"),
    pm25_distance_agg_wlag=Input(rid="ri.foundry.main.dataset.37a67b1f-6f0f-4c08-bff2-1f4e4c8f67ad")
)
SELECT ZCTA, mean(measurement_avg) as mean_pm25_2020, 
    case    when mean(measurement_avg)<=12 then 0
            when mean(measurement_avg)>12 then 1
            else null
            end as pm25_2020_gt12,
    count(measurement_avg) as valid_obs
    
FROM pm25_distance_agg_wlag
WHERE year=2020
group by ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.18d88a94-5c20-455b-ad2b-9f0c8c7babd1"),
    pm25_avg_2020=Input(rid="ri.foundry.main.dataset.99e302f2-3848-43ba-8c84-eac1570342fd")
)
SELECT *,
NTILE(4) OVER(ORDER BY mean_pm25_2020) as pm25_avg_tert_2020,
NTILE(4) OVER(ORDER BY valid_obs) as valid_obs_tert_2020
FROM pm25_avg_2020

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d63b100e-b12d-4411-8b41-5c0285330a6f"),
    monitors_20km=Input(rid="ri.foundry.main.dataset.dcb57862-a033-4a44-a41f-56eafdabc02f"),
    pm25_aggregations=Input(rid="ri.foundry.main.dataset.6504028c-c02d-4696-8d6f-0c9104f4798f")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Link PM2.5 measurements with monitor distance data in preparation for aggregation by 20km Distance
    Inputs: 'monitors_20km', 'pm25_aggregations'
    Outputs: 'pm25_distance'
*/
SELECT monitors_20km.*, pm25_aggregations.*
FROM monitors_20km RIGHT JOIN pm25_aggregations on monitors_20km.Monitor_ID=pm25_aggregations.aqs_site_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3d739e51-4f74-4c50-89e6-3b347dc3fdd5"),
    pm25_distance=Input(rid="ri.foundry.main.dataset.d63b100e-b12d-4411-8b41-5c0285330a6f")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/15/2022
    Purpose: Collapse data to level of ZCTA/day averaging PM2.5 values for all monitors within 20km
*/
SELECT distinct ZCTA, date,mean(measurement_avg) as measurement_avg, count(Monitor_ID) as num_monitors
FROM pm25_distance
group by ZCTA, date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.37a67b1f-6f0f-4c08-bff2-1f4e4c8f67ad"),
    pm25_distance_agg=Input(rid="ri.foundry.main.dataset.3d739e51-4f74-4c50-89e6-3b347dc3fdd5")
)
/*==========================================================================
Author:         Alyssa Platt (alyssa.platt@duke.edu)
Project:        N3C Case-Crossover study
Created:        10/24/2022
Last modified:  10/24/2022
Purpose:        Embed lag variables in original pollution files so I can more easily
                create analytic data with controls. Add day and month variables as
                well for linking to controls
                

Inputs:         'pm25_distance_agg'
Outputs:         pm25_distance_agg_wlag
==============================================================================*/

SELECT pm25_distance_agg.*,
weekday(pm25_distance_agg.date) as weekday,
month(pm25_distance_agg.date) as month,
year(pm25_distance_agg.date) as year,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) 
         AS pm25_2d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) 
         AS pm25_2d_valid,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) 
         AS pm25_5d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) 
         AS pm25_5d_valid,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS pm25_7d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) 
         AS pm25_7d_valid,
   AVG(l0.measurement_avg)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
         AS pm25_21d,
   SUM(CASE WHEN l0.measurement_avg IS NOT NULL then 1 else 0 END)
         OVER(PARTITION BY l0.ZCTA ORDER BY l0.ZCTA, l0.date  ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) 
         AS pm25_21d_valid,
    l0.measurement_avg as pm25,
    l1.measurement_avg as l1_pm25,
    l2.measurement_avg as l2_pm25,
    l3.measurement_avg as l3_pm25,
    l4.measurement_avg as l4_pm25,
    l5.measurement_avg as l5_pm25,
    l6.measurement_avg as l6_pm25,
    l7.measurement_avg as l7_pm25,
    l8.measurement_avg as l8_pm25,
    l9.measurement_avg as l9_pm25,
    l10.measurement_avg as l10_pm25,
    l11.measurement_avg as l11_pm25,
    l12.measurement_avg as l12_pm25,
    l13.measurement_avg as l13_pm25,
    l14.measurement_avg as l14_pm25,
    l15.measurement_avg as l15_pm25,
    l16.measurement_avg as l16_pm25,
    l17.measurement_avg as l17_pm25,
    l18.measurement_avg as l18_pm25,
    l19.measurement_avg as l19_pm25,
    l20.measurement_avg as l20_pm25,
    l21.measurement_avg as l21_pm25
FROM pm25_distance_agg 
LEFT JOIN pm25_distance_agg l0 on pm25_distance_agg.date = l0.date AND pm25_distance_agg.ZCTA=l0.ZCTA
LEFT JOIN pm25_distance_agg l1 on DATE_SUB(pm25_distance_agg.date,1) = l1.date AND pm25_distance_agg.ZCTA=l1.ZCTA
LEFT JOIN pm25_distance_agg l2 on DATE_SUB(pm25_distance_agg.date,2) = l2.date AND pm25_distance_agg.ZCTA=l2.ZCTA
LEFT JOIN pm25_distance_agg l3 on DATE_SUB(pm25_distance_agg.date,3) = l3.date AND pm25_distance_agg.ZCTA=l3.ZCTA
LEFT JOIN pm25_distance_agg l4 on DATE_SUB(pm25_distance_agg.date,4) = l4.date AND pm25_distance_agg.ZCTA=l4.ZCTA
LEFT JOIN pm25_distance_agg l5 on DATE_SUB(pm25_distance_agg.date,5) = l5.date AND pm25_distance_agg.ZCTA=l5.ZCTA
LEFT JOIN pm25_distance_agg l6 on DATE_SUB(pm25_distance_agg.date,6) = l6.date AND pm25_distance_agg.ZCTA=l6.ZCTA
LEFT JOIN pm25_distance_agg l7 on DATE_SUB(pm25_distance_agg.date,7) = l7.date AND pm25_distance_agg.ZCTA=l7.ZCTA
LEFT JOIN pm25_distance_agg l8 on DATE_SUB(pm25_distance_agg.date,8) = l8.date AND pm25_distance_agg.ZCTA=l8.ZCTA
LEFT JOIN pm25_distance_agg l9 on DATE_SUB(pm25_distance_agg.date,9) = l9.date AND pm25_distance_agg.ZCTA=l9.ZCTA
LEFT JOIN pm25_distance_agg l10 on DATE_SUB(pm25_distance_agg.date,10) = l10.date AND pm25_distance_agg.ZCTA=l10.ZCTA
LEFT JOIN pm25_distance_agg l11 on DATE_SUB(pm25_distance_agg.date,11) = l11.date AND pm25_distance_agg.ZCTA=l11.ZCTA
LEFT JOIN pm25_distance_agg l12 on DATE_SUB(pm25_distance_agg.date,12) = l12.date AND pm25_distance_agg.ZCTA=l12.ZCTA
LEFT JOIN pm25_distance_agg l13 on DATE_SUB(pm25_distance_agg.date,13) = l13.date AND pm25_distance_agg.ZCTA=l13.ZCTA
LEFT JOIN pm25_distance_agg l14 on DATE_SUB(pm25_distance_agg.date,14) = l14.date AND pm25_distance_agg.ZCTA=l14.ZCTA
LEFT JOIN pm25_distance_agg l15 on DATE_SUB(pm25_distance_agg.date,15) = l15.date AND pm25_distance_agg.ZCTA=l15.ZCTA
LEFT JOIN pm25_distance_agg l16 on DATE_SUB(pm25_distance_agg.date,16) = l16.date AND pm25_distance_agg.ZCTA=l16.ZCTA
LEFT JOIN pm25_distance_agg l17 on DATE_SUB(pm25_distance_agg.date,17) = l17.date AND pm25_distance_agg.ZCTA=l17.ZCTA
LEFT JOIN pm25_distance_agg l18 on DATE_SUB(pm25_distance_agg.date,18) = l18.date AND pm25_distance_agg.ZCTA=l18.ZCTA
LEFT JOIN pm25_distance_agg l19 on DATE_SUB(pm25_distance_agg.date,19) = l19.date AND pm25_distance_agg.ZCTA=l19.ZCTA
LEFT JOIN pm25_distance_agg l20 on DATE_SUB(pm25_distance_agg.date,20) = l20.date AND pm25_distance_agg.ZCTA=l20.ZCTA
LEFT JOIN pm25_distance_agg l21 on DATE_SUB(pm25_distance_agg.date,21) = l21.date AND pm25_distance_agg.ZCTA=l21.ZCTA
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.40d2b521-a3d0-4935-ba45-52704e7e0788"),
    pm25_distance_agg_wlag=Input(rid="ri.foundry.main.dataset.37a67b1f-6f0f-4c08-bff2-1f4e4c8f67ad"),
    zip_county_cbsa=Input(rid="ri.foundry.main.dataset.ed2f0e2a-399c-418d-aa81-2feb585fa78e")
)
SELECT a.*,b.CBSA_CODE,b.CBSA_Name
FROM pm25_distance_agg_wlag a LEFT JOIN zip_county_cbsa b on a.ZCTA=b.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fdafa11a-3f4e-4cf7-b00a-854ce297d2dd"),
    ZCTA_Monitor_Pairs_Within_or_Within20km=Input(rid="ri.foundry.main.dataset.198610c4-b080-4c4c-b315-416055315094")
)
SELECT *, Distance_m/1000 as distance_km, Distance_m/1609 as distance_miles
FROM ZCTA_Monitor_Pairs_Within_or_Within20km

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dd425f43-683e-4c10-9f8c-f4640358e195"),
    ZCTA_Monitor_Pairs_Within_or_Within20km=Input(rid="ri.foundry.main.dataset.198610c4-b080-4c4c-b315-416055315094"),
    zcta_monitor_gt20km=Input(rid="ri.foundry.main.dataset.440e8902-4f1c-4731-8905-a7cf2b3fa3b7")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/20/2022
    Purpose: Append distance files so I have all distances in one file. Use throughout.
*/
SELECT Row_Number,Monitor_ID,ZCTA,Distance_m,WithinZCTA from ZCTA_Monitor_Pairs_Within_or_Within20km
UNION
SELECT Row_Number,Monitor_ID,ZCTA,Distance_m, WithinZCTA from zcta_monitor_gt20km

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.440e8902-4f1c-4731-8905-a7cf2b3fa3b7"),
    ZCTA_Monitor_Pairs_NotWithin_and_Greaterthan20km=Input(rid="ri.foundry.main.dataset.183a21fd-78f1-4005-862b-39d66005a280")
)
/*  Project: N3C Case Crossover
    Author:  Alyssa Platt
    Date: 9/20/2022
    Purpose: Make >20km dataset consistent with <=20 and within dataset so they can be appended. Use combined dataset throughout to make it easier for me to see the effects of limiting distance on final cohort size
*/
SELECT *, 0 as WithinZCTA
FROM ZCTA_Monitor_Pairs_NotWithin_and_Greaterthan20km

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ed2f0e2a-399c-418d-aa81-2feb585fa78e"),
    BU_SC_SDOH_N3C_2018_20201130=Input(rid="ri.foundry.main.dataset.f9fb2781-bed3-421e-bb57-6eaa24ddd85d"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7"),
    ds_1_1_ZIP_COUNTY_HUD_file=Input(rid="ri.foundry.main.dataset.9ac4d85e-8d54-45b0-ab0c-da4180067c3e")
)
SELECT distinct a.ZIP, b.ZCTA, a.Preferred_county, c.CBSA_CODE,c.CBSA_Name
FROM ds_1_1_ZIP_COUNTY_HUD_file a LEFT JOIN ZiptoZcta_Crosswalk_2021_ziptozcta2020 b on a.ZIP=b.ZCTA
LEFT JOIN
BU_SC_SDOH_N3C_2018_20201130 c on a.Preferred_county=c.FIPS_CODE

