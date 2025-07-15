# 8_Cohort_Narrowing_and_Analysis
(Global Code includes R library functions: tableone, tidyverse, survival, dplyr)

**Purpose**: Implement eligibility criteria for the case-crossover analysis and produce descriptive and inferential output

**Source**: Entirely coded by Alyssa with very limited introduction of data from other sources (i.e. previous code workbooks or N3C data)

### Regression output:
  1. **ch_prelim_analysis**: This program runs the main, pooled inferential analysis 
  2. **cbsa_sub_short**: This program runs the primary stratified regression analysis with stratification by CBSA (requiring at least 50 cases in a CBSA in order to be included in analysis
  3. **cbsa_sub_sub_TEST_3m**: This program runs the regression analysis for the "sub-sub" analysis (data is subset into CBSA, then further subset into our subgroups of: age<65 vs 65+, with and without cardiovascular disease (CVD), with and without respiratory disease (RD), with and without metabolic disease (MD). This analysis requires that subgroups have at least 150 cases (this was the minimum that could produce stable analyses for all of the sub-sub groups)
  4. **other_sub_short**: This program runs the regression analysis on our subgroups of interest (without subsetting by CBSA first): age<65 vs 65+, with and without cardiovascular disease (CVD), with and without respiratory disease (RD), with and without metabolic disease (MD).
  5. **ruca_sub_short**: This program runs the regression analysis on categorized secondary RUCA codes, in categories of urban, suburban, and rural with definitions:
     - RUCA in (1.0,2.0,3.0,1.1,2.1,4.1,5.1,7.1,8.1,10.1) as 'Urban'
     - RUCA in (4.0,5.0,6.0) as 'Suburban'
     - RUCA in (7.0,8.0,9.0,10.0,7.2,8.2,10.2,10.3) as 'Rural'
  6. **census_sub_short**: This program runs the regression analysis subset by Census Region (Northeast, South, Midwest, and West). This was created for convenience to have estimates ready by Census Region for visualization purposes but it is important to understand that these estimates do not correspond exactly with what the meta-analysis might produce because:
Analysis doesn't require 50 cases per CBSA
CBSAs can be in multiple Census Regions and thus may show up multiple times in a meta-analysis but each patient encounter would show up only once in this analysis because assignment to Census Region is by ZIP code/ZCTA not CBSA

For items 1-6 a second version of these transforms (with the same name, and suffix _v2) runs the same analyses but uses natural splines for all adjustment covariates except for precipitation and saves AIC and BIC to compare model fit.

### Descriptive output:
  1. **cbsa_stats_obscure**: This output is a downloadable table with CBSA-level descriptive statistics that has gone through a process of data "obscuring" (note there are 2 main programs that feed into this: 'cbsa_summary_stats_final' and 'cbsa_summary_stats_final_sub'). There is a README file on Google Drive that will provide full interpretation of the output (//Environmental Health (n3c-dt-env-health@googlegroups.com)/Ongoing Analyses/Case-Crossover/Datasets/README.docx) that
  2. **CONSORT_Step2**: This output provides most of the numbers needed to construct the CONSORT diagram used for the manuscript (note: this includes data from 2021 and 2022 but is limited to 2020 after download from N3C). There is a supporting program 'CONSORT_Step1' that calculates all the summaries and saves in a wide datasets whereas this program makes the dataset long for readability and adds labels to the eligibility criteria. Variables can be interpreted as follows:
    - **order**: This is a numerical variable with intended sort order for eligibility criteria (in order of increasing stringency)
    - **NAME**: Eligibility criteria for inclusion in analysis
    - **Criteria**: Count of patient encounters that meet the criteria listed in 'NAME' as well as all criteria before it.
  3. **table1_output_allchars_obscure**: This program/dataset creates the final downloadable version of each stratified table included in analysis results (in total, 3 stratified tables appended together). There are two supporting programs for this final program: (1) a program that pre-cleans all of the data, (2) a program that runs the R CreateTableOne function and appends the 3 stratified tables together. The final program, containing the dataset simply removes rows with cell counts<20. In the final data, the variable "table" indicates which stratified table the output belongs to, which includes
    - **included_excluded**: This is includes all patient encounters (i.e. covid-related hospitalizations occurring in 2020) for adult patients (age 18+) and stratifies by whether the patient/encounter was included in the final analytic sample (i.e. the sample found in ch_prelim_analysis) versus those that were excluded (for whatever reason). Note that statistics in the column labeled '1' correspond to final output for the basic Table 1 presented in the manuscript
    - **ZCTA_noZCTA**: Starting with all patient encounters  (i.e. covid-related hospitalizations occurring in 2020) for adult patients (age 18+), this table stratifies by whether or not the patient/encounter had a corresponding valid ZCTA
    - pm_nopm: Starting with all patient encounters (i.e. covid-related hospitalizations occurring in 2020) for adult patients (age 18+) with valid ZCTAs, this table stratifies by whether or not the patient encounter has a valid 21-day PM2.5 measurement (where valid means non-missing data for at least 18/21 days)

### Figures: 
**Covid-related hospitalization density plots**: All of these plots using the full analytic dataset and reduce it to only those CBSAs that were included in the CBSA-level sub-analysis (see cbsa_sub_short). Then the program creates a density plot (similar to a spaghetti plot) of the timing of the admissions for covid-related hospitalization. Each program subsets to a different "group" of patients, either to individual CBSAs within a Census Region, or the Census Regions themselves. Importantly, figures cannot be downloaded directly, they must first be added to a report and then have the report downloaded. The report through which these figures were download is: 'Timing of cohort hospitalizations'
  1. **ch_timing_plot_WEST**: Timing of covid-related hospitalizations for each CBSA within the West Census Region
  2. **ch_timing_plot_MIDWEST**: Timing of covid-related hospitalizations for each CBSA within the Midwest Census Region
  3. **ch_timing_plot_NORTHEAST**: Timing of covid-related hospitalizations for each CBSA within the Northeast Census Region
  4. **ch_timing_plot_SOUTH**: Timing of covid-related hospitalizations for each CBSA within the South Census Region
  5. **ch_timing_plot_REGION**: Timing of covid-related hospitalizations for each Census Region


### Other Relevant Datasets:
  1. **cbsa_covid_count_obscure**: This dataset lists counts of SARS-CoV-2 infections by CBSA. The counts allow the same person to have multiple infections but will only count additional infections that occur > 90 days following the original infection (this is because there are many cases where a person may have a test result every day for a week or month, etc and we do not want to double count these). It is also important to note that upstream of this dataset, the infections counted here do not necessarily align perfectly with the test results corresponding to the hospitalization that got patients into our final cohort of covid-related hospitalizations. Similar to other CBSA-level files, an obscuring process changed all counts < 20 to '-1'
  2. **covid_pos_dedup**: This dataset contains the final patient/SARS-CoV-2  test result (i.e. 'SARS-CoV-2 infection') list that provides the first number listed on the STROBE diagram (i.e. the number of SARS-CoV-2 test results)
  3. **CONSORT_Step1a**: This data provides the 2nd number in the CONSORT diagram (variable: covid_hosps), i.e. the number of covid-related hospitalizations meeting the 16/1 day criteria following a SARS-CoV-2 test result that occurred in 2020 (hospitalization need not have occurred in 2020 to be counted in this total)





