library(gtsummary)
#library(tableone)
library(tidyverse)
library(survival)
library(dplyr)
#library(multcomp)
library(ggplot2)
library(lattice)
library(gridExtra)
library(grid)
#library(textreg)
library(splines)
library(lubridate)
library(nlme)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8d944145-1911-46c4-b0b1-119e288d1e61"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
CBSA_pctile_rank <- function(ch_analytic_wmeans) {
    data_new <- as.data.frame(ch_analytic_wmeans)
    data_new1<-subset(data_new,case==1)

    data_new1$cases<-sum(data_new1$case)

# Initialize a results data frame
results <- data.frame(CBSA_CODE = unique(data_new1$CBSA_CODE))

variables <- c("pm25_2d_valid", "pm25_5d_valid", "pm25_21d_valid","Precip", "MeanTemp_C", "DewPoint","gender_female","race_white", "race_black", "race_api", "race_aian", "race_other", "race_null", "age_at_pos", "CVD", "RD", "MD")

# Loop through each variable
for (var in variables) {
  # Calculate mean for the current variable by group
  group_means <- aggregate(data_new1[[var]] ~ data_new1$CBSA_CODE, FUN = mean)
  colnames(group_means) <- c("CBSA_CODE", paste0(var, "_mean"))
  
  # Count the number of observations per group
  n_observations <- aggregate(data_new1[[var]] ~ data_new1$CBSA_CODE, FUN = length)
  colnames(n_observations) <- c("CBSA_CODE", paste0(var, "_n_obs"))

    # Merge means and observation counts
  group_means <- merge(group_means, n_observations, by = "CBSA_CODE")

    # Filter CBSAs with at least 50 observations
  filtered_means <- subset(group_means, group_means[[paste0(var, "_n_obs")]] >= 50)

  # Calculate percentile ranks
  #group_means[[paste0(var, "_percentile_rank")]] <- 
  #  ecdf(group_means[[paste0(var, "_mean")]])(group_means[[paste0(var, "_mean")]]) * 100

  # Calculate percentile ranks for filtered CBSAs
  filtered_means[[paste0(var, "_percentile_rank")]] <- 
    ecdf(filtered_means[[paste0(var, "_mean")]])(filtered_means[[paste0(var, "_mean")]]) * 100

  
  # Merge the results into the results data frame
  #results <- merge(results, group_means, by = "CBSA_CODE")
  #results <- merge(results, n_observations, by = "CBSA_CODE")

    results <- merge(results, filtered_means, by = "CBSA_CODE", all.x = TRUE)
}

  return(results)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b676d3bc-04ab-4014-b717-120fbe1fa9df"),
    cbsa_sub_short=Input(rid="ri.foundry.main.dataset.ec7a7826-2c0d-4bf6-98da-2978ca53b870"),
    ch_analytic_for_graphs=Input(rid="ri.foundry.main.dataset.19697fec-0a26-4d5f-a38f-f1c2bf3ef78e")
)
Ch_timing_plot_REGION <- function( cbsa_sub_short, ch_analytic_for_graphs) {
    data_new<-as.data.frame(ch_analytic_for_graphs)
    df<-subset(data_new,case==1&!is.na(pm25_21d)&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE)&CBSA_CODE %in% cbsa_sub_short$Denom)

    #df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,CBSA=as.factor(df$CBSA_Name)), FUN=sum)
    df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,Region=(df$CENSUS_REGION_LABEL)), FUN=sum)

    df_Midwest<-subset(df,CENSUS_REGION_LABEL=="Midwest")
    df_South<-subset(df,CENSUS_REGION_LABEL=="South")
    df_Northeast<-subset(df,CENSUS_REGION_LABEL=="Northeast")
    df_West<-subset(df,CENSUS_REGION_LABEL=="West")

    #p<-ggplot(df2, aes(x = date, y = x, color = Region)) + geom_line()
    
p<-ggplot(df, aes(macrovisit_start_date, color = CENSUS_REGION_LABEL)) +
  geom_density() + labs(y = "Density", x = "Admission Date")+guides(color = guide_legend(title = "Census Region"))+ ggtitle("Census Regions")+theme(
  plot.title = element_text(hjust = 0.5),
  plot.subtitle = element_text(hjust = 0.5)
)

 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #p<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #par(mfrow=c(2,2))
#p1<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p2<-ggplot(df_Midwest, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p3<-ggplot(df_South, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p4<-ggplot(df_West, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 

#grid.arrange(p1, p2,p3,p4, nrow = 2)

  plot(p)
 return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.38f111e3-974f-4da4-9dbb-84b9a7ab1595"),
    cbsa_summary_stats_final_sub=Input(rid="ri.foundry.main.dataset.17e0104c-adb1-4c12-a519-8bb3ddc713a8")
)
/* ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        3/15/2023
# Last modified:  4/27/2023
# Purpose:        This program takes the "table 1" type summaries by CBSA and performs a data obscuring process (i.e. obscuring small cells to make data valid for download from N3C) as follows:
#                       1. Any cell size <20 has value changed to -1 (this is abritraty and meant to numerically flag these cells)
#                       2. In cases where there is only one cell for a mutually exclusive categorical variable (e.g. race) that is <20 (and therefore the count could be back-calculated from the total), a random jitter +/- 5 (excluding 0) is applied to one of the categories that has a >20 count. The resulting count is then turned into a negative number so that it can be flagged during the post-download table creation process as a cell that has had a random jitter applied. 

# Inputs:         'cbsa_summary_stats_final_sub'
# Outputs:        'cbsa_stats_obscure'
#
#==============================================================================*/

cbsa_stats_obscure <- function(cbsa_summary_stats_final_sub) {
   data<-cbsa_summary_stats_final_sub
    vars<-c("race_categ_aian", "race_categ_api","race_categ_black", "race_categ_othermult", "race_categ_white","race_categ_na", "ethnicity_hisp", "ethnicity_nonhisp","ethnicity_otherna", "gender_male","gender_female", "gender_na", "CVD0","MD0", "MD1","RD0", "RD1", "age_gte650", "age_gte651")
    data[vars][data[vars]<20&data[vars]!=0]<--1

    ##Count the number of categories with <20. Will be concerned about the ones where there is only one category with <20 - jitter larger categories in these cases from -5:1 and 1:5 (i.e. no 0 jitter)
        ##Obscuring is as follows: -1 means <20 cases, all other negative numbers mean that number has been jittered (the absolute value of that number will be used)
  	data$count_race_scc<-ifelse(data$race_categ_aian == -1,1,0)+ifelse(data$race_categ_api == -1,1,0)+ifelse(data$race_categ_black == -1,1,0)
	+ifelse(data$race_categ_othermult == -1,1,0)+ifelse(data$race_categ_white == -1,1,0)
	
    ##Count the number of categories with <20 for each categorical variable
	data$count_ethnicity_scc<-ifelse(data$ethnicity_hisp == -1,1,0)+ifelse(data$ethnicity_nonhisp == -1,1,0)+ifelse(data$ethnicity_otherna == -1,1,0)
	data$count_gender_scc<-ifelse(data$gender_male == -1,1,0)+ifelse(data$gender_female == -1,1,0)	
	data$count_age_gte65_scc<-ifelse(data$age_gte650 == -1,1,0)+ifelse(data$age_gte651 == -1,1,0)	
 
    ##Concerned with categorical variables that have only 1 case of <20 because count can be back calculated use jittered upper values in those cases - to keep consistent, jitter counts for white race (majority is usually White, sometimes Black)
    x<-unlist(list(-5:-1,1:5))
    data$jitter<-sample(x,nrow(data), replace=TRUE)
    #data$race_categ_white_copy<-data$race_categ_white
    data$race_categ_white<-ifelse(data$count_race_scc==1,-(data$race_categ_white+data$jitter),data$race_categ_white)
    data$ethnicity_nonhisp<-ifelse(data$count_ethnicity_scc==1,-(data$ethnicity_nonhisp+data$jitter),data$ethnicity_nonhisp)
    data$gender_male<-ifelse(data$count_gender_scc==1,-(data$gender_male+data$jitter),data$gender_male)
    data$CVD0<-ifelse(data$CVD1==-1,-(data$CVD0+data$jitter),data$CVD0)
    data$RD0<-ifelse(data$RD1==-1,-(data$RD0+data$jitter),data$RD0)
    data$MD0<-ifelse(data$MD1==-1,-(data$MD0+data$jitter),data$MD0)

    data$age_gte650<-ifelse(data$count_age_gte65_scc==1&data$age_gte651==-1,-(data$age_gte650+data$jitter),data$age_gte650)
    data$age_gte651<-ifelse(data$count_age_gte65_scc==1&data$age_gte650==-1,-(data$age_gte651+data$jitter),data$age_gte651)
    ##remove the jitter variable
    data1 <- subset(data, select = -c(jitter))

    return(data1)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ec7a7826-2c0d-4bf6-98da-2978ca53b870"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Subanalysis with CBSAs using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for CBSA

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'ch_analytic_cbsa'
# Revisions:	  1/30/2023 - Work in check on number of obs per strata to main analytic program
#                 3/17/2023 - Adding number of cases to export of regression estimates
#                 4/19/2023 - Adding outer loop so I can run through multiple main exposures (21-day, 5-day, 2-day PM2.5)
#                 5/11/2023 - Replacing individual day of the week with weekend indicator
#                 5/25/2023 - Getting rid of day-of-the-week in all forms (not valid for this analysis)
#==============================================================================*/

cbsa_sub_short <- function(ch_analytic_wmeans) {
##First need to identify CBSAs that have 50+ valid cases
data_new <- as.data.frame(ch_analytic_wmeans)

fit_model <- function(var, data) {
  data_new1 <- subset(data, case == 1 & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CBSA_CODE))
  data_new2 <- data_new1 %>% 
    group_by(CBSA_CODE) %>% 
    mutate(cbsa_obs = sum(case, na.rm = TRUE))
  cbsa_sub <- subset(data_new2, cbsa_obs >= 50)
  cbsas <- unique(cbsa_sub$CBSA_CODE)
  
  ans <- lapply(cbsas, function(x) {
    data_sub <- subset(data, CBSA_CODE == x & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CBSA_CODE))
    data_sub1<-subset(data_sub,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    cases <- sum(data_sub1$case)
    formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d + days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
    mod.adj <- clogit(formula = formula, data = data_sub, method="efron")
    summary <- summary(mod.adj)
    result <- cbind(rownames(summary$coefficients), summary$coefficients[,-2], summary$conf.int)
    result <- as.data.frame(result)
    result$n <- mod.adj$n
    result$cases <- cases
    result$Denom <- x
    result$AIC<-extractAIC(mod.adj)[2]
    result$BIC<-BIC(mod.adj)
    result <- result %>% 
      rename("se_coef" = "se(coef)",
             "p_value" = "Pr(>|z|)",
             "exp_coef" = "exp(coef)",
             "exp_neg_coef" = "exp(-coef)",
             "lower" = "lower .95",
             "upper" = "upper .95",
             "robust_se" = "robust se")
    result <- mutate(result, var = var)
    return(result)
  })
  names(ans) <- cbsas
  final <- do.call(rbind.data.frame, ans)
  return(final)
} ##end function

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data_new))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.befba93c-2211-465d-9bb3-dd53ff9df71f"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Subanalysis with CBSAs using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for CBSA

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'cbsa_sub_short_v2'
# Revisions:	  12/5/2025 - this is a slightly edited version of 'cbsa_sub_short' where natural splines are used for all continuous adjustment variables (except for precipitation)
#==============================================================================*/

cbsa_sub_short_v2 <- function(ch_analytic_wmeans) {
##First need to identify CBSAs that have 50+ valid cases
data_new <- as.data.frame(ch_analytic_wmeans)

fit_model <- function(var, data) {
  data_new1 <- subset(data, case == 1 & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CBSA_CODE))
  data_new2 <- data_new1 %>% 
    group_by(CBSA_CODE) %>% 
    mutate(cbsa_obs = sum(case, na.rm = TRUE))
  cbsa_sub <- subset(data_new2, cbsa_obs >= 50)
  cbsas <- unique(cbsa_sub$CBSA_CODE)
  
  ans <- lapply(cbsas, function(x) {
    data_sub <- subset(data, CBSA_CODE == x & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CBSA_CODE))
    data_sub1<-subset(data_sub,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    cases <- sum(data_sub1$case)
    formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + Precip + ns(DewPoint,df=3) + ns(new_cases_7d,df=3) + ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
    mod.adj <- clogit(formula = formula, data = data_sub, method="efron")
    summary <- summary(mod.adj)
    result <- cbind(rownames(summary$coefficients), summary$coefficients[,-2], summary$conf.int)
    result <- as.data.frame(result)
    result$n <- mod.adj$n
    result$cases <- cases
    result$Denom <- x
    result$AIC<-extractAIC(mod.adj)[2]
    result$BIC<-BIC(mod.adj)
    result <- result %>% 
      rename("se_coef" = "se(coef)",
             "p_value" = "Pr(>|z|)",
             "exp_coef" = "exp(coef)",
             "exp_neg_coef" = "exp(-coef)",
             "lower" = "lower .95",
             "upper" = "upper .95",
             "robust_se" = "robust se")
    result <- mutate(result, var = var)
    return(result)
  })
  names(ans) <- cbsas
  final <- do.call(rbind.data.frame, ans)
  return(final)
} ##end function

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data_new))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4a1ad2b9-a588-462d-8a9f-75752ec0986f"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  12/21/2022
# Purpose:        Subanalysis with CBSAs using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for CBSA

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'ch_analytic_cbsa'
# Revisions:	  1/30/2023 - Work in check on number of obs per strata to main analytic program
#                 05/11/2023 - Make days of the week variable binary (weekday, weekend) to help with fitting 
#                 05/31/2023 - Correct issue with abnormal exclusions (back to just pm25_21d)
#                
#==============================================================================*/

cbsa_sub_sub_TEST <- function(ch_analytic_wmeans) {

ch_analytic_wmeans1<-as.data.frame(ch_analytic_wmeans)
		ch_analytic_wmeans2<-subset(ch_analytic_wmeans1,!is.na(case)&!is.na(pm25_21d)&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE))
		ch_analytic_wmeans2$CBSA_CODE<-as.character(ch_analytic_wmeans2$CBSA_CODE)
		ch_analytic_wmeans2 <- ch_analytic_wmeans2 %>%
				rowwise() %>%
					mutate(category = paste0(CBSA_CODE, "_", CVD, "_", MD, "_", RD, "_", age_gte65))
					
		CBSA_CODEs<-unique(as.character(ch_analytic_wmeans2$CBSA_CODE))

		# Create dataframe with one row for each combination of variables we want to create a subgroup for
		df_combinations <- 
		expand_grid(  CBSA_CODE = str_replace(unique(ch_analytic_wmeans2$CBSA_CODE), "-", "_"),
						var_name = c("CVD", "MD", "RD","age_gte65"),
						condition = c(0, 1)) %>%
		as.data.frame() %>%
		mutate(subgroup = paste0(CBSA_CODE, "_", var_name, "_", condition))

		# Split dataframe into list of one-row dataframes
		list_combinations <- split(df_combinations, f = df_combinations$subgroup)

		ans2<-lapply(list_combinations, function(x){
		x <- x %>%
			mutate(var_name_copy = var_name) %>%
			pivot_wider(c(CBSA_CODE, var_name), names_from = var_name_copy, values_from = condition)
		# Merge so that only data for one subgroup remains
		data_new <- left_join(x, ch_analytic_wmeans2, by = c("CBSA_CODE", x$var_name))
		cases<-sum(data_new$case)
		if(nrow(data_new[,"case"==1]) >= 150){
        #if(cases >= 150){
			cases<-sum(data_new$case)
            #cases<-nrow(subset(data_new,case==1&&!is.na(get(var))))
			formula <- as.formula(paste("case ~", "pm25_21d+ MeanTemp_C + Precip + DewPoint + new_cases_7d+days_from_1st_case_cbsa + strata(person_id)"))
			mod.adj<-clogit(formula=formula, data=data_new)
			summary<-summary(mod.adj)
			result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
			result<-as.data.frame(result)
			result$n<-mod.adj$n
			result$cases<-cases
			result$Denom<-x$CBSA_CODE
			result$var_type<-x$var_name
			result <- result %>% 
			rename("se_coef" = "se(coef)",
					"p_value" = "Pr(>|z|)",
					"exp_coef" = "exp(coef)",
					"exp_neg_coef" = "exp(-coef)",
					"lower" = "lower .95",
					"upper" = "upper .95")
					  #result <- mutate(result, var = var)
			return(result)
		}

	 else {
			return(NULL)
		}
		
		})

		# str(ans2)

		# names(ans2)<-races
		test<-do.call(rbind.data.frame, ans2) %>%
		rownames_to_column("subgroup") %>%
		mutate(subgroup = str_remove(subgroup, "\\..*"))
		return(test)
		
	} #end function
		
		

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b2e8419c-949d-41d4-82da-451a5f4aa13e"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/31/2023
# Purpose:        Subanalysis with CBSAs using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for CBSA

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'ch_analytic_cbsa'
# Revisions:	  1/30/2023 - Work in check on number of obs per strata to main analytic program
#                 05/11/2023 - Make days of the week variable binary (weekday, weekend) to help with fitting 
#                 05/31/2023 - Correct issue with abnormal exclusions (back to just pm25_21d)
#                 09/11/2023 - Add race subgroups
#                
#==============================================================================*/

cbsa_sub_sub_TEST2_3m <- function(ch_analytic_wmeans) {

        ch_analytic_wmeans1<-as.data.frame(ch_analytic_wmeans)
fit_model <- function(var,set) {
		ch_analytic_wmeans2<-subset(set,!is.na(case)&!is.na(get(var))&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE))
		ch_analytic_wmeans2$CBSA_CODE<-as.character(ch_analytic_wmeans2$CBSA_CODE)
		ch_analytic_wmeans2 <- ch_analytic_wmeans2 %>%
				rowwise() %>%
					mutate(category = paste0(CBSA_CODE, "_", CVD, "_", MD, "_", RD, "_", age_gte65,"_",race_white,"_",race_black,"_",race_api,"_",race_aian,"_",race_other,"_",race_null))
					
		CBSA_CODEs<-unique(as.character(ch_analytic_wmeans2$CBSA_CODE))

		# Create dataframe with one row for each combination of variables we want to create a subgroup for
		df_combinations <- 
		expand_grid(  CBSA_CODE = str_replace(unique(ch_analytic_wmeans2$CBSA_CODE), "-", "_"),
						var_name = c("CVD", "MD", "RD","age_gte65","race_white","race_black","race_api","race_aian","race_other","race_null"),
						condition = c(0, 1)) %>%
		as.data.frame() %>%
		mutate(subgroup = paste0(CBSA_CODE, "_", var_name, "_", condition))

		# Split dataframe into list of one-row dataframes
		list_combinations <- split(df_combinations, f = df_combinations$subgroup)

		ans2<-lapply(list_combinations, function(x){
		x <- x %>%
			mutate(var_name_copy = var_name) %>%
			pivot_wider(c(CBSA_CODE, var_name), names_from = var_name_copy, values_from = condition)
		# Merge so that only data for one subgroup remains
		data_new <- left_join(x, ch_analytic_wmeans2, by = c("CBSA_CODE", x$var_name))
		cases<-sum(data_new$case)
		if(nrow(data_new[,"case"==1]) >= 150){
        #if(cases >= 150){
            data_new1<-subset(data_new,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
            
			cases<-sum(data_new1$case)
            #cases<-nrow(subset(data_new,case==1&&!is.na(get(var))))
			formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d+days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
			mod.adj<-clogit(formula=formula, data=data_new, method="efron")
			summary<-summary(mod.adj)
			result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
			result<-as.data.frame(result)
			result$n<-mod.adj$n
			result$cases<-cases
			result$Denom<-x$CBSA_CODE
			result$var_type<-x$var_name
            result <- result %>% 
			rename("se_coef" = "se(coef)",
					"p_value" = "Pr(>|z|)",
					"exp_coef" = "exp(coef)",
					"exp_neg_coef" = "exp(-coef)",
					"lower" = "lower .95",
					"upper" = "upper .95",
                    "robust_se" = "robust se")
					  result <- mutate(result, var = var)
			return(result)
		}

	 else {
			return(NULL)
		}
		
		})

		# str(ans2)

		# names(ans2)<-races
		test<-do.call(rbind.data.frame, ans2) %>%
		rownames_to_column("subgroup") %>%
		mutate(subgroup = str_remove(subgroup, "\\..*"))
		return(test)
		
	} #end function

    	var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
	results_list <- lapply(var_list, function(var) fit_model(var, ch_analytic_wmeans1))
    #results_list <- lapply(var_list, fit_model(var, ch_analytic_wmeans1))

	# Combine results for all variables in the list
	results1 <- bind_rows(results_list)

    # Function to extract ending letters from a string
    substrRight <- function(x, n){
        substr(x, nchar(x)-n+1, nchar(x))
    }

    results1$level<-substrRight(results1$subgroup, 1)

    # Remove racial categories = 0
    #results<-subset(results1,!subgroup %in% c("race_white_0","race_black_0","race_api_0","race_aian_0","race_other_0","race_null_0"))
    #results2$drop<-ifelse(grepl("race_white_0|race_black_0|race_api_0|race_aian_0|race_other_0|race_null_0", results1$subgroup), "drop", "keep")
   # results<-subset(results1,grepl("race_white_0", subgroup)==FALSE&&grepl("race_black_0", subgroup)==FALSE&&grepl("race_api_0", subgroup)==FALSE&&grepl("race_aian_0", subgroup)==FALSE&&grepl("race_other_0", subgroup)==FALSE&&grepl("race_null_0", subgroup)==FALSE)
    #results<-subset(results2,drop=="keep")
    results1$flag1<-ifelse(results1$var_type %in% c("race_white","race_black","race_api","race_aian","race_other","race_null"),1,0)
    results1$flag2<-ifelse(results1$level=='0',1,0)
    results<-subset(results1,flag1==0|flag2==0)
			return(results)	
}
		
		

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.128d659b-30f7-454a-a3fa-2f19b7da4c1c"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/31/2023
# Purpose:         Subanalysis of medical condition (MD, RD, CVD) within CBSAs using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for CBSA
# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'cbsa_sub_sub_TEST_3m'
# Revisions:	  1/30/2023 - Work in check on number of obs per strata to main analytic program
#                 05/11/2023 - Make days of the week variable binary (weekday, weekend) to help with fitting 
#                 05/31/2023 - Correct issue with abnormal exclusions (back to just pm25_21d)
#                 09/11/2023 - Add race subgroups
#                
#==============================================================================*/

cbsa_sub_sub_TEST_3m <- function(ch_analytic_wmeans) {

        ch_analytic_wmeans1<-as.data.frame(ch_analytic_wmeans)
fit_model <- function(var,set) {
		ch_analytic_wmeans2<-subset(set,!is.na(case)&!is.na(get(var))&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE))
		ch_analytic_wmeans2$CBSA_CODE<-as.character(ch_analytic_wmeans2$CBSA_CODE)
		ch_analytic_wmeans2 <- ch_analytic_wmeans2 %>%
				rowwise() %>%
					mutate(category = paste0(CBSA_CODE, "_", CVD, "_", MD, "_", RD, "_", age_gte65,"_",race_white,"_",race_black,"_",race_api,"_",race_aian,"_",race_other,"_",race_null))
					
		CBSA_CODEs<-unique(as.character(ch_analytic_wmeans2$CBSA_CODE))

		# Create dataframe with one row for each combination of variables we want to create a subgroup for
		df_combinations <- 
		expand_grid(  CBSA_CODE = str_replace(unique(ch_analytic_wmeans2$CBSA_CODE), "-", "_"),
						var_name = c("CVD", "MD", "RD","age_gte65","race_white","race_black","race_api","race_aian","race_other","race_null"),
						condition = c(0, 1)) %>%
		as.data.frame() %>%
		mutate(subgroup = paste0(CBSA_CODE, "_", var_name, "_", condition))

		# Split dataframe into list of one-row dataframes
		list_combinations <- split(df_combinations, f = df_combinations$subgroup)

		ans2<-lapply(list_combinations, function(x){
		x <- x %>%
			mutate(var_name_copy = var_name) %>%
			pivot_wider(c(CBSA_CODE, var_name), names_from = var_name_copy, values_from = condition)
		# Merge so that only data for one subgroup remains
		data_new <- left_join(x, ch_analytic_wmeans2, by = c("CBSA_CODE", x$var_name))
		cases<-sum(data_new$case)
		if(nrow(data_new[,"case"==1]) >= 150){
        #if(cases >= 150){
            data_new1<-subset(data_new,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
            
			cases<-sum(data_new1$case)
            #cases<-nrow(subset(data_new,case==1&&!is.na(get(var))))
			formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d+days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
			mod.adj<-clogit(formula=formula, data=data_new, method="efron")
			summary<-summary(mod.adj)
			result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
			result<-as.data.frame(result)
			result$n<-mod.adj$n
			result$cases<-cases
			result$Denom<-x$CBSA_CODE
            result$AIC<-extractAIC(mod.adj)[2]
            result$BIC<-BIC(mod.adj)
			result$var_type<-x$var_name
            result <- result %>% 
			rename("se_coef" = "se(coef)",
					"p_value" = "Pr(>|z|)",
					"exp_coef" = "exp(coef)",
					"exp_neg_coef" = "exp(-coef)",
					"lower" = "lower .95",
					"upper" = "upper .95",
                    "robust_se" = "robust se")
					  result <- mutate(result, var = var)
			return(result)
		}

	 else {
			return(NULL)
		}
		
		})

		# str(ans2)

		# names(ans2)<-races
		test<-do.call(rbind.data.frame, ans2) %>%
		rownames_to_column("subgroup") %>%
		mutate(subgroup = str_remove(subgroup, "\\..*"))
		return(test)
		
	} #end function

    	var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
	results_list <- lapply(var_list, function(var) fit_model(var, ch_analytic_wmeans1))
    #results_list <- lapply(var_list, fit_model(var, ch_analytic_wmeans1))

	# Combine results for all variables in the list
	results1 <- bind_rows(results_list)

        # Function to extract ending letters from a string
    substrRight <- function(x, n){
        substr(x, nchar(x)-n+1, nchar(x))
    }

    results1$level<-substrRight(results1$subgroup, 1)

    # Remove racial categories = 0
    #results<-subset(results1,!subgroup %in% c("race_white_0","race_black_0","race_api_0","race_aian_0","race_other_0","race_null_0"))
    #results2$drop<-ifelse(grepl("race_white_0|race_black_0|race_api_0|race_aian_0|race_other_0|race_null_0", results1$subgroup), "drop", "keep")
   # results<-subset(results1,grepl("race_white_0", subgroup)==FALSE&&grepl("race_black_0", subgroup)==FALSE&&grepl("race_api_0", subgroup)==FALSE&&grepl("race_aian_0", subgroup)==FALSE&&grepl("race_other_0", subgroup)==FALSE&&grepl("race_null_0", subgroup)==FALSE)
    #results<-subset(results2,drop=="keep")
    results1$flag1<-ifelse(results1$var_type %in% c("race_white","race_black","race_api","race_aian","race_other","race_null"),1,0)
    results1$flag2<-ifelse(results1$level=='0',1,0)
    results<-subset(results1,flag1==0|flag2==0)
			return(results)
}
		
		

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.99ff4f9f-34dc-412d-96fb-9c7807636b69"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/31/2023
# Purpose:        Subanalysis of medical condition (MD, RD, CVD) within CBSAs using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for CBSA

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'cbsa_sub_sub_TEST_3m_v2'
# Revisions:	  12/5/2025 - This is a slightly edited version of 'cbsa_sub_sub_TEST_3m' where natural splines are used for all continouous adjustment variables (except for precipitation)
#                
#==============================================================================*/

cbsa_sub_sub_TEST_3m_v2 <- function(ch_analytic_wmeans) {

        ch_analytic_wmeans1<-as.data.frame(ch_analytic_wmeans)
fit_model <- function(var,set) {
		ch_analytic_wmeans2<-subset(set,!is.na(case)&!is.na(get(var))&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE))
		ch_analytic_wmeans2$CBSA_CODE<-as.character(ch_analytic_wmeans2$CBSA_CODE)
		ch_analytic_wmeans2 <- ch_analytic_wmeans2 %>%
				rowwise() %>%
					mutate(category = paste0(CBSA_CODE, "_", CVD, "_", MD, "_", RD, "_", age_gte65,"_",race_white,"_",race_black,"_",race_api,"_",race_aian,"_",race_other,"_",race_null))
					
		CBSA_CODEs<-unique(as.character(ch_analytic_wmeans2$CBSA_CODE))

		# Create dataframe with one row for each combination of variables we want to create a subgroup for
		df_combinations <- 
		expand_grid(  CBSA_CODE = str_replace(unique(ch_analytic_wmeans2$CBSA_CODE), "-", "_"),
						var_name = c("CVD", "MD", "RD","age_gte65","race_white","race_black","race_api","race_aian","race_other","race_null"),
						condition = c(0, 1)) %>%
		as.data.frame() %>%
		mutate(subgroup = paste0(CBSA_CODE, "_", var_name, "_", condition))

		# Split dataframe into list of one-row dataframes
		list_combinations <- split(df_combinations, f = df_combinations$subgroup)

		ans2<-lapply(list_combinations, function(x){
		x <- x %>%
			mutate(var_name_copy = var_name) %>%
			pivot_wider(c(CBSA_CODE, var_name), names_from = var_name_copy, values_from = condition)
		# Merge so that only data for one subgroup remains
		data_new <- left_join(x, ch_analytic_wmeans2, by = c("CBSA_CODE", x$var_name))
		cases<-sum(data_new$case)
		if(nrow(data_new[,"case"==1]) >= 150){
        #if(cases >= 150){
            data_new1<-subset(data_new,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
            
			cases<-sum(data_new1$case)
            #cases<-nrow(subset(data_new,case==1&&!is.na(get(var))))
			formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + Precip + ns(DewPoint,df=3) + ns(new_cases_7d,df=3)+ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
			mod.adj<-clogit(formula=formula, data=data_new, method="efron")
			summary<-summary(mod.adj)
			result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
			result<-as.data.frame(result)
			result$n<-mod.adj$n
			result$cases<-cases
			result$Denom<-x$CBSA_CODE
            result$AIC<-extractAIC(mod.adj)[2]
            result$BIC<-BIC(mod.adj)
			result$var_type<-x$var_name
            result <- result %>% 
			rename("se_coef" = "se(coef)",
					"p_value" = "Pr(>|z|)",
					"exp_coef" = "exp(coef)",
					"exp_neg_coef" = "exp(-coef)",
					"lower" = "lower .95",
					"upper" = "upper .95",
                    "robust_se" = "robust se")
					  result <- mutate(result, var = var)
			return(result)
		}

	 else {
			return(NULL)
		}
		
		})

		# str(ans2)

		# names(ans2)<-races
		test<-do.call(rbind.data.frame, ans2) %>%
		rownames_to_column("subgroup") %>%
		mutate(subgroup = str_remove(subgroup, "\\..*"))
		return(test)
		
	} #end function

    	var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
	results_list <- lapply(var_list, function(var) fit_model(var, ch_analytic_wmeans1))
    #results_list <- lapply(var_list, fit_model(var, ch_analytic_wmeans1))

	# Combine results for all variables in the list
	results1 <- bind_rows(results_list)

        # Function to extract ending letters from a string
    substrRight <- function(x, n){
        substr(x, nchar(x)-n+1, nchar(x))
    }

    results1$level<-substrRight(results1$subgroup, 1)

    # Remove racial categories = 0
    #results<-subset(results1,!subgroup %in% c("race_white_0","race_black_0","race_api_0","race_aian_0","race_other_0","race_null_0"))
    #results2$drop<-ifelse(grepl("race_white_0|race_black_0|race_api_0|race_aian_0|race_other_0|race_null_0", results1$subgroup), "drop", "keep")
   # results<-subset(results1,grepl("race_white_0", subgroup)==FALSE&&grepl("race_black_0", subgroup)==FALSE&&grepl("race_api_0", subgroup)==FALSE&&grepl("race_aian_0", subgroup)==FALSE&&grepl("race_other_0", subgroup)==FALSE&&grepl("race_null_0", subgroup)==FALSE)
    #results<-subset(results2,drop=="keep")
    results1$flag1<-ifelse(results1$var_type %in% c("race_white","race_black","race_api","race_aian","race_other","race_null"),1,0)
    results1$flag2<-ifelse(results1$level=='0',1,0)
    results<-subset(results1,flag1==0|flag2==0)
			return(results)
}
		
		

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.871d3c37-056e-44ea-9e66-e77926aa1dcd"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        04/25/2023
# Last modified:  04/25/2023
# Purpose:        Subanalysis with Census Regions using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for Census Region. Note: Will almost certain not use this output for the publication but having it allows me to code visuals and know that the final output (produce by Cavin) will not be that far off.

# Inputs:         'ch_analytic_wmeans', 
# Outputs:        'census_sub_short'
# Revisions:	  
#==============================================================================*/

census_sub_short <- function(ch_analytic_wmeans) {
##First need to identify CBSAs that have 50+ valid cases
data_new <- as.data.frame(ch_analytic_wmeans)

fit_model <- function(var, data) {
  data_new1 <- subset(data, case == 1 & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CBSA_CODE))
  data_new2 <- data_new1 %>% 
    group_by(CENSUS_REGION) %>% 
    mutate(cbsa_obs = sum(case, na.rm = TRUE))
  cbsa_sub <- subset(data_new2, cbsa_obs >= 50)
  cbsas <- unique(cbsa_sub$CENSUS_REGION)
  
  ans <- lapply(cbsas, function(x) {
    data_sub <- subset(data, CENSUS_REGION == x & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CENSUS_REGION))
    data_sub1<-subset(data_sub,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    cases <- sum(data_sub1$case)
    #cases <- sum(data_sub$case)
    formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d + days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
    mod.adj <- clogit(formula = formula, data = data_sub, method="efron")
    summary <- summary(mod.adj)
    result <- cbind(rownames(summary$coefficients), summary$coefficients[,-2], summary$conf.int)
    result <- as.data.frame(result)
    result$n <- mod.adj$n
    result$cases <- cases
    result$Denom <- x
    result$AIC<-extractAIC(mod.adj)[2]
    result$BIC<-BIC(mod.adj)
    result <- result %>% 
      rename("se_coef" = "se(coef)",
             "p_value" = "Pr(>|z|)",
             "exp_coef" = "exp(coef)",
             "exp_neg_coef" = "exp(-coef)",
             "lower" = "lower .95",
             "upper" = "upper .95",
             "robust_se" = "robust se")
    result <- mutate(result, var = var)
    return(result)
  })
  names(ans) <- cbsas
  final <- do.call(rbind.data.frame, ans)
  return(final)
} ##end function

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data_new))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6f2e2b47-0a9b-47e0-8eb8-45807a1588bd"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        04/25/2023
# Last modified:  04/25/2023
# Purpose:        Subanalysis with Census Regions using loop and CBSAs with greater than X (still deciding on final cutoff). Save output with an identifier for Census Region. Note: Will almost certain not use this output for the publication but having it allows me to code visuals and know that the final output (produce by Cavin) will not be that far off.

# Inputs:         'ch_analytic_wmeans', 
# Outputs:        'census_sub_short_v2'
# Revisions:	  12/5/2025 - This is a slightly edited version of 'census_sub_short' where natural splines are used for all continous adjustment variables (except for precipitation)
#==============================================================================*/

census_sub_short_v2 <- function(ch_analytic_wmeans) {
##First need to identify CBSAs that have 50+ valid cases
data_new <- as.data.frame(ch_analytic_wmeans)

fit_model <- function(var, data) {
  data_new1 <- subset(data, case == 1 & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CBSA_CODE))
  data_new2 <- data_new1 %>% 
    group_by(CENSUS_REGION) %>% 
    mutate(cbsa_obs = sum(case, na.rm = TRUE))
  cbsa_sub <- subset(data_new2, cbsa_obs >= 50)
  cbsas <- unique(cbsa_sub$CENSUS_REGION)
  
  ans <- lapply(cbsas, function(x) {
    data_sub <- subset(data, CENSUS_REGION == x & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(CENSUS_REGION))
    data_sub1<-subset(data_sub,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    cases <- sum(data_sub1$case)
    #cases <- sum(data_sub$case)
    formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + Precip + ns(DewPoint,df=3) + ns(new_cases_7d,df=3) + ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
    mod.adj <- clogit(formula = formula, data = data_sub, method="efron")
    summary <- summary(mod.adj)
    result <- cbind(rownames(summary$coefficients), summary$coefficients[,-2], summary$conf.int)
    result <- as.data.frame(result)
    result$n <- mod.adj$n
    result$cases <- cases
    result$Denom <- x
    result$AIC<-extractAIC(mod.adj)[2]
    result$BIC<-BIC(mod.adj)
    result <- result %>% 
      rename("se_coef" = "se(coef)",
             "p_value" = "Pr(>|z|)",
             "exp_coef" = "exp(coef)",
             "exp_neg_coef" = "exp(-coef)",
             "lower" = "lower .95",
             "upper" = "upper .95",
             "robust_se" = "robust se")
    result <- mutate(result, var = var)
    return(result)
  })
  names(ans) <- cbsas
  final <- do.call(rbind.data.frame, ans)
  return(final)
} ##end function

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data_new))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3b7a0817-b42f-41a1-83c3-6f8a56bab33e"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Main case-crossover analysis with all data pooled

# Inputs:         'ch_analytic_wmeans'
# Outputs:        'ch_prelim_analysis'
#
#                 3/17/2023 - Adding number of cases to export of regression estimates
#                 4/19/2023 - Adding outer loop so I can run through multiple main exposures (21-day, 5-day, 2-day PM2.5)
#                 5/11/2023 - Replacing individual day of the week with weekend indicator
#                 5/25/2023 - Getting rid of day-of-the-week in all forms (not valid for this analysis)
#==============================================================================*/

ch_prelim_analysis <- function(ch_analytic_wmeans) {
      data<-as.data.frame(ch_analytic_wmeans)
fit_model <- function(var, data) {
    data1<-subset(data,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    #cases<-sum(ifelse(data$case==1&!is.na(get(var)),1,0))
    cases<-sum(data1$case)
	  formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d + days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
		mod.adj <- clogit(formula=formula, data=data, method="efron")
    #mod.adj<- clogit(case~pm25_21d+MeanTemp_C+Precip+DewPoint+as.factor(weekday)+new_cases_7d+strata(person_id),data)
    summary<-summary( mod.adj)
        result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
        result<-as.data.frame(result)
        result$n<-mod.adj$n
        result$cases<-cases
        result$Denom<-"Total"
        result$AIC<-extractAIC(mod.adj)[2]
        result$BIC<-BIC(mod.adj)
        #Rename columns with invalid names
        result <- result %>% 
        rename("se_coef" = "se(coef)",
               "p_value" = "Pr(>|z|)",
               "exp_coef" = "exp(coef)",
               "exp_neg_coef" = "exp(-coef)",
               "lower" = "lower .95",
               "upper" = "upper .95",
               "robust_se" = "robust se")
			   
  result <- mutate(result, var = var)
  #select(result, var)
}

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)
  
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ac1aa5da-3d45-4857-ab68-917281f76881"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Main case-crossover analysis with all data pooled

# Inputs:         'ch_analytic_wmeans'
# Outputs:        'Ch prelim analysis v2'
#
#                 12/4/2025 - This is a slight edit to 'Ch prelim analysis' where natural splines are implemented for all continuous variables (except for preciptation)
#==============================================================================*/

ch_prelim_analysis_v2 <- function(ch_analytic_wmeans) {
      data<-as.data.frame(ch_analytic_wmeans)
fit_model <- function(var, data) {
    data1<-subset(data,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    #cases<-sum(ifelse(data$case==1&!is.na(get(var)),1,0))
    cases<-sum(data1$case)
	  formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + Precip + ns(DewPoint,df=3) + ns(new_cases_7d,df=3) + ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
		mod.adj <- clogit(formula=formula, data=data, method="efron")
    #mod.adj<- clogit(case~pm25_21d+MeanTemp_C+Precip+DewPoint+as.factor(weekday)+new_cases_7d+strata(person_id),data)
    summary<-summary( mod.adj)
        result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
        result<-as.data.frame(result)
        result$n<-mod.adj$n
        result$cases<-cases
        result$Denom<-"Total"
        result$AIC<-extractAIC(mod.adj)[2]
        result$BIC<-BIC(mod.adj)
        #Rename columns with invalid names
        result <- result %>% 
        rename("se_coef" = "se(coef)",
               "p_value" = "Pr(>|z|)",
               "exp_coef" = "exp(coef)",
               "exp_neg_coef" = "exp(-coef)",
               "lower" = "lower .95",
               "upper" = "upper .95",
               "robust_se" = "robust se")
			   
  result <- mutate(result, var = var)
  #select(result, var)
}

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)
  
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.140c92ed-b6e9-42a5-9c1b-8085868d6ced"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Main case-crossover analysis with all data pooled

# Inputs:         'ch_analytic_wmeans'
# Outputs:        'ch_prelim_analysis'
#
#                 3/17/2023 - Adding number of cases to export of regression estimates
#                 4/19/2023 - Adding outer loop so I can run through multiple main exposures (21-day, 5-day, 2-day PM2.5)
#                 5/11/2023 - Replacing individual day of the week with weekend indicator
#                 5/25/2023 - Getting rid of day-of-the-week in all forms (not valid for this analysis)
#==============================================================================*/

ch_prelim_analysis_v2_TEST <- function(ch_analytic_wmeans) {
      data<-as.data.frame(ch_analytic_wmeans)
fit_model <- function(var, data) {
    data1<-subset(data,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    #cases<-sum(ifelse(data$case==1&!is.na(get(var)),1,0))
    cases<-sum(data1$case)
	  formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + ns(Precip,df=4) + ns(DewPoint,df=3) + ns(new_cases_7d,df=3) + ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
		mod.adj <- clogit(formula=formula, data=data, method="efron")
    #mod.adj<- clogit(case~pm25_21d+MeanTemp_C+Precip+DewPoint+as.factor(weekday)+new_cases_7d+strata(person_id),data)
    summary<-summary( mod.adj)
        result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
        result<-as.data.frame(result)
        result$n<-mod.adj$n
        result$cases<-cases
        result$Denom<-"Total"
        result$AIC<-extractAIC(mod.adj)[2]
        result$BIC<-BIC(mod.adj)
        #Rename columns with invalid names
        result <- result %>% 
        rename("se_coef" = "se(coef)",
               "p_value" = "Pr(>|z|)",
               "exp_coef" = "exp(coef)",
               "exp_neg_coef" = "exp(-coef)",
               "lower" = "lower .95",
               "upper" = "upper .95",
               "robust_se" = "robust se")
			   
  result <- mutate(result, var = var)
  #select(result, var)
}

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)
  
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.3d17e4a8-6b37-451d-a12c-3950123b2fa8"),
    cbsa_sub_short=Input(rid="ri.foundry.main.dataset.ec7a7826-2c0d-4bf6-98da-2978ca53b870"),
    ch_analytic_for_graphs=Input(rid="ri.foundry.main.dataset.19697fec-0a26-4d5f-a38f-f1c2bf3ef78e")
)
ch_timing_plot_MIDWEST <- function( cbsa_sub_short, ch_analytic_for_graphs) {
    data_new<-as.data.frame(ch_analytic_for_graphs)
    df<-subset(data_new,case==1&!is.na(pm25_21d)&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE)&CBSA_CODE %in% cbsa_sub_short$Denom)

    #df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,CBSA=as.factor(df$CBSA_Name)), FUN=sum)
    df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,Region=(df$CENSUS_REGION_LABEL)), FUN=sum)

    df_Midwest<-subset(df,CENSUS_REGION_LABEL=="Midwest")
    df_South<-subset(df,CENSUS_REGION_LABEL=="South")
    df_Northeast<-subset(df,CENSUS_REGION_LABEL=="Northeast")
    df_West<-subset(df,CENSUS_REGION_LABEL=="West")

    #p<-ggplot(df2, aes(x = date, y = x, color = Region)) + geom_line()
    
p<-ggplot(df_Midwest, aes(macrovisit_start_date, color = cbsa_obsc)) +
  geom_density() + labs(y = "Density", x = "Admission Date")+guides(color = guide_legend(title = "CBSA"))+ ggtitle("Midwest Region")+theme(
  plot.title = element_text(hjust = 0.5),
  plot.subtitle = element_text(hjust = 0.5)
)

 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #p<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #par(mfrow=c(2,2))
#p1<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p2<-ggplot(df_Midwest, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p3<-ggplot(df_South, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p4<-ggplot(df_West, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 

#grid.arrange(p1, p2,p3,p4, nrow = 2)

  plot(p)
 return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.86cd1ecc-3cc3-433d-974a-d80b66b57542"),
    cbsa_sub_short=Input(rid="ri.foundry.main.dataset.ec7a7826-2c0d-4bf6-98da-2978ca53b870"),
    ch_analytic_for_graphs=Input(rid="ri.foundry.main.dataset.19697fec-0a26-4d5f-a38f-f1c2bf3ef78e")
)
ch_timing_plot_NORTHEAST <- function( cbsa_sub_short, ch_analytic_for_graphs) {
    data_new<-as.data.frame(ch_analytic_for_graphs)
    df<-subset(data_new,case==1&!is.na(pm25_21d)&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE)&CBSA_CODE %in% cbsa_sub_short$Denom)

    #df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,CBSA=as.factor(df$CBSA_Name)), FUN=sum)
    df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,Region=(df$CENSUS_REGION_LABEL)), FUN=sum)

    df_Midwest<-subset(df,CENSUS_REGION_LABEL=="Midwest")
    df_South<-subset(df,CENSUS_REGION_LABEL=="South")
    df_Northeast<-subset(df,CENSUS_REGION_LABEL=="Northeast")
    df_West<-subset(df,CENSUS_REGION_LABEL=="West")

    #p<-ggplot(df2, aes(x = date, y = x, color = Region)) + geom_line()
    
p<-ggplot(df_Northeast, aes(macrovisit_start_date, color = cbsa_obsc)) +
  geom_density() + labs(y = "Density", x = "Admission Date")+guides(color = guide_legend(title = "CBSA"))+ ggtitle("Northeast Region")+theme(
  plot.title = element_text(hjust = 0.5),
  plot.subtitle = element_text(hjust = 0.5)
)

 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #p<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #par(mfrow=c(2,2))
#p1<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p2<-ggplot(df_Midwest, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p3<-ggplot(df_South, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p4<-ggplot(df_West, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 

#grid.arrange(p1, p2,p3,p4, nrow = 2)

  plot(p)
 return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.d782627a-5f17-4587-becf-24013a011c98"),
    cbsa_sub_short=Input(rid="ri.foundry.main.dataset.ec7a7826-2c0d-4bf6-98da-2978ca53b870"),
    ch_analytic_for_graphs=Input(rid="ri.foundry.main.dataset.19697fec-0a26-4d5f-a38f-f1c2bf3ef78e")
)
ch_timing_plot_SOUTH <- function( cbsa_sub_short, ch_analytic_for_graphs) {
    data_new<-as.data.frame(ch_analytic_for_graphs)
    df<-subset(data_new,case==1&!is.na(pm25_21d)&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE)&CBSA_CODE %in% cbsa_sub_short$Denom)

    #df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,CBSA=as.factor(df$CBSA_Name)), FUN=sum)
    df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,Region=(df$CENSUS_REGION_LABEL)), FUN=sum)

    df_Midwest<-subset(df,CENSUS_REGION_LABEL=="Midwest")
    df_South<-subset(df,CENSUS_REGION_LABEL=="South")
    df_Northeast<-subset(df,CENSUS_REGION_LABEL=="Northeast")
    df_West<-subset(df,CENSUS_REGION_LABEL=="West")

    #p<-ggplot(df2, aes(x = date, y = x, color = Region)) + geom_line()
    
p<-ggplot(df_South, aes(macrovisit_start_date, color = cbsa_obsc)) +
  geom_density() + labs(y = "Density", x = "Admission Date")+guides(color = guide_legend(title = "CBSA"))+ ggtitle("South Region")+theme(
  plot.title = element_text(hjust = 0.5),
  plot.subtitle = element_text(hjust = 0.5)
)

 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #p<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #par(mfrow=c(2,2))
#p1<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p2<-ggplot(df_Midwest, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p3<-ggplot(df_South, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p4<-ggplot(df_West, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 

#grid.arrange(p1, p2,p3,p4, nrow = 2)

  plot(p)
 return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.5ed3d2c7-19c8-4701-b38b-6ec06b05b71c"),
    cbsa_sub_short=Input(rid="ri.foundry.main.dataset.ec7a7826-2c0d-4bf6-98da-2978ca53b870"),
    ch_analytic_for_graphs=Input(rid="ri.foundry.main.dataset.19697fec-0a26-4d5f-a38f-f1c2bf3ef78e")
)
ch_timing_plot_WEST <- function( cbsa_sub_short, ch_analytic_for_graphs) {
    data_new<-as.data.frame(ch_analytic_for_graphs)
    df<-subset(data_new,case==1&!is.na(pm25_21d)&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE)&CBSA_CODE %in% cbsa_sub_short$Denom)

    #df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,CBSA=as.factor(df$CBSA_Name)), FUN=sum)
    df2<-aggregate(df$case, by=list(date=df$macrovisit_start_date,Region=(df$CENSUS_REGION_LABEL)), FUN=sum)

    df_Midwest<-subset(df,CENSUS_REGION_LABEL=="Midwest")
    df_South<-subset(df,CENSUS_REGION_LABEL=="South")
    df_Northeast<-subset(df,CENSUS_REGION_LABEL=="Northeast")
    df_West<-subset(df,CENSUS_REGION_LABEL=="West")

    #p<-ggplot(df2, aes(x = date, y = x, color = Region)) + geom_line()
    
p<-ggplot(df_West, aes(macrovisit_start_date, color = cbsa_obsc)) +
  geom_density() + labs(y = "Density", x = "Admission Date")+guides(color = guide_legend(title = "CBSA"))+ ggtitle("West Region")+theme(
  plot.title = element_text(hjust = 0.5),
  plot.subtitle = element_text(hjust = 0.5)
)

 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #p<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
 #p<-ggplot(df, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() + facet_wrap(~ CENSUS_REGION_LABEL)
 #par(mfrow=c(2,2))
#p1<-ggplot(df_Northeast, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p2<-ggplot(df_Midwest, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p3<-ggplot(df_South, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 
#p4<-ggplot(df_West, aes(macrovisit_start_date, color = CBSA_Name)) + geom_density() 

#grid.arrange(p1, p2,p3,p4, nrow = 2)

  plot(p)
 return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0e45e0e2-3af1-42b0-bdd0-d79709e7c2e3"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Subanalysis over entire sample using other subgroups (CVD, MD, RD, and age)

# Inputs:         'ch_analytic_wmeans'
# Outputs:        
# Revisions:	  05/16/2023 - Replace weekday with weekend variable
#                 05/25/2023 - Get rid of all day-of-the-week variables (not valid for this analysis)
#                 09/06/2023 - Add race to list of subgroups
#==============================================================================*/

other_sub_short <- function(ch_analytic_wmeans) {
ch_analytic_wmeans1<-as.data.frame(ch_analytic_wmeans)

	fit_model <- function(var,data) {
		ch_analytic_wmeans2<-subset(data,!is.na(case)&!is.na(get(var))&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE))
		ch_analytic_wmeans2$CBSA_CODE<-as.character(ch_analytic_wmeans2$CBSA_CODE)
		ch_analytic_wmeans2 <- ch_analytic_wmeans2 %>%
		  rowwise() %>%
		  # Add fake binary conditions
		  mutate(category = paste0(CVD, "_", MD, "_", RD, "_", age_gte65,"_",race_white,"_",race_black,"_",race_api,"_",race_aian,"_",race_other,"_",race_null))

		CBSA_CODEs<-unique(as.character(ch_analytic_wmeans2$CBSA_CODE))

		# Create dataframe with one row for each combination of variables we want to create a subgroup for
		df_combinations <- 
		  expand_grid(  var_name = c("CVD", "MD", "RD","age_gte65","race_white","race_black","race_api","race_aian","race_other","race_null"),
						condition = c(0, 1)) %>%
		  as.data.frame() %>%
		  mutate(subgroup = paste0(var_name, "_", condition))

		# str(df_combinations)

		# Split dataframe into list of one-row dataframes
		list_combinations <- split(df_combinations, f = df_combinations$subgroup)

		# str(list_combinations)

		# Set minimum number of observations
		min_obs <- 50

		# Create list of dataframes, each with clogit results for one subgroup
		ans2<-lapply(list_combinations, function(x){
		  x <- x %>%
			mutate(var_name_copy = var_name) %>%
			pivot_wider(c(var_name), names_from = var_name_copy, values_from = condition)
		  # Merge so that only data for one subgroup remains
		  data_new <- left_join(x, ch_analytic_wmeans2, by = c( x$var_name))
		  if(nrow(subset(data_new,case==1)) >= min_obs){
                  data_new1<-subset(data_new,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))

			  cases<-sum(data_new1$case)
			formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d+days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
			mod.adj<-clogit(formula=formula, data=data_new, method="efron")
			#mod.adj<-clogit(case~pm25_21d+MeanTemp_C+Precip+DewPoint+as.factor(weekday)+new_cases_7d+days_from_1st_case_cbsa+strata(person_id),data=data_new)
			summary<-summary(mod.adj)
			result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
			result<-as.data.frame(result)
			result$n<-mod.adj$n
			result$cases<-cases
            result$AIC<-extractAIC(mod.adj)[2]
            result$BIC<-BIC(mod.adj)
			#result$Denom<-x$CBSA_CODE
			result <- result %>% 
			  rename("se_coef" = "se(coef)",
					 "p_value" = "Pr(>|z|)",
					 "exp_coef" = "exp(coef)",
					 "exp_neg_coef" = "exp(-coef)",
					 "lower" = "lower .95",
					 "upper" = "upper .95",
                     "robust_se" = "robust se")
					  result <- mutate(result, var = var)
			return(result)
		  } else {
			return(NULL)
		  }
		  
		})

		# str(ans2)

		# names(ans2)<-races
		test<-do.call(rbind.data.frame, ans2) %>%
		  rownames_to_column("subgroup") %>%
		  mutate(subgroup = str_remove(subgroup, "\\..*"))
		  return(test)
		  
		} # end function
		
	var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
	results_list <- lapply(var_list, function(var) fit_model(var, ch_analytic_wmeans1))

	# Combine results for all variables in the list
	results1 <- bind_rows(results_list)

    # Remove racial categories = 0
    results<-subset(results1,!subgroup %in% c("race_white_0","race_black_0","race_api_0","race_aian_0","race_other_0","race_null_0"))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b08b4d3b-2593-47b4-9c3f-ef32d7f57922"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  05/25/2023
# Purpose:        Subanalysis over entire sample using other subgroups (CVD, MD, RD, and age)

# Inputs:         'ch_analytic_wmeans'
# Outputs:        'other_sub_short_v2'
# Revisions:	  12/5/2025 - This is a slightly edited version of 'other_sub_short' where natural splines are used for all continuous adjustment variables (except for precipitation)
#==============================================================================*/

other_sub_short_v2 <- function(ch_analytic_wmeans) {
ch_analytic_wmeans1<-as.data.frame(ch_analytic_wmeans)

	fit_model <- function(var,data) {
		ch_analytic_wmeans2<-subset(data,!is.na(case)&!is.na(get(var))&!is.na(Precip)&!is.na(new_cases_7d)&!is.na(CBSA_CODE))
		ch_analytic_wmeans2$CBSA_CODE<-as.character(ch_analytic_wmeans2$CBSA_CODE)
		ch_analytic_wmeans2 <- ch_analytic_wmeans2 %>%
		  rowwise() %>%
		  # Add fake binary conditions
		  mutate(category = paste0(CVD, "_", MD, "_", RD, "_", age_gte65,"_",race_white,"_",race_black,"_",race_api,"_",race_aian,"_",race_other,"_",race_null))

		CBSA_CODEs<-unique(as.character(ch_analytic_wmeans2$CBSA_CODE))

		# Create dataframe with one row for each combination of variables we want to create a subgroup for
		df_combinations <- 
		  expand_grid(  var_name = c("CVD", "MD", "RD","age_gte65","race_white","race_black","race_api","race_aian","race_other","race_null"),
						condition = c(0, 1)) %>%
		  as.data.frame() %>%
		  mutate(subgroup = paste0(var_name, "_", condition))

		# str(df_combinations)

		# Split dataframe into list of one-row dataframes
		list_combinations <- split(df_combinations, f = df_combinations$subgroup)

		# str(list_combinations)

		# Set minimum number of observations
		min_obs <- 50

		# Create list of dataframes, each with clogit results for one subgroup
		ans2<-lapply(list_combinations, function(x){
		  x <- x %>%
			mutate(var_name_copy = var_name) %>%
			pivot_wider(c(var_name), names_from = var_name_copy, values_from = condition)
		  # Merge so that only data for one subgroup remains
		  data_new <- left_join(x, ch_analytic_wmeans2, by = c( x$var_name))
		  if(nrow(subset(data_new,case==1)) >= min_obs){
                  data_new1<-subset(data_new,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))

			  cases<-sum(data_new1$case)
			formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + Precip + ns(DewPoint,df=3) + ns(new_cases_7d,df=3)+ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
			mod.adj<-clogit(formula=formula, data=data_new, method="efron")
			#mod.adj<-clogit(case~pm25_21d+MeanTemp_C+Precip+DewPoint+as.factor(weekday)+new_cases_7d+days_from_1st_case_cbsa+strata(person_id),data=data_new)
			summary<-summary(mod.adj)
			result<-cbind(rownames(summary$coefficients),summary$coefficients[,-2],summary$conf.int)
			result<-as.data.frame(result)
			result$n<-mod.adj$n
			result$cases<-cases
            result$AIC<-extractAIC(mod.adj)[2]
            result$BIC<-BIC(mod.adj)
			#result$Denom<-x$CBSA_CODE
			result <- result %>% 
			  rename("se_coef" = "se(coef)",
					 "p_value" = "Pr(>|z|)",
					 "exp_coef" = "exp(coef)",
					 "exp_neg_coef" = "exp(-coef)",
					 "lower" = "lower .95",
					 "upper" = "upper .95",
                     "robust_se" = "robust se")
					  result <- mutate(result, var = var)
			return(result)
		  } else {
			return(NULL)
		  }
		  
		})

		# str(ans2)

		# names(ans2)<-races
		test<-do.call(rbind.data.frame, ans2) %>%
		  rownames_to_column("subgroup") %>%
		  mutate(subgroup = str_remove(subgroup, "\\..*"))
		  return(test)
		  
		} # end function
		
	var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
	results_list <- lapply(var_list, function(var) fit_model(var, ch_analytic_wmeans1))

	# Combine results for all variables in the list
	results1 <- bind_rows(results_list)

    # Remove racial categories = 0
    results<-subset(results1,!subgroup %in% c("race_white_0","race_black_0","race_api_0","race_aian_0","race_other_0","race_null_0"))

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ad349700-9308-456d-97e1-8cf93a0b4f48"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  12/21/2022
# Purpose:        Subanalysis by RUCA designation. Save output with an identifier for RUCA type

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'ruca_sub_short'
# Revisions:	  1/30/2023 - Work in check on number of obs per strata to main analytic program
#                 3/17/2023 - Adding number of cases to export of regression estimates
#                 4/19/2023 - Adding outer loop so I can run through multiple main exposures (21-day, 5-day, 2-day PM2.5)
#                 5/11/2023 - Replacing day of the week with weekend indicator (for stability) 
#                 5/25/2023 - Getting rid of day-of-the-week variables (not valid for this analysis)
#==============================================================================*/

ruca_sub_short <- function(ch_analytic_wmeans) {
##First need to identify CBSAs that have 50+ valid cases
data_new <- as.data.frame(ch_analytic_wmeans)

fit_model <- function(var, data) {
  data_new1 <- subset(data, case == 1 & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(RUCA2_label))
  data_new2 <- data_new1 %>% 
    group_by(RUCA2_label) %>% 
    mutate(ruca_obs = sum(case, na.rm = TRUE))
  ruca_sub <- subset(data_new2, ruca_obs >= 50)
  rucas <- unique(ruca_sub$RUCA2_label)
  
  ans <- lapply(rucas, function(x) {
    data_sub <- subset(data, RUCA2_label == x & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(RUCA2_label))
    data_sub1<-subset(data_sub,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    cases <- sum(data_sub1$case)
    formula <- as.formula(paste("case ~", var, "+ MeanTemp_C + Precip + DewPoint + new_cases_7d + days_from_1st_case_cbsa + strata(macrovisit_id)+ cluster(person_id)"))
    mod.adj <- clogit(formula = formula, data = data_sub, method="efron")
    summary <- summary(mod.adj)
    result <- cbind(rownames(summary$coefficients), summary$coefficients[,-2], summary$conf.int)
    result <- as.data.frame(result)
    result$n <- mod.adj$n
    result$cases <- cases
    result$Denom <- x
    result$AIC<-extractAIC(mod.adj)[2]
    result$BIC<-BIC(mod.adj)
    result <- result %>% 
      rename("se_coef" = "se(coef)",
             "p_value" = "Pr(>|z|)",
             "exp_coef" = "exp(coef)",
             "exp_neg_coef" = "exp(-coef)",
             "lower" = "lower .95",
             "upper" = "upper .95",
             "robust_se" = "robust se")
    result <- mutate(result, var = var)
    return(result)
  })
  names(ans) <- rucas
  final <- do.call(rbind.data.frame, ans)
  return(final)
} ##end function

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data_new))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.26d865b6-77b8-440a-a539-4d1c929dd314"),
    ch_analytic_wmeans=Input(rid="ri.foundry.main.dataset.d10ea9e9-1ef4-4ad7-9f74-3e2fe4ddb052")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        12/21/2022
# Last modified:  12/21/2022
# Purpose:        Subanalysis by RUCA designation. Save output with an identifier for RUCA type

# Inputs:         'ch_analytic_wmeans', 'cbsacounts'
# Outputs:        'ruca_sub_short_v2'
# Revisions:	  12/5/2025 - This is a slightly edited version of 'ruca_sub_short' where natural splines are used for all continuous adjustment variables (except for precipitation)
#==============================================================================*/

ruca_sub_short_v2 <- function(ch_analytic_wmeans) {
##First need to identify CBSAs that have 50+ valid cases
data_new <- as.data.frame(ch_analytic_wmeans)

fit_model <- function(var, data) {
  data_new1 <- subset(data, case == 1 & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(RUCA2_label))
  data_new2 <- data_new1 %>% 
    group_by(RUCA2_label) %>% 
    mutate(ruca_obs = sum(case, na.rm = TRUE))
  ruca_sub <- subset(data_new2, ruca_obs >= 50)
  rucas <- unique(ruca_sub$RUCA2_label)
  
  ans <- lapply(rucas, function(x) {
    data_sub <- subset(data, RUCA2_label == x & !is.na(get(var)) & !is.na(Precip) & !is.na(new_cases_7d) & !is.na(RUCA2_label))
    data_sub1<-subset(data_sub,!is.na(get(var))&!is.na(MeanTemp_C)&!is.na(Precip)&!is.na(DewPoint)&!is.na(new_cases_7d)&!is.na(days_from_1st_case_cbsa))
    cases <- sum(data_sub1$case)
    formula <- as.formula(paste("case ~", var, "+ ns(MeanTemp_C,df=3) + Precip + ns(DewPoint,df=3) + ns(new_cases_7d,df=3) + ns(days_from_1st_case_cbsa,df=3) + strata(macrovisit_id)+ cluster(person_id)"))
    mod.adj <- clogit(formula = formula, data = data_sub, method="efron")
    summary <- summary(mod.adj)
    result <- cbind(rownames(summary$coefficients), summary$coefficients[,-2], summary$conf.int)
    result <- as.data.frame(result)
    result$n <- mod.adj$n
    result$cases <- cases
    result$Denom <- x
    result$AIC<-extractAIC(mod.adj)[2]
    result$BIC<-BIC(mod.adj)
    result <- result %>% 
      rename("se_coef" = "se(coef)",
             "p_value" = "Pr(>|z|)",
             "exp_coef" = "exp(coef)",
             "exp_neg_coef" = "exp(-coef)",
             "lower" = "lower .95",
             "upper" = "upper .95",
             "robust_se" = "robust se")
    result <- mutate(result, var = var)
    return(result)
  })
  names(ans) <- rucas
  final <- do.call(rbind.data.frame, ans)
  return(final)
} ##end function

var_list <- c("pm25_21d", "pm25_5d", "pm25_2d")
results_list <- lapply(var_list, function(var) fit_model(var, data_new))

# Combine results for all variables in the list
results <- bind_rows(results_list)

        return(results)

}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.35646337-a548-4953-bbbc-e5fd8ad2a19e"),
    table_1_prep=Input(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        03/15/2023
# Last modified:  03/15/2023
# Purpose:        Creating a general table one for study manuscript based on final 
#                   analytic sample (to be directly exported)
#
# Inputs:         'table_1_prep'
# Outputs:        'table1_output_allchars'
#
# Revisions:       6/15/2023 - Make 3 tables, one with overall characteristics compared between included exclude, one with and without ZCTA, one with ZCTA but without PM2.5. 
#                   2/3/2025 - Note that during interim period between manuscript submission and revision request, the R function CreateTableOne is no longer supported, therefore
#                               this program will no longer run and descriptive statistics table would need to be recreated with other R function if someone wanted to update
#                               this table
#                               
# ==============================================================================

table1_output_allchars <- function(table_1_prep) {

    ## First limit sample to 2020
     data<-as.data.frame(table_1_prep)

     data3<-subset(data,has_ZCTA=='Has ZCTA')

    options(width=1000)

    ##Table 1 Included/Excluded
    all_variables_in_table1_rows <- c("age_at_pos","age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "Precip", "MeanTemp_C", "MeanTemp_F", "DewPoint", "poverty_status", "MDs", "weekday", "pm25", "pm25_21d","pm25_5d","pm25_2d","new_cases_7d","days_from_1st_case_cbsa","CENSUS_REGION_LABEL","RUCA2_label","education_lt_hs_diploma","education_hs_diploma","education_some_college","education_associate_degree","education_bachelors_degree","education_graduate_or_professional_degree", "education_HSplus","education_BAplus","has_ZCTA","has_pollution_data")
    categorical_variables_in_table1_rows <- c("age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday","CENSUS_REGION_LABEL","RUCA2_label","has_ZCTA","has_pollution_data" )

    ##Table 2 ZCTA/no ZCTA
    all_variables_in_table2_rows <- c("age_at_pos","age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday")
    categorical_variables_in_table2_rows <- c("age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday" )

    ##Table 3 PM2.5 no PM2.5
    all_variables_in_table3_rows <- c("age_at_pos","age_categ", "gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "Precip", "MeanTemp_C", "MeanTemp_F", "DewPoint", "poverty_status", "MDs", "weekday", "new_cases_7d","days_from_1st_case_cbsa","CENSUS_REGION_LABEL","RUCA2_label","education_lt_hs_diploma","education_hs_diploma","education_some_college","education_associate_degree","education_bachelors_degree","education_graduate_or_professional_degree", "education_HSplus","education_BAplus")
    categorical_variables_in_table3_rows <- c("age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday","CENSUS_REGION_LABEL","RUCA2_label" )

    table_1 <- CreateTableOne(vars = all_variables_in_table1_rows, strata="final_sample", data = data, factorVars = categorical_variables_in_table1_rows)

    df1 = as.data.frame(print(table_1, smd = TRUE,  explain = FALSE, pDigits = 4,catDigits = 1,
  contDigits = 1, formatOptions = list(big.mark = ","))) %>% rownames_to_column("Name")

    drops <- c("test")

    df1 <- df1[ , !(names(df1) %in% drops)]
    df1$table <- "included_excluded"

    table_2 <- CreateTableOne(vars = all_variables_in_table2_rows, strata="alt_has_ZCTA", data = data, factorVars = categorical_variables_in_table2_rows)

    df2 = as.data.frame(print(table_2, smd = TRUE,  explain = FALSE, pDigits = 4,catDigits = 1,
  contDigits = 1, formatOptions = list(big.mark = ","))) %>% rownames_to_column("Name")

    drops <- c("test")

    df2 <- df2[ , !(names(df2) %in% drops)]
    df2$table <- "ZCTA_noZCTA"

        table_3 <- CreateTableOne(vars = all_variables_in_table3_rows, strata="alt_has_pollution_data", data = data3, factorVars = categorical_variables_in_table3_rows)

    df3 = as.data.frame(print(table_3, smd = TRUE,  explain = FALSE, pDigits = 4,catDigits = 1,
  contDigits = 1, formatOptions = list(big.mark = ","))) %>% rownames_to_column("Name")

    drops <- c("test")

    df3 <- df3[ , !(names(df3) %in% drops)]
    df3$table <- "pm_nopm"

    ##For gender, missing/no-concept/unknown is essential zero and can't be exported because of low numbers so just delete
    df<-rbind(df1,df2,df3)

    return(df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e4e176f7-6613-4dbe-a2df-9a34755f59c3"),
    table_1_prep=Input(rid="ri.foundry.main.dataset.cfbe85ef-e71a-4a35-bdd8-f58d694a427c")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        03/15/2023
# Last modified:  03/15/2023
# Purpose:        Creating a general table one for study manuscript based on final 
#                   analytic sample (to be directly exported)
#
# Inputs:         'ch_final_elig'
# Outputs:         table1_output_general
#
# Revisions:       Make 3 tables, one with overall characteristics compared between included exclude, one with and without ZCTA, one with ZCTA but without PM2.5
#                   2/3/2025 - Note that during interim period between manuscript submission and revision request, the R function CreateTableOne is no longer supported, therefore
#                               this program will no longer run and descriptive statistics table would need to be recreated with other R function if someone wanted to update
#                               this table
# ==============================================================================

table1_output_allchars_non_normal <- function(table_1_prep) {

    ## First limit sample to 2020
     data<-as.data.frame(table_1_prep)

     data3<-subset(data,has_ZCTA=='Has ZCTA')

    options(width=1000)

    ##Table 1 Included/Excluded
    all_variables_in_table1_rows <- c("age_at_pos","age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "Precip", "MeanTemp_C", "MeanTemp_F", "DewPoint", "poverty_status", "MDs", "weekday", "pm25", "pm25_21d","pm25_5d","pm25_2d","new_cases_7d","days_from_1st_case_cbsa","CENSUS_REGION_LABEL","RUCA2_label","education_lt_hs_diploma","education_hs_diploma","education_some_college","education_associate_degree","education_bachelors_degree","education_graduate_or_professional_degree", "education_HSplus","education_BAplus","has_ZCTA","has_pollution_data")
    categorical_variables_in_table1_rows <- c("age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday","CENSUS_REGION_LABEL","RUCA2_label","has_ZCTA","has_pollution_data" )

getmedians<-c("pm25", "pm25_21d","pm25_5d","pm25_2d")
    ##Table 2 ZCTA/no ZCTA
    all_variables_in_table2_rows <- c("age_at_pos","age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday")
    categorical_variables_in_table2_rows <- c("age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday" )

    ##Table 3 PM2.5 no PM2.5
    all_variables_in_table3_rows <- c("age_at_pos","age_categ", "gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "Precip", "MeanTemp_C", "MeanTemp_F", "DewPoint", "poverty_status", "MDs", "weekday", "new_cases_7d","days_from_1st_case_cbsa","CENSUS_REGION_LABEL","RUCA2_label","education_lt_hs_diploma","education_hs_diploma","education_some_college","education_associate_degree","education_bachelors_degree","education_graduate_or_professional_degree", "education_HSplus","education_BAplus")
    categorical_variables_in_table3_rows <- c("age_categ","gender_3categ", "race_categ", "ethnicity_3categ","CVD", "RD", "MD","died_during_hosp", "age_gte65", "weekday","CENSUS_REGION_LABEL","RUCA2_label" )

    table_1 <- CreateTableOne(vars = all_variables_in_table1_rows, strata="final_sample", data = data, factorVars = categorical_variables_in_table1_rows)

    df1 = as.data.frame(print(table_1, smd = TRUE, nonnormal =getmedians, explain = FALSE, pDigits = 4,catDigits = 1,
  contDigits = 1, formatOptions = list(big.mark = ","))) %>% rownames_to_column("Name")

    drops <- c("test")

    df1 <- df1[ , !(names(df1) %in% drops)]
    df1$table <- "included_excluded"

    table_2 <- CreateTableOne(vars = all_variables_in_table2_rows, strata="alt_has_ZCTA", data = data, factorVars = categorical_variables_in_table2_rows)

    df2 = as.data.frame(print(table_2, smd = TRUE,  explain = FALSE, pDigits = 4,catDigits = 1,
  contDigits = 1, formatOptions = list(big.mark = ","))) %>% rownames_to_column("Name")

    drops <- c("test")

    df2 <- df2[ , !(names(df2) %in% drops)]
    df2$table <- "ZCTA_noZCTA"

        table_3 <- CreateTableOne(vars = all_variables_in_table3_rows, strata="alt_has_pollution_data", data = data3, factorVars = categorical_variables_in_table3_rows)

    df3 = as.data.frame(print(table_3, smd = TRUE,  explain = FALSE, pDigits = 4,catDigits = 1,
  contDigits = 1, formatOptions = list(big.mark = ","))) %>% rownames_to_column("Name")

    drops <- c("test")

    df3 <- df3[ , !(names(df3) %in% drops)]
    df3$table <- "pm_nopm"

    ##For gender, missing/no-concept/unknown is essential zero and can't be exported because of low numbers so just delete
    df<-rbind(df1,df2,df3)

    return(df)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9fdf8434-35ba-4b7e-af05-d87df3c5f184"),
    table1_output_allchars=Input(rid="ri.foundry.main.dataset.35646337-a548-4953-bbbc-e5fd8ad2a19e")
)
# ==========================================================================
# Author:         Alyssa Platt (alyssa.platt@duke.edu)
# Project:        N3C Case-Crossover study
# Created:        03/15/2023
# Last modified:  03/15/2023
# Purpose:        Simple adjustment to the Table 1 output that removes counts of missing/unknown results, which honest brokers didn't like since some counts are <20. Making this it's own program because I prefer we have the original numbers *somewhere*
#
# Inputs:         'table1_output_allchars'
# Outputs:        'table1_output_allchars_obscure'
#
# Revisions:       
# ==============================================================================

table1_output_allchars_obscure <- function(table1_output_allchars) {
    df<-subset(table1_output_allchars,Name!='   No matching concept, missing, or unknown')
    return(df)
}

