library(table1)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.df27149e-2b90-4558-8ca8-26306cad299a"),
    county_daily_ma=Input(rid="ri.foundry.main.dataset.623e88a2-4e4c-4eb2-be46-6a3804db7bda")
)
date_first_case <- function(county_daily_ma) {
    ##Subset dataset to only those counties with positive case counts and compute the minimum date per county
    df<-subset(county_daily_ma,covid19_total_cases>0)
     df1<-df %>% 
    group_by(local_code) %>% 
    mutate(first_case_date = min(date, na.rm = T))
    df2<-df1[,c("local_code","date","first_case_date")]
    ##Merge back into full dataset
    df3<-merge(county_daily_ma,df2,by=c("local_code","date"))

    return(df3)
}

