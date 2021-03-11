zrequire('timeDate')
require(readxl)
require(ggplot2)
require(dplyr)
require(lubridate)
require(naniar)
require(caret)
require(lattice)
require(hexbin)
require(corrplot)
require(scales)
require(tidyverse)
library(Dict)
require(caTools)
require(gbm)
require(TTR)
library(reshape)
library(timeDate)
####### Spark
#R SPARK
library(sparklyr)
library(usethis)
library(devtools)
#open arrow
library(arrow)




df_infra<-infra

##reshape Infra
df_infra$month<-month(as.POSIXlt(df_infra$Date, format="%Y/%m/%d"))
df_infra$day<-day(as.POSIXlt(df_infra$Date, format="%Y/%m/%d"))

##fuel dataframe

df_gascoal<-df_infra%>%filter(Fuel=="Coal"|Fuel=="Natural Gas")
df_gascoal$Company<-factor(df_gascoal$Company)
df_gascoal$month<-factor(df_gascoal$month)
df_gascoal$day<-factor(df_gascoal$day)
df_gascoal$Hour<-factor(df_gascoal$Hour)
df_gascoal$PP<-factor(df_gascoal$PP)

#binary fuel
df_gascoal <- df_gascoal %>%
  mutate(coal = ifelse(Fuel == "Coal", 1, 0),
         gas = ifelse(Fuel == "Natural Gas", 1, 0))


# option 1
#exclude all bids of zero and use only accepted/ matched bids, 
#then take the mean of
#these bids to get one observation per plant and hour

df_gascoal<-df_gascoal %>% filter(Price!=0)

dat_coal<-df_gascoal  %>% filter(coal==1)

dat_coal<-data.frame(dat_coal%>%select(Company,PP,coal,gas,month,day,
                                       Hour,Date,Price,Energy,MC,Clearing)%>%
                       group_by(Company,PP,month,day,Hour,coal,gas)%>%
                       summarise(  Price=mean(Price,na.rm=TRUE),
                                   Energy=mean(Energy,na.rm=TRUE),
                                   Clearing=mean(Clearing,na.rm=TRUE),
                                   MC=mean(MC,na.rm=TRUE)))


# option 1 
#exclude all bids of zero and use only accepted/ matched bids, 
#then take the mean of
#these bids to get one observation per plant and hour

df_gascoal<-df_gascoal %>% filter(Price!=0)

dat_gas<-df_gascoal  %>% filter(gas==1)

dat_gas<-data.frame(dat_gas%>%select(Company,PP,coal,gas,month,day,
                                     Hour,Date,Price,Energy,MC,Clearing)%>%
                      group_by(Company,PP,month,day,Hour,coal,gas)%>%
                      summarise(  Price=mean(Price,na.rm=TRUE),
                                  Energy=mean(Energy,na.rm=TRUE),
                                  Clearing=mean(Clearing,na.rm=TRUE),
                                  MC=mean(MC,na.rm=TRUE)))







#######FUNCTION 
#functions
func_dfcreate<-function(row,hour){
  y<- data.frame(matrix(ncol = hour,nrow = row))
  x<- list()
  for (i in 1:(hour))
  {x<-append(x,paste("pr",i, sep = "_") )}
  colnames(y)<- x
  y
}

namedf<- func_dfcreate(1,24)
namedf$company<-NA
namedf$PP<-NA
namedf$month<-NA
namedf$day<-NA
namedf$Mc<-NA
namedf<-namedf[,c(c(25:29),1:24)]


ndf<-namedf


res_df<-data.frame(matrix(ncol=29))
colnames(res_df)<-colnames(namedf)

for ( c in as.character(unique(dat_coal$Company)) ){
  f_df1<-dat_coal %>%filter(Company==c)
  
  for ( p in as.character(unique(f_df1$PP)) ){
    f_df2<-f_df1 %>%filter(PP==p)
    
    for ( m in as.numeric(unique(dat_coal$month)  ) ){
      df<-data.frame(matrix(ncol=29))
      colnames(df)<-colnames(ndf)
      
      df_d<-f_df2%>%filter(month==m)
      
      for ( d in 1:max(as.numeric(dat_coal[dat_coal$month==m,]$day)) ){
        df_cd<-df_d %>%filter(day==d)
        for ( i in 1:24){
          df[d,(i+5)]<-df_cd[i,8]
          df[d,5]<-mean(df_cd[,11],na.rm=TRUE)
          df[d,1]<-c
          df[d,2]<-p
          df[d,3]<-m
          df[d,4]<-d

        }
      }
      res_df<-rbind(res_df,df)
    }
  }
} 

#daily res df (drop Na caused by codes ( not data ))
df_day_coal<-res_df %>% filter(!is.na(company))

## NA ANALISYS

na_day_row_coal <-df_day_coal[!complete.cases(df_day_coal),] 
not_na_day_coal<-df_day_coal[complete.cases(df_day_coal),]

summary_not_na_day_coal <- as.data.frame(unique(not_na_day_coal %>% group_by(company)%>%
                                                  mutate( not_na=n())%>% 
                                                  select(company,not_na)))

summary_na_day_coal <- as.data.frame(unique(na_day_row_coal %>% group_by(company)%>%
                                              mutate( na=n())%>% 
                                              select(company,na)))


summary_rate_coal <- merge( summary_not_na_day_coal,summary_na_day_coal, by = 'company')
summary_rate_coal <-summary_rate_coal%>%mutate(na_rate=na/(na+not_na))



### 

func_dfcreate_day<-function(row,day){
  hourname<-func_dfcreate(1,24)
  y<- data.frame(matrix(ncol = day*24,nrow = row))
  x<- list()
  for (i in 1:(day))
  {x<-append(x,paste(colnames(hourname),i, sep = "-") )}
  colnames(y)<- x
  y
}

df_day_coal$index<-NULL

day_frame <-function (day){
  df_day_frame_1<- data.frame(matrix(ncol =29 ,nrow =1 ))
  colnames(df_day_frame_1)<- colnames(df_day_coal)
  df_day_frame_2<-func_dfcreate_day(1,day)
  df_day_frame<-cbind(df_day_frame_1,df_day_frame_2)
}


###############################################################################



day=10
df_day_window<-day_frame(day)
df_company_coal<-data.frame(matrix(ncol=ncol(df_day_window)))
colnames(df_company_coal)<-colnames(df_day_window)



for ( c in as.character(unique(df_day_coal$company)) ){
  f_df1<-df_day_coal %>%filter(company==c)
  
  for ( p in as.character(unique(f_df1$PP)) ){
    f_df2<-f_df1 %>%filter(PP==p)
    f_df2$index<-1:nrow(f_df2)
    
    df_day_window<-day_frame(day)
    df_ml<-data.frame(matrix(ncol=ncol(df_day_window)))
    colnames(df_ml)<-colnames(df_day_window)

    for ( f in (day+1):365){
      df_ml[f,1:(ncol(f_df2)-1)]<-f_df2[f_df2$index==f,1:29]
    }
    
    df_ml<-df_ml[-c(1:(day)),] #drop na values
    
    for(r in 1:nrow(df_ml) ) {
      
      for(i in  1:day ){
        
        df_ml[r,(24*(i)+6):(24*(i)+29)]<-f_df2[f_df2$index==(r+day-i),6:29]
      }
    }
    df_company_coal<-rbind(df_company_coal,df_ml)
  }
}



################################## NA ANALYSIS


#### STEP 1 

###CONVERT NAN VALUE TO NA
df_company_coal$Mc <- sapply(df_company_coal$Mc, function(x) ifelse(is.nan(x), NA, x))

df_coal_MC_NA_ <-  data.frame (df_company_coal[is.na(df_company_coal$Mc),])

##DROP ROwS THAT marjinal cost == NA
df_company_coal<-  df_company_coal[complete.cases(df_company_coal$Mc),]

df_na_company_coal<- df_company_coal[!complete.cases(df_company_coal),]

#na rate  
nrow(df_na_company_coal)/ nrow(df_company_coal)

# ROW MEAN TO NA VALUES 

df_company_coal$pr_mean<-rowMeans(df_company_coal[,c(6:ncol(df_company_coal) )],na.rm = TRUE)
df_company_coal$index<-1:nrow(df_company_coal)


#ASSIGN ROW MEAN TO NA VALUES IN A ROW
for ( i in df_company_coal[!complete.cases(df_company_coal),]$index ) {
  for( r in 6:ncol(df_company_coal[!complete.cases(df_company_coal),]) ) {
    if(is.na(df_company_coal[i,r])){ df_company_coal[i,r]<-df_company_coal[i,"pr_mean"]
    }else{df_company_coal[i,r]<-df_company_coal[i,r]}
    
  }
}


######################################### FEATURES

#adding clearing price

df_clr_coal <-unique(dat_coal%>%select(month,day,Hour,Clearing))

df_clr_coal <-as.data.frame( unique(df_clr_coal %>%group_by(month,day) %>%
                                      mutate(
                                        min_clr=min(Clearing),
                                        mean_clr=mean(Clearing),
                                        max_clr=max(Clearing),
                                      )%>%select(-Hour,-Clearing )))

df_ml_company_coal<-  merge(x=df_company_coal,y=df_clr_coal,by=c("month","day"),all.x=TRUE)



#seasons
df_ml_company_coal$seasons<-NA
df_ml_company_coal<-df_ml_company_coal %>% mutate( seasons = replace(df_ml_company_coal$seasons,
                                                                     df_ml_company_coal$month==12|
                                                                       df_ml_company_coal$month==1|
                                                                       df_ml_company_coal$month==2,"wint"))
df_ml_company_coal<-df_ml_company_coal %>% mutate( seasons = replace(df_ml_company_coal$seasons,
                                                                     df_ml_company_coal$month==3|
                                                                       df_ml_company_coal$month==4|
                                                                       df_ml_company_coal$month==5,"spr"))
df_ml_company_coal<-df_ml_company_coal %>% mutate( seasons = replace(df_ml_company_coal$seasons,
                                                                     df_ml_company_coal$month==6|
                                                                       df_ml_company_coal$month==7|
                                                                       df_ml_company_coal$month==8,"sum"))
df_ml_company_coal<-df_ml_company_coal %>% mutate( seasons = replace(df_ml_company_coal$seasons,
                                                                     df_ml_company_coal$month==9|
                                                                       df_ml_company_coal$month==10|
                                                                       df_ml_company_coal$month==11,"aut"))

#day type
#Adding date
df_ml_company_coal$year<-2018
df_ml_company_coal$date <- as.Date(with(df_ml_company_coal, paste(year, month, day,sep="-")), "%Y-%m-%d")
df_ml_company_coal$date<-as.Date(df_ml_company_coal$date)
df_ml_company_coal$year<-NULL


#Spain national holiday file downloaded from internet
#### attention 2 types of holiday 
holiday <- read_excel("holiday18.xlsx")
holiday$holiday<-as.Date(holiday$holiday)
df_ml_company_coal$daytype<-NA

#df_gascoal[!(df_gascoal$Date  %in%  holiday$holiday),] //the code drops spain holiday

#assign bussiness day to day type
df_ml_company_coal<- df_ml_company_coal%>% mutate( daytype = replace(df_ml_company_coal$daytype,
                                                                     !(df_ml_company_coal$date  %in%  holiday$holiday) & isBizday( as.timeDate(df_ml_company_coal$date)),
                                                                     "wrkday"))
#assing holiday value to reaming NA values as "hlday"

df_ml_company_coal<- df_ml_company_coal%>% mutate( daytype = replace(df_ml_company_coal$daytype,
                                                                     is.na(df_ml_company_coal$daytype),
                                                                     "hlday"))



#### Convert  CATegoriCAL VARIBALE TO  Binary  var
binary_df_ml_company_coal<- df_ml_company_coal %>%
  mutate(
    winter = ifelse(seasons == "wint", 1, 0),
    summer = ifelse(seasons == "sum", 1, 0),
    autumn = ifelse(seasons == "aut", 1, 0),
    spring = ifelse(seasons == "spr", 1, 0),
    
    workday = ifelse(daytype == "wrkday", 1, 0),
    holiday = ifelse(daytype == "hlday", 1, 0) )


### DROP NOT  RELEVANT COLUMNS


# binary_df_ml_company_coal$date<-NULL
binary_df_ml_company_coal$seasons<-NULL
binary_df_ml_company_coal$daytype<-NULL

binary_df_ml_company_coal$index<-NULL
binary_df_ml_company_coal$pr_mean<-NULL
binary_df_ml_company_coal$daytype<-NULL

####################################################
#######################################################
####################################################################
####################################################################################
#################################################################################################





namedf<- func_dfcreate(1,24)
namedf$company<-NA
namedf$PP<-NA
namedf$month<-NA
namedf$day<-NA
namedf$Mc<-NA
namedf<-namedf[,c(c(25:29),1:24)]


ndf<-namedf

res_df<-data.frame(matrix(ncol=29))
colnames(res_df)<-colnames(namedf)

for ( c in as.character(unique(dat_gas$Company)) ){
  f_df1<-dat_gas %>%filter(Company==c)
  
  for ( p in as.character(unique(f_df1$PP)) ){
    f_df2<-f_df1 %>%filter(PP==p)
    
    for ( m in as.numeric(unique(dat_gas$month)  ) ){
      df<-data.frame(matrix(ncol=29))
      colnames(df)<-colnames(ndf)
      
      df_d<-f_df2%>%filter(month==m)
      
      for ( d in min(as.numeric(dat_gas[dat_gas$month==m,]$day)):max(as.numeric(dat_gas[dat_gas$month==m,]$day)) ){
        df_cd<-df_d %>%filter(day==d)
        for ( i in 1:24){
          
          df[d,(i+5)]<-df_cd[i,8]
          df[d,5]<-mean(df_cd[,11],na.rm=TRUE)
          df[d,1]<-c
          df[d,2]<-p
          df[d,3]<-m
          df[d,4]<-d

        }
      }
      res_df<-rbind(res_df,df)
    }
  }
} 


#daily res df (drop Na caused by codes ( not data ))
df_day_gas<-res_df %>% filter(!is.na(company))

## NA ANALISYS

na_day_row_gas <-df_day_gas[!complete.cases(df_day_gas),] 
not_na_day_gas<-df_day_gas[complete.cases(df_day_gas),]

summary_not_na_day_gas <- as.data.frame(unique(not_na_day_gas %>% group_by(company)%>%
                                                 mutate( not_na=n())%>% 
                                                 select(company,not_na)))

summary_na_day_gas <- as.data.frame(unique(na_day_row_gas %>% group_by(company)%>%
                                             mutate( na=n())%>% 
                                             select(company,na)))


summary_rate_gas <- merge( summary_not_na_day_gas,summary_na_day_gas, by = 'company')
summary_rate_gas <-summary_rate_gas%>%mutate(na_rate=na/(na+not_na))


### 

df_day_gas$index<-NULL



###############################################################################



df_day_window<-day_frame(day)
df_company_gas<-data.frame(matrix(ncol=ncol(df_day_window)))
colnames(df_company_gas)<-colnames(df_day_window)



for ( c in as.character(unique(df_day_gas$company)) ){
  f_df1<-df_day_gas %>%filter(company==c)
  
  for ( p in as.character(unique(f_df1$PP)) ){
    f_df2<-f_df1 %>%filter(PP==p)
    f_df2$index<-1:nrow(f_df2)
    
    df_day_window<-day_frame(day)
    df_ml<-data.frame(matrix(ncol=ncol(df_day_window)))
    colnames(df_ml)<-colnames(df_day_window)
    
    for ( f in (day+1):365){
      df_ml[f,1:(ncol(f_df2)-1)]<-f_df2[f_df2$index==f,1:29]
    }
    
    df_ml<-df_ml[-c(1:(day)),]#drop na
    
    for(r in 1:nrow(df_ml) ) {
      
      for(i in  1:day ){
        
        df_ml[r,(24*(i)+6):(24*(i)+29)]<-f_df2[f_df2$index==(r+day-i),6:29]
      }
    }
    df_company_gas<-rbind(df_company_gas,df_ml)
  }
}



################################## NA ANALYSIS


#### STEP 1 

###CONVERT NAN VALUE TO NA
df_company_gas$Mc <- sapply(df_company_gas$Mc, function(x) ifelse(is.nan(x), NA, x))

df_gas_MC_NA_ <-  data.frame (df_company_gas[is.na(df_company_gas$Mc),])

##DROP ROwS THAT marjinal cost == NA
df_company_gas<-  df_company_gas[complete.cases(df_company_gas$Mc),]


#na rate  
df_na_company_gas<- df_company_gas[!complete.cases(df_company_gas),]

nrow(df_na_company_gas)/ nrow(df_company_gas)

# ROW MEAN TO NA VALUES 

df_company_gas$pr_mean<-rowMeans(df_company_gas[,c(6:ncol(df_company_gas) )],na.rm = TRUE)
df_company_gas$index<-1:nrow(df_company_gas)


#ASSIGN ROW MEAN TO NA VALUES IN A ROW
for ( i in df_company_gas[!complete.cases(df_company_gas),]$index ) {
  for( r in 6:ncol(df_company_gas[!complete.cases(df_company_gas),]) ) {
    if(is.na(df_company_gas[i,r])){ df_company_gas[i,r]<-df_company_gas[i,"pr_mean"]
    }else{df_company_gas[i,r]<-df_company_gas[i,r]}
    
  }
}






######################################### FEATURES


#adding clearing price

df_clr_gas <-unique(dat_gas%>%select(month,day,Hour,Clearing))

df_clr_gas <-as.data.frame( unique(df_clr_gas %>%group_by(month,day) %>%
                                     mutate(
                                       min_clr=min(Clearing),
                                       mean_clr=mean(Clearing),
                                       max_clr=max(Clearing),
                                     )%>%select(-Hour,-Clearing )))

df_ml_company_gas<-  merge(x=df_company_gas,y=df_clr_gas,by=c("month","day"),all.x=TRUE)






#seasons
df_ml_company_gas$seasons<-NA
df_ml_company_gas<-df_ml_company_gas %>% mutate( seasons = replace(df_ml_company_gas$seasons,
                                                                   df_ml_company_gas$month==12|
                                                                     df_ml_company_gas$month==1|
                                                                     df_ml_company_gas$month==2,"wint"))
df_ml_company_gas<-df_ml_company_gas %>% mutate( seasons = replace(df_ml_company_gas$seasons,
                                                                   df_ml_company_gas$month==3|
                                                                     df_ml_company_gas$month==4|
                                                                     df_ml_company_gas$month==5,"spr"))
df_ml_company_gas<-df_ml_company_gas %>% mutate( seasons = replace(df_ml_company_gas$seasons,
                                                                   df_ml_company_gas$month==6|
                                                                     df_ml_company_gas$month==7|
                                                                     df_ml_company_gas$month==8,"sum"))
df_ml_company_gas<-df_ml_company_gas %>% mutate( seasons = replace(df_ml_company_gas$seasons,
                                                                   df_ml_company_gas$month==9|
                                                                     df_ml_company_gas$month==10|
                                                                     df_ml_company_gas$month==11,"aut"))




#day type
#Adding date
df_ml_company_gas$year<-2018
df_ml_company_gas$date <- as.Date(with(df_ml_company_gas, paste(year, month, day,sep="-")), "%Y-%m-%d")
df_ml_company_gas$date<-as.Date(df_ml_company_gas$date)
df_ml_company_gas$year<-NULL




#Spain national holiday file downloaded from internet
#### attention 2 types of holiday 
holiday <- read_excel("holiday18.xlsx")
holiday$holiday<-as.Date(holiday$holiday)
df_ml_company_gas$daytype<-NA

#assign business day to day type
df_ml_company_gas<- df_ml_company_gas%>% mutate( daytype = replace(df_ml_company_gas$daytype,
                                                                   !(df_ml_company_gas$date  %in%  holiday$holiday) & isBizday( as.timeDate(df_ml_company_gas$date)),
                                                                   "wrkday"))
#assign holiday value to reaming NA values as "hlday"

df_ml_company_gas<- df_ml_company_gas%>% mutate( daytype = replace(df_ml_company_gas$daytype,
                                                                   is.na(df_ml_company_gas$daytype),
                                                                   "hlday"))



#### Convert  CATegoriCAL VARIBALE TO  Binary  var
binary_df_ml_company_gas<- df_ml_company_gas %>%
  mutate(
    winter = ifelse(seasons == "wint", 1, 0),
    summer = ifelse(seasons == "sum", 1, 0),
    autumn = ifelse(seasons == "aut", 1, 0),
    spring = ifelse(seasons == "spr", 1, 0),
    
    workday = ifelse(daytype == "wrkday", 1, 0),
    holiday = ifelse(daytype == "hlday", 1, 0) )


### DROP NOT  RELEVANT COLUMNS


# binary_df_ml_company_gas$date<-NULL
binary_df_ml_company_gas$seasons<-NULL
binary_df_ml_company_gas$daytype<-NULL

binary_df_ml_company_gas$index<-NULL
binary_df_ml_company_gas$pr_mean<-NULL
binary_df_ml_company_gas$daytype<-NULL

#merge
binary_df_ml_company_coal$coal<-1
binary_df_ml_company_coal$gas<-0

binary_df_ml_company_gas$coal<-0
binary_df_ml_company_gas$gas<-1


merged_df_ml<-rbind(binary_df_ml_company_coal,binary_df_ml_company_gas)
   

#TRAIN AND TEST
Train <-as.data.frame(merged_df_ml %>%filter(month <=7 ))
Test <-as.data.frame(merged_df_ml %>%filter(month >=11 ))

### col names 
names(Test ) <- gsub(x = names(Test), pattern = "\\-", replacement = "_")
names(Train ) <- gsub(x = names(Train), pattern = "\\-", replacement = "_")

Train$month<-NULL
Train$day<-NULL
Train$date<-NULL

Test$month<-NULL
Test$day<-NULL
Test$date<-NULL


str(Test)
ncol(Train)

### don't fort to drop date !!!!!!!!!!!!!
copy_mdfml<-merged_df_ml
# drop not relevant  columns
copy_mdfml$month<-NULL
copy_mdfml$day<-NULL
copy_mdfml$date<-NULL

df_one <-copy_mdfml
str(df_one)
df_one$PP<-factor(df_one$PP)
df_one$company<-factor(df_one$company)








ncol(merged_df_ml)


#write.csv2(df_one,"C:/Users/Mr/Desktop/New folder/new.csv",row.names=FALSE)


#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! SPARKKK!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
##df 
sc<-spark_connect(master="local")

s_tbl_test<-copy_to(sc,Test,"sparkdf_test",overwrite = TRUE)

s_tbl_train<-copy_to(sc,Train,"sparkdf_train",overwrite = TRUE)


#train test 
# tablo referanslarinin olusturulmasi


train_tbl <- s_tbl_train
test_tbl <- s_tbl_test


# Modellerin Egitilmesi
ml_formula <- formula(Mc~.)

# Models

ml_gbt <- ml_gradient_boosted_trees(train_tbl, ml_formula)

##Functions for ML

score_test_data <- function(model, data = test_tbl){
  pred <- ml_predict(model, data)
  select(pred,Mc, prediction)
}


#Score 
score<-  score_test_data(ml_gbt,test_tbl) %>%  collect()

Summary<- defaultSummary(data.frame(obs=score$Mc,
                                    pred=score$prediction))



df_score<- as.data.frame( score_test_data(ml_gbt,test_tbl) %>%  collect())

### plot predict vs observed
ggplot(df_score,aes(Mc,prediction))+geom_hex()



#Importance 

f_importance<- as.data.frame (ml_tree_feature_importance(ml_gbt))

ggplot(f_importance %>%filter(importance>0.022 ), aes(x=feature,y=importance))+geom_bar(stat = "identity",aes(fill=importance))



### Summary


list_df=list(Summary_EDP,Summary_Endesa,Summary_Engie,Summary_Cepsa,
             Summary_Iberdrola,Summary_Naturgy,Summary_REN,Summary_Viesgo)

list_company=list("EDP","Endesa","Engie" ,"Cepsa", "Iberdrola","Naturgy", "REN", "Viesgo")

S_company<-data.frame(matrix(ncol=3))
names =list("RMSE"  ,   "Rsquared" ,"MAE")
colnames(S_company) = names

for(i in list_df){
  df=i
  df <- cbind(Value = rownames(df), df)
  rownames(df) <- 1:nrow(df)
  S<-data.frame(matrix(ncol=1))
  S[,1]<-NULL
  S$RMSE=df[1,]$Summary
  S$Rsquared<-df[2,]$Summary
  S$MAE<-df[3,]$Summary
  S_company<-rbind(S_company,S)
}
S_company<-na.omit(S_company)

S_company$company<-list_company





options(scipen=999)

























































