############### Best in Ginie dashboard #############
library(dplyr)
library(doBy)
library(plyr)
library(lubridate)
library(reshape2)
library(gmodels)


############### Data filtering #####################
setwd("D:/mafoi/ekstep/works/bestingine/data/inputs")
ge_int_feb2 <- read.csv("Feb2017_GE_INTERACT.csv",header = T,stringsAsFactors = F)

#setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/Final CSVs - GE_INTERACT")
ge_int_feb1 <- read.csv("Mar2017_GE_INTERACT(1Mar17-5Mar17).csv",header = T,stringsAsFactors = F)
#ge_int_jan <- read.csv("Jan2017_GE_INTERACT.csv",header = T,stringsAsFactors = F)

############## Extracing the columns ###############
#ge_int_jan_ec <- ge_int_jan[,which(names(ge_int_jan) %in% c("ts","edata.eks.stageid","edata.eks.values.SearchPhrase","edata.eks.id","gdata.id","edata.eks.subtype"))]
ge_int_feb_ec <- ge_int_feb1[,which(names(ge_int_feb1) %in% c("ts","edata.eks.stageid","edata.eks.values.SearchPhrase","edata.eks.id","gdata.id","edata.eks.subtype"))]
ge_int_feb1_ec <- ge_int_feb2[,which(names(ge_int_feb2) %in% c("ts","edata.eks.stageid","edata.eks.values.SearchPhrase","edata.eks.id","gdata.id","edata.eks.subtype"))]

############## Combining all months data ###########
ge_int_final1 <- rbind.fill(ge_int_feb_ec,ge_int_feb1_ec)

rm(ge_int_jan_ec,ge_int_feb_ec,ge_int_feb1_ec)

############# Date_filtering #######################
dummy <- strtrim(ge_int_final1$ts, width = 19)
ge_int_final1$date <- as.Date(dummy,format = "%Y-%m-%dT%H:%M:%S")
ge_int_all <- subset(ge_int_final1,ge_int_final1$date >="2017-02-27" & ge_int_final1$date <="2017-03-05")
ge_int_all$weeks <-format.Date(ge_int_all$date,format = "%W")
table(ge_int_all$weeks)
rm(ge_int_final1)
ge_int_all$ts = NULL

############ Explore content #######################
stage_ids <- c("ExploreContent")
ge_int_exp <- subset(ge_int_all,edata.eks.stageid == stage_ids[1])
range(ge_int_exp$date)
########### Banner filtering ######################
banners <- c("Best of Genie","Newest","Most Popular","Top Stories","Recommended")
ban_1 <- subset(ge_int_exp,edata.eks.values.SearchPhrase == banners[1])
View(ban_1)
range(ge_int_exp$date)
ban_all <- rbind.fill(ban_1)

for(i in 2:length(banners)){
  ban_1 <- subset(ge_int_exp,edata.eks.values.SearchPhrase == banners[i])
  ban_all <- rbind.fill(ban_all,ban_1)
}
table(ban_all$weeks)
rm(ban_1,ge_int_exp)
######### Summarization ###########################
clicks_summary_day <- summaryBy(edata.eks.id~date + edata.eks.values.SearchPhrase,data=ban_all,FUN = length)
clicks_summary_week <- summaryBy(edata.eks.id~weeks + edata.eks.values.SearchPhrase,data=ban_all,FUN = length)
names(clicks_summary_week)
c1 <- dcast(clicks_summary_week,edata.eks.values.SearchPhrase ~ weeks)
c2 <- dcast(clicks_summary_day,edata.eks.values.SearchPhrase ~ date)
c3 <- cbind(c1,c2)
View(c3)
c3[[3]] <- NULL
c3[is.na(c3)] <- 0
View(c3)
setwd("D:/mafoi/ekstep/works/bestingine/data/outputs")
write.csv(c3,"banners_w09.csv",row.names = F)
################# Metrics #########################

############ Best in Genie downloads content initiated ###########
setwd("D:/mafoi/ekstep/works/bestingine/data/inputs")
contents_1 <- read.csv("content_ids.csv",header = T,stringsAsFactors = F)

########### Filtering best in gine contents ######################
best_contents <- subset(ge_int_all,ge_int_all$edata.eks.id == contents_1[1,2])
best_contents_fin <- rbind.fill(best_contents)

for (i in (2:nrow(contents_1))){
  best_contents1 <- subset(ge_int_all,edata.eks.id == contents_1[i,2])
  best_contents_fin <- rbind.fill(best_contents_fin,best_contents1)
}


########### content_intiated #####################################
best_con_df <- subset(best_contents_fin,edata.eks.subtype == "ContentDownload-Initiate"|edata.eks.subtype == "ContentDownload-Cancel" )
best_con_df$i <- ifelse(best_con_df$edata.eks.subtype == "ContentDownload-Initiate",1,0)
best_con_df$c <- ifelse(best_con_df$edata.eks.subtype == "ContentDownload-Cancel",1,0)

########## summarizing the best in ginie by days and weeks #####################
summ_bestg_days <- summaryBy(i ~ date,data = best_con_df,FUN = sum)
summ_bestg_weeks <- summaryBy(i ~ weeks,data = best_con_df,FUN = sum)
names(summ_bestg_days)[2] <- "bg_dl_init"
names(summ_bestg_weeks)[2] <- "bg_dl_init"

########## summarizing the best in ginie by contents and days and weeks #########
summ_bestg_days_c <- summaryBy(i ~ date + edata.eks.id ,data = best_con_df,FUN = sum)
summ_bestg_weeks_c <- summaryBy(i ~ weeks + edata.eks.id ,data = best_con_df,FUN = sum)
names(summ_bestg_days_c) <- c("date","content_id","dl_init")
names(summ_bestg_weeks_c) <- c("weeks","content_id","dl_init")

s1 <- dcast(summ_bestg_weeks_c,content_id~weeks)
s2 <- dcast(summ_bestg_days_c,content_id~date)
s3 <- cbind(s1,s2)
s3[is.na(s3)] <- 0
s3[[3]] = NULL
View(s3)

setwd("D:/mafoi/ekstep/works/bestingine/data/outputs")
write.csv(s3,"bg_content_downloads_init_w09.csv",row.names = F)
########## summarizing all content download intiated ################
summ_all_int <- subset(ge_int_all,edata.eks.subtype == "ContentDownload-Initiate")
summ_all_int$i <- ifelse(summ_all_int$edata.eks.subtype == "ContentDownload-Initiate",1,0)
summ_all_int_days <- summaryBy(i ~ date,data = summ_all_int,FUN = sum)
summ_all_int_weeks <- summaryBy(i ~ weeks,data = summ_all_int,FUN = sum)
names(summ_all_int_days)[2] <- "all_contents_dl"
names(summ_all_int_weeks)[2] <- "all_contents_dl"
View(summ_all_int_days)

########## summarizing all contents Viewed ###################
summ_ge_all <- subset(ge_int_all,edata.eks.stageid == "ContentDetail" & edata.eks.subtype == "Content")
summ_ge_all_days <- summaryBy(edata.eks.id ~ date,data=summ_ge_all,FUN = length)
summ_ge_all_weeks <- summaryBy(edata.eks.id ~ weeks,data=summ_ge_all,FUN = length)
names(summ_ge_all_days)[2] <- "all_content_viewed"
names(summ_ge_all_weeks)[2] <- "all_content_viewed"

########## Session length summary #######################################
setwd("D:/mafoi/ekstep/data/Final CSVs/ekstep data/Final CSVs/session_lengths")
session_len <- read.csv("session_length.csv",header = T,stringsAsFactors = F)

dummy <- strtrim(session_len$ts,width = 19)
session_len$date <- as.Date(dummy,format = "%Y-%m-%dT%H:%M:%S")
session_len$weeks <- format.Date(session_len$date,format = "%W")
sess_len <- subset(session_len,date >= "2017-03-13" & date <= "2017-03-19")

######### Filtering best in gine content in session length #######
sess_len_bc <- subset(sess_len,gdata.id == contents_1[1,2])
sess_len_fin <- rbind.fill(sess_len_bc)

for (i in (2:nrow(contents_1))){
  sess_len_bc <- subset(sess_len,gdata.id == contents_1[i,2])
  sess_len_fin <- rbind.fill(sess_len_fin,sess_len_bc)
}

  ########summary by days and weeks #############
sess_bg_days <- summaryBy(session_length ~ date ,data = sess_len_fin,FUN = sum)
sess_bg_weeks <- summaryBy(session_length ~ weeks,data = sess_len_fin,FUN = sum)

names(sess_bg_days)[2] <- "bg_session_length"
names(sess_bg_weeks)[2] <- "bg_session_length"

######## summary by all contents  ###############
sess_days <- summaryBy(session_length ~ date ,data = sess_len,FUN = sum)
sess_weeks <- summaryBy(session_length ~ weeks ,data = sess_len,FUN = sum)

names(sess_days)[2] <- "all_session_length"
names(sess_weeks)[2] <- "all_session_length"

######## Formating the metrics  ###############################

  ###### Formating by weeks #######################
  #m1:summ_bestg_weeks,m2:sess_bg_weeks,m3:sess_weeks,m4:summ_ge_all_weeks,m5:summ_all_int_weeks
View(summ_ge_all_weeks)
metrics_final_week <- merge(summ_bestg_weeks,sess_bg_weeks,by = c("weeks"),all.x = T)
metrics_final_week <- merge(metrics_final_week,sess_weeks,by = c("weeks"),all.x = T)
metrics_final_week <- merge(metrics_final_week,summ_ge_all_weeks,by = c("weeks"),all.x = T)
metrics_final_week <- merge(metrics_final_week,summ_all_int_weeks,by = c("weeks"),all.x = T)

  ############ converting session length to hours ##########
metrics_final_week$bg_session_length <- as.integer(metrics_final_week$bg_session_length/(60*60))
metrics_final_week$all_session_length <- as.integer(metrics_final_week$all_session_length/(60*60))
  
 metric_week <- as.data.frame(t(metrics_final_week))
 ###### Formating by days ######################
 View(summ_ge_all_weeks)
 metrics_final_days <- merge(summ_bestg_days,sess_bg_days,by = c("date"),all.x = T)
 metrics_final_days <- merge(metrics_final_days,sess_days,by = c("date"),all.x = T)
 metrics_final_days <- merge(metrics_final_days,summ_ge_all_days,by = c("date"),all.x = T)
 metrics_final_days <- merge(metrics_final_days,summ_all_int_days,by = c("date"),all.x = T)
 
 ############ converting session length to hours ##########
 metrics_final_days$bg_session_length <- as.integer(metrics_final_days$bg_session_length/(60*60))
 metrics_final_days$all_session_length <- as.integer(metrics_final_days$all_session_length/(60*60))
View(metric_days)
 metric_days <- as.data.frame(t(metrics_final_days))
 metric_final <- cbind(metric_week,metric_days)
View(metric_final)
 
 ########### writing the output values ###############
 setwd("D:/mafoi/ekstep/works/bestingine/data/outputs")
 write.csv(metric_final,"bestingine_metrics_w11.csv")
 View(days_form_final)
 View(s3)
 View(metric_final)
 