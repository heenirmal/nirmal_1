################ Recommendation funel ##################
remove(list = ls())
library(plyr)
library(doBy)
library(data.table)

fread()
################ loading the inputs  ###################
setwd("D:/mafoi/ekstep/works/recommendation_analysis/inputs/w17/")
gi_df1 <- fread("Apr2017_GE_INTERACT(1Apr17-30Apr17).csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
gi_df2 <- fread("Mar2017_GE_INTERACT.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
gi_df3 <- fread("Feb2017_GE_INTERACT.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df1 <- fread("OE_INTERACT1.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df2 <- fread("OE_INTERACT2.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df3 <- fread("OE_INTERACT3.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df4 <- fread("OE_INTERACT4.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df5 <- fread("OE_END_RELATED.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df6 <- fread("OE_INTERACT_rcm_w16_1.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
oi_df7 <- fread("OE_INTERACT_END_RELw17.csv",data.table = F,header = T,stringsAsFactors = F,select = c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver"))
unique(gi_df$gdata.ver)
names(oi_df)

############### Databinding ########################3####
gi_df <- rbind.fill(gi_df1,gi_df2,gi_df3)
oi_df <- rbind.fill(oi_df1,oi_df2,oi_df3,oi_df4,oi_df5,oi_df6,oi_df7)
rm(gi_df1,gi_df2,gi_df3,oi_df1,oi_df2,oi_df3,oi_df4,oi_df5,oi_df6)
################ Data Filtering #########################
"6c3791818e80b9d05fb975da1e972431d9f8c2a6"
oi_df_fil <- oi_df[,c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver")]
gi_df_fil <- gi_df[,c("did","gdata.id","uid","sid","ts","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","ver","gdata.ver")]

################ Filtering the data #####################
gi_df_fil$version <- strtrim(gi_df_fil$gdata.ver,width = 3)
gi_df_fil$version <- as.numeric(gi_df_fil$version)
gi_df_fil1 <- subset(gi_df_fil,version >= 5.7)
gi_df_fil1 <- subset(gi_df_fil1,gi_df_fil1$did != "6c3791818e80b9d05fb975da1e972431d9f8c2a6")
oi_df_fil1 <- subset(oi_df_fil,did != "6c3791818e80b9d05fb975da1e972431d9f8c2a6")
oi_df_fil1 <- subset(oi_df_fil1,edata.eks.id == "gc_relatedcontent" & edata.eks.stageid == "endpage")
View(gi_df_fil1)
############### Date filtering ##########################
dummy <- strtrim(gi_df_fil1$ts,width = 19)
gi_df_fil1$timestamp <- as.POSIXct(gi_df_fil1$ts,format = "%Y-%m-%dT%H:%M:%S")
gi_df_fil2 <- subset(gi_df_fil1,gi_df_fil1$timestamp >= "2017-02-02" & gi_df_fil1$timestamp <= "2017-04-30")
gi_df_fil2$id <- "gid"
gi_df_fil3 <- gi_df_fil2[,c("did","gdata.id","uid","sid","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","id","timestamp")]
View(gi_df_fil3)

####################### OI ########################
dummy <- strtrim(oi_df_fil1$ts,width = 19)
oi_df_fil1$timestamp <- as.POSIXct(dummy,format = "%Y-%m-%dT%H:%M:%S")
#oi_df_fil1$weeks <- format.Date(oi_df_fil1$timestamp,"%W")
oi_df_fil2 <- subset(oi_df_fil1,oi_df_fil1$timestamp >= "2017-02-02" & oi_df_fil1$timestamp <= "2017-04-30")
range(oi_df_fil2$timestamp)
oi_df_fil2$id = "oid"
#oi_df_fil2$week <- format.Date(oi_df_fil2$timestamp,"%W")
oi_df_fil2 <- oi_df_fil2[!duplicated(oi_df_fil2),]
sum(oi_df_fil2$week)
#oi_uniq2 <- oi_df_fil2[!duplicated(oi_df_fil2),]
oi_df_fil3 <- oi_df_fil2[,c("did","gdata.id","uid","sid","edata.eks.extype","edata.eks.id","edata.eks.stageid","edata.eks.subtype","id","timestamp")]
oi_df_fil3 <- oi_df_fil3[!duplicated(oi_df_fil3),]
#oi_df_fil3$week <- format.Date(oi_df_fil3$timestamp,"%W")
#oi_df_fil3$week <- NULL
range(oi_df_fil3$timestamp)
############### combing the oi and gi ##################
oi_gi_df <- rbind(oi_df_fil3,gi_df_fil3)
oi_gi_df1 <- oi_gi_df[order(oi_gi_df$did,oi_gi_df$timestamp),]
########################################################
oi_gi_df2 <- subset(oi_gi_df1,oi_gi_df1$edata.eks.subtype == "Content" | oi_gi_df1$edata.eks.subtype == "ContentDownload-Initiate" | oi_gi_df1$edata.eks.subtype == "UID" | oi_gi_df1$edata.eks.subtype == "ContentDownload-Success")
oi_final<- rbind.fill(oi_df_fil3,oi_gi_df2)
oi_final <- oi_final[!duplicated(oi_final),]
#View(oi_final)
oi_sort <- oi_final[order(oi_final$did,oi_final$timestamp),]
View(oi_sort)
oi_sort$clicked <- ifelse(oi_sort$id == "oid",1,0)
sum(oi_sort$clicked)
############# Content Downloads #######################
oi_sort[is.na(oi_sort)] <- 0
oi_sort$content_downloads <- 0
oi_sort$content_dlSuccess <- 0
oi_sort$local_contents <- 0
View(oi_sort)
for(i in (1:(nrow(oi_sort)-3))){
  if(oi_sort$id[(i)] == "oid" ){
    if(oi_sort$edata.eks.subtype[(i+1)] == "Content"){
      if(oi_sort$sid[(i+1)] == oi_sort$sid[(i+2)]){
        if(oi_sort$edata.eks.subtype[(i+2)] == "ContentDownload-Initiate"){
          oi_sort$content_downloads[(i+2)] <- 1
          if(oi_sort$edata.eks.subtype[(i+3)] == "ContentDownload-Success"){
            oi_sort$content_dlSuccess[(i+3)] <- 1
          }
          
        }
        if(oi_sort$edata.eks.subtype[(i+2)] == "UID"){
          oi_sort$local_contents[(i+2)] <- 1
        }
      }
      
    }
  }
}
sum(oi_sort$local_contents)
############# Loading ME_SESSION_SUMMARY ################
setwd("D:/mafoi/ekstep/works/recommendation_analysis/inputs/w17")
sess_summ <- read.csv("ME_SESSION_SUMMARY_VISUALIZATION_1Sep16-30Apr17.csv",header = T,stringsAsFactors = F)
names(sess_summ)
#sess_summ$date_time <- as.Date(sess_summ$date_time,format = "%Y-%m-%d")
sess_summ1 <- subset(sess_summ,date_time >= "2017-02-02" & date_time <= "2017-04-30")
sess_summ1$content_played <- 1
#sess_summ1$weeks <- format.Date(sess_summ1$date_time,"%W")
sess_summ1$id <- "sid"
names(sess_summ1)
sess_summ2 <- sess_summ1[,c("dimensions.did..Descending","uid..Descending","dimensions.gdata.id..Descending","edata.eks.timeSpent..Descending","date_time","content_played","id")]
names(sess_summ2) <- c("did","uid","edata.eks.id","time_spent","timestamp","content_played","id")
########################## summarizing by weeks ###################
oi_sort_final <- rbind.fill(oi_sort,sess_summ2)
oi_sort_final <- oi_sort_final[order(oi_sort_final$did,oi_sort_final$timestamp),]
View(oi_sort_final)
########################## Subsetting #############################
oi_sort_final1 <- subset(oi_sort,oi_sort$content_downloads == 1 | oi_sort$local_contents == 1 | oi_sort$content_dlSuccess == 1)
oi_sort_fin2 <- rbind.fill(oi_sort_final1,sess_summ2)
oi_sort_fin2 <- oi_sort_fin2[order(oi_sort_fin2$did,oi_sort_fin2$timestamp),]

oi_sort_si <- subset(oi_sort_fin2,oi_sort_fin2$id == "gid")
oi_sort_si_fin <- rbind(oi_sort_si) 
View(oi_sort_fin2)
for(i in (1:10)){
  oi_sort_si <- oi_sort_fin2[which(oi_sort_fin2$id == "gid")+i,]
  oi_sort_si_fin <- rbind(oi_sort_si_fin,oi_sort_si) 
}

oi_sort_si_fin <- oi_sort_si_fin[order(oi_sort_si_fin$did,oi_sort_si_fin$timestamp),]
oi_sort_si_fin1 <- oi_sort_si_fin[!duplicated(oi_sort_si_fin),] 

oi_sort_si_fin1[is.na(oi_sort_si_fin1)] <- 0
oi_sort_si_fin1$rcontent_played <- 0
oi_sort_si_fin1$rcontent_local <- 0
unique(oi_sort_si_fin1$edata.eks.subtype)
oi_sort_si_fin1$a <- "content_summary"
oi_sort_si_fin1$time_spent <- as.double(oi_sort_si_fin1$time_spent)
sum(oi_sort_si_fin1$time_spent,na.rm = T)
oi_sort_si_fin1[is.na(oi_sort_si_fin1)] <- 0
dim(oi_sort_si_fin1)
############### Local Contents ############
for(i in c(1:(nrow(oi_sort_si_fin1)-2))){
  if(oi_sort_si_fin1$edata.eks.subtype[(i)] == "UID"){
    if(oi_sort_si_fin1$uid[(i)] == oi_sort_si_fin1$uid[(i+1)]){
      if(oi_sort_si_fin1$id[(i+1)] == "sid"){
        oi_sort_si_fin1$rcontent_local[(i+1)] <- 1
      }
    }
  }
} 

############## Rcontents played ###########
for(i in c(1:(nrow(oi_sort_si_fin1)-2))){
  if(oi_sort_si_fin1$edata.eks.subtype[(i)] == "ContentDownload-Initiate"){
    if(oi_sort_si_fin1$edata.eks.subtype[(i+1)] == "ContentDownload-Success"){
      if(oi_sort_si_fin1$uid[(i+1)] == oi_sort_si_fin1$uid[(i+2)]){
        if(oi_sort_si_fin1$id[(i+2)] == "sid"){
          oi_sort_si_fin1$rcontent_played[(i+2)] <- 1
        }
      }
    }
    
  }
} 

############### summary of contents palyed and time spent ###########
content_summ <- subset(oi_sort_si_fin1,oi_sort_si_fin1$rcontent_played == 1)
cont1 <- summaryBy(rcontent_played + time_spent ~ a,data = content_summ,FUN = sum)
content_summ <- subset(oi_sort_si_fin1,oi_sort_si_fin1$rcontent_local == 1)
cont2 <- summaryBy(rcontent_local + time_spent ~ a,data = content_summ,FUN = sum)
View(cont2)
content_final <- merge(cont1,cont2,by="a")

View(content_final)
#########################Recommendation shown #######################
#setwd("D:/mafoi/ekstep/works/recommendation_analysis/inputs/Recon/")
rcm_df <- read.csv("New Visualization(12)W17.csv",header = T,stringsAsFactors = F)
View(rcm_df)
names(rcm_df) <- c("mode","version","time","count")
data1 <- rcm_df
library(stringi)
data1$dummy <- stri_sub(data1$time, 1, -5)
data1$dummy <- paste0(stri_sub(data1$dummy, 1, -18),stri_sub(data1$dummy, -15))
data1$date_time <- as.POSIXct(data1$dummy, format = "%B %d %Y, %H:%M:%S")
data1$dummy <- NULL
data1$weeks <- format.Date(data1$date_time,"%W")
table(data1$weeks)
unique(data2$mode)
data2 <- subset(data1,data1$date_time >= "2017-02-02" & data1$date_time <= "2017-04-30")
data2 <- subset(data2,data2$mode == "MDATA" | data2$mode == "WIFI")
data2$version1 <- strtrim(data2$version,width = 3)
data2$version1 <- as.numeric(data2$version1)
data3 <- subset(data2,data2$version1 >= 5.7)
data3$a <- "content_summary"
rcomm <- summaryBy(count ~ a,data = data3,FUN = sum)
View(rcomm)
############################ SummaryByWeeks ########################

oi_sort$a <- "content_summary"
rcm_shown <- summaryBy(clicked + content_downloads + content_dlSuccess +local_contents ~ a,data = oi_sort,FUN = sum)

rcm_shown <- merge(rcomm,rcm_shown,by = "a")
rcm_shown <- merge(rcm_shown,content_final,by="a")
names(rcm_shown)
names(rcm_shown) <- c("Recommendation","No_ContentShown","No_ContentClicked","No_ContentDownload_init","No_ContentDownload_success","No_Contentlocal","No_dlContent_played","TimeSpent_dlContent","No_LocalContent_played","TimeSpent_localContent")
rcm_shown$Recommendation = NULL

rcm_shown$ClickedVsShown <- round(rcm_shown$No_ContentClicked/rcm_shown$No_ContentShown,2)
rcm_shown$DownloadinitVsShown <- round(rcm_shown$No_ContentDownload_init/rcm_shown$No_ContentShown,2)
rcm_shown$DownloadSuccessVsShown<- round(rcm_shown$No_ContentDownload_success/rcm_shown$No_ContentShown,2)
rcm_shown$DownloadPlayedVsShown <- round(rcm_shown$No_dlContent_played/rcm_shown$No_ContentShown,2)
rcm_shown$localPlayedVsShown <-round(rcm_shown$No_LocalContent_played/rcm_shown$No_ContentShown,2)
rcm_shown$TimeSpent_dlContent <- as.integer(rcm_shown$TimeSpent_dlContent/60)
rcm_shown$TimeSpent_localContent <- as.integer(rcm_shown$TimeSpent_localContent/60)
rcm_final <- as.data.frame(t(rcm_shown))
names(rcm_final) <- "Values"
View(rcm_final)
#################### Writing the outputs ####################
write.csv(rcm_final,"recommendation_analysis_w17_up.csv")

################### End of Code #############################  