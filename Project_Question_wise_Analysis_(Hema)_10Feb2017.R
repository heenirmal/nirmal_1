########################################################################
############### Project - Question wise Analysis (Hema) ################
########################################################################

## Required Packages

## Required Data
data1 <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - OE_ASSESS, OE_ITEM_RESPONSE/Data - Hema Project/OE_ASSESS_OE_ITEM_RESPONSE(Jul16-Dec16).csv", header = T, stringsAsFactors = F)
names(data1)

## Filtering data1 on the basis of required program tags
fil1 <- data1[, c(which(colnames(data1) %in% c("did")),grep("genie", colnames(data1)))]
## Getting all the tags in single row
require(reshape2)
tags1 <- melt(fil1, id.vars = c("did"), na.rm = T)
tags1$variable <- NULL
tags2 <- tags1[!duplicated(tags1),]
require(dplyr)
fil2 <- filter(tags2, value == "dff9175fa217e728d86bc1f4d8f818f6d2959303" | value == "e4d7a0063b665b7a718e8f7e4014e59e28642f8c" | value == "c6ed6e6849303c77c0182a282ebf318aad28f8d1")
did1 <- unique(fil2$did)
rm(fil1,fil2,tags1,tags2)

## Filtering out the relevant variables from data
data2 <- subset(data1, select = c(uid,ts,edata.eks.length,edata.eks.pass,edata.eks.qid,edata.eks.score,did,edata.eks.state,edata.eks.type,eid,gdata.id))
data2 <- filter(data2, did %in% did1)
rm(did1)

## Adding date time field for data2
data2$dummy <- strtrim(data2$ts, width = 19)
data2$date_time <- as.POSIXct(data2$dummy, format = "%Y-%m-%dT%H:%M:%S")
data2$date <- as.Date(data2$dummy, format = "%Y-%m-%dT%H:%M:%S")
data2$dummy <- NULL

## Filtering out the data between 7th July 2016 - 31st Dec 2016
require(dplyr)
data2 <- filter(data2, date >= as.Date("2016-07-07") & date <= as.Date("2016-12-31"))
data2$date <- NULL

## Segregating OE_ASSESS and OE_ITEM_RESPONSE events
oa <- subset(data2, eid == "OE_ASSESS")
oir <- subset(data2, eid == "OE_ITEM_RESPONSE")

## Adding start and end time for activity on question page
oa$start_time <- oa$date_time - oa$edata.eks.length
oa$end_time <- oa$date_time
oa$date_time <- NULL
oa$activity_key <- paste0(oa$uid,oa$edata.eks.qid,oa$gdata.id,oa$did,oa$start_time)
oa$edata.eks.state <- NULL
oa$edata.eks.type <- NULL

## Replacing names of oa and oir containing "." to "_"
names(oa) <- gsub("\\.","_",names(oa))
names(oir) <- gsub("\\.","_",names(oir))

## Getting info of which Item Response was sent for which activity of OE_ASSESS
require(sqldf)
oir2 <- sqldf("SELECT oir.date_time, oir.edata_eks_state, oir.edata_eks_type, activity_key
            FROM oir LEFT JOIN oa 
            ON oir.did = oa.did AND oir.uid = oa.uid AND oir.edata_eks_qid = oa.edata_eks_qid AND oir.gdata_id = oa.gdata_id AND oir.date_time BETWEEN oa.start_time AND oa.end_time")

oir3 <- arrange(oir2,activity_key,desc(date_time))
oir3 <- oir3[!duplicated(oir3$activity_key),]
oir3 <- oir3[!is.na(oir3$activity_key),]
oir3$date_time <- NULL

## Merging oa with oir3
data3 <- merge(oa,oir3,by="activity_key",all.x = T)

## Getting attempted and skipped fields
data3$attempted <- ifelse(is.na(data3$edata_eks_state),0,1)
data3$skipped <- ifelse(is.na(data3$edata_eks_state),1,0)

rm(data2,oa,oir,oir2,oir3)

## Getting Final Summarizations

## Summarization 1 - Total Attempts/Skips for each question of a content
require(doBy)
sum1 <- summaryBy(attempted + skipped ~ gdata_id | edata_eks_qid, data = data3, FUN = sum)

## Summarization 2 - Only for Attempted questions 
##                  Average Time Spent on Question, 
##                  Average Time Spent on Question when answed correctly
require(dplyr)
a1 <- filter(data3, attempted == 1)
a2 <- filter(a1, edata_eks_pass == "Yes")
a3 <- summaryBy(edata_eks_length ~ gdata_id | edata_eks_qid, data = a1, FUN = mean)
a4 <- summaryBy(edata_eks_length ~ gdata_id | edata_eks_qid, data = a2, FUN = mean)
a3 <- rename(a3, avg_time_all = edata_eks_length.mean)
a4 <- rename(a4, avg_time_correct = edata_eks_length.mean)

sum2 <- merge(a3,a4,by=c("gdata_id","edata_eks_qid"),all = T)
sum2[is.na(sum2)] <- 0

rm(a1,a2,a3,a4)

## Summarization 3 - Only for Attempted questions
##                   Based on Users Scores broken in Percentiles
##                   Average Time Spent on Question, 
##                   Average Time Spent on Question when answed correctly
a1 <- filter(data3, attempted == 1)
a2 <- summaryBy(edata_eks_score ~ uid, data = a1, FUN=sum)
require(Hmisc)
a2$percentile <- with(a2, cut(edata_eks_score.sum, 
                                breaks=quantile(edata_eks_score.sum, probs=seq(0,1, by=0.1), na.rm=TRUE), 
                                include.lowest=TRUE,
                                labels = c("0-10","10-20","20-30","30-40","40-50","50-60","60-70","70-80","80-90","90-100")))
a2$percentile_score <- with(a2, cut(edata_eks_score.sum, 
                              breaks=quantile(edata_eks_score.sum, probs=seq(0,1, by=0.1), na.rm=TRUE), 
                              include.lowest=TRUE))
a2$edata_eks_score.sum <- NULL
a3 <- merge(a1,a2,by="uid",all.x = T)
a4 <- filter(a3, edata_eks_pass == "Yes")
a5 <- summaryBy(edata_eks_length ~ percentile | percentile_score, data = a3, FUN = mean)
a6 <- summaryBy(edata_eks_length ~ percentile | percentile_score, data = a4, FUN = mean)
a5 <- rename(a5, avg_time_all = edata_eks_length.mean)
a6 <- rename(a6, avg_time_correct = edata_eks_length.mean)

sum3 <- merge(a5,a6,by=c("percentile","percentile_score"),all = T)
sum3[is.na(sum3)] <- 0

rm(a1,a2,a3,a4,a5,a6)

## Summarization 4 - Only for Attempted questions
##                   Based on Type of Question
##                   Average Time Spent on Question, 
##                   Average Time Spent on Question when answed correctly


##writing the final files
write.csv(sum1, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Question wise Analysis (Hema)/sum1.csv", row.names = F)
write.csv(sum2, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Question wise Analysis (Hema)/sum2.csv", row.names = F)
write.csv(sum3, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project Question wise Analysis (Hema)/sum3.csv", row.names = F)


###################### End of Code ########################
