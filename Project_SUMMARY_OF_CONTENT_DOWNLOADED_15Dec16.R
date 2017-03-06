################################################################
#################### EkStep - Project 1 ########################
############### SUMMARY OF CONTENT DOWNLOADED ##################
################################################################

## Required libraries
library(doBy)
library(plyr)
library(dplyr)

## Setting the working directory for download and uninstall data
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_INTERACT,OE_START,OE_END")

## Getting inputs for 1Sep'16 - 21Nov'16
Nov16 <- read.csv(file = "Nov2016.csv", header = T, stringsAsFactors = F)
Oct16 <- read.csv(file = "Oct2016.csv", header = T, stringsAsFactors = F)
Sep16 <- read.csv(file = "Sep2016.csv", header = T, stringsAsFactors = F)

## Filtering out relevant data
d1_nov <- subset(Nov16, eid == "GE_INTERACT")
d1_nov <- subset(d1_nov, select = c(did,ts,edata.eks.subtype,edata.eks.id,gdata.ver))

d1_oct <- subset(Oct16, eid == "GE_INTERACT")
d1_oct <- subset(d1_oct, select = c(did,ts,edata.eks.subtype,edata.eks.id,gdata.ver))

d1_sep <- subset(Sep16, eid == "GE_INTERACT")
d1_sep <- subset(d1_sep, select = c(did,ts,edata.eks.subtype,edata.eks.id,gdata.ver))

## Combining the relevant data
d1 <- rbind(d1_sep,d1_oct)
d1 <- rbind(d1,d1_nov)

## creating date field
d1$dummy <- strtrim(d1$ts, width = 19)
d1$date <- as.Date(d1$dummy, format = "%Y-%m-%dT%H:%M:%S")
d1$dummy <- NULL
d1$ts <- NULL

## Filtering out entries with gdata.ver less than 5.2
d1$ver_check <- as.numeric(substr(d1$gdata.ver, 1, 3))
d1 <- subset(d1, ver_check >= 5.2)
d1$gdata.ver <- NULL
d1$ver_check <- NULL

## Filtering data for downloads and uninstalls
d1_downloads <- subset(d1, edata.eks.subtype == "ContentDownload-Initiate")
d1_uninstalls <- subset(d1, edata.eks.subtype == "DeleteContent-Initiated")

d1_downloads$edata.eks.subtype <- NULL
d1_downloads$type <- "d"
colnames(d1_downloads)[which(names(d1_downloads) == "edata.eks.id")] = "value"
d1_downloads <- subset(d1_downloads, value != "")

d1_uninstalls$edata.eks.subtype <- NULL
d1_uninstalls$type <- "u"
colnames(d1_uninstalls)[which(names(d1_uninstalls) == "edata.eks.id")] = "value"
d1_uninstalls <- subset(d1_uninstalls, value != "")

rm(d1,d1_nov,d1_oct,d1_sep,Nov16,Oct16,Sep16)

## Setting the working directory for side load data
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Final CSVs/Final CSVs - GE_TRANSFER")

## Getting inputs for 1Sep'16 - 21Nov'16
Nov16 <- read.csv(file = "Nov2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)
Oct16 <- read.csv(file = "Oct2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)
Sep16 <- read.csv(file = "Sep2016_GE_TRANSFER.csv", header = T, stringsAsFactors = F)

## First set of Filtering
d2_nov <- subset(Nov16, edata.eks.datatype == "CONTENT" & edata.eks.direction == "IMPORT")
d2_oct <- subset(Oct16, edata.eks.datatype == "CONTENT" & edata.eks.direction == "IMPORT")
d2_sep <- subset(Sep16, edata.eks.datatype == "CONTENT" & edata.eks.direction == "IMPORT")

## Filtering out entries with gdata.ver less than 5.2
d2_nov$ver_check <- as.numeric(substr(d2_nov$gdata.ver, 1, 3))
d2_nov <- subset(d2_nov, ver_check >= 5.2)
d2_nov$ver_check <- NULL

d2_oct$ver_check <- as.numeric(substr(d2_oct$gdata.ver, 1, 3))
d2_oct <- subset(d2_oct, ver_check >= 5.2)
d2_oct$ver_check <- NULL

d2_sep$ver_check <- as.numeric(substr(d2_sep$gdata.ver, 1, 3))
d2_sep <- subset(d2_sep, ver_check >= 5.2)
d2_sep$ver_check <- NULL

## Getting all content ids
d3_nov <- d2_nov[, c(which(colnames(d2_nov) == c("ts","did")),grep("identifier", colnames(d2_nov)))]
d3_oct <- d2_oct[, c(which(colnames(d2_oct) == c("ts","did")),grep("identifier", colnames(d2_oct)))]
d3_sep <- d2_sep[, c(which(colnames(d2_sep) == c("ts","did")),grep("identifier", colnames(d2_sep)))]

## Combining the relevant data
require(plyr)
d3 <- rbind.fill(d3_nov,d3_oct)
d3 <- rbind.fill(d3,d3_sep)

## creating date field
d3$dummy <- strtrim(d3$ts, width = 19)
d3$date <- as.Date(d3$dummy, format = "%Y-%m-%dT%H:%M:%S")
d3$dummy <- NULL
d3$ts <- NULL

## Getting all the content_id in single row
library(reshape2)
d4 <- melt(d3, id.vars = c("did","date"), na.rm = T)
d4 <- subset(d4, value != "")
d4$variable <- NULL
d4$type <- "s"

rm(d2_nov,d2_oct,d2_sep,Nov16,Oct16,Sep16,d3,d3_nov,d3_oct,d3_sep)

## Appending all the downloads and sideloads
d5 <- rbind.fill(d1_downloads,d4)

## Getting unique count of did from both d5 and d1_uninstalls
u_did <- unique(c(d5$did,d1_uninstalls$did))
## Creating sequence of dates from 1Sep16 to 21Nov16
dates <- seq(from = as.Date("2016-09-01"), to = as.Date("2016-11-21"), by = "day")

## Getting unique count of contents present on device at any point of time
dump=c()
final=c()
i1=1
i2=1

for(i1 in 1:length(u_did))
{
  a <- subset(d5, did == u_did[i1])
  a_dates <- unique(a$date)
  b <- subset(d1_uninstalls, did == u_did[i1])
  con <- c()
  for(i2 in 1:length(a_dates))
  {
    c <- subset(a, date == a_dates[i2])
    c <- c[!duplicated(c$value),]
    d <- subset(b, date == a_dates[i2])
    d <- d$value
    con <- rbind(con,c)
    con <- con[!duplicated(con$value),]
    if(length(d) != 0)
    {
      e <- subset(con,!value %in% d)
      f <- subset(con, value %in% d)
      dump <- rbind(dump,f)
      con <- e
    }
  }
  final <- rbind(final,con)
}

rm(a,b,c,con,d,e,f,a_dates,i1,i2,d4)

# write.csv(final, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/Processed datasets/content_present.csv", row.names = F)

## Filtering out the contents which were preloaded on Genie
## content_id : do_30074541, do_30076072
final <- subset(final, value != "do_30074541")
final <- subset(final, value != "do_30076072")
# dump <- subset(dump, value != "do_30074541")
# dump <- subset(dump, value != "do_30076072")

## Getting information for size of content for each download and sideload
content_info <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Other Data/content_list_7Dec16.csv", header = T, stringsAsFactors = F)
content_info$language <- NULL
content_info$contentType <- NULL
final <- merge(final,content_info,by.x = "value",by.y = "identifier",all.x = T)
final[is.na(final)] <- 0
dump <- merge(dump,content_info,by.x = "value",by.y = "identifier",all.x = T)
dump[is.na(dump)] <- 0

## Creating columns for download and side load and for size of download and sideload for both final and dump
final$download <- ifelse(final$type == "d",1,0)
final$size_download <- ifelse(final$type == "d",final$size,0)
final$side_load <- ifelse(final$type == "s",1,0)
final$size_side_load <- ifelse(final$type == "s",final$size,0)
dump$download <- ifelse(dump$type == "d",1,0)
dump$size_download <- ifelse(dump$type == "d",dump$size,0)
dump$side_load <- ifelse(dump$type == "s",1,0)
dump$size_side_load <- ifelse(dump$type == "s",dump$size,0)

## Summarizing data at did(device id) and date level
final_sum <- summaryBy(download + side_load + size_download + size_side_load ~ did | date, data = final, FUN = sum)
dump_sum <- summaryBy(download + side_load + size_download + size_side_load ~ did | date, data = dump, FUN = sum)

## Creating uninstall number from dump data
dump_sum$uninstalls <- dump_sum$download.sum + dump_sum$side_load.sum
dump_sum$size_uninstalls <- dump_sum$size_download.sum + dump_sum$size_side_load.sum

## Appending all the downloads, side_loads and uninstalls
output <- rbind.fill(final_sum,dump_sum)
output[is.na(output)] <- 0
out_final <- summaryBy(download.sum + side_load.sum + uninstalls + size_download.sum + size_side_load.sum + size_uninstalls ~ did | date, data = output, FUN = sum)

## Creating all date entries for each did
did <- u_did
did <- as.data.frame(did)
did$date <- NA
con <- c()

for(i1 in 1:length(dates))
{
  did$date <- dates[i1]
  con <- rbind(con,did)
}
con$did <- as.character(con$did)

rm(dates,i1)

## Combining con variable with d1_sum and d2_sum
require(dplyr)
final_d <- left_join(con,out_final,by=c("did","date"))
final_d[is.na(final_d)] = 0

## Converting size columns to MB
final_d$size_download <- final_d$size_download.sum.sum / (1024*1024)
final_d$size_side_load <- final_d$size_side_load.sum.sum / (1024*1024)
final_d$size_uninstalls <- final_d$size_uninstalls.sum / (1024*1024)
final_d$size_download.sum.sum <- NULL
final_d$size_side_load.sum.sum <- NULL
final_d$size_uninstalls.sum <- NULL

## Removing all the office devices with office tag : 6c3791818e80b9d05fb975da1e972431d9f8c2a6
office_devices <- read.csv(file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Other Data/Office tags - device ids.csv",header = T,stringsAsFactors = F)
colnames(office_devices) <- "did"
office_devices$tag <- "office_device"
final_d <- merge(final_d,office_devices,by="did",all.x = T)
final_d <- subset(final_d, is.na(tag))
final_d$tag <- NULL

## Arranging the data by did and date
final_d <- arrange(final_d, did, date)

## writing the output master file
write.csv(final_d, file = "E:/Akshit_Workdata/Office Work/Analytics/EkStep/Projects/Project 1-SUMMARY OF CONTENT DOWNLOADED/output_12dec16.csv", row.names = F)

############################### End of Code ###################################