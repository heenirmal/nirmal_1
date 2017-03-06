#######################################################
######## JSON to CSV Convertion with Filtering ########
#######################################################

# Required packages
library(RJSONIO)
library(plyr)
library(dplyr)
library(stringr)

# Filtering Function
func_fil <- function(in_data, event){
  a <- in_data %>% filter(str_detect(V1, event))
  if(nrow(a) != 0)
  {
    out_data <- c()
    for(i1 in 1:nrow(a))
    {
      df <- as.data.frame(t(as.data.frame(unlist(fromJSON(a[i1,1])))))
      colnames(df) <- names(unlist(fromJSON(a[i1,1])))
      out_data <- rbind.fill(out_data,df)
    }
    return(out_data)
  } else { return(NULL) }
}

# ptm <- proc.time()

setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/unzipping folder/30Jan17 - 05Feb17/")
folder_path <- "E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/unzipping folder/30Jan17 - 05Feb17/"

for (i3 in 1:7)
{
  setwd(paste0(folder_path,"/",i3))
  files_log <- list.files(pattern = ".log")
  
  gi <- c()
  gt <- c()
  ose <- c()
  oi <- c()
  gf <- c()
  gcp <- c()
  gdp <- c()
  oa <- c()
  oir <- c()
  
  for(i1 in files_log)
  {
    data99 <- read.delim(i1, header=FALSE, quote="",stringsAsFactors = F)
    require(dplyr)
    require(stringr)
#     oir <- rbind.fill(oir,func_fil(in_data = data99, event = "OE_ITEM_RESPONSE"))
#     oa <- rbind.fill(oa,func_fil(in_data = data99, event = "OE_ASSESS"))
    gi <- rbind.fill(gi,func_fil(in_data = data99, event = "GE_INTERACT"))
    gt <- rbind.fill(gt,func_fil(in_data = data99, event = "GE_TRANSFER"))
    ose <- rbind.fill(ose,func_fil(in_data = data99, event = "OE_START"))
    ose <- rbind.fill(ose,func_fil(in_data = data99, event = "OE_END"))
    oi <- rbind.fill(oi,func_fil(in_data = data99, event = "OE_INTERACT"))
    gf <- rbind.fill(gf,func_fil(in_data = data99, event = "GE_FEEDBACK"))
    gcp <- rbind.fill(gcp,func_fil(in_data = data99, event = "GE_CREATE_PROFILE"))
    gdp <- rbind.fill(gdp,func_fil(in_data = data99, event = "GE_DELETE_PROFILE"))
    print(i1)
  }  
  
  # proc.time() - ptm

  ## writing final file ##
  setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/unzipping folder/30Jan17 - 05Feb17/")
#   if(nrow(oir) > 0) write.csv(oir,file = paste0("OE_ITEM_RESPONSE_",i3,".csv"),row.names = F)
#   if(nrow(oa) > 0) write.csv(oa,file = paste0("OE_ASSESS_",i3,".csv"),row.names = F)
  if(is.null(gi) == F) {write.csv(gi,file = paste0("GE_INTERACT_",i3,".csv"),row.names = F)}
  if(is.null(gt) == F) {write.csv(gt,file = paste0("GE_TRANSFER_",i3,".csv"),row.names = F)}
  if(is.null(ose) == F) {write.csv(ose,file = paste0("OE_START_OE_END_",i3,".csv"),row.names = F)}
  if(is.null(oi) == F) {write.csv(oi,file = paste0("OE_INTERACT_",i3,".csv"),row.names = F)}
  if(is.null(gf) == F) {write.csv(gf,file = paste0("GE_FEEDBACK_",i3,".csv"),row.names = F)}
  if(is.null(gcp) == F) {write.csv(gcp,file = paste0("GE_CREATE_PROFILE_",i3,".csv"),row.names = F)}
  if(is.null(gdp) == F) {write.csv(gdp,file = paste0("GE_DELETE_PROFILE_",i3,".csv"),row.names = F)}

}

## Appending all the part files
fgi = fgt = fgf = fose = fgcp = fgdp = foi = foir = foa =c()

files_oir <- list.files(pattern = "OE_ITEM_RESPONSE_")
files_oa <- list.files(pattern = "OE_ASSESS_")
files_gi <- list.files(pattern = "GE_INTERACT_")
files_gt <- list.files(pattern = "GE_TRANSFER_")
files_gf <- list.files(pattern = "GE_FEEDBACK_")
files_ose <- list.files(pattern = "OE_START_OE_END_")
files_gcp <- list.files(pattern = "GE_CREATE_PROFILE_")
files_gdp <- list.files(pattern = "GE_DELETE_PROFILE_")
files_oi <- list.files(pattern = "OE_INTERACT_")

for(i4 in files_oir)
{
  oir <- read.csv(file=i4,header = T,stringsAsFactors = F)
  foir <- rbind.fill(foir,oir)
}

for(i4 in files_oa)
{
  oa <- read.csv(file=i4,header = T,stringsAsFactors = F)
  foa <- rbind.fill(foa,oa)
}

for(i4 in files_gi)
{
  gi <- read.csv(file=i4,header = T,stringsAsFactors = F)
  fgi <- rbind.fill(fgi,gi)
}

for(i4 in files_gt)
{
  gt <- read.csv(file=i4,header = T,stringsAsFactors = F)
  fgt <- rbind.fill(fgt,gt)
}

for(i4 in files_gf)
{
  gf <- read.csv(file=i4,header = T,stringsAsFactors = F)
  fgf <- rbind.fill(fgf,gf)
}

for(i4 in files_ose)
{
  ose <- read.csv(file=i4,header = T,stringsAsFactors = F)
  fose <- rbind.fill(fose,ose)
}

for(i4 in files_gcp)
{
  gcp <- read.csv(file=i4,header = T,stringsAsFactors = F)
  fgcp <- rbind.fill(fgcp,gcp)
}

for(i4 in files_gdp)
{
  gdp <- read.csv(file=i4,header = T,stringsAsFactors = F)
  fgdp <- rbind.fill(fgdp,gdp)
}

for(i4 in files_oi)
{
  oi <- read.csv(file=i4,header = T,stringsAsFactors = F)
  foi <- rbind.fill(foi,oi)
}

# writing the final combined files
setwd("E:/Akshit_Workdata/Office Work/Analytics/EkStep/data/Production Data Public/unzipping folder/30Jan17 - 05Feb17/final/")
# write.csv(foir,file = "OE_ITEM_RESPONSE.csv",row.names = F)
# write.csv(foa,file = "OE_ASSESS.csv",row.names = F)
write.csv(fgi,file = "GE_INTERACT.csv",row.names = F)
write.csv(fgt,file = "GE_TRANSFER.csv",row.names = F)
write.csv(fgf,file = "GE_FEEDBACK.csv",row.names = F)
write.csv(fose,file = "OE_START_OE_END.csv",row.names = F)
write.csv(fgcp,file = "GE_CREATE_PROFILE.csv",row.names = F)
write.csv(fgdp,file = "GE_DELETE_PROFILE.csv",row.names = F)
write.csv(foi,file = "OE_INTERACT.csv",row.names = F)

############################ End of Code #################################