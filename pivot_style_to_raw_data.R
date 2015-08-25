# Pivot to Raw Data


library(xlsx)
setwd("C:/Users/tantonakis/Google Drive/BUSINESS ANALYSIS/Competition/efood")
report<-read.xlsx("E-food_sales.xlsx", sheetIndex=2,
                  startRow = 1, header=TRUE,stringsAsFactors=FALSE)
setwd("C:/Users/tantonakis/Google Drive/Scripts/AnalyticsProj/cdgr_mysql")
report[,20]<-NULL
report[,19]<-NULL
report[is.na(report)]<-0
library(reshape)
tom<-melt(report, id="RestName")
tom$month<-1
tom$year<-1
tom$year[tom$variable =="Jan.2014"]<-2014
tom$year[tom$variable =="Feb.2014"]<-2014
tom$year[tom$variable =="Mar.2014"]<-2014
tom$year[tom$variable =="Apr.2014"]<-2014
tom$year[tom$variable =="May.2014"]<-2014
tom$year[tom$variable =="Jun.2014"]<-2014
tom$year[tom$variable =="Jul.2014"]<-2014
tom$year[tom$variable =="Aug.2014"]<-2014
tom$year[tom$variable =="Sep.2014"]<-2014
tom$year[tom$variable =="Oct.2014"]<-2014
tom$year[tom$variable =="Nov.2014"]<-2014
tom$year[tom$variable =="Dec.2014"]<-2014
tom$year[tom$variable =="Jan.2015"]<-2015
tom$year[tom$variable =="Feb.2015"]<-2015
tom$year[tom$variable =="Mar.2015"]<-2015
tom$year[tom$variable =="Apr.2015"]<-2015
tom$year[tom$variable =="May.2015"]<-2015

tom$month[tom$variable =="Jan.2014"]<-1
tom$month[tom$variable =="Feb.2014"]<-2
tom$month[tom$variable =="Mar.2014"]<-3
tom$month[tom$variable =="Apr.2014"]<-4
tom$month[tom$variable =="May.2014"]<-5
tom$month[tom$variable =="Jun.2014"]<-6
tom$month[tom$variable =="Jul.2014"]<-7
tom$month[tom$variable =="Aug.2014"]<-8
tom$month[tom$variable =="Sep.2014"]<-9
tom$month[tom$variable =="Oct.2014"]<-10
tom$month[tom$variable =="Nov.2014"]<-11
tom$month[tom$variable =="Dec.2014"]<-12
tom$month[tom$variable =="Jan.2015"]<-1
tom$month[tom$variable =="Feb.2015"]<-2
tom$month[tom$variable =="Mar.2015"]<-3
tom$month[tom$variable =="Apr.2015"]<-4
tom$month[tom$variable =="May.2015"]<-5

tom$variable<-NULL
names(tom)[2]<-"orders"
write.csv2(tom, file="export.csv")