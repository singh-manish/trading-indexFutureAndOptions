#!/usr/bin/Rscript

## Load the requisite Libraries
suppressMessages( {
	library(xts)
	library(zoo)
	library(tseries)
	library(TTR)
	library(fork)
	library(rredis)
	library(kernlab)
	library(caret)
})

as.numeric.factor <- function(x) {as.numeric(levels(x))[x]}

getADXFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	tempDI_Data <- ADX(ts[,c("High","Low","Close")])
	DIp_val <- last(tempDI_Data[,c("DIp")])
	DIn_val <- last(tempDI_Data[,c("DIn")])
	DX_val <- last(tempDI_Data[,c("DX")])
	ADX_val <- last(tempDI_Data[,c("ADX")])    

	return(c(DIp_val,DIn_val,DX_val,ADX_val))
}

getAROONFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]))
	names(ts) <- c("High","Low")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	temparoon_Data <- aroon(ts[,c("High","Low")])
     	aroonUp_val <- last(temparoon_Data[,c("aroonUp")])
     	aroonDn_val <- last(temparoon_Data[,c("aroonDn")])
     	aroonOsci_val <- last(temparoon_Data[,c("oscillator")])

	return(c(aroonUp_val,aroonDn_val,aroonOsci_val))
}

getATRFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	tempATR_Data <- ATR(ts[,c("High","Low","Close")])
	tr_val <- last(tempATR_Data[,c("tr")])
	atr_val <- last(tempATR_Data[,c("atr")])
     	#trueHigh_val <- last(tempATR_Data[,c("true.high")])
     	#trueLow_val <- last(tempATR_Data[,c("true.low")])    

	return(c(tr_val,atr_val))
}

getBBandsFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	tempBB_Data <- BBands(ts[,c("High","Low","Close")])
	bbandpct_val <- last(tempBB_Data[,c("pctB")])
     	BBandDn_val <- last(tempBB_Data[,c("dn")])
     	BBandMAvg_val <- last(tempBB_Data[,c("mavg")])
     	BBandUp_val <- last(tempBB_Data[,c("up")])

	return(c(bbandpct_val,BBandDn_val,BBandMAvg_val,BBandUp_val))
}

getMACDFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]))
	names(ts) <- c("Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempMACD_Data <- MACD(ts[,c("Close")])
	signal_val <- last(tempMACD_Data[,c("signal")])

	return(c(signal_val))
}

getCCIFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	tempCCI_Data <- CCI(ts[,c("High","Low","Close")])
	cci_val <- last(tempCCI_Data)

	return(c(cci_val))
}

getchaikinVolatilityFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]))
	names(ts) <- c("High","Low")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	tempvolatility_Data <- chaikinVolatility(ts[,c("High","Low")])
     	volatility_val <- last(tempvolatility_Data)

	return(c(volatility_val))
}

getCMOFactorsDaily <- function(series) {

	ts <- data.frame(as.numeric(series[,2]))
	names(ts) <- c("Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 1000000) %/% 1000) == 152 )
	tempCMO_Data <- CMO(ts[,c("Close")])
	cmo_val <- last(tempCMO_Data)

	return(c(cmo_val))
}

getADXFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempDI_Data <- ADX(ts[,c("High","Low","Close")])
	DIp_val <- last(tempDI_Data[,c("DIp")])
	DIn_val <- last(tempDI_Data[,c("DIn")])
	DX_val <- last(tempDI_Data[,c("DX")])
	ADX_val <- last(tempDI_Data[,c("ADX")])    

	return(c(DIp_val,DIn_val,DX_val,ADX_val))
}

getAROONFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]))
	names(ts) <- c("High","Low")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	temparoon_Data <- aroon(ts[,c("High","Low")])
     	aroonUp_val <- last(temparoon_Data[,c("aroonUp")])
     	aroonDn_val <- last(temparoon_Data[,c("aroonDn")])
     	aroonOsci_val <- last(temparoon_Data[,c("oscillator")])

	return(c(aroonUp_val,aroonDn_val,aroonOsci_val))
}

getATRFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempATR_Data <- ATR(ts[,c("High","Low","Close")])
	tr_val <- last(tempATR_Data[,c("tr")])
	atr_val <- last(tempATR_Data[,c("atr")])
     	#trueHigh_val <- last(tempATR_Data[,c("true.high")])
     	#trueLow_val <- last(tempATR_Data[,c("true.low")])    

	return(c(tr_val,atr_val))
}

getBBandsFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempBB_Data <- BBands(ts[,c("High","Low","Close")])
	bbandpct_val <- last(tempBB_Data[,c("pctB")])
     	BBandDn_val <- last(tempBB_Data[,c("dn")])
     	BBandMAvg_val <- last(tempBB_Data[,c("mavg")])
     	BBandUp_val <- last(tempBB_Data[,c("up")])

	return(c(bbandpct_val,BBandDn_val,BBandMAvg_val,BBandUp_val))
}

getMACDFactors <- function(series) {

	ts <- data.frame(as.numeric(series[,2]))
	names(ts) <- c("Close")
	rownames(ts) <- series[,1]
	#ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempMACD_Data <- MACD(ts[,c("Close")])
	signal_val <- last(tempMACD_Data[,c("signal")])

	return(c(signal_val))
}

getCCIFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]),as.numeric(series[,4]))
	names(ts) <- c("High","Low","Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempCCI_Data <- CCI(ts[,c("High","Low","Close")])
	cci_val <- last(tempCCI_Data)

	return(c(cci_val))
}

getchaikinVolatilityFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]),as.numeric(series[,3]))
	names(ts) <- c("High","Low")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempvolatility_Data <- chaikinVolatility(ts[,c("High","Low")])
     	volatility_val <- last(tempvolatility_Data)

	return(c(volatility_val))
}

getCMOFactorsHourly <- function(series) {

	ts <- data.frame(as.numeric(series[,2]))
	names(ts) <- c("Close")
	rownames(ts) <- series[,1]
	ts <- subset(ts, ((as.numeric(rownames(ts)) %% 10000) %/% 1000) == 2 )
	tempCMO_Data <- CMO(ts[,c("Close")])
	cmo_val <- last(tempCMO_Data)

	return(c(cmo_val))
}


calculatePredictionFromModel <- function(modelFileName, dataFrameForPrediction) {

	if (!(file.exists(modelFileName))) {
		return(0)
	}
	
	model <- load(modelFileName)
	dataFrameForPrediction <- as.data.frame(dataFrameForPrediction)

	pred <- predict(get(model), newdata = dataFrameForPrediction, type="raw")
	
	returnValue <- as.numeric.factor(pred)[1]

	if (returnValue > 0) {
		return(returnValue)
	} else {
		return(0)
	}
}

## Function, which takes the pair name strings as arguments and creates the pair file

createSingleLegEntrySignals <- function(symbolInfo,elementLotSize,dataDir,STKOrFUT,modelsDirectoryName,modelsToHandle) {

	elementName <- symbolInfo$Symbol
	zscoreRangeHighOrLow <- symbolInfo$ZScoreType
	modelTypeBuyOrShort <- symbolInfo$BuyOrShort

	MFEmodelFileNameSVM <- paste0(modelsDirectoryName,"/model_",zscoreRangeHighOrLow,"_MFE_",modelTypeBuyOrShort,"_",elementName,".RData")
	MAEmodelFileNameSVM <- paste0(modelsDirectoryName,"/model_",zscoreRangeHighOrLow,"_MAE_",modelTypeBuyOrShort,"_",elementName,".RData")

	if (grepl("SVMONLY",modelsToHandle)) {
		if (
			((file.exists(MFEmodelFileNameSVM)) && (file.info(MFEmodelFileNameSVM)$size > 0)) && 
			((file.exists(MAEmodelFileNameSVM)) && (file.info(MAEmodelFileNameSVM)$size > 0)) 
		) {  
		} else {
			return()
		}	
	}
	
	## read the per minute price data of pairs from csv file
	## Data is in format YYYYMMDDHHSSMM,Open,High,Low,Close,Volume split in 10 minutes or lower denomination say minute wise
	csvInputFileFirstLeg <- paste0(dataDir,"/",elementName,"/",elementName,"_",STKOrFUT,".csv")
	elementPricesTimeSeriesData <- read.csv(csvInputFileFirstLeg, stringsAsFactors=F)
	
	## Data is in format YYYYMMDDHHSSMM,Open,High,Low,Close,Volume split 10 minutes or lower denomination say minute wise
	## We are interested in first column which is time stamp and fifth column which is close price / last traded price and third, fourth columns
	Close <- zoo(elementPricesTimeSeriesData[,5], elementPricesTimeSeriesData[,1])
	High <- zoo(elementPricesTimeSeriesData[,3], elementPricesTimeSeriesData[,1])
	Low <- zoo(elementPricesTimeSeriesData[,4], elementPricesTimeSeriesData[,1])
	Volume <- zoo(elementPricesTimeSeriesData[,6], elementPricesTimeSeriesData[,1])	
	## Merge/intersect the data to get a dataframe object for only those time stamp in which all the data exist. 
	t.zoo <- merge(Close, High, Low, Volume, all=FALSE)
	t <- as.data.frame(t.zoo)
	
	## For 10 min Data use the bar ending at hh:m0 - which is more like starting day at 09:20 and ending at 15:20 - with 10 minutes interval - all of which are tradeable price points
	t_10min <- subset(t, (((as.numeric(rownames(t)) %% 1000000) %/% 100) %% 100) %% 10 == 0);
	t_10min <- tail(t_10min,n=1250);
	
	#### Work with 10min Data and having at least 1200 bars of 10 minutes ####
	lastIndex <- nrow(t_10min);

	if (lastIndex < 1200 ) {
		return()
	}
	
	t_10min$timeStamp <- rownames(t_10min)
	t_10min$spread <- elementLotSize * t_10min$Close 
	
	leftList <- tail(t_10min$spread,n=1200);
	rightList <- c(seq(from=1, to=1200,by=1));
	detrendmodel <- lm(leftList ~ rightList);
	t_10min$dtspread <- c(rep(0,50),detrendmodel$residuals);	

	startIndex <- nrow(t_10min) - 1200;

	t_10min$dtsma_200 <- SMA(t_10min$dtspread, n=1200);
	t_10min$dtstddev_200 <- runSD(t_10min$dtspread, 1200);
	t_10min$dtzscore_200 <- (t_10min$dtspread - t_10min$dtsma_200) / t_10min$dtstddev_200;

	zscore <- c(tail(t_10min$dtzscore_200,n=1))
	
	if ( (abs(zscore[1]) <= 1.0) && (grepl("High",zscoreRangeHighOrLow)) ) {
		return()
	}
	if ( (abs(zscore[1]) > 1.0) && (grepl("Low",zscoreRangeHighOrLow)) ) {
		return()
	}
	
	temp <- getADXFactorsDaily(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low","Close")])
	ADXDaily <- temp[4]

	temp <- getADXFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low","Close")])
	ADXHourly <- temp[4]	
	
	temp <- getAROONFactorsDaily(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low")])
	aroonOsciDaily <- temp[3]
	
	temp <- getAROONFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low")])
	aroonOsciHourly <- temp[3]
	
	temp <- getBBandsFactorsDaily(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low","Close")])
	BBandPctDaily <- temp[1]

	temp <- getBBandsFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low","Close")])
	BBandPctHourly <- temp[1]

	CCIDaily <- getCCIFactorsDaily(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low","Close")])
	CCIHourly <- getCCIFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low","Close")])

	volatilityDaily <- getchaikinVolatilityFactorsDaily(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low")])
	volatilityHourly <- getchaikinVolatilityFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","High","Low")])

	CMODaily <- getCMOFactorsDaily(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","Close")])
	CMOHourly <- getCMOFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","Close")])

	macdsignalHourly <- getMACDFactorsHourly(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","Close")])
	macdsignal <- getMACDFactors(t_10min[seq(from=startIndex, to=lastIndex,by=1),c("timeStamp","Close")])

	dataFrameForPrediction <- data.frame(zscore, ADXDaily, ADXHourly, aroonOsciDaily, aroonOsciHourly, BBandPctDaily, BBandPctHourly, CCIDaily, CCIHourly, volatilityDaily, volatilityHourly, CMODaily, CMOHourly, macdsignalHourly, macdsignal, row.names=NULL,stringsAsFactors = FALSE)
	
	mfepredSVM <- 0
	maepredSVM <- 0
	jointPred <- 0
	
	if (grepl("SVM",modelsToHandle)) {
		mfepredSVM <- calculatePredictionFromModel(MFEmodelFileNameSVM, dataFrameForPrediction)
		maepredSVM <- calculatePredictionFromModel(MAEmodelFileNameSVM, dataFrameForPrediction)
		if ((mfepredSVM==2) && (maepredSVM==1)) {
			jointPred <- 1
		}		
	} 

	tradeSide <- 0	
	if ( (jointPred == 1) && grepl("Buy",modelTypeBuyOrShort) ) { 
		tradeSide <- 1
	} else if ( (jointPred == 1) && grepl("Short",modelTypeBuyOrShort) ) {  
		tradeSide <- -1
	} else {
		return()
	}	

	## one percent return determines initial take profit and initial stop loss limits
	onePercentReturn <- ((elementLotSize * t_10min$Close[lastIndex]) ) * 1.0 / 100
	
	## half life determines the holding period
	halflife <- 70 
	
	## Now prepare the signal and push to Redis Queue
	timestamp <- rownames(tail(t_10min,n=1))
	entrySignalForRedis <- paste0(timestamp,",","ON_",elementName,",",tradeSide,",",elementName,"_",elementLotSize,",",zscore[1],",",t_10min$dtsma_200[lastIndex],",",halflife,",",onePercentReturn,",1,1,1",",",t_10min$spread[lastIndex]);

	redisConnect()
	if (tradeSide > 0) { 
		redisLPush("SAMPLEENTRYSIGNALS",charToRaw(entrySignalForRedis))
		cat(format(Sys.time(),"%Y%m%d%H%M%S : ",tz="Asia/Kolkata"),"SingleLeg_JointSVMRF_Buy - ",entrySignalForRedis,"\n")
	} else if (tradeSide < 0) { 
		redisLPush("SAMPLEENTRYSIGNALS",charToRaw(entrySignalForRedis))
		cat(format(Sys.time(),"%Y%m%d%H%M%S : ",tz="Asia/Kolkata"),"SingleLeg_JointSVMRF_Short - ",entrySignalForRedis,"\n")
	} 
	redisClose()

	return();

}

##
## main program start
##
## Read Command Line Arguments
##
args <- commandArgs(trailingOnly = TRUE)

listOfSymbols <- args[1];
listOfLotSizes <- args[2];
numParallelProcesses <- as.numeric(args[3]);
inputDir <- args[4]
stockOrFuture <- args[5];
modelsDirectory <- args[6];

## read the list of files to be processed from csv file
## data has first Following columns
## Symbol,LotSize,Industry,Currency,Exchange,SecurityType,FutExpiry,NSESymbol,ZScoreType,BuyOrShort
inputSymbolsList <- read.csv(listOfSymbols, stringsAsFactors=F)
inputLotSizesList <- read.csv(listOfLotSizes, stringsAsFactors=F)
inputLotSizesList <- as.data.frame(inputLotSizesList) 

index <- 1;
pids <- c(1:numParallelProcesses);
while (index <= numParallelProcesses)
{
	iindex <- index ;
	pids[index] <- fork(NULL);
	if (pids[index] == 0) {
		## This is child process
		while (iindex <= nrow(inputSymbolsList)) 
		{
			lotSize <- 0
			symbolName <- inputSymbolsList$Symbol[iindex]
			if (nrow(inputLotSizesList[inputLotSizesList$Symbol==symbolName,])) {
				lotSize <- inputLotSizesList[inputLotSizesList$Symbol==symbolName,c("LotSize")]
			}
			tryCatch( 
				if (lotSize > 0) {
					createSingleLegEntrySignals(inputSymbolsList[iindex,],lotSize,inputDir,stockOrFuture,modelsDirectory,"SVM_RF")				
				}
				,
				error = function(e) {
					traceback()
					cat(e$message, " : Error Occured for Single Leg : ",inputSymbolsList$Symbol[iindex]," ",lotSize," ",stockOrFuture," ",inputSymbolsList$ZScoreType[iindex]," ",inputSymbolsList$BuyOrShort[iindex],"\n");
					return()
				}
			)
			iindex <- iindex + numParallelProcesses;
		}
		exit();
	}
	index <- index + 1;
}

for (index in 1:numParallelProcesses)
{
	wait(pids[index]);
}	

cat("# of Single Leg Processed For Joint Support Vector Machines Evaluations :",nrow(inputSymbolsList)," Symbols, Total Time taken :",proc.time()[3]," Seconds \n");

##
## main program end
##