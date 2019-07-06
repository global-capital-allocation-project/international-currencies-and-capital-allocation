# --------------------------------------------------------------------------------------------------
# Figi_API
#
# This job queries the OpenFIGI API (provided by Bloomberg) using the consolidated list of identifiers
# obtained from the externalid field in the Morningstar holdings data.
#
# NOTE: Running this job requires an OpenFIGI API key. In order to run it, please register with
#       OpenFIGI and request a key; then enter this in place of <API_KEY> below.
# --------------------------------------------------------------------------------------------------
require(httr)
require(jsonlite)

####################
###FUNCTIONS########
####################

#Intelligently ascribe the code to all potential formats based on length
CodesToJSONInput <- function(x){
  xDf <- data.frame(idValue = x[,1], Length = apply(x,1,nchar))
  key <- data.frame(idType = c("ID_SEDOL",
                             "ID_CUSIP_8_CHR",
                             "ID_COMMON",
                             "ID_CUSIP",
                             "ID_CINS",
                             "ID_BB",
                             "COMPOSITE_ID_BB_GLOBAL",
                             "ID_BB_GLOBAL_SHARE_CLASS_LEVEL",
                             "ID_BB_GLOBAL",
                             "ID_ISIN",
                             "ID_BB_UNIQUE"),
                    Length = c(7,8,rep(9,4),rep(12,4),18))
  return(merge(xDf,key,by='Length')[,c('idType','idValue')])
}

ChunkJSONToList <- function(jsonobj, chunksize){
  n <- nrow(jsonobj)
  idx <- rep(1:ceiling(n/chunksize),each=chunksize)[1:n]
  return(split(jsonobj, idx))
}

CleanNullResult <- function(resultList){
  noMatch <- data.frame(figi=NA, name=NA, ticker=NA, exchCode=NA, compositeFIGI=NA,
                        uniqueID=NA, securityType=NA, marketSector=NA, shareClassFIGI=NA,
                        uniqueIDFutOpt=NA, securityType2=NA, securityDescription=NA)
  noMatchAll <- noMatch[rep(1,100),]
  for (i in 1:length(resultList)){
    if(length(resultList[[i]])==0){
      resultList[[i]] <- noMatchAll
    } else {
        for (j in 1:length(resultList[[i]])){
          if(length(resultList[[i]][[j]])==0){
            resultList[[i]][[j]] <- noMatch
          } else if (nrow(resultList[[i]][[j]]) > 1) {
            resultList[[i]][[j]] <- noMatch
          } else if (ncol(resultList[[i]][[j]]) < 12) {
            resultList[[i]][[j]] <- noMatch
          }
        }
      resultList[[i]] <- do.call(rbind, resultList[[i]])
    }
  }
  return(resultList)
}

OpenFIGIFn <- function (input, apikey = NULL, openfigiurl = "https://api.openfigi.com/v1/mapping", 
          preferdf = F) 
{
  if (is.null(apikey)) {
    h <- httr::add_headers(`Content-Type` = "text/json")
  }
  else {
    h <- httr::add_headers(`Content-Type` = "text/json", 
                           `X-OPENFIGI-APIKEY` = apikey)
  }
  if (class(input) == "json") {
    myjson <- input
  }
  else {
    myjson <- jsonlite::toJSON(input)
  }
  req <- httr::POST(openfigiurl, h, body = myjson)
  if (as.integer(req$status_code) != 200L) {
    warning(paste0("Got return code ", req$status_code, " when POST json request.\n"))
    return(NULL)
  }
  jsonrst <- httr::content(req, as = "text")
  jsonrst <- jsonlite::fromJSON(jsonrst)
  jsonrst <- jsonrst[["data"]]
  if (preferdf && length(jsonrst) == 1L) 
    return(jsonrst[[1L]])
  jsonrst
}

####################
###SETUP############
####################

apikey = '<API_KEY>'
apiLimitPerRequest <- 100
apiRequestsPerMinute <- 250

# test if there is exactly one argument: the working directory. if not, return an error.
# e.g. for regal args would be /n/regal/maggiori_lab/MNS/MNS/mns_data/externalid_matching

args = commandArgs(trailingOnly=TRUE)

if (length(args)!=2) {
  stop("Please supply the working directory for the externalids_to_api file, and the working directory to teh do_not_search file, after calling the rscript.\n", call.=FALSE)
} else if (length(args)==2) {
  for (i in 1:length(args)) {
    eval (parse (text = args[[i]] ))
  }
  print(tempdir)
  print(rawdir)
  setwd(tempdir)
  dirRaw <- rawdir
}

infile <- read.csv('externalids_to_api.csv', stringsAsFactors = FALSE)
known_unmatched_identifiers <- read.csv(paste0(dirRaw,'/known_unmatched_identifiers.csv'), stringsAsFactors = FALSE)[,1]

codes <- data.frame(infile[!(infile[,1] %in% known_unmatched_identifiers),1]) #externalid first column
print(paste('Loaded',nrow(codes),'unique externalids to send to API.'))

####################
###MAIN#############
####################

inputJSON <- CodesToJSONInput(codes)
print(paste('Made JSON for',nrow(inputJSON),'queries.'))
jsonBlockList <- ChunkJSONToList(inputJSON, apiLimitPerRequest)
print(paste('Made JSON Block List, sending to openfigi.org. Total request will take',ceiling(length(jsonBlockList)/apiRequestsPerMinute*1.4),
  'minutes, i.e. approx', ceiling(((length(jsonBlockList)*2)^2)/apiRequestsPerMinute/60), 'hours.'))
jsonRslt <- lapply(jsonBlockList, function(z) {
  Sys.sleep(60/apiRequestsPerMinute*1.2)
  OpenFIGIFn(z, apikey = apikey)
  }
)

print('The following queries found securities which returned NAs.')
which(unlist(lapply(jsonRslt, function(z) sum(unlist(lapply(z, ncol)) < 12)))>0)

print(paste0('Discarding ',sum(unlist(lapply(jsonRslt, lapply, nrow)) > 1),
      ' of ',sum(unlist(lapply(jsonRslt, lapply, nrow)) > 0),
      ' results due to multiple matches. Could do better with fuzzy merge on text.'))

print(paste0(sum(unlist(lapply(jsonRslt, function(z) sum(unlist(lapply(z, ncol)) < 12)))),
      ' securities also were found, but openFIGI does not have a name/description, so we discard.'))

resultsList <- CleanNullResult(jsonRslt)
flatResultsClean <- do.call(rbind,resultsList)
resultsClean <- flatResultsClean[1:nrow(inputJSON),]

resultsClean$externalid_mns <- as.character(inputJSON$idValue)
resultsClean$idformat <- as.character(inputJSON$idType)

resultsFound <- resultsClean[!is.na(resultsClean$figi),]
print(paste0(sum(duplicated(resultsFound$externalid_mns)),
      ' securities also were found, but matched on more than one identifier, so we discard.'))
resultsFound <- resultsFound[!duplicated(resultsFound$externalid_mns),]

print(paste('Keeping',nrow(resultsFound),'successfully matched records.'))

 #check to see all is ok

if(nrow(resultsFound) > 1){
  write.csv(resultsFound, file = "externalid_keyfile.csv", row.names = FALSE)
} else {
  print('Something has changed at openfigi, we have no records. Did not remake externalid_keyfile.csv')
}
