# --------------------------------------------------------------------------------------------------
# Make_Externalid_Master
#
# This job creates an internal flatfile which has all security-level details for each externalid. The
# data is populated using information internal to the HoldingDetail files. This job continues from
# the file collect_externalid_master.do
# --------------------------------------------------------------------------------------------------
require(readstata13)
require(data.table)

####################
###FUNCTIONS########
####################

Mode <- function(x) {
  ux.raw <- unique(x)
  ux.nona <- ux.raw[!is.na(ux.raw)]
  ux <- ux.raw[ux.nona != ""]
  ux[which.max(tabulate(match(x, ux)))]
}

WeightedMode <- function(w) {
  do.call(cbind, lapply(w, 
                        function(z) 
                          if (sum(z!="")>0){
                            aggregate(w$num_records[z!=""], by = list(z[z!=""]), FUN=sum)[
                              which.max(aggregate(w$num_records[z!=""], by = list(z[z!=""]), FUN=sum)$x),1]
                          } else {
                            ""
                          }
          )
  )
}

WeightedAgreement <- function(w) {
  do.call(cbind, lapply(w[,2:9], function(x) sum(x*w$num_records)/sum(w$num_records)))
}

####################
###SETUP############
####################

# test if there is exactly one argument: the working directory. if not, return an error.
# e.g. for regal args would be /n/regal/maggiori_lab/MNS/MNS/mns_data/externalid_matching

args = commandArgs(trailingOnly=TRUE)

if (length(args)!=1) {
  stop("Please supply the working directory after calling the rscript.\n", call.=FALSE)
} else if (length(args)==1) {
  setwd(args)
}

fname <- 'extid_records_allyears_summary.dta'
extid.raw <- read.dta13(fname)
print(paste0('Loaded file ',getwd(),'/',fname))
print(paste('File has',nrow(extid.raw),'rows.'))

field.names <- c('externalid_mns','iso_country_code','cusip','isin','currency_id','securityname','maturitydate',
                 'coupon','mns_class','mns_subclass','num_records')

####################
###MAIN#############
####################

#Step 1: Setup data table, create metadata.

extid.raw$maturitydate <- as.character(extid.raw$maturitydate)
extid.raw[is.na(extid.raw$maturitydate),'maturitydate'] <- ""
extid.raw[extid.raw$mns_class=="Q",'mns_class'] <- ""
extid.all <- extid.raw
extid.all.dt <- extid.all
setDT(extid.all.dt)
print(paste('File has', length(unique(extid.all.dt$externalid_mns)), 'unique externalids.'))
extid.all.dt[, totrec := sum(num_records), by='externalid_mns']
extid.all.dt[, totunique := sum(unique(num_records_exclname) > 1) +
               sum(num_records_exclname==1), by='externalid_mns']
extid.all.dt[, N := .N, by='externalid_mns']
extid.all.dt[, nuniqueperportid := totunique/nunique_portid]

print('Step 1 complete. Created metadata.')

#Step 2: Create the master file by finding modes per externalid_mns

# First is brazilian tax numbers, second is a common pimco currency forward problem
extid.sub.dt <- extid.all.dt[extid.all.dt$nuniqueperportid < 1 & 
                               !grepl("[0-9]+\\.[0-9]+\\.[0-9]+", extid.all.dt$externalid_mns) &
                               !(grepl("^IID", extid.all.dt$externalid_mns) & grepl("(Currency)|(Other)", extid.all.dt$securityname)),]
extid.sub.dt[, N := .N, by='externalid_mns']
extid.sub.summ.dt <- extid.sub.dt[, .SD[1], by = 'externalid_mns']
print(paste('After cleaning, we have', length(unique(extid.sub.summ.dt$externalid_mns)), 'unique externalids.'))
print(paste('After cleaning, we have', nrow(extid.sub.summ.dt), 'unique externalids.'))

extid.sub.fields.dt <- extid.sub.dt[ , field.names, with=FALSE]

detail <- extid.sub.fields.dt
n.extids <- length(unique(detail$externalid_mns))
extid.sub.modes.dt <- detail[ , {
  if(.GRP %% 10^4 == 0) {cat("progress",round(.GRP/n.extids*100,1),"%\n")};
  data.table(WeightedMode(.SD))}, by= externalid_mns]
extid.sub.modes.dt <- merge(extid.sub.modes.dt, extid.sub.summ.dt[, c('externalid_mns','nunique_portid','N'), with=FALSE], 
                            by='externalid_mns', sort=FALSE, all.x=FALSE, all.y=FALSE)
print('Step 2 complete. Created modal records for each externalid.')

#Step 3. Check accuracy to throw away uncertain records

n.extids <- nrow(extid.sub.modes.dt)
extid.sub.modes.rep.dt <- extid.sub.modes.dt[, {
  if(.GRP %% 10^4 == 0) {cat("progress",round(.GRP/n.extids*100,1),"%\n")};
  .SD[rep(1, N)]}, by= externalid_mns]
extid.blanks <- extid.sub.dt[, field.names[1:9], with=FALSE]
extid.blanks[] <- ""
extid.sub.matches <- data.table(extid.sub.dt[, field.names[1:9], with=FALSE]==extid.sub.modes.rep.dt[, field.names[1:9], with=FALSE])
extid.sub.matches.lax <- data.table(extid.sub.dt[, field.names[1:9], with=FALSE]==extid.sub.modes.rep.dt[, field.names[1:9], with=FALSE] |
                                extid.sub.dt[, field.names[1:9], with=FALSE]==extid.blanks)
extid.sub.matches$externalid_mns <- extid.sub.matches.lax$externalid_mns <- extid.sub.modes.rep.dt$externalid_mns
extid.sub.matches$num_records <- extid.sub.matches.lax$num_records <- extid.sub.dt$num_records

extid.sub.mode.matches.weighted <- extid.sub.matches[, data.table(WeightedAgreement(.SD)), by=externalid_mns]
extid.sub.mode.matches.lax.weighted <- extid.sub.matches.lax[, {if(.GRP %% 10^4 == 0) {cat("progress",round(.GRP/n.extids*100,1),"% \n")};
                                                              data.table(WeightedAgreement(.SD))}, by=externalid_mns]

setDF(extid.sub.mode.matches.lax.weighted)

extid.sub.mode.drops <- as.logical(apply(extid.sub.mode.matches.lax.weighted[, c('cusip','isin')], 1, mean) < .75 |
                        extid.sub.mode.matches.lax.weighted[, 'currency_id'] < .65 |
                        extid.sub.mode.matches.lax.weighted[, 'mns_class'] < .5)

extid.drops.names <- extid.sub.modes.dt[extid.sub.mode.drops,]$externalid_mns

#Step 3b: keep accurate records
extid.sub.mode.final.pt1 <- extid.sub.modes.dt[!extid.sub.mode.drops,]
print(paste('Step 3 complete. Dropped',round(sum(extid.sub.mode.drops)/length(extid.sub.mode.drops)*100,1),
            'perc of records with low agreement.'))
print(paste('Thus far, we have', nrow(extid.sub.mode.final.pt1), 'unique externalids.'))


#Step 4. Final chance for the mismatches to be kept by adjudicating on mismatches
extid.sub.corrections.dt <- extid.sub.dt[extid.sub.dt$externalid_mns %in% extid.drops.names,]
extid.sub.corrections.dt[, flagcusipmatches := sum(externalid_mns==cusip) > 0, by=externalid_mns]
extid.sub.corrections.dt[, flagisinmatches := sum(externalid_mns==isin) > 0, by=externalid_mns]
extid.sub.corrections.dt <- extid.sub.corrections.dt[(flagcusipmatches | flagisinmatches) & 
                                                       (externalid_mns==cusip | externalid_mns==isin), field.names, with=FALSE]
extid.sub.corrections.dt[, N := .N, by='externalid_mns']
extid.sub.corrections.dt <- merge(extid.sub.corrections.dt, extid.sub.summ.dt[,c('externalid_mns','nunique_portid'), with=FALSE], 
                            by='externalid_mns', sort=FALSE, all.x=FALSE, all.y=FALSE)

extid.sub.corrections.modes.dt <- extid.sub.corrections.dt[ , data.table(WeightedMode(.SD)), by= externalid_mns]

extid.sub.corrections.modes.rep.dt <- extid.sub.corrections.modes.dt[, .SD[rep(1, N)], by= externalid_mns]
extid.corrections.blanks <- extid.sub.corrections.dt[, field.names[1:9], with=FALSE]
extid.corrections.blanks[] <- ""
extid.sub.corrections.matches <- data.table(extid.sub.corrections.dt[, field.names[1:9], with=FALSE]==extid.sub.corrections.modes.rep.dt[, field.names[1:9], with=FALSE])
extid.sub.corrections.matches.lax <- data.table(extid.sub.corrections.dt[, field.names[1:9], with=FALSE]==extid.sub.corrections.modes.rep.dt[, field.names[1:9], with=FALSE] |
                                                  extid.sub.corrections.dt[, field.names[1:9], with=FALSE]==extid.corrections.blanks)
extid.sub.corrections.matches$externalid_mns <- extid.sub.corrections.matches.lax$externalid_mns <- extid.sub.corrections.modes.rep.dt$externalid_mns
extid.sub.corrections.matches$num_records <- extid.sub.corrections.matches.lax$num_records <- extid.sub.corrections.dt$num_records

extid.sub.corrections.mode.matches.weighted <- extid.sub.corrections.matches[, data.table(WeightedAgreement(.SD)), by=externalid_mns]
extid.sub.corrections.mode.matches.lax.weighted <- extid.sub.corrections.matches.lax[, data.table(WeightedAgreement(.SD)), by=externalid_mns]

setDF(extid.sub.corrections.mode.matches.lax.weighted)

extid.sub.corrections.mode.drops <- as.logical(apply(extid.sub.corrections.mode.matches.lax.weighted[, c('cusip','isin')], 1, mean) < .75 |
                                     extid.sub.corrections.mode.matches.lax.weighted[, 'currency_id'] < .65 |
                                     extid.sub.corrections.mode.matches.lax.weighted[, 'mns_class'] < .5)
#Step 4b: keep accurate records
extid.sub.mode.final.pt2 <- extid.sub.corrections.modes.dt[!extid.sub.corrections.mode.drops,]
print(paste('Step 4 complete. Cleaned and readded', round(sum(extid.sub.corrections.mode.drops)/length(extid.sub.corrections.mode.drops)*100,1),
      'percent of dropped records.'))

extid.sub.mode.final <- rbind(extid.sub.mode.final.pt1, extid.sub.mode.final.pt2)[, c(field.names,'nunique_portid'), with=FALSE]

extid.sub.mode.final[extid.sub.mode.final$mns_class=="E",]$maturitydate <- ""
extid.sub.mode.final[extid.sub.mode.final$mns_class=="E",]$coupon <- NA

print(paste('Now, we have', nrow(extid.sub.mode.final), 'unique externalids.'))
setDF(extid.sub.mode.final)

#Step 5: ensure MNS formats retained
extid.sub.mode.final$coupon <- as.character(extid.sub.mode.final$coupon)
extid.sub.mode.final[extid.sub.mode.final$maturitydate=="",'maturitydate'] <- NA
extid.sub.mode.final$maturitydate <- as.Date(as.numeric(extid.sub.mode.final$maturitydate), origin='1960-01-01')
extid.sub.mode.final[which(extid.sub.mode.final$maturitydate > as.Date('2099-01-01')),]$maturitydate <- NA
extid.sub.mode.final$num_records <- as.integer(extid.sub.mode.final$num_records)
extid.sub.mode.final$nunique_portid <- as.integer(extid.sub.mode.final$nunique_portid)
extid.sub.mode.final[which(extid.sub.mode.final$mns_class=="" | is.na(extid.sub.mode.final$mns_class)),'mns_class'] <- "Q"
extid.sub.mode.final[which(extid.sub.mode.final$mns_class=="" | is.na(extid.sub.mode.final$mns_class)),'mns_subclass'] <- ""
print(paste('Dropping', sum(!extid.sub.mode.final$mns_class %in% c('A','B','E','L','MF','Q')), 'externalids with class C, D'))
extid.sub.mode.final <- extid.sub.mode.final[extid.sub.mode.final$mns_class %in% c('A','B','E','L','MF','Q'),]
print(paste('Now, we have', nrow(extid.sub.mode.final), 'unique externalids.'))

write.csv(extid.sub.mode.final, file="extid_master.csv", row.names=FALSE)
print(paste('Made externalid master file with', nrow(extid.sub.mode.final), 'records.'))
print(paste0('File saved at ',getwd(),'/','extid_master.csv'))
