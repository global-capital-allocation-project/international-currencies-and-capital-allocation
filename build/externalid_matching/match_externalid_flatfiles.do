* --------------------------------------------------------------------------------------------------
* Match_Externalid_Flatfiles
*
* This job take the externalid_mns keyfile obtained from the OpenFIGI data pull and and checks
* whether the downloaded identifiers match to data in the CUSIP and ISIN master files produced by
* the present build. This then creates a linking file, so that at the start of the build process, 
* we can match on externalid and keep ISIN and CUSIP.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_match_externalid_flatfiles", replace

use $externalid_temp/externalid_keyfile.dta
merge m:1 figi using $externalid_temp/bbg_figi_data.dta, keepusing(id_cusip id_isin)
drop _merge

gen cusip = externalid_mns if idformat == "ID_CUSIP"
gen isin = externalid_mns if idformat == "ID_ISIN"
replace cusip = id_cusip if cusip == ""
replace isin = id_isin if isin == ""

keep externalid_mns idformat figi cusip isin

* Merge needs unique fields. Do not want to erroneously match something that's not a cusip or isin to one.
replace cusip = "unique" + externalid_mns if cusip == ""
replace isin = "unique" + externalid_mns if isin == ""
gen found_in_masters = ""

merge m:1 cusip using $tempcgs/allmaster_essentials.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, allmaster_essentials" if _merge==3 & found_in_masters==""
drop _merge
merge m:m isin using $tempcgs/allmaster_essentials.dta, keepusing(isin)
drop if _merge==2
replace found_in_masters = "isin, allmaster_essentials" if _merge==3 & found_in_masters==""
drop _merge
duplicates drop

rename cusip cusip_144a
merge m:m cusip_144a using $tempcgs/ffaplusmaster.dta, keepusing(cusip_144a)
drop if _merge==2
replace found_in_masters = "cusip, ffaplusmaster" if _merge==3 & found_in_masters==""
drop _merge
rename cusip_144a cusip

merge m:1 cusip using $tempcgs/incmstr.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, incmstr" if _merge==3 & found_in_masters==""
drop _merge
merge m:1 isin using $tempcgs/incmstr.dta, keepusing(isin)
drop if _merge==2
replace found_in_masters = "isin, incmstr" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using $tempcgs/FHLMC.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, FHLMC" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using $tempcgs/FNMA.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, FNMA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using $tempcgs/GNMA.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, GNMA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using $tempcgs/SBA.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, SBA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using $tempcgs/TBA.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, TBA" if _merge==3 & found_in_masters==""
drop _merge

merge m:1 cusip using $tempcgs/WorldBank.dta, keepusing(cusip)
drop if _merge==2
replace found_in_masters = "cusip, WorldBank" if _merge==3 & found_in_masters==""
drop _merge

gen splitat = strpos(found_in_masters,", ")
gen match_type = substr(found_in_masters,1,splitat - 1)
gen match_file = substr(found_in_masters,splitat + 2,.)
gen match_flag = !missing(match_type)
drop splitat found_in_masters
replace cusip = "" if regexm(cusip, "unique")
replace isin = "" if regexm(isin, "unique")

keep externalid_mns idformat figi match_type match_file cusip isin
drop if match_type==""
replace isin="" if match_type=="cusip"
replace cusip="" if match_type=="isin"

save $externalid_temp/externalid_linking.dta, replace
cap log close
