* --------------------------------------------------------------------------------------------------
* Factset_Build
*
* This job imports raw data from Factset. This data details relationships between security issuers
* and their ultimate parents, and is used as a source in Ultimate_Parent_Aggregation.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Factset_Build", replace

* Unique CUSIP9 with ISIN in CGS
use issuer_num issue_num issue_check isin currency_code using "$tempcgs/ALLMASTER_ISIN.dta", clear
gen cusip = issuer_num + issue_num + issue_check
rename currency_code iso_currency
append using "$tempcgs/incmstr.dta"
gen cusip6 = substr(cusip, 1, 6)
keep cusip cusip6 iso_currency isin
bysort isin: keep if _n == _N
bysort cusip: keep if _n == _N
save "$temp/factset/cgs_master_isin_appended", replace

* Read in Factset data for CUSIPs observed in the Morningstar holdings data
import excel using $raw/factset/factset_ultimate_parents_MS.xlsx, clear firstrow sheet("Sheet1")
keep CUSIP UPCUSIP UPISIN UPCOUNTRY
rename (CUSIP UPCUSIP UPISIN UPCOUNTRY) (cusip6 cusip6_bg isin_bg country_bg)
replace country_bg = "" if country_bg == "#N/A"
replace cusip6 = substr(cusip6, 1, 6)
replace cusip6_bg = substr(cusip6_bg, 1, 6)
drop if missing(cusip6_bg) & missing(isin_bg)
save $temp/factset/step1_MS, replace

* Read in Factset data for the rest of the CUSIPs in the CGS reference data
import excel using $raw/factset/factset_ultimate_parents_CGS.xlsx, clear firstrow sheet("Sheet1")
rename UPCUSIPCOUNTRY UPCOUNTRY
keep CUSIP UPCUSIP UPISIN UPCOUNTRY
rename (CUSIP UPCUSIP UPISIN UPCOUNTRY) (cusip6 cusip6_bg isin_bg country_bg)
replace country_bg = "" if country_bg == "#N/A"
replace cusip6 = substr(cusip6, 1, 6)
replace cusip6_bg = substr(cusip6_bg, 1, 6)
drop if missing(cusip6_bg) & missing(isin_bg)
save $temp/factset/step1_CGS, replace

* Consolidate these and save output
use $temp/factset/step1_MS, clear
append using $temp/factset/step1_CGS
duplicates drop cusip6 isin, force
duplicates drop cusip6, force
mmerge isin_bg using "$temp/factset/cgs_master_isin_appended", umatch(isin) unmatched(m)
gen cusip6_cgs = substr(cusip, 1, 6)
replace cusip6_bg = cusip6_cgs if missing(cusip6_bg)
keep cusip6 cusip6_bg country_bg
drop if missing(cusip6_bg)
save $temp/factset/factset_cusip6_bg, replace

log close
