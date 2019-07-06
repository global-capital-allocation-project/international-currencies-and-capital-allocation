* --------------------------------------------------------------------------------------------------
* Dealogic_Build
*
* This job imports issue-level bond data from Dealogic; the data is used in 
* Ultimate_Parent_Aggregation as well as in the probit regression in analysis.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Dealogic_Build", replace

* --------------------------------------------------------------------------------------------------
* Ultimate-parent aggregation file
* --------------------------------------------------------------------------------------------------

* Process CIQ identifiers
use "$raw/ciq/wrds_cusip.dta", clear
replace enddate = td(31dec2099) if missing(enddate)
bysort companyid: egen max_enddate = max(enddate)
keep if enddate == max_enddate
gen ciq_issuer_number = substr(cusip, 1, 6)
keep companyid ciq_issuer_number
duplicates drop

* Take the first since these will all be caught by the AI file later
collapse (firstnm) ciq_issuer_number, by(companyid)
rename companyid CapiqId
save "$temp/dealogic/ciq_identifiers_for_dealogic_merge.dta", replace

* Process company listings
use "$raw/dealogic/stata/companylistings.dta", clear
rename isin ISIN
rename companyid Id
replace ISIN = "" if ISIN == "nan"
keep Id ISIN
save "$temp/dealogic/isin_for_dealogic_merge.dta", replace

* Process ISINs
use "$temp/cgs/allmaster_essentials_isin.dta", clear
keep isin issuer_num
rename isin ISIN
rename issuer_num isin_issuer_number
save "$temp/dealogic/isin_mapping_for_dealogic.dta", replace

* Run CIQ match
use "$raw/dealogic/stata/company.dta", clear
rename cusip CUSIP
rename capiqid CapiqId
rename id Id
mmerge CapiqId using "$temp/dealogic/ciq_identifiers_for_dealogic_merge.dta", unmatched(m)
replace CUSIP = ciq_issuer_number if missing(CUSIP) & ~missing(ciq_issuer_number)
mmerge Id using "$temp/dealogic/isin_for_dealogic_merge.dta", unmatched(m)
mmerge ISIN using "$temp/dealogic/isin_mapping_for_dealogic.dta", unmatched(m)
replace CUSIP = isin_issuer_number if missing(CUSIP) & ~missing(isin_issuer_number)
save "$temp/dealogic/company_build_tmp.dta", replace

* Process tranche ISINS
use "$raw/dealogic/stata/dcmdealtranchesisins.dta", clear
rename dcmdealtranchetrancheid trancheid
rename dcmdealtranchedealid dealid
sort dealid trancheid
save "$temp/dealogic/dcmdealtranchesisins.dta", replace

* Merge tranche id's
use "$raw/dealogic/stata/dcmdealtranches.dta", clear
keep dcmdealdealid trancheid cusip
rename dcmdealdealid dealid
rename cusip CUSIP
mmerge dealid trancheid using "$temp/dealogic/dcmdealtranchesisins.dta", unmatched(m)
drop _merge
mmerge dealid using "$raw/dealogic/stata/dcmdeal.dta", unmatched(m) ukeep(issuerid)
drop _merge
replace CUSIP = substr(CUSIP, 1, 6)
mmerge isin using "$temp/dealogic/isin_mapping_for_dealogic.dta", unmatched(m) umatch(ISIN)
replace CUSIP = isin_issuer_number if missing(CUSIP) & ~missing(isin_issuer_number)
drop _merge
cap drop sortnumber
keep if ~missing(CUSIP)
fkeep CUSIP issuerid
duplicates drop
save "$temp/dealogic/dcmdealtranches.dta", replace

use "$temp/dealogic/company_build_tmp.dta", clear
keep Id companyparentid CUSIP name nationalityofincorporationisocod nationalityofbusinessisocode 
append using "$temp/dealogic/dcmdealtranches.dta"
replace companyparentid = issuerid if missing(companyparentid) & ~missing(issuerid)
drop issuerid
replace companyparentid = Id if missing(companyparentid)
save "$temp/dealogic/company_build_tmp2.dta", replace
mmerge companyparentid using "$temp/dealogic/company_build_tmp2.dta", unmatched(m) umatch(Id) ukeep(CUSIP name nationality*) uname(p_)
keep if ~missing(CUSIP) & ~missing(p_CUSIP)
drop _merge Id companyparentid
duplicates drop CUSIP p_CUSIP, force
drop if CUSIP == p_CUSIP & missing(nationalityofbusinessisocode) & missing(nationalityofincorporationisocod)
save "$temp/dealogic/dealogic_aggregation_tmp.dta", replace

* Use CGS associated issuer file to tidy up conflicts
use "$temp/dealogic/dealogic_aggregation_tmp.dta", clear
mmerge CUSIP using "$temp/cgs/ai_master_for_up_aggregation.dta", unmatched(m) umatch(issuer_num) uname(ai_)
duplicates tag CUSIP, gen(_dup)
replace p_CUSIP = ai_ai_parent_issuer_num if ~missing(ai_ai_parent_issuer_num) & _dup > 0
replace p_name	= ai_ai_parent_issuer_name if ~missing(ai_ai_parent_issuer_num) & _dup > 0
replace p_nationalityofbusinessisocode = ai_ai_parent_domicile if ~missing(ai_ai_parent_issuer_num) & _dup > 0
replace p_nationalityofincorporationisoc = ai_ai_parent_domicile if ~missing(ai_ai_parent_issuer_num) & _dup > 0
drop ai_* _merge _dup
duplicates drop CUSIP p_CUSIP, force

* We still have some unresolved CUSIPs that map to different parents; we
* drop these. These look mostly like cases in which M&A activity took place.
duplicates tag CUSIP, gen(_dup)
drop if _dup > 0
drop _dup
save "$temp/dealogic/dealogic_aggregation_file.dta", replace
keep p_CUSIP p_nationalityofbusinessisocode 
bysort p_CUSIP: gen _dup = _N
keep if _dup > 1
drop _dup
gen _drop = 1
duplicates drop p_CUSIP, force
save "$temp/dealogic/p_cusip_for_drop.dta", replace
use "$temp/dealogic/dealogic_aggregation_file.dta", clear
mmerge p_CUSIP using "$temp/dealogic/p_cusip_for_drop.dta", unmatched(m)
drop if _drop == 1
cap drop _drop
cap drop _merge

* Add everything to LHS
rename CUSIP _CUSIP
mmerge _CUSIP using "$temp/dealogic/dealogic_aggregation_file.dta", umatch(p_CUSIP) unmatched(b) uname(u_)
replace name = u_p_name if _merge == 2
replace nationalityofb = u_p_nationalityofb if _merge == 2
replace nationalityofi = u_p_nationalityofi if _merge == 2
replace p_CUSIP = _CUSIP if _merge == 2
replace p_name = name if _merge == 2
replace p_nationalityofb = nationalityofb if _merge == 2
replace p_nationalityofi = nationalityofi if _merge == 2
drop u_*
rename _CUSIP CUSIP
drop _merge
save "$temp/dealogic/dealogic_aggregation_file.dta", replace

* --------------------------------------------------------------------------------------------------
* Bond issuance data
* --------------------------------------------------------------------------------------------------
global dealogic "$raw/dealogic/stata"

* Process tranche ISINS
use "$raw/dealogic/stata/dcmdealtranchesisins.dta", clear
rename dcmdealtranchetrancheid trancheid
rename dcmdealtranchedealid dealid
mmerge isin using "$temp/cgs/isin_to_cusip", ukeep(cusip9) unmatched(m)
drop if missing(dealid)
drop _merge
gen isin_country = substr(isin, 1, 2)
gen cusip9_implied = ""
replace cusip9_implied = substr(isin, 3, 9) if inlist(isin_country, "US", "CA")
replace cusip9 = cusip9_implied if missing(cusip9) & ~missing(cusip9_implied)
drop cusip9_implied
drop isin_country
sort dealid trancheid cusip
by dealid trancheid: keep if _n == _N
save "$temp/dealogic/trancheisin_tmp.dta", replace

* Process tranches
use "$raw/dealogic/stata/dcmdealtranches.dta", clear
keep dcmdealdealid trancheid coupondescription couponpercent couponfrequencyid currencyisocode cusip firstcoupondate maturitydate moodyscurrent moodyslaunch offerpricepercent spcurrent splaunch yieldtomaturityannualpercent
rename dcmdealdealid dealid
mmerge dealid trancheid using "$temp/dealogic/trancheisin_tmp.dta", unmatched(m)
order cusip cusip9
replace cusip = cusip9 if missing(cusip)
drop _merge
save "$temp/dealogic/tranche_tmp1.dta", replace

* Add in values
use "$raw/dealogic/stata/dcmdealtranchesvalue.dta", clear
rename dcmdealtranchedealid dealid
rename dcmdealtranchetrancheid trancheid
rename currencyisocode value_currency_iso_code
bysort dealid trancheid: gen nkeys = _N
drop if nkeys > 1 & value_currency_iso_code != "USD"
drop nkeys
bysort dealid trancheid: gen nkeys = _N
assert nkeys == 1
drop nkeys
save "$temp/dealogic/tranche_values.dta", replace
use "$temp/dealogic/tranche_tmp1.dta", clear
mmerge dealid trancheid using "$temp/dealogic/tranche_values.dta", unmatched(m)
drop cusip9
drop _merge
save "$temp/dealogic/tranche_tmp2.dta", replace

* Add deal information
use "$temp/dealogic/tranche_tmp2.dta", clear
mmerge dealid using "$raw/dealogic/stata/dcmdeal.dta", unmatched(m) ukeep(dealid issuerid pricingdate nationalityisocode)
drop _merge
save "$temp/dealogic/tranche_tmp3.dta", replace

* Add listings info
use "$dealogic/companylistings.dta", clear
collapse (firstnm) cusip9 isin sedol ticker, by(companyid)
save "$temp/dealogic/listings_ids.dta", replace
use "$temp/dealogic/tranche_tmp3.dta", clear
mmerge issuerid using "$temp/dealogic/listings_ids.dta", umatch(companyid) unmatched(m) uname(equity_)
save "$temp/dealogic/tranche_tmp4.dta", replace

* Add company info
use "$temp/dealogic/tranche_tmp4.dta", clear
mmerge issuerid using "$dealogic/company.dta", umatch(id) ukeep(capiqid cik companyparentid cusip name nationalityofbusinessisocode nationalityofincorporationisocod) uname(company_) unmatched(m)
drop _merge
save "$temp/dealogic/tranche_tmp5.dta", replace

* Consolidate identifiers
use "$temp/dealogic/tranche_tmp5.dta", clear
mmerge equity_isin using "$temp/cgs/isin_to_cusip", unmatched(m) umatch(isin) ukeep(cusip9) uname(equity_IoC_)
replace equity_cusip9 = equity_IoC_cusip9 if missing(equity_cusip9) & ~missing(equity_IoC_cusip9)
drop equity_IoC_cusip9
cap drop _merge
drop equity_sedol
rename company_cusip company_cusip6
save "$temp/dealogic/tranche_tmp6.dta", replace

* Now add in the aggregation
use "$temp/dealogic/tranche_tmp6.dta", clear
rename cusip sec_cusip9
gen sec_cusip6 = substr(sec_cusip9, 1, 6)
gen equity_cusip6 = substr(equity_cusip9, 1, 6) 
order sec_cusip6 equity_cusip6 company_cusip6

mmerge sec_cusip6 using "$temp/country_master/up_aggregation_final_compact", unmatched(m) umatch(issuer_number) ukeep(cusip6_up_bg country_bg issuer_name_up) uname(sec_)
rename sec_cusip6_up_bg sec_up_cusip6
rename sec_country_bg sec_up_country_bg
rename sec_issuer_name_up sec_up_name

mmerge equity_cusip6 using "$temp/country_master/up_aggregation_final_compact", unmatched(m) umatch(issuer_number) ukeep(cusip6_up_bg country_bg issuer_name_up) uname(equity_)
rename equity_cusip6_up_bg equity_up_cusip6
rename equity_country_bg equity_up_country_bg
rename equity_issuer_name_up equity_up_name

mmerge company_cusip6 using "$temp/country_master/up_aggregation_final_compact", unmatched(m) umatch(issuer_number) ukeep(cusip6_up_bg country_bg issuer_name_up) uname(company_)
rename company_cusip6_up_bg company_up_cusip6
rename company_country_bg company_up_country_bg
rename company_issuer_name_up company_up_name

cap drop cusip6_bg country_bg name_bg
gen cusip6_bg = ""
gen country_bg = ""
gen name_bg = ""
foreach var in "sec" "equity" "company" {
	replace cusip6_bg = `var'_up_cusip6 if missing(cusip6_bg)
	replace country_bg = `var'_up_country_bg if missing(country_bg)
	replace name_bg = `var'_up_name if missing(name_bg)
}
foreach var in "equity" "company" "sec" {
	replace cusip6_bg = `var'_cusip6 if missing(cusip6_bg)
}
save "$temp/dealogic/dlg_bonds_consolidated", replace

* Close log
log close
