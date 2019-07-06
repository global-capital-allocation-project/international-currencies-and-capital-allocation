* --------------------------------------------------------------------------------------------------
* Orbis_Build, Step 1
*
* This file builds static identifier data for Orbis from Bureau van Dijk. This data includes a
* record of BvD ID changes over time, as well as mappings from BvD IDs to Legal Entity Identifiers
* (LEIs) and ISINs.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Orbis_Build_Step1", replace

* Make the new to old BVDID map stationary
use "$raw/orbis/header/BvDIDChange.dta", clear
save "$temp/orbis/BvDIDChange.dta", replace
use "$raw/orbis/header/BvDIDChange.dta", clear
rename NewID NewID_additional
save "$temp/orbis/BvDIDChange_additional.dta", replace

forvalues i=1/12 {
	di "Running BVDID flattening, iteration `i'"
	use "$temp/orbis/BvDIDChange.dta", clear
	mmerge NewID using "$temp/orbis/BvDIDChange_additional.dta", type(n:1) umatch(OldID) unmatched(master)
	egen _maxmerge = max(_merge)
	local maxmerge = _maxmerge
	if `maxmerge' == 1 {
		continue, break
	}
	replace NewID = NewID_additional if _merge == 3
	drop _maxmerge NewID_additional
	cap drop _merge
	save "$temp/orbis/BvDIDChange.dta", replace
}

* Prep ALLMASTER_ISSUER plus CGS LEI PLUS
use issuer_number lei_gmei using "$temp/cgs/ALLMASTER_ISSUER.dta", clear
drop if missing(lei_gmei) | missing(issuer_number)
gen file = "allmaster"
append using "$temp/cgs/lei_plus_formerge.dta"
replace file = "leiplus" if missing(file)
duplicates drop issuer_number lei_gmei, force
duplicates tag lei_gmei, gen(_dup)
drop if _dup > 0 & file == "allmaster"
drop _dup
duplicates tag lei_gmei, gen(_dup)
assert _dup == 0
drop _dup
save "$temp/cgs/LEI_to_CUSIP6.dta", replace

* Update BVDID in other files
use "$raw/orbis/header/ISIN_BvDID.dta", clear
mmerge bvdid using "$temp/orbis/BvDIDChange.dta", type(n:1) umatch(OldID) unmatched(master)
replace bvdid = NewID if _merge == 3
drop NewID _merge
save "$temp/orbis/ISIN_BvDID.dta", replace

use "$raw/orbis/header/LEI_details.dta", clear
mmerge bvdid using "$temp/orbis/BvDIDChange.dta", type(n:1) umatch(OldID) unmatched(master)
replace bvdid = NewID if _merge == 3
drop NewID _merge
save "$temp/orbis/LEI_details.dta", replace

log close
