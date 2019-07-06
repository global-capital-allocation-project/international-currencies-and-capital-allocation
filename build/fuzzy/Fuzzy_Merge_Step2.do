* --------------------------------------------------------------------------------------------------
* Fuzzy Merge, Step 2
*
* All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier. This allows us to assign a CUSIP to the former records via internal
* cross-linkage.
*
* This file build an integrated dataset from the matched subsamples obtained via the probabilistic
* record linkage routines.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Fuzzy_Merge_Step2", replace

di "`1'"
foreach holdingname in "NonUS" "US" {
	use "$output/fuzzy/fuzzy_matches_bonds_`holdingname'.dta", clear
	gen mns_class="B"
	append using "$output/fuzzy/fuzzy_matches_stocks_`holdingname'.dta"
	drop if missing(Ucusip)
	replace mns_class="E" if missing(mns_class)
	replace securityname=securityname_raw
	keep securityname Usecurityname maturitydate Umaturitydate coupon Ucoupon  currency_id Ucurrency_id  iso_country_code Uiso_country_code mns_class mns_subclass Umns_subclass Ucusip
	duplicates drop securityname maturitydate coupon currency_id iso_country_code mns_class mns_subclass, force
	gen fuzzy = 1
	save "$output/fuzzy/`holdingname'_rsoft_formerge.dta", replace
}

log close
