* --------------------------------------------------------------------------------------------------
* Fuzzy Merge, Step 3
*
* All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier. This allows us to assign a CUSIP to the former records via internal
* cross-linkage.
*
* This file reintroduces the probabilistic matches into the HoldingDetail files in order to generate 
* a final holdings dataset that includes the outcome of the fuzzy merge.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Fuzzy_Merge_Step3_`1'", replace

local year = $firstyear+`1'-1

foreach holdingname in "NonUS" "US" {
	use "$output/HoldingDetail/`holdingname'_`year'_m_step16.dta", clear
	replace securityname=externalname if missing(securityname)
	destring coupon, force replace
	merge m:1 mns_class maturitydate coupon securityname iso_country_code mns_subclass currency_id ///
		using "$output/fuzzy/`holdingname'_rsoft_formerge.dta", nogen keep(1 3)
	foreach var in "coupon" "maturitydate" "iso_country_code" "mns_subclass" "cusip" "currency_id" {
		replace `var' = U`var' if fuzzy==1
	}
	drop U*
	sort date_m MasterPortfolioId _storageid
	cap tostring coupon, force replace
	save "$output/HoldingDetail/`holdingname'_`year'_m_step2.dta", replace
}

log close
