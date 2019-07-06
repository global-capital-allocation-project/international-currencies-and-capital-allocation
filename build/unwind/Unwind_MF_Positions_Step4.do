* --------------------------------------------------------------------------------------------------
* Unwind_MF_Positions, Step 4
*
* All files in this folder (unwind) handle the unwinding of funds' positions in other funds. This
* procedure is referred to as "fund-in-fund" unwinding. If a given fund A holds a position in a
* different fund B, we refer to A as the "holding fund", and to B as the "investing fund".
*
* The holdings data produced by Steps 2 and 3 of the procedure is split up into half-yearly files.
* This job takes care of now aggregating the data to a yearly frequency.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Unwind_MF_Positions_Step4_`1'", replace
local year = `1' + $firstyear - 1

foreach holdingname in "NonUS" "US" {

	di "Processing `holdingname' data"
	
	* First get original fund-level info
	use "$output/HoldingDetail/`holdingname'_`year'_m_step4.dta", clear
	keep MasterPortfolioId region_mstar fundtype_mstar status_mstar DomicileCountryId BroadCategoryGroup region
	duplicates drop
	gsort status_mstar
	collapse (firstnm) status_mstar, by(MasterPortfolioId region fundtype_mstar  DomicileCountryId region_mstar BroadCategoryGroup)
	
	* Certain funds appear as both FO and FE because we get portfolio reports for these funds in both the FO 
	* and FE universes from Morningstar. For any given row, default to FO.
	gsort - fundtype_mstar
	collapse (firstnm) fundtype_mstar, by(MasterPortfolioId region DomicileCountryId region_mstar BroadCategoryGroup)
	
	* Make sure MPID is unique
	duplicates tag MasterPortfolioId, generate(_dup)
	assert _dup == 0
	drop _dup
	save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_fund_characteristics.dta", replace
	
	* Special care of the first year; note that I verified there are no positions to be unwound
	clear
	if `year' == $firstyear {
		use "$temp/mf_unwinding/tmp_hd_files/`holdingname'_`year'_h1_m_step4.dta", clear	
	}
	else {
		use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h1_m_step53.dta", clear
	}
	cap append using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h2_m_step53.dta", force
	replace date = dofc(date)
	format date %td
	replace date_m = mofd(date)
	format date_m %tm
	cap drop index
	cap drop _merge
	
	* Merge with fund characteristics; reconstruct these
	mmerge MasterPortfolioId using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_fund_characteristics.dta", uname(u_) unmatched(m)
	foreach var in "region_mstar" "DomicileCountryId" "region" "BroadCategoryGroup" "fundtype_mstar" {
		replace `var' = u_`var' if mf_unwound == 1
	}
	drop u_*
	cap drop _merge
	
	* Now run a sanity check
	preserve
	gen _ones = 1
	collapse (sum) _ones, by(MasterPortfolioId DomicileCountryId)
	replace _ones = 1
	collapse (sum) _ones, by(MasterPortfolioId)
	assert _ones == 1
	restore
	
	* Save the step5 files
	save "$output/HoldingDetail/`holdingname'_`year'_m_step5.dta", replace
}

log close
