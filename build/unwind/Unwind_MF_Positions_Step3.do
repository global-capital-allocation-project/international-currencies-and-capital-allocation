* --------------------------------------------------------------------------------------------------
* Unwind_MF_Positions, Step 3
*
* All files in this folder (unwind) handle the unwinding of funds' positions in other funds. This
* procedure is referred to as "fund-in-fund" unwinding. If a given fund A holds a position in a
* different fund B, we refer to A as the "holding fund", and to B as the "investing fund".
*
* This job and Step 2 handle the re-generation of  holding detail data that reflect the
* unraveling and rescaling of positions that are computed in Steps 1 and 1.5.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Unwind_MF_Positions_Step3_`1'", replace
set checksum off

* Work out holdingname and year
* Should run array from 1 to 130; task 66 is null
if `1' > 65 {
	local holdingname = "US"
	local yr_offset = 33
}
else {
	local holdingname = "NonUS"
	local yr_offset = 0
}
local year = $firstyear + floor(`1'/2) - `yr_offset'

* Work out year-half, previous periods
local even_taskid = mod(`1', 2)
if `even_taskid' == 0 {
	local yr_half = 1
	local prev_year = `year' - 1
	local prev_yr_half = 2
}
else {
	local yr_half = 2
	local prev_year = `year'
	local prev_yr_half = 1
}

* Small program to retry code
cap program drop cap2
program define cap2
  cap `0'
  local i=0
  local attempts=10
  while _rc != 0 & `i' <= `attempts' {
    local i=`i' + 1
    cap `0'
    sleep 10000
  }
  if `i'>`attempts' {
    di _c as error "Command failed after `attempts' attempts : "
    di as input "`0'"
 }
end

* Load the intermediate results
use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_`yr_half'_m_step52_prelim.dta", clear

* Merge with the scaling list; perform rescaling
di "Merging `holdingname' with scaling lists"
cap2 noi mmerge _obs_id using "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_consolidated_`year'_h`yr_half'.dta", unmatched(m)
replace mf_rescaled = 0 if mf_rescaled != 1
replace mf_scaling_factor = 1 if mf_scaling_factor == .
replace marketvalue = marketvalue * mf_scaling_factor

* Drop anything that is completely exhausted
drop if mf_rescaled == 1 & marketvalue == 0

* Store old and new positions separately (to collapse the new ones)
preserve
keep if mf_unwound == 0
cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_oldpos.dta", replace
restore

preserve
keep if mf_unwound == 1 & cusip == ""
cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_blank_cusip.dta", replace
restore

* Collapse the new positions; note that if we have duplicates in tems of 
* ['date', 'MasterPortfolioId', 'cusip'], we sum marketvalue and we take
* for the rest of the columns the data from whichever observation happens
* to appear first in the dataset (under the assumption that all security
* details have been disambiguated by this point in the build pipeline)

* First check if we have any reassignments
di "Finding varnames for collapse"
preserve
keep if mf_unwound == 1 & cusip != ""
count
if `r(N)'>0 {

	* Collapse the new positions: Preparation
	quietly ds
	local data_varnames "`r(varlist)'" 
	di "Data varnames = `r(varlist)'"
	local collapse_keys = "date MasterPortfolioId cusip marketvalue"
	local extra_varnames : list data_varnames - collapse_keys
	di "Now collapsing the new positions"	
	di "Extra varnames = `extra_varnames'"
	count
	di "Checkpoint B: Number of observations = `r(N)'"
	compress
	if "${whoami}" == "antonio_rcc" | "${whoami}" == "brent" {
		collapse (sum) marketvalue, by(date MasterPortfolioId cusip) fast
	}
	else {
		gcollapse (sum) marketvalue, by(date MasterPortfolioId cusip) fast
	}
	cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_tmp_collapse.dta", replace
	restore
	
	* Ensure we keep non-missing values
	keep if mf_unwound == 1 & cusip != ""
	keep `extra_varnames' date MasterPortfolioId cusip
	if "${whoami}" == "antonio_rcc" | "${whoami}" == "brent" {
		egen _group_id = group(date MasterPortfolioId cusip)
	}
	else {
		gegen _group_id = group(date MasterPortfolioId cusip)
	}
	sort _group_id
	by _group_id: gen _group_n = _n
	by _group_id: gen _group_N = _N
	local n_extra_vars : word count `extra_varnames'
	forval i=1/`n_extra_vars' {
		local this_var `: word `i' of `extra_varnames''
		by _group_id: carryforward `this_var', gen(_ff_`this_var')
		replace `this_var' = _ff_`this_var'
		drop _ff_`this_var'
	}
	keep if _group_n == _group_N
	drop _group_*
	
	* Conclude collapse
	merge 1:1 date MasterPortfolioId cusip using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_tmp_collapse.dta", nogen keep(1 3)
	di "Storing collapsed dataset"	
	append using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_blank_cusip.dta", force
	cap drop _merge
	cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_collapsed.dta", replace

	* Find index intersection with old positions
	keep if cusip != ""
	keep date MasterPortfolioId cusip marketvalue
	rename marketvalue marketvalue_new
	gen _matched_to_new_position = 1
	cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_marketval_formerge.dta", replace

    * If any of the collapsed new positions overlap with an existing one, we
    * simply augment the marketvalue of the latter. In the hypothetical case
    * we should have non-unique records in the original positions, we append
    * to whichever comes first in the dataset
    di "Checking for positions overlap"
	use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_oldpos.dta", clear
	cap2 noi mmerge date MasterPortfolioId cusip using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_marketval_formerge.dta", unmatched(m)
	replace _matched_to_new_position = 0 if _matched_to_new_position != 1
	keep if _matched_to_new_position == 1
	count
	if `r(N)'>0 {

		* Find position additions
		di "Found some positions overlap; collapsing"
		keep date MasterPortfolioId cusip _obs_id marketvalue marketvalue_new
		bysort date MasterPortfolioId cusip: gen _group_ix = _n
		keep if _group_ix == 1
		gen marketvalue_updated = marketvalue + marketvalue_new
		drop marketvalue_new
		gen _position_added_to_existing = 1
		cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_pos_additions.dta", replace

		* Remove them from the list of new positions
		use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_collapsed.dta", clear
		cap2 noi mmerge date MasterPortfolioId cusip using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_pos_additions.dta", ukeep(date MasterPortfolioId cusip _position_added_to_existing) unmatched(m)
		drop if _position_added_to_existing == 1
		drop _position_added_to_existing
		cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_collapsed.dta", replace

		* Reintroduce these positions
		use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_oldpos.dta", clear
		cap2 noi mmerge _obs_id using  "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_pos_additions.dta", ukeep(marketvalue_updated) unmatched(m)
		replace marketvalue = marketvalue_updated if marketvalue_updated != .
		drop marketvalue_updated
		cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_oldpos.dta", replace


	}

	* Finally concatenate everything
	use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_oldpos.dta", clear
	append using "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step52_newpos_collapsed.dta", force

	* Store the results
	cap drop _merge
	cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step53.dta", replace

}
else {
	
	* If no reassignment took place, simply store back the original positions
	restore
	cap drop _merge
	cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step53.dta", replace

}

log close
