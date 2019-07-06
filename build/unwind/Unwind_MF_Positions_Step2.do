* --------------------------------------------------------------------------------------------------
* Unwind_MF_Positions, Step 2
*
* All files in this folder (unwind) handle the unwinding of funds' positions in other funds. This
* procedure is referred to as "fund-in-fund" unwinding. If a given fund A holds a position in a
* different fund B, we refer to A as the "holding fund", and to B as the "investing fund".
*
* This job and Step 3 handle the re-generation of  holding detail data that reflect the
* unraveling and rescaling of positions that are computed in Steps 1 and 1.5.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Unwind_MF_Positions_Step2_`1'", replace
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

* Load in previous step data
di "Processing `holdingname' data for year `year', h`yr_half'"
use "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_h`yr_half'_m_step51.dta", clear
quietly ds
local original_vars "`r(varlist)'" 

* Reconstruct the positions
quietly count
if `r(N)' > 0 {

	di "Reconstructing `holdingname' positions"
	foreach sub_holdingname in "NonUS" "US" {
	foreach period in "current" "prev" {

		* Find target periods
		if "`period'" == "current" {
			local tgt_year = "`year'"
			local tgt_yr_half = "`yr_half'"
		}
		else {
			local tgt_year = "`prev_year'"
			local tgt_yr_half = "`prev_yr_half'"				
		}
		
		* Merge with the original files
		preserve
		use cusip using "$temp/mf_unwinding/tmp_hd_files/`sub_holdingname'_`tgt_year'_h`tgt_yr_half'_m_step4.dta", clear
		quietly count
		restore
		if `r(N)' > 0 {
			cap2 noi mmerge _obs_id using "$temp/mf_unwinding/tmp_hd_files/`sub_holdingname'_`tgt_year'_h`tgt_yr_half'_m_step4.dta", uname(U_) udrop(MasterPortfolioId index cusip date marketvalue mf_unwound) unmatched(m)
			quietly ds U_*
			local post_merge_vars "`r(varlist)'"
			foreach varname in `post_merge_vars' {
				local varname: subinstr local varname "U_" ""
				di "`holdingname' - `sub_holdingname', `tgt_year' H`tgt_yr_half': Processing variable `varname'"
				cap confirm variable `varname'
				if !_rc {
					cap replace `varname' = U_`varname' if `varname' == .
					cap replace `varname' = U_`varname' if `varname' == ""
				}
				else {
					rename U_`varname' `varname'
				}
			}
			drop U_*
		}		
	}
	}

}
else {
	di "Nothing to be unwound"
}

* Store the intermediate results
cap2 noi save "$temp/mf_unwinding/hd_period_info/`holdingname'_`year'_`yr_half'_m_step52_prelim.dta", replace

log close
