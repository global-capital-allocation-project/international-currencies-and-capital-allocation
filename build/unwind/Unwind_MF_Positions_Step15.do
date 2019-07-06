* --------------------------------------------------------------------------------------------------
* Unwind_MF_Positions, Step 1.5
* 
* All files in this folder (unwind) handle the unwinding of funds' positions in other funds. This
* procedure is referred to as "fund-in-fund" unwinding. If a given fund A holds a position in a
* different fund B, we refer to A as the "holding fund", and to B as the "investing fund".
*
* This job consolidates the rescaling data produced in Step 1 of the fund-in-fund procedure. This
* data is used to re-compute appropriately re-scaled versions of the positions of the investing funds.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Unwind_MF_Positions_Step15_`1'", replace
local year = `1' + $firstyear - 1

* Consolidate MF rescaling details
di "Consolidating MF rescaling details"
foreach yr_half in "1" "2" {

	if `year' == $firstyear & `yr_half' == 1 {
		clear
	}
	else {
		use "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_`year'_h`yr_half'.dta", clear
	}
	
	if `yr_half' == 2 {
		local yr_next = `year' + 1
		if `1' == 33 {
			cap append using "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_`yr_next'_h1.dta", force
		}
		else {
			append using "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_`yr_next'_h1.dta", force
		}
	}
	else {
		if `1' == 33 {
			cap append using "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_`year'_h2.dta", force
		} 
		else {
			append using "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_`year'_h2.dta", force
		}
	}

	gen share_reassigned = 1 - mf_scaling_factor
	drop mf_rescaled mf_scaling_factor
	cap drop index
	drop if _obs_id == ""
	di "Now collapsing MF scaling lists, H`yr_half'"
	count
	if `r(N)' > 0 {
		if "${whoami}" == "acoppola" | "${whoami}" == "brent" {
			collapse (sum) share_reassigned, by(_obs_id)
		}
		else {
			gcollapse (sum) share_reassigned, by(_obs_id)
		}
	}
	gsort _obs_id
	gen mf_rescaled = 1
	gen mf_scaling_factor = max(1 - share_reassigned, 0)
	drop share_reassigned
	save "$temp/mf_unwinding/mf_scaling_lists/mf_scalings_consolidated_`year'_h`yr_half'.dta", replace

}

log close
