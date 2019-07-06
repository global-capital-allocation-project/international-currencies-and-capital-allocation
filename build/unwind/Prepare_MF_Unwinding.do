* --------------------------------------------------------------------------------------------------
* Prepare_MF_Unwinding
* 
* All files in this folder (unwind) handle the unwinding of funds' positions in other funds. This
* procedure is referred to as "fund-in-fund" unwinding. If a given fund A holds a position in a
* different fund B, we refer to A as the "holding fund", and to B as the "investing fund".
*
* This initial job prepares some temporary files used by the fund-in-fund unwinding code.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Prepare_MF_Unwinding_Array`1'", replace
local year = `1' + $firstyear - 1

foreach holdingname in "NonUS" "US" {

	* Save copies of the step4 holding detail files split into year-halves
	* This is used in the code for fund-in-fund positions unwinding
	use "$output/HoldingDetail/`holdingname'_`year'_m_step4.dta", clear
	replace maturitydate=. if year(maturitydate)>9999
	gen _obs_id = "`holdingname'" + "`year'" + string(_n, "%30.0gc")
	keep if month(date) <= 6
	saveold "$temp/mf_unwinding/tmp_hd_files/`holdingname'_`year'_h1_m_step4.dta", replace

	use "$output/HoldingDetail/`holdingname'_`year'_m_step4.dta", clear
	replace maturitydate=. if year(maturitydate)>9999
	gen _obs_id = "`holdingname'" + "`year'" + string(_n, "%30.0gc")
	keep if month(date) > 6
	saveold "$temp/mf_unwinding/tmp_hd_files/`holdingname'_`year'_h2_m_step4.dta", replace

}

log close
