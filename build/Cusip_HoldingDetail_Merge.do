* --------------------------------------------------------------------------------------------------
* Cusip_HoldingDetail_Merge
*
* This file merges in security-level data from the CUSIP Global Services (CGS) master files into the
* holdings data. The resulting HoldingDetail files are referred to as "step 2" files.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Cusip_HoldingDetail_Merge_`1'", replace

local test=0
if `test'==1 {
	local append=" if _n<100000"
}
if `test'~=1 {
	local append=""
}
	
* Array from 1-36, split into all possible combos
local holdingtype=mod(`1',2)
local yearrange=floor((`1'-1)/2)
if `holdingtype'==0 {
	local holdingname="NonUS"
}
if `holdingtype'==1 {
	local holdingname="US"
}
if `yearrange'==0 {
	local syear=1986
	local eyear=1999
}
if `yearrange'!=0 {
	local syear=`yearrange'+1999
	local eyear=`yearrange'+1999
}

forvalues x=`syear'/`eyear' {
	display "`holdingname'_`x'_m"
	capture confirm file "$output/HoldingDetail/`holdingname'_`x'_m_step2.dta"
	if _rc==0 {
		use "$output/HoldingDetail/`holdingname'_`x'_m_step2.dta" `append', clear
		mmerge cusip using "$temp/Internal_Currency_NonUS_US.dta", uname(internal_)
		drop if _merge==2
		replace currency_id=internal_currency_id if  _merge==3 & !missing(internal_currency_id)
		drop internal_currency_id _merge

		gen obs=_n
		mmerge cusip using "$tempcgs/allmaster_essentials.dta", uname(cgs_) 
		cap tostring cgs_coupon, force replace
		drop if _merge==2
		replace coupon=cgs_coupon if _merge==3 & !missing(cgs_coupon)
		replace maturitydate=cgs_maturity if _merge==3 & !missing(cgs_maturity)
		drop cgs_issuer_num cgs_mat cgs_co cgs_cu cgs_isi
		gen cusip6=substr(cusip,1,6)
		replace cusip6="" if cusip6=="000000"

		save "$output/HoldingDetail/`holdingname'_`x'_m_step3.dta", replace
	}
	else {
		display "File $output/HoldingDetail/`holdingname'_`x'_m_step2.dta does not exist"
	}
}

log close
