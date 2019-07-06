* --------------------------------------------------------------------------------------------------
* Fuzzy Merge: 'Good Data' Preliminary Step, Part 1
*
* All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier. This allows us to assign a CUSIP to the former records via internal
* cross-linkage.
*
* This file cleans and organizes the security records for which a CUSIP is available (we refer
* to these records as the "good data").
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Fuzzy_Merge_Good_Data_Step11_Array`1'", replace
local x = `1' + $firstyear - 1

* Collapse holding detail tables into unique security records; perform cleaning
foreach holdingname in "NonUS" "US"  {

	* Read in data; use external name if available
	di "`holdingname', `x', Create Good Data"
	use cusip iso_country currency_id coupon maturitydate externalname marketvalue securityname ///
		mns_class mns_subclass using "$output/HoldingDetail/`holdingname'_`x'_m_step16.dta" ///
		if !missing(cusip), clear
	replace securityname=externalname if missing(securityname)

	* Collapse bond dataset
	preserve
	keep if mns_class=="B"
	count
	if `r(N)'>0 {
		collapse (sum) marketvalue, by(cusip iso_country currency_id coupon maturitydate ///
			securityname mns_subclass)
	}

	* Clean the bond data
	drop if cusip==""
	drop if securityname==""
	drop marketvalue
	do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_bonds.do"
	do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_extra.do"
	gen securityname_raw=securityname
	replace securityname=securityname_cln
	drop securityname_cln
	replace iso_co=trim(iso_co)
	replace mns_subclass=trim(mns_subclass)
	replace iso_co=itrim(iso_co)
	replace mns_subclass=itrim(mns_subclass)
	replace cusip=trim(cusip)
	replace cusip=itrim(cusip)
	replace currency=trim(currency)
	replace currency=itrim(currency)
	replace coupon=trim(coupon)
	replace coupon=itrim(coupon)

	* Save bonds data
	save $temp/fuzzy/`holdingname'_good_bonds_`x'.dta, replace emptyok

	* Collapse equities dataset
	restore
	keep if mns_class=="E"
	count
	if `r(N)'>0 {
		collapse (sum) marketvalue, by(cusip iso_country currency_id coupon maturitydate ///
			securityname mns_subclass)
	}
	cap tostring coupon, replace

	* Clean the equities data
	drop if cusip==""
	drop if securityname==""
	drop marketvalue
	do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_stocks.do"
	do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_extra.do"
	gen securityname_raw=securityname
	replace securityname=securityname_cln
	drop securityname_cln
	replace iso_co=trim(iso_co)
	replace mns_subclass=trim(mns_subclass)
	replace iso_co=itrim(iso_co)
	replace mns_subclass=itrim(mns_subclass)
	replace cusip=trim(cusip)
	replace cusip=itrim(cusip)
	replace currency=trim(currency)
	replace currency=itrim(currency)
	replace coupon=trim(coupon)
	replace coupon=itrim(coupon)

	* Save the equities data
	save $temp/fuzzy/`holdingname'_good_stocks_`x'.dta, replace emptyok

}

log close
