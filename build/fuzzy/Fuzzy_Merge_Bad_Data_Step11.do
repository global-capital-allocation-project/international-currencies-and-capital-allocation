* --------------------------------------------------------------------------------------------------
* Fuzzy Merge: 'Bad Data' Preliminary Step, Part 1
*
* All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier. This allows us to assign a CUSIP to the former records via internal
* cross-linkage.
*
* This file cleans and organizes the security records for which a CUSIP is not available (we refer
* to these records as the "bad data").
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Fuzzy_Merge_Bad_Data_Step11_Array`1'", replace
local x = `1' + $firstyear - 1

* Collapse holding detail tables into unique security records
foreach holdingname in "NonUS" "US"  {

	* Read in bond data; use external name if available
	di "`holdingname', `x', Create Bad Data"
	use cusip iso_country currency_id coupon maturitydate externalname marketvalue ///
		securityname mns_class mns_subclass using ///
		"$output/HoldingDetail/`holdingname'_`x'_m_step16.dta" if missing(cusip), clear
	replace securityname=externalname if missing(securityname)
	drop if securityname==""
	drop cusip
	keep if mns_class=="B"

	* Collapse bond dataset
	count
	if `r(N)'>0 {
		collapse (sum) marketvalue, by(iso_country currency_id coupon maturitydate ///
			securityname mns_subclass)

		* Clean bonds dataset
		drop marketvalue
		duplicates drop iso_country currency_id coupon maturitydate securityname mns_subclass, force
		gen idmaster=_n
		preserve
		foreach var in "securityname" "coupon" "maturitydate" {
			gen `var'_original = `var'
		}
		keep idmaster *_original
		save $temp/fuzzy/`holdingname'_bonds_original_info.dta, replace
		restore
		do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_bonds.do"
		do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_extra.do"
		gen securityname_raw=securityname
		replace securityname=securityname_cln
		drop securityname_cln
		replace iso_co=trim(iso_co)
		replace mns_subclass=trim(mns_subclass)
		replace iso_co=itrim(iso_co)
		replace mns_subclass=itrim(mns_subclass)
		replace currency=trim(currency)
		replace currency=itrim(currency)
		replace coupon=trim(coupon)
		replace coupon=itrim(coupon)
	}

	* Save bonds dataset
	save $temp/fuzzy/`holdingname'_bad_bonds_`x'.dta, replace emptyok

	* Read in stocks data; use external name if available
	di "`holdingname', `x', Create Bad Data"
	use cusip iso_country currency_id coupon maturitydate externalname marketvalue ///
		securityname mns_class mns_subclass using ///
		"$output/HoldingDetail/`holdingname'_`x'_m_step16.dta" if missing(cusip), clear
	replace securityname=externalname if missing(securityname)
	drop if securityname==""
	drop cusip
	keep if mns_class=="E"

	* Collapse stocks data
	count
	if `r(N)'>0 {
		collapse (sum) marketvalue, by(iso_country currency_id coupon maturitydate ///
			securityname mns_subclass)
		cap tostring coupon, replace
		
		* Clean equities dataset
		drop marketvalue
		duplicates drop iso_country currency_id coupon maturitydate securityname mns_subclass, force
		gen idmaster=_n
		preserve
		foreach var in "securityname" "coupon" "maturitydate" {
			gen `var'_original = `var'
		}
		keep idmaster *_original
		save $temp/fuzzy/`holdingname'_stocks_original_info.dta, replace
		restore

		*Clean names procedure
		do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_stocks.do"
		do "$build/fuzzy/Fuzzy_Merge_Secname_Strip_extra.do"
		gen securityname_raw=securityname
		replace securityname=securityname_cln
		drop securityname_cln
		replace iso_co=trim(iso_co)
		replace mns_subclass=trim(mns_subclass)
		replace iso_co=itrim(iso_co)
		replace mns_subclass=itrim(mns_subclass)
		replace currency=trim(currency)
		replace currency=itrim(currency)
		replace coupon=trim(coupon)
		replace coupon=itrim(coupon)

	}

	* Save equities dataset
	save $temp/fuzzy/`holdingname'_bad_stocks_`x'.dta, replace emptyok
}

log close
