* --------------------------------------------------------------------------------------------------
* Fuzzy Merge: 'Bad Data' Preliminary Step, Part 2
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
log using "$logs/${whoami}_Fuzzy_Merge_Bad_Data_Step12", replace

* Append collapsed tables
clear
foreach holdingname in "NonUS" "US"  {
	foreach inv_type in "stocks" "bonds"  {
		forvalues x=$lastyear(-1)$firstyear {
			display "`holdingname', `inv_type', `x', Append"
			append using $temp/fuzzy/`holdingname'_bad_`inv_type'_`x'.dta, force
		}
		duplicates drop securityname name_firm name_jur name_bond_type name_bond_legal maturitydate coupon iso_country_code currency_id mns_subclass, force
		order idmaster securityname name_firm name_jur name_bond_type name_bond_legal maturitydate coupon iso_country_code currency_id mns_subclass
		keep idmaster securityname name_firm name_jur name_bond_type name_bond_legal maturitydate coupon iso_country_code currency_id mns_subclass securityname_raw extra_security_descriptors
		replace maturitydate=. if year(maturitydate)>9999
		saveold "$output/fuzzy/`holdingname'_bad_data_`inv_type'.dta", replace
		clear
	}
}

log close
