* --------------------------------------------------------------------------------------------------
* Fuzzy Merge: 'Good Data' Preliminary Step, Part 2
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
log using "$logs/${whoami}_Fuzzy_Merge_Good_Data_Step12", replace

* Append collapsed tables and perform cleaning
clear
foreach holdingname in "NonUS" "US" {
	foreach inv_type in "bonds" "stocks" {
		forvalues x=$lastyear(-1)$firstyear {
			display "`holdingname', `inv_type', `x', Append Good Data"
			append using $temp/fuzzy/`holdingname'_good_`inv_type'_`x'.dta, force
		}
		collapse (firstnm) coupon iso_country_code currency_id mns_subclass maturitydate name_bond_type name_bond_legal name_firm name_jur securityname_raw extra_security_descriptors, by(securityname cusip)
		gen idusing =_n
		order idusing securityname name_bond_type name_bond_legal maturitydate coupon iso_country_code currency_id mns_subclass cusip
		keep idusing securityname name_firm name_jur name_bond_type name_bond_legal maturitydate coupon iso_country_code currency_id mns_subclass cusip securityname_raw extra_security_descriptors
		duplicates drop
		replace maturitydate=. if year(maturitydate)>9999
		saveold "$output/fuzzy/`holdingname'_good_data_`inv_type'.dta", replace
		clear
	}
}

log close
