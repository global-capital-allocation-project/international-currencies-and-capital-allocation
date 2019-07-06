* --------------------------------------------------------------------------------------------------
* Country_Merge
*
* Merge in the newly created ultimate-parent and country assignments from the CMNS ultimate-parent 
* aggregation procedure.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Country_Merge_Array`1'", replace
local year = `1' + $firstyear - 1

* Prepare ultimate parent master temp file
use "$temp/country_master/up_aggregation_final_compact", clear
rename cusip6_up_bg cusip6_bg2
rename country_bg country_bg2
rename country_bg_source country_source
rename issuer_name_up p_issuer_name
keep issuer_number cusip6_bg2 country_bg2 country_source cgs_domicile issuer_name p_issuer_name
tempfile up_master_temp
save "`up_master_temp'"

foreach holdingname in "NonUS" "US" {

	* Perform the merge
	use "$output/HoldingDetail/`holdingname'_`year'_m_step3.dta", clear
	mmerge cusip6 using "$temp/Internal_Country_NonUS_US.dta", uname(U_)
	replace iso_co = U_iso_co if U_iso_co~=""
	drop if _merge==2
	drop _merge U_iso_co
	mmerge cusip6 using "`up_master_temp'", umatch(issuer_num)
	
	* Fill in info for those CUSIP6 in our data that cannot be matched; they are kept as is
	replace country_source="ms" 		if abs(_merge)==1 & iso_co~=""
	replace country_bg=iso_co 			if abs(_merge)==1 & iso_co~=""
	replace cusip6_bg=cusip6			if abs(_merge)==1 & cusip6~=""
	replace issuer_name=securityname 	if abs(_merge)==1 & securityname~=""
	replace cgs_domicile=iso_co			if abs(_merge)==1 & iso_co~=""
	replace p_issuer_name=securityname 	if abs(_merge)==1 & securityname~=""
	drop if _merge==2
	drop _merge
	
	* We do not aggregate sovereign bonds, agency bonds, sovranationals, and munis
	replace country_bg = iso_country_code if mns_class == "B" & inlist(mns_subclass,"S","A","SF","SV","LS")
	replace cusip6_bg = cusip6 if mns_class == "B" & inlist(mns_subclass,"S","A","SF","SV","LS")
	replace p_issuer_name = issuer_name if mns_class == "B" & inlist(mns_subclass,"S","A","SF","SV","LS")

	* Save the merged file
	save "$output/HoldingDetail/`holdingname'_`year'_m_step4.dta", replace

}

log close
