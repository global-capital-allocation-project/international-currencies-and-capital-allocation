* --------------------------------------------------------------------------------------------------
* CPIS_Build
* 
* This file imports the raw Coordinated Portfolio Investment Survey (CPIS) bulk data from the IMF.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_CPIS_Build", replace

* Import CPIS data and clean variable names
import delimited using "$raw/CPIS/CPIS_bulk_time_series.csv", clear varnames(nonames) 
drop v53
foreach var of varlist * {
     replace `var' = subinstr(`var', " ", "_", .) in 1
	 replace `var' = subinstr(`var', "20", "Val20", .) in 1
	 local newname = `var'[1]
     capture rename `var' `newname'
}
rename v1 Country_Name
drop if _n==1
destring Val*, replace ignore("C") force

* Turn IMF codes to country codes
destring Country_Code, replace
destring Counterpart_Country_Code, replace
ssc install Kountry
kountry Country_Code, from(imfn) to(iso3c)
rename _ISO3C_ Country_ISO3
kountry Counterpart_Country_Code, from(imfn) to(iso3c)
rename _ISO3C_ Counterpart_Country_ISO3

* Save 
save "$temp/CPIS/CPIS_bulk.dta", replace

log close
