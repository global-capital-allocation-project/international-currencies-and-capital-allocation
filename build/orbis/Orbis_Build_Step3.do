* --------------------------------------------------------------------------------------------------
* Orbis_Build, Step 3
*
* This file consolidates the country-specific Orbis ownership files generates in Orbis_Build_Step2,
* and produces the appended version of the Orbis ownership database that we use for ultimate parent
* aggregation.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Orbis_Build_Step3", replace

* Get BVDID - country map
local orbis_country_list : dir "$orbis_ownership" dirs "*"
local i = 1
foreach country of local orbis_country_list {
	if "`country'" != "03" {
		di "Country; processing `country' (`i')"
		cap append using "$temp/orbis/country/shareholder_info_`country'.dta", keep(bvdid bvd_iso_country_code)
		local i = `i' + 1
	}
}

* Manual fixes for few BVDID that associate to multiple countries
drop if bvdid == "HK0000074755" & bvd_iso_country_code != "HKG"
drop if bvdid == "NL27069234" & bvd_iso_country_code != "NLD"
drop if bvdid == "RO4034103" & bvd_iso_country_code != "ROU"
drop if bvdid == "GB06423547" & bvd_iso_country_code != "GBR"
drop if bvdid == "GB08398929" & bvd_iso_country_code != "GBR"

* Manual fixes for sovranational issuers
replace bvd_iso_country_code = "XSN" if bvd_iso_country_code == "II"

* Save output
save "$temp/orbis/bvdid_to_country.dta", replace
clear

* Consolidate compact files
local i = 1
foreach country of local orbis_country_list {
	if "`country'" != "03" {
		di "Compact files; processing `country' (`i')"
		cap append using "$temp/orbis/compact/shareholder_info_`country'.dta"
		local i = `i' + 1
	}
}
mmerge guo50 using "$temp/orbis/bvdid_to_country.dta", type(n:1) unmatched(master) umatch(bvdid)
cap drop _merge
rename bvd_iso_country_code guo50_iso_country_code
drop if inlist(guo50_iso_country_code, "WW", "XX", "YY", "ZZ")
save "$temp/orbis/bvd_up_compact.dta", replace

* Also generate a version that only keeps the latest recorded value
by bvdid_cusip6 (informationdate), sort: gen byte last_obs = (_n == _N)
keep if last_obs == 1
drop informationdate file last_obs 
save "$temp/orbis/bvd_up_compact_latest_only.dta", replace

log close
