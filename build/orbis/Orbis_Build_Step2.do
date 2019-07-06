* --------------------------------------------------------------------------------------------------
* Orbis_Build, Step 2
*
* This file builds the Orbis corporate ownership data. We run this file separately for each country.
* For each run, we construct a file with the history of equityholder and subsidiaries information
* for each of the companies in Orbis.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Orbis_Build_Step2_`1'", replace
local process_full_net = 0

* Find out which country we are processing
local orbis_country_list : dir "$orbis_ownership" dirs "*"
local i = 1
foreach country_tmp of local orbis_country_list {
	if `i' == `1' {
		local country = "`country_tmp'"
		di "Processing country `country'"
	}
	local i = `i' + 1
}

* Quit job if country == "03"
if "`country'" == "03" {
	log close
	exit
}

* Program to process identifiers
cap program drop process_identifiers
program define process_identifiers
	args bvd_varlist
	local n_extra_vars : word count `bvd_varlist'
	di "Processing `n_extra_vars' variables in bvd_varlist"
	forval i=1/`n_extra_vars' {

		* Get varname
		local varname `: word `i' of `bvd_varlist''
		di "Processing `varname'"

		* Update BVDID
		mmerge `varname' using "$temp/orbis/BvDIDChange.dta", type(n:1) umatch(OldID) unmatched(master)
		replace `varname' = NewID if _merge == 3
		drop NewID _merge
		
		* Get ISIN
		mmerge `varname' using "$temp/orbis/ISIN_BvDID.dta", type(n:1) umatch(bvdid) unmatched(master)
		rename ISIN `varname'_isin
		drop _merge
		
		* Get LEI
		mmerge `varname' using "$temp/orbis/LEI_details.dta", type(n:1) umatch(bvdid) unmatched(master) ukeep(LEI)
		rename LEI `varname'_lei
		drop _merge
		
		* Match LEI to CUSIP6
		mmerge `varname'_lei using "$temp/cgs/LEI_to_CUSIP6.dta", type(n:1) umatch(lei_gmei) unmatched(master) ukeep(issuer_number)
		rename issuer_number `varname'_cusip6
		drop _merge
		
		* Match ISIN to CUSIP6
		mmerge `varname'_isin using "$temp/cgs/allmaster_essentials_isin.dta", type(n:1) umatch(isin)  unmatched(master) ukeep(issuer_num)
		replace `varname'_cusip6 = issuer_num if missing(`varname'_cusip6)
		drop issuer_num _merge
}
end

cap confirm file "$orbis_ownership/`country'/SHARE_`country'_Links_allyrs.dta"
if _rc==0 {

	* Read in relevant info; we use the guo50c field instead of guo50 to ensure we track companies 
	* to ultimate corporate owners rather than individuals
	use "$orbis_ownership/`country'/SHARE_`country'_Links_allyrs.dta", clear
	keep bvdid shareholderbvdid directonlyfigures totalonlyfigures informationdate typeofrelation guo50 guo50c file bvdid ISO_final_subsidiary ISO_final_shareholder
	replace guo50 = "NA" if missing(guo50)
	replace guo50c = "NA" if missing(guo50c)
	replace guo50 = guo50c
	drop guo50c
	duplicates drop bvdid guo50 informationdate file, force

	* Convert all ISO2 to ISO3
	mmerge ISO_final_subsidiary using "$raw/macro/Concordances/iso2_iso3.dta", type(n:1) umatch(iso2) unmatched(master)
	replace ISO_final_subsidiary = iso3 if _merge == 3
	drop iso3 _merge
	mmerge ISO_final_shareholder using "$raw/macro/Concordances/iso2_iso3.dta", type(n:1) umatch(iso2) unmatched(master)
	replace ISO_final_shareholder = iso3 if _merge == 3
	drop iso3 _merge

	* Renaming columns
	rename ISO_final_shareholder bvd_iso_country_code_shareholder
	rename ISO_final_subsidiary bvd_iso_country_code_subsidiary
	rename directonlyfigures direct
	rename totalonlyfigures total

	* Some straightforward adjustments to GUO50
	replace guo50 = "" if guo50 == "NA"
	replace guo50 = bvdid if missing(guo50) & bvdid == shareholderbvdid & (direct == 100 | total == 100)
	cap drop missing_guo50
	gen missing_guo50 = 0
	replace missing_guo50 = 1 if missing(guo50)
	drop if missing(guo50)

	* Parse date
	tostring informationdate, replace
	gen informationdate_parsed = date(informationdate, "YMD")
	drop informationdate
	rename informationdate_parsed informationdate
	format informationdate %td

	* Process identifiers for SUB-GUO50 unique links only
	process_identifiers "bvdid guo50"

	* Store output, both in full and in compact versions
	save "$temp/orbis/shareholder_info_`country'.dta", replace
	keep bvdid bvd_iso_country_code_subsidiary
	rename bvd_iso_country_code_subsidiary bvd_iso_country_code
	duplicates drop bvdid bvd_iso_country_code, force
	save "$temp/orbis/country/shareholder_info_`country'.dta", replace
	use "$temp/orbis/shareholder_info_`country'.dta", clear
	keep bvdid bvdid_cusip6 guo50_cusip6 file informationdate guo50
	keep if ~missing(bvdid_cusip6) & ~missing(guo50_cusip6)
	save "$temp/orbis/compact/shareholder_info_`country'.dta", replace
}
else {
	di "Skipping `country' as data is absent (this is expected for a subset of countries)"
}

log close
