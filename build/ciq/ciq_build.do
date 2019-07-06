* --------------------------------------------------------------------------------------------------
* CIQ_Build
*
* This file builds the Capital IQ data used in the CMNS ultimate parent aggregation algorithm. The
* primary input file is $temp/ciq/ciq_bg_cusip.dta, which is used in UP_Aggregation.py.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_CIQ_Build", replace

* --------------------------------------------------------------------------------------------------
* Import the Capital IQ data
* --------------------------------------------------------------------------------------------------

* List of CIQ country names mapped to country codes
use "$raw/ciq/ciqcountry.dta", clear
mmerge country using  "$raw/macro/Concordances/codes.dta", umatch(country_name) ukeep(iso_country_code)
drop if _merge==2
replace country="Curacao" if countryId==242
replace iso_country_code="ATG" if country=="Antigua & Barbuda"
replace iso_country_code="BOL" if country=="Bolivia"
replace iso_country_code="BIH" if country=="Bosnia-Herzegovina"
replace iso_country_code="VGB" if country=="British Virgin Islands"
replace iso_country_code="BRN" if country=="Brunei"
replace iso_country_code="CUW" if country=="Curacao"
replace iso_country_code="COD" if country=="Democratic Republic of the Congo"
replace iso_country_code="FLK" if country=="Falkland Islands"
replace iso_country_code="IRN" if country=="Iran"
replace iso_country_code="CIV" if country=="Ivory Coast"
replace iso_country_code="LAO" if country=="Laos"
replace iso_country_code="MAC" if country=="Macau"
replace iso_country_code="MKD" if country=="Macedonia"
replace iso_country_code="MDA" if country=="Moldova"
replace iso_country_code="ANT" if country=="Netherlands Antilles"
replace iso_country_code="PRK" if country=="North Korea"
replace iso_country_code="PSE" if country=="Palestinian Authority"
replace iso_country_code="COD" if country=="Republic of the Congo"
replace iso_country_code="REU" if country=="Reunion"
replace iso_country_code="RUS" if country=="Russia"
replace iso_country_code="KNA" if country=="Saint Kitts & Nevis"
replace iso_country_code="VCT" if country=="Saint Vincent & Grenadines"
replace iso_country_code="KOR" if country=="South Korea"
replace iso_country_code="SYR" if country=="Syria"
replace iso_country_code="TWN" if country=="Taiwan"
replace iso_country_code="TZA" if country=="Tanzania"
replace iso_country_code="TTO" if country=="Trinidad & Tobago"
replace iso_country_code="TCA" if country=="Turks & Caicos Islands"
replace iso_country_code="VAT" if country=="Vatican City"
replace iso_country_code="VEN" if country=="Venezuela"
replace iso_country_code="VNM" if country=="Vietnam"
replace iso_country_code="CHA" if country=="Channel Islands"
drop if iso_co==""
drop _merge
duplicates drop
save "$temp/ciq/ciqcountry_codes.dta", replace

* Import the Capital IQ ultimate-parent mapping data
import delimited "$raw/ciq/cusip6_ultimateparents.csv", encoding(ISO-8859-1) clear
rename ultparent_cusip6 ciqup_cusip6
drop if ciqup_cusip6==""
drop if ciqup_cusip6=="nan"
drop if ciqup_cusip6=="(Invalid Identifier)"

* Fix Curacao country name formatting
replace ultparent_country="Curacao" if ultparent_country=="Curaao"
replace country="Curacao" if country=="Curaao"
mmerge ultparent_country using "$temp/ciq/ciqcountry_codes.dta", umatch(country) ukeep(iso_country_code) uname("ciqup_")
rename ciqup_iso_country_code ciqup_country
drop if _merge==2

* Merge in country names
mmerge country using "$temp/ciq/ciqcountry_codes.dta", umatch(country) ukeep(iso_country_code) uname("ciq_")
drop country
rename ciq_iso_country_code ciq_country
drop if _merge==2
save "$temp/ciq/ciq_building_temp.dta", replace

* Perform a consistency and modal assignement for country on CIQ ultimate parents
use "$temp/ciq/ciq_building_temp.dta", clear
keep ciqup_cusip6 ciqup_country
gen counter = 1 if !missing(ciqup_country)
bysort ciqup_cusip6 ciqup_country: egen country_count=sum(counter)
drop counter
collapse (firstnm) country_count, by(ciqup_cusip6 ciqup_country)

* Rank the frequency of each country by CUSIP. Rank 1 are the most frequently assigned countries. Ties are all assigned the same rank
bysort ciqup_cusip6: egen country_count_rank=rank(-country_count), track
drop if country_count_rank>2
gen country_cusip_nfp=1
replace country_cusip_nfp= 0 if (inlist(ciqup_country,$tax_haven1) | inlist(ciqup_country,$tax_haven2) | inlist(ciqup_country,$tax_haven3) ///
	| inlist(ciqup_country,$tax_haven4)| inlist(ciqup_country,$tax_haven5)| inlist(ciqup_country,$tax_haven6))
bysort ciqup_cusip6: egen country_cusip_count_nfp=sum(country_cusip_nfp)

* Only tax havens: we choose the mode, and at random within the mode 
drop if country_cusip_count_nfp==0 & country_count_rank==2
bysort ciqup_cusip6: gen fp_rand=runiform()
bysort ciqup_cusip6: egen fp_rand_max=max(fp_rand)
drop if country_cusip_count_nfp==0 & country_count_rank==1 & fp_rand<fp_rand_max

* Mixed or only regular countries: we choose the mode if regular country, or the rank 2 if rank 1 only has THs;
* if indifferent, we pick at random within the same rank.
drop if country_cusip_count_nfp>=1 & (inlist(ciqup_country,$tax_haven1) | inlist(ciqup_country,$tax_haven2) | inlist(ciqup_country,$tax_haven3) ///
	| inlist(ciqup_country,$tax_haven4)| inlist(ciqup_country,$tax_haven5)| inlist(ciqup_country,$tax_haven6))
bysort ciqup_cusip6: egen country_count_rank_temp=rank(country_count_rank), track
drop if country_cusip_count_nfp>=1 & country_count_rank_temp>=2
drop fp_rand_max
bysort ciqup_cusip6: egen fp_rand_max=max(fp_rand)
drop if country_cusip_count_nfp>=1 & country_count_rank_temp==1 & fp_rand<fp_rand_max
keep ciqup_cusip6 ciqup_country
save "$temp/ciq/ciq_unqiue_ciqup_country.dta", replace

* Merge in the CIQ ultimate parent unique country info
use "$temp/ciq/ciq_building_temp.dta", clear
drop ciqup_country
mmerge ciqup_cusip6 using  "$temp/ciq/ciq_unqiue_ciqup_country.dta", ukeep(ciqup_country)

* Merge in some info from CGS; we find the domicile of UP according to CGS
mmerge ciqup_cusip6 using "$temp/cgs/cgs_compact_complete.dta", umatch(issuer_num) ukeep(dom) uname("ciqup_cgs_")
drop if _merge==2

* ciqup_cgs_domicile is the domicile of the CIQ ultimate parent (according to CGS) 
* cgs_domicile is the domicile according to the CGS master file of the original cusip6
mmerge cusip6 using "$temp/cgs/cgs_compact_complete.dta", umatch(issuer_num) ukeep(dom) uname("cgs_")
drop if _merge==2

* Note there are CUSIPs in CIQ that are not in CGS in the two operations above; we left them in
label var ciqup_country "CIQ Ultimate Parent Country"
label var ciqup_cgs_domicile "CIQ Ultimate Parent Country According to CGS"
label var ciq_country "CIQ Original Country"
label var cgs_domicile "Country According to CGS"
drop if cusip6=="" | cusip6=="000000"
drop if ciqup_cusip6=="" | ciqup_cusip6=="000000"
save "$temp/ciq/cusip6_ultimateparents.dta", replace

* CIQ maps same cusip6 into multiple ciqup_cusip6. We select a unique match below
use "$temp/ciq/cusip6_ultimateparents.dta", clear
duplicates drop ciqup_cusip6 cusip6 ciqup_country, force
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count_cusip6=sum(counter)
drop counter

* If the procedure returned original, go with other one
drop if cusip6==ciqup_cusip6 & count_cusip6>1
drop count_cusip6

* Duplicates drop as long as cusip6 and ciqup_country is the same
duplicates drop cusip6 ciqup_country, force

* Drop duplicates that don't match with CGS
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count_cusip6=sum(counter)
drop counter
gen cgs_match=0
replace cgs_match=1 if ciqup_country==cgs_dom
order cgs_match
bysort cusip6: egen max_cgs_max=max(cgs_match)
drop if count>1 & cgs_match==0 & max_cgs_max==1
drop count_cusip6 cgs_match max_cgs_max

* All that is now left is 34 structured finance vehicles and funds with several sponsoring banks; keeping the first
gen counter=1 if !missing(cusip6)
bysort cusip6: egen count_cusip6=sum(counter)
drop counter
bysort cusip6: gen n=_n
keep if n==1
drop n count
save "$temp/ciq/cusip6_ultimateparents_unique.dta", replace

* Keep only relevant variables
use "$temp/ciq/cusip6_ultimateparents_unique.dta", clear
keep ciqup_country cusip6 ciqup_cusip6 
rename ciqup_country ciq_country_bg
save "$temp/ciq/ciq_bg_cusip.dta", replace

* Generate consolidated dataset of CIQ company names; for simplicity we take the first record when duplicated
import delimited "$raw/ciq/cusip6_ultimateparents.csv", encoding(ISO-8859-1) clear
tempfile ciq_names_temp1
keep cusip6 companyname
drop if cusip6 == ""
save "`ciq_names_temp1'", replace
import delimited "$raw/ciq/cusip6_ultimateparents.csv", encoding(ISO-8859-1) clear
tempfile ciq_names_temp2
keep ultparent_cusip6 ultparent_name
rename ultparent_cusip6 cusip6
rename ultparent_name companyname
drop if cusip6 == ""
save "`ciq_names_temp2'", replace
use "`ciq_names_temp1'", clear
append using "`ciq_names_temp2'"
collapse (firstnm) companyname, by(cusip6)
save "$temp/ciq/ciq_up_names.dta", replace

log close
