* --------------------------------------------------------------------------------------------------
* SDC_Loans
* 
* This job imports and consolidates raw loan data from the SDC Platinum new issues database.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_sdc_loans", replace

* Import raw XLSX data from SDC loans
fs "$sdc_loans/*.xlsx"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_loans/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xlsx","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_loandta/`temp'.dta, replace
}	

* Append the files
clear
fs "$sdc_loandta/*.dta"
foreach file in `r(files)' {
append using $sdc_loandta/`file', force
}

* Merge in currency tags
mmerge Curr using "$sdc_additional/sdc_cgs_currmapping.dta", uname(cgs_)
replace Curr=cgs_cgs_curr if cgs_cgs_curr~=""
drop cgs_cgs_curr

* Merge in country tags and save output
preserve
tempfile nation
keep Nation
duplicates drop
drop if Nation==""
sort Na
mmerge Nation using "$output/concordances/country_names.dta", umatch(country_n)
replace iso="ANT" if Nation=="Neth Antilles"
replace iso="BOL" if Nation=="Bolivia"
replace iso="BIH" if Nation=="Bosnia"
replace iso="VGB" if Nation=="British Virgin"
replace iso="CZE" if Nation=="Czechoslovakia"
replace iso="DOM" if Nation=="Dominican Rep"
replace iso="GNQ" if Nation=="Equator Guinea"
replace iso="PYF" if Nation=="Fr Polynesia"
replace iso="IRN" if Nation=="Iran"
replace iso="IRL" if Nation=="Ireland-Rep"
replace iso="CIV" if Nation=="Ivory Coast"
replace iso="LAO" if Nation=="Laos"
replace iso="MAC" if Nation=="Macau"
replace iso="MKD" if regexm(Nation,"Macedonia")==1
replace iso="MHL" if regexm(Nation,"Marshall I")==1
replace iso="FSM" if regexm(Nation,"Micronesia")==1
replace iso="MDA" if regexm(Nation,"Moldova")==1
replace iso="XSN" if regexm(Nation,"Multi-National")==1
replace iso="PNG" if regexm(Nation,"Papua N")==1
replace iso="RUS" if regexm(Nation,"Russia")==1
replace iso="SVK" if regexm(Nation,"Slovak")==1
replace iso="KOR" if regexm(Nation,"South Korea")==1
replace iso="LCA" if regexm(Nation,"St Lucia")==1
replace iso="SUR" if regexm(Nation,"Surinam")==1
replace iso="SYR" if regexm(Nation,"Syria")==1
replace iso="TWN" if regexm(Nation,"Taiwan")==1
replace iso="TTO" if regexm(Nation,"Trinidad")==1
replace iso="TCA" if regexm(Nation,"Turks/Caicos")==1
replace iso="UAE" if regexm(Nation,"Utd Arab")==1
replace iso="VEN" if regexm(Nation,"Venezuela")==1
replace iso="VNM" if regexm(Nation,"Vietnam")==1
drop if iso==""
drop _merge
save "`nation'"
restore
mmerge Nation using "`nation'"
drop if _merge==2
tab Nation if iso==""
save "$sdc_datasets/sdc_loans_appended", replace

log close
