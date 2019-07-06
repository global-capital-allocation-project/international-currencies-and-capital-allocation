* --------------------------------------------------------------------------------------------------
* SDC_Build
*
* This job reads in and builds global security issuance data from the SDC Platinum new issues
* database by Refinitiv. The raw data is obtained via direct downloads from the SDC Platinum
* terminal and covers all of bonds, equities, and loans.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_SDC_Build", replace

* Set up CUSIP-ISIN mapping from CGS master file
use issuer_num issue_num issue_check isin curr using "$mns_data/temp/cgs/ALLMASTER_ISIN.dta", clear
cap tostring issue_check, replace
gen cusip9=issuer_num+issue_num+issue_check
drop issuer_num issue_num issue_check
drop if isin==""
rename curr cgs_currency
save "$sdc_additional/cgs_isin.dta", replace

* Program to clean nation field
cap program drop clean_nation_field
program define clean_nation_field
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
end

* --------------------------------------------------------------------------------------------------
* Process the SDC bond issuance data
* --------------------------------------------------------------------------------------------------

* Import the raw SDC XLSX files; bonds issues
fs "$sdc_bonds/*.xlsx"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_bonds/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xlsx","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_dta/`temp'.dta, replace
}

* Import the raw SDC XLS files; bonds issues
fs "$sdc_bonds/*.xls"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_bonds/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xls","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_dta/`temp'.dta, replace
}

* Append the SDC files
clear
fs "$sdc_dta/*.dta"
foreach file in `r(files)' {
	append using $sdc_dta/`file', force
}

* Update CUSIP9 using CUSIP master file
mmerge ISIN using "$sdc_additional/cgs_isin.dta", umatch(isin)
rename cusip9 cusip9_cgs
drop if _merge==2
rename Digit cusip9_sdc
gen cusip6_cgs = substr(cusip9_cgs,1,6)
gen cusip6_sdc = substr(cusip9_sdc,1,6)
order CUSIP UltimateParentCUSIP cusip6*
gen data_date=subinstr(file,"_gv","",.)
split data_date,p("Q")
replace data_date2="Q"+data_date2 if data_date1==""
replace data_date3="Q"+data_date3 if data_date1==""
replace data_date1=subinstr(data_date1,"s","",.)
replace data_date2="Q1"+data_date1 if data_date2==""
replace data_date3="Q4"+substr(data_date1,1,3)+"9" if data_date3==""
gen start_date_str=substr(data_date2,3,6)+substr(data_date2,1,2)
gen end_date_str=substr(data_date3,3,6)+substr(data_date3,1,2)
drop data_date*
gen end_date=quarterly(end_date_str,"YQ",2017)
format end_date %tq
gen start_date=quarterly(start_date_str,"YQ",2017)
format start_date %tq
gen issue_date=date(IssueDate,"DMY")
gen maturity_date=date(FinalMat,"DMY")
format issue_date %td
format maturity_date %td
drop E U AC AG

* Prepare SDC currency mapping file
preserve
tempfile curr
drop if cgs_cu=="" | Cur==""
gen n=_n
collapse (count) n, by(cgs Currency)
drop if cgs==""
drop if Curr==""
bysort Curr: egen max=max(n)
keep if n==max
gen counter = 1 if !missing(Curr)
bysort Curr: egen curr_count=sum(counter)
drop counter
drop if Curr=="ESC" & cgs~="PTE"
drop if Curr=="UP" & cgs=="USD"
drop curr_count
keep Curr cgs_curr
save "$sdc_additional/sdc_cgs_currmapping.dta", replace

* Merge in the currency mapping data
restore
mmerge Curr using "$sdc_additional/sdc_cgs_currmapping.dta", uname(cgs_)
replace Curr=cgs_cgs_curr if cgs_cgs_curr~=""
drop cgs_cgs_curr

* Clean nation field
preserve
tempfile nation
keep Nation
duplicates drop
drop if Nation==""
sort Na
mmerge Nation using "$output/concordances/country_names.dta", umatch(country_n)
clean_nation_field
drop if iso==""
drop _merge
save "`nation'"
restore
mmerge Nation using "`nation'"
drop if _merge==2
tab Nation if iso==""
save "$sdc_datasets/sdc_appended", replace

* Create CUSIP6 to ultimate parent CUSIP6 using data internal to SDC: we do this
* using various fields to map each issuer to a cusip6 code. 

* This step uses information in the CUSIP field, which is a company-level
* six-digit identifier assigned within SDC
use "$sdc_datasets/sdc_appended", clear
keep CUSIP UltimateParentCUSIP start_date
rename CUSIP cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/temp1.dta", replace

* This step uses information in the cusip6_sdc field, which corresponds to
* the first six digits of the cusip9 listed for a given security, if any
use "$sdc_datasets/sdc_appended", clear
keep cusip6_sdc UltimateParentCUSIP start_date
rename cusip6_sdc cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/temp2.dta", replace

* This step uses information in the ImmediateParentCUSIP field, which
* is analogous to the CUSIP field, but for the issuer's immediate parent
use "$sdc_datasets/sdc_appended", clear
keep ImmediateParentCUSIP UltimateParentCUSIP start_date
rename ImmediateParentCUSIP cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/temp3.dta", replace

* This step uses information in the cusip6_cgs field, which uses the first
* six digits obtained from mapping a given security's ISIN to a cusip9, if any
use "$sdc_datasets/sdc_appended", clear
keep cusip6_cgs UltimateParentCUSIP start_date
rename cusip6_cgs cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/temp4.dta", replace

* Append all the above
use "$sdc_datasets/temp1.dta", clear
append using "$sdc_datasets/temp2.dta"
append using "$sdc_datasets/temp3.dta"
append using "$sdc_datasets/temp4.dta"
sort cusip6 cusip6_bg start
drop if cusip6==""
collapse (lastnm) start, by(cusip6*)
save "$sdc_datasets/sdc_cusip6_bg_list_debt.dta", replace

* Keep most recent mapping information only
use "$sdc_datasets/sdc_cusip6_bg_list_debt.dta", clear
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
bysort cusip6: egen max_date=max(start)
drop if start~=max & count>1
drop count max
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter

* Keep a unique record for each cusip6
bysort cusip6: gen n=_n
drop if n>1
keep cusip6*
save "$sdc_datasets/sdc_cusip6_bg_debt.dta", replace

* --------------------------------------------------------------------------------------------------
* Process the SDC equity issuance data
* --------------------------------------------------------------------------------------------------

* Import XLSX files for SDC equities
fs "$sdc_equities/Equities*.xlsx"
foreach file in `r(files)' {
	display "`file'"
	import excel $sdc_equities/`file', sheet("Request 2") allstring firstrow clear
	local temp=subinstr("`file'",".xlsx","",.)
	gen file="`temp'"
	display "`temp'"
	save $sdc_eqdta/`temp'.dta, replace
}	

* Append the files
clear
fs "$sdc_eqdta/Equities*.dta"
foreach file in `r(files)' {
	append using $sdc_eqdta/`file', force
}

* Update CUSIP9 using CUSIP master file
mmerge ISIN using "$sdc_additional/cgs_isin.dta", umatch(isin)
rename cusip9 cusip9_cgs
drop if _merge==2
rename Digit cusip9_sdc
gen cusip6_cgs = substr(cusip9_cgs,1,6)
gen cusip6_sdc = substr(cusip9_sdc,1,6)
order CUSIP UltimateParentCUSIP cusip6*
gen data_date=subinstr(file,"Equities_","",.)
replace data_date="1970" if regexm(data_date,"1970")==1
replace data_date="2000" if regexm(data_date,"2000")==1
replace data_date="2008" if regexm(data_date,"2008")==1
replace data_date="2013" if regexm(data_date,"2013")==1
destring data_date, replace
gen start_date=data_date
replace start_date=qofd(dofy(start_date))
format start_date %tq

* Clean nation field
preserve
tempfile nation
keep Nation
duplicates drop
drop if Nation==""
sort Na
mmerge Nation using "$output/concordances/country_names.dta", umatch(country_n)
clean_nation_field
drop if iso==""
drop _merge
save "`nation'"
restore
mmerge Nation using "`nation'"
drop if _merge==2 | (_merge==-1 & Issuer=="")
tab Nation if iso==""
save "$sdc_datasets/sdc_eq_appended", replace

* Create CUSIP6 to ultimate parent CUSIP6 using data internal to SDC: we do this
* using various fields to map each issuer to a cusip6 code. 

* This step uses information in the CUSIP field, which is a company-level
* six-digit identifier assigned within SDC
use "$sdc_datasets/sdc_eq_appended", clear
keep CUSIP UltimateParentCUSIP start_date
rename CUSIP cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/eq_temp1.dta", replace

* This step uses information in the cusip6_sdc field, which corresponds to
* the first six digits of the cusip9 listed for a given security, if any
use "$sdc_datasets/sdc_eq_appended", clear
keep cusip6_sdc UltimateParentCUSIP start_date
rename cusip6_sdc cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/eq_temp2.dta", replace

* This step uses information in the ImmediateParentCUSIP field, which
* is analogous to the CUSIP field, but for the issuer's immediate parent
use "$sdc_datasets/sdc_eq_appended", clear
keep ImmediateParentCUSIP UltimateParentCUSIP start_date
rename ImmediateParentCUSIP cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/eq_temp3.dta", replace

* This step uses information in the cusip6_cgs field, which uses the first
* six digits obtained from mapping a given security's ISIN to a cusip9, if any
use "$sdc_datasets/sdc_eq_appended", clear
keep cusip6_cgs UltimateParentCUSIP start_date
rename cusip6_cgs cusip6 
rename Ult cusip6_bg
duplicates drop
save "$sdc_datasets/eq_temp4.dta", replace

* Append all the above
use "$sdc_datasets/eq_temp1.dta", clear
append using "$sdc_datasets/eq_temp2.dta"
append using "$sdc_datasets/eq_temp3.dta"
append using "$sdc_datasets/eq_temp4.dta"
sort cusip6 cusip6_bg start
drop if cusip6==""
collapse (lastnm) start, by(cusip6*)
save "$sdc_datasets/sdc_cusip6_bg_list_eq.dta", replace

* Keep most recent mapping information only
use "$sdc_datasets/sdc_cusip6_bg_list_eq.dta", clear
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
bysort cusip6: egen max_date=max(start)
drop if start~=max & count>1
drop count max
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
bysort cusip6: gen n=_n
drop if n>1
keep cusip6*
save "$sdc_datasets/sdc_cusip6_bg_eq.dta", replace

* --------------------------------------------------------------------------------------------------
* Merging debt and equity data
* --------------------------------------------------------------------------------------------------
use "$sdc_datasets/sdc_cusip6_bg_debt.dta", clear
mmerge cusip6 using  "$sdc_datasets/sdc_cusip6_bg_eq.dta", uname(eq_)
replace cusip6_bg=eq_cusip6 if eq_c~=""
keep cusip6*
save "$sdc_datasets/sdc_cusip6_bg.dta", replace

* --------------------------------------------------------------------------------------------------
* Process the country field
* --------------------------------------------------------------------------------------------------
use cusip6* CUSIP iso start_date using "$sdc_datasets/sdc_eq_appended", clear
append using "$sdc_datasets/sdc_appended.dta", keep(cusip6* CUSIP iso start_date)
keep cusip6* CUSIP iso start_date
drop if iso==""
drop if cusip6_cgs=="" & cusip6_sdc=="" & CUSIP==""
duplicates drop
tempfile cgs sdc cusip 
preserve
keep cusip6_cgs iso start
drop if cusip6_cgs==""
duplicates drop
rename cusip6 cusip6
save "`cgs'", replace
restore
preserve
keep cusip6_sdc iso start 
drop if cusip6_sdc==""
duplicates drop
rename cusip6 cusip6 
save "`sdc'", replace

restore
keep CUSIP iso start
drop if CUSIP==""
duplicates drop
rename CUSIP cusip6 
save "`cusip'", replace

use "`cusip'", clear
append using "`cgs'"
append using "`sdc'"
sort cusip6 start
collapse (lastnm) start, by(cusip6 iso)
duplicates drop 

* Flag tax havens
gen tax_haven=0
replace tax_haven=1 if inlist(iso,$tax_haven1)==1 | inlist(iso,$tax_haven2)==1 | inlist(iso,$tax_haven3)==1 ///
	| inlist(iso,$tax_haven4)==1 | inlist(iso,$tax_haven5)==1 | inlist(iso,$tax_haven6)==1 
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
bysort cusip6: egen nontaxhaven_cusip6=min(tax_haven)
drop if count>1 & tax_haven==1 & nontaxhaven_cusip6==0
drop count

bysort cusip6: egen latest_start=max(start)
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
format latest %tq
drop if count>1 & start~=lat
drop count
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
gen multiple=0
replace multiple=1 if count>1
bysort cusip6: gen n=_n
keep if n==1
drop latest count n start non
save "$sdc_additional/cusip6_country_sdc.dta", replace

* --------------------------------------------------------------------------------------------------
* Gather governing law data for bond issues
* --------------------------------------------------------------------------------------------------

* Read in governing law data
use "$sdc_datasets/sdc_appended", clear
keep cusip9_sdc ISIN GoverningLaw
drop if missing(cusip9_sdc) & missing(ISIN)
drop if missing(GoverningLaw)
rename ISIN isin
mmerge isin using  "$sdc_additional/cgs_isin.dta", ukeep(cusip9) unmatched(m)
replace cusip9_sdc = cusip9 if missing(cusip9_sdc)
drop _merge cusip9
rename cusip9_sdc cusip
mmerge GoverningLaw using $raw/macro/Concordances/iso2_iso3, unmatched(m) umatch(iso2)
replace GoverningLaw = iso3 if _merge == 3
replace GoverningLaw = "GBR" if GoverningLaw == "UK"
replace GoverningLaw = "LUX" if GoverningLaw == "LX"
replace GoverningLaw = "GRD" if GoverningLaw == "WG"
replace GoverningLaw = "SWE" if GoverningLaw == "SW"
replace GoverningLaw = "ESP" if GoverningLaw == "SP"
replace GoverningLaw = "BGR" if GoverningLaw == "BU"
replace GoverningLaw = "LKA" if GoverningLaw == "CE"
replace GoverningLaw = "CAF" if GoverningLaw == "CT"
replace GoverningLaw = "PRT" if GoverningLaw == "PO"
replace GoverningLaw = "DNK" if GoverningLaw == "DN"
drop if strlen(GoverningLaw) == 2
drop _merge
drop iso3
rename GoverningLaw governing_law
drop if missing(cusip)
drop isin
bysort cusip: egen nCusip = nvals(governing_law)
drop if nCusip > 1
drop nCusip
bysort cusip: keep if _n == 1

* Consolidate EMU countries and save output
foreach var in "governing_law" {
	replace `var' = "EMU" if inlist(`var', $eu1)
	replace `var' = "EMU" if inlist(`var', $eu2)
	replace `var' = "EMU" if inlist(`var', $eu3)
}
save "$sdc_datasets/sdc_governing_law", replace

log close
