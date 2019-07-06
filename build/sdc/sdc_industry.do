* --------------------------------------------------------------------------------------------------
* SDC_Industry
* 
* This job imports and consolidates company-level industry categorization data from the SDC Platinum
* new issues database.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_sdc_industry", replace

* Gather industry information from SDC loans data
use "$sdc_datasets/sdc_loans_appended", clear
gen cusip6=CUSIP
drop if cusip6==""
rename PrimarySICCode MainSICCode
gen sic_str=MainSICCode
destring sic_str, force replace
order sic_str MainSICCode    
keep cusip6 MainSICCode
drop if cusip6==""
drop if MainSIC==""
duplicates drop Main cusip6, force
bysort cusip6: egen count=count(cusip6)
bysort cusip6: gen n=_n
drop if n>1
drop n
gen type="C"
replace type="S" if substr(MainSIC,1,3)=="999"
replace type="SV" if MainSIC=="999G"
replace type="SF" if MainS=="619B" | MainS=="619A"
replace MainS="4911" if MainS=="499A"
duplicates drop
forvalues x=1/3 {
	gen sic`x'=substr(MainS,1,`x')
}
order sic*
rename Main sic4
gen loans=1
drop count
save "$sdc_datasets/sdc_sic_loans.dta", replace

* Gather industry information from SDC bond issuance data
use "$sdc_datasets/sdc_appended.dta",clear
gen cusip6=CUSIP
replace cusip6=cusip6_sdc if cusip6==""
replace cusip6=cusip6_cgs if cusip6==""
drop if cusip6=="" & UltimateParentCUSIP==""
gen sic_str=MainSIC
destring sic_str, force replace
order sic_str Main Issuer UltimateParentCUSIP  UltimateParentsPrimarySICCo
keep cusip6 MainSICCode UltimateParentsPrimarySICCo UltimateParentCUSIP
preserve
keep cusip6 Main UltimateParentsPrimarySICCo
drop if cusip6==""
drop if MainSIC==""
duplicates drop Main cusip6, force
bysort cusip6: egen count=count(cusip6)
gen same_up=1 if MainS==UltimateParentsPrimarySICCo
bysort cusip6: egen max_s=max(same_up)
drop if count>1 & same==. & max==1
drop count same max Ult*
bysort cusip6: gen n=_n
drop if n>1
drop n
gen type="C"
replace type="S" if substr(MainSIC,1,3)=="999"
replace type="SV" if MainSIC=="999G"
replace type="SF" if MainS=="619B" | MainS=="619A"
replace MainS="4911" if MainS=="499A"
duplicates drop
forvalues x=1/3 {
	gen sic`x'=substr(MainS,1,`x')
}
order sic*
rename Main sic4

* Merge the two data sources above
append using "$sdc_datasets/sdc_sic_loans.dta"
duplicates drop sic4 cusip6, force
bysort cusip6: egen count=count(cusip6)
drop if count==2 & loans==1
drop loans count
save "$sdc_datasets/sdc_sic.dta", replace
restore

* Clean the data
keep  Ult*
rename UltimateParentCUSIP cusip6
rename Ult MainSIC
drop if cusip6==""
drop if MainSIC==""
duplicates drop Main cusip6, force
bysort cusip6: gen n=_n
drop if n>1
drop n
gen type="C"
replace type="S" if substr(MainSIC,1,3)=="999"
replace type="SV" if MainSIC=="999G"
replace type="SF" if MainS=="619B" | MainS=="619A"
replace MainS="4911" if MainS=="499A"
duplicates drop
forvalues x=1/3 {
	gen sic`x'=substr(MainS,1,`x')
}
order sic*
rename Main sic4
save "$sdc_datasets/upsdc_sic.dta", replace

* Further cleaning
use "$sdc_datasets/upsdc_sic.dta", clear
gen up=1
append using "$sdc_datasets/sdc_sic.dta"
replace up=0 if up==.
bysort cusip6: egen count=count(cusip6)
drop if up==0 & count>1
drop up count
forvalues x=1/4 {
	destring sic`x', force replace
}
replace sic4=sic3 if sic4==.
save "$sdc_datasets/sdc_sic.dta", replace
cap erase "$sdc_datasets/upsdc_sic.dta"

* Prepare output, for merging with other datasets
use "$sdc_datasets/sdc_sic.dta", clear
gen sdc_firm_type=. 
replace sdc_firm_type=2 if sic1~=9 & sic1~=6
replace sdc_firm_type=1 if sic1==6
keep cusip6 sdc_firm
save "$output/industry/sdc_industry_merge.dta", replace

log close
