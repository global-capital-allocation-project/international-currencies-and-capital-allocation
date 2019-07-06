* --------------------------------------------------------------------------------------------------
* External_Industry
*
* This job compiles a dataset with company-level industry assignments from external sources. The
* external sources are Capital IQ and Compustat. This file also builds a master reference file
* for GICS codes. The merged external industry data is next merged with industry assignments
* internal to Morningstar in Internal_Industry.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_External_Industry", replace

* --------------------------------------------------------------------------------------------------
* Set up GICS codes file
* --------------------------------------------------------------------------------------------------
tempfile gics
cap mkdir "$raw/ciq"
import excel "$raw/ciq/gics_structure.xls", sheet("Effective close of Aug 31,2016") clear
save "`gics'", replace

* Read in sectors
use "`gics'", clear
keep A B
rename A sector_num 
rename B sector_name
drop if _n<5
drop if sector_num==""
destring sector_num, replace
duplicates drop
save $ciq_industry/sector.dta, replace

* Read in industry groups
use "`gics'", clear
keep C D
rename C industry_group_num 
rename D industry_group_name
drop if _n<5
drop if industry_group_num==""
destring industry_group_num, replace
duplicates drop
save $ciq_industry/industry_group.dta, replace

* Read in industries
use "`gics'", clear
keep E F
rename E industry_num 
rename F industry_name
drop if _n<5
drop if industry_num==""
destring industry_num, replace
duplicates drop
save $ciq_industry/industry.dta, replace

* Read in sub-industries
use "`gics'", clear
keep G H
rename G sub_industry_num 
rename H sub_industry_name
drop if _n<5
drop if sub_industry_num==""
destring sub_industry_num, replace
duplicates drop
save $ciq_industry/sub_industry.dta, replace

* Read in industry cross-walk
use $ciq_industry/industry.dta ,clear
gen n=_n
rename n simpleindustryid
labmask simpleindustryid, values(industry_name)
save $ciq_industry/industry_xwalk.dta, replace

* Consolidate the data
import excel "$raw/ciq/gics_structure.xls", sheet("Effective close of Aug 31,2016") clear
drop if _n<=3
drop I
local i=1
foreach x of varlist _all {
	rename `x' v`i'
	local i=`i'+1
	}
	
forvalues x=2(2)8 {
	local y=`x'-1
	replace v`x'=lower(v`y')+"_name" if _n==1
	replace v`y'=lower(v`y')+"_num" if _n==1
}

forvalues x=1/8 {
	replace v`x'=subinstr(v`x'," ","_",.)
	replace v`x'=subinstr(v`x',"-","_",.)
	local temp=v`x'[1]
	rename v`x' `temp'
}
drop if _n==1
drop if sub_industry_num==""
gen n=_n
order n
tsset n

foreach x of varlist _all {
	if "`x'"~="n" {
		carryforward `x', replace 
		}
		}
drop n	
destring sector_num, replace
destring industry_group_num, replace
destring industry_num , replace
destring sub_industry_num	, replace
labmask sector_num, values(sector_name)
labmask industry_group_num, values(industry_group_name)
labmask industry_num, values(industry_name)
labmask sub_industry_num, values(sub_industry_name)
mmerge industry_num using $ciq_industry/industry_xwalk.dta, ukeep(simpleindustryid)
save "$ciq_industry/gics_master.dta", replace

use "$ciq_industry/gics_master.dta", clear
drop sub*
duplicates drop	
save "$ciq_industry/gics_master_industry.dta", replace
	
* --------------------------------------------------------------------------------------------------
* Build industry data from Capital IQ
* --------------------------------------------------------------------------------------------------

* Read in CIQ data
import delimited "$raw/ciq/cusip6_ultimateparents.csv", encoding(ISO-8859-1) clear
save "$ciq_industry/cusip6_ultimateparents.dta", replace

* Create a dataset containing only the cusip6, ciqid and company name
use cusip6 ciqid companyname using "$ciq_industry/cusip6_ultimateparents.dta", clear
keep cusip6 ciqid companyname
replace ciqid=subinstr(ciqid,"IQ","",.)
destring ciqid, replace
drop if cusip6=="000000"
save "$ciq_industry/cusip6_ciq_name.dta", replace

* Gather all SIC codes for a given company
use companyid companyname companytypeid simpleindustryid countryid siccode using "$raw/ciq/ciqcompanysic.dta" , clear
rename sic sic

* Ignore distinctions past the SIC1 level
duplicates drop companyid simpleindustryid, force
save "$ciq_industry/ciqcompanysic_names_unique.dta", replace

* Company name, cusip6 and ciqid
use "$ciq_industry/cusip6_ciq_name.dta", clear
destring ciqid, replace force
mmerge ciqid using  "$ciq_industry/ciqcompanysic_names_unique.dta", umatch(companyid)
keep if _merge==3 
save "$ciq_industry/temp_merge.dta", replace

use "$ciq_industry/temp_merge.dta", clear
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
bysort cusip6: egen count_ind=count(simple)

* If there is more than 1 match, keep the ones with a SIC
drop if count>1 & count_ind>0 & simple==.
drop count count_ind
mmerge simpleindustryid using "$ciq_industry/gics_master_industry.dta"
drop if _merge==2
duplicates drop cusip6 industry_num, force
duplicates drop cusip6 industry_group_num, force
duplicates drop cusip6 sector_num, force
 
* Consolidate the data
mmerge companytypeid using "$raw/ciq/ciqcompanytype.dta", umatch(companyTypeId)
drop if _merge==2
gen short=substr(companyname,1,25)
order short
keep if cusip6~=""
labmask companytypeid, values(companyTypeName)
drop companyTypeName _merge 
drop sector_name industry_group_name industry_name simple
gen ind_sic=.
replace ind_sic=1 if sic>=100 & sic<=999 & sic~=.
replace ind_sic=2 if sic>=1000 & sic<=1499 & sic~=.
replace ind_sic=3 if sic>=1500 & sic<=1799 & sic~=.
replace ind_sic=4 if sic>=2000 & sic<=3999 & sic~=.
replace ind_sic=5 if sic>=4000 & sic<=4999 & sic~=.
replace ind_sic=6 if sic>=5000 & sic<=5199 & sic~=.
replace ind_sic=7 if sic>=5200 & sic<=5999 & sic~=.
replace ind_sic=8 if sic>=6000 & sic<=6799 & sic~=.
replace ind_sic=9 if sic>=7000 & sic<=8999 & sic~=.
replace ind_sic=10 if sic>=9100 & sic<=9721 & sic~=.
save "$ciq_industry/industry_merge.dta", replace

* --------------------------------------------------------------------------------------------------
* Build industry data from Compustat 
* --------------------------------------------------------------------------------------------------

* Step 1: Merge companyid gvkey isin and companyid gvkey cusip
use "$raw/ciq/wrds_gvkey.dta", clear
drop start end companyN
mmerge companyid using  "$raw/ciq/wrds_cusip.dta", ukeep(cusip)
keep if _merge==3
drop _merge
save "$temp/compustat/companyid_gvkey_cusip.dta", replace

* Step 2: Merge together g_funda and keys
tempfile g_funda_cusip funda_cusip bank_funda_cusip
use gvkey sich using "$wrds/Compustat/g_funda.dta", clear
duplicates drop
rename gvkey gv
mmerge gv using "$temp/compustat/companyid_gvkey_cusip.dta", umatch(gvkey) unmatched(none)
rename gv gvkey
drop _merge
rename sich sic
save "`g_funda_cusip'", replace

* Same for North America
use gvkey sich using "$wrds/Compustat/funda.dta", clear
duplicates drop
rename gvkey gv
mmerge gv using "$temp/compustat/companyid_gvkey_cusip.dta", umatch(gvkey) unmatched(none)
rename gv gvkey
drop _merge
rename sich sic
save "`funda_cusip'", replace

* Bank data
use gvkey sic using "$wrds/Compustat/bank_funda.dta", clear
destring sic, replace
duplicates drop
rename gvkey gv
mmerge gv using "$temp/compustat/companyid_gvkey_cusip.dta", umatch(gvkey) unmatched(none)
rename gv gvkey 
drop _merge
save "`bank_funda_cusip'", replace

* Merge the Compustat files
use "`g_funda_cusip'", clear
append using "`funda_cusip'"
append using "`bank_funda_cusip'"
save "$temp/compustat/cusip_appended.dta", replace

* The following is based on https://www.osha.gov/pls/imis/sic_manual.html
use "$temp/compustat/cusip_appended.dta", clear	
gen cusip6=substr(cusip,1,6)
duplicates drop cusip6 sic, force
gen sic_string=sic
tostring sic_string, replace
gen length=strlen(sic_string)
replace sic_string="0"+sic_string if length==3	
gen sic3=substr(sic_string,1,3)
gen sic2=substr(sic_string,1,2)
gen ind_sic=.
replace ind_sic=1 if sic>=100 & sic<=999  & sic~=.
replace ind_sic=2 if sic>=1000 & sic<=1499 & sic~=.
replace ind_sic=3 if sic>=1500 & sic<=1799 & sic~=.
replace ind_sic=4 if sic>=2000 & sic<=3999 & sic~=.
replace ind_sic=5 if sic>=4000 & sic<=4999 & sic~=.
replace ind_sic=6 if sic>=5000 & sic<=5199 & sic~=.
replace ind_sic=7 if sic>=5200 & sic<=5999 & sic~=.
replace ind_sic=8 if sic>=6000 & sic<=6799 & sic~=.
replace ind_sic=9 if sic>=7000 & sic<=8999 & sic~=.
replace ind_sic=10 if sic>=9100 & sic<=9721 & sic~=.
drop length

* Only use Compustat if we have a SIC code
drop if sic==.
drop if cusip6=="000000"
duplicates drop cusip6 sic, force
duplicates drop cusip6 sic3, force
duplicates drop cusip6 sic2, force
duplicates drop cusip6 ind_sic, force
label define sic_name  0 "Government" 1 "Agriculture" 2 "Mining" 3 "Construction" 4 "Manufacturing" 5 "Transport and Utilities" 6 "Wholesale Trade" 7 "Retail Trade" 8 "Finance, Insurance and Real Estate" 9 "Services" 10 "Public Administration" 10 "Conglomerate"
label values ind_sic sic_name

* --------------------------------------------------------------------------------------------------
* Merge Capital IQ and Compustat data
* --------------------------------------------------------------------------------------------------

* Use SIC overlap to try to remove duplicates
mmerge cusip6 using "$ciq_industry/industry_merge.dta", uname("ciq_")
replace ciq_sector_num=0 if ciq_companyt==20
replace ciq_ind_sic=0 if ciq_companyt==20
replace ind_sic=0 if ciq_companyt==20

* If there are 2, keep the one with a SIC
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
gen sic_dummy=1 if sic~=.
bysort cusip6: egen max_sic_dummy=max(sic_dummy)
drop if count>1 & max_sic_dum==1 & sic_dum~=1
drop sic_dum max_sic_dum count

gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
gen match_ind=1 if sic==ciq_sic & sic~=.
bysort cusip6: egen max_match_dummy=max(match_ind)
drop if count>1 & max_match_dummy==1 & match_ind~=1
drop match_ind max count

gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
gen match_ind=1 if ind_sic==ciq_ind_sic & ind_sic~=.
bysort cusip6: egen max_match_dummy=max(match_ind)
drop if count>1 & max_match_dummy==1 & match_ind~=1
drop match_ind max count

gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
duplicates drop cusip6 ciq_sic, force
duplicates drop cusip6 sic, force
drop count
gen counter = 1 if !missing(cusip6)
bysort cusip6: egen count=sum(counter)
drop counter
duplicates drop cusip6, force 
drop count
drop _merge sic_string
label values ciq_ind_sic sic_name
gen sic_ind_composite=ciq_ind_sic
label values sic_ind_composite sic_name
replace sic_ind_composite=ind_sic if sic_ind_comp==.

* Firm types: 1 is financials, 0 is government, 2 is non-financials
gen firm_type=2
replace firm_type=1 if ciq_sec==40 | ciq_sec==60 | sic_ind_comp==8
replace firm_type=0 if ciq_sec==0 | sic_ind_comp==0
replace firm_type=. if ciq_sec==. & sic_ind_comp==.
label define firm_type  0 "Government" 1 "Finance" 2 "Non-Fin Corp" 
label values firm_type firm_type
order cusip6 firm_type ciq_sector_num sic_ind_composite ind_sic ciq_ind_sic ciq_companytypeid
save "$output/industry/compustat_sic_merge.dta", replace
	
* Save merged data
use "$output/industry/compustat_sic_merge.dta", clear
keep cusip6 firm_type ciq_sector_num sic_ind_composite ciq_companytypeid
label var firm_type "Firm Type Dummy"
label var ciq_sector_num "GICS Sector from CIQ"
label var sic "SIC Industry Divisions via CIQ and Compustat"
label var ciq_companytypeid "Company Type via CIQ"
save "$output/industry/compustat_sic_compact.dta", replace

log close
