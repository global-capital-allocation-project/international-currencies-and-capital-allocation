* --------------------------------------------------------------------------------------------------
* Internal_Industry
*
* This job compiles a list of the modal industry assignments for each security within the Morningstar
* holdings data, as classified by each of the individual reporting funds. We look for modal industry
* assignments within funds and then across funds. This file also merges this data with the the
* external industry data built prior to it (in the External_Industry and SDC_Industry steps), 
* and outputs a consolidated industry dataset.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Internal_Industry", replace

* Append monthly files and keep only relevant variables
forvalues x=$firstyear/$lastyear {
	di "`x'" 
	append using "$output/HoldingDetail/HD_`x'_m.dta", keep(cusip6 gicsindustryid MasterPortfolioId)
	desc
} 
save $temp/forintind, replace

* Adjust MS classificaions
* MS uses the old classifications that have real estate in there
gen gics6 = substr(gicsindustryid,1,6)
replace gics6="601010" if gics6=="404010" | gics6=="404020"
replace gics6="601020" if gics6=="404030"

* Semi-conductors
replace gics6="453010" if gics6=="452050"

drop if cusip6=="" | gics6==""

* Find fund-specific modal industry assigned to each CUSIP
gen counter = 1 if !missing(gics6)
bysort cusip6 gics6 MasterPort: egen industry_fund_count=sum(counter)
collapse (firstnm) industry_fund_count, by(cusip6 gics6 MasterPort) fast
bysort cusip6 MasterPort: egen industry_fund_count_max=max(industry_fund_count)
drop if industry_fund_count<industry_fund_count_max

* Split the equally frequent
gen counter = 1 if !missing(gics6)
bysort cusip6 MasterPort: egen industry_fund_count_split=sum(counter)
drop counter
bysort cusip6 MasterPort: gen randt=runiform()
bysort cusip6 MasterPort: egen randt_max=max(randt)
drop if industry_fund_count_split>=2 & randt<randt_max
drop randt randt_max

* Find modal industry assigned to each CUSIP across funds
gen counter = 1 if !missing(gics6)
bysort cusip6 gics6: egen industry_count=sum(counter)
collapse (firstnm) industry_count, by(cusip6 gics6) fast
bysort cusip6: egen industry_count_max=max(industry_count)
drop if industry_count<industry_count_max

* Split the equally frequent
gen counter = 1 if !missing(gics6)
bysort cusip6: egen industry_count_split=sum(count)
drop counter
bysort cusip6: gen randt=runiform()
bysort cusip6: egen randt_max=max(randt)
drop if industry_count_split>=2 & randt<randt_max

* Save modal industry assignments
keep cusip6 gics6
save "$tempindustry/Internal_Industry_NonUS_US.dta", replace
rm $temp/forintind.dta

* --------------------------------------------------------------------------------------------------
* Create a conversion key between GICS6 industry codes and financial, nonfinancials
* --------------------------------------------------------------------------------------------------
	
import excel "$raw/ciq/gics_structure.xls", sheet("Effective close of Aug 31,2016") clear
keep E F
rename E gics6_num
rename F gics6_name
drop if _n<5
drop if gics6_num==""
destring gics6_num, replace
duplicates drop
gen simplegics6id =_n
labmask simplegics6id, values(gics6_name)
gen firm_type = 2
*above 67 for real estate
replace firm_type=1 if (simplegics6id>=46 & simplegics6id<=52) | simplegics6id>=67
save "$tempindustry/gics6_industries.dta", replace	
	
* --------------------------------------------------------------------------------------------------
* Create a final dataset of industry cslassifications
* --------------------------------------------------------------------------------------------------
	
use "$tempindustry/Internal_Industry_NonUS_US.dta", clear
destring gics6, replace
mmerge gics6 using "$tempindustry/gics6_industries.dta", umatch(gics6_num) ukeep(firm_type) uname(ms_)
keep if _merge==3	
drop _merge
unique cusip6
rename gics6 ms_gics6
mmerge cusip6 using "$output/industry/compustat_sic_merge.dta", ukeep(firm_type ciq_industry_num ciq_sector_num)	
drop if _merge==2 & firm_type==. & missing(ciq_industry_num) & missing(ciq_sector_num)	
drop _merge
mmerge cusip6 using "$output/industry/sdc_industry_merge.dta"
rename (firm_type ciq_industry_num ciq_sector_num) (ext_firm_type ext_gics6 ext_gics1)
gen firm_type = ms_firm_type
replace firm_type = ext_firm_type if missing(firm_type) & !missing(ext_firm_type)
replace firm_type = ext_firm_type if firm_type==1 & ext_firm_type==2

* Overwrite firm_type with SDC firm type
replace firm_type=sdc_firm if sdc_firm~=.
cap label values firm_type firm_type
save "$output/industry/Industry_classifications_master.dta", replace

log close
