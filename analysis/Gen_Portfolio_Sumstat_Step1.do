* Gen_Portfolio_Sumstat_Step1.do takes as an input the files HD_`yr'_m.dta that were created in the build and saves them as portfolio_sumstat_`year'_y, where `year' takes on the year's value. 
* It is run in parallel, separately for each year. Much like the HD_foranalysis files that are created as an output from Gen_HD_Sumstats.do, this file is used for all
* subsequent analysis, but unlike HD_foranalysis files, it is used for those that are security-level (such as our regressions), rather than country level. It therefore
* collapses as much of the data as possible (see the "keep" on line 17) while preserving what we need in those analyses. After generating these files for each year in parallel, we run
* Gen_Portfolio_Sumstat_Step2.do to aggregate these into one file called portfolio_sumstat_y.dta.

cap log close
log using "$logs/${whoami}_Gen_Portfolio_Sumstat_Step1_`1'", replace
local j = `1' + $firstyear - 1

di "`j'"
use "$mns_data/output/HoldingDetail/HD_`j'_y.dta", clear
keep $country_bg_name $cusip6_bg_name cgs_domicile DomicileCountryId iso_country_code mns_class currency_id maturitydate date date_m mns_subclass cusip cusip6 externalname marketvalue lcu_per_usd_eop coupon fuzzy
if $fuzzy==1 {
	di "DROPPING FUZZY"
	drop if fuzzy==1
}
drop fuzzy
gen country_bg=$country_bg_name
gen cusip6_bg=$cusip6_bg_name
merge m:1 cusip6 using $output/industry/Industry_classifications_master, keepusing(firm_type) nogen keep(1 3)
rename (firm_type cusip6 cusip6_bg) (firm_type_nobg cusip6_orig cusip6)
merge m:1 cusip6 using $output/industry/Industry_classifications_master, keepusing(firm_type) nogen keep(1 3)
replace firm_type = firm_type_nobg if missing(firm_type) & !missing(firm_type_nobg)
replace firm_type = 0 if inlist(mns_subclass,"A","S","LS")
replace cusip6 = cusip6_orig if missing(cusip6) & !missing(cusip6_orig)
replace iso_country_code=country_bg if !missing(country_bg)
replace mns_class = "B" if iso_country_code=="XSN"
replace mns_subclass = "SV" if iso_country_code=="XSN"
drop cusip6_orig country_bg firm_type_nobg
gen marketvalue_usd = marketvalue/lcu_per_usd_eop
replace currency_id="E" if mns_class=="E"
gen maturity_type="long"
replace maturity_type="short" if maturitydate-date<365
replace maturity_type="medium" if maturitydate-date<365*5 & maturity_type~="short"
keep if month(dofm(date_m))==12
gen date_y = year(dofm(date_m))
format %ty date_y
drop date_m date lcu_per_usd_eop marketvalue
sort DomicileCountryId iso_country_code date_y
merge m:1 DomicileCountryId date_y using $resultstemp/keeper_cty_dates_y, keep(3) nogen
forvalues k = 1(1)3 {
	cap replace iso_country_code = "EMU" if inlist(iso_country_code,${eu`k'})
	cap replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,${eu`k'})
} 	
gen keeper = 1 if inlist(DomicileCountryId,$ctygroupA1)
replace keeper = 1 if inlist(DomicileCountryId,$ctygroupA2)
keep if keeper==1
drop keeper

if $drop_emu==1{
	di "DROPPING EMU"
	forvalues k = 1(1)3 {
		drop if inlist(DomicileCountryId,${eu`k'})
	}
}

save $resultstemp/portfolio_sumstat_`j'_y, replace

log close
