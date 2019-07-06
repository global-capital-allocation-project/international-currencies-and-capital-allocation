* This file takes as an input the files HD_`yr'_m.dta that were created in the build and saves them first as HD_sumstats_m (or _q or _y) and then as HD_foranalysis_m.
* The HD_sumstats files collapse the market values to useful groups such as by month, asset class, country, currency, etc. for use in analyses that do not need security-level detail.
* The HD_foranalysis files then collapse further so the eurozone countries are all grouped together and drops those countries that lack sufficient coverage of AUM (as given by ICI).
* These files are created under three scenarios. First, with positions calculated at market exchange rates. Second, calculated with exchange rates fixed at a data supplied as an input argument. Third, without dropping countries based on ICI coverage (this isn't used anywhere in paper).

cap log close
log using "$logs/${whoami}_Gen_HD_Sumstats_`1'", replace

if "`1'" == "1" {
	local sumstat_type = "Market_FX"
}
if "`1'" == "2" {
	local sumstat_type = "Fixed_FX"
}
if "`1'" == "3" {
	local sumstat_type = "No_ICI_Drop"
}

local suffix = ""
* Generate file "for_fixed_fx" that will be used to undo market exchange rate movements for case 2.
if "`sumstat_type'"=="Fixed_FX" {
	local suffix = "_fixedfx"
	forvalues yr= $firstyear(1)$lastyear {
		di "`yr'"
		append using "$mns_data/output/HoldingDetail/HD_`yr'_m.dta", keep(iso_currency_code lcu_per_usd_eop date_m)
		duplicates drop
	}
	gen lcu_in_`2'_tmp = lcu_per_usd_eop if date_m==tm(`2')
	bys iso_currency_code: egen lcu_in_`2' = mean(lcu_in_`2'_tmp)
	collapse (lastnm) lcu_in_`2' lcu_per_usd_eop , by(iso_currency_code date_m)
	gen lcu_ratio = lcu_in_`2'/lcu_per_usd_eop
	rename iso_currency_code currency_id 
	keep date_m currency_id lcu_ratio
	save $resultstemp/for_fixed_fx, replace
}
if "`sumstat_type'"=="No_ICI_Drop" {
	local suffix = "_no_ici_drop"
}
* Collapse data to cells by categories shown in Line 70
forvalues yr = $firstyear(1)$lastyear {
	di "`yr'"
	use $country_bg_name $cusip6_bg_name iso_currency_code BroadCategoryGroup iso_country_code currency_id DomicileCountryId mns_class mns_subclass date_m maturitydate marketvalue date lcu_per_usd_eop cusip6 fundtype_mstar fuzzy using "$mns_data/output/HoldingDetail/HD_`yr'_m.dta", clear
	if $fuzzy==1 {
		drop if fuzzy==1
	}
	drop fuzzy
	gen country_bg=$country_bg_name 
	gen cusip6_bg=$cusip6_bg_name 
	replace mns_class = "B" if iso_country_code=="XSN"
	replace mns_subclass = "SV" if iso_country_code=="XSN"
	merge m:1 cusip6 using $output/industry/Industry_classifications_master, keepusing(firm_type) nogen keep(1 3)
	rename firm_type firm_type_nobg
	drop cusip6
	rename cusip6_bg cusip6
	merge m:1 cusip6 using $output/industry/Industry_classifications_master, keepusing(firm_type) nogen keep(1 3)
	replace firm_type = firm_type_nobg if missing(firm_type) & !missing(firm_type_nobg)
	replace firm_type = 0 if inlist(mns_subclass,"A","S")
	if "`sumstat_type'"=="Fixed_FX" {
		merge m:1 currency_id date_m using "$resultstemp/for_fixed_fx.dta", keep(1 3) nogen
	}
	replace iso_country_code=country_bg if !missing(country_bg)
	gen marketvalue_usd = marketvalue/lcu_per_usd_eop/10^9
	if "`sumstat_type'"=="Fixed_FX" {
		replace marketvalue_usd = marketvalue_usd/lcu_ratio/10^9
	}
	replace currency_id="E" if mns_class=="E"
	gen maturity_type="long"
	replace maturity_type="short" if maturitydate-date<365
	replace maturity_type="medium" if maturitydate-date<365*5 & maturity_type~="short"
	collapse (sum) marketvalue_usd, by(BroadCategoryGroup DomicileCountryId mns_class mns_subclass iso_country_code currency_id maturity_type date_m firm_type fundtype_mstar)
	save $resultstemp/HD_`yr'_m_sumstats`suffix', replace
}
clear
forvalues yr = $firstyear(1)$lastyear {
	append using $resultstemp/HD_`yr'_m_sumstats`suffix'
}

merge m:1 iso_country_code using $output/concordances/country_names, keep(1 3) nogen
save $resultstemp/HD_sumstats_m`suffix', replace
* Generate end-of-quarter and end-of-year versions of same dataset
gen month = month(dofm(date_m))
keep if month==3 | month==6 | month==9 | month==12
gen date_q = yq(year(dofm(date_m)),quarter(dofm(date_m)))
format %tq date_q
drop month date_m
save $resultstemp/HD_sumstats_q`suffix', replace
gen quarter = quarter(dofq(date_q))
keep if quarter==4
gen date_y = year(dofq(date_q))
format %ty date_y
drop quarter date_q
save $resultstemp/HD_sumstats_y`suffix', replace

* Input data for use in checking whether coverage of AUM relative to ICI is sufficient
use $output/ici_data/NonUS_ICI_sumstats_q, clear
encode country_name, gen(country_coded)
tsset country_coded date_q, quarterly
tsfill
drop country_coded
replace country_name=country_name[_n-1] if missing(country_name) & date_q==date_q[_n-1]+1 
foreach var in "total" "equity" "bond" "mm" "hybrid" "other" {
	gen ln_`var'= ln(`var')
	bys country_name: ipolate ln_`var' date_q, gen(ln_`var'_ipol)
	replace `var' = exp(ln_`var'_ipol)
}
drop ln*
save $resultstemp/for_ici_filter_q, replace

*** Create Usability Score Based on ICI Coverage
if "`sumstat_type'"=="Market_FX" {
	use $resultstemp/HD_sumstats_q, clear
	collapse (sum) marketvalue_usd, by(DomicileCountryId date_q BroadCategoryGroup)
	rename DomicileCountryId iso_country_code
	merge m:1 iso_country_code using $output/concordances/country_names, keep(1 3) nogen
	merge m:1 country_name date_q using $resultstemp/for_ici_filter_q, keep(1 3) nogen
	rename iso_country_code DomicileCountryId
	drop if missing(DomicileCountryId)
	rename marketvalue_usd mns
	bys Dom date_q: egen mns_total = sum(mns)
	gen mns_equity = mns if BroadCategoryGroup=="Equity"
	gen mns_bond = mns if BroadCategoryGroup=="Fixed Income" | BroadCategoryGroup=="Tax Preferred" | BroadCategoryGroup=="Convertibles"
	gen mns_mm = mns if BroadCategoryGroup=="Money Market"
	gen mns_hybrid = mns if BroadCategoryGroup=="Allocation"  
	gen mns_other = mns if inlist(BroadCategoryGroup,"Alternative","Commodities","Miscellaneous","Property")
	rename (total equity bond mm hybrid other) (ici_total ici_equity ici_bond ici_mm ici_hybrid ici_other)
	collapse (mean) mns_total ici_total ici_equity ici_bond ici_mm ici_hybrid ici_other (sum) mns_equity mns_bond mns_mm mns_hybrid mns_other, by(DomicileCountryId date_q)
	replace ici_total = ici_total-ici_mm if DomicileCountryId=="USA"
	foreach class in "total" "equity" "bond" "mm" "hybrid" "other" {
		replace ici_`class'=ici_`class'/1000
		gen coverage_`class' = mns_`class'/ici_`class'
	}
	save $resultstemp/coverage_vs_ici_q, replace
	clear
}

* Make list of country-quarters and country-years that meet the ICI coverage criterion, themselves supplied as input parameters to the .do file
use DomicileCountryId coverage* date_q using $resultstemp/coverage_vs_ici_q if date_q>=tq(2005q1), clear
drop if inlist(DomicileCountryId,${excluded_ctys})
keep if coverage_total>`5' & coverage_total<`8' & coverage_equity>`3' & coverage_equity<`6' & coverage_bond>`4' & coverage_bond<`7'
keep DomicileCountryId date_q
save $resultstemp/keeper_cty_dates_q, replace
gen date_y = year(dofq(date_q))
format %ty date_y
keep DomicileCountryId date_y
duplicates drop
save $resultstemp/keeper_cty_dates_y, replace
clear

use fundtype_mstar DomicileCountryId iso_country_code currency_id mns_class mns_subclass date_m marketvalue_usd maturity_type BroadCategoryGroup firm_type using $resultstemp/HD_sumstats_m`suffix', clear
drop if missing(iso_country_code) | missing(currency_id)
gen date_q = yq(year(dofm(date_m)), quarter(dofm(date_m)))
format %tq date_q
* Drop countries that lack coverage of ICI
if "`sumstat_type'"!="No_ICI_Drop" {
	merge m:1 DomicileCountryId date_q using $resultstemp/keeper_cty_dates_q, keep(3) nogen 
}
drop date_q
save $resultstemp/HD_foranalysis_tmp1_m`suffix', replace
use iso_country_code iso_currency_code start_date end_date using $output/concordances/country_currencies, clear
rename (iso_country_code iso_currency_code) (DomicileCountryId DomicileCurrencyId)
mmerge DomicileCountryId using $resultstemp/HD_foranalysis_tmp1_m`suffix', unmatched(none)  type(n:n)
rm $resultstemp/HD_foranalysis_tmp1_m`suffix'.dta
keep if _merge==3
drop _merge
drop if (date_m<mofd(start_date) & !missing(start_date)) | (date_m>mofd(end_date) & !missing(end_date))
drop start_date end_date
save $resultstemp/HD_foranalysis_tmp2_m`suffix', replace
use iso_country_code iso_currency_code start_date end_date using $output/concordances/country_currencies, clear
mmerge iso_country_code using $resultstemp/HD_foranalysis_tmp2_m`suffix', unmatched(none)  type(n:n)
rm $resultstemp/HD_foranalysis_tmp2_m`suffix'.dta
keep if _merge==3
drop _merge
drop if (date_m<mofd(start_date) & !missing(start_date)) | (date_m>mofd(end_date) & !missing(end_date))
drop start_date end_date
gen iso_country_code_orig = iso_country_code
gen DomicileCountryId_orig = DomicileCountryId
* Replace Euro Country Codes with EMU and Replace Euro Currency Codes with EUR
forvalues j = 1(1)3 {
	replace iso_country_code = "EMU" if inlist(iso_country_code,${eu`j'})
	replace iso_currency_code = "EUR" if inlist(iso_country_code,${eu`j'})
	replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,${eu`j'})
} 
gen keeper = 1 if inlist(DomicileCountryId,$ctygroupA1)
replace keeper = 1 if inlist(DomicileCountryId,$ctygroupA2)
keep if keeper==1
drop keeper
save $resultstemp/HD_foranalysis_m`suffix', replace
gen month = month(dofm(date_m))
keep if month==3 | month==6 | month==9 | month==12
gen date_q = yq(year(dofm(date_m)),quarter(dofm(date_m)))
format %tq date_q 
drop date_m month
save $resultstemp/HD_foranalysis_q`suffix', replace
gen quarter = quarter(dofq(date_q))
keep if quarter==4
gen date_y = year(dofq(date_q))
format %ty date_y 
drop date_q quarter
save $resultstemp/HD_foranalysis_y`suffix', replace
clear

log close
