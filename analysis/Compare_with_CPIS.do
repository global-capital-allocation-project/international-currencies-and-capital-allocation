* This file combines the HD_sumstats_y files with data from CPIS in order to compare portfolio shares in external assets and bonds for our key countries. The resulting plots are in Appendix Figures A.5, A.6, and A.7 in our paper.
* At the end, it also creates a Figure With RoW Claims on LUX MF based on CPIS Data, which appears as Appendix Figure A.21.

cap log close
log using "$logs/${whoami}_Compare_with_CPIS", replace

****************************
*Clean the file for analysis
****************************

use "$temp/CPIS/CPIS_bulk.dta", clear

drop if Attribute=="Status"

*Drop semi annual surveys
keep if Val2001S2==. &Val2002S2==. & Val2003S2==. & Val2004S2==. & Val2005S2==. & Val2006S2==. &Val2007S2==.& Val2008S2==. & Val2009S2==.& Val2010S2==. & Val2011S2==.& Val2012S2==.& Val2013S2==.& Val2014S2==.& Val2015S2==. & Val2016S2==. & Val2017S2==. 
*this is a cheap fix to drop the rows corresponding to semi-annual data
drop Val*S1
drop Val*S2

* Reduce to cross section
keep Country_Name Country_Code Indicator_Name Indicator_Code Counterpart_Country_Name Counterpart_Country_Code Counterpart_Sector_Name Counterpart_Sector_Code Sector_Name Sector_Code Attribute Country_ISO3 Counterpart_Country_ISO3 Val*

* Drop domestic assets so that we only focus on external debt and equity assets/liabilities.
* Note that only Turkey and Saudi Arabia report domestic assets. 
* This doesn't apply to anyone else.
* I'm preserving the World-World aggregates.
g domestic_indic = 1 if Country_Code == Counterpart_Country_Code
tab Country_Name if domestic_indic == 1 
drop if domestic_indic == 1 & Country_Name != "World"
drop domestic_indic

* Rename some countries for display purposes
foreach v of varlist Country_Name Counterpart_Country_Name {
replace `v' = "Kosovo" if `v' == "Kosovo, Republic of"
replace `v' = "Korea" if `v' == "Korea, Republic of"
replace `v' = "Russia" if `v' == "Russian Federation"
replace `v' = "Venezuela" if `v' == "Venezuela, Republica Bolivariana de"
replace `v' = "Slovakia" if `v' == "Slovak Republic"
replace `v' = "Hong Kong" if `v' == "China, P.R.: Hong Kong"
replace `v' = "China" if `v' == "China, P.R.: Mainland"
replace `v' = "Not Specified" if `v' == "Not Specified (including Confidential)"
}

* Save cleaned data
save "$resultstemp/CPIS_cleaned.dta", replace

********************************************************************************
***DEBT ASSET SPLITS BY CURRENCY************************************************
********************************************************************************

use "$resultstemp/CPIS_cleaned.dta", clear
keep if substr(Indicator_Code,9,1) != "T"
keep if substr(Indicator_Code,9,2) != "SN"
keep if substr(Indicator_Name,1,26) == "Assets, Debt Securities, D"
g Curr = substr(Indicator_Code,9,3)
replace Curr = "Other" if Curr == "O_B"

drop Counterpart_Sector_Name Counterpart_Sector_Code Sector_Name Sector_Code Attribute Counterpart_Country_Name Counterpart_Country_Code Counterpart_Country_ISO3 Indicator_Name Indicator_Code Country_Code

keep if inlist(Country_ISO,$eu1)|inlist(Country_ISO,$eu2)|inlist(Country_ISO,$eu3)|inlist(Country_ISO,$ctygroupA1)|inlist(Country_ISO,$ctygroupA2)

foreach v of varlist Val* {
	replace `v' = `v'/10^9
}

* Create percentages
reshape long Val, i(Country_Name Country_ISO3 Curr) j(year) string
reshape wide Val, i(Country_Name Country_ISO3 year) j(Curr) string
drop if ValCHF==. & ValEUR==. & ValGBP==. & ValJPY==. & ValOther==. & ValUSD==.
drop if ValCHF==0 & ValEUR==0 & ValGBP==0 & ValJPY==0 & ValOther==0 & ValUSD==0
egen ValTotal = rsum(Val*)
foreach v of varlist Val* {
	g perc`v' = `v'/ValTotal*100
	replace perc`v'=0 if perc`v'==.
}
drop percValTotal

destring year, replace force

sort Country_Name year

*Save data by currency
save "$resultstemp/CPIS_by_currency.dta", replace

use $resultstemp/HD_sumstats_y if !missing(DomicileCountryId) & !missing(iso_co, currency_id), clear
keep if mns_class=="B"
drop if Dom==iso_co
rename date_y year
*XXX Need to create EUR here
replace curr="Other" if curr!="EUR" & curr!="USD" & curr!="CHF" & curr!="JPY" & curr!="GBP"
collapse (sum) value_mstar = marketvalue_usd, by(year Dom currency_id)
reshape wide val, i(Dom year) j(curr) string
drop if value_mstarCHF==. & value_mstarEUR==. & value_mstarGBP==. & value_mstarJPY==. & value_mstarOther==. & value_mstarUSD==.
drop if value_mstarCHF==0 & value_mstarEUR==0 & value_mstarGBP==0 & value_mstarJPY==0 & value_mstarOther==0 & value_mstarUSD==0
egen valTotal = rsum(val*)
foreach v of varlist val* {
	g perc`v' = `v'/valTotal*100
	replace perc`v'=0 if perc`v'==.
}
drop percvalTotal
keep year Dom perc*
mmerge Dom year using "$resultstemp/CPIS_by_currency.dta", umatch(Country_ISO3 year) ukeep(perc*) uname(cpis_)
keep if _merge==3
drop _merge

rename cpis_percVal* percvalue_cpis*
sort year Dom
order year Dom percvalue_mstarUSD percvalue_cpisUSD percvalue_mstarEUR percvalue_cpisEUR percvalue_mstarGBP percvalue_cpisGBP percvalue_mstarJPY percvalue_cpisJPY percvalue_mstarCHF percvalue_cpisCHF
save "$resultstemp/table_mstar_cpis_by_currency_all.dta", replace
keep if year==2017
save "$resultstemp/table_mstar_cpis_by_currency.dta", replace


********************************************************************************
***REPEAT ON RESIDENCY BASIS
********************************************************************************


use $output/HoldingDetail/HD_2017_y, clear
keep if !missing(DomicileCountryId) & !missing(cgs_domicile, currency_id)
keep if mns_class=="B"
drop if Dom==cgs_domicile
rename date_y year
gen marketvalue_usd = marketvalue / lcu_per_usd

replace curr="Other" if curr!="EUR" & curr!="USD" & curr!="CHF" & curr!="JPY" & curr!="GBP"
collapse (sum) value_mstar = marketvalue_usd, by(year Dom currency_id)
reshape wide val, i(Dom year) j(curr) string
drop if value_mstarCHF==. & value_mstarEUR==. & value_mstarGBP==. & value_mstarJPY==. & value_mstarOther==. & value_mstarUSD==.
drop if value_mstarCHF==0 & value_mstarEUR==0 & value_mstarGBP==0 & value_mstarJPY==0 & value_mstarOther==0 & value_mstarUSD==0
egen valTotal = rsum(val*)
foreach v of varlist val* {
	g perc`v' = `v'/valTotal*100
	replace perc`v'=0 if perc`v'==.
}
drop percvalTotal
keep year Dom perc*
mmerge Dom year using "$mns_data/results/temp/CPIS_by_currency.dta", umatch(Country_ISO3 year) ukeep(perc*) uname(cpis_)
keep if _merge==3
drop _merge

rename cpis_percVal* percvalue_cpis*
sort year Dom
order year Dom percvalue_mstarUSD percvalue_cpisUSD percvalue_mstarEUR percvalue_cpisEUR percvalue_mstarGBP percvalue_cpisGBP percvalue_mstarJPY percvalue_cpisJPY percvalue_mstarCHF percvalue_cpisCHF
save "$mns_data/results/temp/table_mstar_cpis_by_currency_residency_all.dta", replace
keep if year==2017
save "$mns_data/results/temp/table_mstar_cpis_by_currency_residency.dta", replace


********************************************************************************
***REPEAT ON DOMICILE BASIS
********************************************************************************

use $output/HoldingDetail/HD_2017_y, clear
mmerge cusip6 using "$temp/country_master/up_aggregation_final_compact", unmatched(m) umatch(issuer_number)
replace country_bg = cgs_domicile if missing(country_bg)
keep if !missing(DomicileCountryId) & !missing(country_bg, currency_id)
keep if mns_class=="B"
drop if Dom==country_bg
rename date_y year
gen marketvalue_usd = marketvalue / lcu_per_usd

replace curr="Other" if curr!="EUR" & curr!="USD" & curr!="CHF" & curr!="JPY" & curr!="GBP"
collapse (sum) value_mstar = marketvalue_usd, by(year Dom currency_id)
reshape wide val, i(Dom year) j(curr) string
drop if value_mstarCHF==. & value_mstarEUR==. & value_mstarGBP==. & value_mstarJPY==. & value_mstarOther==. & value_mstarUSD==.
drop if value_mstarCHF==0 & value_mstarEUR==0 & value_mstarGBP==0 & value_mstarJPY==0 & value_mstarOther==0 & value_mstarUSD==0
egen valTotal = rsum(val*)
foreach v of varlist val* {
	g perc`v' = `v'/valTotal*100
	replace perc`v'=0 if perc`v'==.
}
drop percvalTotal
keep year Dom perc*
mmerge Dom year using "$mns_data/results/temp/CPIS_by_currency.dta", umatch(Country_ISO3 year) ukeep(perc*) uname(cpis_)
keep if _merge==3
drop _merge

rename cpis_percVal* percvalue_cpis*
sort year Dom
order year Dom percvalue_mstarUSD percvalue_cpisUSD percvalue_mstarEUR percvalue_cpisEUR percvalue_mstarGBP percvalue_cpisGBP percvalue_mstarJPY percvalue_cpisJPY percvalue_mstarCHF percvalue_cpisCHF
save "$mns_data/results/temp/table_mstar_cpis_by_currency_domicile_all.dta", replace
keep if year==2017
save "$mns_data/results/temp/table_mstar_cpis_by_currency_domicile.dta", replace

********************************************************************************
***DEBT ASSET SPLITS BY COUNTRY OF ISSUER***************************************
********************************************************************************
use "$resultstemp/CPIS_cleaned.dta", clear
keep if inlist(Country_ISO,$eu1)|inlist(Country_ISO,$eu2)|inlist(Country_ISO,$eu3)|inlist(Country_ISO,$ctygroupA1)|inlist(Country_ISO,$ctygroupA2)

drop if Counterpart_Country_Name == "World"
keep if Sector_Name == "Total Holdings"
keep if Counterpart_Sector_Name == "Total Holdings"
keep if Attribute == "Value"
keep if (Indicator_Name == "Assets, Debt Securities, BPM6, US Dollars" | Indicator_Name == "Assets, Equity, BPM6, US Dollars")

drop Sector_Name Counterpart_Sector_Name Sector_Code Counterpart_Sector_Code Attribute Indicator_Code Country_Code Counterpart_Country_Code

foreach v of varlist Val* {
	replace `v' = `v'/10^9
}

rename Country_ISO3 DomicileCountryId
rename Counterpart_Country_ISO3 iso_country_code
gen mns_class="B"
replace mns_class="E" if Indicator_Name == "Assets, Equity, BPM6, US Dollars"
keep Dom iso mns_c Val*
drop if iso_co==""

* Finland irregularity
duplicates drop
bysort DomicileCountryId mns_class iso_country_code: drop if _n > 1 & DomicileCountryId == "FIN"

reshape long Val, i(iso_co Dom mns_class) j(year) string
replace Val=0 if Val==.
rename Val value_cpis
destring year, replace
save "$resultstemp/CPIS_by_country.dta", replace

use $resultstemp/HD_sumstats_y if !missing(DomicileCountryId) & !missing(iso_country_code), clear
rename date_y year
keep if DomicileCountryId!=iso_co
keep if mns_class=="B" | mns_class=="E"
collapse (sum) value_mstar = marketvalue_usd, by(year Dom iso mns_class) 
save $resultstemp/mstarforcpis_country.dta, replace

mmerge year iso Dom mns_class using "$resultstemp/CPIS_by_country.dta"
drop if year<=2003
drop if year==2016
keep if inlist(Dom,$ctygroupA1) |inlist(Dom,$ctygroupA2) | inlist(Dom,$eu1) | inlist(Dom,$eu2) | inlist(Dom,$eu3) 
keep if _merge==3
drop _merge
save $resultstemp/mstar_and_cpis_country.dta, replace

bys year Dom mns_class: egen value_mstar_yrtot = sum(value_mstar) 
bys year Dom mns_class: egen value_cpis_yrtot = sum(value_cpis)
gen share_mstar = value_mstar / value_mstar_yrtot
gen share_cpis = value_cpis / value_cpis_yrtot

foreach x in $ctygroupA_list {
	foreach class in  "E" "B" {
		scatter share_mstar share_cpis if year==2005 & mns_class=="`class'" & Dom=="`x'", ytitle("Morningstar") xtitle("CPIS") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_cpis if year==2010 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_cpis if year==2017 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_cpis share_cpis if mns_class=="`class'" & Dom=="`x'", lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/CPIS_compare_`x'_`class'_.eps, as(eps) replace
	}
}

foreach x in $eu_list {
	foreach class in  "E" "B" {
		scatter share_mstar share_cpis if year==2005 & mns_class=="`class'" & Dom=="`x'", ytitle("Morningstar") xtitle("CPIS") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_cpis if year==2010 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_cpis if year==2017 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_cpis share_cpis if mns_class=="`class'" & Dom=="`x'", lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/CPIS_compare_`x'_`class'.eps, as(eps) replace
	}
}

keep if inlist(iso_country_code,$eu1)|inlist(iso_country_code,$eu2)|inlist(iso_country_code,$eu3)|inlist(iso_country_code,$ctygroupA1)|inlist(iso_country_code,$ctygroupA2)

foreach x in $ctygroupA_list {
	foreach class in  "E" "B" {
		scatter share_mstar share_cpis if year==2005 & mns_class=="`class'" & Dom=="`x'", ytitle("Morningstar") xtitle("CPIS") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_cpis if year==2010 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_cpis if year==2017 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_cpis share_cpis if mns_class=="`class'" & Dom=="`x'", lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/CPIS_compare_`x'_`class'_select.eps, as(eps) replace
	}
}

foreach x in $eu_list {
	foreach class in  "E" "B" {
		scatter share_mstar share_cpis if year==2005 & mns_class=="`class'" & Dom=="`x'", ytitle("Morningstar") xtitle("CPIS") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_cpis if year==2010 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_cpis if year==2017 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_cpis share_cpis if mns_class=="`class'" & Dom=="`x'", lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/CPIS_compare_`x'_`class'_select.eps, as(eps) replace
	}
}

*Build aggregate Europe Results
use $resultstemp/mstar_and_cpis_country.dta, clear
replace Dom="EMU" if inlist(Dom,$eu1)|inlist(Dom,$eu2)|inlist(Dom,$eu3)
replace iso_co="EMU" if inlist(iso_co,$eu1)|inlist(iso_co,$eu2)|inlist(iso_co,$eu3)
drop if Dom==iso_co
collapse (sum) value_mstar value_cpis, by(year Dom iso mns_class)
bys year Dom mns_class: egen value_mstar_yrtot = sum(value_mstar) 
bys year Dom mns_class: egen value_cpis_yrtot = sum(value_cpis)
gen share_mstar = value_mstar / value_mstar_yrtot
gen share_cpis = value_cpis / value_cpis_yrtot

foreach x in $ctygroupA_list {
	foreach class in  "E" "B" {
		scatter share_mstar share_cpis if year==2005 & mns_class=="`class'" & Dom=="`x'", ytitle("Morningstar") xtitle("CPIS") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_cpis if year==2010 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_cpis if year==2017 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_cpis share_cpis if mns_class=="`class'" & Dom=="`x'", lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/CPIS_compare_`x'_`class'_consEMU.eps, as(eps) replace
	}
}

keep if inlist(iso,$ctygroupA1) | inlist(iso,$ctygroupA2)


foreach x in $ctygroupA_list {
	foreach class in  "E" "B" {
		scatter share_mstar share_cpis if year==2005 & mns_class=="`class'" & Dom=="`x'", ytitle("Morningstar") xtitle("CPIS") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_cpis if year==2010 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_cpis if year==2017 & mns_class=="`class'" & Dom=="`x'", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_cpis share_cpis if mns_class=="`class'" & Dom=="`x'", lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/CPIS_compare_`x'_`class'_consEMU_select.eps, as(eps) replace
	}
}

*************************************************************
* Creates Figure With RoW Claims on LUX MF based on CPIS Data
*************************************************************

use "$temp/CPIS/CPIS_bulk.dta",clear
keep if Country_Code==137
drop Country_Code Country_Name
keep if Indicator_Code=="I_L_E_T_T_BP6_DV_USD"
drop Indicator_Name Indicator_Code
drop if Counterpart_Country_Code==1
keep if Sector_Code=="T"
drop Sector_Code Sector_Name Counterpart_Sector_Name Counterpart_Sector_Code

mmerge Counterpart_Country_Code using "$raw/macro/Concordances/md4stata_code_list.dta", umatch(ifs) ukeep(wbcode)
rename wbcode iso
replace iso="OTH" if _merge==1
drop if _merge==2 | _merge==-2
drop  _merge 
drop *S1 *S2
collapse (sum) Val*, by (iso)
reshape long Val, i(iso) j(date) string
replace date=substr(date,1,4)
destring date, replace
sort date iso
rename Val market
bysort date: egen tot=sum(market)
gen share=market/tot
sort date share

replace iso="EMU" if (inlist(iso,$eu1) | inlist(iso,$eu2) | inlist(iso,$eu3))
collapse (sum) market, by(date iso)
bysort date: egen tot=sum(market)
gen share=market/tot
gsort date share

twoway (line share date if iso=="EMU") (line share date if iso=="USA") (line share date if iso=="CHE") (line share date if iso=="JPN") (line share date if iso=="GBR"), legend(order(1 "EMU" 2 "USA" 3 "CHE" 4 "JPN" 5 "GBR")) title("Percentage of Investment in Luxembourg Equities/Fund Shares", size(med)) ytitle("Share of Total") xtitle("") graphregion(color(white))  
			graph export "$results/temp/graphs/CPIS_LUX.eps", replace	

log close
