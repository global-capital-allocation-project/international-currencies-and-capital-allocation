* This file reads in portfolio_sumstat_y and generates figures comparing the share of LC-only firms in Domestic and Foreign Portfolios, for bonds and for equities. These results are in Figure 9 of the main paper, and for 2005, are in Appendix Figure A.22.

cap log close
log using "$logs/${whoami}_LC_Firm_Sumstats", replace

use iso_country_code iso_currency_code date_y using $resultstemp/HD_foranalysis_y, clear
duplicates drop
save $resultstemp/currencyconcord, replace

forvalues yr = 2005(1)2017 {
	use firm_type DomicileCountryId iso_country_code mns_class mns_subclass currency_id marketvalue_usd cusip6 date_y using $resultstemp/portfolio_sumstat_y if date_y==`yr', clear
	keep if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS")
	merge m:1 date_y iso_country_code using $resultstemp/currencyconcord, keep(1 3) nogen
	gen totborlcu = marketvalue_usd if currency_id==iso_currency_code
	gen totbor = marketvalue_usd
	gen totborlcufromfor = marketvalue_usd if currency_id==iso_currency_code & DomicileCountryId==iso_country_code
	gen locbor = marketvalue_usd if iso_country_code==DomicileCountryId
	gen locborlcu = marketvalue_usd if iso_country_code==DomicileCountryId & currency_id==iso_currency_code
	collapse (sum) totborlcu totbor totborlcufromfor locbor locborlcu, by(iso_country_code cusip6 date_y)
	drop if missing(cusip6)
	gen lc_only = 1 if totborlcu==totbor
	preserve 
	keep if lc_only==1
	keep cusip6 date_y iso_country_code
	duplicates drop
	save $resultstemp/lcsumstatbars_foreq_`yr', replace
	restore
	collapse (sum) totborlcu totbor totborlcufromfor locbor locborlcu, by(iso_country_code lc_only date_y)
	gen forbor = totbor-locbor
	gen totborlconly_tmp = totbor if lc_only==1
	gen forborlconly_tmp = forbor if lc_only==1
	gen locborlconly_tmp = locbor if lc_only==1
	bys iso_country_code date_y: egen totborboth = sum(totbor)
	bys iso_country_code date_y: egen totborlconly = sum(totborlconly_tmp)
	gen shareborfromlcuonly = totborlconly/totborboth
	bys iso_country_code date_y: egen forborboth = sum(forbor)
	bys iso_country_code date_y: egen forborlconly = sum(forborlconly_tmp)
	gen lcuonlyshareoffor = forborlconly/forborboth
	bys iso_country_code date_y: egen locborboth = sum(locbor)
	bys iso_country_code date_y: egen locborlconly = sum(locborlconly_tmp)
	gen lcuonlyshareofloc = locborlconly/locborboth
	keep iso_country_code date_y shareborfromlcuonly lcuonlyshareofloc lcuonlyshareoffor 
	duplicates drop
	keep if !missing(lcuonlyshareofloc)
	save $resultstemp/lcsumstatbars_`yr', replace
}

forvalues yr = 2005(1)2017 {
	use $resultstemp/lcsumstatbars_`yr', clear
	graph bar lcuonlyshareofloc lcuonlyshareoffor , ytitle("Share of Portfolio") over(iso_country_code, sort(lcuonlyshareoffor ) descending) graphregion(color(white)) legend(label(1 "LC-only Share in Domestic Portfolios (Sum of Red Dots)") label(2 "LC-only Share in Foreign Portfolios (Sum of Blue Diamonds)") rows(2) order(1 2)) bar(2, lcolor(blue) fcolor(blue)) bar(1, lcolor(red) fcolor(red) )
	graph export $resultstemp/graphs/LCSumstat_Bars2_`yr'.eps, as(eps) replace
	graph export $resultstemp/graphs/LCSumstat_Bars2_`yr'.pdf, as(pdf) replace
}

forvalues yr = 2005(1)2017 {
	use firm_type DomicileCountryId iso_country_code mns_class mns_subclass currency_id marketvalue_usd cusip6 date_y using $resultstemp/portfolio_sumstat_y if date_y==`yr', clear
	merge m:1 date_y iso_country_code cusip6 using $resultstemp/lcsumstatbars_foreq_`yr', keep(1 2 3)
	keep if mns_class=="E"
	merge m:1 date_y iso_country_code using $resultstemp/currencyconcord, keep(1 3) nogen
	gen lc_only = 1 if _merge==3
	gen totborlcu = marketvalue_usd if lc_only==1
	gen totbor = marketvalue_usd
	gen totborlcufromfor = marketvalue_usd if lc_only==1 & DomicileCountryId==iso_country_code
	gen locbor = marketvalue_usd if iso_country_code==DomicileCountryId
	gen locborlcu = marketvalue_usd if iso_country_code==DomicileCountryId & lc_only==1
	collapse (sum) totborlcu totbor totborlcufromfor locbor locborlcu, by(iso_country_code lc_only date_y)
	gen forbor = totbor-locbor
	gen totborlconly_tmp = totbor if lc_only==1
	gen forborlconly_tmp = forbor if lc_only==1
	gen locborlconly_tmp = locbor if lc_only==1
	bys iso_country_code date_y: egen totborboth = sum(totbor)
	bys iso_country_code date_y: egen totborlconly = sum(totborlconly_tmp)
	gen shareborfromlcuonly = totborlconly/totborboth
	bys iso_country_code date_y: egen forborboth = sum(forbor)
	bys iso_country_code date_y: egen forborlconly = sum(forborlconly_tmp)
	gen lcuonlyshareoffor = forborlconly/forborboth
	bys iso_country_code date_y: egen locborboth = sum(locbor)
	bys iso_country_code date_y: egen locborlconly = sum(locborlconly_tmp)
	gen lcuonlyshareofloc = locborlconly/locborboth
	keep iso_country_code date_y shareborfromlcuonly lcuonlyshareofloc lcuonlyshareoffor 
	duplicates drop
	keep if !missing(lcuonlyshareofloc)
	save $resultstemp/lcsumstatbars_eq_`yr', replace
}

forvalues yr = 2005(1)2017 {
	use $resultstemp/lcsumstatbars_eq_`yr', clear
	graph bar lcuonlyshareofloc lcuonlyshareoffor , ytitle("Share of Portfolio") over(iso_country_code, sort(lcuonlyshareoffor) descending) graphregion(color(white)) legend(label(1 "LC-only Share in Domestic Equity Portfolios") label(2 "LC-only Share in Foreign Equity Portfolios") rows(2) order(1 2)) bar(2, lcolor(blue) fcolor(blue)) bar(1, lcolor(red) fcolor(red) )
	graph export $resultstemp/graphs/LCSumstat_Bars2_eq_`yr'.eps, as(eps) replace
	graph export $resultstemp/graphs/LCSumstat_Bars2_eq_`yr'.pdf, as(pdf) replace
}

log close
