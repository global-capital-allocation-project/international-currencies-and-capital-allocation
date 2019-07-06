* This file combines the HD_sumstats_y files with data from ICI in order to compare asset coverage. The resulting plots are Figure 1 and Appendix Figures 1-2 in our paper.

cap log close
log using "$logs/${whoami}_Compare_with_ICI", replace

*---------------------------------------------------------------------------------
* Mutual funds plus ETFs, US
*---------------------------------------------------------------------------------
use $resultstemp/HD_sumstats_y, clear
keep if DomicileCountryId=="USA"
rename date_y year
replace BroadCategoryGroup="Missing" if missing(BroadCategoryGroup)
collapse (sum) value=marketvalue_usd, by(year BroadCategoryGroup)
replace value=. if value==0
bys year: egen total = sum(value)
replace total=. if total==0
replace BroadCategoryGroup = subinstr(BroadCategoryGroup," ","",.)
reshape wide value, i(year) j(BroadCategoryGroup) string
rename value* mstar_*
rename total mstar_total
merge 1:1 year using $output/ici_data/US_ICI_sumstats_y, keep(1 2 3)
mmerge year using "$temp/ici_data/ICI_US_ETF_AUM_Factbook_2018_Consolidated.dta", unmatched(b) uname(etf_)
drop _merge
sort year

drop if year>=2018
gen mstar_Bonds = mstar_FixedIncome+mstar_TaxPreferred+mstar_Convertibles
gen etf_ici_equity = etf_domesticequitybroadbased + etf_domesticequitysector + etf_globalinternationalequity
replace etf_ici_equity = etf_ici_equity / 1000
gen etf_ici_bonds = etf_bond / 1000
replace etf_ici_equity = 0 if missing(etf_ici_equity)
replace etf_ici_total = 0 if missing(etf_ici_total)
replace etf_ici_bonds = 0 if missing(etf_ici_bonds)

replace ici_total_us=(ici_total_us + etf_ici_total)/1000
replace mstar_total=mstar_total/1000
replace ici_equity=(ici_equity_us + etf_ici_equity)/1000
replace mstar_Equity=mstar_Equity/1000
replace ici_bond=(ici_bond_us + etf_ici_bonds)/1000
replace mstar_Bonds=mstar_Bonds/1000
replace ici_hybrid=ici_hybrid/1000
replace mstar_Allocation=mstar_Allocation/1000

line ici_total year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_total year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_US_total.eps, as(eps) replace
line ici_equity year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Equity year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_US_equities.eps, as(eps) replace
line ici_bond year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Bonds year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_US_bonds.eps, as(eps) replace
line ici_hybrid year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Allocation year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_US_hybrid.eps, as(eps) replace

drop if year < 1990
line ici_bond year, lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Bonds year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red) xlabel(1990(10)2020) xscale(r(1990 2020))
graph export $resultstemp/graphs/ICI_compare_US_bonds_shortaxis.eps, as(eps) replace

clear

*---------------------------------------------------------------------------------
* Mutual funds plus ETFs, NonUS
*---------------------------------------------------------------------------------
use date_q DomicileCountryId ici_total ici_equity ici_bond ici_mm mns_total mns_equity mns_bond mns_mm using $resultstemp/coverage_vs_ici_q, clear

drop if date_q>=tq(2018q1) 
forvalues j = 1(1)3 {
	capture replace DomicileCountryId = "EMU" if inlist(DomicileCountryId,${eu`j'})
} 
collapse (sum) ici* mns*, by(DomicileCountryId date_q)
replace ici_total=. if ici_total==0
replace ici_equity=. if ici_equity==0
replace ici_bond=. if ici_bond==0
replace ici_mm=. if ici_mm==0
replace ici_total = ici_total/1000
replace ici_equity = ici_equity/1000
replace ici_bond = ici_bond/1000
replace mns_total = mns_total/1000
replace mns_equity = mns_equity/1000
replace mns_bond = mns_bond/1000
foreach cty in "EMU" "GBR" "CHN" {
	if "`cty'"=="EMU" {
		local ctylab = "European Monetary Union"
	}
	if "`cty'"=="GBR" {
		local ctylab = "United Kingdom"
	}
	if "`cty'"=="CHN" {
		local ctylab = "China"
	}
	line ici_total date_q if DomicileCountryId=="`cty'" & date_q>tq(2002q1), lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_total date_q if DomicileCountryId=="`cty'" & date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
	graph export $resultstemp/graphs/ICI_compare_`cty'_total.eps, as(eps) replace
	line ici_equity date_q if DomicileCountryId=="`cty'" & date_q>tq(2002q1),  lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_equity date_q if DomicileCountryId=="`cty'" & date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
	graph export $resultstemp/graphs/ICI_compare_`cty'_equities.eps, as(eps) replace
	line ici_bond date_q if DomicileCountryId=="`cty'" & date_q>tq(2002q1), lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_bond date_q if DomicileCountryId=="`cty'" & date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
	graph export $resultstemp/graphs/ICI_compare_`cty'_bonds.eps, as(eps) replace
}
drop if DomicileCountryId=="USA"

drop if date_q<tq(2014q4) & (missing(ici_total) | missing(ici_equity) | missing(ici_bond) | missing(mns_total) | missing(mns_equity) | missing(mns_bond))
collapse (sum) ici* mns*, by(date_q)
replace ici_total=. if ici_total==0
replace ici_equity=. if ici_equity==0
replace ici_bond=. if ici_bond==0
replace ici_mm=. if ici_mm==0
line ici_total date_q if date_q>tq(2002q1), lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_total date_q if date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_NonUS_total.eps, as(eps) replace
line ici_equity date_q if date_q>tq(2002q1), lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_equity date_q if date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_NonUS_equities.eps, as(eps) replace
gen date_lab = year(dofq(date_q)) + quarter(dofq(date_q))/4-0.25
line ici_bond date_lab if date_q>tq(2002q1), xlabel(1980(10)2020) lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_bond date_lab if date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_NonUS_bonds_new.eps, as(eps) replace
line ici_bond date_q if date_q>tq(2002q1), lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_bond date_q if date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_compare_NonUS_bonds.eps, as(eps) replace

drop if date_q < tq(1990q1)
line ici_bond date_lab if date_q>tq(2002q1), xlabel(1980(10)2020) lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mns_bond date_lab if date_q>tq(2002q1), legend(label(2 "Morningstar")) lpattern(dash) lcolor(red) xlabel(1990(10)2020) xscale(r(1990 2020))
graph export $resultstemp/graphs/ICI_compare_NonUS_bonds_new_shortaxis.eps, as(eps) replace

*---------------------------------------------------------------------------------
* Mutual funds only, US
*---------------------------------------------------------------------------------
use $resultstemp/HD_sumstats_y, clear
keep if DomicileCountryId=="USA"
rename date_y year
replace BroadCategoryGroup="Missing" if missing(BroadCategoryGroup)
keep if fundtype_mstar != "FE"
collapse (sum) value=marketvalue_usd, by(year BroadCategoryGroup)
replace value=. if value==0
bys year: egen total = sum(value)
replace total=. if total==0
replace BroadCategoryGroup = subinstr(BroadCategoryGroup," ","",.)
reshape wide value, i(year) j(BroadCategoryGroup) string
rename value* mstar_*
rename total mstar_total
merge 1:1 year using $output/ici_data/US_ICI_sumstats_y, keep(1 2 3)
drop _merge
sort year

drop if year>=2018
gen mstar_Bonds = mstar_FixedIncome+mstar_TaxPreferred+mstar_Convertibles
replace ici_total_us=ici_total_us/1000
replace mstar_total=mstar_total/1000
replace ici_equity=ici_equity_us/1000
replace mstar_Equity=mstar_Equity/1000
replace ici_bond=ici_bond_us/1000
replace mstar_Bonds=mstar_Bonds/1000
replace ici_hybrid=ici_hybrid/1000
replace mstar_Allocation=mstar_Allocation/1000
line ici_total year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_total year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_MFonly_compare_US_total.eps, as(eps) replace
line ici_equity year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Equity year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_MFonly_compare_US_equities.eps, as(eps) replace
line ici_bond year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Bonds year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_MFonly_compare_US_bonds.eps, as(eps) replace
line ici_hybrid year , lpattern(solid) lcolor(blue) graphregion(color(white)) xtitle("") ytitle("Trillions (USD, Current)") legend(label(1 "ICI")) || line mstar_Allocation year, legend(label(2 "Morningstar")) lpattern(dash) lcolor(red)
graph export $resultstemp/graphs/ICI_MFonly_compare_US_hybrid.eps, as(eps) replace
clear

log close
