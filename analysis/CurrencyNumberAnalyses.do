* This file reads in portfolio_sumstat_y.dta and produces the scatter plots in Figure 6 of the paper showing the number of different currencies used to denominated bonds issued by companies against their total borrowing in our dataset.

cap log close
log using "$logs/${whoami}_CurrencyNumberAnalyses", replace

local xmax = 500
local plotyear = 2017

use cusip6 externalname firm_type marketvalue_usd using $resultstemp/portfolio_sumstat_y, clear
collapse (sum) marketvalue_usd, by(cusip6 firm_type externalname)
sort cusip6 marketvalue_usd
collapse (lastnm) firm_type externalname, by(cusip6)
save $resultstemp/cusip6namestypes, replace

foreach cty in "USA" "EMU" "GBR" "CAN" {
	use $resultstemp/portfolio_sumstat_y if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS") & date_y==`plotyear' & iso_country_code=="`cty'" , clear
	replace marketvalue_usd=marketvalue_usd
	drop if missing(currency_id)
	gen foreign = marketvalue_usd if DomicileCountryId!="`cty'"
	egen curtag = tag(currency_id cusip6)
	collapse (sum) borrow_tot = marketvalue_usd borrow_for = foreign curcount = curtag , by(cusip6)
	sort cusip6 borrow_tot
	collapse (sum) borrow_tot borrow_for curcount , by(cusip6)
	gsort -borrow_tot
	gen rank = _n
	merge 1:1 cusip6 using $resultstemp/cusip6namestypes, keep(1 3) nogen
	gen ln_borrow_tot = ln(borrow_tot)
	drop if ln_borrow_tot<10
	sort rank
	keep if curcount>=1
	scatter curcount rank if rank<=`xmax' , xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Number of Currencies (Left)") label(2 "Total Debt (In Logs, Right)") label(3 "Foreign Borrowing") rows(1)) mc(red) ms(oh) xtitle("Issuer's Rank by Total Borrowing") ytitle("")   ||  line borrow_tot rank if rank<=`xmax', lc(black) yaxis(2) ytitle("", axis(2)) || line borrow_for rank if rank<=`xmax', lc(blue) lp(dash) yaxis(2) ytitle("", axis(2))
	graph export $resultstemp/graphs/CurrNums_`cty'_v1.eps, as(eps) replace
	scatter curcount rank if rank<=`xmax' , xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Number of Currencies (Left)") label(2 "Total Debt (In Logs, Right)") label(3 "Foreign Borrowing") rows(1)) mc(red) ms(oh) xtitle("Issuer's Rank by Total Borrowing") ytitle("")  ||  line borrow_tot rank if rank<=`xmax', lc(black) yaxis(2) ytitle("", axis(2)) || line borrow_for rank if rank<=`xmax', lc(blue) lp(dash) yaxis(2) ytitle("", axis(2))
	graph save $resultstemp/graphs/CurrNums_`cty'_v1.gph, replace
	scatter curcount rank if rank<=`xmax' , xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Number of Currencies (Left)") label(2 "Total Debt (In Logs, Right)") rows(1)) mc(red) ms(oh) xtitle("Issuer's Rank by Total Borrowing") ytitle("")  ||  line ln_borrow_tot rank if rank<=`xmax', lc(black) yaxis(2) ytitle("", axis(2)) 
	graph export $resultstemp/graphs/CurrNums_`cty'_v2.eps, as(eps) replace
	scatter curcount rank if rank<=`xmax' , xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Number of Currencies (Left)") label(2 "Total Debt (In Logs, Right)") rows(1)) mc(red) ms(oh) xtitle("Issuer's Rank by Total Borrowing") ytitle("")  ||  line ln_borrow_tot rank if rank<=`xmax', lc(black) yaxis(2) ytitle("", axis(2)) 
	graph save $resultstemp/graphs/CurrNums_`cty'_v2.gph, replace
	scatter curcount rank if rank<=`xmax' , xlabel(0(100)`xmax') graphregion(color(white)) ytitle("Number of Currencies") mc(red) ms(oh) xtitle("Issuer's Rank by Total Borrowing")   
	graph export $resultstemp/graphs/CurrNums_`cty'_v3.eps, as(eps) replace
	scatter curcount rank if rank<=`xmax' , xlabel(0(100)`xmax') graphregion(color(white)) ytitle("") mc(red) ms(oh) xtitle("Issuer's Rank by Total Borrowing")    
	graph save $resultstemp/graphs/CurrNums_`cty'_v3.gph, replace
	save $resultstemp/currnum_rank_`cty', replace
}
grc1leg $resultstemp/graphs/CurrNums_CAN_v1.gph $resultstemp/graphs/CurrNums_EMU_v1.gph $resultstemp/graphs/CurrNums_GBR_v1.gph $resultstemp/graphs/CurrNums_USA_v1.gph, rows(2) legendfrom($resultstemp/graphs/CurrNums_USA_v1.gph) graphregion(color(white)) position(6) span 
graph export $resultstemp/graphs/CurrNums_All_v1.eps, as(eps) replace
grc1leg $resultstemp/graphs/CurrNums_CAN_v2.gph $resultstemp/graphs/CurrNums_EMU_v2.gph $resultstemp/graphs/CurrNums_GBR_v2.gph $resultstemp/graphs/CurrNums_USA_v2.gph, rows(2) legendfrom($resultstemp/graphs/CurrNums_USA_v2.gph) graphregion(color(white)) position(6) span 
graph export $resultstemp/graphs/CurrNums_All_v2.eps, as(eps) replace
graph combine $resultstemp/graphs/CurrNums_CAN_v3.gph $resultstemp/graphs/CurrNums_EMU_v3.gph $resultstemp/graphs/CurrNums_GBR_v3.gph $resultstemp/graphs/CurrNums_USA_v3.gph, rows(2) graphregion(color(white)) 
graph export $resultstemp/graphs/CurrNums_All_v3.eps, as(eps) replace

log close
