* This file generates the bar chart used in Figure 3 of the paper, showing the split between USD and home currency all external investments made by domicile countries. 

cap log close
log using "$logs/${whoami}_External_Curr_Share", replace


forvalues i=1/2 {
display `i'
use "$resultstemp/HD_foranalysis_y.dta", clear
if `i'==1 {
keep if mns_class=="B" & !inlist(mns_subclass,"S","A","LS","SV","SF") & !missing(currency_id)
local bondlabel = "Corporate Bonds"
local bondlabel2 = ""
local app=""
}
if `i'==2 {
keep if mns_class=="B" 
local bondlabel = "All Bonds"
local bondlabel2 = ""
local app="_B"
}
preserve
drop if DomicileCountryId==iso_country_code
replace currency_id="DC" if currency_id==DomicileCurrencyId & DomicileCountryId~="USA"
collapse (sum) marketvalue_usd, by(currency_id DomicileCountryId DomicileCurrencyId date_y)
bysort DomicileCountryId date_y: egen Dom_total=sum(market)
gen share=market/Dom_total

gen keep=0
foreach x of global ctygroupA_list {
	replace keep=1 if DomicileCountryId=="`x'"
}	
keep if keep==1 & (currency_id=="USD" | currency_=="DC")
keep DomicileCountryId currency_id share date_y
reshape wide share, i(Dom date_y) j(curr) string
renpfix share

graph bar USD DC if date_y==2017, over(Dom) stack bar(1, fcolor(red)) bar(2, fcolor(white) lcolor(black) lwidth(thick) lpattern(solid)) graphregion(color(white)) legend(order(1 "USD" 2 "Home Currency")) ytitle("Share of External Portfolio")  name("baseline`app'", replace)
graph export "$resultstemp/graphs/External_USD_DC`app'.eps", replace
save  "$resultstemp/External_USD_DC`app'.dta", replace

restore
drop if DomicileCountryId==iso_country_code | iso_country_code=="USA"
replace currency_id="DC" if currency_id==DomicileCurrencyId & DomicileCountryId~="USA"
collapse (sum) marketvalue_usd, by(currency_id DomicileCountryId DomicileCurrencyId date_y)
bysort DomicileCountryId date_y: egen Dom_total=sum(market)
gen share=market/Dom_total

gen keep=0
foreach x of global ctygroupA_list {
	replace keep=1 if DomicileCountryId=="`x'"
}	
keep if keep==1 & (currency_id=="USD" | currency_=="DC")
keep DomicileCountryId currency_id share date_y
reshape wide share, i(Dom date_y) j(curr) string
renpfix share

graph bar USD DC if date_y==2017, over(Dom) stack bar(1, fcolor(red)) bar(2, fcolor(white) lcolor(black) lwidth(thick) lpattern(solid)) graphregion(color(white)) legend(order(1 "USD" 2 "Home Currency")) ytitle("Share of External Portfolio") name("exusa`app'", replace)
graph export "$resultstemp/graphs/External_USD_DC_exusa`app'.eps", replace
save  "$resultstemp/External_USD_DC_exusa`app'.dta", replace
}
cap log close
