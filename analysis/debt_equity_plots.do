*------------------------------------------------------------------------------
* This file generates Appendix Figure A.20 on the joint holdings of firms' debt 
*and equity by domestic and foreign investors.
*------------------------------------------------------------------------------

cap log close
log using "$logs/${whoami}_debt_equity_plots", replace
local plotyear = 2017

*Calculate Currency Composition of Total Firm Debt in the year to be plotted
use cgs_dom firm_type iso_country_code mns_class mns_sub cusip cusip6 mns_subclass marketvalue_usd DomicileCountryId coupon maturitydate currency_id using "$resultstemp/portfolio_sumstat_`plotyear'_y.dta" if (mns_class=="B"  & DomicileCountryId~="" & cusip6~="" & !inlist(mns_subclass,"S","A","LS","SV","SF")), clear
drop if currency_id==""
drop if market<0
collapse (sum) marketvalu, by(cusip6 currency_id)
bysort cusip6: egen total=sum(market)
gen share_=market/total
drop market total
bysort cusip6: egen share_total=sum(share_)
drop if share_total==0
drop share_total
reshape wide share_, i(cusip6) j(currency_id) str
foreach x of varlist share* {
	replace `x'=0 if `x'==.
	}
save $resultstemp/firm_curr_`plotyear'_all.dta, replace

*Calculate Share of Foreign and Domestic Debt and Equity Investment Going to Each Firm
use firm_type DomicileCountryId iso_country_code mns_class mns_subclass currency_id marketvalue_usd cusip6 date_y using $resultstemp/portfolio_sumstat_`plotyear'_y.dta, clear
keep if (mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS")) | mns_class=="E"
gen sectype="B" if mns_class=="B"
replace sectype="E" if mns_class=="E"

gen iso_keep=0
gen dom_keep=0
foreach x in $ctygroupA_list {
	replace iso_keep=1 if iso_c=="`x'"
	replace dom_keep=1 if Dom=="`x'"
	}
keep if iso_keep==1 & dom_keep==1	
drop iso_keep dom_keep

drop if cusip6==""
gen domestic=0
replace domestic=1 if iso==Dom
replace market=. if market<0
collapse (sum) market, by(iso domestic sectype cusip6)
bysort iso dom sectype: egen portfolio_sum=sum(market)
gen portshare=market/portfolio_sum
drop portfolio_sum 
rename market market

reshape wide portshare market, i(iso cusip sectype) j(domestic)
rename portshare1 domshare
rename portshare0 forshare
rename market1 dommarket
rename market0 formarket
reshape wide  formarket forshare dommarket domshare, i(iso cusip) j(sectype) str

foreach x in  formarket forshare dommarket domshare {
	rename `x'B b_`x'
	rename `x'E e_`x'
}	
	
label var b_formarket "Market Value of Foreign Holdings of this Firm's Debt"
label var b_dommarket "Market Value of Domestic Holdings of this Firm Debt"
label var b_forshare "Share of foreign debt investment in country that goes to this firm"
label var b_domshare "Share of domestic debt investment in country that goes to this firm"
label var e_formarket "Market Value of Foreign Holdings of this Firm's Equity"
label var e_dommarket "Market Value of Domestic Holdings of this Firm Equity"
label var e_forshare "Share of foreign Equity investment in country that goes to this firm"
label var e_domshare "Share of domestic Equity investment in country that goes to this firm"

foreach x in "b" "e" {
replace `x'_formarket=0 if `x'_formarket==. & `x'_dommarket~=.
replace `x'_dommarket=0 if `x'_dommarket==. & `x'_formarket~=.
replace `x'_forshare=0 if `x'_formarket==0
replace `x'_domshare=0 if `x'_dommarket==0
}

*Merge in Currency composition of debt in dataset for each firm
mmerge cusip6 using "$resultstemp/firm_curr_`plotyear'_all.dta"
drop if _merge==2

cap rename share_AUD curr_AUS
cap rename share_BRL curr_BRA
cap rename share_CAD curr_CAN
cap rename share_CHF curr_CHE
cap rename share_CNY curr_CHN
cap rename share_CLP curr_CHL
cap rename share_DKK curr_DNK
cap rename share_EUR curr_EMU
cap rename share_GBP curr_GBR
cap rename share_MXN curr_MEX
cap rename share_NOK curr_NOR
cap rename share_NZD curr_NZL
cap rename share_SEK curr_SWE
cap rename share_USD curr_USA
drop share*

gen curr_lc=.
gen curr_fc=.
foreach x in $ctygroupA_list  {
	display "`x'"
	cap replace curr_lc=curr_`x' if iso=="`x'"
	cap replace curr_fc=1-curr_lc if iso=="`x'"
	}

gen lc_only=0 if curr_lc~=.
replace lc_only=1 if curr_lc==1
drop curr_*

gsort iso -b_domshare -b_forshare
bys iso: gen b_rank = _n if b_domshare~=.
gsort iso -e_domshare -e_forshare
bys iso: gen e_rank = _n if e_domshare~=.

foreach x in "b" "e" {
gen `x'_log_ratio=log(`x'_forshare)-log(`x'_domshare)
winsor `x'_log_ratio, gen(w_`x'_log_ratio) p(.01)
gen `x'_markettotal=`x'_formarket+`x'_dommarket
}

*Plot how relatively overweight foreign and domestic investors are in a firm's equity
*and debt for LC only and MC firms
foreach x in $ctygroupB_list  {
	display "`x'"
	reg w_b_log_ratio w_e_log_ratio if lc_o==1 & iso_co=="`x'" [aweight = b_markettotal]
	local b_lc=round(_b[w_e_log_ratio],.001)
	local r2_lc=round(e(r2),.001)
	reg w_b_log_ratio w_e_log_ratio if lc_o==0 & iso_co=="`x'" [aweight = b_markettotal]
	local b_mc=round(_b[w_e_log_ratio],.001)
	local r2_mc=round(e(r2),.001)
	local note="LC: Slope=`b_lc', R2=`r2_lc';    MC: Slope=`b_mc', R2=`r2_mc'"
	display "`note'"
	twoway (scatter w_b_log_ratio w_e_log_ratio if lc_o==1 [aweight = b_markettotal], msize(small) mfcolor(none) mlcolor(red))  (scatter w_b_log_ratio w_e_log_ratio if lc_o==0 [aweight = b_markettotal], msize(small) mfcolor(none) mlcolor(blue)) (lfit w_b_log_ratio w_e_log_ratio if lc_o==1 [aweight = b_markettotal], lcolor(red))  (lfit w_b_log_ratio w_e_log_ratio if lc_o==0 [aweight = b_markettotal], lcolor(blue))  if iso_co=="`x'", legend(order(1 "LC" 2 "MC")) xtitle("Equity") ytitle("Debt") name("`x'", replace) graphregion(color(white)) title("`x'") ylabel(-5(5)5) xlabel(-6(2)6) 
	graph export "$resultstemp/firmgraphs/`x'_DE_Scatter.eps", replace
	twoway (scatter w_b_log_ratio w_e_log_ratio if lc_o==1 [aweight = b_markettotal], msize(small) mfcolor(none) mlcolor(red))  (scatter w_b_log_ratio w_e_log_ratio if lc_o==0 [aweight = b_markettotal], msize(small) mfcolor(none) mlcolor(blue)) (lfit w_b_log_ratio w_e_log_ratio if lc_o==1 [aweight = b_markettotal], lcolor(red))  (lfit w_b_log_ratio w_e_log_ratio if lc_o==0 [aweight = b_markettotal], lcolor(blue))  if iso_co=="`x'", legend(order(1 "LC" 2 "MC")) xtitle("Equity") ytitle("Debt") name("`x'", replace) graphregion(color(white)) ylabel(-5(5)5) xlabel(-6(2)6)
	graph export "$resultstemp/firmgraphs/`x'_DE_Scatter_notitle.eps", replace
}
save "$resultstemp/bond_equity_shares.dta", replace

cap log close
