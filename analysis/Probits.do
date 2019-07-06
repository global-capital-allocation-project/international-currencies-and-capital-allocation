* --------------------------------------------------------------------------------------------------
* Probit regressions for MC issuance
*This file produces the probit regressions results examining the relationship between 
*firm size and a firm's probability of borrowing in foreing currency.
*The file produces Table 5 and Appendix Table A.7
* --------------------------------------------------------------------------------------------------
log using "$logs/${whoami}_Probits", replace
cap mkdir $temp/probits
cap mkdir $temp/probits/segment
cap mkdir $regressions/probits

* --------------------------------------------------------------------------------------------------
* Prepare Worldscope segment data
* --------------------------------------------------------------------------------------------------

* Ensure unique mapping (ISIN-CUSIP)
use $sdc_additional/cgs_isin.dta, clear
sort isin cusip cgs_currency, stable
by isin: keep if _n == 1
save $sdc_additional/cgs_isin_unique.dta, replace

* Cleaning Worldscope data
global segment_temp $temp/probits/segment

* Clean Worldscope, but only keep foreign share
use $raw/segment/wrds_ws_segments.dta, clear
keep code year_ freq ITEM5601 ITEM6004 ITEM6008 ITEM6035 ITEM6038 ITEM6105 ITEM7101 ITEM8731 Date_
foreach x of varlist _all {
	local temp=lower("`x'")
	rename `x' `temp'
}
rename item8731 foreign_sales_share
rename item5601 ticker
rename item6004 cusip9
rename item6008 isin
rename item6035 worldscopeid
rename item6038 ibes_ticker
rename item6105 ws_permid
rename item7101 foreign_sales

cap drop _counter
gen _counter = 0
replace _counter = 1 if ~missing(isin)
sort isin year_, stable
by isin year_: egen count=total(_counter)
drop if count==0
gen A_dum_temp=0
gen B_dum_temp=0
replace A_dum_temp=1 if freq=="A"
replace B_dum_temp=1 if freq=="B"
sort isin year_, stable
by isin year_: egen A_dum=max(A_dum_temp)
by isin year_: egen B_dum=max(B_dum_temp)

tab A_dum B_dum if count==1
tab A_dum B_dum if count==2 
keep if freq=="A"
drop A_d* B_d* count
duplicates drop isin year, force
replace foreign_sales_share=. if foreign_sales_share<0 | foreign_sales_share>100
replace foreign_sales_share=foreign_sales_share/100
rename cusip9 ws_cusip9
rename year year
mmerge isin using $sdc_additional/cgs_isin_unique.dta, ukeep(cusip9) uname(cgs_) unmatched(m)
sort year isin cgs_cusip9, stable
gen cusip9=ws_cusip9
replace cusip9=cgs_cusip9 if cusip9==""
drop if cusip9==""
gen cusip6=substr(cusip9,1,6)
mmerge cusip6 using $temp/country_master/up_aggregation_final_compact.dta, umatch(issuer_number) ukeep(cusip6_up_bg  country_bg issuer_name_up) unmatched(m)
sort year isin cgs_cusip9, stable 
keep if _merge==3
drop if foreign_sales_share==.

* Within year
sort cusip6_up_bg year, stable
by cusip6_up_bg year: egen max_date=max(date)
keep if date==max_date
drop max_date

* If the same cusip6_bg has the same foreign sale share, it's all the ame
duplicates drop foreign_sales_share cusip6_up_bg year, force

cap drop _counter
gen _counter = 0
replace _counter = 1 if ~missing(cusip6)
sort cusip6 year, stable
by cusip6 year: egen count=total(_counter)
cap drop _counter
gen _counter = 0
replace _counter = 1 if ~missing(cusip6_up_bg)
sort cusip6_up_bg year, stable
by cusip6_up_bg year: egen up_count=total(_counter)

gen up_nameshort=substr(issuer_name_up,1,30)
order year cusip6 cusip6_up_bg up_nameshort foreign_sales_share
gen orig_up=0
replace orig_up=1 if cusip6==cusip6_up_bg
sort cusip6_up_bg year, stable
by cusip6_up_bg year: egen max_orig_up=max(orig_up)

*if there are 2, keep the data for the one that was originally the ultimate parent
drop if up_count>1 & orig_up==0 & max_orig_up==1
drop count up_count orig_up max_orig_up

*IF THERE ARE STILL DUPLICATES, KEEP THE ONE WITH THE MOST FOREIGN SALES IN LEVELS
*THE BIGGEST ONE IS PROBABLY THE PARENT.  
*THIS DOES NOT IMPOSE CONSISTENTCY ACROSS YEARS
cap drop _counter
gen _counter = 0
replace _counter = 1 if ~missing(cusip6)
sort cusip6 year, stable
by cusip6 year: egen count=total(_counter)
cap drop _counter
gen _counter = 0
replace _counter = 1 if ~missing(cusip6_up_bg)
sort cusip6_up_bg year, stable
by cusip6_up_bg year: egen up_count=total(_counter)

replace foreign_sales=foreign_sales/(10^6)
gen fs_round=round(foreign_sales,1)
sort cusip6_up_bg year, stable
by cusip6_up_bg year: egen max_fs=max(fs_round)
gen max_fs_dum=0
replace max_fs_dum=1 if fs_round==max_fs
order cusip6 cusip6_up_bg year up_nameshort max_fs_dum max_fs fs_round
sort cusip6_up_bg, stable
drop if up_co>1 & max_fs_dum==0
drop max_fs_dum max_fs count up_count
*Very few duplicates remain
duplicates drop cusip6_up_bg year, force
save $segment_temp/segment_clean.dta, replace
keep if year == 2017
save $segment_temp/segment_clean_2017.dta, replace

* --------------------------------------------------------------------------------------------------
* Data preparation
* --------------------------------------------------------------------------------------------------

* Prep country master file for merge
use "$temp/country_master/up_aggregation_final_compact.dta", clear
rename issuer_number cusip6
rename cusip6_up_bg cusip6_bg
rename country_bg cusip6_bg_country_bg
keep cusip6 cusip6_bg cusip6_bg_country_bg
sort cusip6, stable
drop if missing(cusip6)
drop if strlen(trim(cusip6)) < 6
drop if cusip6 == "#N/A N"
drop if cusip6 == "000000"
save "$temp/country_master/country_for_sdc_merge.dta", replace

* SDC temp file
tempfile sdctemp
use PrincipalAmountmil cusip6_sdc cusip6_cgs CUSIP issue_date maturity_date Currency IssuerBorrowerSEDOL using "$sdc_datasets/sdc_appended" if issue_date<=td(01jan2017) & maturity_date>=td(31dec2017), clear
rename IssuerBorrowerSEDOL sedol
gen cusip6 = CUSIP 
replace cusip6=cusip6_sdc  if cusip6==""
replace cusip6=cusip6_cgs if cusip6==""
drop if cusip6==""
mmerge  cusip6 using "$temp/country_master/country_for_sdc_merge.dta", ukeep(cusip6_bg cusip6_bg_country_bg) 
drop if _merge==2
drop _merge
destring PrincipalAmountmil, replace
replace PrincipalAmountmil=PrincipalAmountmil*(10^6)
drop CUSIP cusip6_cgs cusip6_sdc cusip6 issue matur
save "`sdctemp'", replace

* DLG temp file
tempfile dlgtemp
use $temp/dealogic/dlg_bonds_consolidated.dta if pricingdate<=td(01jan2017) & maturitydate>=td(31dec2017), clear
keep cusip6_bg country_bg currencyisocode value
gen sedol = ""
gen PrincipalAmountmil = value
rename country_bg cusip6_bg_country_bg
rename currencyisocode Currency
drop value
drop if missing(cusip6_bg)
save "`dlgtemp'", replace

* DLG SIC codes (in cases of conflicts, take the issuerid with largest issuance)
use $raw/dealogic/stata/companysiccodes, clear
keep if isprimary == 1
save $temp/probits/dlg_companysic_primary, replace
use $temp/dealogic/dlg_bonds_consolidated.dta, clear
drop if missing(cusip6_bg)
collapse (sum) value, by(issuerid cusip6_bg)
sort cusip6_bg value, stable
by cusip6_bg: keep if _n == _N
keep issuerid cusip6_bg
rename issuerid companyid
mmerge companyid using $temp/probits/dlg_companysic_primary, unmatched(m)
drop if missing(cusip6_bg) | missing(code)
keep cusip6_bg code
rename code dlg_sic4
tostring dlg_sic4, force replace
gen dlg_sic2 = substr(dlg_sic4, 1, 2)
destring dlg_sic4, gen(_sic4)
gen dlg_type = "C"
replace dlg_type = "S" if _sic4 >= 9000
replace dlg_type = "SF" if inlist(_sic4, 6111, 8888, 6189)
save $temp/probits/dlg_sic, replace

* Classify multi-currency (MC) issuers from SDC data
tempfile sdc_mc
use $temp/country_currencies if (missing(start_date) | start_date<=td(01jan2017)) & (missing(end_date) | end_date>=td(31dec2017)), clear
drop *_date
rename iso_country_code cusip6_bg_country_bg
merge 1:m cusip6_bg_country_bg using  "`sdctemp'", keep(3) nogen
drop if missing(Currency)
gen lc = 1 if Currency==iso_currency_code
gen fc = 1 if Currency!=iso_currency_code
gen mktval_lc = PrincipalAmountmil if lc==1
gen mktval_fc = PrincipalAmountmil if fc==1
replace cusip6_bg_country_bg="EMU" if inlist(cusip6_bg_country_bg,$eu1)==1 | inlist(cusip6_bg_country_bg,$eu2)==1 |inlist(cusip6_bg_country_bg,$eu3)==1
rename cusip6_bg_country_bg country_bg
replace country_name = "European Monetary Union" if country_bg=="EMU"
collapse (max) lc fc (sum) mktval_lc mktval_fc (lastnm) country_name country_bg iso_currency_code, by(cusip6_bg)
drop lc fc
gen mktval = mktval_lc+mktval_fc
gen fc_share=mktval_fc/(mktval_lc+mktval_fc)
gen mc=0
replace mc=1 if fc_share>$fc_thresh & fc_share~=.
save "$temp/probits/sdc_mc", replace

* Classify multi-currency (MC) issuers from DLG data
use $temp/country_currencies if (missing(start_date) | start_date<=td(01jan2017)) & (missing(end_date) | end_date>=td(31dec2017)), clear
drop *_date
rename iso_country_code cusip6_bg_country_bg
merge 1:m cusip6_bg_country_bg using  "`dlgtemp'", keep(3) nogen
drop if missing(Currency)
gen lc = 1 if Currency==iso_currency_code
gen fc = 1 if Currency!=iso_currency_code
gen mktval_lc = PrincipalAmountmil if lc==1
gen mktval_fc = PrincipalAmountmil if fc==1
replace cusip6_bg_country_bg="EMU" if inlist(cusip6_bg_country_bg,$eu1)==1 | inlist(cusip6_bg_country_bg,$eu2)==1 |inlist(cusip6_bg_country_bg,$eu3)==1
rename cusip6_bg_country_bg country_bg
replace country_name = "European Monetary Union" if country_bg=="EMU"
collapse (max) lc fc (sum) mktval_lc mktval_fc (lastnm) country_name country_bg iso_currency_code, by(cusip6_bg)
drop lc fc
gen mktval = mktval_lc+mktval_fc
gen fc_share=mktval_fc/(mktval_lc+mktval_fc)
gen mc=0
replace mc=1 if fc_share>$fc_thresh & fc_share~=.
save "$temp/probits/dlg_mc", replace

* Combine SDC and DLG
use "$temp/probits/dlg_mc", clear
gen _file = "dlg"
append using "$temp/probits/sdc_mc"
replace _file = "sdc" if missing(_file)
sort cusip6_bg _file, stable
by cusip6_bg: keep if _n == 1
gen _cusip6_bg = cusip6_bg
mmerge _cusip6_bg using "$temp/country_master/up_aggregation_final_compact.dta", unmatched(m) umatch(issuer_number) ukeep(issuer_name)
rename issuer_name bg_name
drop _cusip6_bg
mmerge cusip6_bg using "$sdc_datasets/sdc_sic.dta", umatch(cusip6) unmatched(m)
mmerge cusip6_bg using "$temp/probits/dlg_sic.dta", unmatched(m)
destring dlg_sic2, force replace
replace sic2 = dlg_sic2 if missing(sic2)
tostring sic2, gen(_sic2)
cap drop sic1
gen sic1 = substr(_sic2, 1, 1)
destring sic1, force replace
drop _sic2
drop _merge
replace type = dlg_type if missing(type)

* Manual correction: drop certain JP Morgan subsidiaries that we fail to aggregate
drop if (strpos(bg_name, "J P MORGAN") | strpos(bg_name, "J.P. MORGAN")) & cusip6_bg != "46625H"

* Add NAICS divisions
gen naic_division = ""
replace naic_division = "A" if sic2 >= 1 & sic2 <= 9
replace naic_division = "B" if sic2 >= 10 & sic2 <= 14
replace naic_division = "C" if sic2 >= 15 & sic2 <= 17
replace naic_division = "D" if sic2 >= 20 & sic2 <= 39
replace naic_division = "E" if sic2 >= 40 & sic2 <= 49
replace naic_division = "F" if sic2 >= 50 & sic2 <= 51
replace naic_division = "G" if sic2 >= 52 & sic2 <= 59
replace naic_division = "H" if sic2 >= 60 & sic2 <= 67
replace naic_division = "I" if sic2 >= 70 & sic2 <= 89
replace naic_division = "J" if sic2 >= 90 & sic2 <= 99
drop if missing(sic2)
assert naic_division != ""

* Merge in foreign share % from Worldscope segment data
mmerge cusip6_bg using $segment_temp/segment_clean_2017.dta, unmatched(m) ukeep(foreign_sales_share) umatch(cusip6_up_bg)

* Save the merge file
save $temp/probits/dlg_sdc_appended_firms, replace

* --------------------------------------------------------------------------------------------------
* Issuance regressions (do not use CS/WS)
* --------------------------------------------------------------------------------------------------

use $temp/probits/dlg_sdc_appended_firms, clear

* Regressions with issuance
foreach x in mktval {
	cap drop `x'_b
	cap drop log_`x'_b
	gen `x'_b=`x'/(10^9)
	gen log_`x'_b = log(`x'_b)
}
encode naic_division, gen(_naic_division)

* Standard regressions
foreach z in "log_" {
	foreach var in mktval_b {
		cap rm "$regressions/probits/`z'`var'_baseline.xls"
		cap rm "$regressions/probits/`z'`var'_baseline_dta.dta"
		cap rm "$regressions/probits/`z'`var'_baseline.txt"
		cap rm "$regressions/probits/`z'`var'_baseline.tex"
		foreach control in "i.sic2" "i._naic_division" "" {
			foreach country in $ctygroupA_list {
				display "`z' `var' `country'"

				* Standard regression
				probit mc `z'`var' `control'  if sic1~=9 & type=="C" & country_bg=="`country'", r
				margins, dydx(`z'`var') post
				if "`control'" == "i.sic2" {
					outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var') ctitle("`country'") addtext("Industry FE", SIC2) dec(3)  excel tex dta					
				}
				else if "`control'" == "i._naic_division" {
					outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var') ctitle("`country'") addtext("Industry FE", NAICS Division) dec(3)  excel tex dta					
				}
				else {
					outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var') ctitle("`country'") addtext("Industry FE", None) dec(3)  excel tex dta					
				}

				* Regression with foreign share control
				cap {
					probit mc `z'`var' `control' foreign_sales_share if sic1~=9 & type=="C" & country_bg=="`country'", r
					margins, dydx(`z'`var' foreign_sales_share) post
					if "`control'" == "i.sic2" {
						outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var' foreign_sales_share) ctitle("`country'") addtext("Industry FE", SIC2) dec(3)  excel tex dta					
					}
					else if "`control'" == "i._naic_division" {
						outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var' foreign_sales_share) ctitle("`country'") addtext("Industry FE", NAICS Division) dec(3)  excel tex dta					
					}
					else {
						outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var' foreign_sales_share) ctitle("`country'") addtext("Industry FE", None) dec(3)  excel tex dta					
					}
				}
			}
		}
	}
}

* --------------------------------------------------------------------------------------------------
* Exchange rates
* --------------------------------------------------------------------------------------------------

import excel using $raw/factset/Factset_FX_EOY_2017.xlsx, clear firstrow sheet("Sheet1")
rename exch s_eop
save $temp/probits/factset_fx_2017, replace

* --------------------------------------------------------------------------------------------------
* Process Worldscope
* --------------------------------------------------------------------------------------------------

use $raw/wrds/Worldscope/worldscope_feb19, clear
order item6004 item8236 
order item6035
order item6008
keep item6099 item6001 item6003 item3255 item6035 item6004 item6008 item8236 item2999 item6006 item6010 item6011 item1001 item6026 item8355 item8435 item2301 item2501 item8431 item18191 item18198
rename item6001 company_name
rename item6003 company_name_short
rename item6004 cusip_ws
rename item6006 sedol
rename item6008 isin
rename item8236 leverage
rename item2999 assets
rename item3255 debt
rename item6010 gics
rename item6011 industry_group
rename item1001 revenue
rename item6026 nation
rename item8355 sales_employee
rename item8431 sales_assets
rename item8435 sales_assets_5ylag
rename item2301 ppe_gross
rename item2501 ppe_net
rename item6035 ws_identifier
rename item6099 currency
rename item18191 ebit
rename item18198 ebitda
mmerge isin using "$sdc_additional/cgs_isin_unique.dta", ukeep(cusip9) unmatched(m)
rename cusip9 cusip
replace cusip = cusip_ws if missing(cusip)
gen cusip6_cgs=substr(cusip,1,6)
drop _merge cusip_ws

sort cusip6_cgs revenue ppe_gross ppe_net assets sales_employee sales_assets sales_assets_5ylag ebit ebitda gics industry_group nation debt company_name company_name_short currency sedol, stable

collapse (lastnm) cusip isin revenue ppe_gross ppe_net assets sales_employee sales_assets sales_assets_5ylag gics industry_group nation debt company_name company_name_short currency ebit ebitda sedol, by(cusip6_cgs)

foreach x in revenue ppe_gross ppe_net assets sales_employee debt ebit ebitda {
	replace `x'=`x'/(10^6)
}	
gen equity=assets-debt
gen source="ws"

gen cusip6 = cusip6_cgs
drop cusip6_*
drop if cusip6==""
sort cusip6, stable
by cusip6: gen n=_n
keep if n==1
drop n 

mmerge currency using $temp/probits/factset_fx_2017, umatch(iso_currency) 
drop if _merge==2

* Assumes if currency if missing and it's US, currency is USD
replace s_eop=1 if currency=="USD" | (currency=="" & nation=="UNITED STATES")
drop if s_eop==.
foreach x in revenue ppe_gross ppe_net assets sales_employee debt equity ebit ebitda {
	replace `x'=`x'/s_eop
}	
drop _merge 

* If bigger than Walmart, problem
drop if nation=="INDONESIA" | nation=="CHILE" | nation=="RUSSIA"
drop if revenue>=1000000 & revenue~=.
save $temp/probits/ws_keyvars_2017, replace

* --------------------------------------------------------------------------------------------------
* Process Compustat
* --------------------------------------------------------------------------------------------------

* North America
use gvkey tic cusip fyear datadate at dt ppent ppegt capx ceq revt conm sich curcd currtr ebit ebitda using $raw/wrds/Compustat/cs_northam_feb19, clear
sort gvkey datadate, stable
by gvkey: keep if _n == _N
save $temp/probits/cs_northam_feb19_latest, replace

* Global
use gvkey isin fyear datadate at ppent ppegt capx ceq revt sich curcd conm ebit ebitda using $raw/wrds/Compustat/cs_global_feb19, clear
sort gvkey datadate, stable
by gvkey: keep if _n == _N
mmerge isin using  "$sdc_additional/cgs_isin_unique.dta", ukeep(cusip9) unmatched(m)
rename cusip9 cusip
gen source="cs_g"
save $temp/probits/cs_global_feb19_latest, replace

* Append
use $temp/probits/cs_global_feb19_latest, clear
append using $temp/probits/cs_northam_feb19_latest
replace source="cs_na" if source==""
rename revt revenue
rename at assets
rename dt debt
rename ppegt ppe_gross
rename ppent ppe_net
gen equity=assets-debt
rename conm company_name
order gvkey tic isin cusip

mmerge curcd using $temp/probits/factset_fx_2017.dta, umatch(iso_currency) 
drop if _merge==2
replace s_eop=1 if curcd=="USD"
foreach x in assets ppe_net ppe_gross capx ceq revenue debt equity ebit ebitda {
	replace `x'=`x'/s_eop
}

gen counter = 1 if !missing(gvkey)
sort gvkey, stable
by gvkey: egen count=sum(counter)
drop counter
drop if source=="cs_g" & count==2
drop count
gen cusip6=substr(cusip,1,6)
drop if cusip6==""
gen counter = 1 if !missing(cusip6)
sort cusip6, stable
by cusip6: egen count=sum(counter)
drop counter
order cusip6 company_name
*ALL COUNTS >=3 ARE FUND SHARES
drop if count>=3
sort cusip6, stable
by cusip6: gen n=_n
*keep first of remainders
keep if n==1
drop n count

foreach x of varlist * {
	rename `x' cs_`x'
}	
rename cs_cusip cusip9
rename cs_cusip6 cusip6
save $temp/probits/cs_keyvars_2017, replace

* --------------------------------------------------------------------------------------------------
* CS/WS merge
* --------------------------------------------------------------------------------------------------

* Merge CS and WS; adjust fundamentals
use $temp/probits/cs_keyvars_2017, clear
mmerge cusip6 using $temp/probits/ws_keyvars_2017, unmatched(b) uname(ws_)
drop _merge

foreach x in revenue ppe_gross ppe_net assets debt equity sales_employee sales_assets sales_assets_5ylag gics industry_group sich ebit ebitda {
	cap gen `x'=.
	cap replace `x'=cs_`x'
	cap replace `x'=ws_`x' if `x'==.
	cap local vars="`vars' `x'"
	}
	
foreach x in  isin company_name {
	cap gen `x'=""
	cap replace `x'=cs_`x'
	cap replace `x'=ws_`x' if `x'==""
	cap local vars="`vars' `x'"
	}	
	
*UNITS AREN'T RIGHT
drop if ws_na=="COLOMBIA" | ws_na=="CHILE" | ws_nat=="INDONESIA" | regexm(company_n,"DOOSAN")==1

*WINSORIZING
foreach x in revenue ppe_gross ppe_net assets debt equity sales_employee sales_assets sales_assets_5ylag ebit ebitda {
		cap winsor `x', gen(w_`x') p(0.01) 
}		
	
foreach x in revenue ppe_gross ppe_net assets debt equity sales_employee sales_assets sales_assets_5ylag  w_revenue w_ppe_gross w_ppe_net w_assets w_debt w_equity w_sales_employee w_sales_assets w_sales_assets_5ylag ebit ebitda w_ebit w_ebitda {
	gen log_`x'=log(`x')
}	

*2 digit industry
tostring ind, replace	
gen ind2=substr(ind,1,2)
destring ind2, replace
destring industry_group, replace

* Drop if there are large discrepancies
foreach x in revenue assets debt {
	gen ratio_`x'=cs_`x'/ws_`x'
}	
drop if (ratio_rev>2 | ratio_rev<.5) & ratio_rev~=. & cs_curcd~=ws_curr	
drop cs* ws*

*GICS
label define gics 1 "Industrial" 2 "Utility" 3 "Transportation" 4 "Bank" 5 "Insurance" 6 "Other_Financial"
label values gics gics
save $temp/probits/ws_cs_prelim, replace

* Merge with UPs
use $temp/probits/ws_cs_prelim, clear
mmerge cusip6 using "$temp/country_master/country_for_sdc_merge.dta", ukeep(cusip6_bg cusip6_bg_cou)
keep if _merge==3
sort cusip6_bg, stable
by cusip6_bg: egen max_assets=max(assets)

gen counter = 1 if !missing(cusip6_bg)
sort cusip6_bg, stable
by cusip6_bg: egen count=sum(counter)
drop counter

drop if count > 1 & float(assets) != float(max_assets)
drop count
sort cusip6_bg, stable
by cusip6_bg: egen max_revenue=max(revenue)

gen counter = 1 if !missing(cusip6_bg)
sort cusip6_bg, stable
by cusip6_bg: egen count=sum(counter)
drop counter

drop if count>1 & float(revenue) != float(max_revenue)
sort cusip6_bg, stable
by cusip6_bg: gen n=_n
keep if n==1
drop n count
replace cusip6_bg_country_bg="EMU" if inlist(cusip6_bg_country_bg,$eu1)==1 | inlist(cusip6_bg_country_bg,$eu2)==1 |inlist(cusip6_bg_country_bg,$eu3)==1
rename cusip6_bg_cou country_bg
save $temp/probits/ws_cs_prelim_bg, replace

* --------------------------------------------------------------------------------------------------
* Now perform the CS/WS merge
* --------------------------------------------------------------------------------------------------

* Merge files
use $temp/probits/dlg_sdc_appended_firms, clear
keep if type == "C" & sic1~=9
mmerge cusip6_bg using $temp/probits/ws_cs_prelim_bg, unmatched(m)
order mktval bg_name _file sic4 dlg_sic4
br if _merge == 1
save $temp/probits/dlg_sdc_appended_firms_bg, replace

* --------------------------------------------------------------------------------------------------
* Prep data from Capital IQ
* --------------------------------------------------------------------------------------------------

* Process identifiers
use $raw/ciq/wrds_cusip, clear
gen cusip6 = substr(cusip,1,6)
mmerge cusip6 using "$temp/country_master/country_for_sdc_merge.dta", ukeep(cusip6_bg cusip6_bg_cou) unmatched(m)
drop _merge
sort cusip6_bg, stable
save $temp/probits/ciq_cusip, replace

* Parsing program
cap program drop process_ciq_fundamentals
program process_ciq_fundamentals

	destring IQ_TOTAL_REV, force replace
	destring IQ_REV, force replace
	destring IQ_EBIT, force replace
	destring IQ_TOTAL_ASSETS, force replace
	replace IQ_TOTAL_REV = . if IQ_TOTAL_REV == 0
	replace IQ_REV = . if IQ_REV == 0
	replace IQ_EBIT = . if IQ_EBIT == 0
	replace IQ_TOTAL_ASSETS = . if IQ_TOTAL_ASSETS == 0
	replace IQ_TOTAL_REV = IQ_REV if missing(IQ_TOTAL_REV) & ~missing(IQ_REV)
	drop IQ_REV
	drop if missing(IQ_TOTAL_REV) & missing(IQ_EBIT) & missing(IQ_TOTAL_ASSETS)
	duplicates drop cusip6_bg IQ_TOTAL_ASSETS IQ_TOTAL_REV IQ_EBIT, force

	* In case of ties, we use the IQ ID with the largest scale (in terms of assets, then revenue)
	gen nonmissing_assets = 0
	replace nonmissing_assets = 1 if ~missing(IQ_TOTAL_ASSETS)
	sort cusip6_bg, stable
	by cusip6_bg: egen max_nonmissing_assets = max(nonmissing_assets)
	by cusip6_bg: egen max_assets = max(IQ_TOTAL_ASSETS)
	keep if max_nonmissing_assets == 0 | float(IQ_TOTAL_ASSETS) == float(max_assets)
	sort cusip6_bg, stable
	by cusip6_bg: egen max_revenue = max(IQ_TOTAL_REV)
	keep if float(IQ_TOTAL_REV) == float(max_revenue)
	sort cusip6_bg, stable
	by cusip6_bg: keep if _n == 1
	keep cusip6_bg IQ_*

end

* Process SDC/DLG companies: this gives us a list of identifiers for which we look up
* fundamentals in Capital IQ
use $temp/probits/dlg_sdc_appended_firms_bg, clear
keep if _merge == 1
keep cusip6_bg
mmerge cusip6_bg using $temp/probits/ciq_cusip, unmatched(m)
keep if _merge == 3
keep cusip6 cusip6_bg companyid cusip
save $temp/probits/ciq_for_retrieval, replace
export excel using "$temp/probits/ciq_for_retrieval.xlsx", replace firstrow(variables)

* This step involves a manual download in Capital IQ: we use the file above (ciq_for_retrieval)
* to perform a search for fundamentals in Capital IQ, and the output of that search is the file
* that is read in below ("ciq_fundamentals_for_probits")
import excel using $raw/ciq/ciq_fundamentals_for_probits.xlsx, clear firstrow sheet("Sheet1")
process_ciq_fundamentals
save $temp/probits/ciq_fundamentals_formerge, replace

* Now merge everything
use $temp/probits/dlg_sdc_appended_firms_bg, clear
gen fundamentals_merged = 0
gen fundamentals_source = ""
replace fundamentals_merged = 1 if _merge == 3
replace fundamentals_source = "cs_ws" if _merge == 3
mmerge cusip6_bg using $temp/probits/ciq_fundamentals_formerge, unmatched(m)
replace fundamentals_merged = 1 if _merge == 3
replace fundamentals_source = "ciq" if _merge == 3
replace revenue = IQ_TOTAL_REV if _merge == 3 & ~missing(IQ_TOTAL_REV)
replace ebit = IQ_EBIT if _merge == 3 & ~missing(IQ_EBIT) 
replace assets = IQ_TOTAL_ASSETS if _merge == 3 & ~missing(IQ_TOTAL_ASSETS) 
drop w_* log_* IQ_*
drop _sic4 max_assets max_revenue _merge
save $temp/probits/firm_appended_for_fundamental_regs, replace

* --------------------------------------------------------------------------------------------------
* Fundamentals regressions
* --------------------------------------------------------------------------------------------------

use $temp/probits/firm_appended_for_fundamental_regs, clear

* Regressions with issuance: new approach
* 1) We censor EBIT at the bottom percentile of the distribution, conditional on positive EBIT
* 2) We drop firms with negative assets
foreach x in assets ebit revenue {
	cap drop `x'_b
	cap drop log_`x'_b
	cap drop min_log_`x'_b
	winsor2 `x', cuts(1 99)
	gen `x'_b=`x'_w/(10^3)
}
assert revenue_b >= 0
drop if assets_b < 0
quietly su ebit_b if ebit_b > 0, detail
replace ebit_b = `r(p1)' if ebit_b < 0 
foreach x in assets ebit revenue{
	gen log_`x'_b = log(1 + `x'_b)
}
encode naic_division, gen(_naic_division)

* Regressions using SDC and DLG data
foreach z in "log_" {
	foreach var in ebit_b assets_b revenue_b  {
		cap rm "$regressions/probits/`z'`var'_baseline.xls"
		cap rm "$regressions/probits/`z'`var'_baseline_dta.dta"
		cap rm "$regressions/probits/`z'`var'_baseline.txt"
		cap rm "$regressions/probits/`z'`var'_baseline.tex"
		foreach control in "i.sic2" "i._naic_division" "" {
			foreach country in $ctygroupA_list {
				display "`z' `var' `country'"

				* Standard regressions
				probit mc `z'`var' `control'  if sic1~=9 & type=="C" & country_bg=="`country'", r
				margins, dydx(`z'`var') post
				if "`control'" == "i.sic2" {
					outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var') ctitle("`country'") addtext("Industry FE", SIC2) dec(3)  excel tex dta
				}
				else if "`control'" == "i._naic_division" {
					outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var') ctitle("`country'") addtext("Industry FE", NAICS Division) dec(3)  excel tex dta
				}
				else {
					outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var') ctitle("`country'") addtext("Industry FE", None) dec(3)  excel tex dta
				}

				* Regressions with foreign share control
				cap {
					probit mc `z'`var' `control' foreign_sales_share if sic1~=9 & type=="C" & country_bg=="`country'", r
					margins, dydx(`z'`var' foreign_sales_share) post
					if "`control'" == "i.sic2" {
						outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var' foreign_sales_share) ctitle("`country'") addtext("Industry FE", SIC2) dec(3)  excel tex dta
					}
					else if "`control'" == "i._naic_division" {
						outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var' foreign_sales_share) ctitle("`country'") addtext("Industry FE", NAICS Division) dec(3)  excel tex dta
					}
					else {
						outreg2  using "$regressions/probits/`z'`var'_baseline.xls", keep(`z'`var' foreign_sales_share) ctitle("`country'") addtext("Industry FE", None) dec(3)  excel tex dta
					}
				}
			}
		}
	}
}

* --------------------------------------------------------------------------------------------------
* Produce final tables, various versions
* --------------------------------------------------------------------------------------------------

* Main tables
foreach z in "log_" {
	foreach controls in "SIC2" "None" "NAICS Division" {
		foreach x in mktval_b ebit_b assets_b revenue_b {
			di "`z'`x', `controls'"
			use "$regressions/probits/`z'`x'_baseline_dta.dta", clear
			sxpose, clear
			keep if _n < 3 | missing(_var6)
			keep if _n == 1 | _var10 == "`controls'"
			keep _var2 _var4 _var5 _var9
			drop if _n == 1
			rename _var2 country
			rename _var4 `x'beta
			rename _var5 `x'se
			rename _var9 `x'obs
			order `x'beta `x'se `x'obs
			destring `x'obs, force replace ignore(",")
			sort country (`x'obs), stable
			by country: gen n = _n
			by country: gen N = _N
			keep if n == N
			drop n N
			tostring `x'obs, force replace format(%8.0fc)
			reshape long `x', i(country) j(var) str
			if "`controls'"	== "SIC2" {
				local control_lab = "sic2"
			}
			else if  "`controls'"	== "NAICS Division" {
				local control_lab = "naicdiv"
			}
			else {
				local control_lab = "noind"
			}
			save "$regressions/probits/`z'`x'_merge_`control_lab'_baseline.dta", replace
		}
	}
}

* Merge them
foreach z in "log_" {
	foreach control_lab in "sic2" "naicdiv" "noind" {
		use "$regressions/probits/`z'mktval_b_merge_`control_lab'_baseline.dta", clear
		foreach x in ebit_b assets_b revenue_b {
			mmerge country var using "$regressions/probits/`z'`x'_merge_`control_lab'_baseline.dta"
		}
		drop _merge
		gen order=.
		replace order=1 if var=="beta"
		replace order=2 if var=="se"
		replace order=3 if var=="obs"
		sort country order, stable
		drop order
		label var country "Country"
		label var var ""
		label var mktval_b "Bond Issuance"
		label var ebit_b "EBIT"
		label var assets_b "Assets"
		label var revenue_b "Revenue"
		export excel using "$regressions/probits/`z'probit_table_baseline_`control_lab'.xls", firstrow(varlabels) replace
	}
}

* Appendix tables with foreign sales %
foreach z in "log_" {
	foreach controls in "SIC2" "None" "NAICS Division" {
		foreach x in mktval_b ebit_b assets_b revenue_b {
			di "`z'`x', `controls'"
			use "$regressions/probits/`z'`x'_baseline_dta.dta", clear
			sxpose, clear
			keep if _n == 1 | ~missing(_var6)
			keep if _n == 1 | _var10 == "`controls'"
			keep _var2 _var4 _var5 _var6 _var7 _var9
			drop if _n == 1
			rename _var2 country
			rename _var4 `x'beta
			rename _var5 `x'se
			rename _var6 `x'forshare_coef
			rename _var7 `x'forshare_se
			rename _var9 `x'obs
			order `x'beta `x'se `x'obs
			reshape long `x', i(country) j(var) str
				if "`controls'"	== "SIC2" {
				local control_lab = "sic2"
			}
			else if  "`controls'"	== "NAICS Division" {
				local control_lab = "naicdiv"
			}
			else {
				local control_lab = "noind"
			}
			save "$regressions/probits/`z'`x'_merge_`control_lab'_baseline_forshare.dta", replace
		}
	}
}

* Merge them
foreach z in "log_" {
	foreach control_lab in "sic2" "naicdiv" "noind" {
		use "$regressions/probits/`z'mktval_b_merge_`control_lab'_baseline_forshare.dta", clear
		foreach x in ebit_b assets_b revenue_b {
			mmerge country var using "$regressions/probits/`z'`x'_merge_`control_lab'_baseline_forshare.dta"
		}
		drop _merge
		gen order=.
		replace order=1 if var=="beta"
		replace order=2 if var=="se"
		replace order=3 if var=="forshare_coef"
		replace order=4 if var=="forshare_se"
		replace order=5 if var=="obs"
		sort country order, stable
		drop order
		label var country "Country"
		label var var ""
		label var mktval_b "Bond Issuance"
		label var ebit_b "EBIT"
		label var assets_b "Assets"
		label var revenue_b "Revenue"
		export excel using "$regressions/probits/fsharetab_`z'probit_table_baseline_`control_lab'.xls", firstrow(varlabels) replace
	}
}

log close
