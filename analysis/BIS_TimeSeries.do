* This File Generates the comparison of Dollar/Euro currency shares in our data with those shown by BIS data, plotted in Appendix Figure A.8.

cap log close
log using "$logs/${whoami}_BIS_TimeSeries", replace

* Import full BIS debt statistics data set
set more off
cap mkdir $temp/BIS
insheet using "$raw/BIS/WEBSTATS_DEBTSEC_DATAFLOW_csv_col.csv", clear
save "$temp/BIS/BIS_bulk.dta", replace

* Pick desired values
use "$temp/BIS/BIS_bulk.dta", clear
keep if issuecurrencygroup == "Foreign currencies"
keep if measure == "I"
keep if ratetype == "All rate types"
keep if originalmaturity == "Total (all maturities)"
keep if remainingmaturity == "Total (all maturities)"
keep if issuernationality == "All countries excluding residents"
keep if issuemarket == "International markets"
keep if issuerresidence == "All countries excluding residents"
keep if inlist(issue_cur, "EU1", "TO1", "USD")

* Reshape the data
drop v30
keep issuersectorimmediate issuersectorultimateborrower q* v* issue_cur
foreach i of varlist _all {
	local a : variable label `i'
	local a: subinstr local a "-" "_"
	label var `i' "`a'"
}
foreach v of varlist q* v* {
   local x : variable label `v'
   rename `v' value_`x'
}
foreach v of varlist value* {
   destring `v', force replace
}
reshape long value, j(date, string) i(issuersectorultimateborrower issue_cur issuersectorimmediate)
replace date = subinstr(date, "_", "", .)
replace date = subinstr(date, "Q", "q", .)
gen qdate = quarterly(date, "YQ")
format %tq qdate
drop date
rename qdate date

* Finalize the BIS data
keep if date >= tq(2005q1)
keep if issuersectorimmediateborrower == "All issuers" & issuersectorultimateborrower == "All issuers"
drop issuersectorimmediateborrower issuersectorultimateborrower
replace issue_cur = "EUR" if issue_cur == "EU1"
gen _tot_val = value if issue_cur == "TO1"
replace _tot_val = 0 if missing(_tot_val)
bysort date: egen tot_val = total(_tot_val)
drop _tot_val
drop if issue_cur == "TO1"
gen currency_share = value / tot_val
save "$temp/BIS/USD_EUR_shares_clean", replace

* Currency shares for merge
use "$temp/BIS/USD_EUR_shares_clean", clear
keep issue_cur date currency_share
rename date date_q
reshape wide currency_share, i(date) j(issue_cur, string)
save $temp/BIS/currency_shares_long, replace

* Reconstruct the internal series
local suffix = ""
local bondlab = "B"
local geolab = "inotj"
local inds = "All"
use $mns_data/results/temp/HD_foranalysis_q`suffix', clear
cap drop if date_q==tq(2018q4)
if "`inds'"=="All" {
	local indslab = ""
}
if "`geolab'"=="inotj" {
	local subtitle = "i != j (i.e. international flows)"
	drop if DomicileCountryId==iso_country_code
}
if "`bondlab'"=="B" {
	keep if mns_class=="B" & !missing(currency_id)
	local bondtitle = "Bonds"
}
collapse (sum) marketvalue_usd, by(date_q currency_id mns_class mns_subclass DomicileCountryId iso_country_code)

*Generate Global Plots
bys date_q: egen mv_world = sum(marketvalue_usd)
foreach globcur in "USD" "EUR" "GBP" "JPY" { 
	gen mv_`globcur' = marketvalue_usd if currency_id=="`globcur'"
	bys date_q: egen mv_`globcur'_world = sum(mv_`globcur')
	gen `globcur'_share_world = mv_`globcur'_world/mv_world
}
local lineloc = tq(2008q3)
egen date_tag = tag(date_q)

* Merge with BIS
keep EUR_share_world USD_share_world date_q
duplicates drop
mmerge date_q using $temp/BIS/currency_shares_long, unmatched(m)
drop _merge

* Final graph
local lineloc = tq(2008q3)
line USD_share_world EUR_share_world currency_shareUSD currency_shareEUR date_q if date_q>=tq(2005q1), text(1.0 `lineloc' "08:Q3", place(c) size(vsmall) color(gray)) xline(`lineloc', lw(thin) lp(dash) lc(gray)) ylabel(0(0.2)1) graphregion(color(white)) lpattern(solid dash dash_dot longdash) lcolor(red blue red blue) legend(label(1 "USD") rows(1)) legend(label(2 "EUR")) legend(label(3 "USD BIS")) legend(label(4 "EUR BIS")) xtitle("")
graph export $resultstemp/graphs/cs2_raw_BIS.eps, as(eps) replace

log close
