* --------------------------------------------------------------------------------------------------
* Create_Final_Files
*
* This job generates the final "HD" (Holding Detail) files that are used in the analysis. The HD
* files are generated at both monthly, quarterly, and yearly frequencies.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Create_Final_Files_`2'", replace
local year = `2' + $firstyear - 1

* Load step6 files, which include all steps through fund-in-fund position unwinding
use "$output/HoldingDetail/US_`year'_m_step6.dta", clear
append using "$output/HoldingDetail/NonUS_`year'_m_step6.dta"
cap drop _merge obs
cap drop index
cap drop _obs_id
sort MasterPortfolioId date _storageid

* Ensure consistency of mns_class and mns_subclass
mmerge cusip using "$temp/Internal_Class_NonUS_US.dta", unmatched(m) uname(modal_)
replace mns_class = modal_mns_class if ~missing(modal_mns_class) & _merge == 3
replace mns_subclass = modal_mns_subclass if ~missing(modal_mns_subclass) & _merge == 3
drop modal_*

* Fix Chilean currency codes
replace currency_id = "CLP" if currency_id == "CLF"

* Adjust domicile for a number of funds that Morningstar incorrectly reports as AUS-domiciled;
* for some funds we cannot establish a domicile, and these are dropped
foreach mpid in "31693" "1000801" "1636280" "1120876" {
	replace DomicileCountryId = "USA" if MasterPortfolioId == `mpid' & DomicileCountryId == "AUS"
}
foreach mpid in "1243050" "887103" "887102" {
	replace DomicileCountryId = "LUX" if MasterPortfolioId == `mpid' & DomicileCountryId == "AUS"
}
foreach mpid in "97979" "929457" "14262" {
	drop if MasterPortfolioId == `mpid' & DomicileCountryId == "AUS"
}

* Remove duplicated reports on multiple dates
gen marketvalue_usd = marketvalue / lcu
bysort MasterPortfolioId date_m: egen n_dates = nvals(date)
bysort MasterPortfolioId date date_m: egen nav = total(marketvalue_usd)
bysort MasterPortfolioId date_m: egen long max_nav = max(nav)
gen nav_as_share_of_max = nav / max_nav
bysort MasterPortfolioId date_m: egen max_date = max(date)
gen keeper = 0
replace keeper = 1 if n_dates == 1
replace keeper = 1 if date == max_date & n_dates > 1 & nav_as_share_of_max >= .9
bysort MasterPortfolioId date_m: egen max_keeper = max(keeper)
replace keeper = 1 if max_keeper == 0 & n_dates > 1
drop max_keeper
bysort MasterPortfolioId date_m: egen max_keeper = max(keeper)
assert max_keeper == 1
drop if keeper == 0
drop keeper marketvalue_usd n_dates nav max_nav max_date max_keeper

* Compress and save the output; we store monthly, quarterly, and yearly versions
compress
save "$output/HoldingDetail/HD_`year'_m.dta", replace
gen month = month(date)
keep if month==3 | month==6 | month==9 | month==12
gen quarter = quarter(date)
gen year = year(date)
gen date_q = yq(year,quarter)
format date_q %tq
drop month quarter year
compress
save "$output/HoldingDetail/HD_`year'_q.dta", replace
gen month = month(date)
keep if month==12
gen date_y = year(date)
format date_y %ty
drop month
compress
save "$output/HoldingDetail/HD_`year'_y.dta", replace

log close
