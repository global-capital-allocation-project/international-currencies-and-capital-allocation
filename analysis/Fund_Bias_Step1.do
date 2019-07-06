*==============================================================================
* Home-currency bias: fund-by-fund analysis reported in Section 3.3 of Paper
* Step 1: Preliminary statistics
*==============================================================================
cap log close
log using "$logs/${whoami}_Fund_Bias_Step1_`1'", replace
local yr = `1' + $firstyear - 1

* Construct relevant fund statistics
use $country_bg_name $cusip6_bg_name MasterPortfolioId iso_currency_code BroadCategoryGroup iso_country_code currency_id DomicileCountryId mns_class mns_subclass date_m maturitydate marketvalue date lcu_per_usd_eop cusip6 fundtype_mstar fuzzy using "$mns_data/output/HoldingDetail/HD_`yr'_y.dta", clear
if $fuzzy==1 {
	drop if fuzzy==1
}
drop fuzzy
gen country_bg=$country_bg_name 
gen cusip6_bg=$cusip6_bg_name 
replace mns_class = "B" if iso_country_code=="XSN"
replace mns_subclass = "SV" if iso_country_code=="XSN"
drop cusip6
rename cusip6_bg cusip6
replace iso_country_code=country_bg if !missing(country_bg)
gen marketvalue_usd = marketvalue/lcu_per_usd_eop/10^9
collapse (sum) marketvalue_usd, by(MasterPortfolioId BroadCategoryGroup DomicileCountryId mns_class mns_subclass iso_country_code currency_id date_m)
save $resultstemp/Fund_Stats_`yr', replace

log close
