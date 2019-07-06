*==============================================================================
* Home-currency bias: fund-by-fund analysis reported in Section 3.3 of paper
* Step 3: Regression
*==============================================================================
cap log close
log using "$logs/${whoami}_Fund_Bias_Step3_`1'", replace
local investing_countries = `" "USA" "EMU" "GBR" "CAN" "CHE" "AUS" "SWE" "DNK" "NOR" "NZL" "'
local reg_month = "2017m12"
local star_type = ""

* Determine sample
if `1' == 1 {
	local asset_type = "BC"
}
if `1' == 2 {
	local asset_type = "B"
}
di "Running regressions on sample `asset_type'"

* Program for investment shares computation
cap program drop compute_foreign_shares
program compute_foreign_shares
	
	gen foreign_invest_mval = marketvalue_usd if DomicileCountryId != iso_country_code
	gen foreign_invest_HC_mval = marketvalue_usd if DomicileCountryId != iso_country_code & currency_id == iso_currency_code
	gen foreign_invest_HC_USD_mval = marketvalue_usd if DomicileCountryId != iso_country_code & inlist(currency_id, iso_currency_code, "USD")
	gen foreign_invest_USD_mval = marketvalue_usd if DomicileCountryId != iso_country_code & inlist(currency_id, "USD")

	bysort date_m MasterPortfolioId: egen mpid_mval_tot = total(marketvalue_usd)
	bysort date_m MasterPortfolioId: egen mpid_mval_foreign = total(foreign_invest_mval)
	bysort date_m MasterPortfolioId: egen mpid_mval_foreign_HC = total(foreign_invest_HC_mval)
	bysort date_m MasterPortfolioId: egen mpid_mval_foreign_HC_USD = total(foreign_invest_HC_USD_mval)
	bysort date_m MasterPortfolioId: egen mpid_mval_foreign_USD = total(foreign_invest_USD_mval)

	gen mpid_foreign_HC_share = mpid_mval_foreign_HC / mpid_mval_foreign
	gen mpid_foreign_share = mpid_mval_foreign / mpid_mval_tot
	gen mpid_foreign_HC_USD_share = mpid_mval_foreign_HC_USD / mpid_mval_foreign
	gen mpid_foreign_USD_share = mpid_mval_foreign_USD / mpid_mval_foreign

end

* ------------------------------------------------------------------------------
* Pooled regression specification, without destination details
* ------------------------------------------------------------------------------

* Pooled regression prep
use MasterPortfolioId date_m DomicileCountryId iso_currency_code currency_id marketvalue_usd iso_country_code mns_class mns_subclass using $resultstemp/Fund_Stats_Merged_2, clear
if "`asset_type'"=="B" {
	keep if mns_class=="B" & !missing(currency_id)
	local asset_title = "all bonds"
}
if "`asset_type'"=="BC" {
	keep if mns_class=="B" & !missing(currency_id) & !inlist(mns_subclass,"S","A","SF","SV","LS")
	local asset_title = "corporate bonds"
}

* Process domiciles and destinations
foreach var in "DomicileCountryId" "iso_country_code" {
	replace `var' = "EMU" if inlist(`var', $eu1)
	replace `var' = "EMU" if inlist(`var', $eu2)
	replace `var' = "EMU" if inlist(`var', $eu3)
}

* Compute shares
compute_foreign_shares
keep MasterPortfolioId DomicileCountryId date_m iso_currency_code mpid_foreign_*
duplicates drop 
mmerge MasterPortfolioId using $temp/mpid_static_details, unmatched(m)
mmerge MasterPortfolioId date_m using $temp/fund_bias/fund_observed_aum, unmatched(m)
destring ConvertedFundAUM, force replace
destring ProspectusNetExpenseRatio, force replace
save $temp/fund_bias/reg_prep_`asset_type', replace

* Sample subset
use $temp/fund_bias/reg_prep_`asset_type', replace
gen year = year(dofm(date_m))
keep if year >= 2004 & year <= 2017

* Portfolio AUM rank
sort date_m
gsort date_m -fund_observed_aum
by date_m: gen aum_rank = _n
gen log_aum_rank = log10(aum_rank)

* Log AUM
winsor2 fund_observed_aum, cuts(1 99)
gen log_aum = log(fund_observed_aum_w * 1e9)

* Treat poorly behaved shares (due to short positions) as missing
foreach var in "mpid_foreign_HC_USD_share" "mpid_foreign_HC_share" "mpid_foreign_share" {
	replace `var' = . if `var' < 0 | `var' > 1
} 

* Subset domiciles
encode DomicileCountryId, gen(domicile_indicator)
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE") | inlist(DomicileCountryId, "AUS", "SWE", "DNK", "NOR", "NZL")

* Regressions; iterate over dependent variables
foreach var in "mpid_foreign_HC_USD_share" "mpid_foreign_HC_share" {

	* Labels
	if "`var'" == "mpid_foreign_HC_USD_share" {
		local reglab = "HC_USD"
	}
	if "`var'" == "mpid_foreign_HC_share" {
		local reglab = "HC"
	}

	* Simple pooled OLS
	reg `var' mpid_foreign_share log_aum if date_m == tm(`reg_month'), vce(cluster domicile_indicator)
	outreg2 using  $regressions/fund_bias/`asset_type'_noDest_`reglab'_OLS.xls, `star_type' dec(3) excel replace e(N N_clust r2)

	* Fixed effects pooled regression
	areg `var' mpid_foreign_share log_aum if date_m == tm(`reg_month'), absorb(domicile_indicator) vce(cluster domicile_indicator)
	outreg2 using  $regressions/fund_bias/`asset_type'_noDest_`reglab'_FE.xls, `star_type' dec(3) excel replace e(N N_clust r2)

	* Fixed effects regressions by domicile
	foreach domicile in `investing_countries' {
		di "Regression for `domicile'"
		areg `var' mpid_foreign_share log_aum if DomicileCountryId == "`domicile'" & date_m == tm(`reg_month'), absorb(domicile_indicator) robust
		outreg2 using  $regressions/fund_bias/`asset_type'_noDest_byDom_`reglab'_FE_`domicile'.xls, `star_type' dec(3) excel replace e(N N_clust r2)
	}

}

* ------------------------------------------------------------------------------
* Multilateral regression specification with destination fixed effects
* ------------------------------------------------------------------------------

* Regression prep
use MasterPortfolioId date_m DomicileCountryId iso_currency_code currency_id marketvalue_usd iso_country_code mns_class mns_subclass using $resultstemp/Fund_Stats_Merged_2, clear
if "`asset_type'"=="B" {
	keep if mns_class=="B" & !missing(currency_id)
	local asset_title = "all bonds"
}
if "`asset_type'"=="BC" {
	keep if mns_class=="B" & !missing(currency_id) & !inlist(mns_subclass,"S","A","SF","SV","LS")
	local asset_title = "corporate bonds"
}

* Process domiciles and destinations
foreach var in "DomicileCountryId" "iso_country_code" {
	replace `var' = "EMU" if inlist(`var', $eu1)
	replace `var' = "EMU" if inlist(`var', $eu2)
	replace `var' = "EMU" if inlist(`var', $eu3)
}

* Compute relevant investment shares by destination
gen foreign_invest_mval = marketvalue_usd if DomicileCountryId != iso_country_code
bysort date_m MasterPortfolioId: egen mpid_mval_tot = total(marketvalue_usd)
bysort date_m MasterPortfolioId: egen mpid_mval_foreign = total(foreign_invest_mval)
gen mpid_foreign_share = mpid_mval_foreign / mpid_mval_tot
drop mpid_mval_tot mpid_mval_foreign

keep if DomicileCountryId != iso_country_code
gen foreign_invest_HC_mval = marketvalue_usd if DomicileCountryId != iso_country_code & currency_id == iso_currency_code
gen foreign_invest_HC_USD_mval = marketvalue_usd if DomicileCountryId != iso_country_code & inlist(currency_id, iso_currency_code, "USD")
gen foreign_invest_USD_mval = marketvalue_usd if DomicileCountryId != iso_country_code & inlist(currency_id, "USD")

bysort date_m MasterPortfolioId iso_country_code: egen mpid_mval_tot_by_dest = total(marketvalue_usd)
bysort date_m MasterPortfolioId iso_country_code: egen mpid_mval_foreign_HC = total(foreign_invest_HC_mval)
bysort date_m MasterPortfolioId iso_country_code: egen mpid_mval_foreign_HC_USD = total(foreign_invest_HC_USD_mval)
bysort date_m MasterPortfolioId iso_country_code: egen mpid_mval_foreign_USD = total(foreign_invest_USD_mval)

gen mpid_foreign_HC_share = mpid_mval_foreign_HC / mpid_mval_tot_by_dest
gen mpid_foreign_HC_USD_share = mpid_mval_foreign_HC_USD / mpid_mval_tot_by_dest
gen mpid_foreign_USD_share = mpid_mval_foreign_USD / mpid_mval_tot_by_dest

* Keep unique records
keep MasterPortfolioId DomicileCountryId date_m iso_country_code iso_currency_code mpid_foreign_*
duplicates drop 

* Merge fund details
mmerge MasterPortfolioId using $temp/mpid_static_details, unmatched(m)
mmerge MasterPortfolioId date_m using $temp/fund_bias/fund_observed_aum, unmatched(m)
destring ConvertedFundAUM, force replace
destring ProspectusNetExpenseRatio, force replace
save $temp/fund_bias/reg_prep_by_dest_`asset_type', replace

* Sample subset
use $temp/fund_bias/reg_prep_by_dest_`asset_type', replace
gen year = year(dofm(date_m))
keep if year >= 2004 & year <= 2017

* Portfolio AUM rank
sort date_m
gsort date_m -fund_observed_aum
by date_m: gen aum_rank = _n
gen log_aum_rank = log10(aum_rank)

* Log AUM
winsor2 fund_observed_aum, cuts(1 99)
gen log_aum = log(fund_observed_aum_w * 1e9)

* Treat poorly behaved shares (due to short positions) as missing
foreach var in "mpid_foreign_HC_USD_share" "mpid_foreign_HC_share" "mpid_foreign_share" {
	replace `var' = . if `var' < 0 | `var' > 1
} 

* Process domiciles
encode DomicileCountryId, gen(domicile_indicator)
keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE") | inlist(DomicileCountryId, "AUS", "SWE", "DNK", "NOR", "NZL")
rename iso_country_code destination
encode destination, gen(destination_indicator)

* Merge with fund family info
mmerge MasterPortfolioId using $temp/fund_bias/fund_family_domiciles, unmatched(m)

* Regressions; iterate over dependent variables
foreach var in "mpid_foreign_HC_USD_share" "mpid_foreign_HC_share" {

	* Labels
	if "`var'" == "mpid_foreign_HC_USD_share" {
		local reglab = "HC_USD"
	}
	if "`var'" == "mpid_foreign_HC_share" {
		local reglab = "HC"
	}

	* Fixed effects pooled regression
	areg `var' mpid_foreign_share log_aum i.domicile_indicator if date_m == tm(`reg_month'), absorb(destination_indicator) vce(cluster MasterPortfolioId)
	outreg2 using  $regressions/fund_bias/`asset_type'_withDest_`reglab'_FE.xls, `star_type' dec(3) excel replace e(N N_clust r2)

	* Fixed effects regressions by domicile
	foreach domicile in `investing_countries' {
		di "Regression for `domicile'"
		areg `var' mpid_foreign_share log_aum i.domicile_indicator if DomicileCountryId == "`domicile'" & date_m == tm(`reg_month'), absorb(destination_indicator) vce(cluster MasterPortfolioId)
		outreg2 using  $regressions/fund_bias/`asset_type'_withDest_byDom_`reglab'_FE_`domicile'.xls, `star_type' dec(3) excel replace e(N N_clust r2)
	}

	* Specification with family-presence indicator
	cap drop family_in_destination
	gen family_in_destination = strpos(family_domiciles, destination)
	replace family_in_destination = 1 if family_in_destination > 0
	replace family_in_destination = . if missing(family_domiciles)
	foreach dom in "ITA" "DEU" "FRA" "ESP" "GRC" "NLD" "AUT" "BEL" "FIN" "PRT" "CYP" "EST" "LAT" "LTU" "SVK" "SVN" "MLT" {
		replace family_in_destination = 1 if strpos(family_domiciles, "`dom'") > 0 & DomicileCountryId == "EMU" 
	}
	areg `var' mpid_foreign_share log_aum family_in_destination i.domicile_indicator if date_m == tm(`reg_month'), absorb(destination_indicator) vce(cluster MasterPortfolioId)
	outreg2 using  $regressions/fund_bias/`asset_type'_withDest_`reglab'_FE_Family.xls, `star_type' dec(3) excel replace e(N N_clust r2)

	* Specification with family-presence indicator, by domicile
	foreach domicile in `investing_countries' {
		di "Regression for `domicile'"
		areg `var' mpid_foreign_share log_aum family_in_destination i.domicile_indicator if DomicileCountryId == "`domicile'" & date_m == tm(`reg_month'), absorb(destination_indicator) vce(cluster MasterPortfolioId)
		outreg2 using  $regressions/fund_bias/`asset_type'_withDest_byDom_`reglab'_FE_`domicile'_Family.xls, `star_type' dec(3) excel replace e(N N_clust r2)
	}

}

log close
