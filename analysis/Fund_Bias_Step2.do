*==============================================================================
* Home-currency bias: fund-by-fund analysis reported in Section 3.3 of Paper
* Step 2: Consolidating statistics; graphs
*==============================================================================
cap log close
log using "$logs/${whoami}_Fund_Bias_Step2", replace
local plot_quarter = "2017m12"
local standard_errors "vce(cluster MasterPortfolioId)"
local investing_countries = `" "USA" "EMU" "GBR" "CAN" "CHE" "AUS" "SWE" "DNK" "NOR" "NZL" "'

* ------------------------------------------------------------------------------
* Data preparation
* ------------------------------------------------------------------------------

* Append all stats from Step1
clear
gen year = .
forvalues yr = $firstyear(1)$lastyear {
	di "Appending `yr'"
	append using $resultstemp/Fund_Stats_`yr'
	replace year = `yr' if missing(year)
}
drop if date_m < tm(1986m1)
save $resultstemp/Fund_Stats_All, replace

* Get MPID -> static info map; we use the first row if there are multiple funds per MPID, since this info is very homogeneous
use $output/morningstar_api_data/Mapping_Plus_Static.dta, clear
collapse (firstnm) FundId InvestmentIdName CurrencyId Currency DomicileId FundName FundLegalName ProviderCompanyName IndexFund ConvertedFundAUM FirmName ProspectusNetExpenseRatio ProspectusOperatingExpenseRatio AnnualReportGrossExpenseRatio InceptionDate, by(MasterPortfolioId)
destring MasterPortfolioId, force replace
drop if missing(MasterPortfolioId)
save $temp/mpid_static_details, replace

* Merge with static fund details
use $resultstemp/Fund_Stats_All, clear
mmerge MasterPortfolioId using $temp/mpid_static_details, unmatched(m)
save $resultstemp/Fund_Stats_Merged, replace

* Get latest currency concordances
use $output/concordances/country_currencies, clear
keep if missing(end_date)
save $temp/fund_bias/country_currencies_latest, replace

* Add home currency info
use $resultstemp/Fund_Stats_Merged, clear
mmerge DomicileCountryId using $output/concordances/country_currencies, unmatched(m) umatch(iso_country_code) ukeep(iso_currency_code start_date end_date)
gen start_month = mofd(start_date)
gen end_month = mofd(end_date)
drop start_date end_date
replace start_month = tm(1970m1) if missing(start_month)
replace end_month = tm(2100m1) if missing(end_month)
keep if date_m >= start_month & date_m < end_month
drop start_month end_month
save $resultstemp/Fund_Stats_Merged_2, replace

* Compute fund observed AUM
use $resultstemp/Fund_Stats_Merged, clear
bysort MasterPortfolioId date_m: egen fund_observed_aum = total(marketvalue_usd)
keep MasterPortfolioId date_m fund_observed_aum
duplicates drop
save $temp/fund_bias/fund_observed_aum, replace

* Compute external AUM
foreach asset_type in "B" "BC" {
	use $resultstemp/Fund_Stats_Merged, clear
	if "`asset_type'"=="B" {
		keep if mns_class=="B" & !missing(currency_id)
	}
	if "`asset_type'"=="BC" {
		keep if mns_class=="B" & !missing(currency_id) & !inlist(mns_subclass,"S","A","SF","SV","LS")
	}
	gen external_mval = marketvalue_usd if DomicileCountryId != iso_country_code
	bysort MasterPortfolioId date_m: egen fund_external_aum_`asset_type' = total(external_mval)
	keep MasterPortfolioId date_m fund_external_aum_`asset_type'
	duplicates drop
	save $temp/fund_bias/fund_external_aum_`asset_type', replace
}

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

* Remove box shading
cap program drop remove_box_shading
program remove_box_shading
	gr_edit .plotregion1.subtitle[3].style.editstyle drawbox(no) editcopy
end

* ------------------------------------------------------------------------------
* Plots
* ------------------------------------------------------------------------------

* Cross-sectional graphs
foreach asset_type in "B" "BC" {

	* Load the sample
	use $resultstemp/Fund_Stats_Merged if date_m == tm(`plot_quarter'), clear
	if "`asset_type'"=="B" {
		keep if mns_class=="B" & !missing(currency_id)
		local asset_title = "all bonds"
		local rankplot_lab = "All Bonds"
	}
	if "`asset_type'"=="BC" {
		keep if mns_class=="B" & !missing(currency_id) & !inlist(mns_subclass,"S","A","SF","SV","LS")
		local asset_title = "corporate bonds"
		local rankplot_lab = "Corporate Bonds"
	}
	mmerge DomicileCountryId using $temp/fund_bias/country_currencies_latest, unmatched(m) umatch(iso_country_code) ukeep(iso_currency_code)
	order MasterPortfolioId iso_country_code iso_currency_code currency_id DomicileCountryId 

	* EMU-adjust and compute shares
	foreach var in "DomicileCountryId" "iso_country_code" {
		replace `var' = "EMU" if inlist(`var', $eu1)
		replace `var' = "EMU" if inlist(`var', $eu2)
		replace `var' = "EMU" if inlist(`var', $eu3)
	}	
	compute_foreign_shares

	* Collapse to single MPID
	keep MasterPortfolioId mpid_foreign_* mpid_mval_* DomicileCountryId iso_currency_code FundId InvestmentIdName CurrencyId Currency DomicileId FundName FundLegalName ProviderCompanyName IndexFund ConvertedFundAUM FirmName ProspectusNetExpenseRatio ProspectusOperatingExpenseRatio AnnualReportGrossExpenseRatio
	duplicates drop
	destring ConvertedFundAUM, force replace
	destring ProspectusNetExpenseRatio, force replace

	* Process domiciles; merge AUM data
	keep if inlist(DomicileCountryId, "USA", "EMU", "GBR", "CAN", "CHE") | inlist(DomicileCountryId, "AUS", "SWE", "DNK", "NOR", "NZL")
	gen date_m = tm(`plot_quarter')
	mmerge MasterPortfolioId date_m using $temp/fund_bias/fund_external_aum_`asset_type', unmatched(m)

	* Drop missing shares
	drop if missing(mpid_foreign_HC_share) | missing(mpid_foreign_HC_USD_share)


	* ------------------------------------------------------------------------------
	* Unbinned rank plot; pooled; version with different markets and loess line
	* ------------------------------------------------------------------------------
	preserve
	local xmax = 300
	gsort -  fund_external_aum_`asset_type'
	gen aum_rank = _n
	keep if aum_rank <= `xmax'
	gsort aum_rank
	local bar_red = "165 15 21"
	local bar_grey = "99 99 99"

	lowess mpid_foreign_HC_USD_share aum_rank, graphregion(color(white)) ytitle("HC & USD Share") xtitle("Fund's Rank by Outward Investment in `rankplot_lab'") mc(red%90) ms(Oh) bwidth(0.8) yscale(range(0 1)) ylab(0(0.2)1, grid) title("") note("") saving($resultstemp/graphs/fund_bias_unpooled_rankplot_lowess_HC_USD_`asset_type', replace) lineopts(lcolor(black) lwidth(1)) aspect(0.6)
	graph export $resultstemp/graphs/fund_bias_pooled_rankplot_unbinned_lowess_`asset_type'_HC_USD.eps, as(eps) replace

	lowess mpid_foreign_HC_share aum_rank, graphregion(color(white)) lineopts(lcolor(black) lwidth(1)) xtitle("Fund's Rank by Outward Investment in `rankplot_lab'") ytitle("HC Share") yscale(range(0 1)) ylab(0(0.2)1, grid) mc("`bar_grey'") ms(Oh) title("") note("") saving($resultstemp/graphs/fund_bias_unpooled_rankplot_lowess_HC_`asset_type', replace) aspect(0.6)
	graph export $resultstemp/graphs/fund_bias_pooled_rankplot_unbinned_lowess_`asset_type'_HC.eps, as(eps) replace

	restore

}

log close
