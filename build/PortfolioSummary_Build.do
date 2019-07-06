* --------------------------------------------------------------------------------------------------
* PortfolioSummary_Build
*
* This file reads in the PortfolioSummary files, cleans and appends them, and then merges with both 
* the API and FX data. The files, called PortfolioSummary_* (*=m,q,y) are of manageable size and can 
* be used in analyses.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_PortfolioSummary_Build", replace

* Append all the PortfolioSummary shards
local dropvars_organdappend "portfolio_ordinal portfoliosummary_ordinal previousportfoliodate numberofholdingshort numberofstockholdingshort numberofbondholdingshort numberofholdinglong numberofstockholdinglong numberofbondholdinglong"
foreach filetype in "NonUS" "US" {
	foreach fundtype in "FO" "FM" "FE" {
		foreach atype in "Active" "Inactive" {
			foreach mtype in "" "_NonMonthEnd" {	
				forvalues year=$firstyear/$lastyear {
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {
						capture fs "$dir_stataupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/PortfolioSummaryX_*.dta"
						if _rc==0 {
							foreach file in `r(files)' {
								append using "$dir_stataupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'"
							}
							cap drop `dropvars_organdappend'
							cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
							save "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta", replace emptyok
						}
						clear
					}
				}
				forvalues year=$firstyear/$lastyear{
					di "Appending Year `year' for `filetype', `activetype'"
					foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {	
						capture append using "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
						capture rm "$temp/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
					}
				}
				capture missings dropvars, force
				gen region_mstar="Rest" if "`filetype'"=="NonUS"
				replace region_mstar="US" if "`filetype'"=="US"	
				gen fundtype_mstar="`fundtype'"
				gen status_mstar="`atype'"
				save "$output/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'.dta", replace emptyok
				clear
			}
		}
	}
}

* Consolidate the filess
clear
foreach filetype in "NonUS" "US" {
	foreach fundtype in "FO" "FM" "FE" {
		foreach atype in "Active" "Inactive" {
			foreach mtype in "" "_NonMonthEnd" {					
				append using "$output/PortfolioSummary/`filetype'_`fundtype'_`atype'`mtype'.dta"
			}
		}
	}
}

* Merge in the fund metadata		
keep *_mstar MasterPortfolioId CurrencyId date totalmarketvalueshort totalmarketvaluelong
drop fundtype_mstar
merge m:1 MasterPortfolioId using "$output/morningstar_api_data/API_for_merging_uniqueonly.dta"
rename fundtype fundtype_mstar
drop if _merge==2
replace BroadCategory="Money Market" if fundtype=="FM" & _merge==1
replace DomicileCountryId="USA" if region_mstar=="US" & _merge==1
drop if DomicileCountryId==""
drop _merge

* Prepare output PortfolioSummary files; merge in exchange rates
drop status* region
rename *_mstar *
sort MasterPortfolioId date
gen date_m = mofd(date)
format date_m %tm
rename CurrencyId iso_currency_code
rename DomicileCountryId iso_country_code
ds MasterPortfolioId date_m, not
collapse (lastnm) `r(varlist)', by(MasterPortfolioId date_m)
merge m:1 iso_currency date_m using "$output/ER_data/WMRExchRates_spot_m", keep(1 3) nogen keepusing(lcu_per_usd_eop)
replace lcu_per_usd_eop=1 if iso_currency_code=="USD"
save $output/PortfolioSummary/PortfolioSummary_m, replace
gen month = month(date)
keep if month==3 | month==6 | month==9 | month==12
gen date_q = qofd(date)
format date_q %tq
drop month
sort date_q MasterPortfolioId
save $output/PortfolioSummary/PortfolioSummary_q, replace
gen month=month(date)
keep if month==12
gen date_y = yofd(date)
format date_y %ty
drop month
sort date_y MasterPortfolioId
save $output/PortfolioSummary/PortfolioSummary_y, replace
clear

log close
