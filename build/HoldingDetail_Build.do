* --------------------------------------------------------------------------------------------------
* HoldingDetail_Build
*
* This file reads in the HoldingDetail Files, cleans and appends them, and then merges with both the 
* API and FX data. The files, called HoldingDetail_?_* (?=year, *=m,q,y), are of mananageable sizes 
* and can be used in analyses.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_HoldingDetail_Build_Array`1'", replace

* Read and consolidate the holdings data
local dropvars_organdappend "portfolio_ordinal portfoliosummary_ordinal holding_ordinal previousportfoliodate _id country currency rule144aeligible altmintaxeligible"
local year = `1' + $firstyear + - 1
foreach filetype in "NonUS" "US" {
	foreach fundtype in "FO" "FM" "FE" {
		foreach atype in "Active" "Inactive" {
			foreach mtype in "" "_NonMonthEnd" {
				foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {
					capture fs "$dir_stataupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/HoldingDetailX_*.dta"
					if _rc==0 {
						foreach file in `r(files)' {
							append using "$dir_stataupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'"
						}
						cap drop `dropvars_organdappend'
						cap rename (_masterportfolioid _currencyid) (MasterPortfolioId CurrencyId)
						gen region_mstar="Rest" if "`filetype'"=="NonUS"
						replace region_mstar="US" if "`filetype'"=="US"	
						gen fundtype="`fundtype'"
						gen status_mstar="`atype'"	
						save "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta", replace emptyok
					}
					clear
				}
			}
		}
	}
}

foreach filetype in "NonUS" "US" {
	foreach fundtype in "FO" "FM" "FE" {
		foreach atype in "Active" "Inactive" {
			foreach mtype in "" "_NonMonthEnd" {
				di "Appending Year `year' for `filetype'"
				foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {	
					capture append using "$temp/HoldingDetail/`filetype'_`fundtype'_`atype'`mtype'_`year'_`month'.dta"
				}
			}
		}
	}
	capture drop _merge
	capture missings dropvars, force
	count
	if `r(N)'>0 {
		
		merge m:1 MasterPortfolioId using $output/morningstar_api_data/API_for_merging_uniqueonly.dta
		drop if _merge==2
		rename fundtype fundtype_mstar

		replace BroadCategory="Money Market" if fundtype=="FM" & _merge==1
		replace DomicileCountryId="USA" if region_mstar=="US" & _merge==1
		drop if DomicileCountryId==""
		drop _merge
		drop status
		replace region="US" if region_mstar=="US"
		replace region="Rest" if region_mstar=="Rest"
		destring _storageid, replace
		rename _detailholdingtypeid typecode
		merge m:1 typecode using "$output/morningstar_api_data/Categories_Asset_Class.dta", keep(1 3) nogen
		sort MasterPortfolioId date _storageid
		gen month = month(date)
		gen date_m = mofd(date)
		format date_m %tm
		drop month
		rename CurrencyId iso_currency_code
		rename country_id iso_country_code
		merge m:1 iso_currency_code date_m using "$output/ER_data/WMRExchRates_spot_m", keep(1 3) gen(merge_w_er_data) keepusing(lcu_per_usd_eop)
		replace lcu_per_usd_eop=1 if iso_currency_code=="USD"
		replace merge_w_er_data=3 if iso_currency_code=="USD"
		drop merge_w_er_data
		sort MasterPortfolioId date_m _storageid
		cap gen coupon=""
		cap gen maturitydate=.
		capture destring _storageid, force replace
		duplicates drop MasterPortfolioId date_m iso_currency_code date typecode externalname iso_country_code cusip currency_id securityname weighting numberofshare marketvalue costbasis region, force
		save $output/HoldingDetail/`filetype'_`year'_m_step09, replace
		clear
	}
}

* Now resolve duplicates across regions
clear
foreach filetype in "NonUS" "US" {
	append using $output/HoldingDetail/`filetype'_`year'_m_step09
}
gen _filetype = "US"
replace _filetype = "NonUS" if region != "US" 
duplicates drop MasterPortfolioId date_m iso_currency_code date typecode externalname iso_country_code cusip currency_id securityname weighting numberofshare marketvalue costbasis, force
foreach filetype in "NonUS" "US" {
	preserve
	keep if "`filetype'" == _filetype 
	save $output/HoldingDetail/`filetype'_`year'_m_step1, replace
	restore
}
clear

log close
