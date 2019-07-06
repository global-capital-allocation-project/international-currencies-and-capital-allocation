* --------------------------------------------------------------------------------------------------
* Macro_Build, Step 2
*
* This Do-file cleans and builds exchange rate data from Thomson Reuters (available from Datastream) 
* and other macro data from the IMF's IFS (downloaded as a flat file). It concords these files with 
* ISO country and currency codes for easy interaction with the Morningstar data. Output of this 
* build includes separate files called Macro_Vars_* and WMRExchRates_*, where * captures the 
* frequency as M=monthly, Q=quarterly, and Y=yearly. 
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Macro_Build_Step2", replace

* --------------------------------------------------------------------------------------------------
* Step 2, Part 1: Read in and clean concordance and metadata to build relational concordances
* --------------------------------------------------------------------------------------------------
import excel "$raw/macro/Concordances/Country_and_Currency_Codes.xlsx", sheet("Data") firstrow clear
keep country_name iso_country_code
duplicates drop
save $temp/country_names, replace
save $output/concordances/country_names, replace

import excel "$raw/macro/Concordances/Country_and_Currency_Codes.xlsx", sheet("Data") firstrow clear
keep country_name iso_country_code iso_currency_code start_date end_date
drop if missing(iso_country_code) | missing(iso_currency_code)
duplicates drop
save $temp/country_currencies, replace
save $output/concordances/country_currencies, replace

* Builds 2-variable concordance mapping ISO and WMR Currency Codes
import excel using "$raw/macro/Concordances/Country_and_Currency_Codes.xlsx", sheet("Data") clear first
keep iso_currency_code WMR_currency
drop if missing(WMR_currency)
duplicates drop
save "$temp/ER_data/WMR_to_ISO_Concord", replace
save "$output/concordances/WMR_to_ISO_Concord", replace

* Builds 3-variable concordance mapping 3-digit IMF code, 3-letter ISO code, and full country name
import excel using "$raw/macro/Concordances/IMFCountryCodes.xlsx", sheet("Data") first clear
rename (imf_code iso_code) (imf_country_code iso_country_code)
drop if missing(country_name)
destring imf_country_code, replace
sort iso_country_code
save "$temp/macro_data/IMF_Country_Codes_and_ISO_Country_Codes", replace
save "$output/concordances/IMF_Country_Codes_and_ISO_Country_Codes", replace

* Builds a 2-variable concordance mapping 3-letter ISO currency code to full currency name
import excel using "$raw/macro/Concordances/Country_and_Currency_Codes.xlsx", sheet("Data") clear first
keep iso_currency_code currency_name
drop if missing(iso_currency)
duplicates drop
sort iso_currency_code
save "$temp/macro_data/ISO_Currency_Codes_and_Currency_Names", replace
save "$output/concordances/ISO_Currency_Codes_and_Currency_Names", replace

* Builds 4-variable country-level dataset mapping 3-letter ISO country code to current and old 3-letter ISO currency code and start dates
import excel using "$raw/macro/Concordances/Country_and_Currency_Codes.xlsx", sheet("Data") clear first
drop if !missing(end_date)
format start_date %td
keep iso_country_code iso_currency_code start_date 
save "$temp/macro_data/ISO_Country_Codes_and_Currency_Codes_current", replace
import excel using "$raw/macro/Concordances/Country_and_Currency_Codes.xlsx", sheet("Data") clear first
keep if !missing(end_date)
rename iso_currency_code iso_currency_code_old
keep iso_country_code iso_currency_code_old
merge 1:1 iso_country_code using "$temp/macro_data/ISO_Country_Codes_and_Currency_Codes_current", keep(1 2 3) nogen
drop if missing(iso_currency_code)
save "$temp/macro_data/ISO_Country_Codes_and_Currency_Codes", replace
rm "$temp/macro_data/ISO_Country_Codes_and_Currency_Codes_current.dta"
save "$output/concordances/ISO_Country_Codes_and_Currency_Codes", replace

* Upload Thompson Reuters metadata
import excel using "$raw/macro/Datastream/WMRExRatetoUSDList.xlsx", sheet("Data") clear first
missings dropvars, force
* Replace all the "$" signs with "S" because Stata will not allow variable names with "$"
gen fx_var = subinstr(Symbol,"$","S",.)
label variable fx_var "Datastream Variable Symbol"
* Generate "exch_rate_type" based on Symbol and FullName
gen type = substr(fx_var,-1,1)
gen ndf_ind = (strpos(FullName,"NDF")>0)
gen fwd_ind = (strpos(FullName,"Forward")>0)
* Search for forwards that didn't come up in FullName
gen F_ind = (ndf_ind==0 & fwd_ind==0 & type=="F")
gen exch_rate_type = "NDF" if ndf_ind==1
replace exch_rate_type = "FORWARD" if fwd_ind==1 | F_ind==1
replace exch_rate_type = "SPOT" if missing(exch_rate_type)
label variable fx_var "Exchange Rate Type"
drop Name Hist Category ToCurrency Source StartDate *ind Symbol type
rename FromCurrency WMR_currency
merge m:1 WMR_currency using "$temp/ER_data/WMR_to_ISO_Concord", keep(3) nogen
drop WMR_currency
save "$temp/ER_data/WMR_ExRate_to_USD_List", replace
rm "$temp/ER_data/WMR_to_ISO_Concord.dta"

* --------------------------------------------------------------------------------------------------
* Step 2, Part 2: Read in Thomson Reuters FX data, clean, merge metadata, and save output files
* --------------------------------------------------------------------------------------------------
clear 
local WMR_sheets "A B C D E F G H I J K L M N O P Q R S1 S2 T U V Y Z"	
foreach sheet of local WMR_sheets {
	append using "$temp/ER_data/WMRExchRates`sheet'.dta"
}
save "$temp/ER_data/WMRExchRates", replace

use "$temp/ER_data/WMRExchRates", clear
merge m:1 fx_var using "$temp/ER_data/WMR_ExRate_to_USD_List", keep(3) nogen
sort exch_rate_type iso_currency_code fx_var date
save "$temp/ER_data/WMRExchRates_allvars", replace

use "$temp/ER_data/WMRExchRates_allvars", clear
keep if exch_rate_type == "FORWARD"
drop if missing(lcu_per_usd)
save "$output/ER_data/WMRExchRates_fwd", replace

use "$temp/ER_data/WMRExchRates_allvars", clear
keep if exch_rate_type=="NDF"
drop if missing(lcu_per_usd)
save "$output/ER_data/WMRExchRates_ndf", replace

use "$temp/ER_data/WMRExchRates_allvars", clear
keep if exch_rate_type=="SPOT"
drop if missing(lcu_per_usd)

* When Multiple Spot Price Sources Exist, Choose Preferred One
drop if iso_currency=="BWP" & FullName!="Botswana Pula to United States Dollar (WMR)"
drop if iso_currency=="CNY" & FullName!="Chinese Yuan to United States Dollar (WMR)"
drop if iso_currency=="CYP" & fx_var!="CYPRUSS"
drop if iso_currency=="EUR" & FullName!="Euro to United States Dollar (WMR and DS)"
drop if iso_currency=="GBP" & fx_var!="UKDOLLR"
drop if iso_currency=="GHS" & FullName!="Ghanaian Cedi to United States Dollar (WMR)"
drop if iso_currency=="INR" & FullName!="Indian Rupee to United States Dollar (WMR)"
drop if iso_currency=="ISK" & FullName!="Icelandic Krona to United States Dollar (WMR)"
drop if iso_currency=="KRW" & FullName!="South Korean Won to United States Dollar (WMR)"
drop if iso_currency=="NZD" & FullName!="New Zealand Dollar to United States Dollar (WMR and DS)"
drop if iso_currency=="THB" & FullName!="Thai Baht to United States Dollar (WMR)"
drop if iso_currency=="TRY" & FullName!="New Turkish Lira to United States Dollar (WMR)"
drop if iso_currency=="VEF" & FullName!="Venezuelan Bolivar to United States Dollar (WMR)"
drop if iso_currency=="ZAR" & FullName!="South Africa Rand to United States Dollar (WMR)"
drop if iso_currency=="ZWL" & FullName!="Zimbabwe$ (Notional) to United States Dollar (WMR)"
drop if iso_currency=="USD"
save "$output/ER_data/WMRExchRates_spot", replace
rm "$temp/ER_data/WMR_ExRate_to_USD_List.dta"
rm "$temp/ER_data/WMRExchRates.dta"
rm "$temp/ER_data/WMRExchRates_allvars.dta"

* Now iterate through and try to save exchange rate files of each frequency
foreach freq in "m" "q" "y" {
	preserve
	di "`freq'"
	gen date_`freq' = `freq'ofd(date)
	format date_`freq' %t`freq'
	collapse (mean) lcu_per_usd_avg=lcu_per_usd (lastnm) lcu_per_usd_eop=lcu_per_usd, by(fx_var date_`freq' iso_currency)
	gen frequency=strupper("`freq'")
	save $output/ER_data/WMRExchRates_spot_`freq', replace
	restore
}

* --------------------------------------------------------------------------------------------------
* Step 3: Read in and clean IFS flatfile data, merge with exchange rates
* --------------------------------------------------------------------------------------------------

* Builds indicator-level dataset of selected macro variables from IFS flatfile with desired labels
import excel using "$raw/macro/IFS/Selected_Macro_Vars_from_IFS_Flatfile.xlsx", sheet("Data") first clear
duplicates drop
sort indicatorname
save "$temp/macro_data/Selected_Macro_Vars", replace
quietly count
local total_variables = r(N)

* Store all variable names and associated labels from "label" variable in dataset to relabel the wide dataset next
foreach num of numlist 1/`total_variables' {
	local varname_`num' = newvarname[`num']
	local label_varname_`num' = label[`num']
}

* Read-in IFS Flatfile, Keep only Selected Variables, Merge in ISO and Currency Codes, Format Time Periods
insheet using "$raw/macro/IFS/IFS_Flatfile.csv", names clear
drop if missing(value)
drop v8 status
merge m:1 indicatorname using "$temp/macro_data/Selected_Macro_Vars", keep(3) nogen
rm "$temp/macro_data/Selected_Macro_Vars.dta"
rename (countrycode countryname) (imf_country_code country_name)
merge m:1 imf_country_code using "$temp/macro_data/IMF_Country_Codes_and_ISO_Country_Codes", keep(3) nogen
merge m:1 iso_country_code using "$temp/macro_data/ISO_Country_Codes_and_Currency_Codes", keep(3) nogen
rm "$temp/macro_data/IMF_Country_Codes_and_ISO_Country_Codes.dta"
rm "$temp/macro_data/ISO_Country_Codes_and_Currency_Codes.dta"
rm "$temp/macro_data/ISO_Currency_Codes_and_Currency_Names.dta"
gen frequency = "Y" if length(timeperiod)==4
replace frequency = "Q" if substr(timeperiod,5,1)=="Q"
replace frequency = "M" if substr(timeperiod,5,1)=="M"
label variable frequency "Time Period Frequency"
gen year = substr(timeperiod,1,4)
destring year, replace
gen date_y = year if frequency=="Y"
format date_y %ty
label variable date_y "Yearly Date"
gen month = substr(timeperiod,6,.) if frequency=="M"
destring month, replace
gen date_m = ym(year,month)
format date_m %tm
label variable date_m "Monthly Date"
gen quarter = substr(timeperiod,6,.) if frequency=="Q"
destring quarter, replace
gen date_q = yq(year,quarter)
format date_q %tq
label variable date_q "Quarterly Date"
replace iso_currency_code = iso_currency_code_old if (date_m<ym(year(start_date),month(start_date)) | date_q<yq(year(start_date),quarter(start_date)) | date_y<year(start_date) ) & !missing(iso_currency_code_old)
rename newvarname var
keep var iso_country_code iso_currency_code value frequency date*
reshape wide value, i(iso_country_code iso_currency_code frequency date*) j(var) string
rename value* *
foreach num of numlist 1/`total_variables' {
	capture label variable `varname_`num'' "`label_varname_`num''"
}

* Note: Below loop replaces variables with version that is "SA" or "ANN", etc., when not otherwise available. 
* We never mix variables -- i.e. it's one def or another. "_sa_ann" must precede the others.
foreach suffix in "_sa_ann" "_ann" "_sa" {
	foreach long_var of varlist *`suffix' {
		local base_var = subinstr("`long_var'","`suffix'","",1)
		gen hasbase_tmp = 0
		cap replace hasbase_tmp = 1 if !missing(`base_var')
		bys iso_country_code: egen hasbase= max(hasbase_tmp)
		cap replace `base_var' = `long_var' if hasbase!=1
		drop hasbase* `long_var'
	}		
}
foreach suffix in "_usd" "_euros" {
	foreach long_var of varlist *`suffix' {
		local base_var = subinstr("`long_var'","`suffix'","",1)+"_lcu"
		gen hasbase_tmp = 0
		cap replace hasbase_tmp = 1 if !missing(`base_var')
		bys iso_country_code: egen hasbase= max(hasbase_tmp)
		if "`suffix'"=="_usd" {
			cap replace `base_var' = `long_var' if hasbase!=1 | iso_currency_code=="USD"
		}
		if "`suffix'"=="_euros" {
			cap replace `base_var' = `long_var' if hasbase!=1 | iso_currency_code=="EUR"
		}
		drop hasbase* `long_var'
	}		
}
replace eq_ind = eq_ind_eop if missing(eq_ind)
replace nx_lcu = exp_lcu-imp_lcu if missing(nx_lcu)
rename rer_ulc2 rer_ulc
drop rer_ulc1
order iso_country_code iso_currency_code frequency date_m date_q date_y
sort iso_country_code iso_currency_code frequency date_m date_q date_y
save "$output/macro_data/Macro_Vars", replace

foreach freq in "Y" "Q" "M" {
	preserve
	keep if frequency=="`freq'"
	if "`freq'"=="Y" {
		drop date_m date_q frequency
	}
	if "`freq'"=="Q" {
		drop date_m date_y frequency
	}
	if "`freq'"=="M" {
		drop date_q date_y frequency
	}
	local freqlow = strlower("`freq'")
	save "$output/macro_data/Macro_Vars_`freqlow'", replace
	restore
}

clear
log close	
