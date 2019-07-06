* --------------------------------------------------------------------------------------------------
* Macro_Build, Step 1
*
* This do-file cleans and builds exchange rate data from Thomson Reuters (available from Datastream) 
* and other macro data from the IMF's IFS (downloaded as a flat file). It concords these files with 
* ISO country and currency codes for easy interaction with the Morningstar data. Output of this 
* build includes separate files called Macro_Vars_* and WMRExchRates_*, where * captures the 
* frequency as M=monthly, Q=quarterly, and Y=yearly. 
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Macro_Build_Step1_Array`1'", replace

* --------------------------------------------------------------------------------------------------
* Step 1: Read-in raw Thomson Reuters FX data, clean, merge metadata, and save output files
* --------------------------------------------------------------------------------------------------

* Define local of first letter of each currency in raw data. There are no currencies beginning with W or X.
clear
input str6 arrayno
"blank"
end
replace arrayno = "`1'"
egen arraylet = msub(arrayno), f(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25) r(A B C D E F G H I J K L M N O P Q R S1 S2 T U V Y Z) word
local sheet = arraylet[1]
di "`sheet'"

* Data pulled using different terminals which had different output formats, the below code cleans 
* each separately and handles this.
foreach vintage in "USD" "USD_Jun18Update" {
	import excel using "$raw/macro/Datastream/WMReuters_Exchange_Rates_to_`vintage'.xlsm", sheet("`sheet'") clear
	missings dropvars, force
	missings dropobs, force
	drop if inlist(A,"Start","End","Frequency","MNEM","CURRENCY")

	* Label each variable with first row and rename each variable as second row with a stub so we can reshape
	foreach var of varlist _all {
		if "`vintage'" == "USD" {
			local label = `var'[1]
		}
		else if "`vintage'" == "USD_Jun18Update" {
			local label = `var'[2]
		}
		* Drop the variables that caused a download error in Datastream
		if "`label'" == "#ERROR" {
			drop `var'
		}
		* Otherwise change the variable name to use the WMR name
		else {
			* Change dollar sign characters to letter "S" because Stata cannot name variables as dollar signs
			local varname = `var'[2]
			local varname = subinstr("`varname'","$","S",.)
			cap rename `var' exch_var_`varname'
			if _rc == 110 {
				di "dropping column `var' as `varname' downloaded twice"
				drop `var'
			}
		}
	}
	drop if inlist(exch_var_Code,"Name","Code")
	gen date = date(exch_var_Code, "MDY")
	format date %td
	drop exch_var_Code
	destring, replace force
	missings dropvars, force
	missings dropobs, force
	reshape long exch_var_, i(date) j(fx_var) string
	rename exch_var_ lcu_per_usd
	sort fx_var date
	drop if missing(date)
	save "$temp/ER_data/WMRExchRates`sheet'_`vintage'", replace
	clear all
}

* Load and do checks on both cleaned pulls to ensure there exists an overlapping period with perfect correlation.
* The simplest way of ensuring perfect overlap when there may be missing data 
* within each file (datastream excel tool is not perfect) is to first 
* fill any missing data within, then drop all duplicates, then assert there are
* no more than one record per every currency date combination. A duplicate in 
* the remaining union is a disagreement and needs to halt.
foreach vintage in "USD" "USD_Jun18Update" {
	append using "$temp/ER_data/WMRExchRates`sheet'_`vintage'"
	shell rm "$temp/ER_data/WMRExchRates`sheet'_`vintage'.dta"
}
bysort fx_var date (lcu_per_usd): replace lcu_per_usd=lcu_per_usd[1] if missing(lcu_per_usd)
duplicates drop

* Floating point problem prevents drops of some duplicates
bysort fx_var date: gen ratio = lcu_per_usd[_n] / lcu_per_usd[1] if _n > 1
drop if ratio > 0.9999 & ratio < 1.0001
drop if fx_var == "GHANCES" & ratio==10000 * a single download error
quietly by fx_var date: gen dup = cond(_N==1,0,_n)
assert dup == 0
drop ratio
drop dup
save "$temp/ER_data/WMRExchRates`sheet'", replace

log close	
