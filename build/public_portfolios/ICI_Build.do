* --------------------------------------------------------------------------------------------------
* ICI_Build
* 
* This file imports raw data from the Investment Company Institute (ICI).
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_ICI_Build", replace

import excel "$raw/ICI/18_fb_table3.xls", sheet("final") clear
keep if _n>=25
drop H K
rename (A B C D E F G I J ) (year total equity_dom equity_intl hybrid bond_tax bond_muni mm_tax mm_notax)
destring *, force replace
gen ici_equity_us = equity_dom+equity_intl
gen ici_bond_us = bond_tax+bond_muni
gen ici_mm_us = mm_tax+mm_notax
gen ici_hybrid_us = hybrid
rename total ici_total_us
drop if missing(year)
save $temp/ici_data/US_ICI_sumstats_y, replace
save $output/ici_data/US_ICI_sumstats_y, replace
clear

clear
forvalues y=$firstyear(1)$lastyear {
	local yrshort = substr("`y'",-2,2)
	forvalues q=1(1)4 {
		di "`yrshort'q`q'"
		capture import excel "$raw/ICI/World_Quarterly.xlsx", sheet("`yrshort'q`q'") clear
		count
		if `r(N)'>0 {
			keep A B C D E F G
			rename (A B C D E F G) (country_name total equity bond mm hybrid other)
			drop if _n==1
			destring total equity bond mm hybrid other, force replace
			gen date_q = yq(`y',`q')
			format %tq date_q
			save $temp/ici_data/NonUS_`y'q`q', replace
			clear
		}
	}
}

clear
forvalues y=$firstyear(1)$lastyear {
	forvalues q=1(1)4 {
		capture append using $temp/ici_data/NonUS_`y'q`q'
		capture tab date_q
	}
}
drop if missing(country_name) | country_name=="0"
replace country_name="Australia" if country_name=="Australia2"
replace country_name="Netherlands" if country_name=="Netherlands2"
replace country_name="United Kingdom" if country_name=="United Kingdo"
drop if missing(total) & missing(equity) & missing(bond) & missing(mm)
sort country_name date_q
save $temp/ici_data/NonUS_ICI_sumstats_q, replace
save $output/ici_data/NonUS_ICI_sumstats_q, replace
clear

* Save the ICI ETF data in DTA format
insheet using "$raw/ICI/ICI_US_ETF_AUM_Factbook_2018.csv", clear
replace ici_total  = subinstr(ici_total ,",","",.)
destring ici_total, replace
save "$temp/ici_data/ICI_US_ETF_AUM_Factbook_2018.dta", replace

insheet using  "$raw/ICI/ICI_US_ETF_AUM_Factbook_2018_Extended.csv", clear
keep year domesticequitybroadbased domesticequitysector globalinternationalequity commodities hybrid bond fundoffunds
save "$temp/ici_data/ICI_US_ETF_AUM_Factbook_2018_Extended.dta", replace

use "$temp/ici_data/ICI_US_ETF_AUM_Factbook_2018.dta", clear
merge 1:1 year using "$temp/ici_data/ICI_US_ETF_AUM_Factbook_2018_Extended.dta", keep(1 2 3) nogen
sort year
save "$temp/ici_data/ICI_US_ETF_AUM_Factbook_2018_Consolidated.dta", replace

log close

