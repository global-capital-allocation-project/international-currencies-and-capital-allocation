* --------------------------------------------------------------------------------------------------
* OECD_Build
* 
* This file imports raw data from the OECD (we only use selected indicators from the OECD's 
* statistical warehouse).
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_OECD_Build", replace

* --------------------------------------------------------------------------------------------------
* Fund Assets as Fraction of GDP
* --------------------------------------------------------------------------------------------------
import delimited $raw/OECD/OECD_funds_over_gdp.csv, clear
rename ïlocation Domicile
rename time date
drop country indicator v4 v6 unitcode unit powercodecode powercode referenceperiodcode referenceperiod  flagcodes flags
drop if strlen(date)~=4
destring date, replace
sort Dom date
save "$output/oecd_data/OECD_funds_over_gdp_cleaned.dta", replace

* --------------------------------------------------------------------------------------------------
* Fund Assets as Fraction of Total Assets
* --------------------------------------------------------------------------------------------------

* Data are in current USD millions
* These use:  Financial balance sheets - non consolidated - SNA 2008
import delimited $raw/OECD/OECD_funds_assets.csv, clear
rename ïlocation Domicile
rename time date
rename v6 sector_name
drop  transact sector country measure v8 v10 unitcode unit powercodecode powercode referenceperiodcode referenceperiod flagcodes flags
order Dom date transaction sector_name value
sort Dom date transaction sector_name
save "$output/oecd_data/OECD_funds_assets_cleaned.dta", replace

* Some countries only report on consolidated base so we use
* These use:  Financial balance sheets - non consolidated - SNA 2008
import delimited $raw/OECD/OECD_funds_assets_consolidated.csv, clear
rename ïlocation Domicile
rename time date
rename v6 sector_name
drop  transact sector country measure v8 v10 unitcode unit powercodecode powercode referenceperiodcode referenceperiod flagcodes flags
order Dom date transaction sector_name value
sort Dom date transaction sector_name
save "$output/oecd_data/OECD_funds_assets_consolidated_cleaned.dta", replace

* --------------------------------------------------------------------------------------------------
* Bond vs. Loan Financing
* --------------------------------------------------------------------------------------------------
import delimited $raw/OECD/OECD_loans_bonds.csv, clear
rename ïlocation Domicile
rename time date
rename v6 sector_name
drop country  measure v8 v10 unitcode unit powercodecode powercode referenceperiodcode referenceperiod flagcodes flags
order Dom date sector_name transaction  value
sort Dom date sector_name transaction 
save "$output/oecd_data/OECD_loans_bonds_cleaned.dta", replace

import delimited $raw/OECD/OECD_loans_bonds_consolidated.csv, clear
rename ïlocation Domicile
rename time date
rename v6 sector_name
drop country  measure v8 v10 unitcode unit powercodecode powercode referenceperiodcode referenceperiod flagcodes flags
order Dom date sector_name transaction  value
sort Dom date sector_name transaction 
save "$output/oecd_data/OECD_loans_bonds_consolidated_cleaned.dta", replace

log close
