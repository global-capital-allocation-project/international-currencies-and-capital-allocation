* --------------------------------------------------------------------------------------------------
* Merge_OpenFigi_BBG
*
* This job loads CSV output data obtained from OpenFIGI as well as the corresponding raw data pulled
* from the Bloomberg terminal. It merges these and produces a consolidated DTA file with information
* from the OpenFIGI pull.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_merge_openfigi_bbg", replace

* Load keyfile
clear
insheet using $externalid_temp/externalid_keyfile.csv, names
foreach var of varlist exchcode-securitydescription {
	replace `var' = "" if `var'=="NA"
}
append using $externalid_raw/externalid_keyfile.dta
duplicates drop externalid_mns, force
order externalid_mns idformat
gen length = length(externalid_mns)
gsort length idformat
drop length
save $externalid_temp/externalid_keyfile.dta, replace

* Load raw data from Bloomberg
clear
insheet using $externalid_raw/bbg_figi.csv, names

* Next two lines are for safety
* In case somebody pulls in a duplicate record in a future BBG pull
duplicates drop
bysort figi: drop if _n>1

save $externalid_temp/bbg_figi_data.dta, replace

* Join with raw data from OpenFIGI
merge 1:m figi using $externalid_temp/externalid_keyfile.dta
keep if _merge > 1
order externalid_mns idformat figi-_merge
drop _merge
duplicates drop
gen length = length(externalid_mns)
gsort length idformat externalid_mns
drop length

* Make consistent with MNS data
replace crncy = upper(crncy)
ren id_isin isin
ren id_cusip cusip
ren cntry_issue_iso iso2
ren crncy currency_id
ren cpn coupon
ren maturity maturitydate

* Fix coupon format
tostring coupon, replace force

* Fix iso_country_codes
replace iso2 = "XSN" if iso2 == "MULT" | iso2 == "SNAT" 
mmerge iso2 using "$raw/macro/Concordances/iso2_iso3.dta"
drop if _merge==2
replace iso3 = "XSN" if iso2 == "XSN"
drop _merge iso2
rename iso3 iso_country_code

* Fix maturitydate format
replace maturitydate = subinstr(maturitydate, "/", "-", .)
gen maturitydate_cln = date(maturitydate, "MDY", 2085)
format maturitydate_cln %d
drop maturitydate
rename maturitydate_cln maturitydate
recast long maturitydate

* Generate mns_class and mns_subclass according to openfigi & bloomberg data.
gen figi_mns_class = ""
gen figi_mns_subclass = ""

* Market Sector Comdty
replace figi_mns_class = "D" if marketsector=="Comdty"
replace figi_mns_subclass = "NC" if marketsector=="Comdty"

* Market Sector Corp
replace figi_mns_class = "B" if marketsector== "Corp"
replace figi_mns_subclass = "C" if marketsector== "Corp"

* Market Sector Curncy
replace figi_mns_class = "D" if marketsector== "Curncy"
replace figi_mns_subclass = "C" if marketsector== "Curncy"

* Market Sector Equity
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Common Stock"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Depositary Receipt"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype=="GDR"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "FUTURE"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "Mutual Fund"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "Option" & securitytype == "Equity Option"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Partnership Shares" & securitytype == "Ltd Part"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Partnership Shares" & securitytype == "MLP"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Preference" & securitytype == "Preference"
replace figi_mns_class = "E" if marketsector== "Equity" & (securitytype2== "REIT" | securitytype == "REIT")
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Right" & securitytype == "Right"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Unit" & securitytype == "Stapled Security"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "Unit" & securitytype == "Unit"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "Warrant"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype == "ADR"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Closed-End Fund"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Hedge Fund"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype == "Common Stock"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity Option"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity WRT"
replace figi_mns_class = "E" if marketsector== "Equity" & securitytype2== "" & securitytype == "ETP"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Fund of Funds"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "I.R. Swp WRT"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Index WRT"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Indx Fut WRT"
replace figi_mns_class = "MF" if marketsector== "Equity" & securitytype2== "" & securitytype == "Open-End Fund"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "Right"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FORWARD"
replace figi_mns_class = "D" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FUTURE"

replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "FUTURE"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "Option" & securitytype == "Equity Option"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "Warrant"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity Option"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Equity WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "I.R. Swp WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Index WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Indx Fut WRT"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "Right"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FORWARD"
replace figi_mns_subclass = "NC" if marketsector== "Equity" & securitytype2== "" & securitytype == "SINGLE STOCK FUTURE"

replace figi_mns_class = "B" if figi=="BBG000L50ZL3" // single fix for a conflicting openfigi error
replace figi_mns_subclass = "C" if figi=="BBG000L50ZL3" // single fix for a conflicting openfigi error

* Market Sector Govt
replace figi_mns_class = "B" if marketsector== "Govt"
replace figi_mns_subclass = "S" if marketsector== "Govt"

* Market Sector Index
replace figi_mns_class = "D" if marketsector== "Index"
replace figi_mns_subclass = "NC" if marketsector== "Index"

* Market Sector Mortgage
replace figi_mns_class = "B" if marketsector== "Mtge"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "2ND LIEN" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Auto"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Card"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Home"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "ABS Other"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "Agncy ABS Home"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "Agncy ABS Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "ABS Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "MV"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS Other" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/HG" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/HG" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/MEZZ" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/MEZZ" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "ABS/MEZZ" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDO2" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS(ABS)" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS(CRP)" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CDS(CRP)" & securitytype == "HB"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMBS" & securitytype == "Agncy CMBS"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMBS" & securitytype == "CMBS"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO FLT"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO INV"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO IO"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO Other"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO PO"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Agncy CMO Z"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Prvt CMO FLT"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Prvt CMO IO"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CMO" & securitytype == "Prvt CMO Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "CRE" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "HY" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "HY" & securitytype == "MV"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "IG" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "HB"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "MV"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "LL" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "MEZZ" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "MML" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "Cadian"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 10yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 15yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 20yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS 30yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS ARM"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "MBS Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Pool" & securitytype == "SBA Pool"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "RMBS" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "SME" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "SME" & securitytype == "SN"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "TBA" & securitytype == "MBS balloon"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "TRP" & securitytype == "CF"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "TRP/REIT" & securitytype == "CF"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Agncy CMO FLT"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Agncy CMO IO"
replace figi_mns_subclass = "A" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Agncy CMO Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO FLT"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO IO"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO Other"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO PO"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "Whole Loan" & securitytype == "Prvt CMO Z"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 10yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 15yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 20yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS 30yr"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS ARM"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS balloon"
replace figi_mns_subclass = "SF" if marketsector== "Mtge" & securitytype2== "" & securitytype == "MBS Other"

* Market Sector Muni
replace figi_mns_class = "B" if marketsector== "Muni"
replace figi_mns_subclass = "LS" if marketsector== "Muni"

* Market Sector Pfd
replace figi_mns_class = "E" if marketsector== "Pfd"

* Save output
order externalid_mns figi cusip isin name currency_id maturitydate coupon iso_country_code figi_mns_class figi_mns_subclass 
sort externalid_mns
save $externalid_temp/externalid_openfigi_bloomberg.dta, replace

cap log close
