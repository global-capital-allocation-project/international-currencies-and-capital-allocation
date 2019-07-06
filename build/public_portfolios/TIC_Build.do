* --------------------------------------------------------------------------------------------------
* TIC_Build
* 
* This file imports and cleans raw data from the U.S. Treasury's Treasury International Capital
* (TIC) System. This data provides statistics on cross-border portfolio holdings involving the USA.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_TIC_Build", replace

* Import basic reference data
import excel "$raw/Datafeed_Delivery_Documentation/Categories_Asset_Class.xls", sheet("Typecodes") firstrow clear
keep Typecode mns_class mns_subclass
rename Typecode typecode
save "$output/tic_data/Categories_Asset_Class.dta", replace
clear

* Import TIC crosswalk
tempfile xwalk
import excel "$ticraw/xwalk_iso.xlsx", sheet("Sheet1") firstrow clear
save "`xwalk'", replace

* --------------------------------------------------------------------------------------------------
* Inward and outward US portfolios
* --------------------------------------------------------------------------------------------------

* Import TIC data: Overall outward portfolios
import delimited "$ticraw/Annual/shchistdat_2017update.csv", encoding(ISO-8859-1) clear
replace v2 = "Australia" if v1 == "60089"
drop if _n<4
sxpose, clear
drop if _n==1
replace _var1="year" if _n==1
foreach x of varlist _all {
	local temp=`x'[1]
	if "`temp'"=="" {
		drop `x'
	}
}
foreach x of varlist _all {
	local temp=`x'[1]
	forvalues yy=0/9 {
		local temp=subinstr("`temp'","`yy'","",.)
	}
	local temp=subinstr("`temp'","(","",.)
	local temp=subinstr("`temp'",")","",.)
	local temp=subinstr("`temp'"," ","",.)
	local temp=subinstr("`temp'"," ","",.)
	local temp=subinstr("`temp'","'","",.)
	local temp=subinstr("`temp'",".","",.)
	local temp=subinstr("`temp'",",","",.)
	local temp=subinstr("`temp'","&","",.)
	local temp=subinstr("`temp'","-","",.)
	local temp=substr("`temp'",1,25)
	rename `x' `temp'
}
foreach x of varlist _all {
	rename `x' ccc_`x'
}
rename ccc_year year
rename ccc_Country type
reshape long ccc_, i(year type) j(country) string
rename ccc_ value
replace country="Luxembourg" if country=="BelgiumandLuxembourg"
replace country="British Virgin Islands" if country=="BritishVirginIslands"
replace country="Cayman Islands" if country=="CaymanIslands"
replace country="Hong Kong, Special Administrative Region of China" if country=="HongKong"
replace country="Korea, Republic of" if country=="KoreaSouth"
replace country="New Zealand" if country=="NewZealand"
replace country="Russian Federation" if country=="RussiaUSSRuntil"
replace country="South Africa" if country=="SouthAfrica"
replace country="Taiwan, Republic of China" if country=="Taiwan"
replace country="United Kingdom" if country=="UnitedKingdom"
mmerge country using "`xwalk'"
keep if _merge==3
drop _merge	
destring year, replace force
drop if year==.
drop if type=="Total"
gen debt=1
replace debt=0 if type=="Equity"
replace value=subinstr(value,",","",.)
destring value, force replace
collapse (sum) value, by(year debt iso country)
gen mns_class="E" if debt==0
replace mns_class="B" if debt==1
drop debt
save "$output/tic_data/tic_agg_outward.dta", replace

* Import TIC data: Overall inward portfolios
import delimited "$ticraw/Annual/historical_foreignownus.csv", encoding(ISO-8859-1)clear
drop if _n<8
sxpose, clear
drop if _n==1
drop if regexm(_var1,"19")==1
drop if regexm(_var1,"2000")==1
replace _var1=subinstr(_var1,"Jun","",.)
replace _var1=trim(_var1)
replace _var1="year" if _n==1
replace _var2="type" if _n==1
drop if _var3=="Treasury debt" | _var3=="Agency debt" | _var3=="Corporate debt"
drop _var2
replace _var3="Country" if _n==1

foreach x of varlist _all {
	local temp=`x'[1]
	if "`temp'"=="" {
		drop `x'
	}
}
foreach x of varlist _all {
	local temp=`x'[1]
	forvalues yy=0/9 {
		local temp=subinstr("`temp'","`yy'","",.)
	}
	local temp=subinstr("`temp'","(","",.)
	local temp=subinstr("`temp'",")","",.)
	local temp=subinstr("`temp'"," ","",.)
	local temp=subinstr("`temp'"," ","",.)
	local temp=subinstr("`temp'","'","",.)
	local temp=subinstr("`temp'",".","",.)
	local temp=subinstr("`temp'",",","",.)
	local temp=subinstr("`temp'","&","",.)
	local temp=subinstr("`temp'","-","",.)
	local temp=substr("`temp'",1,25)
	rename `x' `temp'
}
foreach x of varlist _all {
	rename `x' ccc_`x'
}
rename ccc_year year
rename ccc_Country type
reshape long ccc_, i(year type) j(country) string
rename ccc_ value
replace country="Luxembourg" if country=="BelgiumandLuxembourg"
replace country="British Virgin Islands" if country=="BritishVirginIslands"
replace country="Cayman Islands" if country=="CaymanIslands"
replace country="Hong Kong, Special Administrative Region of China" if country=="HongKong"
replace country="Korea, Republic of" if country=="KoreaSouth"
replace country="New Zealand" if country=="NewZealand"
replace country="Russian Federation" if country=="Russia"
replace country="South Africa" if country=="SouthAfrica"
replace country="Taiwan, Republic of China" if country=="Taiwan"
replace country="United Kingdom" if country=="UnitedKingdom"
replace country="Czech Republic" if country=="CzechRepublic"
mmerge country using "`xwalk'"
keep if _merge==3
drop _merge	
destring year, replace force
drop if year==.
drop if type=="Total securities"
gen debt=1
replace debt=0 if type=="Equity"
replace value=subinstr(value,",","",.)
destring value, force replace
collapse (sum) value, by(year debt iso country)
gen mns_class="E" if debt==0
replace mns_class="B" if debt==1
drop debt
tostring year, force replace
replace year = trim(year)
replace year = subinstr(year, "-", "20", .) if strlen(year) == 3
replace year = subinstr(year, "-", "200", .) if strlen(year) == 2
destring year, replace force
save "$output/tic_data/tic_agg_inward.dta", replace

* Importing TIC data: Internal crosswalk
use "$output/tic_data/tic_agg_outward.dta", clear
keep country iso
duplicates drop
replace country=trim(country)
replace country="Taiwan" if regexm(country,"Taiwan")==1
replace country="Hong Kong" if regexm(country,"Hong Kong")==1
replace country="Korea" if regexm(country,"Korea")==1
replace country="Russia" if regexm(country,"Russia")==1
save "$temp/tic_data/tic_xwalk.dta", replace

* Historical data: 2007
import excel "$ticraw/Annual/TIC2007.xlsx", sheet("Table 29") clear
keep A B C D E F G H
rename A country
rename B total
rename C govt_total
rename D govt_usd 
rename E govt_lc 
rename F corp_total 
rename G corp_usd
rename H corp_lc
drop if _n<=5
gen year=2007
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
	replace `x'="0" if `x'=="*"
	destring `x', force replace
	}
order country year	
drop if total==.
save $temp/tic_data/TIC2007_disagg.dta, replace

* Historical data: 2008
import excel "$ticraw/Annual/TIC2008.xlsx", sheet("TB29") clear
keep A B C D E F G H
rename A country
rename B total
rename C govt_total
rename D govt_usd 
rename E govt_lc 
rename F corp_total 
rename G corp_usd
rename H corp_lc
drop if _n<=6
gen year=2008
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
	replace `x'="0" if `x'=="*"
	destring `x', force replace
	replace `x'=`x'/1000
	}
order country year	
drop if total==.
save $temp/tic_data/TIC2008_disagg.dta, replace

* Historical data: 2009-2012
forvalues y=2009/2012{
	import excel "$ticraw/Annual/TIC`y'.xlsx", sheet("A11") clear
	keep A B C D E F G H
	rename A country
	rename B total
	rename C govt_total
	rename D govt_usd 
	rename E govt_lc 
	rename F corp_total 
	rename G corp_usd
	rename H corp_lc
	drop if _n<=6
	gen year=`y'
	foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
		replace `x'="0" if `x'=="*"
		destring `x', force replace
		replace `x'=`x'/1000 if `y'>=2011 | `y'==2009
		}
	order country year	
	drop if total==.
	save $temp/tic_data/TIC`y'_disagg.dta, replace
}

* Historical data: 2013
import delimited "$ticraw/Annual/shc2013_appendix/appendix_tab11.csv", delim(",") encoding(ISO-8859-1) clear
keep v1 v2 v3 v4 v5 v6 v7 v8
rename v1 country
rename v2 total
rename v3 govt_total
rename v4 govt_usd 
rename v5 govt_lc 
rename v6 corp_total 
rename v7 corp_usd
rename v8 corp_lc
drop if _n<=11
gen year=2013
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
	replace `x'="0" if `x'=="*"
	destring `x', force replace
	replace `x'=`x'/1000
	}
order country year		
drop if total==.	
save $temp/tic_data/TIC2013_disagg.dta, replace

* Historical data: 2014
import excel "$ticraw/Annual/shc2014_appendix/shc14_app11_fixed.xlsx", sheet("Sheet2") clear
rename A country
rename B total
rename C govt_total
rename D govt_usd 
rename E govt_lc 
rename F corp_total 
rename G corp_usd
rename H corp_lc
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
	cap replace `x'="0" if `x'=="*"
	destring `x', force replace
	replace `x'=`x'/1000
}
gen year=2014
save $temp/tic_data/TIC2014_disagg.dta, replace

* Historical data: 2015
import delimited "$ticraw/Annual/shca2015_appendix_and_exhibits/shc15_app11.csv", encoding(ISO-8859-1)clear
rename v1 country
rename v2 total
rename v3 govt_total
rename v4 govt_usd 
rename v5 govt_lc 
rename v6 corp_total 
rename v7 corp_usd
rename v8 corp_lc
drop if _n<=11
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
	cap replace `x'="0" if `x'=="*"
	destring `x', force replace
	replace `x'=`x'/1000
}
gen year=2015
save $temp/tic_data/TIC2015_disagg.dta, replace

* Historical data: 2016
import delimited "$ticraw/Annual/shca2017_appendix/shc_app11_2017.csv", encoding(ISO-8859-1)clear
rename v1 country
rename v2 total
rename v3 govt_total
rename v4 govt_usd 
rename v5 govt_lc 
rename v6 corp_total 
rename v7 corp_usd
rename v8 corp_lc
drop if _n<=11
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc {
	cap replace `x'="0" if `x'=="*"
	destring `x', force replace
	replace `x'=`x'/1000
}
gen year=2017
save $temp/tic_data/TIC2017_disagg.dta, replace

* Append the historical data
use $temp/tic_data/TIC2007_disagg.dta, clear
forvalues x = 2008/2015 {
	append using $temp/tic_data/TIC`x'_disagg.dta
}
append using $temp/tic_data/TIC2017_disagg.dta
sort country year

* Clean the data
drop if regexm(country,"Total")==1 | regexm(country,"exporters")==1 | regexm(country,"Greater") | regexm(country,"International")
foreach x in "1" "2" "3" "4" "5" "6" "7" "8" "9" "0" "(" ")" {
	replace country=subinstr(country,"`x'","",.)
}
mmerge country using "$temp/tic_data/tic_xwalk.dta"
drop  if _merge==2
replace iso="CHN" if regexm(country,"China, m")==1
replace iso="CZE" if regexm(country,"Czech")==1
replace iso="KOR" if regexm(country,"Korea")==1
keep if iso~=""
drop country
order iso
rename iso iso_country_code 
drop _merge
gen govt_other=govt_total-govt_usd-govt_lc
gen corp_other=corp_total-corp_usd-corp_lc
save "$output/tic_data/disagg_tic.dta", replace

use "$output/tic_data/disagg_tic.dta", clear
foreach x in total govt_total govt_usd govt_lc corp_total corp_usd corp_lc govt_other corp_other {
	rename `x' value`x'
}
duplicates drop 
reshape long value, i(iso year) j(mns_temp) str	
split mns_temp, p("_")
rename mns_temp1 _detail
replace _detail="B" if _detail=="corp"
replace _detail="BT" if _detail=="govt"
rename mns_temp2 curr
replace curr="all" if curr==""
drop  mns_temp
drop if _detail=="total" | curr=="total"
save "$output/tic_data/disagg_tic_long.dta", replace

* --------------------------------------------------------------------------------------------------
* US foreign assets
* --------------------------------------------------------------------------------------------------

* Historical data: 2001
import excel "$ticraw/Annual/Data_Entry/sch2001r.xlsx", sheet("22") cellrange(A1:F124) firstrow clear
destring Total Common Preferred Mutualfunds Other, replace force
gen Preferred_Other=Preferred+Other
drop Preferred Other
rename Mutualfunds Funds
rename Countryorcategory Countryorregion
gen date=2001
save "$temptic/2001_US_outward_mf.dta", replace

* Historical data: 2003
import excel "$ticraw/Annual/Data_Entry/shc2003r.xlsx", sheet("24") cellrange(A1:F125) firstrow clear
destring Total Common Preferred Mutualfunds Other, replace force
gen Preferred_Other=Preferred+Other
drop Preferred Other
rename Mutualfunds Funds
rename Countryorcategory Countryorregion
gen date=2003
save "$temptic/2003_US_outward_mf.dta", replace

* Historical data: 2004
import excel "$ticraw/Annual/Data_Entry/shc2004r.xlsx", sheet("27") cellrange(A1:F113) firstrow clear
rename Preffered Preferred
destring Total Common Preferred Funds Other, replace force
gen Preferred_Other=Preferred+Other
drop Preferred Other
rename Countryorcategory Countryorregion
gen date=2004
save "$temptic/2004_US_outward_mf.dta", replace

* Historical data: 2005
import excel "$ticraw/Annual/Data_Entry/shc2005r.xlsx", sheet("27") cellrange(A1:F110) firstrow clear
rename Preffered Preferred
destring Total Common Preferred Funds Other, replace force
gen Preferred_Other=Preferred+Other
drop Preferred Other
rename Countryorcategory Countryorregion
gen date=2005
save "$temptic/2005_US_outward_mf.dta", replace

* Historical data: 2006
import excel "$ticraw/Annual/Data_Entry/shc2006r.xlsx", sheet("27") cellrange(A1:F114) firstrow clear
destring Total Common Preferred Funds Other, replace force
gen Preferred_Other=Preferred+Other
drop Preferred Other
rename Countryorcategory Countryorregion
gen date=2006
save "$temptic/2006_US_outward_mf.dta", replace

* Historical data: 2007
import excel "$ticraw/Annual/TIC2007.xlsx", sheet("Table 30") cellrange(A4:F124) firstrow clear
rename PreferredOther Preferred_Other
destring Total Common Preferred_Other Funds, replace force
gen date=2007
rename Countryorcategory Countryorregion
rename CommonStock Common
drop CountryCode
save "$temptic/2007_US_outward_mf.dta", replace

* Historical data: 2008
import excel "$ticraw/Annual/TIC2008.xlsx", sheet("TB30") cellrange(A5:E137) firstrow clear
rename PreferredOther Preferred_Other
destring Total Common Preferred_Other Funds, replace force
rename Countryorcategory Countryorregion
rename CommonStock Common
gen date=2008
save "$temptic/2008_US_outward_mf.dta", replace

* Historical data: 2009
import excel "$ticraw/Annual/TIC2009.xlsx", sheet("A12") cellrange(A5:E134) firstrow clear
rename PreferredOther Preferred_Other
destring Total Common Preferred_Other Funds, replace force
rename Countryorcategory Countryorregion
rename CommonStock Common
gen date=2009
save "$temptic/2009_US_outward_mf.dta", replace

* Historical data: 2010
import excel "$ticraw/Annual/TIC2010.xlsx", sheet("A12") cellrange(A5:E132) firstrow clear
rename PreferredOther Preferred_Other
destring Total Common Preferred_Other Funds, replace force
rename Countryorcategory Countryorregion
rename CommonStock Common
gen date=2010
save "$temptic/2010_US_outward_mf.dta", replace

* Historical data: 2011
import excel "$ticraw/Annual/TIC2011.xlsx", sheet("A12") cellrange(A5:E129) firstrow clear
rename PreferredOther Preferred_Other
destring Total Common Preferred_Other Funds, replace force
rename Countryorcategory Countryorregion
rename CommonStock Common
gen date=2011
save "$temptic/2011_US_outward_mf.dta", replace

* Historical data: 2012
import excel "$ticraw/Annual/TIC2012.xlsx", sheet("A12") cellrange(A5:E134) firstrow clear
rename PreferredOther Preferred_Other
destring Total Common Preferred_Other Funds, replace force
rename Countryorcategory Countryorregion
rename CommonStock Common
gen date=2012
save "$temptic/2012_US_outward_mf.dta", replace

* Historical data: 2013
import delimited "$ticraw/Annual/shc2013_appendix/appendix_tab12.csv",delimit(",") rowrange(10:143) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="Stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2013
save "$temptic/2013_US_outward_mf.dta", replace

* 2014 is missing

* Historical data: 2015
import delimited "$ticraw/Annual/shca2015_appendix_and_exhibits/shc15_app12.csv",delimit(",") rowrange(5:133) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2015
save "$temptic/2015_US_outward_mf.dta", replace

* Historical data: 2016
import delimited "$ticraw/Annual/shc2016_appendix/shc_app12_2016.csv",delimit(",") rowrange(5:133) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2016
save "$temptic/2016_US_outward_mf.dta", replace

* Historical data: 2017
import delimited "$ticraw/Annual/shca2017_appendix/shc_app12_2017.csv",delimit(",") rowrange(5:133) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2017
save "$temptic/2017_US_outward_mf.dta", replace

* --------------------------------------------------------------------------------------------------
* US foreign liabilities
* --------------------------------------------------------------------------------------------------

* Historical data: 2002
import excel "$ticraw/Annual/Data_Entry/shl2002r.xlsx", sheet("17") cellrange(A1:H212) firstrow clear
destring Total Commonstock Otherequity USTreasLTdebt USgovtagencyLTdebt CorporateandmunicipalLTdebt Shorttermdebt, replace force
drop Total USTreasLTdebt USgovtagencyLTdebt CorporateandmunicipalLTdebt Shorttermdebt
gen date=2002
save "$temptic/2002_US_inward_mf.dta", replace

* Historical data: 2003
import excel "$ticraw/Annual/Data_Entry/shl2003r.xlsx", sheet("16") cellrange(A1:I213) firstrow clear
drop if G=="Other"
rename Agencydebt Agency_ABS
rename G Agency_Other
rename Corpdebt Corp_ABS
rename I Corp_other
destring Total Commonstock Otherequity Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other, replace force
gen date=2003
drop Total Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other
save "$temptic/2003_US_inward_mf.dta", replace

* Historical data: 2004
import excel "$ticraw/Annual/Data_Entry/shl2004r.xlsx", sheet("14") cellrange(A1:I213) firstrow clear
drop if G=="Other"
rename Agencydebt Agency_ABS
rename G Agency_Other
rename Corpdebt Corp_ABS
rename I Corp_other
destring Total Commonstock Otherequity Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other, replace force
gen date=2004
drop Total Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other
save "$temptic/2004_US_inward_mf.dta", replace

* Historical data: 2005
import excel "$ticraw/Annual/Data_Entry/shl2005r.xlsx", sheet("16") cellrange(A1:I214) firstrow clear
drop if G=="Other"
rename Agencydebt Agency_ABS
rename G Agency_Other
rename Corpdebt Corp_ABS
rename I Corp_other
destring Total Commonstock Otherequity Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other, replace force
gen date=2005
drop Total Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other
save "$temptic/2005_US_inward_mf.dta", replace

* Historical data: 2006
import excel "$ticraw/Annual/Data_Entry/shl2006r.xlsx", sheet("18") cellrange(A1:I215) firstrow clear
drop if G=="Other"
rename Agencydebt Agency_ABS
rename G Agency_Other
rename Corpdebt Corp_ABS
rename I Corp_other
destring Total Commonstock Otherequity Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other, replace force
gen date=2006
drop Total Treasdebt Agency_ABS Agency_Other Corp_ABS Corp_other
save "$temptic/2006_US_inward_mf.dta", replace

* 2007 to 2013 are issing

* Historical data: 2014
import delimited "$ticraw/Annual/shl2014r-appx/appendix_tab04.csv",delimit(",") rowrange(10:227) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="Stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2014
replace Common=Common+Preferred_Other
drop Preferred Total
rename Countryorregion Country
save "$temptic/2014_US_inward_mf.dta", replace

* Historical data: 2015
import delimited "$ticraw/Annual/shl2015r-appx/shl_app04_2015.csv",delimit(",") rowrange(5:232) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="Stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2015
replace Common=Common+Preferred_Other
drop Preferred Total
rename Countryorregion Country
save "$temptic/2015_US_inward_mf.dta", replace

* Historical data: 2016
import delimited "$ticraw/Annual/shla2016r-appx/shl_app04_2016.csv",delimit(",") rowrange(5:232) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="Stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2016
replace Common=Common+Preferred_Other
drop Preferred Total
rename Countryorregion Country
save "$temptic/2016_US_inward_mf.dta", replace

* Historical data: 2017
import delimited "$ticraw/Annual/shla2017r-appx/shl_app04_2017.csv",delimit(",") rowrange(5:232) encoding(ISO-8859-1)clear
rename v1 Countryorregion 
rename v2 Total 
rename v3 Common
rename v4 Funds
rename v5 Preferred_Other
drop if Common=="Stock"
destring Total Common Preferred_Other Funds, replace force
gen date=2017
replace Common=Common+Preferred_Other
drop Preferred Total
rename Countryorregion Country
save "$temptic/2017_US_inward_mf.dta", replace

* --------------------------------------------------------------------------------------------------
* Merge all the above into a single dataset
* --------------------------------------------------------------------------------------------------

* US Outward
use "$temptic/2001_US_outward_mf.dta", clear
forvalues z=2003/2013 {
	append using "$temptic/`z'_US_outward_mf.dta"
}
	forvalues z=2015/2017 {
append using "$temptic/`z'_US_outward_mf.dta"
}
replace Countryorregion="China" if Countryorregion=="China, Peoples Republic of" | Countryorregion=="China, mainland (1)"| Countryorregion=="China, mainland1"| Countryorregion=="China,mainland"| Countryorregion=="China, mainland"
replace Countryorregion="Taiwan" if Countryorregion=="China, Republic of (Taiwan)" 
replace Countryorregion="Korea" if Countryorregion=="Korea, South"
replace Countryorregion="Curacao" if Countryorregion=="Curacao (2)" 
replace Countryorregion="Guadeloupe" if Countryorregion=="Guadeloupe (3)" 
replace Countryorregion="Hong Kong" if Countryorregion=="Hong Kong, S.A.R." 
replace Countryorregion="Kirabati" if Countryorregion=="Kiribati" 
replace Countryorregion="Serbia" if Countryorregion=="Serbia (2)" | Countryorregion=="Serbia (4)"
replace Countryorregion="Serbia" if Countryorregion=="Serbia and Montenegro"
mmerge Countryorregion using "$temptic/tic_xwalk.dta", umatch(country)
replace iso="CZE" if Countryorregion=="Czech Republic"
replace iso="ARE" if Countryorregion=="United Arab Emirates"
replace iso="OTH" if _merge==1
drop if missing(iso) | missing(date)
drop _merge Countryorregion
order date iso Total Common Funds Preferred
sort date iso
save "$output/tic_data/US_outward_mf.dta", replace

* US Inward
use "$temptic/2002_US_inward_mf.dta", clear
forvalues z=2003/2006 {
	append using "$temptic/`z'_US_inward_mf.dta"
}
* Note that we are overestimating funds in this early sample this way
rename Otherequity Funds
rename Commonstock Common
forvalues z=2014/2017 {
	append using "$temptic/`z'_US_inward_mf.dta"
}
rename Country Countryorregion

replace Countryorregion="China" if Countryorregion=="China, Peoples Republic of" | Countryorregion=="China, mainland (1)"| Countryorregion=="China, mainland (2)"| Countryorregion=="China, mainland1"| Countryorregion=="China,mainland"| Countryorregion=="China, mainland" 
replace Countryorregion="China" if Countryorregion=="China, P.R." | Countryorregion=="China, P.R.C" | Countryorregion=="China, P.R.C." | Countryorregion=="China, P.R." | Countryorregion=="China, P.R." | Countryorregion=="China, P.R."
replace Countryorregion="Taiwan" if Countryorregion=="China, Republic of (Taiwan)" |  Countryorregion==" China, Rep. of (Taiwan)"
replace Countryorregion="Korea" if Countryorregion=="Korea, South"
replace Countryorregion="Curacao" if Countryorregion=="Curacao (2)" 
replace Countryorregion="Guadeloupe" if Countryorregion=="Guadeloupe (3)" 
replace Countryorregion="Hong Kong" if Countryorregion=="Hong Kong, S.A.R." 
replace Countryorregion="Hong Kong" if Countryorregion=="Hong Kong S.A.R." 
replace Countryorregion="Kirabati" if Countryorregion=="Kiribati" 
replace Countryorregion="Serbia" if Countryorregion=="Serbia (2)" | Countryorregion=="Serbia (4)"
replace Countryorregion="Serbia" if Countryorregion=="Serbia and Montenegro"

mmerge Countryorregion using "$temptic/tic_xwalk.dta", umatch(country)
replace iso="CZE" if Countryorregion=="Czech Republic"
replace iso="ARE" if Countryorregion=="United Arab Emirates"
replace iso="OTH" if _merge==1
drop if missing(iso) | missing(date)
drop _merge Countryorregion
order date iso  Common Funds 
sort date iso
save "$output/tic_data/US_inward_mf.dta", replace

* Delete intermediate files created above
forvalues z=2001/2017 {
	cap erase "$temptic/`z'_US_inward_mf.dta"
	cap erase "$temptic/`z'_US_outward_mf.dta"
}

* Read in the raw US liabilities data from TIC; export to Stata format
foreach debt_type in "long_term" "short_term" {
	insheet using "$raw/TIC/RawData/Annual/US_Liabilities/tic_us_`debt_type'_liabilities.csv", clear
	save "$temp/tic_data/tic_us_`debt_type'_liabilities.dta", replace
}

* Merge long term and short term US liabilities data; store merged dataset
use "$temp/tic_data/tic_us_long_term_liabilities.dta", clear
rename debt_amount lt_debt_amount
merge 1:1 year currency mns_subclass mns_class using "$temp/tic_data/tic_us_short_term_liabilities.dta", keepusing(debt)
rename debt_amount st_debt_amount
gen includes_short_term_debt = 0
replace includes_short_term_debt = 1 if _merge == 3
replace st_debt = 0 if st_debt == .
gen debt_amount_usd_billions = lt_debt + st_debt
drop lt_deb st_debt _merge
rename currency currency_id
save "$temp/tic_data/tic_us_all_liabilities.dta", replace

log close
