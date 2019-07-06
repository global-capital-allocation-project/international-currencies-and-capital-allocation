* --------------------------------------------------------------------------------------------------
* Morningstar_API_Build
*
* This job processes fund-level metadata received from Morningstar: the data comes primarily from a
* direct FTP delivery from Morningstar, but also includes portions obtained from the Morningstar
* Direct platform. The output of this file consists of several master mapping files that are used at 
* various points throughout the build and analysis.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Morningstar_API_Build", replace

/////////////////////////////////////
/// LOAD AND CLEAN MAPPING FILES + STATIC DATA FROM FTP DELIVERY, TIME SERIES FROM MORNINGSTAR DIRECT
/////////////////////////////////////

clear all
set excelxlsxlargefile on
local sheet_names ""Active USA" "nonActive USA" "Active NonUS" "NonActive NonUS""

foreach sheet of local sheet_names {
	preserve	
	import excel using "$raw/morningstar_direct_supplementary/Mapping_Booth_20181220.xlsx" , sheet("`sheet'") first clear
	count
	save temp, replace
	restore
	append using temp, force
	count
}
rm temp.dta // remove temporary .dta file
drop if missing(InvestmentIdName) & missing(SecId )
destring MasterPortfolioId ExchangeTradedShare ConvertedFundAUM ProspectusNetExpenseRatio ProspectusOperatingExpenseRatio AnnualReportGrossExpenseRatio, replace force
gen region="US" if Domicile=="United States"
replace region="Rest" if Domicile!="United States"
save "$output/morningstar_api_data/Mapping_Plus_Static.dta", replace emptyok

clear
insheet using "$raw/morningstar_direct_supplementary/secid_all_monthly_returns.csv", names
save "$output/morningstar_api_data/secid_all_monthly_returns.dta", replace emptyok

clear
insheet using "$raw/morningstar_direct_supplementary/secid_all_monthly_flows.csv", names
save "$output/morningstar_api_data/secid_all_monthly_flows.dta", replace emptyok

/////////////////////////////////////
/// PROCESS FACTSET DATA FOR FO/FE AUM APPORTIONING
/////////////////////////////////////

cap program drop parse_factset_aum
program parse_factset_aum
	drop if _n < 3
	local oldnames ""
	local newnames ""
	foreach var of varlist * {
	    local label : variable label `var'
	    if ("`label'" != "") {
	        local oldnames `oldnames' `var'
	        local newnames `newnames' _`label'_
	    }
	}
	rename (`oldnames')(`newnames')
	rename A date
	cap drop AJ
	gen year = year(date)
	drop date
	order year
	drop if missing(year)
	drop if year > $lastyear | year < $firstyear
	destring _*, force replace
	gen anchor = 1
	reshape wide _*, j(year) i(anchor)
	foreach var of varlist * {
	    local label : variable label `var'
	    if ("`label'" != "") {
	        local oldnames `oldnames' `var'
	        local newnames `newnames' _`label'_
	    }
	}
	foreach i of num $firstyear/$lastyear {
		rename _*_`i' y`i'_*	
	}
	local stubs ""
	foreach i of num $firstyear/$lastyear {
		local stubs "`stubs' y`i'_"
	}
	reshape long `stubs', i(anchor) j(MasterPortfolioId)
	drop anchor
	rename *_ *
end

* Load ETF AUM
import excel using $raw/factset/factset_etf_aum.xlsx, clear sheet("ETF AUM") firstrow
quietly parse_factset_aum
save "$temp/morningstar_api_data/etf_aum.dta", replace emptyok

* Load total fund AUM
import excel using $raw/factset/factset_etf_aum.xlsx, clear sheet("Total Fund AUM") firstrow
quietly parse_factset_aum
save "$temp/morningstar_api_data/total_fund_aum.dta", replace emptyok

* Construct weights; we assign to FO by default if data is missing
use "$temp/morningstar_api_data/total_fund_aum.dta", clear
mmerge MasterPortfolioId using "$temp/morningstar_api_data/etf_aum.dta", unmatched(m) uname(e_)
foreach i of num $firstyear/$lastyear {
	gen etf_weight_`i' = min(1, e_y`i' / y`i')
	replace etf_weight_`i' = 0 if missing(e_y`i') | missing(y`i')
}
keep MasterPortfolioId etf_weight_*
save "$temp/morningstar_api_data/etf_weights.dta", replace emptyok

/////////////////////////////////////
/// CLEAN API DATA AND RESOLVE DUPLICATES PER MASTERPORTFOLIOID
/////////////////////////////////////

use "$output/morningstar_api_data/Mapping_Plus_Static.dta", clear
rename BroadCategoryName BroadCategoryGroup
rename Domicile DomicileCountryId
drop if missing(MasterPortfolioId)
gen _DomicileId = substr(DomicileId , 8, 10)
keep MasterPortfolioId FundName DomicileCountryId Status BroadCategoryGroup LegalType _DomicileId
bysort MasterPortfolioId : egen numDom = nvals(Domicile)
bysort MasterPortfolioId : egen numBroad = nvals(BroadCategoryGroup)
gen preferentialDom = Domicile
gen allDom = Domicile
bysort MasterPortfolioId (Domicile): replace allDom = allDom + ", " + DomicileCountryId[_n-1] if _n>1 & Domicile != Domicile[_n-1]
bysort MasterPortfolioId (Domicile): replace allDom = allDom[_n-1] if allDom[_n-1] != allDom[_n] & strpos(allDom[_n-1], DomicileCountryId) > 0
bysort MasterPortfolioId (Domicile): replace allDom = allDom[_N]

* RESOLVING DUPLICATES OF (DomicileCountryId, Status, BroadCategoryGroup, LegalType) WITHIN MasterPortfolioId

* Preference ordering for multiple domiciles
gen resolved_conflict = .
foreach dom in "United States"	"United Kingdom" "Ireland" "Portugal" "Luxembourg" "Australia" "Singapore" "Sweden"	"Finland" "China" "Hong Kong" "Cayman" "Bermuda" "Guernsey" "British Virgin Islands" "Isle of Man" "Curaçao" "Jersey" "Malta" "Mauritius" "Panama" {
	replace preferential = "`dom'" if strpos(allDom, "`dom'") & numDom > 1 & resolved_conflict==.
	replace resolved_conflict = 1 if strpos(allDom, "`dom'") & numDom > 1 & resolved_conflict==.
}
assert resolved_conflict == 1 if numDom > 1
replace Domicile=preferential
gen region="US" if Domicile=="United States"
replace region="Rest" if Domicile!="United States"
drop resolved_conflict preferential allDom numDom

* MasterPortfolioId is active if any are active
bysort MasterPortfolioId : egen maxStatus = max(Status)
replace Status=maxStatus
gen status="Active" if Status==1
replace status="Inactive" if Status==0
drop maxStatus Status

* MasterPortfolioId takes modal BroadCategoryGroup
bysort MasterPortfolioId: egen modalBroad = mode(BroadCategoryGroup), maxmode
replace BroadCategoryGroup = modalBroad
drop modalBroad numBroad
duplicates drop

* We keep only ONE record per MasterPortfolioId / LegalType combination; note that this mapping data
* does not carry any share-class specific information
bysort MasterPortfolioId LegalType: keep if _n == 1

* Use ISO3 codes
gen _domicile_for_merge = DomicileCountryId
replace _domicile_for_merge = "Virgin Islands, British" if _domicile_for_merge == "British Virgin Islands"
replace _domicile_for_merge = "Russian Federation" if _domicile_for_merge == "Russia"
replace _domicile_for_merge = "Korea, Republic of" if _domicile_for_merge == "South Korea"
replace _domicile_for_merge = "Taiwan, Province of China" if _domicile_for_merge == "Taiwan"
replace _domicile_for_merge = "Virgin Islands, U.S." if _domicile_for_merge == "US Virgin Islands"
mmerge _domicile_for_merge using "$output/concordances/country_names", umatch(country_name) unmatched(m) uname(u_)
assert _merge == 3
drop _merge
replace DomicileCountryId = u_iso
drop _DomicileId _domicile_for_merge u_iso

* MasterPortfolioId takes LegalType=FO by default if we get aggregated reports that are repeated in the
* FO and FE universes, unless an apportioning weight is given in $temp/morningstar_api_data/etf_weights.dta.
* We construct this apportioning weights for any fund with AUM >10 billion USD at any point in time that
* reports as a single portfolio for both their FO and ETF structures. These are primarily large Vanguard funds
* that use this idiosyncratic reporting convention.
mmerge MasterPortfolioId using "$temp/morningstar_api_data/etf_weights.dta", unmatched(m)
bysort MasterPortfolioId : egen numLeg = nvals(LegalType)
foreach i of num $firstyear/$lastyear {
	gen fundtype_weight_`i' = etf_weight_`i' if LegalType == "FE" & ~missing(etf_weight_`i')
	replace fundtype_weight_`i' = (1 - etf_weight_`i') if LegalType == "FO" & ~missing(etf_weight_`i')
	replace fundtype_weight_`i' = 1 if missing(fundtype_weight_`i') & numLeg == 1
	replace fundtype_weight_`i' = 0 if numLeg > 1 & missing(fundtype_weight_`i') & LegalType!="F0"
}
rename LegalType fundtype
drop etf_weight_*
cap drop _merge
drop numLeg
save "$output/morningstar_api_data/API_for_merging.dta", replace
drop if fundtype_weight_$lastyear <.5
drop fundtype_weight_*
save "$output/morningstar_api_data/API_for_merging_uniqueonly.dta", replace

/////////////////////////////////////
/// MERGE WITH BENCHMARK + RETURN DATA, FLOWS DATA
/////////////////////////////////////

use "$output/morningstar_api_data/Mapping_Plus_Static.dta", clear
mmerge SecId using "$output/morningstar_api_data/secid_all_monthly_returns.dta", type(1:1) unmatched(master) umatch(secid)
ren _merge mergereturns
order monthlygross*, last

local maxIndices = 8

* First strip the benchmark + x% (promise of alpha irrelevant)
gen PrimaryProspectusBenchmarkCln = regexr(primaryprospectusbenchmark, "\+[0-9]\%", "")
gen PrimaryProspectusBenchmarkShr = regexr(primaryprospectusbenchmark, "\+[0-9]\%", "")
C
foreach i of num 1/`maxIndices' {
	replace PrimaryProspectusBenchmarkCln = regexr(PrimaryProspectusBenchmarkCln, " [0-9]*\.[0-9]*% ", "")
}
replace PrimaryProspectusBenchmarkCln = regexr(PrimaryProspectusBenchmarkCln, " [0-9]*\.[0-9]*%", "")
split PrimaryProspectusBenchmarkCln, parse("+") limit(`maxIndices') gen(PrimaryProspectusBenchmarkCln_) 
foreach i of num 1/`maxIndices' {
	replace PrimaryProspectusBenchmarkShr = regexr(PrimaryProspectusBenchmarkShr, PrimaryProspectusBenchmarkCln_`i', "") if !missing(PrimaryProspectusBenchmarkCln_`i')
}
split PrimaryProspectusBenchmarkShr, parse(" + ") limit(`maxIndices') gen(PrimaryProspectusBenchmarkShr_) 
foreach i of num 1/`maxIndices' {
	replace PrimaryProspectusBenchmarkCln_`i' = strtrim(PrimaryProspectusBenchmarkCln_`i')
	replace PrimaryProspectusBenchmarkShr_`i' = strtrim(PrimaryProspectusBenchmarkShr_`i')
}
foreach i of num 1/`maxIndices' {
	destring PrimaryProspectusBenchmarkShr_`i', ignore("\%") replace force 
	replace PrimaryProspectusBenchmarkShr_`i'=0 if missing(PrimaryProspectusBenchmarkShr_`i')
}
gen NearestIndex = ""
gen NearestIndexShr = 0
replace PrimaryProspectusBenchmarkShr_1 = 100 if missing(PrimaryProspectusBenchmarkCln_2)
foreach i of num 1/`maxIndices' {
	replace NearestIndex = PrimaryProspectusBenchmarkCln_`i' if PrimaryProspectusBenchmarkShr_`i' > NearestIndexShr
	replace NearestIndexShr = PrimaryProspectusBenchmarkShr_`i' if PrimaryProspectusBenchmarkShr_`i' > NearestIndexShr
}
replace NearestIndexShr = 100 if missing(NearestIndex) * !missing(PrimaryProspectusBenchmarkCln)
replace NearestIndex = PrimaryProspectusBenchmarkCln if missing(NearestIndex)

drop PrimaryProspectusBenchmarkCln-PrimaryProspectusBenchmarkShr_`maxIndices'

mmerge SecId using "$output/morningstar_api_data/secid_all_monthly_flows.dta", type(1:1) unmatched(master) umatch(secid)
ren _merge mergeflows
save "$output/morningstar_api_data/Mapping_Plus_Static_Returns_Flows.dta", replace emptyok

/////////////////////////////////////
/// DETERMINE MOST IMPORTANT INDICES
/////////////////////////////////////

use NearestIndex ConvertedFundAUM using "$output/morningstar_api_data/Mapping_Plus_Static_Returns_Flows.dta", clear
drop if missing(NearestIndex)

bysort NearestIndex: gen N=_N
bysort NearestIndex: gen TotTrackingUSD = sum(ConvertedFundAUM)
bysort NearestIndex: keep if _n==_N
keep if N > 2
keep NearestIndex TotTrackingUSD N 
order NearestIndex TotTrackingUSD N 

rename NearestIndex Name
save "$temp/morningstar_api_data/keyIndices.dta", replace emptyok

/////////////////////////////////////
/// CLEAN AND MERGE MOST IMPORTANT INDICES, TIME SERIES
/////////////////////////////////////

import excel using "$raw/morningstar_direct_supplementary/all_indices_20180820.xlsx", sheet("Global_Indexes") firstrow clear
gen indexid=_n
tostring indexid, replace
replace indexid = "Idx" + indexid
preserve
keep Name-HedgingCurrency indexid
order indexid
save "$temp/morningstar_api_data/indicesdetails.dta", replace emptyok
restore
rename *USD *
keep indexid Name Monthly*
merge 1:1 Name using "$temp/morningstar_api_data/keyIndices.dta", keep(1 3) nogen
preserve
keep indexid Name N
save "$temp/morningstar_api_data/indicesKeyFile.dta", replace emptyok
restore
gen _varname = indexid
* now save a keyfile of indicesdetails and indexid
keep if !missing(N)
keep Monthly* _varname
xpose, clear
gen date = _n+tm(1992m12) // data is from 1993m1 onwards.
format date %tm
order date
drop if date > tm(2017m12)
save "$output/morningstar_api_data/indicestimeseries.dta", replace emptyok

/////////////////////////////////////
/// GET R-SQUARED TO NAMED BENCHMARK
/////////////////////////////////////

clear all
local rcc_max = 32000
local nreturns = 15000
local nloops = 3
set maxvar `rcc_max'
use "$output/morningstar_api_data/Mapping_Plus_Static_Returns_Flows.dta", clear
mmerge NearestIndex using "$temp/morningstar_api_data/indicesKeyFile.dta", type(n:1) unmatched(master) umatch(Name) ukeep(indexid)
drop _merge
save "$output/morningstar_api_data/Mapping_Plus_Static_Returns_Flows.dta", replace emptyok
foreach i of numlist 1/`nloops' {
	use "$output/morningstar_api_data/Mapping_Plus_Static_Returns_Flows.dta", clear
	keep SecId FundId monthly* indexid
	gen returns_n = 0
	foreach var of varlist monthly* {
		replace returns_n = returns_n+1 if !missing(`var')
	}
	keep if !missing(indexid) & returns_n > 11
	bysort FundId indexid (returns_n): keep if _n==_N
	describe
	local nrows = r(N)
	assert(`nrows' < (`nreturns' * `nloops')) // so that in future, we will be flagged if we need to include nloops if the number of funds grows a lot.
	keep if _n > (`i'-1)*`nreturns' & _n <=(`i')*`nreturns'
	gen _varname = SecId + "_" + indexid
	keep monthly* _varname
	xpose, clear promote
	gen date = _n+tm(2002m12) // data is from 1993m1 onwards.
	format date %tm
	drop if date > tm(2017m12)
	merge 1:1 date using "$output/morningstar_api_data/indicestimeseries.dta", keep(3)
	drop _merge
	expand 2 in l
	foreach var of varlist _all {
		replace `var' = . if _n==_N
	}
	foreach var of varlist F*_* {
		local namedidx = substr("`var'", strpos("`var'", "_") + 1, .)
		capture confirm variable `namedidx'
		if _rc==0 {
			disp "`var' on `namedidx'"
			cap quietly reg `var' `namedidx'
			if _rc==0 {
				replace `var' = `e(r2)' if _n==_N
			}
		}
	}
	keep if _n==_N
	keep F*
	xpose, clear v
	rename v1 Rsquare
	rename _varname SecIdToIndex
	split SecIdToIndex, parse("_") generate(part)
	drop SecIdToIndex
	rename (part1 part2) (SecId indexid)
	save "$temp/morningstar_api_data/SecId_To_Prospectus_Index_Rsq_pt`i'.dta", replace emptyok
}
clear

foreach i of numlist 1/3 {
	append using "$temp/morningstar_api_data/SecId_To_Prospectus_Index_Rsq_pt`i'.dta"
}
save "$output/morningstar_api_data/SecId_To_Prospectus_Index_Rsq.dta", replace emptyok


/////////////////////////////////////
/// BRING R-SQUARED BACK TO FUND INFORMATION
/////////////////////////////////////

use "$output/morningstar_api_data/Mapping_Plus_Static_Returns_Flows.dta", clear
merge 1:1 SecId using "$output/morningstar_api_data/SecId_To_Prospectus_Index_Rsq.dta", nogen

bysort MasterPortfolioId : egen Rsquare_mpid = mode(Rsquare), maxmode
gen tmp1 = 1 if Rsquare_mpid == Rsquare
replace tmp1 = 0 if tmp1==.
bysort MasterPortfolioId (tmp1) : gen NearestIndex_mpid = NearestIndex[_N]
bysort MasterPortfolioId (tmp1) : gen NearestIndexShr_mpid = NearestIndexShr[_N]
bysort MasterPortfolioId (tmp1) : gen indexid_mpid = indexid[_N]
drop tmp1

ds FundId, not
collapse (firstnm) `r(varlist)', by(FundId) fast
save "$output/morningstar_api_data/FundIdUnique_Mapping_Plus_Static_Returns_Flows.dta", replace
drop if missing(MasterPortfolioId)
drop FundId SecId
drop Rsquare NearestIndex NearestIndexShr indexid
rename (Rsquare_mpid NearestIndex_mpid NearestIndexShr_mpid indexid_mpid) (Rsquare NearestIndex NearestIndexShr indexid)
ds mon* est* MasterPortfolioId, not
foreach var in `r(varlist)' {
	local var_suffix = substr("`var'",1,30)
	bysort MasterPortfolioId: egen m_`var_suffix' = mode(`var'), maxmode
}
keep mon* est* MasterPortfolioId m_*
rename m_* *
ds mon* est*
foreach var in `r(varlist)' {
	bysort MasterPortfolioId (`var'): gen m_`var' = missing(`var'[1])
}
ds mon* est* m_*, not
collapse (sum) mon* est* (max) m_*, by(`r(varlist)') fast
foreach var of varlist m_* {
	local var_prefix = substr("`var'", 3, .)
	replace `var_prefix' = . if `var'==1
}
drop m_*

save "$output/morningstar_api_data/MPIDUnique_Mapping_Plus_Static_Returns_Flows.dta", replace

/////////////////////////////////////
/// NET INFLOWS, MERGEABLE FILE
/////////////////////////////////////

use "$output/morningstar_api_data/MPIDUnique_Mapping_Plus_Static_Returns_Flows.dta", clear
keep MasterPortfolioId estfundlevnet*
reshape long estfundlevnetflowcomp, i(MasterPortfolioId) j(Month)
tostring Month, replace
gen date_m = mofd(date(Month, "YM"))
format date_m %tm
drop Month
rename estfundlevnetflowcomp net_inflow
save "$output/morningstar_api_data/MPID_netinflow_M.dta", replace emptyok
gen date_q = qofd(dofm(date_m))
format date_q %tq
drop date_m
bysort MasterPortfolioId date_q: egen net_inflow_q = total(net_inflow)
drop net_inflow
rename net_inflow_q net_inflow
duplicates drop
save "$output/morningstar_api_data/MPID_netinflow_Q.dta", replace emptyok

/////////////////////////////////////
/// CUSIP-TO-MPID LINK TABLE
/////////////////////////////////////

* CUSIP links
use "$output/morningstar_api_data/Mapping_Plus_Static.dta", clear
rename CUSIP cusip
rename MasterPortfolioId investing_mpid
order cusip investing_mpid
duplicates drop cusip investing_mpid, force
sort cusip (investing_mpid)
drop if cusip == ".z_"
drop if missing(investing_mpid)
drop if cusip == "NULL" & ISIN == "NULL"
preserve
keep if cusip == "NULL"
save "$temp/morningstar_api_data/mf_isin_tmp1.dta", replace
restore
drop if cusip == "NULL"
by cusip: egen nMpid = nvals(investing_mpid)
preserve
keep if nMpid == 1
save "$temp/morningstar_api_data/mf_final_p1.dta", replace

* Merge the data with ISINs, step 1
use "$temp/cgs/allmaster_essentials_m.dta", clear
drop if isin==""
bysort isin: gen dup=_n
drop if dup>1
drop dup
save "$temp/cgs/allmaster_essentials_m_noblank.dta", replace

* Merge the data with ISINs, step 2
use "$temp/morningstar_api_data/mf_isin_tmp1.dta", clear
drop if ISIN=="ISIN"
rename ISIN isin
keep cusip isin investing_mpid
mmerge isin using "$temp/cgs/allmaster_essentials_m_noblank.dta", unmatched(m) ukeep(cusip) uname(u_)
drop if _merge == 1
drop cusip
rename u_cusip cusip
bysort cusip: egen nMpid = nvals(investing_mpid)
keep if nMpid == 1
keep if ~missing(cusip)
save "$temp/morningstar_api_data/mf_final_p2.dta", replace

* Append and save the cleaned data
use "$temp/morningstar_api_data/mf_final_p1.dta", clear
append using "$temp/morningstar_api_data/mf_final_p2.dta"
keep cusip investing_mpid
bysort cusip: egen nMpid = nvals(investing_mpid)
keep if nMpid == 1
drop nMpid
save "$output/morningstar_api_data/Morningstar_API_Data_Cleaned.dta", replace

log close
