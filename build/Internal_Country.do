* --------------------------------------------------------------------------------------------------
* Internal_Country
*
* This jobs finds the modal country code assigned to each CUSIP in the Morningstar data. We look for
* modal country assignments within funds and then across funds.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Internal_Country", replace
clear

* Append monthly files and keep only relevant variables
foreach holdingname in "NonUS" "US" { 
	forvalues x=$firstyear/$lastyear {
		display "$output/HoldingDetail/`holdingname'_`x'_m_step3.dta"
		append using "$output/HoldingDetail/`holdingname'_`x'_m_step3.dta", keep(cusip6 iso_co MasterPo)
	} 
}
save "$temp/NonUS_US.dta", replace

* Perform the country assignement
local test=0
local app=""
if `test'==1 {
	local app=" if _n<=50000"
}
use "$temp/NonUS_US.dta"`app', clear
drop if cusip6=="" | iso_co==""
replace iso_co="ANT" if iso_co=="AN"
replace iso_co="SRB" if iso_co=="CS"
replace iso_co="FXX" if iso_co=="FX"
replace iso_co="XSN" if iso_co=="S2"
replace iso_co="XSN" if iso_co=="XS"
replace iso_co="YUG" if iso_co=="YU"
replace iso_co="ZAR" if iso_co=="ZR"

* Find fund-specific mode country assigned to each cusip
gen counter = 1 if !missing(iso_country_code)
bysort cusip6 iso_co MasterPort: egen country_fund_count=sum(counter)
drop counter
collapse (firstnm) country_fund_count, by(cusip6 iso_co MasterPort)
bysort cusip6 MasterPort: egen country_fund_count_max=max(country_fund_count)
drop if country_fund_count<country_fund_count_max

* Split the equally frequent

* Generate an indicator for FP (takes value 1 if at least one country is not a FP) 
gen country_fund_nfp=1
forvalues j=1(1)10 {
		capture replace country_fund_nfp=0 if (inlist(iso_country_code,${tax_haven`j'}))
}
bysort cusip6 MasterPort: egen country_fund_count_nfp=sum(country_fund_nfp)

* Focus on those that have 2 or more countries in the mode
gen counter = 1 if !missing(iso_country_code)
bysort cusip6 MasterPort: egen country_fund_count_split=sum(counter)
drop counter

forvalues j=1(1)10 {
		capture drop if country_fund_count_split>=2 & country_fund_count_nfp>=1 &  (inlist(iso_country_code,${tax_haven`j'}))
}
bysort cusip6 MasterPort: gen fp_rand=runiform()
bysort cusip6 MasterPort: egen fp_rand_max=max(fp_rand)
drop if country_fund_count_split>=2 & fp_rand<fp_rand_max

* Find mode country assigned to each cusip across funds
gen counter = 1 if !missing(iso_country_code)
bysort cusip6 iso_co: egen country_count=sum(counter)
drop counter
collapse (firstnm) country_count, by(cusip6 iso_co)

* Rank the frequency of each country by cusip. Rank 1 are the most frequently assigned countries. Ties are all assigned the same rank
bysort cusip6: egen country_count_rank=rank(-country_count), track
drop if country_count_rank>2

gen country_cusip_nfp=1
forvalues j=1(1)10 {
		capture replace country_cusip_nfp=0 if (inlist(iso_country_code,${tax_haven`j'}))
}
bysort cusip6: egen country_cusip_count_nfp=sum(country_cusip_nfp)

* Only fiscal paradises: we choose the mode, and at random within the mode 
drop if country_cusip_count_nfp==0 & country_count_rank==2
bysort cusip6: gen fp_rand=runiform()
bysort cusip6: egen fp_rand_max=max(fp_rand)
drop if country_cusip_count_nfp==0 & country_count_rank==1 & fp_rand<fp_rand_max

* Mixed or only regular countries: we choose the mode if regular country, or the rank 2 if rank 1 only has FPs. If indifferent, we pick at random within the same rank.
forvalues j=1(1)10 {
		capture drop if country_cusip_count_nfp>=1 & (inlist(iso_country_code,${tax_haven`j'}))
}
bysort cusip6: egen country_count_rank_temp=rank(country_count_rank), track
drop if country_cusip_count_nfp>=1 & country_count_rank_temp>=2
drop fp_rand_max
bysort cusip6: egen fp_rand_max=max(fp_rand)
drop if country_cusip_count_nfp>=1 & country_count_rank_temp==1 & fp_rand<fp_rand_max
keep cusip6 iso_co

save "$temp/Internal_Country_NonUS_US.dta", replace
log close
