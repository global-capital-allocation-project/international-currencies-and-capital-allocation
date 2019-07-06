* --------------------------------------------------------------------------------------------------
* Cusip_Fill_Isin
*
* This file performs a series of data cleaning steps that improve the quality of security metadata
* in the HoldingDetail files produced by the build so far. First, we use ISIN identifiers to merge in
* information from the CGS security master file. We use these to refine the security-level information
* such as asset class categorization. Second, we merge in data using the identifiers recovered via
* the OpenFIGI/Bloomberg data pull.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Cusip_Fill_Isin_`1'", replace
local step2_vars_to_keep "isin cusip iso_country_code currency_id coupon maturitydate figi_mns_class figi_mns_subclass"
local year = `1' + $firstyear + - 1

foreach filetype in "NonUS" "US" {
	
	use  $output/HoldingDetail/`filetype'_`year'_m_step11, clear

	* Step 1: Merge with CGS master file; update cusip if cusip originally 
	mmerge externalid_mns using $externalid_temp/externalid_linking.dta, ukeep(cusip isin) update	
	drop if _merge==2
	ren _merge _merge_step_1
	capture confirm variable isin
	if !_rc {
		mmerge isin using "$tempcgs/allmaster_essentials_isin.dta", ukeep(cusip) update
		drop if _merge==2
		drop _merge 
	}
	
	* Reclassify type codes using ISIN/CUSIP
	mmerge cusip using "$tempcgs/allmaster_essentials_m.dta", uname(cgs_)
	drop if _merge==2
	cap replace isin=cgs_isin if _merge==3 & isin==.
	cap replace maturitydate = cgs_maturity_date if _merge==3 & maturitydate==.
	replace iso_co=cgs_dom if _merge==3 & iso_co=="" 
	replace currency_id=cgs_currency_code if _merge==3 & currency_id=="" 
	replace coupon=cgs_coupon_rate if _merge==3 & coupon=="" 
	gen cfi1=substr(cgs_iso_cfi,1,1)
	gen cfi2=substr(cgs_iso_cfi,2,1)
	replace mns_class="B" if _merge==3 & cfi1=="D" & (mns_class=="" | mns_class=="Q")
	replace mns_class="E" if _merge==3 & cfi1=="E" & (mns_class=="" | mns_class=="Q")
	replace mns_subclass="SH" if _merge==3 & cfi1=="E" & cfi2=="S" & mns_subclass=="" & mns_class=="E"
	replace mns_subclass="PR" if _merge==3 & cfi1=="E" & (cfi2=="P"| cfi2=="R" | cfi2=="F") & mns_subclass=="" & mns_class=="E"
	replace mns_subclass="E" if _merge==3 & cfi1=="E" & (cfi2=="C" | cfi2=="U" | cfi2=="M") & mns_subclass=="" & mns_class=="E"
	replace mns_subclass="B" if _merge==3 & cfi1=="B" & mns_subclass=="" & mns_class=="B"
	replace mns_subclass="S" if cgs_issuer_type=="S" & substr(cusip,1,6)=="260543"
	replace mns_class="B" if cgs_agency~=""
	replace mns_subclass="A" if cgs_agency~=""
	replace mns_subclass="SV" if cgs_domicile=="XSN" & _merge==3	
	drop cgs*
		
	* Step 2: Merge in the identifiers obtained via OpenFIGI/Bloomberg data pull
	mmerge externalid_mns using $externalid_temp/externalid_openfigi_bloomberg.dta, ukeep(`step2_vars_to_keep') update
	replace mns_subclass = figi_mns_subclass if (mns_class=="Q" & figi_mns_class != "" & figi_mns_subclass != "")
	replace mns_class = figi_mns_class if (mns_class=="Q" & figi_mns_class != "")
	drop figi_mns_class figi_mns_subclass
	drop if _merge==2
	ren _merge _merge2

	save $output/HoldingDetail/`filetype'_`year'_m_step15, replace
}

log close

