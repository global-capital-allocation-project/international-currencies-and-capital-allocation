* --------------------------------------------------------------------------------------------------
* Externalid_Merge
*
* This job merges information from the internally-generated externalid master file into the holdings
* data. The resulting HoldingDetail files are referred to as "step 1.6" files.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_externalid_merge`1'", replace

local externid_to_keep "isin cusip iso_country_code currency_id coupon maturitydate mns_class mns_subclass"

local year = `1' + $firstyear + - 1

foreach filetype in "NonUS" "US"  {
	use $output/HoldingDetail/`filetype'_`year'_m_step15, clear
	count
	if `r(N)'>60000000 & "`2'"=="brent" {
		keep if _n<=60000000
		mmerge externalid_mns using $externalid_temp/extid_master.dta, ukeep(`externid_to_keep') uname(temp_)
		drop if _merge==2
		foreach var in `externid_to_keep' {
			display "`var'"
			replace `var'=temp_`var' if missing(`var')==1
		}
		replace mns_subclass = temp_mns_subclass if (mns_class=="Q" & temp_mns_class != "" & temp_mns_subclass != "")
		replace mns_class = temp_mns_class if (mns_class=="Q" & temp_mns_class != "")
		ren _merge _merge3		
		save $output/HoldingDetail/`filetype'_`year'_m_step16, replace
		use $output/HoldingDetail/`filetype'_`year'_m_step15, clear
		keep if _n>60000000
		mmerge externalid_mns using $externalid_temp/extid_master.dta, ukeep(`externid_to_keep') uname(temp_)
		drop if _merge==2
		foreach var in `externid_to_keep' {
			display "`var'"
			replace `var'=temp_`var' if missing(`var')==1
		}
		replace mns_subclass = temp_mns_subclass if (mns_class=="Q" & temp_mns_class != "" & temp_mns_subclass != "")
		replace mns_class = temp_mns_class if (mns_class=="Q" & temp_mns_class != "")
		ren _merge _merge3		
		append using $output/HoldingDetail/`filetype'_`year'_m_step16
		compress	
		save $output/HoldingDetail/`filetype'_`year'_m_step16, replace
	}
	mmerge externalid_mns using $externalid_temp/extid_master.dta, ukeep(`externid_to_keep') uname(temp_)
	drop if _merge==2
	foreach var in `externid_to_keep' {
		display "`var'"
		replace `var'=temp_`var' if missing(`var')==1
	}
	replace mns_subclass = temp_mns_subclass if (mns_class=="Q" & temp_mns_class != "" & temp_mns_subclass != "")
	replace mns_class = temp_mns_class if (mns_class=="Q" & temp_mns_class != "")
	ren _merge _merge3		
	save $output/HoldingDetail/`filetype'_`year'_m_step16, replace
}
	
log close
