* --------------------------------------------------------------------------------------------------
* Make_Externalid_List
*
* This job consolidates the list of externalids to be sent to OpenFIGI via API.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_make_externalid_list", replace

* Load in the data
foreach year of numlist 1995/$lastyear {
	foreach filetype in "NonUS" "US" {
		cap append using $output/HoldingDetail/`filetype'_`year'_m_step11.dta, keep(externalid_mns securityname mns_class iso_country_code currency_id)
		keep if externalid_mns != "" & (mns_class=="Q" | iso_country_code=="" | currency_id=="")
	}
}

* Drop those which cannot be security identifiers
drop if length(externalid_mns) < 6 | length(externalid_mns) > 9 & length(externalid_mns) < 12 | ///
	length(externalid) > 12 & length(externalid) < 18 | length(externalid) > 18

* Drop duplicates
bysort externalid_mns: gen first_record = _n == 1 
keep if first_record
drop first_record

* Drop obvious candidates since openfigi API search is slow due to server limits
drop if regexm(externalid_mns, "&QUOTB")
drop if regexm(externalid_mns, "AU.ASX")
drop if regexm(externalid_mns, "BANK")
drop if regexm(externalid_mns, "BLANK")
drop if regexm(externalid_mns, "BOND")
drop if regexm(externalid_mns, "BUY")
drop if regexm(externalid_mns, "CALL")
drop if regexm(externalid_mns, "CASH")
drop if regexm(externalid_mns, "CCS")
drop if regexm(externalid_mns, "COMDTY")
drop if regexm(externalid_mns, "COMMER")
drop if regexm(externalid_mns, "CURRE")
drop if regexm(externalid_mns, "DEBT")
drop if regexm(externalid_mns, "EQUITY")
drop if regexm(externalid_mns, "FOREIGN")
drop if regexm(externalid_mns, "FORWARD")
drop if regexm(externalid_mns, "FUTURE")
drop if regexm(externalid_mns, "ILLIQ")
drop if regexm(externalid_mns, "INCOME")
drop if regexm(externalid_mns, "INTER")
drop if regexm(externalid_mns, "INVCERT")
drop if regexm(externalid_mns, "INVESTMENT")
drop if regexm(externalid_mns, "IRS")
drop if regexm(externalid_mns, "LOAN")
drop if regexm(externalid_mns, "MBSTBA")
drop if regexm(externalid_mns, "MORTGAGE")
drop if regexm(externalid_mns, "MUNI")
drop if regexm(externalid_mns, "MUNICIPAL")
drop if regexm(externalid_mns, "MUNISEC")
drop if regexm(externalid_mns, "MUTUAL")
drop if regexm(externalid_mns, "NOTE")
drop if regexm(externalid_mns, "OPTION")
drop if regexm(externalid_mns, "OTHERS")
drop if regexm(externalid_mns, "PRIV")
drop if regexm(externalid_mns, "PUT")
drop if regexm(externalid_mns, "QUOTE")
drop if regexm(externalid_mns, "SECUR")
drop if regexm(externalid_mns, "SELL")
drop if regexm(externalid_mns, "SHARES")
drop if regexm(externalid_mns, "SHORT")
drop if regexm(externalid_mns, "SOVEREIGN")
drop if regexm(externalid_mns, "SWAP")
drop if regexm(externalid_mns, "TBILLS")
drop if regexm(externalid_mns, "VARIABLE")
drop if regexm(externalid_mns, "WARRANT")
drop if regexm(externalid_mns, "^([0-9])\1*$") // this matches 111111, 0000000 etc.

drop if regexm(upper(securityname), "REPO") 
drop if regexm(upper(securityname), "REPURCHASE") 
drop if regexm(upper(securityname), "INTER")
drop if regexm(upper(securityname), "LOAN")
drop if regexm(upper(securityname), "CCS")
drop if regexm(upper(securityname), "CALL")
drop if regexm(upper(securityname), "PUT")
drop if regexm(upper(securityname), "IRS")
drop if regexm(upper(securityname), "CASH")
drop if regexm(upper(securityname), "SECUR")
drop if regexm(upper(securityname), "COMMER")
drop if regexm(upper(securityname), "PRIV")
drop if regexm(upper(securityname), "CURRE")
drop if regexm(upper(securityname), "ILLIQ")
drop if regexm(upper(securityname), "WARRANT")
drop if regexm(upper(securityname), "BUY")
drop if regexm(upper(securityname), "SELL")
drop if regexm(upper(securityname), "SWAP")
drop if regexm(upper(securityname), "CBT")
drop if regexm(upper(securityname), "FUT")
drop if regexm(upper(securityname), "OPTION")
drop if regexm(upper(securityname), "DEPOSIT")
drop if regexm(upper(securityname), "S&AMP")
drop if regexm(upper(securityname), "CDX")
drop if regexm(upper(securityname), "CDS")
drop if regexm(upper(securityname), "OIS")
drop if regexm(upper(securityname), "INDEX")
drop if regexm(upper(securityname), "LIBOR")
drop if regexm(upper(securityname), "FX")

drop if regexm(externalid_mns, "\.")
drop if regexm(externalid_mns, "\*")
drop if regexm(externalid_mns, "\&")
drop if regexm(externalid_mns, "\^")
drop if regexm(externalid_mns, "\?")
drop if regexm(externalid_mns, "\+")
drop if regexm(externalid_mns, "\|")
drop if regexm(externalid_mns, "\(")
drop if regexm(externalid_mns, "\)")

*Any special characters in securityname can cause issues for csv import
gen clnSecurityName = "" 
gen length = length(securityname) 
su length, meanonly 

forval i = 1/`r(max)' { 
     local char substr(securityname, `i', 1) 
     local OK inrange(`char', "a", "z") | inrange(`char', "A", "Z") | inrange(`char', "0", "9") 
     replace clnSecurityName = clnSecurityName + `char' if `OK' 
}

*Write to file
sort externalid_mns
outsheet externalid_mns clnSecurityName mns_class iso_country_code currency_id using $externalid_temp/externalids_to_api.csv, comma replace

log close
