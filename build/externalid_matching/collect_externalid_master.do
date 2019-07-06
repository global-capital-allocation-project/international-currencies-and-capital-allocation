* --------------------------------------------------------------------------------------------------
* Collect_Externalid_Master
*
* This job creates an internal flatfile which has all security-level details for each externalid. The
* data is populated using information internal to the HoldingDetail files. This job is continued in
* the file make_externalid_master.R
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_collect_externalid_master", replace

local vars_to_keep "externalid_mns MasterPortfolioId isin cusip securityname mns_class mns_subclass date_m iso_country_code currency_id coupon maturitydate"

foreach year of numlist $firstyear/$lastyear {
	foreach filetype in "NonUS" "US" {
		cap append using $output/HoldingDetail/`filetype'_`year'_m_step15.dta, keep(`vars_to_keep')
		keep if externalid_mns != ""
	}
}

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

* Can dump externalids that have no numbers in them (never truly are identifiers)
gen nums = real(regexs(1)) if regexm(externalid_mns,"([0-9]+)")
drop if nums == .
drop nums

gen length = length(externalid_mns)
drop if length < 6 | length > 18

egen nunique_portid = nvals(MasterPortfolioId), by(externalid_mns)

drop date_m
drop MasterPortfolioId

local vars_to_match "externalid_mns isin cusip mns_class mns_subclass securityname cusip isin iso_country_code currency_id coupon maturitydate"

bysort externalid_mns: gen num_externalid = _N
drop if num_externalid == 1
bysort `vars_to_match': gen num_records = _N if _n == _N
keep if !missing(num_records)

local vars_to_match_noname "externalid_mns isin cusip mns_class mns_subclass cusip isin iso_country_code currency_id coupon maturitydate"

bysort `vars_to_match_noname': gen num_records_intermed = sum(num_records)
bysort `vars_to_match_noname': gen num_records_exclname = num_records_intermed[_N]
drop num_records_intermed

gsort -nunique_portid externalid_mns -num_records_exclname -num_records

save $externalid_temp/extid_records_allyears_summary.dta, replace
log close

