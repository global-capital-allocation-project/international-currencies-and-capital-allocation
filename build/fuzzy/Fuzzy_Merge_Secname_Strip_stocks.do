* --------------------------------------------------------------------------------------------------
* Fuzzy Merge: Security Name Cleaning, Stocks
*
* All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier. This allows us to assign a CUSIP to the former records via internal
* cross-linkage.
*
* This file parses and cleans the security name field for equity records and extracts relevant info.
* --------------------------------------------------------------------------------------------------

* no double dashes (the double dashes decrease match quality spuriously, since only some report this way)
gen securityname_cln = subinstr(securityname, "--", "", .)

* performs a systematic match to find digits that directly proceed a percentage to find a coupon, extract them
gen coup_in_name = regexs(0) if(regexm(securityname_cln, "[0-9]*[\.]*[0-9]*%"))
replace securityname_cln = subinstr(securityname_cln, coup_in_name, "", .)
replace coup_in_name = subinstr(coup_in_name, "%", "", .)
*destring coup_in_name, force replace
replace coupon = coup_in_name if coupon~=coup_in_name & coup_in_name~=""
*replace coupon = coup_in_name if coupon=="" & coup_in_name!=""

* match on dates (first full, then month-year or year-month, then yyyymmdd or yyyyddmm) and extract
gen date_in_name = regexs(0) if(regexm(securityname_cln, "[0-9]*[/-][0-9]*[/-][0-9]*"))
gen date_in_name_mthyr = regexs(0) if(regexm(securityname_cln, "[0-9]*[/-][0-9]*"))
gen date_in_name_yr = regexs(0) if(regexm(securityname_cln, "[1-2][0129][0-9][0-9]"))
gen date_rawdigits = regexs(0) if(regexm(securityname_cln, "[1-2][0129][0-9][0-9][0-3][0-9][0-3][0-9]"))

replace date_in_name_mthyr = "" if (date_in_name_mthyr == "/" | date_in_name_mthyr == "-" | regexm(date_in_name_mthyr, "^[/-]|[/-]$"))
destring date_in_name_yr, replace
replace date_in_name_yr = . if (date_in_name_yr < 1975 | date_in_name_yr > 2250)

* extract in actual date format from the matched dates
replace date_in_name = date_in_name_mthyr if date_in_name=="" & date_in_name_mthyr!=""
gen date_in_name_mthyr1 = regexs(0) if regexm(date_in_name_mthyr, "[1-2][0129][0-9][0-9][/-][0-9]*")
gen year = substr(date_in_name_mthyr1,1,4) if date_in_name_mthyr1!=""
gen month = substr(date_in_name_mthyr1,6,.) if date_in_name_mthyr1!=""

gen date_in_name_mthyr2 = regexs(0) if regexm(date_in_name_mthyr, "[0-3][0-9][/-][0-9][0-9]")
gen twodigityear = substr(date_in_name_mthyr2,4,2)
destring twodigityear, replace
replace year = "19" + substr(date_in_name_mthyr2,4,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name_mthyr2!=""
replace year = "20" + substr(date_in_name_mthyr2,4,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name_mthyr2!=""
drop twodigityear
replace month = substr(date_in_name_mthyr2,1,2) if month=="" & date_in_name_mthyr2!=""

gen date_in_name_mthyr3 = regexs(0) if regexm(date_in_name_mthyr, "[0-9][/-][0-9][0-9]")
gen twodigityear = substr(date_in_name_mthyr3,3,2)
destring twodigityear, replace
replace year = "19" + substr(date_in_name_mthyr3,3,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name_mthyr3!=""
replace year = "20" + substr(date_in_name_mthyr3,3,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name_mthyr3!=""
drop twodigityear
replace month = substr(date_in_name_mthyr3,1,1) if month=="" & date_in_name_mthyr3!=""

gen date_in_name_mthyr4 = regexs(0) if regexm(date_in_name_mthyr, "[0-9][0-9][-][0-9]")
gen twodigityear = substr(date_in_name_mthyr4,1,2)
destring twodigityear, replace
replace year = "19" + substr(date_in_name_mthyr4,1,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name_mthyr4!=""
replace year = "20" + substr(date_in_name_mthyr4,1,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name_mthyr4!=""
drop twodigityear
replace month = substr(date_in_name_mthyr3,4,1) if month=="" & date_in_name_mthyr4!=""

replace month = "" if date_in_name!=""
replace year = "" if date_in_name!=""

gen date_in_name1 = regexs(0) if regexm(date_in_name, "[1-2][0129][0-9][0-9][/-][0-3][0-9][/-][0-3][0-9]")
replace year = substr(date_in_name1,1,4) if year=="" & date_in_name1!=""
replace month = substr(date_in_name1,6,2) if month=="" & date_in_name1!=""
gen day = substr(date_in_name1,9,2) if date_in_name1!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name1, "", .)

gen date_in_name2 = regexs(0) if regexm(date_in_name, "[1-2][0129][0-9][0-9][/-][0-3][0-9][/-][0-9]")
replace year = substr(date_in_name2,1,4) if year=="" & date_in_name2!=""
replace month = substr(date_in_name2,6,2) if month=="" & date_in_name2!=""
replace day = substr(date_in_name2,9,1) if day=="" & date_in_name2!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name2, "", .)

gen date_in_name3 = regexs(0) if regexm(date_in_name, "[1-2][0129][0-9][0-9][/-][0-9][/-][0-3][0-9]")
replace year = substr(date_in_name3,1,4) if year=="" & date_in_name3!=""
replace month = substr(date_in_name3,6,1) if month=="" & date_in_name3!=""
replace day = substr(date_in_name3,8,2) if day=="" & date_in_name3!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name3, "", .)

gen date_in_name4 = regexs(0) if regexm(date_in_name, "[1-2][0129][0-9][0-9][/-][0-9][/-][0-9]")
replace year = substr(date_in_name4,1,4) if year=="" & date_in_name4!=""
replace month = substr(date_in_name4,6,1) if month=="" & date_in_name4!=""
replace day = substr(date_in_name4,8,1) if day=="" & date_in_name4!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name4, "", .)

gen date_in_name5 = regexs(0) if regexm(date_in_name, "[0-3][0-9][/-][0-3][0-9][/-][0-9][0-9]")
gen twodigityear = substr(date_in_name5,7,2)
destring twodigityear, force replace
replace year = "19" + substr(date_in_name5,7,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name5!=""
replace year = "20" + substr(date_in_name5,7,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name5!=""
drop twodigityear
replace month = substr(date_in_name5,1,2) if month=="" & date_in_name5!=""
replace day = substr(date_in_name5,4,2) if day=="" & date_in_name5!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name5, "", .)

gen date_in_name6 = regexs(0) if regexm(date_in_name, "[0-3][0-9][/-][0-9][/-][0-9][0-9]")
gen twodigityear = substr(date_in_name5,6,2)
destring twodigityear, force replace
replace year = "19" + substr(date_in_name6,6,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name6!=""
replace year = "20" + substr(date_in_name6,6,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name6!=""
drop twodigityear
replace month = substr(date_in_name6,1,2) if month=="" & date_in_name6!=""
replace day = substr(date_in_name6,4,1) if day=="" & date_in_name6!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name6, "", .)

gen date_in_name7 = regexs(0) if regexm(date_in_name, "[0-9][/-][0-3][0-9][/-][0-9][0-9]")
gen twodigityear = substr(date_in_name7,6,2)
destring twodigityear, force replace
replace year = "19" + substr(date_in_name7,6,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name7!=""
replace year = "20" + substr(date_in_name7,6,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name7!=""
drop twodigityear
replace month = substr(date_in_name7,1,1) if month=="" & date_in_name7!=""
replace day = substr(date_in_name7,3,2) if day=="" & date_in_name7!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name7, "", .)

gen date_in_name8 = regexs(0) if regexm(date_in_name, "[0-9][/-][0-9][/-][0-9][0-9]")
gen twodigityear = substr(date_in_name8,5,2)
destring twodigityear, force replace
replace year = "19" + substr(date_in_name8,5,2) if twodigityear >= 86 & twodigityear!=. & year=="" & date_in_name8!=""
replace year = "20" + substr(date_in_name8,5,2) if twodigityear < 86 & twodigityear!=. & year=="" & date_in_name8!=""
drop twodigityear
replace month = substr(date_in_name8,1,1) if month=="" & date_in_name8!=""
replace day = substr(date_in_name8,3,1) if day=="" & date_in_name8!=""
replace securityname_cln = subinstr(securityname_cln, date_in_name8, "", .)

replace day = "15" if month!="" & year!="" & day==""

destring month day year, replace
gen maturitydate_in_name = mdy(month, day, year)
replace maturitydate = maturitydate_in_name if maturitydate==. & maturitydate_in_name!=.

* clean up parseing fields

drop date_in_*
drop coup_in_name - maturitydate_in_name

* special character stripping

replace securityname_cln = subinstr(securityname_cln, "amp;", " ", .)
replace securityname_cln = subinstr(securityname_cln, "([Wts/Rts])", " ", .)
replace securityname_cln = subinstr(securityname_cln, ":", " ", .)
replace securityname_cln = subinstr(securityname_cln, ";", " ", .)
replace securityname_cln = subinstr(securityname_cln, ",", " ", .)
replace securityname_cln = subinstr(securityname_cln, ".", " ", .)
replace securityname_cln = subinstr(securityname_cln, "#", " ", .)
replace securityname_cln = subinstr(securityname_cln, "@", " ", .)
replace securityname_cln = subinstr(securityname_cln, "(", " ", .)
replace securityname_cln = subinstr(securityname_cln, ")", " ", .)
replace securityname_cln = subinstr(securityname_cln, "[", " ", .)
replace securityname_cln = subinstr(securityname_cln, "]", " ", .)
replace securityname_cln = subinstr(securityname_cln, "&", " & ", .)

replace securityname_cln = upper(securityname_cln)
replace securityname_cln = itrim(securityname_cln)

* categorical stripping from names (reduce spurious matches due to presence of "Bond", "Treasury" and "Corp" etc.)
* separated into separate lines because Stata has a limit on brackets in Regex commands, and to ensure systematic ordering of strips

gen name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )INC($| )")
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )CORP($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )LTD($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )AUTH($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )CO($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )GROUP($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )GRP($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )HOLDINGS($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )HLDGS($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )PLC($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )AG($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )SPA($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )SA($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )AB($| )") & name_firm_categ==""
replace name_firm_categ = trim(regexs(0)) if regexm(securityname_cln, "(^| )NV($| )") & name_firm_categ==""
gen name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )BOND($| )|(^| )NOTE($| )|(^| )BILL($| )|(^| )FRN($| )")
replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )CMO($| )") & name_bond_type==""
replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )ZCB($| )") & name_bond_type==""
replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )ADR($| )") & name_bond_type==""
replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )GDR($| )") & name_bond_type==""
replace name_bond_type = trim(regexs(0)) if regexm(securityname_cln, "(^| )NTS($| )|(^| )MTG($| )|(^| )(TERM LOAN)($| )") & name_bond_type==""
gen name_bond_legal = trim(regexs(0)) if regexm(securityname_cln, "(^| )144A($| )")

* strip the extracted words from clean name
replace securityname_cln = subinstr(securityname_cln, name_firm_categ, "", .)
replace securityname_cln = subinstr(securityname_cln, name_bond_type, "", .)
replace securityname_cln = subinstr(securityname_cln, name_bond_legal, "", .)

* collapse categories of firms
replace name_firm_categ = "GROUP" if regexm(name_firm_categ, "GROUP|GRP")
replace name_firm_categ = "HOLDINGS" if regexm(name_firm_categ, "HOLDINGS|HLDGS")

* final standardisation
replace securityname_cln = itrim(securityname_cln)
replace securityname_cln = trim(securityname_cln)

gen name_jurisdict_categ=""
