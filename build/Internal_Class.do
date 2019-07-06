* --------------------------------------------------------------------------------------------------
* Internal_Class
*
* This jobs finds the modal typecode assigned to each CUSIP in the Morningstar data. We look for
* modal typecode assignments within funds and then across funds.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Internal_Class", replace
clear

* Append monthly files and keep only relevant variables
clear
foreach holdingname in "NonUS" "US" { 
	forvalues x=$firstyear/$lastyear {
		display "$output/HoldingDetail/`holdingname'_`x'_m_step3.dta"
		append using "$output/HoldingDetail/`holdingname'_`x'_m_step3.dta", keep(cusip mns_class mns_subclass MasterPo)
	} 
}
save "$temp/Class_NonUS_US.dta", replace

* Perform the country assignement
local test=0
local app=""
if `test'==1 {
	local app=" if _n<=50000"
}	

use "$temp/Class_NonUS_US.dta"`app', clear
drop if cusip=="" | (mns_class == "" & mns_subclass=="")
drop if cusip == "000000000"
gen mns_category = mns_class + "-" + mns_subclass

* Find fund-specific modal category assigned to each cusip
gen counter = 1 if !missing(mns_category)
bysort cusip mns_category MasterPort: egen class_fund_count=sum(counter)
drop counter
collapse (firstnm) class_fund_count, by(cusip mns_category MasterPort mns_class mns_subclass)
bysort cusip MasterPort: egen class_fund_count_max=max(class_fund_count)
drop if class_fund_count<class_fund_count_max

* If the conflict is due to a missing subclass for which we have information, keep the observation that has information
gen counter = 1 if !missing(mns_category)
bysort cusip MasterPort: egen category_fund_count_split=sum(counter)
gen all_categories = mns_category
bysort cusip MasterPort (mns_category): replace all_categories = all_categories + ", " + all_categories[_n-1] if _n>1 & mns_category != mns_category[_n-1]
bysort cusip MasterPort (mns_category): replace all_categories = all_categories[_N]
gen drop_instance = 0
foreach category in "B" "E" "L" "D" "A" {
	replace drop_instance = 1 if regexm(all_categories, "`category'-, `category'-[A-Z]") & missing(mns_subclass) & category_fund_count_split == 2 
	replace drop_instance = 1 if regexm(all_categories, "`category'-[A-Z], `category'-") & missing(mns_subclass) & category_fund_count_split == 2 
	replace drop_instance = 1 if regexm(all_categories, "`category'-, `category'-[A-Z][A-Z]") & missing(mns_subclass) & category_fund_count_split == 2 
	replace drop_instance = 1 if regexm(all_categories, "`category'-[A-Z][A-Z], `category'-") & missing(mns_subclass) & category_fund_count_split == 2 
}
drop if drop_instance == 1
drop drop_instance

* Choose one at random if there are still multiple modes
drop counter category_fund_count_split
gen counter = 1 if !missing(mns_category)
bysort cusip MasterPort: egen category_fund_count_split=sum(counter)
bysort cusip MasterPort: gen fp_rand=runiform()
bysort cusip MasterPort: egen fp_rand_max=max(fp_rand)
drop if category_fund_count_split>=2 & fp_rand<fp_rand_max

* Find modal category assigned to each cusip across funds
cap drop counter
gen counter = 1 if !missing(mns_category)
bysort cusip mns_category: egen category_count=sum(counter)
drop counter
collapse (firstnm) category_count, by(cusip mns_category mns_class mns_subclass)

* Drop blank subclass instances if there is a corresponding instance with a filled-in subclass
bysort cusip: gen num_category_for_cusip = _N
gen all_categories = mns_category
bysort cusip (mns_category): replace all_categories = all_categories + ", " + all_categories[_n-1] if _n>1 & mns_category != mns_category[_n-1]
bysort cusip (mns_category): replace all_categories = all_categories[_N]
replace all_categories = all_categories + ","
cap drop drop_instance
gen drop_instance = 0
foreach category in "B" "E" "L" "D" "A" {
	replace drop_instance = 1 if num_category_for_cusip > 1 & regexm(all_categories, "`category'-,") & regexm(all_categories, "`category'-[A-Z]") & missing(mns_subclass) & mns_class == "`category'"
}
drop if drop_instance == 1
drop all_categories num_category_for_cusip drop_instance

* Rank the frequency of each category by cusip. Rank 1 are the most frequently assigned categories. Ties are all assigned the same rank
bysort cusip: egen category_count_rank=rank(-category_count), track
drop if category_count_rank>=2
bysort cusip: gen fp_rand=runiform()
bysort cusip: egen fp_rand_max=max(fp_rand)
drop if fp_rand<fp_rand_max
keep cusip mns_category mns_class mns_subclass

save "$temp/Internal_Class_NonUS_US.dta", replace
log close
