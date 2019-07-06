* --------------------------------------------------------------------------------------------------
* CGS_AI_Build
*
* This job generates the CGS associated issuer mapping file used in the CMNS ultimate parent
* aggregation algorithm. This is simply a compact version of the AI file processed in cgs_import.do.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_cgs_ai_build", replace

* Create the AI file for use in UP aggregation
use issuer_link issuer_num action_type_1 using "$temp/cgs/AIMASTER.dta", clear
rename issuer_num issuer_number
drop if issuer_link=="" | issuer_num==""
drop if issuer_link==issuer_num
drop if regexm(action_type_1,"Copyright 2016")==1
bysort issuer_num: gen n=_n
drop if n>1
drop n action
rename issuer_link ai_parent_issuer_num
mmerge ai_parent_issuer_num using "$temp/cgs/cgs_compact_complete.dta", umatch(issuer_num) uname("ai_parent_")
drop if _merge==2
drop _merge
save "$temp/cgs/ai_master_for_up_aggregation.dta", replace

log close
