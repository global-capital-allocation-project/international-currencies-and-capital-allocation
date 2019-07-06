* --------------------------------------------------------------------------------------------------
* Parse_Externalid
*
* Cleans the externalid field in the Morningstar holdings data; attempts to find externalid within 
* text in this field, and creates a new cleaned field called externalid_mns. The IDs contained in the
* externalid field are heterogeneous; we use the OpenFIGI API in order to use them to identify
* securities whenever a CUSIP is lacking.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_ParseExternalID_Array`1'", replace

local year = `1' + $firstyear + - 1

foreach filetype in "NonUS" "US" {
	use  $output/HoldingDetail/`filetype'_`year'_m_step1, clear

	capture confirm variable _externalid
	if !_rc {
		gen externalid_mns = upper(_externalid)
		replace externalid_mns = "" if length(externalid_mns) < 6

		replace externalid_mns = subinstr(externalid_mns, ",", " ", .)
		replace externalid_mns = subinstr(externalid_mns, char(34), "", .)
		replace externalid_mns = subinstr(externalid_mns, "#", "", .)
		replace externalid_mns = subinstr(externalid_mns, "'", "", .)
		replace externalid_mns = subinstr(externalid_mns, "$", "", .)
		replace externalid_mns = subinstr(externalid_mns, "!", "", .)
		replace externalid_mns = subinstr(externalid_mns, ";", "", .)
		replace externalid_mns = subinstr(externalid_mns, "&AMP", "", .)
		replace externalid_mns = subinstr(externalid_mns, "%", "", .)
		replace externalid_mns = subinstr(externalid_mns, "|", "", .)
		replace externalid_mns = subinstr(externalid_mns, "{", "", .)
		replace externalid_mns = subinstr(externalid_mns, "}", "", .)
		replace externalid_mns = itrim(externalid_mns)

		* Clean up the externalid by excluding redundant text that appears before/after space or slash or underscore etc.
		cap {
			split externalid_mns if regexm(externalid_mns, " ") | regexm(externalid_mns, "_") | regexm(externalid_mns, "/") | regexm(externalid_mns, "-"), 		parse(" " "_" "/" "-") limit(3) gen(externalid_mns_cln) 
			gen externalid_mns_cln_longest = ""
			forvalues i = 1/3 {
			  replace externalid_mns_cln_longest = externalid_mns_cln`i' if strlen(externalid_mns_cln`i')>strlen(externalid_mns_cln_longest)
			}
			replace externalid_mns = externalid_mns_cln_longest if externalid_mns_cln_longest != ""
			drop externalid_mns_cln*
		}
		replace externalid_mns = itrim(externalid_mns)
	}
	else {
		gen externalid_mns=""
	}	
	save $output/HoldingDetail/`filetype'_`year'_m_step11, replace
}

log close
