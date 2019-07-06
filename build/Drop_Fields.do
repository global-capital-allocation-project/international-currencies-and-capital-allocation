* --------------------------------------------------------------------------------------------------
* Drop_Field
*
* This code reads in the semi-raw (i.e. directly after inputting from .xml to .dta) Morningstar 
* holdings data files and drops unnecessary variables.
* --------------------------------------------------------------------------------------------------
cap log close
log using "$logs/${whoami}_Drop_Fields_Array`1'", replace
local year = `1' + $firstyear + - 1

local PortfolioSummaryVars `"portfolio_ordinal portfoliosummary_ordinal _masterportfolioid _currencyid date previousportfoliodate netexpenseratio numberofholdingshort numberofstockholdingshort numberofbondholdingshort totalmarketvalueshort numberofholdinglong numberofstockholdinglong numberofbondholdinglong totalmarketvaluelong"'
local HoldingDetailVars `"portfolio_ordinal portfoliosummary_ordinal holding_ordinal _masterportfolioid _currencyid date previousportfoliodate netexpenseratio _id _detailholdingtypeid _storageid _externalid externalname country country_id cusip sedol isin ticker currency currency_id securityname localname weighting numberofshare sharepercentage numberofjointlyownedshare marketvalue costbasis sharechange sector maturitydate accruedinterest coupon creditquality duration industryid globalindustryid localcurrencycode localmarketvalue paymenttype rule144aeligible altmintaxeligible bloombergticker isoexchangeid contractsize secondarysectorid companyid firstboughtdate underlyingsecid underlyingsecurityname performanceid lessthanoneyearbond surveyedweighting globalsector gicsindustryid"'

foreach filetype in "NonUS" "US" {
	foreach fundtype in "FO" "FM" "FE" {
		foreach atype in "Active" "Inactive" {
			foreach mtype in "" "_NonMonthEnd" {
				foreach month in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12" {
					foreach whichfile in "PortfolioSummary" "HoldingDetail" {
						display "`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'"
						capture fs "$dir_xmlupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`whichfile'X_*.dta"			
						if _rc==0 {
							di "found $dir_xmlupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`whichfile'X_*.dta"
							foreach file in `r(files)' {
								di "making directory for above file"
								cap mkdir "$dir_stataupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'"
								use ``whichfile'Vars' using "$dir_xmlupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'", clear
								save "$dir_stataupdate/`filetype'_`fundtype'_`atype'`mtype'_`year'-`month'/`file'", replace
							}
						}
					}
				}				
			}
		}
	}
}
