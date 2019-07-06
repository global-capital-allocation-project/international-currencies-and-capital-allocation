* This file runs at the beginning of Master_Analysis and does 3 things. It (1) installs various Stata packages, (2) Defines various directories and paths as global macros, and (3) defines various country groupings as macros.

* Install various Stata packages
cap ssc install isvar
cap ssc install checkfor2
cap ssc install outreg2
cap ssc install winsor
cap ssc install winsor2
cap ssc install estout
cap ssc install moreobs
cap ssc install labutil.pkg
cap ssc install carryforward
cap ssc install binscatter.pkg
cap ssc install winsor
cap net from http://www.stata.com
cap net cd users
cap net cd vwiggins
cap net install grc1leg
cap net install binscatter.pkg

* Define various directories and paths as global macros
global analysis "$mns_code/analysis"
global output "$mns_data/output"
global raw "$mns_data/raw"
global results "$mns_data/results"
global resultstemp "$mns_data/results/temp"
global temp "$mns_data/temp"
global cusip "$build/cusip"
global wrds "$raw/wrds"
global compustat "$temp/compustat"
global naics_merge "$temp/naics_merge"
global tempcgs "$temp/cgs"
global tempciq "$temp/ciq"
global tempcusip "$temp/cusip"
global temptic "$temp/tic_data"
global ticraw "$raw/TIC/RawData"
global ticinter "$raw/TIC/RawData/inter"
global dir_active "$raw/Stata/active"
global dir_inactive "$raw/Stata/inactive"
global logs "$results/logs"
global regressions "$resultstemp/regoutput_panel"
global weights "$resultstemp/weights"
global regressions_dta "$resultstemp/regs_for_tex_panel/`year'"
global firmgraphs $resultstemp/firmgraphs
global sdc "$mns_data/raw/SDC"
global sdc_bonds "$mns_data/raw/SDC/bonds"
global sdc_temp "$mns_data/temp/SDC"
global sdc_additional "$sdc_temp/additional"
global sdc_dta "$sdc_temp/dta"
global sdc_datasets "$sdc_temp/datasets"
global sdc_equities "$mns_data/raw/SDC/equities"
global sdc_eqdta "$sdc_temp/eqdta"
global sdc_loans "$mns_data/raw/SDC/loans"
global sdc_equities "$mns_data/raw/SDC/equities"
global sdc_loandta "$sdc_temp/loandta"
global sdc_cs_ws "$sdc_temp/sdc_cs_ws"
global sdc_results "$results/sdc"
global sdc_regressions "$sdc_results/regressions"
cap mkdir $logs
cap mkdir $temp/Int_Inv_Differences
cap mkdir $temp/Int_Inv_Differences/results
cap mkdir $temp/Int_Inv_Differences/graphs
cap mkdir $temp/fund_bias
cap mkdir $output/fuzzy
cap mkdir $temp/fuzzy
cap mkdir $compustat
cap mkdir $naics_merge
cap mkdir $tempcgs
cap mkdir $tempcusip
cap mkdir $output/PortfolioSummary
cap mkdir $temp/PortfolioSummary
cap mkdir $temp/ici_data
cap mkdir $temp/ici_data/graphs
cap mkdir $tempciq
cap mkdir $resultstemp
cap mkdir $resultstemp/graphs
cap mkdir $resultstemp/tables
cap mkdir $regressions
cap mkdir $regressions/fund_bias
cap mkdir $resultstemp/firmgraphs
cap mkdir $weights
cap mkdir $sdc_results
cap mkdir $sdc_regressions

* Define various country groupings as Macros

	* Below list defines Domiciles we use in our analyses
	* Note: To run analysis excluding EMU countries, delete EMU from ctygroupA1 AND ctygroupA_list
	global ctygroupA1 `""USA","EMU","GBR","CAN","CHE" "'
	global ctygroupA_list "AUS CAN CHE DNK EMU GBR NOR NZL USA SWE"
	global ctygroupA2 `""AUS","SWE","DNK","NOR","NZL" "'
	global ctygroupB_list "CAN EMU GBR USA"


	* Below lists are used for our Comparison with CPIS Data
	global country_group2	`" "CAN","CHE","CHL","DNK","GBR","NOR","SWE","USA", "EMU" "'
	global country_list2	"CAN CHE CHL DNK GBR MEX NOR SWE USA EMU"
	global eu_list  "LUX IRL ITA DEU FRA ESP GRC NLD AUT BEL FIN PRT CYP EST LAT LTU SVK SVN MLT EMU"

	* Below macros capture countries we include in Euro Zone
	* Note: These are a super-set of countries actually included since we lack data on some of the below
	global  eu1  `""ITA","DEU","FRA","ESP","GRC","NLD","AUT" "'
	global  eu2  `""BEL","FIN","PRT","CYP","EST","LAT","LTU","SVK","SVN" "'
	global  eu3  `""MLT","EMU","LUX","IRL" "'

	* Below macros capture countries we consider Tax Havens
	* Note: These are a super-set of countries actually included since we lack data on some of the below
	global tax_haven1 	`""BHS","CYM","COK","DMA","LIE","MHL","NRU","NIU","PAN" "'
	global tax_haven2 	`""KNA","VCT","BMU","CUW","JEY","BRB","MUS","VGB" "'
	global tax_haven3 	`""VIR","ATG","AND","AIA","ABW","BLZ","BRN","CPV","GIB" "'
	global tax_haven4 	`""GRD","DOM","GGY","IMN","MCO","MSR","PLW","VCT" "'
	global tax_haven5 	`""WSM","SMR","SYC","MAF","TTO","TCA","VUT","ANT","CHI" "'
	global tax_haven6 	`""GLP","IMY","MTQ","REU","SHN","TUV","WLF","HKG" "'

	* Below macros capture countries we drop due to data quality concerns
	global excluded_ctys 	`""IND","LIE","THV" "'

	* Below macros define country groups for regression analyses
	global  regdomkeep1  	`""CAN", "CHE", "CHL", "DNK" "' 
	global  regdomkeep2  	`""EMU", "GBR", "MEX", "NOR", "SWE", "USA" "' 
	global droplist_reg=""
		foreach x in $ctygroupA_list {
			if regexm("$ctygroupA_list","`x'")==0 {
			global droplist_reg "$droplist_reg `x'"
		}
	}

* Set cusip6_bg_name to "cusip6_bg2" or "cusip6" to include or exclude parent-match algorithm
* Set country_bg_name to "country_bg2" or "iso_country_code" to include or exclude parent-match algorithm
global cusip6_bg_name "cusip6_bg2"
global country_bg_name "country_bg2" 

* Define global macros to set parameters for use in all analyses
global firstyear 	= 	1986
global lastyear 	= 	2017

* Define threshold for foreign-currency probit regressions
global fc_thresh = 0.01
