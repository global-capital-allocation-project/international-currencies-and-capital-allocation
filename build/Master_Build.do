* --------------------------------------------------------------------------------------------------
* Master_Build
*
* This master file runs all jobs in the build other than those that are executed in Python. This file
* is called directly by Master_Build.sh, and sets the appropriate Stata environment for the project,
* before launching the individual jobs. . Each of the steps of the build are outlined below, with short
* descriptions of their functions.
*
* Notes:
*	- In order to set the Stata environment, this file assumes that the following global variables
*	  have been set in your profile.do file, so that these are automatically defined in Stata:
*			$whoami: Your username
*			$mns_code: Path to the build code on the host system
*			$mns_data: Path to the folder containing the data, in which the build is executed
* --------------------------------------------------------------------------------------------------
clear
version 14
set more off
set excelxlsxlargefile on

* Install all required packages from SSC
cap ssc install carryforward
cap ssc install distinct
cap ssc install egenmore
cap ssc install freduse
cap ssc install fs
cap ssc install ivreg2
cap ssc install missings
cap ssc install mmerge
cap ssc install stcmd
cap ssc install sxpose
cap ssc install tabout
cap ssc install unique
cap ssc install labutil
cap ssc install rsource
cap ssc install margeff
cap ssc install wbopendata
cap ssc install winsor
cap ssc install moremata
cap ssc install ftools
cap ssc install carryforward

* Install all required packages from the web
local github "https://raw.githubusercontent.com"
cap net from http://www-personal.umich.edu/~nwasi/programs
cap net install reclink2
cap net install stnd_compname
cap net install stnd_address
cap net install clrevmatch
cap net install gtools, from(`github'/mcaceresb/stata-gtools/master/build/)

* Set global paths
global build "$mns_code/build"
global output "$mns_data/output"
global raw "$mns_data/raw"
global results "$mns_data/results"
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
global dir_xmlupdate "$raw/morningstar_ftp_master/sas"
global dir_stataupdate "$raw/Stata_update"
global dir_active "$raw/Stata/active"
global dir_inactive "$raw/Stata/inactive"
global logs "$results/logs"
global ciq_industry "$temp/ciq_industry"
global tempindustry "$temp/industry"
global master_country_datasets "$temp/country_master"
global orbis_ownership "$raw/orbis/ownership_data"
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
global externalid_raw "$raw/externalid"
global externalid_temp "$temp/externalid"

* The following is the list of tax havens throughout the build, defined as
* in Coppola et al. (2019)
global tax_haven1 	`""BHS","CYM","COK","DMA","LIE","MHL","NRU","NIU","PAN" "'
global tax_haven2 	`""KNA","VCT","BMU","CUW","JEY","BRB","MUS","VGB" "'
global tax_haven3 	`""VIR","ATG","AND","AIA","ABW","BLZ","BRN","CPV","GIB" "'
global tax_haven4 	`""GRD","DOM","GGY","IMN","MCO","MSR","PLW","VCT" "'
global tax_haven5 	`""WSM","SMR","SYC","MAF","TTO","TCA","VUT","ANT","CHI" "'
global tax_haven6 	`""GLP","IMY","MTQ","REU","SHN","TUV","WLF","HKG" "'

* The following is a list of EU member countries (as of 2018)
global  eu1  `""LUX","IRL","ITA","DEU","FRA","ESP","GRC","NLD","AUT" "'
global  eu2  `""BEL","FIN","PRT","CYP","EST","LAT","LTU","SVK","SVN" "'
global  eu3  `""MLT","EMU" "'

* These globals specify the beginning and end year for the build
global firstyear = 1986
global lastyear = 2018

* Create necessary output folders
cap mkdir $externalid_raw
cap mkdir $externalid_temp
cap mkdir $output
cap mkdir $output/fuzzy
cap mkdir $output/PortfolioSummary
cap mkdir $output/ER_data
cap mkdir $output/macro_data
cap mkdir $output/concordances
cap mkdir $output/ici_data
cap mkdir $output/industry
cap mkdir $output/tic_data
cap mkdir $output/oecd_data
cap mkdir $output/morningstar_api_data
cap mkdir $output/HoldingDetail
cap mkdir $output/CPIS_data
cap mkdir $output/morningstar_direct_supplementary
cap mkdir $output/flows_data
cap mkdir $output/factset

* Create necessary temporary folders
cap mkdir $temp
cap mkdir $temp/industry
cap mkdir $temp/ciq_industry
cap mkdir $temp/fuzzy
cap mkdir $temp/PortfolioSummary
cap mkdir $temp/ici_data
cap mkdir $temp/ici_data/graphs
cap mkdir $temp/ER_data
cap mkdir $temp/macro_data
cap mkdir $temp/tic_data
cap mkdir $temp/morningstar_api_data
cap mkdir $temp/HoldingDetail
cap mkdir $temp/mf_unwinding
cap mkdir $temp/mf_unwinding/tmp_hd_files
cap mkdir $temp/orbis
cap mkdir $temp/orbis/country
cap mkdir $temp/orbis/compact
cap mkdir $temp/orbis/full_net
cap mkdir $temp/dealogic
cap mkdir $temp/etc
cap mkdir $temp/country_master
cap mkdir $temp/CPIS
cap mkdir $temp/flows_data
cap mkdir $temp/factset

* Create remaining folders
cap mkdir $results
cap mkdir $logs
cap mkdir $compustat
cap mkdir $naics_merge
cap mkdir $tempcgs
cap mkdir $tempcusip
cap mkdir $tempciq
cap mkdir $dir_stataupdate
cap mkdir $sdc 
cap mkdir $sdc_bonds
cap mkdir $sdc_temp
cap mkdir $sdc_additional 
cap mkdir $sdc_dta 
cap mkdir $sdc_datasets 
cap mkdir $sdc_eqdta 
cap mkdir $sdc_equities
cap mkdir $sdc_loans 
cap mkdir $sdc_loandta
cap mkdir $sdc_cs_ws
cap mkdir $sdc_results
cap mkdir $sdc_regressions

* Clear the contents of the output and temporary folders
if "`2'"=="Get_Started" {
	do $build/Erase_Temp_Folder_Contents.do
	do $build/Erase_Output_Folder_Contents.do
}

* Drop unnecessary fields from the extracted raw Morningstar holdings data 
if "`2'"=="Drop_Fields" {
	do $build/Drop_Fields.do `3'
}

* Clean and build macroeconomic data (e.g. exchange rates) from various sources
if "`2'"=="Macro_Build_Step1" {
	do $build/macro/Macro_Build_Step1.do `3'
}

* Clean and build macroeconomic data (e.g. exchange rates) from various sources
if "`2'"=="Macro_Build_Step2" {
	do $build/macro/Macro_Build_Step2.do
}

* Build the ownership data from Bureau van Dijk's ORBIS database
if "`2'"=="Orbis_Build_Step1" {
	do $build/orbis/Orbis_Build_Step1.do
}

* Build the ownership data from Bureau van Dijk's ORBIS database
if "`2'"=="Orbis_Build_Step2" {
	do $build/orbis/Orbis_Build_Step2.do `3'
}

* Build the ownership data from Bureau van Dijk's ORBIS database
if "`2'"=="Orbis_Build_Step3" {
	do $build/orbis/Orbis_Build_Step3.do
}

* Build issue-level data from Dealogic
if "`2'"=="Dealogic_Build" {
	do $build/dealogic/Dealogic_Build.do
}

* Read in Treasury International Capital (TIC) data, ICI data, CPIS data, and OECD data
if "`2'"=="Public_Portfolios_Build" {
	do $build/public_portfolios/TIC_Build.do
	do $build/public_portfolios/ICI_Build.do
	do $build/public_portfolios/CPIS_Build.do
	do $build/public_portfolios/OECD_build.do
}

* This job builds the security and issuer master files from CUSIP Global Services (CGS)
if "`2'"=="Cusip_Build" {
	do $cusip/cgs_import.do
}

* Build accompanying metadata (especially fund-level information) from Morningstar
if "`2'"=="Morningstar_Build" {
	do $build/Morningstar_API_Build.do
}

* Generate clean dataset with portfolio summary data
if "`2'"=="PortfolioSummary_Build" {
	do $build/PortfolioSummary_Build.do
}

* Read in the HoldingDetail files, clean and append them, and then merge with API and FX data 
if "`2'"=="HoldingDetail_Build" {
	do $build/HoldingDetail_Build.do `3'
}

* Clean and parse the externalid field in the Morningstar holdings data; this will
* be used in conjunction with the OpenFIGI API in order to identify securities for
* which we are otherwise lacking identifiers
if "`2'"=="parse_externalid" {
	do $build/externalid_matching/parse_externalid.do `3'
}

* Steps related to OpenFIGI/Bloomberg identifier data download
if "`2'"=="externalid" {

	* Consolidate the list of externalids to be sent to OpenFIGI via API
	do $build/externalid_matching/make_externalid_list.do
	
	* Query identifiers via the OpenFIGI API
	!module load R; R CMD BATCH --no-save --no-restore '--args tempdir="$temp/externalid" rawdir="$raw/externalid"' $build/externalid_matching/figi_api.R $logs/figi_api.out

	* The build now involves a manual step using Bloomberg. Instructions are as follows:"
	* 1. Check list of FIGIs; 2. Pull any new ones from BBG; 3. Add additional rows to $raw/bbg_figi.csv (do NOT use excel, use a text editor)

	* Turn the CSV file into a DTA file and merge it with the externalid list
	do $build/externalid_matching/merge_openfigi_bbg.do
	
	* Create a flat file that links each externalid to a CUSIP/ISIN, i.e. make keyfile from externalid to CUSIP/ISIN master files
	do $build/externalid_matching/match_externalid_flatfiles.do
	
}

* Perform a series of data cleaning steps that improve the quality of security metadata by merging in information
* from the CGS security master files and the OpenFIGI/Bloomberg data pull
if "`2'"=="Data_Improvement" {
	do $build/Cusip_Fill_Isin.do `3'
}

* Steps related to the generation of and internal flatfile with externalid details
if "`2'"=="externalid_make" {

	* Create an internal flatfile which has all security-level details for each externalid: Step 1
	do $build/externalid_matching/collect_externalid_master.do

	* Create an internal flatfile which has all security-level details for each externalid: Step 2
	!module load R/3.4.2-fasrc01; module load R_core/3.4.2-fasrc01; module load R_packages/3.4.2-fasrc01; R CMD BATCH --no-save --no-restore '--args $temp/externalid' $build/externalid_matching/make_externalid_master.R $logs/make_externalid_master.out

	* Convert the externalid internal file to DTA
	do $build/externalid_matching/make_externalid_csvtodta.do
}

* Merge information from the internally-generated externalid master file into the holdings data
if "`2'"=="externalid_merge" {
	do $build/externalid_matching/externalid_merge.do `3' `1'
}

* The 'fuzzy merge' steps below handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier.

* Fuzzy merge: Clean and organize the security records for which a CUSIP is available
if "`2'"=="Fuzzy_Merge_Good_Data_Step11" {
	do $build/fuzzy/Fuzzy_Merge_Good_Data_Step11.do `3'
}

* Fuzzy merge: Clean and organize the security records for which a CUSIP is available
if "`2'"=="Fuzzy_Merge_Good_Data_Step12" {
	do $build/fuzzy/Fuzzy_Merge_Good_Data_Step12.do
}

* Fuzzy merge: Clean and organize the security records for which a CUSIP is not available
if "`2'"=="Fuzzy_Merge_Bad_Data_Step11" {
	do $build/fuzzy/Fuzzy_Merge_Bad_Data_Step11.do `3'
}

* Fuzzy merge: Clean and organize the security records for which a CUSIP is not available
if "`2'"=="Fuzzy_Merge_Bad_Data_Step12" {
	do $build/fuzzy/Fuzzy_Merge_Bad_Data_Step12.do
}

* Fuzzy merge: Build an integrated dataset from the matched subsamples obtained via 
* the probabilistic record linkage
if "`2'"=="Fuzzy_Merge_Step2" {
	local fuzzyarraynum = substr("`3'",strpos("`3'","-") + 1,.)
	do $build/fuzzy/Fuzzy_Merge_Step2.do `fuzzyarraynum'
}

* Fuzzy merge: Reintroduce the probabilistic matches into the HoldingDetail files
if "`2'"=="Fuzzy_Merge_Step3" {
	do $build/fuzzy/Fuzzy_Merge_Step3.do `3'
}

* Construct a dataset with the modal currency assignments for each security within
* the Morningstar holdings data
if "`2'"=="Currency" {
	do $build/Internal_Currency.do
}

* Merge in security-level data from the CUSIP Global Services (CGS) master files 
* into the holdings data
if "`2'"=="Cusip_HoldingDetail_Merge" {
	do $build/Cusip_HoldingDetail_Merge.do `3'
}

* This step runs a series of data builds that prepare data used in the 
* ultimate parent aggregation procedure
if "`2'"=="Country_Prelim" {

	* Find the modal country code assigned to each CUSIP in the Morningstar data
	do $build/Internal_Country.do
	
	* Build Capital IQ data
	do $build/ciq/ciq_build.do
	
	* Build SDC Platinum data, bonds and equities
	do $build/sdc/sdc_build.do

	* Build SDC Platinum data, loans
	do $build/sdc/sdc_loans.do

	* Build SDC Platinum data, industry classifications
	do $build/sdc/sdc_industry.do

	* Build CGS Associated Issuers (AI) file, for use in aggregation
	do $build/cusip/cgs_ai_build.do
}

* Merge in the ultimate-parent and country assignments from the CMNS 
* ultimate-parent aggregation procedure
if "`2'"=="Country_Merge" {
	do $build/Country_Merge.do `3'
}

* Perform manual corrections which address outliers and mistaken reporting 
* in the raw Morningstar data
if "`2'"=="Manual_Corrections" {
	do $build/Final_Manual_Corrections.do
}

* The 'Unwind_MF' steps above handle the unwinding of funds' positions in other funds. 
* This procedure is referred to as fund-in-fund unwinding.

* Prepare some temporary files used by the fund-in-fund unwinding code
if "`2'"=="Prepare_MF_Unwinding" {
	do $build/unwind/Prepare_MF_Unwinding.do `3'
}

* Consolidate adjustment factors used to compute appropriately re-scaled versions of 
* the positions involved in the fund-in-fund unwinding.
if "`2'"=="Unwind_MF_Positions_Step15" {
	do $build/unwind/Unwind_MF_Positions_Step15.do `3'
}

* Re-generate holding detail data reflecting the unraveling and rescaling of fund-in-fund positions
if "`2'"=="Unwind_MF_Positions_Step2" {
	do $build/unwind/Unwind_MF_Positions_Step2.do `3'
}

* Re-generate holding detail data reflecting the unraveling and rescaling of fund-in-fund positions
if "`2'"=="Unwind_MF_Positions_Step3" {
	do $build/unwind/Unwind_MF_Positions_Step3.do `3'
}

* Aggregate the fund-in-fund unraveled data to a yearly frequency
if "`2'"=="Unwind_MF_Positions_Step4" {
	do $build/unwind/Unwind_MF_Positions_Step4.do `3'
}

* Generate the final "HD" (Holding Detail) files that are used in the analysis
if "`2'"=="Create_Final_Files" {
	do $build/Create_Final_Files.do `1' `3'
}

* Compile datasets with company-level industry assignments from both Morningstar and external sources
if "`2'"=="Industry" {
	do $build/External_Industry.do 
	do $build/Internal_Industry.do
}
