* ---------------------------------------------------------------------------------------------------------------------------------------------------------
* Analysis files for Maggiori, Neiman, and Schreger: "International Currencies and Capital Allocation", forthcoming (2019) in Journal of Political Economy
* ---------------------------------------------------------------------------------------------------------------------------------------------------------

I. INTRODUCTION

The purpose of this file is to give an overview of the Master_Analysis.sh file, which must be run after Master_Build.sh and which then generates all results (i.e. all tables and figures) in the paper. 
Our paper has 6 tables and 11 figures. They are generated from the following .do files called by Master_Analysis.do:

Table 1:        Calculated separately, no .do file
Tables 2-4:     Regressions_Panel.do
Table 5:        Probits.do
Table 6:        CurrencyShare_TimeSeries_Table.do
Figure 1:       Compare_with_ICI.do
Figure 2:       CurrencyShare_BarCharts.do
Figure 3:       External_Curr_Share.do
Figure 4:       Fund_Bias_Step2.do
Figure 5:       FirmBubbles.do
Figure 6:       CurrencyNumberAnalyses.do
Figures 7-9:    Gen_Rank_Plots.do
Figures 10-11:  CurrencyShare_TimeSeries.do

Each .do file includes a description at the top giving a broad overview of the file's purpose.

* --------------------------------------------------------------------------------------------------

II. EXECUTING THE ANALYSIS

As described in the overall README.txt, the bash script Master_Analysis.sh is the main executable file for the analysis. Launching Master_Analysis.sh 
will run the full analysis, building largely on the output of the build. Each of the steps of the analysis are outlined in the file, 
with short descriptions of their functions.

This file should be called as: ./Master_Analysis.sh

Unlike the build, there is no username required as an parameter when calling this file, but such an adjustment could be easily made.

* --------------------------------------------------------------------------------------------------

III. TECHNICAL NOTES AND INPUT DATA

The Build must be run before the Analysis can be performed. The key input files needed to run the analysis, many of which are generated in the Build (and therefore are in the "output" subdirectory, though some are in temp), include are listed below. The definitions of the global macros (those starting with "$" and needed to find the full path, can be found in "Initialize_and_Macros.do").

Inputs for Analysis come from (i) the Build, (ii) public or publicly available sources (ICI, TIC, CPIS, BIS, Concordances), and (iii)  private providers that sell the data (SDC, CGS, Dealogic, Compustat, CapitalIQ, and Factset). More information about their provenance and format, including in many cases the files themselves, are found in the "MNS_Data_Guide". The input files for our analysis are:

1) $output/HoldingDetail/HD_`yr'_`freq'.dta, where `yr' includes values for each year in our data and where `freq' can be either "m", "q", or "y".
2) $output/morningstar_api_data/Mapping_Plus_Static.dta
3) $output/concordances/country_currencies
4) $output/industry/Industry_classifications_master
5) $output/concordances/country_names
6) $output/ici_data/NonUS_ICI_sumstats_q (and _y)
7) $output/ici_data/US_ICI_sumstats_q (and _y)
8) $temp/ici_data/ICI_US_ETF_AUM_Factbook_2018_Consolidated.dta
9) $output/tic_data/tic_agg_inward.dta
10) $output/tic_data/tic_agg_outward.dta
11) $output/tic_data/disagg_tic_long.dta
12) $temp/CPIS/CPIS_bulk.dta
13) $raw/macro/Concordances/md4stata_code_list.dta
14) $raw/BIS/WEBSTATS_DEBTSEC_DATAFLOW_csv_col.csv
15) $temp/BIS/BIS_bulk.dta
16) $sdc_additional/cgs_isin.dta
17) $raw/segment/wrds_ws_segments.dta
18) $sdc_additional/cgs_isin_unique.dta
19) $temp/country_master/up_aggregation_final_compact.dta
20) $sdc_datasets/sdc_appended.dta
21) $temp/dealogic/dlg_bonds_consolidated.dta
22) $raw/dealogic/stata/companysiccodes
23) $temp/dealogic/dlg_bonds_consolidated.dta
24) $sdc_datasets/sdc_sic.dta
25) $temp/probits/factset_fx_2017
26) $raw/wrds/Compustat/cs_northam_feb19
27) $raw/wrds/Compustat/cs_global_feb19
28) $raw/ciq/wrds_cusip
29) $sdc_datasets/sdc_governing_law




