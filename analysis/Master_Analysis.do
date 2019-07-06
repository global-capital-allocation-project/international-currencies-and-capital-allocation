* This file runs our analysis. It starts by installing relevant Stata packages, defining various directories and paths, and setting global macro values. It then calls .do files that run each step of our analysis, 
* passing along relevant arguments that it receives from the call in Master_Analysis_Controller.sh.

clear
version 14
set more off
set excelxlsxlargefile on

* Calls program that installs Stata packages, defines directories, and sets global macros
do Initialize_and_Macros.do

* Set various local macros to organize parameters passed into .do file calls below

	* Select month used for fixed-exchange-rate analysis
	local fixedfxmonth	=	"2005m12"
	* Drop (the incomplete) 2018q1 data from time-series analyses
	local datedrop_q_1 = "2018q1"
	* Set quarters in which to report summary statistics for Currency Shares
	local currsharetab_date1 = "2005q4"
	local currsharetab_date2 = "2008q4"
	local currsharetab_date3 = "2017q4"
	* Set dates to normalize the fixed effects in the FE regressions and establish weights for the Currency Share plots
	local date_fenorm_q = "2008q3"
	local datewt_q_1 = "2009q1"
	local datewt_q_2 = "2015q4"
	* Set date to start plots for the time-series analyses
	local plotstartdate_q = "2004q1"

************************************
* Below lines call all Analyses, passing the appropriate parameters as arguments. See actual .do files for more detailed description of what each .do file does.

if "`1'"=="Fund_Bias_Step1" {
	do $analysis/Fund_Bias_Step1.do `2'
}	

if "`1'"=="Fund_Bias_Step2" {
	* This Generates Plots Used in Figure 4 of Paper
	do $analysis/Fund_Bias_Step2.do
}

if "`1'"=="Fund_Bias_Step3" {
	* This Generates Regressions Results in Appendix Tables A.4 and A.5
	do $analysis/Fund_Bias_Step3.do `2'
}

if "`1'"=="Gen_HD_Sumstats" {
	* Create Summary Files from the Holding Detail Files for use in Aggregate Analyses
	do $analysis/Gen_HD_Sumstats.do `2' `fixedfxmonth'
}

if "`1'"=="Gen_Portfolio_Sumstat_Step1" {
	* Create Yearly Security-Level Summary Files from the Holding Detail Files for use in Security-Level Analyses
	do $analysis/Gen_Portfolio_Sumstat_Step1.do `2'
}

if "`1'"=="Gen_Portfolio_Sumstat_Step2" {
	* Aggregate the Yearly Security-Level Summary Files into Single one for use in Security-Level Analyses
	do $analysis/Gen_Portfolio_Sumstat_Step2.do
}

if "`1'"=="Data_Comparison_Plots" {
	* Create Plots Comparing Summary Files to Other Data Sources
	do $analysis/Compare_with_ICI.do
	do $analysis/Compare_with_TIC_NoCurrency.do
	do $analysis/Compare_with_TIC_Currency.do
	do $analysis/Compare_with_CPIS.do
}

if "`1'"=="CurrencyShare_TimeSeries" {
	* Generate Time Series of Key Currency Shares from Summary Files, and Compare to Equivalents in the BIS
	do $analysis/CurrencyShare_TimeSeries.do `datewt_q_1' `datewt_q_2' `date_fenorm_q' `plotstartdate_q' `datedrop_q_1' `datedrop_q_2' `datedrop_q_3' `datedrop_q_4'
	do $analysis/CurrencyShare_TimeSeries_Table.do `currsharetab_date1' `currsharetab_date2' `currsharetab_date3'
	do $analysis/BIS_TimeSeries.do
}

if "`1'"=="CurrencyShare_BarCharts" {
	* Generate Bar Charts of Key Currency Shares from Summary Files
	do $analysis/CurrencyShare_BarCharts.do
}

if "`1'"=="Probits" {
	* Run Probits for Tables Showing that Larger Firms Are More likely to Issue in Foreign Currency
	do $analysis/Probits.do
}

if "`1'"=="Portfolio_Conc_and_Dist" {
	* These Files Generate a Number of our Security- or Firm-Level Results
	do $analysis/CurrencyNumberAnalyses.do
	do $analysis/FirmBubbles.do
	do $analysis/LC_Firm_Sumstats.do
	do $analysis/Gen_Rank_Plots.do
	do $analysis/External_Curr_Share.do
}

if "`1'"=="Regressions" {
	* Analyze Portfolio-Level Home-Bias Regressions
	do $analysis/Regressions_Panel.do `2'
}

if "`1'"=="Debt_Equity_Plots" {
	* Generates Additional Plots Comparing Foreign And Domestic Holding of Debt vs. Equity of Same Firms
	do $analysis/debt_equity_plots.do
}	

