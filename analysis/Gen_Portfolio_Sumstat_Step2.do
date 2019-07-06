* Gen_Portfolio_Sumstat_Step1.do takes as an input the files HD_`yr'_m.dta that were created in the build and saves them as portfolio_sumstat_`year'_y, where `year' takes on the year's value. 
* It is run in parallel, separately for each year. Much like the HD_foranalysis files that are created as an output from Gen_HD_Sumstats.do, this file is used for all
* subsequent analysis, but unlike HD_foranalysis files, it is used for those that are security-level (such as our regressions), rather than country level. It therefore
* collapses as much of the data as possible (see the "keep" on line 17) while preserving what we need in those analyses. After generating these files for each year in parallel, we run
* Gen_Portfolio_Sumstat_Step2.do to aggregate these into one file called portfolio_sumstat_y.dta.

cap log close
log using "$logs/${whoami}_Gen_Portfolio_Sumstat_Step2", replace

clear
forvalues j=$firstyear(1)$lastyear {
	cap append using $resultstemp/portfolio_sumstat_`j'_y
}
save $resultstemp/portfolio_sumstat_y, replace

log close
