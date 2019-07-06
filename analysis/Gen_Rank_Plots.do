* This file reads in portfolio_sumstat_y and generates figures comparing the share of a company's bonds in domestic versus foreign portfolio, firm-by-firm. It also separates these analyses by firms that issue in local currency and those that issue also in foreign currency.
* The plots are used in the paper in Figures 7 and 8. We also repeat the exercise separately, industry-by-industry, which are in our Appendix Figures A.14-A.18.

cap log close
log using "$logs/${whoami}_Gen_Rank_Plots", replace

use externalname cusip6 using $resultstemp/portfolio_sumstat_y, clear
collapse (firstnm) externalname, by(cusip6)
save $resultstemp/cusip6names, replace

foreach destinations in "USA" "EMU" "GBR" "CAN" {
	if "`destinations'"=="USA" {
		local destcur = "USD"
		local destlab = "U.S."
	}
	if "`destinations'"=="EMU" {
		local destcur = "EUR"
		local destlab = "European"
	}
	if "`destinations'"=="GBR" {
		local destcur = "GBP"
		local destlab = "British"
	}
	if "`destinations'"=="CAN" {
		local destcur = "CAD"
		local destlab = "Canadian"
	}
	use firm_type DomicileCountryId iso_country_code mns_class mns_subclass currency_id marketvalue_usd cusip6 date_y using $resultstemp/portfolio_sumstat_y, clear
	drop if missing(currency_id) | missing(cusip6)
	keep if iso_country_code=="`destinations'"
	keep if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS")
	replace DomicileCountryId="ROW" if DomicileCountryId != iso_country_code
	replace currency_id = "FCU" if currency_id!="`destcur'"
	collapse (sum) mv=marketvalue_usd (lastnm) firm_type, by(cusip6 DomicileCountryId currency_id date_y)
	bys date_y DomicileCountryId: egen porttot = sum(mv)
	gen portshare = mv/porttot
	drop porttot mv
	reshape wide portshare, i(cusip6 firm_type currency_id date_y) j(DomicileCountryId) string
	reshape wide portshareROW portshare`destinations', i(cusip6 date_y firm_type) j(currency_id) string
	replace portshare`destinations'`destcur' = 0 if portshare`destinations'`destcur'==.
	replace portshare`destinations'FCU = 0 if portshare`destinations'FCU==.
	replace portshareROW`destcur' = 0 if portshareROW`destcur'==.
	replace portshareROWFCU = 0 if portshareROWFCU==.
	gen domshare = portshare`destinations'`destcur'+portshare`destinations'FCU
	gen forshare = portshareROWFCU+portshareROW`destcur'
	gsort date_y -domshare -forshare
	drop if forshare==0 & domshare==0
	bys date_y: gen rank = _n
	replace portshare`destinations'`destcur' = . if portshare`destinations'`destcur'==0
	replace portshare`destinations'FCU = . if portshare`destinations'FCU==0
	replace portshareROW`destcur' = . if portshareROW`destcur'==0
	replace portshareROWFCU = . if portshareROWFCU==0
	replace domshare = . if domshare==0
	replace forshare = . if forshare==0
	merge m:1 cusip6 using $resultstemp/cusip6names, nogen keep(1 3)
	gen y1dom = .
	gen y1for = .
	gen x1 = 80
	gen x2 = 25

	forvalues plotyear=2005(1)2017 {
		sum rank if date_y==`plotyear'
		local xmax = min(100*ceil(`r(max)'/100),600)

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear', xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") ytitle("") title("All `destinations' Issuers")				
		graph export $resultstemp/graphs/Rank_All_`destinations'_ML_`plotyear'.eps, as(eps) replace

		sum domshare if rank==1 & date_y==`plotyear'
		local ydom = `r(mean)'
		sum domshare if rank==1 & date_y==`plotyear'
		replace y1dom = `r(mean)'
		sum forshare if rank==1 & date_y==`plotyear'
		replace y1for = `r(mean)'
		sum forshare if rank==1 & date_y==`plotyear'
		local yfor = `r(mean)'
		
		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear', ytitle("Share of Portfolio") text(`yfor' 165 "Share of foreign portfolio", size(vsmall) color(blue)) text(`ydom' 169 "Share of domestic portfolio", size(vsmall) color(red)) xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1) order(1 2)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing")  title("All Issuers") || pcarrow y1dom x1 y1dom x2, mc(black) mfc(black) mlc(black) || pcarrow y1for x1 y1for x2, mfc(black) mlc(black) mc(black) 			
		graph save $resultstemp/graphs/Rank_All_`destinations'_ML_`plotyear'.gph, replace
		
		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & portshare`destinations'FCU==. & portshareROWFCU==. , ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") 				
		graph export $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_`plotyear'.eps, as(eps) replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & portshare`destinations'FCU==. & portshareROWFCU==. , ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") title("Only Local Currency Issuers")				
		graph save $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_`plotyear'0.gph, replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & portshare`destinations'FCU==. & portshareROWFCU==. , ytitle("Share of Portfolio") title("Debt") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") 			
		graph save $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_`plotyear'1.gph, replace

		grc1leg $resultstemp/graphs/Rank_All_`destinations'_ML_`plotyear'.gph $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_`plotyear'0.gph, legendfrom($resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_`plotyear'0.gph) span position(6) rows(1) graphregion(color(white)) 
		graph export $resultstemp/graphs/Rank_2plots_`destinations'_ML_`plotyear'.eps, as(eps) replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==1900 , ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") title(" ")
		graph save $resultstemp/graphs/Rank_blank.gph, replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & portshare`destinations'FCU==. & portshareROWFCU==. , ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing")  title("`destinations'")				
		graph save $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_`plotyear'.gph, replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & firm_type==1, ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") title("All `destinations' Financial Issuers")			
		graph export $resultstemp/graphs/Rank_All_`destinations'_ML_Fin_`plotyear'.eps, as(eps) replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & firm_type==1 & portshare`destinations'FCU==. & portshareROWFCU==. , ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") 				
		graph export $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_Fin_`plotyear'.eps, as(eps) replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & firm_type==2, ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing")  title("All `destinations' Non-Financial Issuers")			
		graph export $resultstemp/graphs/Rank_All_`destinations'_ML_NonFin_`plotyear'.eps, as(eps) replace

		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & firm_type==2 & portshare`destinations'FCU==. & portshareROWFCU==. , ytitle("Share of Portfolio") xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") 				
		graph export $resultstemp/graphs/Rank_LocalOnly_`destinations'_ML_NonFin_`plotyear'.eps, as(eps) replace
	}
	keep if portshare`destinations'FCU==. & portshareROWFCU==. 
	keep cusip6 rank date_y
	duplicates drop cusip6 date_y, force
	save $resultstemp/rankdata_`destinations'_ML, replace
}

forvalues plotyear = 2005(1)2017 {
	grc1leg $resultstemp/graphs/Rank_LocalOnly_CAN_ML_`plotyear'.gph $resultstemp/graphs/Rank_LocalOnly_EMU_ML_`plotyear'.gph $resultstemp/graphs/Rank_LocalOnly_GBR_ML_`plotyear'.gph $resultstemp/graphs/Rank_LocalOnly_USA_ML_`plotyear'.gph, legendfrom($resultstemp/graphs/Rank_LocalOnly_USA_ML_`plotyear'.gph) rows(2) position(6) span graphregion(color(white)) title("Local Currency Only Issuers")
	graph export $resultstemp/graphs/Rank_LocalOnly_4cty_ML_`plotyear'.eps, as(eps) replace

	grc1leg $resultstemp/graphs/Rank_LocalOnly_CAN_ML_`plotyear'.gph $resultstemp/graphs/Rank_LocalOnly_EMU_ML_`plotyear'.gph $resultstemp/graphs/Rank_LocalOnly_GBR_ML_`plotyear'.gph , legendfrom($resultstemp/graphs/Rank_LocalOnly_CAN_ML_`plotyear'.gph) rows(2) position(6) span graphregion(color(white)) title("Local Currency Only Issuers")
	graph export $resultstemp/graphs/Rank_LocalOnly_3cty_ML_`plotyear'.eps, as(eps) replace
}
grc1leg $resultstemp/graphs/Rank_LocalOnly_EMU_ML_2017.gph $resultstemp/graphs/Rank_blank.gph , l1title("Portfolio Shares") legendfrom($resultstemp/graphs/Rank_LocalOnly_EMU_ML_2017.gph) rows(1) position(6) span graphregion(color(white)) title("Local Currency Only Issuers")
graph export $resultstemp/graphs/Rank_LocalOnly_2cty_ML_intro1.eps, as(eps) replace
grc1leg $resultstemp/graphs/Rank_LocalOnly_EMU_ML_2017.gph $resultstemp/graphs/Rank_LocalOnly_USA_ML_2017.gph , l1title("Portfolio Shares") legendfrom($resultstemp/graphs/Rank_LocalOnly_USA_ML_2017.gph) rows(1) position(6) span graphregion(color(white)) title("Local Currency Only Issuers")
graph export $resultstemp/graphs/Rank_LocalOnly_2cty_ML_intro2.eps, as(eps) replace

*Below repeats analysis at industry level

local plotyear = 2017
foreach destinations in "USA" "EMU" "GBR" "CAN" {

	if "`destinations'"=="USA" {
		local destcur = "USD"
		local destlab = "U.S."
	}
	if "`destinations'"=="EMU" {
		local destcur = "EUR"
		local destlab = "European"
	}
	if "`destinations'"=="GBR" {
		local destcur = "GBP"
		local destlab = "British"
	}
	if "`destinations'"=="CAN" {
		local destcur = "CAD"
		local destlab = "Canadian"
	}
	use firm_type DomicileCountryId iso_country_code mns_class mns_subclass currency_id marketvalue_usd cusip6 date_y using $resultstemp/portfolio_sumstat_y, clear
	drop if missing(currency_id) | missing(cusip6)
	keep if iso_country_code=="`destinations'"
	keep if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS")
	replace DomicileCountryId="ROW" if DomicileCountryId != iso_country_code
	replace currency_id = "FCU" if currency_id!="`destcur'"
	merge m:1 cusip6 using $output/industry/Industry_classifications_master, keepusing(ext_gics1 ext_gics6) nogen keep(1 3)

	collapse (sum) mv=marketvalue_usd (lastnm) firm_type ext_gics1 ext_gics6, by(cusip6 DomicileCountryId currency_id date_y)
	bys date_y DomicileCountryId: egen porttot = sum(mv)
	gen portshare = mv/porttot
	drop porttot mv
	reshape wide portshare, i(cusip6 firm_type ext_gics1 ext_gics6 currency_id date_y) j(DomicileCountryId) string
	reshape wide portshareROW portshare`destinations', i(cusip6 date_y firm_type ext_gics1 ext_gics6) j(currency_id) string

	replace portshare`destinations'`destcur' = 0 if portshare`destinations'`destcur'==.
	replace portshare`destinations'FCU = 0 if portshare`destinations'FCU==.
	replace portshareROW`destcur' = 0 if portshareROW`destcur'==.
	replace portshareROWFCU = 0 if portshareROWFCU==.
	gen domshare = portshare`destinations'`destcur'+portshare`destinations'FCU
	gen forshare = portshareROWFCU+portshareROW`destcur'
	gsort date_y -domshare -forshare
	drop if forshare==0 & domshare==0
	bys date_y: gen rank = _n
	replace portshare`destinations'`destcur' = . if portshare`destinations'`destcur'==0
	replace portshare`destinations'FCU = . if portshare`destinations'FCU==0
	replace portshareROW`destcur' = . if portshareROW`destcur'==0
	replace portshareROWFCU = . if portshareROWFCU==0
	replace domshare = . if domshare==0
	replace forshare = . if forshare==0
	merge m:1 cusip6 using $resultstemp/cusip6names, nogen keep(1 3)
	gen y1dom = .
	gen y1for = .
	gen x1 = 80
	gen x2 = 25

	sum rank if date_y==`plotyear'
	local xmax = min(100*ceil(`r(max)'/100),600)

	sum domshare if rank==1 & date_y==`plotyear'
	local ydom = `r(mean)'
	sum domshare if rank==1 & date_y==`plotyear'
	replace y1dom = `r(mean)'
	sum forshare if rank==1 & date_y==`plotyear'
	replace y1for = `r(mean)'
	sum forshare if rank==1 & date_y==`plotyear'
	local yfor = `r(mean)'

	foreach gics1_lab in "Energy_and_Utilities" "Consumer_products" "Materials_and_Industrials" "IT_and_Telecommunication" {

		preserve
		if "`gics1_lab'" == "Energy_and_Utilities" {
			keep if inlist(ext_gics1, 10, 55)
			local gics_title = "Energy and utilities"
		}
		if "`gics1_lab'" == "Consumer_products" {
			keep if inlist(ext_gics1, 25, 30)
			local gics_title = "Consumer products"
		}
		if "`gics1_lab'" == "Materials_and_Industrials" {
			keep if inlist(ext_gics1, 15, 20)
			local gics_title = "Materials and industrials"
		}
		if "`gics1_lab'" == "IT_and_Telecommunication" {
			keep if inlist(ext_gics1, 45, 50)
			local gics_title = "IT and telecommunication"
		}
		
		scatter domshare forshare rank if rank<=`xmax' & date_y==`plotyear' & portshare`destinations'FCU==. & portshareROWFCU==. , xlabel(0(100)`xmax') graphregion(color(white)) legend(label(1 "Domestic") label(2 "Foreign") rows(1)) mc(red blue) ms(o Dh) xtitle("Issuer's Rank by Domestic Borrowing") ytitle("")				
		graph export $resultstemp/graphs/Rank_GICS1_LocalOnly_`destinations'_ML_`gics1_lab'_`plotyear'.eps, as(eps) replace
		restore
	}

}

log close
