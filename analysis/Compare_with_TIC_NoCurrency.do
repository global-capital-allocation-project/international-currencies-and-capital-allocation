* This file combines the HD_sumstats_y files with data from TIC in order to compare portfolio shares. The resulting plots are in Appendix Figure A.3 in our paper.

cap log close
log using "$logs/${whoami}_Compare_with_TIC_NoCurrency", replace

foreach direction in "inward" "outward" {
	if "`direction'"=="outward" {
		local region = "US"
		local dirlab = "Outward from U.S."
		local dirlab2 = "Outward"
	} 
	else {
		local region = "NonUS"
		local dirlab = "Inward to U.S."
		local dirlab2 = "Inward"
	}
	use $resultstemp/HD_sumstats_y if !missing(DomicileCountryId) & !missing(iso_country_code), clear
	rename iso_country_code iso
	rename date_y year
	if "`direction'"=="outward" {
		keep if DomicileCountryId=="USA" & iso!="USA"
	}		
	if "`direction'"=="inward" {
		keep if DomicileCountryId!="USA" & iso=="USA"
		drop iso
		rename DomicileCountryId iso
	}
	forvalues j = 1(1)3 {
		replace iso = "EMU" if inlist(iso,${eu`j'})
	} 
	collapse (sum) value_mstar = marketvalue_usd, by(year iso mns_class) 
	save $resultstemp/mstarfortic_`direction'_y_collapsed, replace
	use $output/tic_data/tic_agg_`direction'.dta, clear
	forvalues j = 1(1)3 {
		replace iso = "EMU" if inlist(iso,${eu`j'})
	} 
	collapse (sum) value_tic=value, by(year iso mns_class)
	merge 1:1 year iso mns_class using $resultstemp/mstarfortic_`direction'_y_collapsed.dta, keep(1 2 3)
	replace value_mstar = value_mstar/1000000
	gen ln_value_tic=ln(value_tic)
	gen ln_value_mstar=ln(value_mstar)
	save $temp/tic_data/agg_mstar_tic_`direction', replace

	bys year mns_class: egen value_mstar_yrtot = sum(value_mstar) 
	bys year mns_class: egen value_tic_yrtot = sum(value_tic)
	gen share_mstar = value_mstar / value_mstar_yrtot
	gen share_tic = value_tic / value_tic_yrtot

	scatter share_mstar share_tic if year==2005 & mns_class=="B", ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_tic if year==2010 & mns_class=="B", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_tic if year==2017 & mns_class=="B", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_tic share_tic if mns_class=="B", lp(thin) lc(black) legend(order(1 2 3))
	graph export $resultstemp/graphs/TIC_compare_bonds_`direction'.eps, as(eps) replace

	scatter share_mstar share_tic if year==2005 & mns_class=="E", ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_tic if year==2010 & mns_class=="E", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_tic if year==2017 & mns_class=="E", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_tic share_tic if mns_class=="E", lp(thin) lc(black) legend(order(1 2 3))
	graph export $resultstemp/graphs/TIC_compare_equities_`direction'.eps, as(eps) replace
}


foreach direction in "inward" "outward" {
	if "`direction'"=="outward" {
		local region = "US"
		local dirlab = "Outward from U.S."
		local dirlab2 = "Outward"
	} 
	else {
		local region = "NonUS"
		local dirlab = "Inward to U.S."
		local dirlab2 = "Inward"
	}
	use $resultstemp/HD_sumstats_y if !missing(DomicileCountryId) & !missing(iso_country_code), clear
	drop if inlist(iso_country_code,"JPN","CYM","CHN") | inlist(DomicileCountryId,"JPN","CYM","CHN")
	rename iso_country_code iso
	rename date_y year
	if "`direction'"=="outward" {
		keep if DomicileCountryId=="USA" & iso!="USA"
	}		
	if "`direction'"=="inward" {
		keep if DomicileCountryId!="USA" & iso=="USA"
		drop iso
		rename DomicileCountryId iso
	}
	forvalues j = 1(1)3 {
		replace iso = "EMU" if inlist(iso,${eu`j'})
	} 
	collapse (sum) value_mstar = marketvalue_usd, by(year iso mns_class) 
	save $resultstemp/mstarfortic_`direction'_y_collapsed_exCHNJPNCYM, replace
	use $output/tic_data/tic_agg_`direction'.dta, clear
	drop if inlist(iso,"JPN","CYM","CHN") 
	forvalues j = 1(1)3 {
		replace iso = "EMU" if inlist(iso,${eu`j'})
	} 
	collapse (sum) value_tic=value, by(year iso mns_class)
	merge 1:1 year iso mns_class using $resultstemp/mstarfortic_`direction'_y_collapsed_exCHNJPNCYM.dta, keep(1 2 3)
	replace value_mstar = value_mstar/1000000
	gen ln_value_tic=ln(value_tic)
	gen ln_value_mstar=ln(value_mstar)
	save $temp/tic_data/agg_mstar_tic_`direction'_exCHNJPNCYM, replace

	bys year mns_class: egen value_mstar_yrtot = sum(value_mstar) 
	bys year mns_class: egen value_tic_yrtot = sum(value_tic)
	gen share_mstar = value_mstar / value_mstar_yrtot
	gen share_tic = value_tic / value_tic_yrtot

	scatter share_mstar share_tic if year==2005 & mns_class=="B", note("Note: Excludes Cayman Islands, China, and Japan") ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_tic if year==2010 & mns_class=="B", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_tic if year==2017 & mns_class=="B", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_tic share_tic if mns_class=="B", lp(thin) lc(black) legend(order(1 2 3))
	graph export $resultstemp/graphs/TIC_compare_bonds_`direction'_exCHNJPNCYM.eps, as(eps) replace

	scatter share_mstar share_tic if year==2005 & mns_class=="E", note("Note: Excludes Cayman Islands, China, and Japan") ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2005") ) || scatter share_mstar share_tic if year==2010 & mns_class=="E", ml(iso) ms(Th) legend(label(2 "2010") ) || scatter share_mstar share_tic if year==2017 & mns_class=="E", ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line share_tic share_tic if mns_class=="E", lp(thin) lc(black) legend(order(1 2 3))
	graph export $resultstemp/graphs/TIC_compare_equities_`direction'_exCHNJPNCYM.eps, as(eps) replace
}


log close
