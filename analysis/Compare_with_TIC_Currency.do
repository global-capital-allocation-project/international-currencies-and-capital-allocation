* This file combines the HD_sumstats_y files with data from TIC in order to compare portfolio shares of subtypes of investment (by type of bond and currency). The resulting plots are in Appendix Figure A.4 in our paper.

cap log close
log using "$logs/${whoami}_Compare_with_TIC_Currency", replace

*Outward Analysis with Currency 
local region = "US"
local dirlab = "Outward from U.S."
local dirlab2 = "Outward"

use $resultstemp/HD_sumstats_y if !missing(DomicileCountryId) & !missing(iso_country_code), clear
rename date_y year
keep if DomicileCountryId=="USA" & iso_country_code!="USA"
gen _detail = "B" if mns_subclass!="S" & mns_subclass!="A" & mns_class=="B"
replace _detail = "BT" if inlist(mns_subclass,"S","A") & mns_class=="B"
drop if missing(_detail)
collapse (sum) value_mstar = marketvalue_usd, by(year iso_country_code _detail currency_id) 
mmerge iso_country_code using $output/concordances/country_currencies, ukeep(iso_currency_code start_date end_date)
drop if _merge==2 & iso_country_code!="EMU"
gen start_year=yofd(start_date)
gen end_year=yofd(end_date)
drop if year>end_year & end_year~=.
drop if year<start_year & start_year~=.

keep if year>=2007 & year<=2017 & (_merge==3 | iso_country_code=="EMU")
*replace value_mstar = value_mstar/10^9
forvalues j = 1(1)3 {
	replace iso_country_code = "EMU" if inlist(iso_country_code,${eu`j'})
	replace iso_currency_code = "EUR" if inlist(iso_country_code,${eu`j'})
} 
gen curr="other" if currency_id~=""
replace curr="lc" if currency_id==iso_currency_code
replace curr="usd" if currency_id=="USD"
drop if missing(curr)
collapse (sum) value_mstar, by(year iso_country_code curr _detail)
save $resultstemp/mstarfortic_outward_y_collapsed_wcurr, replace
use $output/tic_data/disagg_tic_long.dta, clear
forvalues j = 1(1)3 {
	replace iso_country_code = "EMU" if inlist(iso_country_code,${eu`j'})
} 
drop if missing(curr) | missing(_detail)
collapse (sum) value_tic=value, by(year curr iso_country_code _detail)
merge 1:1 year iso_country_code curr _detail using $resultstemp/mstarfortic_outward_y_collapsed_wcurr, keep(3) nogen
gen temp="_"+_detail+"_"+curr
drop _detail curr 
reshape wide value*, i(year iso_co) j(temp) string
renpfix value_
foreach x in mstar_BT_lc tic_BT_lc mstar_BT_other tic_BT_other mstar_BT_usd tic_BT_usd mstar_B_lc tic_B_lc  mstar_B_other tic_B_other mstar_B_usd tic_B_usd {
	replace `x'=0 if `x'==.
}
foreach x in mstar tic {
	foreach y in B BT {
		gen `x'_`y'_total=`x'_`y'_lc+`x'_`y'_other+`x'_`y'_usd
	}
}
foreach source in "mstar" "tic" {
	gen `source'_total=`source'_B_total+`source'_BT_total 
	bys year: egen `source'_total_world = sum(`source'_total)
	gen `source'_share = `source'_total / `source'_total_world
}

foreach source in "mstar" "tic" {
	foreach curcode in "usd" "lc" "other" {
		gen `source'_`curcode'=`source'_B_`curcode'+`source'_BT_`curcode'
		bys year: egen `source'_`curcode'_world = sum(`source'_`curcode')
		gen `source'_`curcode'_share = `source'_`curcode' / `source'_`curcode'_world
		gen `source'_`curcode'_ctyshare = `source'_`curcode' / `source'_total
	}
	gen `source'_usd_worldshare = `source'_usd_world / `source'_total_world
}
foreach source in "mstar" "tic" {
	foreach bondtype in "B" "BT" {
		gen `source'_`bondtype'=`source'_`bondtype'_usd+`source'_`bondtype'_lc+`source'_`bondtype'_other
		bys year: egen `source'_`bondtype'_world = sum(`source'_`bondtype')
		gen `source'_`bondtype'_share = `source'_`bondtype' / `source'_`bondtype'_world
		gen `source'_`bondtype'_ctyshare = `source'_`bondtype' / `source'_total
	}
}
foreach source in "mstar" "tic" {
	foreach bondtype in "B" "BT" {
		foreach curcode in "usd" "lc" "other"  {
			bys year: egen `source'_`bondtype'_`curcode'_world = sum(`source'_`bondtype'_`curcode')
			gen `source'_`bondtype'_`curcode'_share = `source'_`bondtype'_`curcode' / `source'_`bondtype'_`curcode'_world
			gen `source'_`bondtype'_`curcode'_ctyBshare = `source'_`bondtype'_`curcode' / (`source'_total*`source'_B_ctyshare)
			gen `source'_`bondtype'_`curcode'_ctyBTshare = `source'_`bondtype'_`curcode' / (`source'_total*`source'_BT_ctyshare)
			gen `source'_`bondtype'_`curcode'_ctyusdshare = `source'_`bondtype'_`curcode' / (`source'_total*`source'_usd_ctyshare)
			gen `source'_`bondtype'_`curcode'_ctylcshare = `source'_`bondtype'_`curcode' / (`source'_total*`source'_lc_ctyshare)
		}
		gen `source'_`bondtype'_usd_worldshare = `source'_`bondtype'_usd_world / `source'_`bondtype'_world
	}
}

gen ln_mstar_total = ln(mstar_total)
gen ln_tic_total = ln(tic_total)
gen gap = ln_mstar_total - ln_tic_total
bys iso_country_code: egen avegap = mean(gap)
gen ln_tic_total_scaled = ln_tic_total + avegap
sort iso_country_code year
drop if iso=="VGB"
foreach bondtype in "B" "BT" {
	foreach curcode in "usd" "lc" "other" {
		local capcurcode = strupper("`curcode'")
		local mergetype = "`bondtype'"+"_"+"`capcurcode'"

		if "`bondtype'"=="B" {
			local mns_lab = "BCBO"
		}
		if "`bondtype'"=="BT" {
			local mns_lab = "BS"
		}

		scatter mstar_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share if year==2007, ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2007") ) || scatter mstar_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share if year==2010, ml(iso) ms(Th) legend(label(2 "2010") ) || scatter mstar_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share if year==2017 , ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line tic_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share , lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/TIC_compare_`mns_lab'_`curcode'_outward.eps, as(eps) replace
		
		scatter mstar_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share if year==2007, ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2007") ) || scatter mstar_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share if year==2010, ml(iso) ms(Th) legend(label(2 "2010") ) || scatter mstar_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share if year==2017 , ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line tic_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share , lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/TIC_compare_`mns_lab'_`curcode'_outward_ctyshare.eps, as(eps) replace
		
	}
}

scatter mstar_usd_ctyshare tic_usd_ctyshare if year==2007, ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2007") ) || scatter mstar_usd_ctyshare tic_usd_ctyshare if year==2010, ml(iso) ms(Th) legend(label(2 "2010") ) || scatter mstar_usd_ctyshare tic_usd_ctyshare if year==2017 , ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line tic_usd_ctyshare tic_usd_ctyshare , lp(thin) lc(black) legend(order(1 2 3))
graph export $resultstemp/graphs/TIC_compare_usd_outward_ctyshare.eps, as(eps) replace


*Select Country focus, repeat analysis for select destinations
keep if iso=="EMU"|iso=="GBR"|iso=="JPN"|iso=="CAN"|iso=="SWE"|iso=="AUS"|iso=="NZL"|iso=="NOR"
foreach bondtype in "B" "BT" {
	foreach curcode in "usd" "lc" "other" {
		local capcurcode = strupper("`curcode'")
		local mergetype = "`bondtype'"+"_"+"`capcurcode'"

		if "`bondtype'"=="B" {
			local mns_lab = "BCBO"
		}
		if "`bondtype'"=="BT" {
			local mns_lab = "BS"
		}

		scatter mstar_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share if year==2007, ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2007") ) || scatter mstar_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share if year==2010, ml(iso) ms(Th) legend(label(2 "2010") ) || scatter mstar_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share if year==2017 , ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line tic_`bondtype'_`curcode'_share tic_`bondtype'_`curcode'_share , lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/TIC_compare_`mns_lab'_`curcode'_outward_select.eps, as(eps) replace
		
		scatter mstar_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share if year==2007, ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2007") ) || scatter mstar_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share if year==2010, ml(iso) ms(Th) legend(label(2 "2010") ) || scatter mstar_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share if year==2017 , ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line tic_`bondtype'_`curcode'_cty`bondtype'share tic_`bondtype'_`curcode'_cty`bondtype'share , lp(thin) lc(black) legend(order(1 2 3))
		graph export $resultstemp/graphs/TIC_compare_`mns_lab'_`curcode'_outward_ctyshare_select.eps, as(eps) replace
		
	}
}

scatter mstar_usd_ctyshare tic_usd_ctyshare if year==2007, ytitle("Morningstar") xtitle("TIC") graphregion(color(white)) ml(iso) ms(Oh) legend(label(1 "2007") ) || scatter mstar_usd_ctyshare tic_usd_ctyshare if year==2010, ml(iso) ms(Th) legend(label(2 "2010") ) || scatter mstar_usd_ctyshare tic_usd_ctyshare if year==2017 , ml(iso) ms(Sh) legend(label(3 "2017") rows(1)) || line tic_usd_ctyshare tic_usd_ctyshare , lp(thin) lc(black) legend(order(1 2 3))
graph export $resultstemp/graphs/TIC_compare_usd_outward_ctyshare_select.eps, as(eps) replace

keep year tic_B_usd_worldshare mstar_B_usd_worldshare tic_BT_usd_worldshare mstar_BT_usd_worldshare tic_usd_worldshare mstar_usd_worldshare
duplicates drop
order year tic_B_usd_worldshare mstar_B_usd_worldshare tic_BT_usd_worldshare mstar_BT_usd_worldshare tic_usd_worldshare mstar_usd_worldshare
save $resultstemp/table_mstar_tic_wcurr, replace


log close
