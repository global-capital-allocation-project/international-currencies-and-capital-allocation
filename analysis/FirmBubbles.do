* This file reads in portfolio_sumstat_y and generates figures comparing the share of a company's bonds that are held by foreigners with the share issued in foreign currency, plotted in Figure 5 of the paper. 
* It repeats the analysis using SDC data to get the foreign currency issuance data and generates Appendix Figure A.19.

cap log close
log using "$logs/${whoami}_FirmBubbles", replace

use firm_type DomicileCountryId iso_country_code mns_class mns_subclass currency_id marketvalue_usd cusip6 date_y using $resultstemp/portfolio_sumstat_y if inlist(iso_country_code,"EMU","USA","CAN","GBR") & date_y==2017, clear
keep if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS")
gen totborlcu = marketvalue_usd if (currency_id=="EUR" & iso_country_code=="EMU") | (currency_id=="USD" & iso_country_code=="USA") | (currency_id=="CAD" & iso_country_code=="CAN") | (currency_id=="GBP" & iso_country_code=="GBR")
gen totbor = marketvalue_usd
gen locbor = marketvalue_usd if iso_country_code==DomicileCountryId
collapse (sum) totborlcu totbor locbor, by(iso_country_code cusip6)
drop if missing(cusip6)
gen wt = totbor
gen fc_share = 1-totborlcu/totbor
gen forshare = 1-locbor/totbor
gen dumx = .
gen dumy = .

mmerge cusip6 using $temp/probits/dlg_sdc_appended_firms, umatch(cusip6_bg) uname(sdc_dlg_) ukeep(fc_share) unmatched(m)

drop if forshare < 0 | forshare > 1
drop if fc_share < 0 | fc_share > 1

scatter forshare fc_share if iso_country_code=="EMU" [aw=wt], mfc(none) mlcolor(blue) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(small)) xtitle("Share of Firm's Debt in Foreign Currency", size(small))  legend(off)
graph export $resultstemp/graphs/firmbubbles_wintercept_EMU.eps, as(eps) replace
graph save $resultstemp/graphs/firmbubbles_wintercept_EMU.gph, replace
scatter forshare fc_share if iso_country_code=="GBR" [aw=wt], mfc(none) mlcolor(blue) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(small)) xtitle("Share of Firm's Debt in Foreign Currency", size(small)) legend(off)
graph export $resultstemp/graphs/firmbubbles_wintercept_GBR.eps, as(eps) replace
graph save $resultstemp/graphs/firmbubbles_wintercept_GBR.gph, replace
scatter forshare fc_share if iso_country_code=="CAN" [aw=wt], mfc(none) mlcolor(blue) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(small)) xtitle("Share of Firm's Debt in Foreign Currency", size(small)) legend(off)
graph export $resultstemp/graphs/firmbubbles_wintercept_CAN.eps, as(eps) replace
graph save $resultstemp/graphs/firmbubbles_wintercept_CAN.gph, replace
scatter forshare fc_share if iso_country_code=="USA" [aw=wt], mfc(none) mlcolor(red) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(small)) xtitle("Share of Firm's Debt in Foreign Currency", size(small)) legend(off)
graph export $resultstemp/graphs/firmbubbles_wintercept_USA.eps, as(eps) replace
graph save $resultstemp/graphs/firmbubbles_wintercept_USA.gph, replace
graph combine $resultstemp/graphs/firmbubbles_wintercept_CAN.gph $resultstemp/graphs/firmbubbles_wintercept_EMU.gph $resultstemp/graphs/firmbubbles_wintercept_GBR.gph $resultstemp/graphs/firmbubbles_wintercept_USA.gph, graphregion(color(white))
graph export $resultstemp/graphs/firmbubbles_wintercept_All.eps, as(eps) replace

scatter forshare fc_share if iso_country_code=="EMU" [aw=wt], mfc(none) mlcolor(blue) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners") xtitle("Share of Firm's Debt in Foreign Currency") || scatter dumy dumx if iso_country_code=="USA" [aw=wt], mfc(none) mlcolor(red) || lfit forshare fc_share if iso_country_code=="EMU" [aw=wt], lc(blue) lw(thick) legend(label(1 "EMU")) legend(label(2 "USA")) legend(order(1))
graph export $resultstemp/graphs/firmbubbles_wintercept0.eps, as(eps) replace
scatter forshare fc_share if iso_country_code=="EMU" [aw=wt], mfc(none) mlcolor(blue) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners") xtitle("Share of Firm's Debt in Foreign Currency") || scatter dumy dumx if iso_country_code=="USA" [aw=wt], mfc(none) mlcolor(red) || lfit forshare fc_share if iso_country_code=="EMU" [aw=wt], lc(blue) lw(thick) legend(label(1 "EMU")) legend(label(2 "USA")) legend(order(1 2))
graph export $resultstemp/graphs/firmbubbles_wintercept1.eps, as(eps) replace
scatter forshare fc_share if iso_country_code=="EMU" [aw=wt], mfc(none) mlcolor(blue) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners") xtitle("Share of Firm's Debt in Foreign Currency") || scatter forshare fc_share if iso_country_code=="USA" [aw=wt], mfc(none) mlcolor(red) || lfit forshare fc_share if iso_country_code=="EMU" [aw=wt], lc(blue) lw(thick) || lfit forshare fc_share if iso_country_code=="USA" [aw=wt], lc(red) lw(thick) legend(label(1 "EMU")) legend(label(2 "USA")) legend(order(1 2))
graph export $resultstemp/graphs/firmbubbles_wintercept.eps, as(eps) replace

foreach x in EMU USA GBR CAN {

	if "`x'" == "USA" {
		local _color = "red"
	}
	else {
		local _color = "blue"
	}

	scatter forshare fc_share if iso_country_code=="`x'" [aw=wt],  mfc(none) mlcolor(black) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(med)) xtitle("Share of Firm's Debt in Foreign Currency", size(med))  legend(off)
	graph export $resultstemp/graphs/firmbubbles_wintercept_`x'_bw.eps, as(eps) replace
	cap graph export $resultstemp/graphs/firmbubbles_wintercept_`x'_bw.tif, as(tif) replace
	cap graph export $resultstemp/graphs/firmbubbles_wintercept_`x'_bw.png, as(png) replace

	scatter forshare sdc_dlg_fc_share if iso_country_code=="`x'" [aw=wt],  mfc(none) mlcolor(black) graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(med)) xtitle("Share of Firm's Debt in Foreign Currency", size(med))  legend(off)
	graph export $resultstemp/graphs/sdc_firmbubbles_wintercept_`x'_bw.eps, as(eps) replace
	cap graph export $resultstemp/graphs/sdc_firmbubbles_wintercept_`x'_bw.tif, as(tif) replace
	cap graph export $resultstemp/graphs/sdc_firmbubbles_wintercept_`x'_bw.png, as(png) replace

	scatter forshare sdc_dlg_fc_share if iso_country_code=="`x'" [aw=wt],  mfc(none) mlcolor(`_color') graphregion(color(white)) ytitle("Share of Firm's Debt Owned by Foreigners", size(med)) xtitle("Share of Firm's Debt in Foreign Currency", size(med))  legend(off)
	graph export $resultstemp/graphs/sdc_firmbubbles_wintercept_`x'_color.eps, as(eps) replace
	cap graph export $resultstemp/graphs/sdc_firmbubbles_wintercept_`x'_color.tif, as(tif) replace
	cap graph export $resultstemp/graphs/sdc_firmbubbles_wintercept_`x'_color.png, as(png) replace

	twoway (scatter sdc_dlg_fc_share fc_share if iso_country_code=="`x'" [aw=wt],  mfc(none) mlcolor(black) ) (lfit sdc_dlg_fc_share fc_share if iso_country_code=="`x'" [aw=wt]) , graphregion(color(white)) ytitle("Share of Firm's Debt in FC (SDC and DLG)", size(med)) xtitle("Share of Firm's Debt in FC (Morningstar)", size(med))  legend(off)
	graph export $resultstemp/graphs/sdc_ms_fcshare_`x'_bw.eps, as(eps) replace
}

log close
