* This file produces the bar charts showing currency shares of investment in various investor/borrower combinations. These are shown, for example, in Figure 2 of the main text and Appendix Figures A.11 and A.12.
* The graphs called "vbar*" show currency shares in bilateral relationships, such as in Appendix Figure A.11. The ones called "hbars" show multilateral shares, where investors are divided into domestic and foreign,  
* as opposed to separated by investing country. 

cap log close
log using "$logs/${whoami}_CurrencyShare_BarCharts", replace

foreach ctybase in "USA" "EMU" "GBR" "CHE" {
	foreach btype in "B" "BS" "BC" {
		foreach inds in "All" "Fin" "NonFin" {
			use $resultstemp/HD_foranalysis_y, clear	
			if "`inds'"=="All" {
				local indslab = ""
			}
			if "`inds'"=="Fin" {
				local indslab = "_Fin"
				keep if firm_type==1
			}
			if "`inds'"=="NonFin" {
				local indslab = "_NonFin"
				keep if firm_type==2
			}
			if "`btype'"=="B" {
				keep if mns_class=="B" & !missing(currency_id)
				local bondlabel = "All Bonds"
			}
			if "`btype'"=="BC" {
				keep if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS") & !missing(currency_id)
				local bondlabel = "Corporate Bonds"
			}
			if "`btype'"=="BS" {
				keep if mns_class=="B" & inlist(mns_subclass,"S","A","LS") & !missing(currency_id)
				local bondlabel = "Sovereign Bonds"
			}
			if "`ctybase'"=="USA" {
				local currbase = "USD"
				local ctybasename = "United States"
			}
			if "`ctybase'"=="EMU" {
				local currbase = "EUR"
				local ctybasename = "European Monetary Union"
			}
			if "`ctybase'"=="GBR" {
				local currbase = "GBP"
				local ctybasename = "United Kingdom"
			}
			if "`ctybase'"=="CHE" {
				local currbase = "CHF"
				local ctybasename = "Switzerland"
			}
			gen marketvalue_usd_owncur = marketvalue_usd if currency_id==DomicileCurrencyId
			gen marketvalue_usd_destcur = marketvalue_usd if currency_id==iso_currency_code
			collapse (sum) marketvalue_usd_owncur marketvalue_usd_destcur marketvalue_usd, by(DomicileCountryId iso_country_code date_y)
			gen destcurshare = marketvalue_usd_destcur/marketvalue_usd
			gen owncurshare = marketvalue_usd_owncur/marketvalue_usd
			gen barrank = DomicileCountryId
			replace barrank = "AAA" if DomicileCountryId=="`ctybase'"
			separate destcurshare, by(DomicileCountryId=="`ctybase'")
			forvalues plotyear = 2005(1)2017 {
				count if iso_country_code=="`ctybase'" & date_y==`plotyear'
				if `r(N)'>0 {
					graph bar destcurshare0 destcurshare1 if iso_country_code=="`ctybase'" & date_y==`plotyear', ylabel(0(0.2)1) bar(1, lcolor(blue) bstyle(outline) ) bar(2, color(red)) nofill over(barrank, label(labs(small)) relabel(1 "`ctybase'")) legend(off) graphregion(color(white)) b1title("Domicile Country (j)") 
				graph export $resultstemp/graphs/vbar_`ctybase'_`currbase'_`btype'`indslab'_`plotyear'.eps, as(eps) replace
				}
			}
		}
	}
}

foreach btype in "B" "BS" "BC" "B_nousd" "BS_nousd" "BC_nousd" {
	foreach inds in "All" "Fin" "NonFin" {
		use $resultstemp/HD_foranalysis_y, clear
			if "`inds'"=="All" {
				local indslab = ""
			}
			if "`inds'"=="Fin" {
				local indslab = "_Fin"
				keep if firm_type==1
			}
			if "`inds'"=="NonFin" {
				local indslab = "_NonFin"
				keep if firm_type==2
			}
			if "`btype'"=="B" {
				keep if mns_class=="B" & !missing(currency_id)
				local bondlabel = "All Bonds"
				local bondlabel2 = ""
			}
			if "`btype'"=="BC" {
				keep if mns_class=="B" & !inlist(mns_subclass,"S","A","LS","SV","SF") & !missing(currency_id)
				local bondlabel = "Corporate Bonds"
				local bondlabel2 = ""
			}
			if "`btype'"=="BS" {
				keep if mns_class=="B" & inlist(mns_subclass,"S","A","LS") & !missing(currency_id)
				local bondlabel = "Sovereign Bonds"
				local bondlabel2 = ""
			}
			if "`btype'"=="B_nousd" {
				keep if mns_class=="B" & !missing(currency_id)
				local bondlabel = "All Bonds"	
				drop if currency_id=="USD"
				local bondlabel2 =  " (Ex-USD)"
			}
			if "`btype'"=="BC_nousd" {
				keep if mns_class=="B" & !inlist(mns_subclass,"S","A","LS","SV","SF") & !missing(currency_id)
				local bondlabel = "Corporate Bonds"	
				drop if currency_id=="USD"
				local bondlabel2 =  " (Ex-USD)"
			}
			if "`btype'"=="BS_nousd" {
				keep if mns_class=="B" & inlist(mns_subclass,"S","A","LS") & !missing(currency_id)
				local bondlabel = "Sovereign Bonds"	
				drop if currency_id=="USD"
				local bondlabel2 =  " (Ex-USD)"
			}
			gen mv_home_inrowcurr = marketvalue_usd if iso_country_code==DomicileCountryId & currency_id!=DomicileCurrencyId
			gen mv_row_inrowcurr = marketvalue_usd if iso_country_code!=DomicileCountryId & currency_id==iso_currency_code
			gen mv_home_inhomecurr = marketvalue_usd if iso_country_code==DomicileCountryId & currency_id==DomicileCurrencyId
			gen mv_row_inhomecurr = marketvalue_usd if iso_country_code!=DomicileCountryId & currency_id==DomicileCurrencyId
			gen mv_home = marketvalue_usd if iso_country_code==DomicileCountryId 
			gen mv_row = marketvalue_usd if iso_country_code!=DomicileCountryId 
			collapse (sum) mv_home_inrowcurr mv_row_inrowcurr mv_home_inhomecurr mv_row_inhomecurr  mv_home mv_row, by(DomicileCountryId date_y)
			gen mv_home_inrowcurr_share = -mv_home_inrowcurr/mv_home
			gen mv_row_inrowcurr_share = mv_row_inrowcurr/mv_row	
			gen mv_home_inhomecurr_share = -mv_home_inhomecurr/mv_home
			gen mv_row_inhomecurr_share = -mv_row_inhomecurr/mv_row	
			save $resultstemp/hbar_tmp_`btype'`indslab'`excl', replace
			use DomicileCountryId using $resultstemp/HD_foranalysis_y, clear
			levelsof DomicileCountryId, local(goodcovcty2)
			foreach keepcty of local goodcovcty2 {
				use $resultstemp/HD_foranalysis_y, clear
				levelsof(DomicileCurrencyId) if DomicileCountryId=="`keepcty'", local(keepcty_cur)
				if "`inds'"=="All" {
					local indslab = ""
				}
				if "`inds'"=="Fin" {
					local indslab = "_Fin"
					keep if firm_type==1
				}
				if "`inds'"=="NonFin" {
					local indslab = "_NonFin"
					keep if firm_type==2
				}
				if "`btype'"=="B" {
					keep if mns_class=="B" & !missing(currency_id)
					local bondlabel = "Bonds"
					local bondlabel2 = ""
					local bondlabel3 ="B"
				}
				if "`btype'"=="BC" {
					keep if mns_class=="B" & !inlist(mns_subclass,"S","A","LS","SV","SF") & !missing(currency_id)
					local bondlabel = "Corporate Bonds"
					local bondlabel2 = ""
					local bondlabel3 ="BC"
				}
				if "`btype'"=="BS" {
					keep if mns_class=="B" & inlist(mns_subclass,"S","A") & !missing(currency_id)
					local bondlabel = "Sovereign Bonds"
					local bondlabel2 = ""
					local bondlabel3 ="BS"
				}
				if "`btype'"=="B_nousd" {
					keep if mns_class=="B" & !missing(currency_id)
					local bondlabel = "All Bonds"	
					drop if currency_id=="USD"
					local bondlabel2 =  " (Ex-USD)"
					local bondlabel3 = "B"
				}
				if "`btype'"=="BC_nousd" {
					keep if mns_class=="B" & !inlist(mns_subclass,"S","A","SV","SF","LS") & !missing(currency_id)
					local bondlabel = "Corporate Bonds"	
					drop if currency_id=="USD"
					local bondlabel2 =  " (Ex-USD)"
					local bondlabel3 = "BC"
				}
				if "`btype'"=="BS_nousd" {
					keep if mns_class=="B" & inlist(mns_subclass,"S","A") & !missing(currency_id)
					local bondlabel = "Sovereign Bonds"	
					drop if currency_id=="USD"
					local bondlabel2 =  " (Ex-USD)"
					local bondlabel3 = "BS"
				}	
				gen graphcountry = "`keepcty'"
				drop if DomicileCountryId=="`keepcty'"
				gen mv_rowtohome_inhomecurr = marketvalue_usd if iso_country_code=="`keepcty'" & currency_id==iso_currency_code
				gen mv_rowtohome = marketvalue_usd if iso_country_code=="`keepcty'" 
				cap gen mv_rowtorow_inhomecurr = marketvalue_usd if iso_country_code!="`keepcty'" & currency_id==`keepcty_cur'
				gen mv_rowtorow = marketvalue_usd if iso_country_code!="`keepcty'" 
				count
				if `r(N)'>0 {
					collapse (sum) mv_rowtohome_inhomecurr mv_rowtohome mv_rowtorow_inhomecurr mv_rowtorow, by(graphcountry date_y)
					gen mv_rowtohome_inhomecurr_share = mv_rowtohome_inhomecurr/mv_rowtohome	
					gen mv_rowtorow_inhomecurr_share = mv_rowtorow_inhomecurr/mv_rowtorow
					save "$resultstemp/hbar_tmp_`keepcty'_`btype'`indslab'`excl'.dta", replace emptyok
				}
			}
			clear
			foreach keepcty of local goodcovcty2 {
				cap append using "$resultstemp/hbar_tmp_`keepcty'_`btype'`indslab'`excl'.dta"
				cap rm "$resultstemp/hbar_tmp_`keepcty'_`btype'`indslab'`excl'.dta"
			}
			count
			if `r(N)'>0 {
				keep graphcountry mv_rowtohome_inhomecurr_share mv_rowtorow_inhomecurr_share date_y
				replace mv_rowtohome_inhomecurr_share=. if mv_rowtohome_inhomecurr_share==0
				replace mv_rowtorow_inhomecurr_share=. if mv_rowtorow_inhomecurr_share==0
				rename graphcountry DomicileCountryId
				merge 1:1 date_y DomicileCountryId using $resultstemp/hbar_tmp_`btype'`indslab'`excl', keep(3) nogen
				gen mv_home_inhomecurr_share_pos = -mv_home_inhomecurr_share
				forvalues plotyear = 2005(1)2017 {

					graph hbar mv_home_inhomecurr_share mv_rowtohome_inhomecurr_share if date_y==`plotyear', bar(2, lcolor(blue) bstyle(outline) ) bar(1, color(red)) title("Share of Investor's Portfolio in Issuer's Currency", size(large)) subtitle("Domestic Investors                 Foreign Investors", size(medlarge)) stack ylabel(-1 "1" -0.75 "0.75" -0.5 "0.5" -0.25 "0.25" 0(0.25)1) over(DomicileCountryId) legend(off) graphregion(color(white)) l1title("Issuing Country", size(medlarge))
					graph export $resultstemp/graphs/hbar_`btype'`indslab'_`plotyear'`excl'.eps, as(eps) replace
					
					graph hbar mv_home_inhomecurr_share mv_rowtohome_inhomecurr_share if date_y==`plotyear' & DomicileCountryId!="USA", bar(2, lcolor(blue) bstyle(outline) ) bar(1, color(red)) title("Investor's Share in Issuer's Currency", size(large)) subtitle("Domestic Investors                 Foreign Investors", size(large)) stack ylabel(-1 "1" -0.75 "0.75" -0.5 "0.5" -0.25 "0.25" 0(0.25)1) over(DomicileCountryId) legend(off) graphregion(color(white)) l1title("Issuing Country", size(medlarge))
					graph export $resultstemp/graphs/hbar_`btype'`indslab'_`plotyear'_exUSA`excl'.eps, as(eps) replace
					
					graph hbar mv_home_inhomecurr_share mv_rowtohome_inhomecurr_share if date_y==`plotyear', bar(2, lcolor(blue) bstyle(outline) ) bar(1, color(red)) stack ylabel(-1 "1" -0.75 "0.75" -0.5 "0.5" -0.25 "0.25" 0(0.25)1) over(DomicileCountryId) legend(off) graphregion(color(white)) l1title("Issuing Country", size(medlarge))
					graph export $resultstemp/graphs/hbar_`btype'`indslab'_`plotyear'`excl'_nosub.eps, as(eps) replace
					graph hbar mv_home_inhomecurr_share mv_rowtohome_inhomecurr_share if date_y==`plotyear' & DomicileCountryId!="USA", bar(2, lcolor(blue) bstyle(outline) ) bar(1, color(red)) stack ylabel(-1 "1" -0.75 "0.75" -0.5 "0.5" -0.25 "0.25" 0(0.25)1) over(DomicileCountryId) legend(off) graphregion(color(white)) l1title("Issuing Country", size(medlarge))
					graph export $resultstemp/graphs/hbar_`btype'`indslab'_`plotyear'_exUSA`excl'_nosub.eps, as(eps) replace

			}
		}
	}
}

log close
