* This file starts with the quarterly HD_foranalysis files and generates the time-series plots of the dollar versus euro's use, shown in Figures 10 and 11 in the main text in our appendix. 
* The file takes 8 inputs. The first two consider various dates at which to define the weights for fixed effects regressions. The third input gives the date 
* used to normalize the plots from the fixed effects regressions. The fourth gives the start date for the plots. Finally, the 5th-8th give dates (if any) that we might want to exclude from these
* time-series analyses. 

cap log close
log using "$logs/${whoami}_CurrencyShare_TimeSeries", replace

* Main plots
foreach suffix in "" "_fixedfx" {
	foreach bondlab in "B" "BS" "BC" {
		foreach geolab in "World" "inotj" "inotj_exUSAEMU" "ieqj" "ieqj_exUSAEMU" {
			foreach inds in "All" "Fin" "NonFin" {
				di "`suffix'_`bondlab'_`geolab'_`inds'"
				use $resultstemp/HD_foranalysis_q`suffix', clear
				forvalues j = 5(1)10 {
					cap drop if date_q==tq(``j'')
				}
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
				if "`geolab'"=="World" {
					local subtitle = "all i and j"
				}
				if "`geolab'"=="inotj" {
					local subtitle = "i != j (i.e. international flows)"
					drop if DomicileCountryId==iso_country_code
				}
				if "`geolab'"=="ieqj" {
					local subtitle = "i = j (i.e. domestic flows)"
					keep if DomicileCountryId==iso_country_code
				}
				if "`geolab'"=="inotj_exUSAEMU" {
					local subtitle = "i != j (i.e. international flows), excluding USA/EMU"
					drop if DomicileCountryId==iso_country_code | inlist(DomicileCountryId,"USA","EMU") | inlist(iso_country_code,"USA","EMU")
				}
				if "`geolab'"=="ieqj_exUSAEMU" {
					local subtitle = "i = j (i.e. domestic flows), excluding USA/EMU"
					keep if DomicileCountryId==iso_country_code
					drop if inlist(DomicileCountryId,"USA","EMU") | inlist(iso_country_code,"USA","EMU")
				}
				if "`bondlab'"=="B" {
					keep if mns_class=="B" & !missing(currency_id)
					local bondtitle = "Bonds"
				}
				if "`bondlab'"=="BS" {
					keep if mns_class=="B" & !missing(currency_id) & inlist(mns_subclass,"S","A","LS")
					local bondtitle = "Sovereign Bonds"
				}
				if "`bondlab'"=="BC" {
					keep if mns_class=="B" & !missing(currency_id) & !inlist(mns_subclass,"S","A","SF","SV","LS")
					local bondtitle = "Corporate Bonds"
				}
				collapse (sum) marketvalue_usd, by(date_q currency_id mns_class mns_subclass DomicileCountryId iso_country_code)

				*Generate Global Plots
				bys date_q: egen mv_world = sum(marketvalue_usd)
				foreach globcur in "USD" "EUR" { 
					gen mv_`globcur' = marketvalue_usd if currency_id=="`globcur'"
					bys date_q: egen mv_`globcur'_world = sum(mv_`globcur')
					gen `globcur'_share_world = mv_`globcur'_world/mv_world
				}
				local lineloc = tq(2008q3)
				egen date_tag = tag(date_q)
				line USD_share_world EUR_share_world date_q if date_q>tq(`4') & date_tag==1, ytitle("Share of Global Total") text(1.0 `lineloc' "08:Q3", place(c) size(vsmall) color(gray)) xline(`lineloc', lw(thin) lp(dash) lc(gray)) ylabel(0(0.2)1) xtitle("") graphregion(color(white)) lpattern(solid dash) lcolor(red blue) legend(label(1 "USD") rows(1)) legend(label(2 "EUR"))
				graph export $resultstemp/graphs/cs2_raw_`bondlab'_`geolab'`indslab'_q`suffix'.eps, as(eps) replace
				
				*Generate (Bilateral Pair) Fixed Effect Plot
				drop mv_*_world mv_world *_share_world
				gen ijpair = DomicileCountryId + iso_country_code
				bys date_q ijpair: egen mv_tot = sum(marketvalue_usd)
				foreach globcur in "USD" "EUR" { 
					bys date_q ijpair: egen mv_`globcur'_tot = sum(mv_`globcur')
					gen `globcur'_share = mv_`globcur'_tot/mv_tot
				}
				collapse (lastnm) *_share (sum) mv_tot, by(date_q ijpair)
				levelsof(date_q), local(dates)
				gen wt_0 = 1
				forvalues wtvalind = 1(1)2 {
					gen wt_`wtvalind'_tmp = mv_tot if date_q==tq(``wtvalind'')
					bys ijpair: egen wt_`wtvalind' = mean(wt_`wtvalind'_tmp)
				}
				xi i.date_q, noomit
				foreach globcur in "USD" "EUR" { 
					forvalues wtvalind = 0(1)2 {
						gen fe_`globcur'_`bondlab'_`geolab'_`wtvalind' = .
						gen se_`globcur'_`bondlab'_`geolab'_`wtvalind' = .
						cap {
							areg `globcur'_share _Idate_q* [w=wt_`wtvalind'], absorb(ijpair)
							foreach date of local dates {
								replace fe_`globcur'_`bondlab'_`geolab'_`wtvalind' = _b[_Idate_q_`date'] if date_q==`date'
								replace se_`globcur'_`bondlab'_`geolab'_`wtvalind' = _se[_Idate_q_`date'] if date_q==`date'
							}
						}
					}
				}
				keep fe* se* date_q
				duplicates drop
				drop if missing(date_q)
				sort date_q
				gen row = _n
				sum row if date_q==tq(`3')
				foreach globcur in "USD" "EUR" { 
					forvalues wtvalind = 0(1)2 {
						cap {
							local shifter = fe_`globcur'_`bondlab'_`geolab'_`wtvalind'[`r(mean)']
							replace fe_`globcur'_`bondlab'_`geolab'_`wtvalind' = fe_`globcur'_`bondlab'_`geolab'_`wtvalind' - `shifter' 
						}
					}
				}
				cap {
					line fe_USD_`bondlab'_`geolab'_0 fe_EUR_`bondlab'_`geolab'_0 date_q if date_q>=tq(`4'), ytitle("Share of Global Total (Index, 2008q3=0)") text(1.0 `lineloc' "08:Q3", place(c) size(vsmall) color(gray)) xline(`lineloc', lw(thin) lp(dash) lc(gray)) ylabel(-0.2(0.05)0.2) graphregion(color(white)) xtitle("")  lpattern(solid dash) lcolor(red blue) legend(label(1 "USD") label(2 "EUR"))
					graph export $resultstemp/graphs/cs2_fe_`bondlab'`indslab'_`geolab'_unwtd`suffix'.eps, as(eps) replace
				}
				cap {
					line fe_USD_`bondlab'_`geolab'_1 fe_EUR_`bondlab'_`geolab'_1 date_q if date_q>=tq(`4'), ytitle("Share of Global Total (Index, 2008q3=0)") text(1.0 `lineloc' "08:Q3", place(c) size(vsmall) color(gray)) xline(`lineloc', lw(thin) lp(dash) lc(gray)) ylabel(-0.2(0.05)0.2) graphregion(color(white)) xtitle("")  lpattern(solid dash) lcolor(red blue) legend(label(1 "USD") label(2 "EUR"))
					graph export $resultstemp/graphs/cs2_fe_`bondlab'`indslab'_`geolab'_wtd_`1'`suffix'.eps, as(eps) replace
				}
				cap {
					line fe_USD_`bondlab'_`geolab'_2 fe_EUR_`bondlab'_`geolab'_2 date_q if date_q>=tq(`4'), ytitle("Share of Global Total (Index, 2008q3=0)") text(1.0 `lineloc' "08:Q3", place(c) size(vsmall) color(gray)) xline(`lineloc', lw(thin) lp(dash) lc(gray)) ylabel(-0.2(0.05)0.2) graphregion(color(white)) xtitle("") lpattern(solid dash) lcolor(red blue) legend(label(1 "USD") label(2 "EUR"))
					graph export $resultstemp/graphs/cs2_fe_`bondlab'`indslab'_`geolab'_wtd_`2'`suffix'.eps, as(eps) replace
				}
			}
		}
	}
}

log close
