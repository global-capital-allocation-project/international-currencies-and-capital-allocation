* This file generates the values in Table 6, which gives the dollar and euro shares of various portfolios of bonds.  

cap log close
log using "$logs/${whoami}_CurrencyShare_TimeSeries_Table", replace

foreach bondlab in "B" "BS" "BC" {
	foreach geolab in "World" "inotj" "inotj_exUSAEMU" {
		foreach inds in "All" "Fin" "NonFin" {
			use date_q firm_type currency_id marketvalue_usd mns_class mns_subclass DomicileCountryId iso_country_code using $resultstemp/HD_foranalysis_q if date_q==tq(`1') | date_q==tq(`2') | date_q==tq(`3'), clear
			gen geo = "`geolab'"
			gen inds = "`inds'"
			gen bonds = "`bondlab'"
			if "`inds'"=="Fin" {
				keep if firm_type==1
			}
			if "`inds'"=="NonFin" {
				keep if firm_type==2
			}
			if "`geolab'"=="inotj" {
				drop if DomicileCountryId==iso_country_code
			}
			if "`geolab'"=="inotj_exUSAEMU" {
				drop if DomicileCountryId==iso_country_code | inlist(DomicileCountryId,"USA","EMU") | inlist(iso_country_code,"USA","EMU")
			}
			if "`bondlab'"=="B" {
				keep if mns_class=="B" & !missing(currency_id)
			}
			if "`bondlab'"=="BS" {
				keep if mns_class=="B" & !missing(currency_id) & inlist(mns_subclass,"S","A","LS")
			}
			if "`bondlab'"=="BC" {
				keep if mns_class=="B" & !missing(currency_id) & !inlist(mns_subclass,"S","A","LS","SV","SF")
			}
			gen usd_bonds = marketvalue_usd if currency_id=="USD" 
			gen eur_bonds = marketvalue_usd if currency_id=="EUR" 
			count
			if `r(N)'>0 {
				collapse (sum) usd_bonds eur_bonds tot_bonds=marketvalue_usd (lastnm) geo bonds inds, by(date_q)
				gen usd_share = usd_bonds/tot_bonds
				gen eur_share = eur_bonds/tot_bonds
				drop *_bonds
				gen usd_share999999 = usd_share[3]-usd_share[1]  
				gen eur_share999999 = eur_share[3]-eur_share[1]
				reshape wide usd_share eur_share, i(geo bonds inds) j(date_q)
				order geo bonds usd* eur* inds*
			}
			save $resultstemp/TimeSeries_Table_`bondlab'_`geolab'_`inds', emptyok replace
		}
	}
}

clear
foreach bondlab in "B" "BS" "BC" {
	foreach geolab in "World" "inotj" "inotj_exUSAEMU" {
		foreach inds in "All" "Fin" "NonFin" {
			append using $resultstemp/TimeSeries_Table_`bondlab'_`geolab'_`inds'
		}
	}
}
keep geo bonds inds usd_share* eur_share* 
reshape long eur_share usd_share, i(geo bonds inds) j(period)
rename (usd_share eur_share) (shareusd shareeur)
reshape long share, i(geo bonds inds period) j(currency) string
gen quarterlab = "1" if period==tq(`1')
replace quarterlab = "2" if period==tq(`2')
replace quarterlab = "3" if period==tq(`3')
replace quarterlab = "Long_Diff" if period==999999
drop period
reshape wide share, i(geo bonds inds currency) j(quarterlab) string
gen rank = 1 if geo=="World" & bonds=="B" & currency=="usd" & inds=="All"
replace rank = 2 if geo=="World" & bonds=="B" & currency=="eur" & inds=="All"
replace rank = 3 if geo=="inotj" & bonds=="B" & currency=="usd" & inds=="All"
replace rank = 4 if geo=="inotj" & bonds=="B" & currency=="eur" & inds=="All"
replace rank = 5 if geo=="inotj" & bonds=="BS" & currency=="usd" & inds=="All"
replace rank = 6 if geo=="inotj" & bonds=="BS" & currency=="eur" & inds=="All"
replace rank = 7 if geo=="inotj" & bonds=="BC" & currency=="usd" & inds=="All"
replace rank = 8 if geo=="inotj" & bonds=="BC" & currency=="eur" & inds=="All"
replace rank = 9 if geo=="inotj" & bonds=="BC" & currency=="usd" & inds=="Fin"
replace rank = 10 if geo=="inotj" & bonds=="BC" & currency=="eur" & inds=="Fin"
replace rank = 11 if geo=="inotj" & bonds=="BC" & currency=="usd" & inds=="NonFin"
replace rank = 12 if geo=="inotj" & bonds=="BC" & currency=="eur" & inds=="NonFin"
replace rank = 13 if geo=="inotj_exUSAEMU" & bonds=="BC" & currency=="usd" & inds=="All"
replace rank = 14 if geo=="inotj_exUSAEMU" & bonds=="BC" & currency=="eur"& inds=="All"

drop if missing(rank)
sort rank

save $resultstemp/tables/TimeSeries_Table, replace

log close
