* This file generates our the regressions results found in Tables 2-4 
*and the appendix regression tables A.2, A.3, A.6, A.9, A.10, and A.11

cap log close
log using "$logs/${whoami}_Regressions_`1'", replace
local test=0

local year = 2004+`1'
local standard_errors "vce(cluster cusip6)"
local regressions_year "$regressions/`year'"


	!rm -rf `regressions_year'
	cap mkdir `regressions_year'
	*Specify which types of bonds to run regressions for. 
	local bondtype_list "BCnotLSSV BCBO B" 
	*Specify regression type.  Options are wls for weighted least squares, or ols for Ordinary Least Squares
	local regtype_list "wls"
	*define LHS variable as the share of a security owned by investors from a given country
	local lhs="secshare"
					

foreach bondtype of local bondtype_list {
		foreach regtype of local regtype_list {		

			if "`bondtype'"=="BCBO" {
				use cgs_dom firm_type iso_country_code mns_class mns_sub cusip cusip6 mns_subclass marketvalue_usd DomicileCountryId coupon maturitydate currency_id using "$resultstemp/portfolio_sumstat_`year'_y.dta" if (mns_class=="B"  & DomicileCountryId~="" & cusip6~="" & !inlist(mns_subclass,"S","A")), clear
				drop if firm_type==0
			}
			if "`bondtype'"=="B" {
				use cgs_dom firm_type iso_country_code mns_class mns_sub cusip cusip6 mns_subclass marketvalue_usd DomicileCountryId coupon maturitydate currency_id using "$resultstemp/portfolio_sumstat_`year'_y.dta" if (mns_class=="B"  & DomicileCountryId~="" & cusip6~=""), clear
				replace firm_type=0 if  (mns_sub=="S" | mns_sub=="A" | mns_sub=="LS" | mns_sub=="SV")
			}

			if "`bondtype'"=="BCnotLSSV" {
				use cgs_dom firm_type iso_country_code mns_class mns_sub cusip cusip6 mns_subclass marketvalue_usd DomicileCountryId coupon maturitydate currency_id using "$resultstemp/portfolio_sumstat_`year'_y.dta" if (mns_class=="B"  & DomicileCountryId~="" & cusip6~="" & !inlist(mns_subclass,"S","A","LS","SV","SF")), clear
				drop if firm_type==0
			}
	
			if "`regtype'"=="wls" {
				local wls_command=" [aweight=security_total]"
				local wls_ind="Yes"
			}
			if "`regtype'"=="ols" {
				local wls_command=""
				local wls_ind="No"
			}
			
	 		replace cgs_dom="EMU" if inlist(cgs_dom,$eu1)==1 |  inlist(cgs_dom,$eu2)==1 |  inlist(cgs_dom,$eu3)==1
			cap destring coupon, force replace
			collapse (sum) marketvalue_usd (firstnm) maturitydate coupon cgs_dom firm_type currency_id iso_country_code, by(cusip*  DomicileCountryId )
			levelsof Dom, local(alldom)
			*IMPOSE CONSISTENCY WITHIN CUSIP9
			bysort cusip6: egen firm_type_temp=mode(firm_type)
			replace firm_type=firm_type_temp 	
			reshape wide  marketvalue_usd , i(currency_id cusip* iso_country_code maturitydate coupon cgs) j(DomicileCountryId) str
			collapse (sum) marketvalue_usd* (firstnm) maturitydate coupon  firm_type cgs_dom currency_id iso_country_code, by( cusip* )
			drop if missing(currency_id)
			renpfix marketvalue_usd
			foreach x of local alldom {
				cap replace `x'=0 if `x'==. | `x'<0
			}	

			*Define the controls to be dummies for maturity and coupon
			local controls "i.matdum i.coupondum"
			local controldrop "matdum coupondum"
			
			*Confirm all variables exist
			isvar $ctygroupA_list
			
			*Only keep investments to source counries
			keep if inlist(iso_co,$ctygroupA1)==1 | inlist(iso_co,$ctygroupA2)==1 

			*Make missing=0 if there are any non-zero holdings for a single country	
			gen missing=1
			foreach x in `r(varlist)' {
				replace missing=0 if `x'>0
			}	
			keep if missing==0
			drop missing

			foreach x in `r(varlist)'  {
				gen `x'_ind=0
				replace `x'_ind=1 if iso_country_code=="`x'"
				gen `x'_currind=0
				gen `x'_cgsind=0
				replace `x'_cgsind=1 if cgs_dom=="`x'"	
			}
			
			*Define each country's home currency
			cap replace AUS_currind=1 if currency_id=="AUD"
			cap replace CAN_currind=1 if currency_id=="CAD"
			cap replace CHE_currind=1 if currency_id=="CHF" 
			cap replace CHL_currind=1 if currency_id=="CLP"
			cap replace DNK_currind=1 if currency_id=="DKK"  
			cap replace EMU_currind=1 if currency_id=="EUR" 
			cap replace GBR_currind=1 if currency_id=="GBP" 
			cap replace MEX_currind=1 if currency_id=="MXN" 
			cap replace NZL_currind=1 if currency_id=="NZD" 		
			cap replace NOR_currind=1 if currency_id=="NOK" 
			cap replace SWE_currind=1 if currency_id=="SEK" 
			cap replace USA_currind=1 if currency_id=="USD"

			*currency dummies
			gen usd_ind=0
			replace usd_ind=1 if USA_currind==1
			cap gen eur_ind=0
			cap replace eur_ind=1 if EMU_currind==1
			
			*Calculate the total market value of each security in the dataset
			gen security_total=0
			foreach x of local alldom  {
				replace  security_total=security_total+`x'
			}

			*Calculate the share of a security owned by investor's from each source country
			foreach x in `r(varlist)' {
				display "`x'_secshare"
				gen `x'_secshare=`x'/security_total
			}	
			
			egen total=sum(security_total)
			
			isvar $ctygroupA_list
			foreach x in `r(varlist)' {
				egen `x'_total=sum(`x')
			}			
			
			gen mat=(maturitydate-td(31dec`year'))/365

			*Create Maturity Bins
			gen matdum=.
			replace matdum=0 if mat==.
			replace matdum=1 if mat<=2 & mat~=.
			replace matdum=2 if mat>2 & mat<=5 & mat~=.
			replace matdum=3 if mat>5 & mat<=10 & mat~=.
			replace matdum=4 if mat>10

			*Create Coupon bins
			gen coupondum=. 
			cap destring coupon, force replace
			replace coupondum=0 if coupon==.
			replace coupondum=1 if coupon<=1 & coupon~=.
			replace coupondum=2 if coupon>1 & coupon<=2 & coupon~=.
			replace coupondum=3 if coupon>2 & coupon<=3 & coupon~=.
			replace coupondum=4 if coupon>3 & coupon<=4 & coupon~=.
			replace coupondum=5 if coupon>4 & coupon<=5 & coupon~=.
			replace coupondum=6 if coupon>5 & coupon<=6 & coupon~=.
			replace coupondum=7 if coupon>6 & coupon~=.
			
			gen firm_type_string=""
			replace firm_type_string="Finance" if firm_type==1
			replace firm_type_string="Non-Finance" if firm_type==2
			replace firm_type_string="Government" if firm_type==0

			label define firm_type_label 0 "Government" 1 "Fin" 2 "Non-Fin"
			label values firm_type firm_type_label

			*Foreign issue: Variable for whether bond is issued outside of firm's nationality
			gen foreign_issue=0
			replace foreign_issue=1 if iso_co~=cgs_dom

			
			isvar $ctygroupA_list
			local temp=""
			foreach x in `r(varlist)' {
				drop `x'
				local temp="`temp' `x'*"
			}	
			keep  cusip* `temp' matdum coupondum firm_type security_total foreign_issue cgs_dom eur_ind usd_ind
			reshape long `r(varlist)', i(cusip* mat coupon firm_type foreign_issue security_total eur_ind usd_ind) j(var) str


			foreach x in $droplist_reg {
			cap drop `x'*
			}

			isvar $ctygroupA_list
			foreach x in `r(varlist)' {
				rename `x' xxx`x'
				}
				
			reshape long xxx, i(cusip* mat coupon var security_total) j(Dom) str
			reshape wide xxx, i(cusip* mat coupon Dom security_total) j(var) str
			renpfix xxx_

			encode Dom, gen(domid)
			encode cgs_domicile, gen(cgs_domid)
			gen country_currency=ind*currind
			label var currind "Currency"
			label var ind "Country"
			label var country_currency "CurrencyXCountry"

			gen country_string="Home" if ind==1
			replace country_string="Foreign" if ind==0
			gen cgs_string="Home-CGS" if cgsind==1
			replace cgs_string="Foreign-CGS" if cgsind==0
			gen currency_string="LC" if currind==1
			replace currency_string="FC" if currind==0
			gen firm_type_string="Non-Finance" if firm_type==2
			replace firm_type_string="Finance" if firm_type==1
			if "`bondtype'"=="BS" | "`bondtype'"=="B" {
			replace firm_type_string="Government" if firm_type==0
			}

			egen groupvar=group(country_string currency_string firm_type_string), label
			gen group_label=country_string + " " + currency_string + " " + firm_type_string if groupvar~=.
					
			egen cgsgroup=group(country_string currency_string cgs_string), label
			gen cgsgroup_label=country_string + " " + currency_string + " " + cgs_string if cgsgroup~=.

			*CGS GROUP MANUAL
			gen nationality=ind
			gen residency=cgsind
			gen nationality_x_residency=ind*cgsind
			gen nationality_x_currency=ind*currind
			gen residency_x_currency=cgsind*currind
			gen nationality_x_resid_x_curr=ind*cgsind*currind

			*multi-currency indicator
			bysort cusip6 Dom: egen lc_temp=max(currind)
			bysort cusip6 Dom: egen fc_temp=min(currind)
			gen both_curr=0
			replace both_curr=1 if lc_temp==1 & fc_temp==0

			*Foreign Regression 
			gen foreignissue_x_currency=0
			replace foreignissue_x_currency=1 if foreign_issue==1 & currind==1

			*labmask groupvar, values(group_label)
			label values groupvar groupvar
			label values cgsgroup cgsgroup
			label var ind "Home Country"
			label var currind "LC"	
						
			cap xi i.mat i.coupon

			* Merge in governing law data
			mmerge cusip using "$sdc_datasets/sdc_governing_law", unmatched(m)
			gen own_law = .
			replace own_law = 1 if Dom == governing_law & ~missing(governing_law)
			replace own_law = 0 if Dom != governing_law & ~missing(governing_law)
			gen own_law_x_currency=own_law*currind

			* Merge in foreign sales shares
			global segment_temp $temp/probits/segment
			mmerge cusip6 using $segment_temp/segment_clean_2017.dta, unmatched(m) umatch(cusip6_up_bg) ukeep(foreign_sales_share)
			rename _merge _forshare_merge
			xtile forshare_quartile = foreign_sales_share, n(4)

			cap mkdir "$temp/regdata"
			save "$temp/regdata/reg_`year'.dta", replace

			levelsof (Dom), local (Dom)
			foreach x of local Dom {
						
				*Table 2: Home Currency Bias: Within-Firm Variation
				areg `lhs' currind `controls' `wls_command' if (Dom=="`x'"), abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfe_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta		

				*Table 4: Home-Country Bias and Home-Currency Bias
				reg `lhs' ind `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/homebias_`regtype'_`lhs'_`bondtype'_`year'.xls, noaster keep(ind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, No, WLS, `wls_ind', Controls, No) excel tex dta		
				reg `lhs' currind  `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/homebias_`regtype'_`lhs'_`bondtype'_`year'.xls, noaster keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, No, WLS, `wls_ind', Controls, No)   excel tex dta			
				reg `lhs' ind currind `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/homebias_`regtype'_`lhs'_`bondtype'_`year'.xls, noaster keep(ind currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, No, WLS, `wls_ind', Controls, No)   excel tex dta				

				*Table 4 with Asterisks for statistical significance levels 
				reg `lhs' ind `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/aster_`regtype'_`lhs'_`bondtype'_`year'.xls,  keep(ind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, No, WLS, `wls_ind', Controls, No) excel tex dta		
				reg `lhs' currind  `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/aster_`regtype'_`lhs'_`bondtype'_`year'.xls,  keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, No, WLS, `wls_ind', Controls, No)   excel tex dta			
				reg `lhs' ind currind `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/aster_`regtype'_`lhs'_`bondtype'_`year'.xls,  keep(ind currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, No, WLS, `wls_ind', Controls, No)   excel tex dta				
										
				*Country, Currency, and Country*Currency Interaction, with controls, NO FE
				reg `lhs' ind currind country_currency `controls' `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/baseline_`lhs'_`bondtype'_`year'.xls, keep(ind currind country_currency)  dec(3) ctitle("`x'")	addtext(Controls, Yes, WLS, `wls_ind')  label excel tex dta

				*Country, Currency, and Country*Currency Interaction, without controls
				reg `lhs' ind currind country_currency `wls_command' if Dom=="`x'", `standard_errors'
				outreg2 using `regressions_year'/baseline_inter_`lhs'_`bondtype'_`year'.xls, keep(ind currind country_currency)  dec(3) ctitle("`x'")	addtext(Controls, No, WLS, `wls_ind')  label excel tex dta
						
						
				****************************************				
				*FE Regressions for Table 3: Robustness*
				****************************************
				
				*FE Regression, with external debt only
				areg `lhs' currind `controls' `wls_command' if (Dom=="`x'" & ind==0), abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/external_firmfe_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta							

				*FE regressions for financial and non-financial firms separately
				areg `lhs' currind `controls' `wls_command' if Dom=="`x'", abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_ALL")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
				areg `lhs' currind `controls' `wls_command' if firm_type==2 & Dom=="`x'", abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_NonFin")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
				areg `lhs' currind `controls' `wls_command' if firm_type==1 & Dom=="`x'", abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_Fin")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
				areg `lhs' currind `controls' `wls_command' if firm_type==. & Dom=="`x'", abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_Uncl")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
		
				*FE regressions for external debt only,separately for financial and non-financial firms
				areg `lhs' currind `controls' `wls_command' if Dom=="`x'" & ind==0, abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/external_firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_ext_all")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
				areg `lhs' currind `controls' `wls_command' if firm_type==2 & Dom=="`x'" & ind==0, abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/external_firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_ext_nonfin")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
				areg `lhs' currind `controls' `wls_command' if firm_type==1 & Dom=="`x'" & ind==0, abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/external_firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_extfin")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
				areg `lhs' currind `controls' `wls_command' if firm_type==. & Dom=="`x'" & ind==0, abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/external_firmfefull_`regtype'_`lhs'_`bondtype'_`year'.xls, keep(currind) dec(3) ctitle("`x'_extuncl")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta
								
				*Firm FE regressions with a control for the location where a bond is issued (CGS domicile)
				areg `lhs' residency currind residency_x_currency `controls' `wls_command' if (Dom=="`x'"), abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfe_cgs_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(residency currind residency_x_currency) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta					
				areg `lhs' residency currind `controls' `wls_command' if (Dom=="`x'"), abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfe_cgs_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(residency currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta					
				areg `lhs' residency currind  `controls' `wls_command' if (Dom=="`x'" & ind==0), abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfe_cgs_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(residency currind) dec(3) ctitle("`x'_External")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta					
				areg `lhs' residency currind  `controls' `wls_command' if (Dom=="`x'" & ind==0 & firm_type==2), abs(cusip6) `standard_errors'
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/firmfe_cgs_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(residency currind) dec(3) ctitle("`x'_External_Nonfin")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta														

				
				*FE Regressions, restricted to sample with non-missing foreign sales share, by quartile
				cap {
					areg `lhs' currind `controls' `wls_command' if (Dom=="`x'") & ~missing(foreign_sales_share), abs(cusip6) `standard_errors'
					local temp=e(df_a)+1
					outreg2 using `regressions_year'/firmfe_forshare_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta	
				}

				cap {
					forval qtile = 1/4 {
						areg `lhs' currind `controls' `wls_command' if (Dom=="`x'") & ~missing(foreign_sales_share) & forshare_quartile == `qtile', abs(cusip6) `standard_errors'
						local temp=e(df_a)+1
						outreg2 using `regressions_year'/firmfe_forshare_q`qtile'_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta		
					}

				}
				
				*Firm FE regressions with a control for the country of the bond's governing law
				cap {
					areg `lhs' own_law currind own_law_x_currency `controls' `wls_command' if (Dom=="`x'"), abs(cusip6) `standard_errors'
					local temp=e(df_a)+1
					outreg2 using `regressions_year'/firmfe_govlaw_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(own_law currind own_law_x_currency) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta					
					areg `lhs' own_law currind `controls' `wls_command' if (Dom=="`x'"), abs(cusip6) `standard_errors'
					local temp=e(df_a)+1
					outreg2 using `regressions_year'/firmfe_govlaw_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(own_law currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta					
					areg `lhs' own_law currind  `controls' `wls_command' if (Dom=="`x'" & ind==0), abs(cusip6) `standard_errors'
					local temp=e(df_a)+1
					outreg2 using `regressions_year'/firmfe_govlaw_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(own_law currind) dec(3) ctitle("`x'_External")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta					
					areg `lhs' own_law currind  `controls' `wls_command' if (Dom=="`x'" & ind==0 & firm_type==2), abs(cusip6) `standard_errors'
					local temp=e(df_a)+1
					outreg2 using `regressions_year'/firmfe_govlaw_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(own_law currind) dec(3) ctitle("`x'_External_Nonfin")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta														
				}

				*FE Regressions Separated by whether the bond is a foreign issue
				areg secshare currind  `controls' `wls_command' if Dom=="`x'", `standard_errors' abs(cusip6)
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/foreignissue_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta														
				areg secshare currind  `controls' `wls_command' if Dom=="`x'" & nationality==0, `standard_errors' abs(cusip6)
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/foreignissue_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'_EXT")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta														
				areg secshare currind  `controls' `wls_command' if Dom=="`x'" & nationality==0 & foreign_issue==1, `standard_errors' abs(cusip6)
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/foreignissue_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'_EXT_FOR")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta																	
				areg secshare currind  `controls' `wls_command' if Dom=="`x'" & nationality==0 & foreign_issue==1 & firm_type==2, `standard_errors' abs(cusip6)
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/foreignissue_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'_EXT_FOR_NFC")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta																	
				areg secshare currind  `controls' `wls_command' if Dom=="`x'" & nationality==0 & foreign_issue==1 & firm_type==2 & residency==0, `standard_errors' abs(cusip6)
				local temp=e(df_a)+1
				outreg2 using `regressions_year'/foreignissue_`regtype'_`lhs'_`bondtype'_`year'.xls, nocons keep(currind) dec(3) ctitle("`x'_EXT_FOR_NFC_NONRES")  addtext(Cusip6 FE, Yes, WLS, `wls_ind', Controls, Yes, Firms,`temp')  label excel tex dta																				
		}		
}




**************************************
* Organize and format the regressions*
**************************************
*Format regressions to be put into Robustness Table 
use "`regressions_year'/fe_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var9
rename _var2 type
rename _var4 beta
rename _var9 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
gsort reg -VAR
replace VAR="beta" if VAR=="currind"
replace VAR="Obs" if regexm(VAR,"Obs")
save "`regressions_year'/robustness1.dta", replace


use "`regressions_year'/foreignissue_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var7
rename _var2 type
rename _var4 beta
rename _var7 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
foreach x in $ctygroupA_list {
	cap rename `x' `x'_base
}	
reshape long $ctygroupA_list, i(VAR) j(reg) str
gsort reg VAR
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
save "`regressions_year'/robustness2.dta", replace 


use "`regressions_year'/firmfe_cgs_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var6 _var8 _var11
rename _var2 type
rename _var4 residency
rename _var6 beta
rename _var8 residency_beta
rename _var11 obs
replace type=type+"_cgs"
replace type=type+"_inter" if residency_beta~="" 
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
keep if reg=="_cgs"
drop if VAR=="residency_x_currency"
gsort reg VAR
rename VAR VARIABLES
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")	
save "`regressions_year'/robustness3.dta", replace


use "`regressions_year'/external_firmfefull_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var9
rename _var2 type
rename _var4 beta
rename _var9 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
gsort reg -VAR
cap replace reg="_extuncl" if reg=="_uncl"
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
save "`regressions_year'/robustness4.dta", replace


use "`regressions_year'/firmfefull_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var9
rename _var2 type
rename _var4 beta
rename _var9 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
gsort reg -VAR
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
save "`regressions_year'/robustness5.dta", replace	


use "`regressions_year'/firmfe_wls_secshare_BCBO_`year'_dta.dta", clear
drop if _n==1
foreach x of varlist _all {
	local temp=`x'[1]
	rename `x' `temp'
}	
keep if VAR=="LC" | VAR=="Observations"
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gen reg="BCBO" 
order reg 
save "`regressions_year'/robustness_bcbo.dta", replace


use "`regressions_year'/firmfe_forshare_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
drop if _n==1
foreach x of varlist _all {
	local temp=`x'[1]
	rename `x' `temp'
}	
keep if VAR=="LC" | VAR=="Observations"
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gen reg="_forshare_full" 
order reg 
save "`regressions_year'/robustness_forshare_full.dta", replace


forval qtile = 1/4 {
	use "`regressions_year'/firmfe_forshare_q`qtile'_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
	drop if _n==1
	foreach x of varlist _all {
		local temp=`x'[1]
		rename `x' `temp'
	}	
	keep if VAR=="LC" | VAR=="Observations"
	replace VAR="beta" if VAR=="LC"
	replace VAR="Obs" if regexm(VAR,"Obs")
	gen reg="_forshare_q`qtile'" 
	order reg 
	save "`regressions_year'/robustness_forshare_q`qtile'.dta", replace
}

use "`regressions_year'/firmfe_govlaw_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var6 _var8 _var11
rename _var2 type
rename _var4 own_law
rename _var6 beta
rename _var8 own_law_beta
rename _var11 obs
replace type=type+"_govlaw"
replace type=type+"_inter" if own_law_beta~="" 
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
keep if reg=="_govlaw"
drop if VAR=="own_law_x_currency"
gsort reg VAR
rename VAR VARIABLES
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
cap drop _var*
save "`regressions_year'/robustness_own_law.dta", replace


use "`regressions_year'/firmfe_wls_secshare_B_`year'_dta.dta", clear
drop if _n==1
foreach x of varlist _all {
	local temp=`x'[1]
	rename `x' `temp'
}	
keep if VAR=="LC" | VAR=="Observations"
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gen reg="B" 
order reg 
save "`regressions_year'/robustness_b.dta", replace


use "`regressions_year'/robustness1.dta", clear
append using "`regressions_year'/robustness2.dta"
append using "`regressions_year'/robustness3.dta"
append using "`regressions_year'/robustness4.dta"
append using "`regressions_year'/robustness5.dta"
append using "`regressions_year'/robustness_b.dta"
append using "`regressions_year'/robustness_bcbo.dta"
append using "`regressions_year'/robustness_own_law.dta"
append using "`regressions_year'/robustness_forshare_full.dta"
append using "`regressions_year'/robustness_forshare_q1.dta"
append using "`regressions_year'/robustness_forshare_q2.dta"
append using "`regressions_year'/robustness_forshare_q3.dta"
append using "`regressions_year'/robustness_forshare_q4.dta"


gen varorder=.
replace varorder=1 if VAR=="beta"
replace varorder=2 if VAR=="residency"
replace varorder=2 if VAR=="own_law"
replace varorder=3 if VAR=="Obs"
gen regnum=.
replace regnum=1 if reg=="_MC"
replace regnum=2 if reg=="_EXT"
replace regnum=3 if reg=="_EXT_FOR"
replace regnum=4 if reg=="_Fin"
replace regnum=5 if reg=="_NonFin"
replace regnum=6 if reg=="_extfin"
replace regnum=7 if reg=="_ext_nonfin"
replace regnum=8 if reg=="BCBO"
replace regnum=9 if reg=="B"
replace regnum=10 if reg=="_cgs"
replace regnum=11 if reg=="_govlaw"
replace regnum=12 if reg=="_forshare_full"
replace regnum=13 if reg=="_forshare_q1"
replace regnum=14 if reg=="_forshare_q2"
replace regnum=15 if reg=="_forshare_q3"
replace regnum=16 if reg=="_forshare_q4"
keep if regnum~=.
gsort regnum varorder
order varorder regnum
export excel using "`regressions_year'/robustness_table_`year'.xls", firstrow(variables) replace




*Format Table 4: Home-Country Bias and Home-Currency Bias
use `regressions_year'/homebias_wls_secshare_BCnotLSSV_`year'_dta.dta, clear
sxpose, clear
keep _var2 _var4 _var6 _var12
rename _var2 iso
rename _var4 country
rename _var6 currency
rename _var12 r2
drop if _n==1
gen n=_n
save `regressions_year'/homebias_temp.dta, replace


use `regressions_year'/homebias_temp.dta, clear
keep if mod(n,3)==1
keep iso country r2
foreach x in country r2 {
rename `x' spec1_`x'
}
save `regressions_year'/homebias_1.dta, replace


use `regressions_year'/homebias_temp.dta, clear
keep if mod(n,3)==2
keep iso currency r2
foreach x in currency r2 {
rename `x' spec2_`x'
}
save `regressions_year'/homebias_2.dta, replace


use `regressions_year'/homebias_temp.dta, clear
keep if mod(n,3)==0
keep iso country currency r2
foreach x in country currency r2 {
rename `x' spec3_`x'
}
save `regressions_year'/homebias_3.dta, replace


use  `regressions_year'/homebias_1.dta, clear
mmerge iso using `regressions_year'/homebias_2.dta
mmerge iso using `regressions_year'/homebias_3.dta
drop _merge


forvalues x=1/6 {
	gen s`x'=""
}	


order iso s1 s2 spec1_country spec1_r2 s3 s4 spec2_currency spec2_r2 s5 s6 spec3_country spec3_currency spec3_r2
save `regressions_year'/homebias.dta, replace
append using  `regressions_year'/homebias.dta
sort iso
forvalues i=2(2)20 {
	foreach x of varlist _all {
		replace `x'="" if _n==`i'
	}
}
export excel using "`regressions_year'/homebias_table_`year'.xls", firstrow(variables) replace


* Home bias table with standard errors
cap mkdir $resultstemp/regoutput_panel/alternate_tables_se
use `regressions_year'/homebias_wls_secshare_BCnotLSSV_`year'_dta.dta, clear
export excel using "$resultstemp/regoutput_panel/alternate_tables_se/homebias_table_`year'.xls", firstrow(variables) replace


* Robustness table with standard errors
use "`regressions_year'/fe_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var5 _var9
rename _var2 type
rename _var4 beta
rename _var5 se
rename _var9 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
replace VAR="SE" if VAR==""
replace VAR="beta" if VAR=="currind"
replace VAR="Obs" if regexm(VAR,"Obs")
gsort reg -VAR
save "`regressions_year'/robustness1_se.dta", replace


use "`regressions_year'/foreignissue_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var5 _var7
rename _var2 type
rename _var4 beta
rename _var5 se
rename _var7 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
foreach x in $ctygroupA_list {
	cap rename `x' `x'_base
}	
reshape long $ctygroupA_list, i(VAR) j(reg) str
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
replace VAR="SE" if missing(VAR)
gsort reg VAR
save "`regressions_year'/robustness2_se.dta", replace 


use "`regressions_year'/firmfe_cgs_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var5 _var6 _var7 _var8 _var11
rename _var2 type
rename _var4 residency
rename _var5 se
rename _var6 beta
rename _var7 aux_se
rename _var8 residency_beta
rename _var11 obs
replace type=type+"_cgs"
replace type=type+"_inter" if residency_beta~="" 
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
replace VAR = "SE_AUX" if _n == 5
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
keep if reg=="_cgs"
drop if VAR=="residency_x_currency"
rename VAR VARIABLES
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")	
replace VAR="SE" if missing(VAR)
gsort reg VAR
save "`regressions_year'/robustness3_se.dta", replace


use "`regressions_year'/external_firmfefull_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var5 _var9
rename _var2 type
rename _var4 beta
rename _var5 se
rename _var9 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
replace VAR = "SE" if missing(VAR)
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gsort reg -VAR
cap replace reg="_extuncl" if reg=="_uncl"
save "`regressions_year'/robustness4_se.dta", replace


use "`regressions_year'/firmfefull_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var5 _var9
rename _var2 type
rename _var4 beta
rename _var5 se
rename _var9 obs
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
replace VAR="SE" if missing(VAR)
gsort reg -VAR
save "`regressions_year'/robustness5_se.dta", replace	


use "`regressions_year'/firmfe_wls_secshare_BCBO_`year'_dta.dta", clear
drop if _n==1
foreach x of varlist _all {
	local temp=`x'[1]
	rename `x' `temp'
}	
replace VAR="SE" if _n == 4
keep if VAR=="LC" | VAR=="Observations" | VAR=="SE"
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gen reg="BCBO" 
order reg 
save "`regressions_year'/robustness_bcbo_se.dta", replace


use "`regressions_year'/firmfe_govlaw_wls_secshare_BCnotLSSV_`year'_dta.dta", clear
sxpose, clear		
keep _var2 _var4 _var5 _var6 _var7 _var8 _var11
rename _var2 type
rename _var4 own_law
rename _var5 se
rename _var6 beta
rename _var7 aux_se
rename _var8 own_law_beta
rename _var11 obs
replace type=type+"_govlaw"
replace type=type+"_inter" if own_law_beta~="" 
sxpose, clear
foreach x of varlist _all {
	local temp=`x'[1]
	cap rename `x' `temp'
}
replace VAR = "SE_AUX" if _n == 5
drop if _n==1
reshape long $ctygroupA_list, i(VAR) j(reg) str
keep if reg=="_govlaw"
drop if VAR=="own_law_x_currency"
replace VAR="SE" if missing(VAR)
rename VAR VARIABLES
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gsort reg VAR
cap drop _var*
save "`regressions_year'/robustness_own_law_se.dta", replace


use "`regressions_year'/firmfe_wls_secshare_B_`year'_dta.dta", clear
drop if _n==1
foreach x of varlist _all {
	local temp=`x'[1]
	rename `x' `temp'
}	
replace VAR="SE" if _n == 4
keep if VAR=="LC" | VAR=="Observations" | VAR=="SE"
replace VAR="beta" if VAR=="LC"
replace VAR="Obs" if regexm(VAR,"Obs")
gen reg="B" 
order reg 
save "`regressions_year'/robustness_b_se.dta", replace


use "`regressions_year'/robustness1_se.dta", clear
append using "`regressions_year'/robustness2_se.dta"
append using "`regressions_year'/robustness3_se.dta"
append using "`regressions_year'/robustness4_se.dta"
append using "`regressions_year'/robustness5_se.dta"
append using "`regressions_year'/robustness_b_se.dta"
append using "`regressions_year'/robustness_bcbo_se.dta"
append using "`regressions_year'/robustness_own_law_se.dta"

gen varorder=.
replace varorder=1 if VAR=="beta"
replace varorder=2 if VAR=="SE"
replace varorder=3 if VAR=="residency"
replace varorder=3 if VAR=="own_law"
replace varorder=4 if VAR=="SE_AUX"
replace varorder=5 if VAR=="Obs"
replace VAR = "SE" if VAR=="SE_AUX"
gen regnum=.
replace regnum=1 if reg=="_MC"
replace regnum=2 if reg=="_EXT"
replace regnum=3 if reg=="_EXT_FOR"
replace regnum=4 if reg=="_Fin"
replace regnum=5 if reg=="_NonFin"
replace regnum=6 if reg=="_extfin"
replace regnum=7 if reg=="_ext_nonfin"
replace regnum=8 if reg=="BCBO"
replace regnum=9 if reg=="B"
replace regnum=10 if reg=="_cgs"
replace regnum=11 if reg=="_govlaw"
keep if regnum~=.
gsort regnum varorder
order varorder regnum
export excel using "$resultstemp/regoutput_panel/alternate_tables_se/robustness_table_`year'.xls", firstrow(variables) replace

cap log close
