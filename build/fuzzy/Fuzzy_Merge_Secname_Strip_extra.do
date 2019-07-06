* --------------------------------------------------------------------------------------------------
* Fuzzy Merge: Security Name Cleaning, Extra Pass
*
* All files in this folder (fuzzy) handle the probabilistic record linkage of observations in the 
* Morningstar holdings data for which we lack a CUSIP identifier to other observations for which we
* do have an identifier. This allows us to assign a CUSIP to the former records via internal
* cross-linkage.
*
* This routine parses and cleans the security name field for all records, using as much regional
* institutional detail as possible. The methods here are largely based on those made available 
* by the NBER Patent Data Project (see https://sites.google.com/site/patentdataproject/Home).
* --------------------------------------------------------------------------------------------------

* Hardcoded setting: Whether to process rarer strings
local process_rare_strings = 0
local trim_strings_in_local_scope = 1

* Brazil: Strip unknown identifier in name before removing more punctuation
gen unknown_id_in_name = ""
foreach regex_pattern in "OPTER(\/.*$|$)" "KLAB(\/.*$|$)" "TER-\w.*$" "T-D[0-9](\/.*$|$)" ///
	"CMIG[0-9](\/.*$|$)" "BRAP[0-9](\/.*$|$)" "TERM[0-9](\/.*$|$)" ///
	"VIV\w(\/.*$|$)" "OIBR\w(\/.*$|$)" "TERM\w(\/.*$|$)" "TERMO(\/.*$|$)" ///
	"TER.+.\/(\w\w\w\w).*$" "TER-.(\/.*$|$)" "PAT(\/.*$|$)" "BPNM(\/.*$|$)" ///
	"PANA(\/.*$|$)" "CZRS(\/.*$|$)" "BRKM(\/.*$|$)" "EDGA(\/.*$|$)" "BSLI(\/.*$|$)" ///
	"TERM\w(\/.*$|$)" "OCPAG-.*$" "EDGA(\/.*$|$)" "CRPG(\/.*$|$)" "BRSR[0-9](\/.*$|$)" ///
	"ITSA[0-9](\/.*$|$)" "PETR[0-9](\/.*$|$)" "ITUB[0-9](\/.*$|$)" "BBDC[0-9](\/.*$|$)" ///
	"VALE[0-9](\/.*$|$)" "GGBR[0-9](\/.*$|$)" "PCAR[0-9](\/.*$|$)" "SUZB[0-9](\/.*$|$)" ///
	"BRKM[0-9](\/.*$|$)" "BTTL[0-9](\/.*$|$)" "CESP[0-9](\/.*$|$)" "CGRA[0-9](\/.*$|$)" ///
	"DAYC[0-9](\/.*$|$)" "ELPL[0-9](\/.*$|$)" "GOAU[0-9](\/.*$|$)" "GOLL[0-9](\/.*$|$)" ///
	"IDVL[0-9](\/.*$|$)" "JBSS[0-9](\/.*$|$)" "JPPC[0-9](\/.*$|$)" "TRMP\w(\/.*$|$)" ///
	"(ITSA|ITUB)[0-9].*$" {
	
	replace unknown_id_in_name = trim(regexs(0)) if regexm(securityname_cln, "`regex_pattern'") ///
		& unknown_id_in_name==""
	replace securityname_cln = regexr(securityname_cln, "`regex_pattern'", "")
	
}

* Extra punctuation removal
replace securityname_cln = subinstr(securityname_cln,  "/",  " ", 30)
replace securityname_cln = subinstr(securityname_cln,  "*",  " ", 30)
replace securityname_cln = subinstr(securityname_cln,  "-",  " ", 30)
replace securityname_cln = subinstr(securityname_cln,  "|", " ", 30)
replace securityname_cln = subinstr(securityname_cln,  "^",  "", 30)
replace securityname_cln = subinstr(securityname_cln,  "$",  "", 30)
replace securityname_cln = subinstr(securityname_cln,  "'", "", 30)
replace securityname_cln = subinstr(securityname_cln,  "-", " ", 30)
replace securityname_cln = subinstr(securityname_cln,  "  ", " ", 30)

* Generate extra security descriptors field from previously generated columns
gen extra_security_descriptors = ""
replace extra_security_descriptors = extra_security_descriptors + ", " + name_firm_categ if name_firm_categ != ""
replace extra_security_descriptors = extra_security_descriptors + ", " + name_jurisdict_categ if name_jurisdict_categ != ""
replace extra_security_descriptors = extra_security_descriptors + ", " + name_bond_type if name_bond_type != ""
replace extra_security_descriptors = extra_security_descriptors + ", " + name_bond_legal if name_bond_legal != ""

* Generate descriptors from common corporate naming patterns; strip from security name
foreach regex_pattern in "(^| )& BRO( |$)" ///
	"(^| )& BROTHER( |$)" ///
	"(^| )& C( |$)" ///
	"(^| )& CIA( |$)" ///
	"(^| )& CIE( |$)" ///
	"(^| )& CO( |$)" ///
	"(^| )& FILS( |$)" ///
	"(^| )& PARTNER( |$)" ///
	"(^| )& SOEHNE( |$)" ///
	"(^| )& SOHN( |$)" ///
	"(^| )& SON( |$)" ///
	"(^| )& SONS( |$)" ///
	"(^| )& ZN( |$)" ///
	"(^| )& ZONEN( |$)" ///
	"(^| )A B( |$)" ///
	"(^| )A CALIFORNIA CORP( |$)" ///
	"(^| )A DELAWARE CORP( |$)" ///
	"(^| )A G( |$)" ///
	"(^| )A RL( |$)" ///
	"(^| )A S( |$)" ///
	"(^| )A/S( |$)" ///
	"(^| )AANSPRAKELIJKHEID( |$)" ///
	"(^| )AB( |$)" ///
	"(^| )ACADEMY( |$)" ///
	"(^| )ACCIONES( |$)" ///
	"(^| )ACTIEN GESELLSCHAFT( |$)" ///
	"(^| )ACTIENGESELLSCHAFT( |$)" ///
	"(^| )AD( |$)" ///
	"(^| )AG( |$)" ///
	"(^| )AGG( |$)" ///
	"(^| )AGRICOLA( |$)" ///
	"(^| )AGRICOLAS( |$)" ///
	"(^| )AGRICOLE( |$)" ///
	"(^| )AGRICOLES( |$)" ///
	"(^| )AGRICOLI( |$)" ///
	"(^| )AGRICOLTURE( |$)" ///
	"(^| )AGRICULTURA( |$)" ///
	"(^| )AGRICULTURAL( |$)" ///
	"(^| )AGRICULTURE( |$)" ///
	"(^| )AKTIEN( |$)" ///
	"(^| )AS( |$)" ///
	"(^| )ASA( |$)" ///
	"(^| )ASKTIENGESELLSCHAFT( |$)" ///
	"(^| )ASOCIADOS( |$)" ///
	"(^| )ASSCOIATES( |$)" ///
	"(^| )ASSOCIACAO( |$)" ///
	"(^| )ASSOCIADOS( |$)" ///
	"(^| )ASSOCIATE( |$)" ///
	"(^| )ASSOCIATED( |$)" ///
	"(^| )ASSOCIATES( |$)" ///
	"(^| )ASSOCIATI( |$)" ///
	"(^| )ASSOCIATION( |$)" ///
	"(^| )ASSOCIATO( |$)" ///
	"(^| )ASSOCIES( |$)" ///
	"(^| )ASSSOCIATES( |$)" ///
	"(^| )ATELIER( |$)" ///
	"(^| )ATELIERS( |$)" ///
	"(^| )ATIBOLAG( |$)" ///
	"(^| )ATKIEBOLAG( |$)" ///
	"(^| )BANK( |$)" ///
	"(^| )BK( |$)" ///
	"(^| )BRANDS( |$)" ///
	"(^| )BROS( |$)" ///
	"(^| )BROTHERS( |$)" ///
	"(^| )BUSINESS( |$)" ///
	"(^| )BV( |$)" ///
	"(^| )BV:( |$)" ///
	"(^| )BV?( |$)" ///
	"(^| )CC( |$)" ///
	"(^| )CENTER( |$)" ///
	"(^| )CENTRAL( |$)" ///
	"(^| )CENTRALE( |$)" ///
	"(^| )CENTRE( |$)" ///
	"(^| )CENTRO( |$)" ///
	"(^| )CHEMICAL( |$)" ///
	"(^| )CHEMICALS( |$)" ///
	"(^| )CHEMICZNE( |$)" ///
	"(^| )CHEMIE( |$)" ///
	"(^| )CIE( |$)" ///
	"(^| )CO( |$)" ///
	"(^| )CO:( |$)" ///
	"(^| )COMERCIAL( |$)" ///
	"(^| )COMERCIO( |$)" ///
	"(^| )COMMERCIAL( |$)" ///
	"(^| )COMMERCIALE( |$)" ///
	"(^| )COMMON STOCK( |$)" ///
	"(^| )COMP( |$)" ///
	"(^| )COMPAGNI( |$)" ///
	"(^| )COMPAGNIA( |$)" ///
	"(^| )COMPANH( |$)" ///
	"(^| )COMPANIA( |$)" ///
	"(^| )COMPANIE( |$)" ///
	"(^| )COMPANY( |$)" ///
	"(^| )COMPAY( |$)" ///
	"(^| )CONSOLIDATED( |$)" ///
	"(^| )CONSTRUCCION( |$)" ///
	"(^| )CONSTRUCCIONE( |$)" ///
	"(^| )CONSTRUCCIONES( |$)" ///
	"(^| )CONSTRUCTION( |$)" ///
	"(^| )CONSTRUCTIONS( |$)" ///
	"(^| )CONSULTING( |$)" ///
	"(^| )COOP( |$)" ///
	"(^| )COPORATION( |$)" ///
	"(^| )CORP( |$)" ///
	"(^| )CORPN( |$)" ///
	"(^| )CORPO( |$)" ///
	"(^| )CV( |$)" ///
	"(^| )DD( |$)" ///
	"(^| )DEPARTMENT( |$)" ///
	"(^| )DEUTSCHLAND( |$)" ///
	"(^| )DEVELOP( |$)" ///
	"(^| )ENGINEERING( |$)" ///
	"(^| )ENGINEERS( |$)" ///
	"(^| )ENGINES( |$)" ///
	"(^| )ENTERPRISE( |$)" ///
	"(^| )ENTREPRISE( |$)" ///
	"(^| )ENTREPRISES( |$)" ///
	"(^| )EQUIP( |$)" ///
	"(^| )EST( |$)" ///
	"(^| )ETS( |$)" ///
	"(^| )EUROPE( |$)" ///
	"(^| )EUROPEA( |$)" ///
	"(^| )EUROPEAN( |$)" ///
	"(^| )FABRIC( |$)" ///
	"(^| )FABRIK( |$)" ///
	"(^| )FACTORY( |$)" ///
	"(^| )FILM( |$)" ///
	"(^| )FINANCIERE( |$)" ///
	"(^| )FIRM( |$)" ///
	"(^| )FOUNDATION( |$)" ///
	"(^| )GENERAL( |$)" ///
	"(^| )GENERALES( |$)" ///
	"(^| )GNBH( |$)" ///
	"(^| )GROEP( |$)" ///
	"(^| )GROUP( |$)" ///
	"(^| )HANDEL( |$)" ///
	"(^| )HOLDING( |$)" ///
	"(^| )HOLDINGS( |$)" ///
	"(^| )INC( |$)" ///
	"(^| )INC:( |$)" ///
	"(^| )INCORPORATED( |$)" ///
	"(^| )INDUSTRI( |$)" ///
	"(^| )INDUSTRIA( |$)" ///
	"(^| )INDUSTRY( |$)" ///
	"(^| )INT( |$)" ///
	"(^| )INTERNATIONAL( |$)" ///
	"(^| )INTL( |$)" ///
	"(^| )INVESTMENT( |$)" ///
	"(^| )IS( |$)" ///
	"(^| )K K( |$)" ///
	"(^| )KK( |$)" ///
	"(^| )KY( |$)" ///
	"(^| )L C( |$)" ///
	"(^| )L L C( |$)" ///
	"(^| )L P( |$)" ///
	"(^| )LAB( |$)" ///
	"(^| )LC( |$)" ///
	"(^| )LIMITE( |$)" ///
	"(^| )LIMITED PARTNERSHIP( |$)" ///
	"(^| )LIMITED( |$)" ///
	"(^| )LLC( |$)" ///
	"(^| )LLLC( |$)" ///
	"(^| )LLLP( |$)" ///
	"(^| )LLP( |$)" ///
	"(^| )LP( |$)" ///
	"(^| )LT EE( |$)" ///
	"(^| )LTA( |$)" ///
	"(^| )LTC( |$)" ///
	"(^| )LTD CO( |$)" ///
	"(^| )LTD LTEE( |$)" ///
	"(^| )LTD( |$)" ///
	"(^| )LTD:( |$)" ///
	"(^| )LTDA( |$)" ///
	"(^| )LTDS( |$)" ///
	"(^| )LTEE$;( |$)" ///
	"(^| )LTEE( |$)" ///
	"(^| )LTS( |$)" ///
	"(^| )MANUFACTURING( |$)" ///
	"(^| )MARKETING( |$)" ///
	"(^| )MEDICAL( |$)" ///
	"(^| )MFG( |$)" ///
	"(^| )N A( |$)" ///
	"(^| )N V( |$)" ///
	"(^| )NA( |$)" ///
	"(^| )NATIONAL( |$)" ///
	"(^| )NATL( |$)" ///
	"(^| )NV SA( |$)" ///
	"(^| )NV( |$)" ///
	"(^| )NV:( |$)" ///
	"(^| )NVSA( |$)" ///
	"(^| )OY( |$)" ///
	"(^| )OYJ( |$)" ///
	"(^| )PARTNER( |$)" ///
	"(^| )PARTNERS( |$)" ///
	"(^| )PARTNERSHIP( |$)" ///
	"(^| )PHARM( |$)" ///
	"(^| )PHARMACEUTICAL( |$)" ///
	"(^| )PHARMACEUTICALS( |$)" ///
	"(^| )PLC( |$)" ///
	"(^| )PRODUCT( |$)" ///
	"(^| )PUBLISHING( |$)" ///
	"(^| )RESEARCH( |$)" ///
	"(^| )RT( |$)" ///
	"(^| )S A( |$)" ///
	"(^| )S P A( |$)" ///
	"(^| )SALES( |$)" ///
	"(^| )SCA( |$)" ///
	"(^| )SCIENCE( |$)" ///
	"(^| )SERVICE( |$)" ///
	"(^| )SHARES( |$)" ///
	"(^| )SOCIETE( |$)" ///
	"(^| )SOCIETY( |$)" ///
	"(^| )SP( |$)" ///
	"(^| )SPA( |$)" ///
	"(^| )TECHNOLOGIES( |$)" ///
	"(^| )TECHNOLOGY( |$)" ///
	"(^| )TELECOMMUNICATIONS( |$)" ///
	"(^| )TRADING( |$)" ///
	"(^| )UNIVERSITY( |$)" ///
	"(^| )UNIV( |$)" ///
	"(^| )VENTURE( |$)" ///
	"(^| )WERK( |$)" ///
	"(^| )WORKS( |$)" {

	* Append tag
	replace extra_security_descriptors = extra_security_descriptors + ", " + trim(regexs(0)) ///
		if regexm(securityname_cln, "`regex_pattern'")
	
	* Strip tag from securityname
	replace securityname_cln = regexr(securityname_cln, "`regex_pattern'", "")

}

* More rare variables
if `process_rare_strings' {

	foreach regex_pattern in "(^| )ADVIESBUREAU( |$)" ///
		"(^| )AE( |$)" ///
		"(^| )AG & CO( |$)" ///
		"(^| )AG CO KG( |$)" ///
		"(^| )AG CO OHG( |$)" ///
		"(^| )AG CO( |$)" ///
		"(^| )AG COKG( |$)" ///
		"(^| )AG COOHG( |$)" ///
		"(^| )AG U CO KG( |$)" ///
		"(^| )AG U CO OHG( |$)" ///
		"(^| )AG U CO( |$)" ///
		"(^| )AG U COKG( |$)" ///
		"(^| )AG U COOHG( |$)" ///
		"(^| )AGSA( |$)" ///
		"(^| )AK TIEBOLAGET( |$)" ///
		"(^| )AKADEMI( |$)" ///
		"(^| )AKADEMIA( |$)" ///
		"(^| )AKADEMIE( |$)" ///
		"(^| )AKADEMIEI( |$)" ///
		"(^| )AKADEMII( |$)" ///
		"(^| )AKADEMIJA( |$)" ///
		"(^| )AKADEMIYA( |$)" ///
		"(^| )AKADEMIYAKH( |$)" ///
		"(^| )AKADEMIYAM( |$)" ///
		"(^| )AKADEMIYAMI( |$)" ///
		"(^| )AKADEMIYU( |$)" ///
		"(^| )AKCIOVA SPOLECNOST( |$)" ///
		"(^| )AKIEBOLAG( |$)" ///
		"(^| )AKIEBOLG( |$)" ///
		"(^| )AKIENGESELLSCHAFT( |$)" ///
		"(^| )AKITENGESELLSCHAFT( |$)" ///
		"(^| )AKITIEBOLAG( |$)" ///
		"(^| )AKLIENGISELLSCHAFT( |$)" ///
		"(^| )AKSJESELSKAP( |$)" ///
		"(^| )AKSJESELSKAPET( |$)" ///
		"(^| )AKSTIEBOLAGET( |$)" ///
		"(^| )AKTAINGESELLSCHAFT( |$)" ///
		"(^| )AKTEIBOLAG( |$)" ///
		"(^| )AKTEINGESELLSCHAFT( |$)" ///
		"(^| )AKTIBOLAG( |$)" ///
		"(^| )AKTIE BOLAGET( |$)" ///
		"(^| )AKTIEBDAG( |$)" ///
		"(^| )AKTIEBLOAG( |$)" ///
		"(^| )AKTIEBOALG( |$)" ///
		"(^| )AKTIEBOALGET( |$)" ///
		"(^| )AKTIEBOCAG( |$)" ///
		"(^| )AKTIEBOLAC( |$)" ///
		"(^| )AKTIEBOLAF( |$)" ///
		"(^| )AKTIEBOLAG( |$)" ///
		"(^| )AKTIEBOLAGET( |$)" ///
		"(^| )AKTIEBOLAQ( |$)" ///
		"(^| )AKTIEBOLOG( |$)" ///
		"(^| )AKTIEGBOLAG( |$)" ///
		"(^| )AKTIEGESELLSCHAFT( |$)" ///
		"(^| )AKTIEGOLAGET( |$)" ///
		"(^| )AKTIELBOLAG( |$)" ///
		"(^| )AKTIEN GESELLSCHAFT( |$)" ///
		"(^| )AKTIENBOLAG( |$)" ///
		"(^| )AKTIENBOLAGET( |$)" ///
		"(^| )AKTIENEGESELLSCHAFT( |$)" ///
		"(^| )AKTIENEGSELLSCHAFT( |$)" ///
		"(^| )AKTIENGEGESELLSCHAFT( |$)" ///
		"(^| )AKTIENGELLSCHAFT( |$)" ///
		"(^| )AKTIENGESCELLSCHAFT( |$)" ///
		"(^| )AKTIENGESELL SCHAFT( |$)" ///
		"(^| )AKTIENGESELLCHAFT( |$)" ///
		"(^| )AKTIENGESELLESCHAFT( |$)" ///
		"(^| )AKTIENGESELLESHAFT( |$)" ///
		"(^| )AKTIENGESELLS( |$)" ///
		"(^| )AKTIENGESELLSCAFT( |$)" ///
		"(^| )AKTIENGESELLSCGAFT( |$)" ///
		"(^| )AKTIENGESELLSCHAFT( |$)" ///
		"(^| )AKTIENGESELLSCHART( |$)" ///
		"(^| )AKTIENGESELLSCHATT( |$)" ///
		"(^| )AKTIENGESELLSCHGT( |$)" ///
		"(^| )AKTIENGESELLSCHRAFT( |$)" ///
		"(^| )AKTIENGESELLSHAFT( |$)" ///
		"(^| )AKTIENGESELLSHAT( |$)" ///
		"(^| )AKTIENGESELLSHCAFT( |$)" ///
		"(^| )AKTIENGESELSCHAFT( |$)" ///
		"(^| )AKTIENGESESCHAFT( |$)" ///
		"(^| )AKTIENGESILLSCHAFT( |$)" ///
		"(^| )AKTIENGESLLSCHAFT( |$)" ///
		"(^| )AKTIENGESSELLSCHAFT( |$)" ///
		"(^| )AKTIENGESSELSCHAFT( |$)" ///
		"(^| )AKTIENGSELLSCHAFT( |$)" ///
		"(^| )AKTIENGTESELLSCHAFT( |$)" ///
		"(^| )AKTIENRESELLSCHAFT( |$)" ///
		"(^| )AKTIESELSKAB( |$)" ///
		"(^| )AKTIESELSKABET( |$)" ///
		"(^| )AKTINGESELLSCHAFT( |$)" ///
		"(^| )AKTIONIERNO DRUSHESTWO( |$)" ///
		"(^| )AKTSIONERNAYA KOMPANIA( |$)" ///
		"(^| )AKTSIONERNO( |$)" ///
		"(^| )AKTSIONERNOE OBCHESTVO( |$)" ///
		"(^| )AKTSIONERNOE OBSCHEDTVO( |$)" ///
		"(^| )AKTSIONERNOE OBSCNESTVO( |$)" ///
		"(^| )AKTSIONERNOE OBSHESTVO( |$)" ///
		"(^| )AKTSIONERNOE OSBCHESTVO( |$)" ///
		"(^| )AKTSIONERNOEOBSCHESTVO( |$)" ///
		"(^| )ALLGEMEINE( |$)" ///
		"(^| )ALLGEMEINER( |$)" ///
		"(^| )ALLMENNAKSJESELSKAP( |$)" ///
		"(^| )ALLMENNAKSJESELSKAPET( |$)" ///
		"(^| )ALTIENGESELLSCHAFT( |$)" ///
		"(^| )AMBA( |$)" ///
		"(^| )AND SONS( |$)" ///
		"(^| )ANDELSLAG( |$)" ///
		"(^| )ANDELSLAGET( |$)" ///
		"(^| )ANDELSSELSKAB( |$)" ///
		"(^| )ANDELSSELSKABET( |$)" ///
		"(^| )ANLAGENGESELLSCHAFT( |$)" ///
		"(^| )ANONYME DITE( |$)" ///
		"(^| )ANONYMOS ETAIRIA( |$)" ///
		"(^| )ANPARTSSELSKAB( |$)" ///
		"(^| )ANPARTSSELSKABET( |$)" ///
		"(^| )ANSVARLIG SELSKAP( |$)" ///
		"(^| )ANSVARLIG SELSKAPET( |$)" ///
		"(^| )ANTREPRIZA( |$)" ///
		"(^| )APARARII( |$)" ///
		"(^| )APARATE( |$)" ///
		"(^| )APARATELOR( |$)" ///
		"(^| )APPARATE( |$)" ///
		"(^| )APPARATEBAU( |$)" ///
		"(^| )APPARATUS( |$)" ///
		"(^| )APPARECHHI( |$)" ///
		"(^| )APPAREIL( |$)" ///
		"(^| )APPAREILLAGE( |$)" ///
		"(^| )APPAREILLAGES( |$)" ///
		"(^| )APPAREILS( |$)" ///
		"(^| )APPERATEBAU( |$)" ///
		"(^| )APPLICATION( |$)" ///
		"(^| )APPLICATIONS( |$)" ///
		"(^| )APPLICAZIONE( |$)" ///
		"(^| )APPLICAZIONI( |$)" ///
		"(^| )ARL( |$)" ///
		"(^| )ATKIENGESELLSCHAFT( |$)" ///
		"(^| )AVV( |$)" ///
		"(^| )BANQUE( |$)" ///
		"(^| )BEDRIJF( |$)" ///
		"(^| )BEDRIJVEN( |$)" ///
		"(^| )BEPERK( |$)" ///
		"(^| )BEPERKTE AANSPRAKELIJKHEID( |$)" ///
		"(^| )BEPERKTE AANSPREEKLIJKHEID( |$)" ///
		"(^| )BESCHRAENKTER HAFTUNG( |$)" ///
		"(^| )BESCHRANKTER HAFTUNG( |$)" ///
		"(^| )BESCHRANKTER( |$)" ///
		"(^| )BESLOTEN VENNOOTSCHAP MET( |$)" ///
		"(^| )BESLOTEN VENNOOTSCHAP( |$)" ///
		"(^| )BESLOTENGENOOTSCHAP( |$)" ///
		"(^| )BESLOTENVENNOOTSCHAP( |$)" ///
		"(^| )BETEILIGUNGS GESELLSCHAFT MIT( |$)" ///
		"(^| )BETEILIGUNGSGESELLSCHAFT MBH( |$)" ///
		"(^| )BETEILIGUNGSGESELLSCHAFT( |$)" ///
		"(^| )BETRIEBE( |$)" ///
		"(^| )BMBH( |$)" ///
		"(^| )BRODERNA( |$)" ///
		"(^| )BRODRENE( |$)" ///
		"(^| )BROEDERNA( |$)" ///
		"(^| )BROEDRENE( |$)" ///
		"(^| )BV BEPERKTE AANSPRAKELIJKHEID( |$)" ///
		"(^| )BVBA( |$)" ///
		"(^| )BVBASPRL( |$)" ///
		"(^| )BVIO( |$)" ///
		"(^| )BVSA( |$)" ///
		"(^| )CAMPAGNIE( |$)" ///
		"(^| )CAMPANY( |$)" ///
		"(^| )CENTRAAL( |$)" ///
		"(^| )CENTRALA( |$)" ///
		"(^| )CENTRALES( |$)" ///
		"(^| )CENTRAUX( |$)" ///
		"(^| )CENTRUL( |$)" ///
		"(^| )CENTRUM( |$)" ///
		"(^| )CERCETARE( |$)" ///
		"(^| )CERCETARI( |$)" ///
		"(^| )CHEMICKE( |$)" ///
		"(^| )CHEMICKEJ( |$)" ///
		"(^| )CHEMICKY( |$)" ///
		"(^| )CHEMICKYCH( |$)" ///
		"(^| )CHEMICZNY( |$)" ///
		"(^| )CHEMII( |$)" ///
		"(^| )CHEMISCH( |$)" ///
		"(^| )CHEMISCHE( |$)" ///
		"(^| )CHEMISKEJ( |$)" ///
		"(^| )CHEMISTRY( |$)" ///
		"(^| )CHIMIC( |$)" ///
		"(^| )CHIMICA( |$)" ///
		"(^| )CHIMICE( |$)" ///
		"(^| )CHIMICI( |$)" ///
		"(^| )CHIMICO( |$)" ///
		"(^| )CHIMIE( |$)" ///
		"(^| )CHIMIEI( |$)" ///
		"(^| )CHIMIESKOJ( |$)" ///
		"(^| )CHIMII( |$)" ///
		"(^| )CHIMIKO( |$)" ///
		"(^| )CHIMIQUE( |$)" ///
		"(^| )CHIMIQUES( |$)" ///
		"(^| )CHIMIYA( |$)" ///
		"(^| )CHIMIYAKH( |$)" ///
		"(^| )CHIMIYAM( |$)" ///
		"(^| )CHIMIYAMI( |$)" ///
		"(^| )CHIMIYU( |$)" ///
		"(^| )CLOSE CORPORATION( |$)" ///
		"(^| )CMOPANY( |$)" ///
		"(^| )CO OPERATIVE( |$)" ///
		"(^| )CO OPERATIVES( |$)" ///
		"(^| )COFP( |$)" ///
		"(^| )COIRPORATION( |$)" ///
		"(^| )COMANY( |$)" ///
		"(^| )COMAPANY( |$)" ///
		"(^| )COMAPNY( |$)" ///
		"(^| )COMBINATUL( |$)" ///
		"(^| )COMMANDITAIRE VENNOOTSCHAP OP AANDELEN( |$)" ///
		"(^| )COMMANDITAIRE VENNOOTSCHAP OP ANDELEN( |$)" ///
		"(^| )COMMANDITAIRE VENNOOTSCHAP( |$)" ///
		"(^| )COMMANDITE SIMPLE( |$)" ///
		"(^| )COMMERCIALISATIONS( |$)" ///
		"(^| )COMNPANY( |$)" ///
		"(^| )COMPAGNE( |$)" ///
		"(^| )COMPAGNIE FRANCAISE( |$)" ///
		"(^| )COMPAGNIE GENERALE( |$)" ///
		"(^| )COMPAGNIE INDUSTRIALE( |$)" ///
		"(^| )COMPAGNIE INDUSTRIELLE( |$)" ///
		"(^| )COMPAGNIE INDUSTRIELLES( |$)" ///
		"(^| )COMPAGNIE INTERNATIONALE( |$)" ///
		"(^| )COMPAGNIE NATIONALE( |$)" ///
		"(^| )COMPAGNIE PARISIEN( |$)" ///
		"(^| )COMPAGNIE PARISIENN( |$)" ///
		"(^| )COMPAGNIE PARISIENNE( |$)" ///
		"(^| )COMPAGNIE( |$)" ///
		"(^| )COMPAGNIN( |$)" ///
		"(^| )COMPAGNY( |$)" ///
		"(^| )COMPAIGNIE( |$)" ///
		"(^| )COMPAMY( |$)" ///
		"(^| )COMPANAY( |$)" ///
		"(^| )COMPANHIA( |$)" ///
		"(^| )COMPANIES( |$)" ///
		"(^| )COMPNAY( |$)" ///
		"(^| )COMPNY( |$)" ///
		"(^| )COMPORATION( |$)" ///
		"(^| )CONSORTILE PER AZIONE( |$)" ///
		"(^| )CONSORZIO( |$)" ///
		"(^| )CONSTRUCTIE( |$)" ///
		"(^| )CONSTRUCTII( |$)" ///
		"(^| )CONSTRUCTIILOR( |$)" ///
		"(^| )CONSTRUCTOR( |$)" ///
		"(^| )CONSTRUCTORTUL( |$)" ///
		"(^| )CONSTRUCTORUL( |$)" ///
		"(^| )CONZORZIO( |$)" ///
		"(^| )COOEPERATIE( |$)" ///
		"(^| )COOEPERATIEVE VERENIGING( |$)" ///
		"(^| )COOEPERATIEVE VERKOOP( |$)" ///
		"(^| )COOEPERATIEVE( |$)" ///
		"(^| )COOP A RL( |$)" ///
		"(^| )COOPERATIE( |$)" ///
		"(^| )COOPERATIEVE VENOOTSCHAP( |$)" ///
		"(^| )COOPERATIEVE( |$)" ///
		"(^| )COOPERATION( |$)" ///
		"(^| )COOPERATIVA AGICOLA( |$)" ///
		"(^| )COOPERATIVA LIMITADA( |$)" ///
		"(^| )COOPERATIVA PER AZIONI( |$)" ///
		"(^| )COOPERATIVA( |$)" ///
		"(^| )COOPERATIVE( |$)" ///
		"(^| )COOPERATIVES( |$)" ///
		"(^| )COORPORATION( |$)" ///
		"(^| )COPANY( |$)" ///
		"(^| )COPR( |$)" ///
		"(^| )COPRORATION( |$)" ///
		"(^| )COPRPORATION( |$)" ///
		"(^| )COROPORTION( |$)" ///
		"(^| )COROPRATION( |$)" ///
		"(^| )COROPROATION( |$)" ///
		"(^| )CORORATION( |$)" ///
		"(^| )CORPARATION( |$)" ///
		"(^| )CORPERATION( |$)" ///
		"(^| )CORPFORATION( |$)" ///
		"(^| )CORPOARTION( |$)" ///
		"(^| )CORPOATAION( |$)" ///
		"(^| )CORPOATION( |$)" ///
		"(^| )CORPOIRATION( |$)" ///
		"(^| )CORPOORATION( |$)" ///
		"(^| )CORPOPRATION( |$)" ///
		"(^| )CORPORAATION( |$)" ///
		"(^| )CORPORACION( |$)" ///
		"(^| )CORPORAION( |$)" ///
		"(^| )CORPORAITON( |$)" ///
		"(^| )CORPORARION( |$)" ///
		"(^| )CORPORARTION( |$)" ///
		"(^| )CORPORASTION( |$)" ///
		"(^| )CORPORATAION( |$)" ///
		"(^| )CORPORATE( |$)" ///
		"(^| )CORPORATED( |$)" ///
		"(^| )CORPORATI( |$)" ///
		"(^| )CORPORATIION( |$)" ///
		"(^| )CORPORATIN( |$)" ///
		"(^| )CORPORATINO( |$)" ///
		"(^| )CORPORATINON( |$)" ///
		"(^| )CORPORATIO( |$)" ///
		"(^| )CORPORATIOIN( |$)" ///
		"(^| )CORPORATIOLN( |$)" ///
		"(^| )CORPORATIOM( |$)" ///
		"(^| )CORPORATION OF AMERICA( |$)" ///
		"(^| )CORPORATION( |$)" ///
		"(^| )CORPORATIOON( |$)" ///
		"(^| )CORPORATIOPN( |$)" ///
		"(^| )CORPORATITON( |$)" ///
		"(^| )CORPORATOIN( |$)" ///
		"(^| )CORPORDATION( |$)" ///
		"(^| )CORPORQTION( |$)" ///
		"(^| )CORPORTAION( |$)" ///
		"(^| )CORPORTATION( |$)" ///
		"(^| )CORPORTION( |$)" ///
		"(^| )CORPPORATION( |$)" ///
		"(^| )CORPRATION( |$)" ///
		"(^| )CORPROATION( |$)" ///
		"(^| )CORPRORATION( |$)" ///
		"(^| )COSTRUZIONI( |$)" ///
		"(^| )CROP( |$)" ///
		"(^| )CROPORATION( |$)" ///
		"(^| )CRPORATION( |$)" ///
		"(^| )C{OVERSCORE O}RP( |$)" ///
		"(^| )D ENTERPRISES( |$)" ///
		"(^| )D ENTREPRISE( |$)" ///
		"(^| )D O O( |$)" ///
		"(^| )DEMOKRATISCHE REPUBLIK( |$)" ///
		"(^| )DEMOKRATISCHEN REPUBLIK( |$)" ///
		"(^| )DEPARTEMENT( |$)" ///
		"(^| )DEUTSCH( |$)" ///
		"(^| )DEUTSCHEN( |$)" ///
		"(^| )DEUTSCHER( |$)" ///
		"(^| )DEUTSCHES( |$)" ///
		"(^| )DEVELOPMENT( |$)" ///
		"(^| )DEVELOPMENTS( |$)" ///
		"(^| )DEVELOPPEMENT( |$)" ///
		"(^| )DEVELOPPEMENTS( |$)" ///
		"(^| )DIVISION( |$)" ///
		"(^| )DIVISIONE( |$)" ///
		"(^| )DOING BUSINESS( |$)" ///
		"(^| )DOO( |$)" ///
		"(^| )DORPORATION( |$)" ///
		"(^| )DRUSHESTWO S ORGRANITSCHENA OTGOWORNOST( |$)" ///
		"(^| )EDMS( |$)" ///
		"(^| )EG( |$)" ///
		"(^| )EINGETRAGENE GENOSSENSCHAFT( |$)" ///
		"(^| )EINGETRAGENER VEREIN( |$)" ///
		"(^| )ELECTRONIQUE( |$)" ///
		"(^| )EN ZN( |$)" ///
		"(^| )EN ZONEN( |$)" ///
		"(^| )ENNOBLISSEMENT( |$)" ///
		"(^| )ENTRE PRISES( |$)" ///
		"(^| )ENTREPOSE( |$)" ///
		"(^| )ENTREPRISE UNIPERSONNELLE A RESPONSABILITE LIMITEE( |$)" ///
		"(^| )EQUIPAMENTOS( |$)" ///
		"(^| )EQUIPEMENT( |$)" ///
		"(^| )EQUIPEMENTS( |$)" ///
		"(^| )EQUIPMENT( |$)" ///
		"(^| )EQUIPMENTS( |$)" ///
		"(^| )ESTABILSSEMENTS( |$)" ///
		"(^| )ESTABLISHMENT( |$)" ///
		"(^| )ESTABLISHMENTS( |$)" ///
		"(^| )ESTABLISSEMENT( |$)" ///
		"(^| )ESTABLISSEMENTS( |$)" ///
		"(^| )ESTABLISSMENTS( |$)" ///
		"(^| )ET FILS( |$)" ///
		"(^| )ETABLISSEMENT( |$)" ///
		"(^| )ETABLISSEMENTS( |$)" ///
		"(^| )ETABLISSMENTS( |$)" ///
		"(^| )ETABS( |$)" ///
		"(^| )ETAIRIA PERIORISMENIS EVTHINIS( |$)" ///
		"(^| )ETERRORRYTHMOS( |$)" ///
		"(^| )ETUDE( |$)" ///
		"(^| )ETUDES( |$)" ///
		"(^| )EUROPAEISCHE( |$)" ///
		"(^| )EUROPAEISCHEN( |$)" ///
		"(^| )EUROPAEISCHES( |$)" ///
		"(^| )EUROPAISCHE( |$)" ///
		"(^| )EUROPAISCHEN( |$)" ///
		"(^| )EUROPAISCHES( |$)" ///
		"(^| )EUROPEEN( |$)" ///
		"(^| )EUROPEENNE( |$)" ///
		"(^| )EXPLOATERING( |$)" ///
		"(^| )EXPLOATERINGS( |$)" ///
		"(^| )EXPLOITATIE( |$)" ///
		"(^| )EXPLOITATION( |$)" ///
		"(^| )EXPLOITATIONS( |$)" ///
		"(^| )F LLI( |$)" ///
		"(^| )FABBRICA( |$)" ///
		"(^| )FABBRICAZIONI( |$)" ///
		"(^| )FABBRICHE( |$)" ///
		"(^| )FABRICA( |$)" ///
		"(^| )FABRICATION( |$)" ///
		"(^| )FABRICATIONS( |$)" ///
		"(^| )FABRICS( |$)" ///
		"(^| )FABRIEK( |$)" ///
		"(^| )FABRIEKEN( |$)" ///
		"(^| )FABRIKER( |$)" ///
		"(^| )FABRIQUE( |$)" ///
		"(^| )FABRIQUES( |$)" ///
		"(^| )FABRIZIO( |$)" ///
		"(^| )FABRYKA( |$)" ///
		"(^| )FARMACEUTICA( |$)" ///
		"(^| )FARMACEUTICE( |$)" ///
		"(^| )FARMACEUTICHE( |$)" ///
		"(^| )FARMACEUTICI( |$)" ///
		"(^| )FARMACEUTICO( |$)" ///
		"(^| )FARMACEUTICOS( |$)" ///
		"(^| )FARMACEUTISK( |$)" ///
		"(^| )FARMACEVTSKIH( |$)" ///
		"(^| )FARMACIE( |$)" ///
		"(^| )FEDERATED( |$)" ///
		"(^| )FIRMA( |$)" ///
		"(^| )FLLI( |$)" ///
		"(^| )FONDATION( |$)" ///
		"(^| )FONDAZIONE( |$)" ///
		"(^| )FOUNDATIONS( |$)" ///
		"(^| )FRANCAIS( |$)" ///
		"(^| )FRANCAISE( |$)" ///
		"(^| )FRATELLI( |$)" ///
		"(^| )GAISHA( |$)" ///
		"(^| )GAISYA( |$)" ///
		"(^| )GAKKO HOJIN( |$)" ///
		"(^| )GAKKO HOUJIN( |$)" ///
		"(^| )GBMH( |$)" ///
		"(^| )GBR( |$)" ///
		"(^| )GEB( |$)" ///
		"(^| )GEBR( |$)" ///
		"(^| )GEBRODER( |$)" ///
		"(^| )GEBRODERS( |$)" ///
		"(^| )GEBROEDER( |$)" ///
		"(^| )GEBROEDERS( |$)" ///
		"(^| )GEBRUDER( |$)" ///
		"(^| )GEBRUDERS( |$)" ///
		"(^| )GEBRUEDER( |$)" ///
		"(^| )GEBRUEDERS( |$)" ///
		"(^| )GENERALA( |$)" ///
		"(^| )GENERALE POUR LES TECHNIQUES NOUVELLE( |$)" ///
		"(^| )GENERAUX( |$)" ///
		"(^| )GENOSSENSCHAFT( |$)" ///
		"(^| )GES M B H( |$)" ///
		"(^| )GES MB H( |$)" ///
		"(^| )GES MBH( |$)" ///
		"(^| )GES MHH( |$)" ///
		"(^| )GES( |$)" ///
		"(^| )GESELLSCHAFT BURGERLICHEN RECHTS( |$)" ///
		"(^| )GESELLSCHAFT M B H( |$)" ///
		"(^| )GESELLSCHAFT M B( |$)" ///
		"(^| )GESELLSCHAFT MB H( |$)" ///
		"(^| )GESELLSCHAFT MBH( |$)" ///
		"(^| )GESELLSCHAFT MGH( |$)" ///
		"(^| )GESELLSCHAFT MIT BESCHRANKTER HAFT( |$)" ///
		"(^| )GESELLSCHAFT MIT BESCHRANKTER HAFTUNG( |$)" ///
		"(^| )GESELLSCHAFT MIT BESCHRANKTER( |$)" ///
		"(^| )GESELLSCHAFT MIT( |$)" ///
		"(^| )GESELLSCHAFT( |$)" ///
		"(^| )GESELLSCHAFTMIT BESCHRANKTER( |$)" ///
		"(^| )GESMBH( |$)" ///
		"(^| )GESSELLSCHAFT MIT BESCHRAENKTER HAUFTUNG( |$)" ///
		"(^| )GEWERKSCHAFT( |$)" ///
		"(^| )GEWONE COMMANDITAIRE VENNOOTSCHAP( |$)" ///
		"(^| )GIE( |$)" ///
		"(^| )GMBA( |$)" ///
		"(^| )GMBB( |$)" ///
		"(^| )GMBG( |$)" ///
		"(^| )GMBH CO KG( |$)" ///
		"(^| )GMBH CO OHG( |$)" ///
		"(^| )GMBH CO( |$)" ///
		"(^| )GMBH COKG( |$)" ///
		"(^| )GMBH COOHG( |$)" ///
		"(^| )GMBH U CO KG( |$)" ///
		"(^| )GMBH U CO OHG( |$)" ///
		"(^| )GMBH U CO( |$)" ///
		"(^| )GMBH U COKG( |$)" ///
		"(^| )GMBH U COOHG( |$)" ///
		"(^| )GMBH( |$)" ///
		"(^| )GOMEI GAISHA( |$)" ///
		"(^| )GOMEI KAISHA( |$)" ///
		"(^| )GORPORATION( |$)" ///
		"(^| )GOSHI KAISHA( |$)" ///
		"(^| )GOUSHI GAISHA( |$)" ///
		"(^| )GREAT BRITAIN( |$)" ///
		"(^| )GROUPEMENT D ENTREPRISES( |$)" ///
		"(^| )GROUPEMENT D INTERET ECONOMIQUE( |$)" ///
		"(^| )GROUPEMENT( |$)" ///
		"(^| )GROUPMENT( |$)" ///
		"(^| )GUTEHOFFNUNGSCHUETTE( |$)" ///
		"(^| )GUTEHOFFNUNGSCHUTTE( |$)" ///
		"(^| )HAFRUNG( |$)" ///
		"(^| )HANDELABOLAGET( |$)" ///
		"(^| )HANDELEND ONDER( |$)" ///
		"(^| )HANDELORGANISATION( |$)" ///
		"(^| )HANDELS BOLAGET( |$)" ///
		"(^| )HANDELS( |$)" ///
		"(^| )HANDELSBOLAG( |$)" ///
		"(^| )HANDELSBOLAGET( |$)" ///
		"(^| )HANDELSGESELLSCHAFT( |$)" ///
		"(^| )HANDELSMAATSCHAPPIJ( |$)" ///
		"(^| )HANDELSMIJ( |$)" ///
		"(^| )HANDESBOLAG( |$)" ///
		"(^| )HATFUNG( |$)" ///
		"(^| )HER MAJESTY THE QUEEN IN RIGHT OF CANADA AS REPRESENTED BY THE MINISTER OF( |$)" ///
		"(^| )HER MAJESTY THE QUEEN( |$)" ///
		"(^| )INCORPATED( |$)" ///
		"(^| )INCORPORATE( |$)" ///
		"(^| )INCORPORATION( |$)" ///
		"(^| )INCORPORORATED( |$)" ///
		"(^| )INCORPORTED( |$)" ///
		"(^| )INCORPOTATED( |$)" ///
		"(^| )INCORPRATED( |$)" ///
		"(^| )INCORPRORATED( |$)" ///
		"(^| )INCROPORATED( |$)" ///
		"(^| )INDISTRIES( |$)" ///
		"(^| )INCOPORATED( |$)" ///
		"(^| )INCORORATED( |$)" ///
		"(^| )INCORPARATED( |$)" ///
		"(^| )INDUSRTIES( |$)" ///
		"(^| )INDUSTRIAL COP( |$)" ///
		"(^| )INDUSTRIAL( |$)" ///
		"(^| )INDUSTRIALA( |$)" ///
		"(^| )INDUSTRIALE( |$)" ///
		"(^| )INDUSTRIALI( |$)" ///
		"(^| )INDUSTRIALIZARE( |$)" ///
		"(^| )INDUSTRIALIZAREA( |$)" ///
		"(^| )INDUSTRIALNA( |$)" ///
		"(^| )INDUSTRIALS( |$)" ///
		"(^| )INDUSTRIAS( |$)" ///
		"(^| )INDUSTRIE( |$)" ///
		"(^| )INDUSTRIEELE( |$)" ///
		"(^| )INDUSTRIEI( |$)" ///
		"(^| )INDUSTRIEL( |$)" ///
		"(^| )INDUSTRIELL( |$)" ///
		"(^| )INDUSTRIELLE( |$)" ///
		"(^| )INDUSTRIELLES( |$)" ///
		"(^| )INDUSTRIELS( |$)" ///
		"(^| )INDUSTRIER( |$)" ///
		"(^| )INDUSTRIES( |$)" ///
		"(^| )INDUSTRII( |$)" ///
		"(^| )INDUSTRIJ( |$)" ///
		"(^| )INDUSTRIJA( |$)" ///
		"(^| )INDUSTRIJSKO( |$)" ///
		"(^| )INDUSTRIYA( |$)" ///
		"(^| )INDUSTRIYAKH( |$)" ///
		"(^| )INDUSTRIYAM( |$)" ///
		"(^| )INDUSTRIYAMI( |$)" ///
		"(^| )INDUSTRIYU( |$)" ///
		"(^| )INGENIER( |$)" ///
		"(^| )INGENIERIA( |$)" ///
		"(^| )INGENIEUR( |$)" ///
		"(^| )INGENIEURBUERO( |$)" ///
		"(^| )INGENIEURBUREAU( |$)" ///
		"(^| )INGENIEURBURO( |$)" ///
		"(^| )INGENIEURGESELLSCHAFT( |$)" ///
		"(^| )INGENIEURS( |$)" ///
		"(^| )INGENIEURSBUERO( |$)" ///
		"(^| )INGENIEURSBUREAU( |$)" ///
		"(^| )INGENIEURTECHNISCHE( |$)" ///
		"(^| )INGENIEURTECHNISCHES( |$)" ///
		"(^| )INGENIOERFIRMAET( |$)" ///
		"(^| )INGENIOERSBYRA( |$)" ///
		"(^| )INGENIORSFIRMA( |$)" ///
		"(^| )INGENIORSFIRMAN( |$)" ///
		"(^| )INGENJOERSFIRMA( |$)" ///
		"(^| )INGENJOERSFIRMAN( |$)" ///
		"(^| )INGENJORSFIRMA( |$)" ///
		"(^| )INGINERIE( |$)" ///
		"(^| )INORPORATED( |$)" ///
		"(^| )INSINOORITOMISTO( |$)" ///
		"(^| )INSTITUT FRANCAIS( |$)" ///
		"(^| )INSTITUT NATIONAL( |$)" ///
		"(^| )INSTITUT( |$)" ///
		"(^| )INSTITUTA( |$)" ///
		"(^| )INSTITUTAM( |$)" ///
		"(^| )INSTITUTAMI( |$)" ///
		"(^| )INSTITUTAMKH( |$)" ///
		"(^| )INSTITUTE FRANCAISE( |$)" ///
		"(^| )INSTITUTE NATIONALE( |$)" ///
		"(^| )INSTITUTE( |$)" ///
		"(^| )INSTITUTES( |$)" ///
		"(^| )INSTITUTET( |$)" ///
		"(^| )INSTITUTO( |$)" ///
		"(^| )INSTITUTOM( |$)" ///
		"(^| )INSTITUTOV( |$)" ///
		"(^| )INSTITUTT( |$)" ///
		"(^| )INSTITUTU( |$)" ///
		"(^| )INSTITUTUL( |$)" ///
		"(^| )INSTITUTY( |$)" ///
		"(^| )INSTITUUT( |$)" ///
		"(^| )INSTITZHT( |$)" ///
		"(^| )INSTRUMENT( |$)" ///
		"(^| )INSTRUMENTATION( |$)" ///
		"(^| )INSTRUMENTE( |$)" ///
		"(^| )INSTRUMENTS( |$)" ///
		"(^| )INSTYTUT( |$)" ///
		"(^| )INT L( |$)" ///
		"(^| )INTERESSENTSKAB( |$)" ///
		"(^| )INTERESSENTSKABET( |$)" ///
		"(^| )INTERNACIONAL( |$)" ///
		"(^| )INTERNAITONAL( |$)" ///
		"(^| )INTERNATIONAL BUSINESS( |$)" ///
		"(^| )INTERNATIONALE( |$)" ///
		"(^| )INTERNATIONALEN( |$)" ///
		"(^| )INTERNATIONAUX( |$)" ///
		"(^| )INTERNATIONELLA( |$)" ///
		"(^| )INTERNATL( |$)" ///
		"(^| )INTERNAZIONALE( |$)" ///
		"(^| )INTERNTIONAL( |$)" ///
		"(^| )INTREPRINDEREA( |$)" ///
		"(^| )INUDSTRIE( |$)" ///
		"(^| )ISTITUTO( |$)" ///
		"(^| )ITALI( |$)" ///
		"(^| )ITALIA( |$)" ///
		"(^| )ITALIAN( |$)" ///
		"(^| )ITALIANA( |$)" ///
		"(^| )ITALIANE( |$)" ///
		"(^| )ITALIANI( |$)" ///
		"(^| )ITALIANO( |$)" ///
		"(^| )ITALIEN( |$)" ///
		"(^| )ITALIENNE( |$)" ///
		"(^| )ITALO( |$)" ///
		"(^| )JOINTVENTURE( |$)" ///
		"(^| )JULKINEN OSAKEYHTIO( |$)" ///
		"(^| )JUNIOR( |$)" ///
		"(^| )K G( |$)" ///
		"(^| )KABAUSHIKI GAISHA( |$)" ///
		"(^| )KABAUSHIKI KAISHA( |$)" ///
		"(^| )KABISHIKI GAISHA( |$)" ///
		"(^| )KABISHIKI KAISHA( |$)" ///
		"(^| )KABUSHI KIGAISHA( |$)" ///
		"(^| )KABUSHI KIKAISHA( |$)" ///
		"(^| )KABUSHIBI GAISHA( |$)" ///
		"(^| )KABUSHIBI KAISHA( |$)" ///
		"(^| )KABUSHIGAISHA( |$)" ///
		"(^| )KABUSHIKAISHA( |$)" ///
		"(^| )KABUSHIKGAISHA( |$)" ///
		"(^| )KABUSHIKI GAISHA( |$)" ///
		"(^| )KABUSHIKI GAISYA( |$)" ///
		"(^| )KABUSHIKI KAISHA( |$)" ///
		"(^| )KABUSHIKI KAISYA( |$)" ///
		"(^| )KABUSHIKI( |$)" ///
		"(^| )KABUSHIKIGAISHA( |$)" ///
		"(^| )KABUSHIKIGAISYA( |$)" ///
		"(^| )KABUSHIKIKAISHA( |$)" ///
		"(^| )KABUSHIKIKAISYA( |$)" ///
		"(^| )KABUSHIKKAISHA( |$)" ///
		"(^| )KABUSHIKU GASISHA( |$)" ///
		"(^| )KABUSHIKU KASISHA( |$)" ///
		"(^| )KABUSHKIKI GAISHI( |$)" ///
		"(^| )KABUSHKIKI KAISHI( |$)" ///
		"(^| )KABUSIKI GAISHA( |$)" ///
		"(^| )KABUSIKI GAISYA( |$)" ///
		"(^| )KABUSIKI KAISHA( |$)" ///
		"(^| )KABUSIKI KAISYA( |$)" ///
		"(^| )KABUSIKI( |$)" ///
		"(^| )KABUSIKIGAISHA( |$)" ///
		"(^| )KABUSIKIKAISHA( |$)" ///
		"(^| )KAGUSHIKI GAISHA( |$)" ///
		"(^| )KAGUSHIKI KAISHA( |$)" ///
		"(^| )KAISYA( |$)" ///
		"(^| )KAUSHIKI GAISHA( |$)" ///
		"(^| )KAUSHIKI KAISHA( |$)" ///
		"(^| )KB KY( |$)" ///
		"(^| )KB( |$)" ///
		"(^| )KFT( |$)" ///
		"(^| )KG( |$)" ///
		"(^| )KABSUHIKI( |$)" ///
		"(^| )KAISHA( |$)" ///
		"(^| )KGAA( |$)" ///
		"(^| )KOM GES( |$)" ///
		"(^| )KOMANDIT GESELLSCHAFT( |$)" ///
		"(^| )KOMANDITGESELLSCHAFT( |$)" ///
		"(^| )KOMANDITNI SPOLECNOST( |$)" ///
		"(^| )KOMANDITNO DRUSHESTWO S AKZII( |$)" ///
		"(^| )KOMANDITNO DRUSHESTWO( |$)" ///
		"(^| )KOMBINAT( |$)" ///
		"(^| )KOMBINATU( |$)" ///
		"(^| )KOMBINATY( |$)" ///
		"(^| )KOMM GES( |$)" ///
		"(^| )KOMMANDIITTIYHTIO( |$)" ///
		"(^| )KOMMANDIT BOLAG( |$)" ///
		"(^| )KOMMANDIT BOLAGET( |$)" ///
		"(^| )KOMMANDIT GESELLSCHAFT AUF AKTIEN( |$)" ///
		"(^| )KOMMANDIT GESELLSCHAFT( |$)" ///
		"(^| )KOMMANDITAKTIESELSKAB( |$)" ///
		"(^| )KOMMANDITAKTIESELSKABET( |$)" ///
		"(^| )KOMMANDITBOLAG( |$)" ///
		"(^| )KOMMANDITBOLAGET( |$)" ///
		"(^| )KOMMANDITGESELLSCHAFT AUF AKTIEN( |$)" ///
		"(^| )KOMMANDITGESELLSCHAFT( |$)" ///
		"(^| )KOMMANDITSELSKAB( |$)" ///
		"(^| )KOMMANDITSELSKABET( |$)" ///
		"(^| )KOMMANDITTSELSKAP( |$)" ///
		"(^| )KOMMANDITTSELSKAPET( |$)" ///
		"(^| )KONCERNOVY PODNIK( |$)" ///
		"(^| )KONINKLIJKE( |$)" ///
		"(^| )KONSTRUKTIONEN( |$)" ///
		"(^| )KOOPERATIVE( |$)" ///
		"(^| )KS( |$)" ///
		"(^| )KUBUSHIKI KAISHA( |$)" ///
		"(^| )KUNSTSTOFF( |$)" ///
		"(^| )KUNSTSTOFFTECHNIK( |$)" ///
		"(^| )KUTATO INTEZET( |$)" ///
		"(^| )KUTATO INTEZETE( |$)" ///
		"(^| )KUTATOINTEZET( |$)" ///
		"(^| )KUTATOINTEZETE( |$)" ///
		"(^| )LABARATOIRE( |$)" ///
		"(^| )LABO( |$)" ///
		"(^| )LABORATOIR( |$)" ///
		"(^| )LABORATOIRE( |$)" ///
		"(^| )LABORATOIRES( |$)" ///
		"(^| )LABORATORI( |$)" ///
		"(^| )LABORATORIA( |$)" ///
		"(^| )LABORATORIE( |$)" ///
		"(^| )LABORATORIEI( |$)" ///
		"(^| )LABORATORIES( |$)" ///
		"(^| )LABORATORIET( |$)" ///
		"(^| )LABORATORII( |$)" ///
		"(^| )LABORATORIJ( |$)" ///
		"(^| )LABORATORIO( |$)" ///
		"(^| )LABORATORIOS( |$)" ///
		"(^| )LABORATORIUM( |$)" ///
		"(^| )LABORATORY( |$)" ///
		"(^| )LABORTORI( |$)" ///
		"(^| )LABRATIORIES( |$)" ///
		"(^| )LABS( |$)" ///
		"(^| )LAVORAZA( |$)" ///
		"(^| )LAVORAZI( |$)" ///
		"(^| )LAVORAZIO( |$)" ///
		"(^| )LAVORAZIONE( |$)" ///
		"(^| )LAVORAZIONI( |$)" ///
		"(^| )LCC( |$)" ///
		"(^| )LDA( |$)" ///
		"(^| )LDT( |$)" ///
		"(^| )LIMIDADA( |$)" ///
		"(^| )LIMINTED( |$)" ///
		"(^| )LIMITADA( |$)" ///
		"(^| )LIMITADO( |$)" ///
		"(^| )LIMITATA( |$)" ///
		"(^| )LIIMITED( |$)" ///
		"(^| )LIMITEE( |$)" ///
		"(^| )LIMTED( |$)" ///
		"(^| )LINITED( |$)" ///
		"(^| )LITD( |$)" ///
		"(^| )LMITED( |$)" ///
		"(^| )MAATSCHAPPIJ( |$)" ///
		"(^| )MAGYAR TUDOMANYOS AKADEMIA( |$)" ///
		"(^| )MANIFATTURA( |$)" ///
		"(^| )MANIFATTURAS( |$)" ///
		"(^| )MANIFATTURE( |$)" ///
		"(^| )MANUFACTURAS( |$)" ///
		"(^| )MANUFACTURE D ARTICLES( |$)" ///
		"(^| )MANUFACTURE DE( |$)" ///
		"(^| )MANUFACTURE( |$)" ///
		"(^| )MANUFACTURER( |$)" ///
		"(^| )MANUFACTURERS( |$)" ///
		"(^| )MANUFACTURES( |$)" ///
		"(^| )MANUFACTURINGS( |$)" ///
		"(^| )MANUFATURA( |$)" ///
		"(^| )MASCHIN( |$)" ///
		"(^| )MASCHINEN( |$)" ///
		"(^| )MASCHINENBAU( |$)" ///
		"(^| )MASCHINENBAUANSTALT( |$)" ///
		"(^| )MASCHINENFAB( |$)" ///
		"(^| )MASCHINENFABRIEK( |$)" ///
		"(^| )MASCHINENFABRIK( |$)" ///
		"(^| )MASCHINENFABRIKEN( |$)" ///
		"(^| )MASCHINENVERTRIEB( |$)" ///
		"(^| )MBH & CO( |$)" ///
		"(^| )MBH( |$)" ///
		"(^| )MERCHANDISING( |$)" ///
		"(^| )MET BEPERKTE( |$)" ///
		"(^| )MINISTER( |$)" ///
		"(^| )MINISTERE( |$)" ///
		"(^| )MINISTERIUM( |$)" ///
		"(^| )MINISTERO( |$)" ///
		"(^| )MINISTERSTV( |$)" ///
		"(^| )MINISTERSTVA( |$)" ///
		"(^| )MINISTERSTVAKH( |$)" ///
		"(^| )MINISTERSTVAM( |$)" ///
		"(^| )MINISTERSTVAMI( |$)" ///
		"(^| )MINISTERSTVE( |$)" ///
		"(^| )MINISTERSTVO( |$)" ///
		"(^| )MINISTERSTVOM( |$)" ///
		"(^| )MINISTERSTVU( |$)" ///
		"(^| )MINISTERSTWO( |$)" ///
		"(^| )MINISTERUL( |$)" ///
		"(^| )MINISTRE( |$)" ///
		"(^| )MINISTRY( |$)" ///
		"(^| )MIT BESCHRANKTER HAFTUNG( |$)" ///
		"(^| )NAAMLOOSE VENOOTSCHAP( |$)" ///
		"(^| )NAAMLOSE( |$)" ///
		"(^| )NAAMLOZE VENNOOTSCAP( |$)" ///
		"(^| )NAAMLOZE VENNOOTSCHAP( |$)" ///
		"(^| )NAAMLOZE VENNOOTSHCAP( |$)" ///
		"(^| )NAAMLOZE( |$)" ///
		"(^| )NAAMLOZEVENNOOTSCHAP( |$)" ///
		"(^| )NARODNI PODNIK( |$)" ///
		"(^| )NARODNIJ PODNIK( |$)" ///
		"(^| )NARODNY PODNIK( |$)" ///
		"(^| )NATIONAAL( |$)" ///
		"(^| )NATIONALE( |$)" ///
		"(^| )NATIONAUX( |$)" ///
		"(^| )NAUCHNO PRIOZVODSTVENNAYA FIRMA( |$)" ///
		"(^| )NAUCHNO PRIOZVODSTVENNOE OBIEDINENIE( |$)" ///
		"(^| )NAUCHNO PRIOZVODSTVENNY KOOPERATIV( |$)" ///
		"(^| )NAUCHNO PROIZVODSTVENNOE OBJEDINENIE( |$)" ///
		"(^| )NAUCHNO PROIZVODSTVENNOE( |$)" ///
		"(^| )NAUCHNO TEKHNICHESKY KOOPERATIV( |$)" ///
		"(^| )NAUCHNO TEKHNICHESKYKKOOPERATIV( |$)" ///
		"(^| )NAUCHNO TEKHNOLOGICHESKOE( |$)" ///
		"(^| )NAUCHNO TEKHNOLOGICHESKOEPREDPRIYATIE( |$)" ///
		"(^| )NAUCHNOPRIOZVODSTVENNOE( |$)" ///
		"(^| )NAUCHNOPROIZVODSTVENNOE( |$)" ///
		"(^| )NAUCHNOTEKHNICHESKYKKOOPERATIV( |$)" ///
		"(^| )NAUCHNOTEKNICHESKY( |$)" ///
		"(^| )NAZIONALE( |$)" ///
		"(^| )NAZIONALI( |$)" ///
		"(^| )NORDDEUTSCH( |$)" ///
		"(^| )NORDDEUTSCHE( |$)" ///
		"(^| )NORDDEUTSCHER( |$)" ///
		"(^| )NORDDEUTSCHES( |$)" ///
		"(^| )OBIDINENIE( |$)" ///
		"(^| )OBIED( |$)" ///
		"(^| )OBOROVY PODNIK( |$)" ///
		"(^| )OBSCHESRYO( |$)" ///
		"(^| )OBSCHESTVO & OGRANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )OBSCHESTVO & ORGANICHENNOI OTVETSTVENNOSTIJU( |$)" ///
		"(^| )OBSCHESTVO C( |$)" ///
		"(^| )OBSCHESTVO S OGRANICHENNOI OTVETSTVEN NOSTJU( |$)" ///
		"(^| )OBSCHESTVO S OGRANICHENNOI OTVETSTVENNOSTIJU( |$)" ///
		"(^| )OBSCHESTVO S OGRANICHENNOI OTVETSTVENNPSTJU( |$)" ///
		"(^| )OBSCHESTVO S OGRANICHENNOI( |$)" ///
		"(^| )OBSCHESTVO S OGRANICHENNOY OTVETSTVENNOSTJU( |$)" ///
		"(^| )OBSCHESTVO S OGRANICHENOI( |$)" ///
		"(^| )OBSCHESTVO S ORGANICHENNOI OTVETSTVENNOSTIJU( |$)" ///
		"(^| )OBSCHESTVO S ORGANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )OBSCHESTVO S( |$)" ///
		"(^| )OBSHESTVO S OGRANNICHENNOJ( |$)" ///
		"(^| )OBSHESTVO S ORGANICHENNOI OTVETSTVENNOSTIJU( |$)" ///
		"(^| )OBSHESTVO S ORGANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )OBSHESTVO S( |$)" ///
		"(^| )OCTROOIBUREAU( |$)" ///
		"(^| )OESTERREICH( |$)" ///
		"(^| )OESTERREICHISCH( |$)" ///
		"(^| )OESTERREICHISCHE( |$)" ///
		"(^| )OESTERREICHISCHES( |$)" ///
		"(^| )OFFENE HANDELS GESELLSCHAFT( |$)" ///
		"(^| )OFFENE HANDELSGESELLSCHAFT( |$)" ///
		"(^| )OFFICINE MECCANICA( |$)" ///
		"(^| )OFFICINE MECCANICHE( |$)" ///
		"(^| )OFFICINE NATIONALE( |$)" ///
		"(^| )OGRANICHENNOI OTVETSTVENNOSTIJU FIRMA( |$)" ///
		"(^| )OGRANICHENNOI OTVETSTVENNOSTIJU( |$)" ///
		"(^| )OGRANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )OGRANICHENNOY OTVETSTVENNOSTYU( |$)" ///
		"(^| )OHG( |$)" ///
		"(^| )OMORRYTHMOS( |$)" ///
		"(^| )ONDERNEMING( |$)" ///
		"(^| )ONTWIKKELINGS( |$)" ///
		"(^| )ONTWIKKELINGSBUREAU( |$)" ///
		"(^| )ORDINARY SHARES( |$)" ///
		"(^| )ORGANISATIE( |$)" ///
		"(^| )ORGANISATION( |$)" ///
		"(^| )ORGANISATIONS( |$)" ///
		"(^| )ORGANIZATION( |$)" ///
		"(^| )ORGANIZATIONS( |$)" ///
		"(^| )ORGANIZZAZIONE( |$)" ///
		"(^| )OSAKEYHTIO( |$)" ///
		"(^| )OSTERREICH( |$)" ///
		"(^| )OSTERREICHISCH( |$)" ///
		"(^| )OSTERREICHISCHE( |$)" ///
		"(^| )OSTERREICHISCHES( |$)" ///
		"(^| )OTVETCTVENNOSTJU( |$)" ///
		"(^| )OTVETSTVENNOSTIJU( |$)" ///
		"(^| )OTVETSTVENNOSTJU( |$)" ///
		"(^| )OTVETSTVENNOSTOU( |$)" ///
		"(^| )OTVETSTVENNOSTYU( |$)" ///
		"(^| )OYABLTD( |$)" ///
		"(^| )OYG( |$)" ///
		"(^| )OYI( |$)" ///
		"(^| )OYL( |$)" ///
		"(^| )P C( |$)" ///
		"(^| )P L C( |$)" ///
		"(^| )PARNERSHIP( |$)" ///
		"(^| )PARNTERSHIP( |$)" ///
		"(^| )PATENT OFFICE( |$)" ///
		"(^| )PATENTVERWALTUNGS GESELLSCHAFT MBH( |$)" ///
		"(^| )PATENTVERWALTUNGSGESELLSCHAFT( |$)" ///
		"(^| )PATENTVERWERTUNGSGESELLSCHAFT( |$)" ///
		"(^| )PATNERSHIP( |$)" ///
		"(^| )PC( |$)" ///
		"(^| )PER AZIONA( |$)" ///
		"(^| )PERSONENVENNOOTSCHAP MET BE PERKTE AANSPRAKELIJKHEID( |$)" ///
		"(^| )PERSONENVENNOOTSCHAP MET( |$)" ///
		"(^| )PHARMACEUTICA( |$)" ///
		"(^| )PHARMACEUTIQUE( |$)" ///
		"(^| )PHARMACEUTIQUES( |$)" ///
		"(^| )PHARMACIA( |$)" ///
		"(^| )PHARMACIE( |$)" ///
		"(^| )PHARMACUETICALS( |$)" ///
		"(^| )PHARMAZEUTIKA( |$)" ///
		"(^| )PHARMAZEUTISCH( |$)" ///
		"(^| )PHARMAZEUTISCHE( |$)" ///
		"(^| )PHARMAZEUTISCHEN( |$)" ///
		"(^| )PHARMAZIE( |$)" ///
		"(^| )PLANTS( |$)" ///
		"(^| )PREDPRIVATIE( |$)" ///
		"(^| )PREDPRIYATIE( |$)" ///
		"(^| )PREFERENCE SHARES( |$)" ///
		"(^| )PREFERENEC SHARES( |$)" ///
		"(^| )PREFERRED SERIES( |$)" ///
		"(^| )PRELUCRARE( |$)" ///
		"(^| )PRELUCRAREA( |$)" ///
		"(^| )PREPRIVATIE( |$)" ///
		"(^| )PRODOTTI( |$)" ///
		"(^| )PRODUCE( |$)" ///
		"(^| )PRODUCTA( |$)" ///
		"(^| )PRODUCTAS( |$)" ///
		"(^| )PRODUCTEURS( |$)" ///
		"(^| )PRODUCTIE( |$)" ///
		"(^| )PRODUCTION( |$)" ///
		"(^| )PRODUCTIONS( |$)" ///
		"(^| )PRODUCTIQUE( |$)" ///
		"(^| )PRODUCTO( |$)" ///
		"(^| )PRODUCTORES( |$)" ///
		"(^| )PRODUCTOS( |$)" ///
		"(^| )PRODUCTS( |$)" ///
		"(^| )PRODUIT CHIMIQUE( |$)" ///
		"(^| )PRODUIT CHIMIQUES( |$)" ///
		"(^| )PRODUIT( |$)" ///
		"(^| )PRODUITS( |$)" ///
		"(^| )PRODUKCJI( |$)" ///
		"(^| )PRODUKT( |$)" ///
		"(^| )PRODUKTE( |$)" ///
		"(^| )PRODUKTER( |$)" ///
		"(^| )PRODUKTION( |$)" ///
		"(^| )PRODUKTIONS( |$)" ///
		"(^| )PRODUKTIONSGESELLSCHAFT( |$)" ///
		"(^| )PRODUKTUTVECKLING( |$)" ///
		"(^| )PRODURA( |$)" ///
		"(^| )PRODUSE( |$)" ///
		"(^| )PRODUTIS( |$)" ///
		"(^| )PRODUTOS( |$)" ///
		"(^| )PRODUZIONI( |$)" ///
		"(^| )PROIECTARE( |$)" ///
		"(^| )PROIECTARI( |$)" ///
		"(^| )PROIZVODSTENNOE OBIEDINENIE( |$)" ///
		"(^| )PROIZVODSTVENNOE OBIEDINENIE( |$)" ///
		"(^| )PROIZVODSTVENNOE( |$)" ///
		"(^| )PROPRIETARY( |$)" ///
		"(^| )PRZEDSIEBIOSTWO( |$)" ///
		"(^| )PRZEMYSLU( |$)" ///
		"(^| )PTY LIM( |$)" ///
		"(^| )PTY( |$)" ///
		"(^| )PTYLTD( |$)" ///
		"(^| )PUBLIC LIABILITY COMPANY( |$)" ///
		"(^| )PUBLIC LIMITED COMPANY( |$)" ///
		"(^| )PUBLIC LIMITED( |$)" ///
		"(^| )PUBLIKT AKTIEBOLAG( |$)" ///
		"(^| )PVBA( |$)" ///
		"(^| )REALISATION( |$)" ///
		"(^| )REALISATIONS( |$)" ///
		"(^| )RECH & DEV( |$)" ///
		"(^| )RECHERCHE ET DEVELOPMENT( |$)" ///
		"(^| )RECHERCHE ET DEVELOPPEMENT( |$)" ///
		"(^| )RECHERCHE( |$)" ///
		"(^| )RECHERCHES ET DEVELOPMENTS( |$)" ///
		"(^| )RECHERCHES ET DEVELOPPEMENTS( |$)" ///
		"(^| )RECHERCHES( |$)" ///
		"(^| )REDEEMABLE( |$)" ///
		"(^| )RES & DEV( |$)" ///
		"(^| )RESEARCH & DEVELOPMENT( |$)" ///
		"(^| )RESEARCH AND DEVELOPMENT( |$)" ///
		"(^| )RESPONSABILITA LIMITATA( |$)" ///
		"(^| )RESPONSABILITAâ LIMITATA( |$)" ///
		"(^| )RESPONSABILITE LIMITE( |$)" ///
		"(^| )RIJKSUNIVERSITEIT( |$)" ///
		"(^| )RO( |$)" ///
		"(^| )S A R L( |$)" ///
		"(^| )S A RL( |$)" ///
		"(^| )S COOP LTDA( |$)" ///
		"(^| )S COOP( |$)" ///
		"(^| )S NC( |$)" ///
		"(^| )S OGRANICHENNOI OTVETSTVENNEST( |$)" ///
		"(^| )S PA( |$)" ///
		"(^| )S R L( |$)" ///
		"(^| )S RL( |$)" ///
		"(^| )SA:( |$)" ///
		"(^| )SAAG( |$)" ///
		"(^| )SAARL( |$)" ///
		"(^| )S S( |$)" ///
		"(^| )SA A RL( |$)" ///
		"(^| )SA DITE( |$)" ///
		"(^| )SA RL( |$)" ///
		"(^| )SA( |$)" ///
		"(^| )SANV( |$)" ///
		"(^| )SAPA( |$)" ///
		"(^| )SARL UNIPERSONNELLE( |$)" ///
		"(^| )SARL( |$)" ///
		"(^| )SARL:( |$)" ///
		"(^| )SAS UNIPERSONNELLE( |$)" ///
		"(^| )SAS( |$)" ///
		"(^| )SC( |$)" ///
		"(^| )SCARL( |$)" ///
		"(^| )SCHWEIZER( |$)" ///
		"(^| )SCHWEIZERISCH( |$)" ///
		"(^| )SCHWEIZERISCHE( |$)" ///
		"(^| )SCHWEIZERISCHER( |$)" ///
		"(^| )SCHWEIZERISCHES( |$)" ///
		"(^| )SCIENCES( |$)" ///
		"(^| )SCIENTIFIC( |$)" ///
		"(^| )SCIENTIFICA( |$)" ///
		"(^| )SCIENTIFIQUE( |$)" ///
		"(^| )SCIENTIFIQUES( |$)" ///
		"(^| )SCIETE ANONYME( |$)" ///
		"(^| )SCOOP( |$)" ///
		"(^| )SCPA( |$)" ///
		"(^| )SCRAS( |$)" ///
		"(^| )SCRL( |$)" ///
		"(^| )SDRUZENI PODNIK( |$)" ///
		"(^| )SDRUZENI PODNIKU( |$)" ///
		"(^| )SECREATRY( |$)" ///
		"(^| )SECRETARY OF STATE FOR( |$)" ///
		"(^| )SECRETARY( |$)" ///
		"(^| )SECRETATY( |$)" ///
		"(^| )SECRETRY( |$)" ///
		"(^| )SELSKAP MED DELT ANSAR( |$)" ///
		"(^| )SEMPLICE( |$)" ///
		"(^| )SERIVICES( |$)" ///
		"(^| )SERVICES( |$)" ///
		"(^| )SHADAN HOJIN( |$)" ///
		"(^| )SHOP( |$)" ///
		"(^| )SIDERURGIC( |$)" ///
		"(^| )SIDERURGICA( |$)" ///
		"(^| )SIDERURGICAS( |$)" ///
		"(^| )SIDERURGIE( |$)" ///
		"(^| )SIDERURGIQUE( |$)" ///
		"(^| )SIMPLIFIEE( |$)" ///
		"(^| )SL( |$)" ///
		"(^| )SNC( |$)" ///
		"(^| )SOC A RESPONSABILITÃ LIMITATA( |$)" ///
		"(^| )SOC ANONYME( |$)" ///
		"(^| )SOC ARL( |$)" ///
		"(^| )SOC COOOP ARL( |$)" ///
		"(^| )SOC COOP A RESP LIM( |$)" ///
		"(^| )SOC COOP A RL( |$)" ///
		"(^| )SOC COOP R L( |$)" ///
		"(^| )SOC COOP RL( |$)" ///
		"(^| )SOC DITE( |$)" ///
		"(^| )SOC EN COMMANDITA( |$)" ///
		"(^| )SOC IN ACCOMANDITA PER AZIONI( |$)" ///
		"(^| )SOC IND COMM( |$)" ///
		"(^| )SOC LIMITADA( |$)" ///
		"(^| )SOC PAR ACTIONS SIMPLIFIEES( |$)" ///
		"(^| )SOC RL( |$)" ///
		"(^| )SOC( |$)" ///
		"(^| )SOCCOOP ARL( |$)" ///
		"(^| )SOCCOOPARL( |$)" ///
		"(^| )SOCIEDAD ANONIMA( |$)" ///
		"(^| )SOCIEDAD ANONIMYA( |$)" ///
		"(^| )SOCIEDAD CIVIL( |$)" ///
		"(^| )SOCIEDAD DE RESPONSABILIDAD LIMITADA( |$)" ///
		"(^| )SOCIEDAD ESPANOLA( |$)" ///
		"(^| )SOCIEDAD INDUSTRIAL( |$)" ///
		"(^| )SOCIEDAD LIMITADA( |$)" ///
		"(^| )SOCIEDAD( |$)" ///
		"(^| )SOCIEDADE LIMITADA( |$)" ///
		"(^| )SOCIEDADE( |$)" ///
		"(^| )SOCIET CIVILE( |$)" ///
		"(^| )SOCIETA A RESPONSABILITA LIMITATA( |$)" ///
		"(^| )SOCIETA A( |$)" ///
		"(^| )SOCIETA ANONIMA( |$)" ///
		"(^| )SOCIETA APPLICAZIONE( |$)" ///
		"(^| )SOCIETA CONSORTILE A RESPONSABILITA( |$)" ///
		"(^| )SOCIETA CONSORTILE ARL( |$)" ///
		"(^| )SOCIETA CONSORTILE PER AZION( |$)" ///
		"(^| )SOCIETA CONSORTILE PER AZIONI( |$)" ///
		"(^| )SOCIETA CONSORTILE( |$)" ///
		"(^| )SOCIETA COOPERATIVA A( |$)" ///
		"(^| )SOCIETA COOPERATIVA( |$)" ///
		"(^| )SOCIETA IN ACCOMANDITA SEMPLICE( |$)" ///
		"(^| )SOCIETA IN ACCOMANDITA( |$)" ///
		"(^| )SOCIETA IN NOME COLLECTIVO( |$)" ///
		"(^| )SOCIETA IN NOME COLLETTIVO( |$)" ///
		"(^| )SOCIETA INDUSTRIA( |$)" ///
		"(^| )SOCIETA PER AXIONI( |$)" ///
		"(^| )SOCIETA PER AZINOI( |$)" ///
		"(^| )SOCIETA PER AZINONI( |$)" ///
		"(^| )SOCIETA PER AZIONI( |$)" ///
		"(^| )SOCIETA PER AZIONI:( |$)" ///
		"(^| )SOCIETA PER L INDUSTRIA( |$)" ///
		"(^| )SOCIETA PERAZIONI( |$)" ///
		"(^| )SOCIETA( |$)" ///
		"(^| )SOCIETAPERAZIONI( |$)" ///
		"(^| )SOCIETE A RESPONSABILITE DITE( |$)" ///
		"(^| )SOCIETE A RESPONSABILITE LIMITEE( |$)" ///
		"(^| )SOCIETE A RESPONSABILITE( |$)" ///
		"(^| )SOCIETE A RESPONSABILITEE( |$)" ///
		"(^| )SOCIETE A RESPONSIBILITE LIMITEE( |$)" ///
		"(^| )SOCIETE A( |$)" ///
		"(^| )SOCIETE ALSACIENNE( |$)" ///
		"(^| )SOCIETE ANANYME( |$)" ///
		"(^| )SOCIETE ANNOYME( |$)" ///
		"(^| )SOCIETE ANOMYME( |$)" ///
		"(^| )SOCIETE ANOMYNE( |$)" ///
		"(^| )SOCIETE ANONVME( |$)" ///
		"(^| )SOCIETE ANONYM( |$)" ///
		"(^| )SOCIETE ANONYME DITE( |$)" ///
		"(^| )SOCIETE ANONYME SIMPLIFIEE( |$)" ///
		"(^| )SOCIETE ANONYME( |$)" ///
		"(^| )SOCIETE ANOYME( |$)" ///
		"(^| )SOCIETE APPLICATION( |$)" ///
		"(^| )SOCIETE AUXILIAIRE( |$)" ///
		"(^| )SOCIETE CHIMIQUE( |$)" ///
		"(^| )SOCIETE CIVILE IMMOBILIERE( |$)" ///
		"(^| )SOCIETE CIVILE( |$)" ///
		"(^| )SOCIETE COMMERCIALE( |$)" ///
		"(^| )SOCIETE COMMERCIALES( |$)" ///
		"(^| )SOCIETE COOPERATIVE( |$)" ///
		"(^| )SOCIETE D APPLICATIONS GENERALES( |$)" ///
		"(^| )SOCIETE D APPLICATIONS MECANIQUES( |$)" ///
		"(^| )SOCIETE D EQUIPEMENT( |$)" ///
		"(^| )SOCIETE D ETUDE ET DE CONSTRUCTION( |$)" ///
		"(^| )SOCIETE D ETUDE ET DE RECHERCHE EN VENTILATION( |$)" ///
		"(^| )SOCIETE D ETUDES ET( |$)" ///
		"(^| )SOCIETE D ETUDES TECHNIQUES ET D ENTREPRISES( |$)" ///
		"(^| )SOCIETE DE CONSEILS DE RECHERCHES ET D APPLICATIONS( |$)" ///
		"(^| )SOCIETE DE CONSTRUCTIO( |$)" ///
		"(^| )SOCIETE DE FABRICAITON( |$)" ///
		"(^| )SOCIETE DE FABRICATION( |$)" ///
		"(^| )SOCIETE DE PRODUCTION ET DE( |$)" ///
		"(^| )SOCIETE DE( |$)" ///
		"(^| )SOCIETE DES TRANSPORTS( |$)" ///
		"(^| )SOCIETE DITE :( |$)" ///
		"(^| )SOCIETE DITE( |$)" ///
		"(^| )SOCIETE DITE:( |$)" ///
		"(^| )SOCIETE EN COMMANDITE ENREGISTREE( |$)" ///
		"(^| )SOCIETE EN COMMANDITE PAR ACTIONS( |$)" ///
		"(^| )SOCIETE EN COMMANDITE SIMPLE( |$)" ///
		"(^| )SOCIETE EN COMMANDITE( |$)" ///
		"(^| )SOCIETE EN NOM COLLECTIF( |$)" ///
		"(^| )SOCIETE EN PARTICIPATION( |$)" ///
		"(^| )SOCIETE EN( |$)" ///
		"(^| )SOCIETE ETUDE( |$)" ///
		"(^| )SOCIETE ETUDES ET DEVELOPPEMENTS( |$)" ///
		"(^| )SOCIETE ETUDES ET( |$)" ///
		"(^| )SOCIETE ETUDES( |$)" ///
		"(^| )SOCIETE EXPLOITATION( |$)" ///
		"(^| )SOCIETE GENERALE POUR LES TECHNIQUES NOVELLES( |$)" ///
		"(^| )SOCIETE GENERALE POUR LES( |$)" ///
		"(^| )SOCIETE GENERALE( |$)" ///
		"(^| )SOCIETE INDUSTRIELLE( |$)" ///
		"(^| )SOCIETE INDUSTRIELLES( |$)" ///
		"(^| )SOCIETE MECANIQUE( |$)" ///
		"(^| )SOCIETE MECANIQUES( |$)" ///
		"(^| )SOCIETE METALLURGIQUE( |$)" ///
		"(^| )SOCIETE NATIONALE( |$)" ///
		"(^| )SOCIETE NOUVELLE( |$)" ///
		"(^| )SOCIETE PAR ACTIONS SIMPLIFEE( |$)" ///
		"(^| )SOCIETE PAR ACTIONS SIMPLIFIEE( |$)" ///
		"(^| )SOCIETE PAR ACTIONS( |$)" ///
		"(^| )SOCIETE PARISIEN( |$)" ///
		"(^| )SOCIETE PARISIENN( |$)" ///
		"(^| )SOCIETE PARISIENNE( |$)" ///
		"(^| )SOCIETE PRIVEE A RESPONSABILITE LIMITEE( |$)" ///
		"(^| )SOCIETE TECHNIQUE D APPLICATION ET DE RECHERCHE( |$)" ///
		"(^| )SOCIETE TECHNIQUE DE PULVERISATION( |$)" ///
		"(^| )SOCIETE TECHNIQUE( |$)" ///
		"(^| )SOCIETE TECHNIQUES( |$)" ///
		"(^| )SOCIETEANONYME( |$)" ///
		"(^| )SOCIETEDITE( |$)" ///
		"(^| )SOCIETEINDUSTRIELLE( |$)" ///
		"(^| )SOCRL( |$)" ///
		"(^| )SOEHNE( |$)" ///
		"(^| )SOGRANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )SOHN( |$)" ///
		"(^| )SOHNE( |$)" ///
		"(^| )SONNER( |$)" ///
		"(^| )SP A( |$)" ///
		"(^| )SP Z OO( |$)" ///
		"(^| )SP ZOO( |$)" ///
		"(^| )SPITALUL( |$)" ///
		"(^| )SPOKAZOO( |$)" ///
		"(^| )SPOL S R O( |$)" ///
		"(^| )SPOL S RO( |$)" ///
		"(^| )SPOL SRO( |$)" ///
		"(^| )SPOL( |$)" ///
		"(^| )SPOLECNOST S RUCENIM OMEZENYM( |$)" ///
		"(^| )SPOLECNOST SRO( |$)" ///
		"(^| )SPOLKA AKCYJNA( |$)" ///
		"(^| )SPOLKA KOMANDYTOWA( |$)" ///
		"(^| )SPOLKA PRAWA CYWILNEGO( |$)" ///
		"(^| )SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA( |$)" ///
		"(^| )SPOLKA Z OO( |$)" ///
		"(^| )SPOLKA ZOO( |$)" ///
		"(^| )SPOLS RO( |$)" ///
		"(^| )SPOLSRO( |$)" ///
		"(^| )SPRL( |$)" ///
		"(^| )SPZ OO( |$)" ///
		"(^| )SPZOO( |$)" ///
		"(^| )SR L( |$)" ///
		"(^| )SR( |$)" ///
		"(^| )SR1( |$)" ///
		"(^| )SRI( |$)" ///
		"(^| )SRL( |$)" ///
		"(^| )SRO( |$)" ///
		"(^| )STE ANONYME( |$)" ///
		"(^| )STIINTIFICA( |$)" ///
		"(^| )SUDDEUTSCH( |$)" ///
		"(^| )SUDDEUTSCHE( |$)" ///
		"(^| )SUDDEUTSCHER( |$)" ///
		"(^| )SUDDEUTSCHES( |$)" ///
		"(^| )SURL( |$)" ///
		"(^| )TEAM( |$)" ///
		"(^| )TECHNICAL( |$)" ///
		"(^| )TECHNICO( |$)" ///
		"(^| )TECHNICZNY( |$)" ///
		"(^| )TECHNIK( |$)" ///
		"(^| )TECHNIKAI( |$)" ///
		"(^| )TECHNIKI( |$)" ///
		"(^| )TECHNIQUE( |$)" ///
		"(^| )TECHNIQUES NOUVELLE( |$)" ///
		"(^| )TECHNIQUES( |$)" ///
		"(^| )TECHNISCH( |$)" ///
		"(^| )TECHNISCHE( |$)" ///
		"(^| )TECHNISCHES( |$)" ///
		"(^| )TELECOMMUNICACION( |$)" ///
		"(^| )TELECOMMUNICATION( |$)" ///
		"(^| )TELECOMMUNICAZIONI( |$)" ///
		"(^| )TELECOMUNICAZIONI( |$)" ///
		"(^| )THE FIRM( |$)" ///
		"(^| )TOHO BUSINESS( |$)" ///
		"(^| )TOVARISCHESIVO S OGRANICHENNOI OIVETSIVENNOSTIJU( |$)" ///
		"(^| )TOVARISCHESTVO S OGRANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )TOVARISCHESTVO S OGRANICHENNOI OTVETSVENNOSTJU( |$)" ///
		"(^| )TOVARISCHESTVO S OGRANICHENNOI( |$)" ///
		"(^| )TOVARISCHESTVO S ORGANICHENNOI OTVETSTVENNOSTJU( |$)" ///
		"(^| )TOVARISCHESTVO( |$)" ///
		"(^| )TOVARISCHETSTVO S ORGANICHENNOI( |$)" ///
		"(^| )TRADING AS( |$)" ///
		"(^| )TRADING UNDER( |$)" ///
		"(^| )TRUSTUL( |$)" ///
		"(^| )UGINE( |$)" ///
		"(^| )UNITED KINGDOM( |$)" ///
		"(^| )UNITED STATES GOVERNMENT AS REPRESENTED BY THE SECRETARY OF( |$)" ///
		"(^| )UNITED STATES OF AMERICA ADMINISTRATOR( |$)" ///
		"(^| )UNITED STATES OF AMERICA AS REPRESENTED BY THE ADMINISTRATOR( |$)" ///
		"(^| )UNITED STATES OF AMERICA AS REPRESENTED BY THE DEPT( |$)" ///
		"(^| )UNITED STATES OF AMERICA AS REPRESENTED BY THE SECRETARY( |$)" ///
		"(^| )UNITED STATES OF AMERICA AS REPRESENTED BY THE UNITED STATES DEPT( |$)" ///
		"(^| )UNITED STATES OF AMERICA REPRESENTED BY THE SECRETARY( |$)" ///
		"(^| )UNITED STATES OF AMERICA SECRETARY OF( |$)" ///
		"(^| )UNITED STATES OF AMERICA( |$)" ///
		"(^| )UNITED STATES OF AMERICAN AS REPRESENTED BY THE UNITED STATES DEPT( |$)" ///
		"(^| )UNITED STATES OF AMERICAS AS REPRESENTED BY THE SECRETARY( |$)" ///
		"(^| )UNITED STATES( |$)" ///
		"(^| )UNITES STATES OF AMERICA AS REPRESENTED BY THE SECRETARY( |$)" ///
		"(^| )UNIVERSIDAD( |$)" ///
		"(^| )UNIVERSIDADE( |$)" ///
		"(^| )UNIVERSITA DEGLI STUDI( |$)" ///
		"(^| )UNIVERSITA( |$)" ///
		"(^| )UNIVERSITAET( |$)" ///
		"(^| )UNIVERSITAIR( |$)" ///
		"(^| )UNIVERSITAIRE( |$)" ///
		"(^| )UNIVERSITAT( |$)" ///
		"(^| )UNIVERSITATEA( |$)" ///
		"(^| )UNIVERSITE( |$)" ///
		"(^| )UNIVERSITEIT( |$)" ///
		"(^| )UNIVERSITET( |$)" ///
		"(^| )UNIVERSITETA( |$)" ///
		"(^| )UNIVERSITETAM( |$)" ///
		"(^| )UNIVERSITETAMI( |$)" ///
		"(^| )UNIVERSITETE( |$)" ///
		"(^| )UNIVERSITETOM( |$)" ///
		"(^| )UNIVERSITETOV( |$)" ///
		"(^| )UNIVERSITETU( |$)" ///
		"(^| )UNIVERSITETY( |$)" ///
		"(^| )UNIWERSYTET( |$)" ///
		"(^| )UNTERNEHMEN( |$)" ///
		"(^| )USINES( |$)" ///
		"(^| )UTILAJ( |$)" ///
		"(^| )UTILAJE( |$)" ///
		"(^| )UTILISATION VOLKSEIGENER BETRIEBE( |$)" ///
		"(^| )UTILISATIONS VOLKSEIGENER BETRIEBE( |$)" ///
		"(^| )VAKMANSCHAP( |$)" ///
		"(^| )VEB KOMBINAT( |$)" ///
		"(^| )VENNOOTSCHAP ONDER FIRMA( |$)" ///
		"(^| )VENNOOTSCHAP ONDER FIRMA:( |$)" ///
		"(^| )VENNOOTSCHAP( |$)" ///
		"(^| )VENNOOTSHAP( |$)" ///
		"(^| )VENNOTSCHAP( |$)" ///
		"(^| )VENOOTSCHAP( |$)" ///
		"(^| )VERARBEITUNG( |$)" ///
		"(^| )VEREENIGDE( |$)" ///
		"(^| )VEREIN( |$)" ///
		"(^| )VEREINIGTE VEREINIGUNG( |$)" ///
		"(^| )VEREINIGTES VEREINIGUNG( |$)" ///
		"(^| )VEREINIGUNG VOLKSEIGENER BETRIEBUNG( |$)" ///
		"(^| )VEREJNA OBCHODNI SPOLECNOST( |$)" ///
		"(^| )VERENIGING( |$)" ///
		"(^| )VERKOOP( |$)" ///
		"(^| )VERSICHERUNGSBUERO( |$)" ///
		"(^| )VERTRIEBSGESELLSCHAFT( |$)" ///
		"(^| )VERWALTUNGEN( |$)" ///
		"(^| )VERWALTUNGS( |$)" ///
		"(^| )VERWALTUNGSGESELLSCHAFT( |$)" ///
		"(^| )VERWERTUNGS( |$)" ///
		"(^| )VOF( |$)" ///
		"(^| )VYZK USTAV( |$)" ///
		"(^| )VYZK VYVOJOVY USTAV( |$)" ///
		"(^| )VYZKUMNY USTAV( |$)" ///
		"(^| )VYZKUMNY VYVOJOVY USTAV( |$)" ///
		"(^| )VYZKUMNYUSTAV( |$)" ///
		"(^| )WERKE( |$)" ///
		"(^| )WERKEN( |$)" ///
		"(^| )WERKHUIZEN( |$)" ///
		"(^| )WERKS( |$)" ///
		"(^| )WERKSTAETTE( |$)" ///
		"(^| )WERKSTATT( |$)" ///
		"(^| )WERKZEUGBAU( |$)" ///
		"(^| )WERKZEUGMASCHINENFABRIK( |$)" ///
		"(^| )WERKZEUGMASCHINENKOMBINAT( |$)" ///
		"(^| )WESTDEUTSCH( |$)" ///
		"(^| )WESTDEUTSCHE( |$)" ///
		"(^| )WESTDEUTSCHER( |$)" ///
		"(^| )WESTDEUTSCHES( |$)" ///
		"(^| )WINKEL( |$)" ///
		"(^| )WISSENSCHAFTLICHE(S)( |$)" ///
		"(^| )WISSENSCHAFTLICHES TECHNISCHES ZENTRUM( |$)" ///
		"(^| )YUGEN KAISHA( |$)" ///
		"(^| )YUGENKAISHA( |$)" ///
		"(^| )YUUGEN GAISHA( |$)" ///
		"(^| )YUUGEN KAISHA( |$)" ///
		"(^| )YUUGEN KAISYA( |$)" ///
		"(^| )YUUGENKAISHA( |$)" ///
		"(^| )ZAIDAN HOJIN( |$)" ///
		"(^| )ZAIDAN HOUJIN( |$)" ///
		"(^| )ZAVODU( |$)" ///
		"(^| )ZAVODY( |$)" ///
		"(^| )ZENTRALE( |$)" ///
		"(^| )ZENTRALEN( |$)" ///
		"(^| )ZENTRALES( |$)" ///
		"(^| )ZENTRALINSTITUT( |$)" ///
		"(^| )ZENTRALLABORATORIUM( |$)" ///
		"(^| )ZENTRALNA( |$)" ///
		"(^| )ZENTRUM( |$)" ///
		"(^| )ZOO( |$)" {

		* Append tag
		replace extra_security_descriptors = extra_security_descriptors + ", " + trim(regexs(0)) ///
			if regexm(securityname_cln, "`regex_pattern'")
		
		* Strip tag from securityname
		replace securityname_cln = regexr(securityname_cln, "`regex_pattern'", "")

		}
}

* Special care for GNMA, FNMA
replace extra_security_descriptors = extra_security_descriptors + ", " + trim(regexs(0)) if regexm(securityname_cln, "GNMA")
replace extra_security_descriptors = extra_security_descriptors + ", " + trim(regexs(0)) if regexm(securityname_cln, "FNMA")
replace extra_security_descriptors = extra_security_descriptors + ", " + "FNMA" if regexm(securityname_cln, "FANNIE MAE")
replace extra_security_descriptors = extra_security_descriptors + ", " + "GNMA" if regexm(securityname_cln, "GINNIE MAE")

* Parse a few more date formats from the name string
local monthlist = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|JANUARY|FEBRUARY|MARCH|APRIL|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER|SEPT"
gen date_in_name = regexs(0) if regexm(securityname_cln, "[0-9][0-9] ?(`monthlist') ?[0-9]+")

gen day_in_name = regexs(0) if regexm(date_in_name, "[0-9]+")
gen month_in_name = regexs(0) if regexm(date_in_name, "(`monthlist')")
gen year_in_name = regexs(2) if regexm(date_in_name, "[0-9][0-9] ?(`monthlist') ?([0-9]+)")

replace date_in_name = regexs(0) if regexm(securityname_cln, "(`monthlist') ?[0-9]+") & date_in_name==""
replace day_in_name = "1" if day_in_name=="" & date_in_name!=""
replace month_in_name = regexs(0) if regexm(date_in_name, "(`monthlist')") & month_in_name=="" & date_in_name!=""
replace year_in_name = regexs(2) if regexm(date_in_name, "(`monthlist') ?([0-9]+)") & year_in_name=="" & date_in_name!=""

replace month_in_name = "1" if inlist(month_in_name, "JAN", "JANUARY")
replace month_in_name = "2" if inlist(month_in_name, "FEB", "FEBRUARY")
replace month_in_name = "3" if inlist(month_in_name, "MAR", "MARCH")
replace month_in_name = "4" if inlist(month_in_name, "APR", "APRIL")
replace month_in_name = "5" if inlist(month_in_name, "MAY")
replace month_in_name = "6" if inlist(month_in_name, "JUN", "JUNE")
replace month_in_name = "7" if inlist(month_in_name, "JUL", "JULY")
replace month_in_name = "8" if inlist(month_in_name, "AUG", "AUGUST")
replace month_in_name = "9" if inlist(month_in_name, "SEP", "SEPT", "SEPTEMBER")
replace month_in_name = "10" if inlist(month_in_name, "OCT", "OCTOBER")
replace month_in_name = "11" if inlist(month_in_name, "NOV", "NOVEMBER")
replace month_in_name = "12" if inlist(month_in_name, "DEC", "DECEMBER")

replace year_in_name = "20" + year_in_name if strlen(year_in_name) == 2 & real(year_in_name) < 50
replace year_in_name = "19" + year_in_name if strlen(year_in_name) == 2 & real(year_in_name) > 50

gen maturitydate_in_name = mdy(real(month_in_name), real(day_in_name),real( year_in_name))
replace maturitydate = maturitydate_in_name if maturitydate==. & maturitydate_in_name!=.
drop date_in_name day_in_name month_in_name year_in_name maturitydate_in_name

* Strip out months; 1-blocks, and fully numerical blocs
foreach i of num 1/10 {
	replace securityname_cln = regexr(securityname_cln, "(( |^)(.)( |$))+", " ")
	replace securityname_cln = regexr(securityname_cln, "([0-9]| )(`monthlist')([0-9]| )", " ")
	replace securityname_cln = regexr(securityname_cln, "( |^)[0-9]+( |$)", " ")
}

* Remove initial punctuation
replace extra_security_descriptors = regexr(extra_security_descriptors, "^, ", "")

* Trim final strings
if `trim_strings_in_local_scope' {
	replace securityname_cln = itrim(securityname_cln)
	replace securityname_cln = trim(securityname_cln)
}
