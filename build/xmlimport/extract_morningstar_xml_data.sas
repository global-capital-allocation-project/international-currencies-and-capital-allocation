*-------------------------------------------------------------------------------------------------*;
* Extract_Morningstar_XLM_Data                                                                    *;
*                                                                                                 *;
* This SAS script handles extraction of the raw Morningstar XML files                             *;
*-------------------------------------------------------------------------------------------------*;
options nocenter mprint mlogic fullstimer nosyntaxcheck;
proc options;
run;

proc options option = work value define ;
run;
proc datasets library = work kill;
run;
quit;
**********************************************************************;
* SYSPARM has contents of SYSPARM option on SAS command line         *;
* Remember to put a comma between the three passed parameters        *;
* The infile path is the path to the directory that holds INFILENAME *;
**********************************************************************;
%let sparm = &sysparm.;
data work.temp;
  sparm = "&sparm.";
  xallsparm = symget('sysparm');
  put sparm;
run;
%macro path(x)/parmbuff;
      %global ipath opath ifile;
      %let ipath = %scan(&syspbuff,1," ,");
      %let opath = %scan(&syspbuff,2," ,");
      %let ifile = %scan(&syspbuff,3," ,");
%put  Input Path: &ipath.;
%put Output Path: &opath.;
%put  Input File: &ifile.;
%mend;
%path (&sparm.);
data _null_;
run;
*****************************************************************************************;
* Read XML file, save to INTEXT (not really needed, it's just to validate things)       *;
*                        HoldingDetailsX, and PortfolioSummaryX                         *;
* We read the file as an unformatted file (recfm = n), read in each field and determine *;
* what variable to write it to.  That's what all the IF statements are doing            *;
*****************************************************************************************;
%macro reder;
  %put InPath: &ipath.    OutPath: &opath.    Infile: &ifile.;

data /* work.intext (keep = intext: hdtext: xmltag: section) */
     work.PortfolioSummaryX (keep = Filename
                                    Portfolio_ORDINAL
                                    _MasterPortfolioId
                                    Date
                                    _CurrencyId
                                    PortfolioSummary_ORDINAL
                                    PreviousPortfolioDate
                                    NetExpenseRatio
                                    NumberOfHoldingShort
                                    NumberOfStockHoldingShort
                                    NumberOfBondHoldingShort
                                    TotalMarketValueShort
                                    NumberOfHoldingLong
                                    NumberOfStockHoldingLong
                                    NumberOfBondHoldingLong
                                    TotalMarketValueLong)
     work.HoldingDetailX (keep =    Filename
                                    Portfolio_ORDINAL
                                    _MasterPortfolioId
                                    Date
                                    _CurrencyId
                                    PortfolioSummary_ORDINAL
                                    PreviousPortfolioDate
                                    NetExpenseRatio
                                    Holding_Ordinal
                                    _DetailHoldingTypeId
                                    _StorageId
                                    _ExternalId
                                    _Id
                                    ExternalName
                                    Country_Id
                                    Country
                                    CUSIP
                                    SEDOL
                                    ISIN
                                    Ticker
                                    Currency
                                    Currency_Id
                                    SecurityName
                                    LocalName
                                    Weighting
                                    NumberOfShare
                                    SharePercentage
                                    NumberOfJointlyOwnedShare
                                    MarketValue
                                    CostBasis
                                    ShareChange
                                    Sector
                                    MaturityDate
                                    AccruedInterest
                                    Coupon
                                    CreditQuality
                                    Duration
                                    IndustryId
                                    GlobalIndustryId
                                    GlobalSector
                                    GICSIndustryId
                                    LocalCurrencyCode
                                    LocalMarketValue
                                    ZAFAssetType
                                    PaymentType
                                    Rule144AEligible
                                    AltMinTaxEligible
                                    BloombergTicker
                                    ISOExchangeID
                                    ContractSize
                                    SecondarySectorId
                                    CompanyId
                                    FirstBoughtDate
                                    MexicanTipoValor
                                    MexicanSerie
                                    MexicanEmisora
                                    UnderlyingSecId
                                    UnderlyingSecurityName
                                    PerformanceId
                                    LessThanOneYearBond
                                    ZAFBondIssuerClass
                                    IndianCreditQualityClassificatio
                                    IndianIndustryClassification
                                    SurveyedWeighting                   );
 length XMLTag                              $128
        intext                              $1024
        hdtext_01-hdtext_20                 $80
        filnam                              $128
        FileName                            $128
        Section                             $1
        Portfolio_Ordinal                   8
        PortfolioSummary_Ordinal            8
        Holding_Ordinal                     8
        _MasterPortfolioId                  8
        _CurrencyId                         $32
        Portfolio_ExternalId                $32
        Date                                8
        PreviousPortfolioDate               8
        NetExpenseRatio                     $32
        NumberOfHoldingShort                8
        NumberOfStockHoldingShort           8
        NumberOfBondHoldingShort            8
        TotalMarketValueShort               8
        NumberOfHoldingLong                 8
        NumberOfStockHoldingLong            8
        NumberOfBondHoldingLong             8
        TotalMarketValueLong                8
        _Id                                 $32
        _DetailHoldingTypeId                $32
        _StorageId                          $16
        _ExternalId                         $32
        ExternalName                        $128
        Country                             $32
        Country_Id
        CUSIP                               $32
        SEDOL                               $32
        ISIN                                $12
        Ticker                              $32
        Currency                            $32
        Currency_Id                         $32
        SecurityName                        $128
        LocalName                           $32
        Weighting                           $32
        NumberOfShare                       8
        SharePercentage                     $32
        NumberOfJointlyOwnedShare           8
        MarketValue                         8
        CostBasis                           $32
        ShareChange                         8
        Sector                              $32
        MaturityDate                        8
        AccruedInterest                     $32
        Coupon                              $32
        CreditQuality                       $32
        Duration                            $32
        IndustryId                          8
        GlobalIndustryId                    8
        GlobalSector                        8
        GICSIndustryId                      $32
        LocalCurrencyCode                   $3
        LocalMarketValue                    8
        ZAFAssetType                        $3
        PaymentType                         $32
        Rule144AEligible                    $32
        AltMinTaxEligible                   $32
        BloombergTicker                     $32
        ISOExchangeID                       $32
        ContractSize                        $32
        SecondarySectorId                   8
        CompanyId                           $32
        FirstBoughtDate                     8
        MexicanTipoValor                    $32
        MexicanSerie                        $32
        MexicanEmisora                      $32
        UnderlyingSecId                $32
        UnderlyingSecurityName              $32
        PerformanceId                       $32
        LessThanOneYearBond                 $32
        ZAFBondIssuerClass                  $4
        IndianCreditQualityClassificatio    $25   /*IndianCreditQualityClassification*/
        IndianIndustryClassification        $32
        SurveyedWeighting                   $32;
 informat _MasterPortfolioId                  comma24.0
          _CurrencyId                         $32.
          Portfolio_ExternalId                $32.
          Date                                IS8601DA10.
          PreviousPortfolioDate               IS8601DA10.
          NetExpenseRatio                     $32.
          NumberOfHoldingShort                comma24.0
          NumberOfStockHoldingShort           comma24.0
          NumberOfBondHoldingShort            comma24.0
          TotalMarketValueShort               comma24.0
          NumberOfHoldingLong                 comma24.0
          NumberOfStockHoldingLong            comma24.0
          NumberOfBondHoldingLong             comma24.0
          TotalMarketValueLong                comma24.0
          _Id                                 $32.
          _DetailHoldingTypeId                $32.
          _StorageId                          $26.
          _ExternalId                         $32.
          ExternalName                        $128.
          Country                             $32.
          Country_Id                          $32.
          CUSIP                               $32.
          SEDOL                               $32.
          ISIN                                $12.
          Ticker                              $32.
          Currency                            $32.
          Currency_Id                         $32.
          SecurityName                        $128.
          LocalName                           $32.
          Weighting                           $32.
          NumberOfShare                       comma24.0
          SharePercentage                     $32.
          NumberOfJointlyOwnedShare           comma24.0
          MarketValue                         comma24.0
          CostBasis                           $32.
          ShareChange                         comma24.0
          Sector                              $32.
          MaturityDate                        IS8601DA10.
          AccruedInterest                     $32.
          Coupon                              $32.
          CreditQuality                       $32.
          Duration                            $32.
          IndustryId                          comma24.0
          GlobalIndustryId                    comma24.0
          GlobalSector                        comma24.0
          GICSIndustryId                      $32.
          LocalCurrencyCode                   $3.
          LocalMarketValue                    comma24.0
          ZAFAssetType                        $3.
          PaymentType                         $32.
          Rule144AEligible                    $32.
          AltMinTaxEligible                   $32.
          BloombergTicker                     $32.
          ISOExchangeID                       $32.
          ContractSize                        $32.
          SecondarySectorId                   comma24.0
          CompanyId                           $32.
          FirstBoughtDate                     IS8601DA10.
          MexicanTipoValor                    $32.
          MexicanSerie                        $32.
          MexicanEmisora                      $32.
          UnderlyingSecId                $32.
          UnderlyingSecurityName              $32.
          PerformanceId                       $32.
          LessThanOneYearBond                 $32.
          ZAFBondIssuerClass                  $4.
          IndianCreditQualityClassificatio    $25.  /*IndianCreditQualityClassification*/
          IndianIndustryClassification        $32.
          SurveyedWeighting                   $32.;
retain  hdtext_01-hdtext_20
        filnam
        FileName
        Portfolio_Ordinal
        Holding_Ordinal
        PortfolioSummary_Ordinal
        Section
        _MasterPortfolioId
        _CurrencyId
        Portfolio_ExternalId
        Date
        PreviousPortfolioDate
        NetExpenseRatio
        NumberOfHoldingShort
        NumberOfStockHoldingShort
        NumberOfBondHoldingShort
        TotalMarketValueShort
        NumberOfHoldingLong
        NumberOfStockHoldingLong
        NumberOfBondHoldingLong
        TotalMarketValueLong
        _Id
        _DetailHoldingTypeId
        _StorageId
        _ExternalId
        ExternalName
        Country
        Country_Id
        CUSIP
        SEDOL
        ISIN
        Ticker
        Currency
        Currency_Id
        SecurityName
        LocalName
        Weighting
        NumberOfShare
        SharePercentage
        NumberOfJointlyOwnedShare
        MarketValue
        CostBasis
        ShareChange
        Sector
        MaturityDate
        AccruedInterest
        Coupon
        CreditQuality
        Duration
        IndustryId
        GlobalIndustryId
        GlobalSector
        GICSIndustryId
        LocalCurrencyCode
        LocalMarketValue
        ZAFAssetType
        PaymentType
        Rule144AEligible
        AltMinTaxEligible
        BloombergTicker
        ISOExchangeID
        ContractSize
        SecondarySectorId
        CompanyId
        FirstBoughtDate
        MexicanTipoValor
        MexicanSerie
        MexicanEmisora
        UnderlyingSecId
        UnderlyingSecurityName
        PerformanceId
        LessThanOneYearBond
        ZAFBondIssuerClass
        IndianCreditQualityClassificatio
        IndianIndustryClassification
        SurveyedWeighting                   ;
array nray _MasterPortfolioId-numeric-SurveyedWeighting;
array cray  Section-character-SurveyedWeighting;
array hrayc _Id-character-SurveyedWeighting;
array hrayn _Id-numeric-SurveyedWeighting;
array hdtext hdtext_01-hdtext_20;
infile "&ipath./&ifile."
        recfm = N filename = filnam dlm = "<";
filename = filnam;
input intext;
intext = left(trim(intext));
XMLTag = lowcase(scan(intext, 1, "<"));
XMLTag1 = scan(XMLTag,1,"<> ");
* output work.intext;
if XMLTag =: '/holdingdetail>' then do;
   output work.HoldingDetailX;
   do i = 1 to 20;
      hdtext{i} = "";
   end;
   return;
end;

if XMLTag =: '?xml version="1.0" encoding="utf-8"?>' then do;
  do over nray;
     nray = .;
  end;
  do over cray;
     cray = "";
  end;
end;
if XMLTag =: "portfolio _masterportfolioid" then do;
     _MasterPortfolioId = scan(intext, 3,'<>= "');
     _CurrencyId        = scan(intext,-1,'<>= "');
     Portfolio_Ordinal + 1;
end;
else if XMLTag =: "portfoliosummary>" then do;
     PortfolioSummary_Ordinal+1;
end;
else if XMLTag =: "/portfoliosummary>" then do;
     output work.PortfolioSummaryX;
     Holding_Ordinal = 0;
end;
else if XMLTag =: "date>"                               then Date = input(scan(intext,-1,'<>'),yymmdd10.);
else if XMLTag =: "previousportfoliodate>"              then PreviousPortfolioDate = input(scan(intext,-1,'<>'),yymmdd10.);
else if XMLTag =: 'holdingaggregate _saleposition="s">' then do;
          NumberOfHoldingShort        = .;
          NumberOfStockHoldingShort   = .;
          NumberOfBondHoldingShort    = .;
          TotalMarketValueShort       = .;
          Section = "S";
end;
else if XMLTag =: 'holdingaggregate _saleposition="l">' then do;
          NumberOfHoldingLong         = .;
          NumberOfStockHoldingLong    = .;
          NumberOfBondHoldingLong     = .;
          TotalMarketValueLong        = .;
          Section = "L";
end;
else if XMLTag =: 'holdingdetail' then do;
          do k = 1 to 20;
            hdtext{k} = left(scan(intext,k,'<>"'));
          end;
          hdtext{1} = left(scan(hdtext{1},2,' <>"'));
          do over hrayc;
              hrayc = "";
          end;
          do over hrayn;
             hrayn = .;
          end;
          Section = "D";
          do j = 1 to 19 by 2;
                   if lowcase(hdtext{j}) =: "_detailholdingtypeid" then _DetailHoldingTypeId = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "externalname"         then         ExternalName = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "_storageid"           then           _StorageId = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "_externalid"          then          _ExternalId = hdtext{j+1};
              else if lowcase(hdtext{j}) =: "_id"                  then                  _Id = hdtext{j+1};
          end;
          Holding_Ordinal + 1;
end;
if Section = "S" then do;
        if XMLTag =: 'numberofholding'      then NumberOfHoldingShort      = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofstockholding' then NumberOfStockHoldingShort = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofbondholding'  then NumberOfBondHoldingShort  = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'totalmarketvalue'     then TotalMarketValueShort     = input(scan(intext,-1,'<>'),comma16.0);
end;
if Section = "L" then do;
        if XMLTag =: 'numberofholding'      then NumberOfHoldingLong       = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofstockholding' then NumberOfStockHoldingLong  = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'numberofbondholding'  then NumberOfBondHoldingLong   = input(scan(intext,-1,'<>'),comma16.0);
   else if XMLTag =: 'totalmarketvalue'     then TotalMarketValueLong      = input(scan(intext,-1,'<>'),comma16.0);
end;

if Section = "D" then do;
        if XMLTag =: 'securityname>'                        then   SecurityName   = scan(intext,-1,'<>');
   else if XMLTag =: 'localname>'                           then   LocalName      = scan(intext,-1,'<>');
*    else if XMLTag =: '_id>'                                 then  _Id     = scan(intext,-1,'<>');
*    else if XMLTag =: '_detailholdingtypeid>'                then  _DetailHoldingTypeId     = scan(intext,-1,'<>');
*    else if XMLTag =: '_storageid>'                          then  _StorageId     = scan(intext,-1,'<>');
*    else if XMLTag =: '_externalid>'                         then  _ExternalId     = scan(intext,-1,'<>');
   else if XMLTag =: 'externalname>'                        then  ExternalName     = scan(intext,-1,'<>');
   else if XMLTag =: 'country _id'                          then  do;
           Country     = scan(intext,-1,'<>');
           Country_ID  = scan(intext, 2,'"<>=');
   end;
   else if XMLTag =: 'currency _id'                          then  do;
           Currency    = scan(intext,-1,'<>');
           Currency_Id = scan(intext, 2,'"<>=');
   end;
   else if XMLTag =: 'cusip>'                               then  CUSIP     = scan(intext,-1,'<>');
   else if XMLTag =: 'sedol>'                               then  SEDOL     = scan(intext,-1,'<>');
   else if XMLTag =: 'isin>'                                then  ISIN     = scan(intext,-1,'<>');
   else if XMLTag =: 'ticker>'                              then  Ticker     = scan(intext,-1,'<>');
*    else if XMLTag =: '/currency>'                            then  Currency     = scan(intext,-1,'<>');
   else if XMLTag =: 'securityname>'                        then  SecurityName     = scan(intext,-1,'<>');
   else if XMLTag =: 'localname>'                           then  LocalName     = scan(intext,-1,'<>');
   else if XMLTag =: 'weighting>'                           then  Weighting     = scan(intext,-1,'<>');
   else if XMLTag =: 'numberofshare>'                       then  NumberOfShare     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'sharepercentage>'                     then  SharePercentage     = scan(intext,-1,'<>');
   else if XMLTag =: 'numberofjointlyownedshare>'           then  NumberOfJointlyOwnedShare     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'marketvalue>'                         then  MarketValue     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'costbasis>'                           then  CostBasis     = scan(intext,-1,'<>');
   else if XMLTag =: 'sharechange>'                         then  ShareChange     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'sector>'                              then  Sector     = scan(intext,-1,'<>');
   else if XMLTag =: 'maturitydate>'                        then  MaturityDate     = input(scan(intext,-1,'<>'),yymmdd10.);
   else if XMLTag =: 'accruedinterest>'                     then  AccruedInterest     = scan(intext,-1,'<>');
   else if XMLTag =: 'coupon>'                              then  Coupon     = scan(intext,-1,'<>');
   else if XMLTag =: 'creditquality>'                       then  CreditQuality     = scan(intext,-1,'<>');
   else if XMLTag =: 'duration>'                            then  Duration     = scan(intext,-1,'<>');
   else if XMLTag =: 'industryid>'                          then  IndustryId     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'globalindustryid>'                    then  GlobalIndustryId     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'globalsector>'                        then  GlobalSector     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'gicsindustryid>'                      then  GICSIndustryId     = scan(intext,-1,'<>');
   else if XMLTag =: 'localcurrencycode>'                   then  LocalCurrencyCode     = scan(intext,-1,'<>');
   else if XMLTag =: 'localmarketvalue>'                    then  LocalMarketValue     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'zafassettype>'                        then  ZAFAssetType     = scan(intext,-1,'<>');
   else if XMLTag =: 'paymenttype>'                         then  PaymentType     = scan(intext,-1,'<>');
   else if XMLTag =: 'rule144aeligible>'                    then  Rule144AEligible     = scan(intext,-1,'<>');
   else if XMLTag =: 'altmintaxeligible>'                   then  AltMinTaxEligible     = scan(intext,-1,'<>');
   else if XMLTag =: 'secondarysectorid>'                   then  SecondarySectorId     = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'bloombergticker>'                     then  BloombergTicker     = scan(intext,-1,'<>');
   else if XMLTag =: 'isoexchangeid>'                       then  ISOExchangeId       = scan(intext,-1,'<>');
   else if XMLTag =: 'contractsize>'                        then  ContractSize       = input(scan(intext,-1,'<>'),comma24.0);
   else if XMLTag =: 'companyid>'                           then  CompanyId     = scan(intext,-1,'<>');
   else if XMLTag =: 'firstboughtdate>'                     then  FirstBoughtDate     = input(scan(intext,-1,'<>'),yymmdd10.);
   else if XMLTag =: 'mexicantipovalor>'                    then  MexicanTipoValor     = scan(intext,-1,'<>');
   else if XMLTag =: 'mexicanserie>'                        then  MexicanSerie     = scan(intext,-1,'<>');
   else if XMLTag =: 'mexicanemisora>'                      then  MexicanEmisora     = scan(intext,-1,'<>');
   else if XMLTag =: 'underlyingsecid>'                     then  UnderlyingSecId     = scan(intext,-1,'<>');
   else if XMLTag =: 'underlyingsecurityname>'              then  UnderlyingSecurityName     = scan(intext,-1,'<>');
   else if XMLTag =: 'performanceid>'                       then  PerformanceId     = scan(intext,-1,'<>');
   else if XMLTag =: 'lessthanoneyearbond>'                 then  LessThanOneYearBond     = scan(intext,-1,'<>');
   else if XMLTag =: 'zafbondissuerclass>'                  then  ZAFBondIssuerClass     = scan(intext,-1,'<>');
   else if XMLTag =: 'indiancreditqualityclassification>'   then  IndianCreditQualityClassificatio     = scan(intext,-1,'<>');
   else if XMLTag =: 'indianindustryclassification>'        then  IndianIndustryClassification     = scan(intext,-1,'<>');
   else if XMLTag =: 'surveyedweighting>'                   then  SurveyedWeighting     = scan(intext,-1,'<>');
end;
format MaturityDate
       Date
       PreviousPortfolioDate
       FirstBoughtDate     yymmddn8.;
run;
quit;
%mend;
%reder;

/**/
/**/
/**/


/**/
***************************************************************;
* Here we determine the number of portfolios altogether        *;
* and write out Stata datasets with 10,000 portfolios in each *;
***************************************************************;
%macro riter;
proc means data = work.PortfolioSummaryX noprint;
  var Portfolio_Ordinal;
  output out = work.Portfolio_Ordinal
           n = Portfolio_Count
         min = Portfolio_Min
         max = Portfolio_Max;
run;
data _null_;
 set work.portfolio_ordinal;
 call symput("FirstP",Portfolio_Min);
 call symput("LastP" ,Portfolio_Max);
run;
%put First Portfolio_Ordinal: &firstp.;
%put  Last Portfolio_Ordinal: &lastp. ;
libname S "&opath./";
%do p = &FirstP. %to &LastP. %by 10000;
    %let startp = %eval(&p.);
    %let   endp = %eval(&p. + 10000);
%put StartP:  &startp.;
%put   EndP:  &endp.  ;

data work.PortfolioSummaryX_&StartP.;
 set work.PortfolioSummaryX (where = (&startp. le Portfolio_Ordinal le &endp.));
run;
data work.HoldingDetailX_&startp.;
 set work.HoldingDetailX (where = (&startp. le Portfolio_Ordinal le &endp.));;
run;
proc export data = work.PortfolioSummaryX_&StartP.
   outfile = "&opath./PortfolioSummaryX_&StartP..dta"
   dbms = dta replace;
run;
quit;
proc export data = work.HoldingDetailX_&StartP.
   outfile = "&opath./HoldingDetailX_&StartP..dta"
   dbms = dta replace;
run;
quit;
%end;
%mend;
%riter;
