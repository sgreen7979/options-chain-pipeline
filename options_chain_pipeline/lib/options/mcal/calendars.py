from enum import Enum


class CMEGlobex:
    Livestock = 'CMEGlobex_Livestock'
    Live_Cattle = 'CMEGlobex_Live_Cattle'
    Feeder_Cattle = 'CMEGlobex_Feeder_Cattle'
    Lean_Hog = 'CMEGlobex_Lean_Hog'
    Port_Cutout = 'CMEGlobex_Port_Cutout'
    Cryptocurrencies = 'CME Globex Cryptocurrencies'
    Crypto = 'CME Globex Crypto'
    EnergyAndMetals = 'CMEGlobex_EnergyAndMetals'
    Energy = 'CMEGlobex_Energy'
    CrudeAndRefined = 'CMEGlobex_CrudeAndRefined'
    NYHarbor = 'CMEGlobex_NYHarbor'
    HO = 'CMEGlobex_HO'
    Crude = 'CMEGlobex_Crude'
    CL = 'CMEGlobex_CL'
    Gas = 'CMEGlobex_Gas'
    RB = 'CMEGlobex_RB'
    MicroCrude = 'CMEGlobex_MicroCrude'
    MCL = 'CMEGlobex_MCL'
    NatGas = 'CMEGlobex_NatGas'
    NG = 'CMEGlobex_NG'
    Dutch_NatGas = 'CMEGlobex_Dutch_NatGas'
    TTF = 'CMEGlobex_TTF'
    LastDay_NatGas = 'CMEGlobex_LastDay_NatGas'
    NN = 'CMEGlobex_NN'
    CarbonOffset = 'CMEGlobex_CarbonOffset'
    CGO = 'CMEGlobex_CGO'
    NGO = 'CMEGlobex_NGO'
    GEO = 'CMEGlobex_GEO'
    Metals = 'CMEGlobex_Metals'
    PreciousMetals = 'CMEGlobex_PreciousMetals'
    Gold = 'CMEGlobex_Gold'
    GC = 'CMEGlobex_GC'
    Silver = 'CMEGlobex_Silver'
    SI = 'CMEGlobex_SI'
    Platinum = 'CMEGlobex_Platinum'
    PL = 'CMEGlobex_PL'
    BaseMetals = 'CMEGlobex_BaseMetals'
    Copper = 'CMEGlobex_Copper'
    HG = 'CMEGlobex_HG'
    Aluminum = 'CMEGlobex_Aluminum'
    ALI = 'CMEGlobex_ALI'
    QC = 'CMEGlobex_QC'
    FerrousMetals = 'CMEGlobex_FerrousMetals'
    HRC = 'CMEGlobex_HRC'
    BUS = 'CMEGlobex_BUS'
    TIO = 'CMEGlobex_TIO'
    Equity = 'CME Globex Equity'
    FX = 'CMEGlobex_FX'
    Fixed_Income = 'CME Globex Fixed Income'
    Interest_Rate_Products = 'CME Globex Interest Rate Products'


class Commodities:
    class Agriculture:
        CME = 'CME_Agriculture'
        CBOT = 'CBOT_Agriculture'
        COMEX = 'COMEX_Agriculture'
        NYMEX = 'NYMEX_Agriculture'

    class PreciousMedals:
        CME_Precious = CMEGlobex.PreciousMetals
        CME_Silver = CMEGlobex.Silver
        CME_SI = CMEGlobex.SI
        CME_Platinum = CMEGlobex.Platinum
        CME_PL = CMEGlobex.PL
        CME_Gold = CMEGlobex.Gold
        CME_GC = CMEGlobex.GC
        CME_Aluminum = CMEGlobex.Aluminum
        CME_ALI = CMEGlobex.ALI
        CME_Copper = CMEGlobex.Copper
        CME_HG = CMEGlobex.HG

    class Metals:
        CME_Metals = CMEGlobex.Metals
        CME_BaseMetals = CMEGlobex.BaseMetals


class MarketCalendars(Enum):
    ASX = 'ASX'
    BMF = 'BMF'
    B3 = 'B3'
    BSE = 'BSE'
    NSE = 'NSE'
    XNSE = 'XNSE'
    CFE = 'CFE'
    CBOE_Futures = 'CBOE_Futures'
    CBOE_Equity_Options = 'CBOE_Equity_Options'
    CBOE_Index_Options = 'CBOE_Index_Options'
    CME_Equity = 'CME_Equity'
    CBOT_Equity = 'CBOT_Equity'
    CME_Agriculture = 'CME_Agriculture'
    CBOT_Agriculture = 'CBOT_Agriculture'
    COMEX_Agriculture = 'COMEX_Agriculture'
    NYMEX_Agriculture = 'NYMEX_Agriculture'
    CME_Rate = 'CME_Rate'
    CBOT_Rate = 'CBOT_Rate'
    CME_InterestRate = 'CME_InterestRate'
    CBOT_InterestRate = 'CBOT_InterestRate'
    CME_Bond = 'CME_Bond'
    CBOT_Bond = 'CBOT_Bond'
    CMEGlobex_Livestock = 'CMEGlobex_Livestock'
    CMEGlobex_Live_Cattle = 'CMEGlobex_Live_Cattle'
    CMEGlobex_Feeder_Cattle = 'CMEGlobex_Feeder_Cattle'
    CMEGlobex_Lean_Hog = 'CMEGlobex_Lean_Hog'
    CMEGlobex_Port_Cutout = 'CMEGlobex_Port_Cutout'
    CMEGlobex_Cryptocurrencies = 'CME Globex Cryptocurrencies'
    CMEGlobex_Crypto = 'CME Globex Crypto'
    CMEGlobex_EnergyAndMetals = 'CMEGlobex_EnergyAndMetals'
    CMEGlobex_Energy = 'CMEGlobex_Energy'
    CMEGlobex_CrudeAndRefined = 'CMEGlobex_CrudeAndRefined'
    CMEGlobex_NYHarbor = 'CMEGlobex_NYHarbor'
    CMEGlobex_HO = 'CMEGlobex_HO'
    HO = 'HO'
    CMEGlobex_Crude = 'CMEGlobex_Crude'
    CMEGlobex_CL = 'CMEGlobex_CL'
    CL = 'CL'
    CMEGlobex_Gas = 'CMEGlobex_Gas'
    CMEGlobex_RB = 'CMEGlobex_RB'
    RB = 'RB'
    CMEGlobex_MicroCrude = 'CMEGlobex_MicroCrude'
    CMEGlobex_MCL = 'CMEGlobex_MCL'
    MCL = 'MCL'
    CMEGlobex_NatGas = 'CMEGlobex_NatGas'
    CMEGlobex_NG = 'CMEGlobex_NG'
    NG = 'NG'
    CMEGlobex_Dutch_NatGas = 'CMEGlobex_Dutch_NatGas'
    CMEGlobex_TTF = 'CMEGlobex_TTF'
    TTF = 'TTF'
    CMEGlobex_LastDay_NatGas = 'CMEGlobex_LastDay_NatGas'
    CMEGlobex_NN = 'CMEGlobex_NN'
    NN = 'NN'
    CMEGlobex_CarbonOffset = 'CMEGlobex_CarbonOffset'
    CMEGlobex_CGO = 'CMEGlobex_CGO'
    CGO = 'CGO'
    CGEO = 'C-GEO'
    CMEGlobex_NGO = 'CMEGlobex_NGO'
    NGO = 'NGO'
    CMEGlobex_GEO = 'CMEGlobex_GEO'
    GEO = 'GEO'
    CMEGlobex_Metals = 'CMEGlobex_Metals'
    CMEGlobex_PreciousMetals = 'CMEGlobex_PreciousMetals'
    CMEGlobex_Gold = 'CMEGlobex_Gold'
    CMEGlobex_GC = 'CMEGlobex_GC'
    GC = 'GC'
    CMEGlobex_Silver = 'CMEGlobex_Silver'
    CMEGlobex_SI = 'CMEGlobex_SI'
    SI = 'SI'
    CMEGlobex_Platinum = 'CMEGlobex_Platinum'
    CMEGlobex_PL = 'CMEGlobex_PL'
    PL = 'PL'
    CMEGlobex_BaseMetals = 'CMEGlobex_BaseMetals'
    CMEGlobex_Copper = 'CMEGlobex_Copper'
    CMEGlobex_HG = 'CMEGlobex_HG'
    HG = 'HG'
    CMEGlobex_Aluminum = 'CMEGlobex_Aluminum'
    CMEGlobex_ALI = 'CMEGlobex_ALI'
    ALI = 'ALI'
    CMEGlobex_QC = 'CMEGlobex_QC'
    QC = 'QC'
    CMEGlobex_FerrousMetals = 'CMEGlobex_FerrousMetals'
    CMEGlobex_HRC = 'CMEGlobex_HRC'
    HRC = 'HRC'
    CMEGlobex_BUS = 'CMEGlobex_BUS'
    BUS = 'BUS'
    CMEGlobex_TIO = 'CMEGlobex_TIO'
    TIO = 'TIO'
    CMEGlobex_Equity = 'CME Globex Equity'
    CMEGlobex_FX = 'CMEGlobex_FX'
    CME_FX = 'CME_FX'
    CME_Currency = 'CME_Currency'
    CMEGlobex_Fixed_Income = 'CME Globex Fixed Income'
    CMEGlobex_Interest_Rate_Products = 'CME Globex Interest Rate Products'
    EUREX = 'EUREX'
    EUREX_Bond = 'EUREX_Bond'
    HKEX = 'HKEX'
    ICE = 'ICE'
    ICEUS = 'ICEUS'
    NYFE = 'NYFE'
    NYSE = 'NYSE'
    stock = 'stock'
    NASDAQ = 'NASDAQ'
    BATS = 'BATS'
    DJIA = 'DJIA'
    DOW = 'DOW'
    IEX = 'IEX'
    Investors_Exchange = 'Investors_Exchange'
    JPX = 'JPX'
    XJPX = 'XJPX'
    LSE = 'LSE'
    OSE = 'OSE'
    SIFMAUS = 'SIFMAUS'
    SIFMA_US = 'SIFMA_US'
    Capital_Markets_US = 'Capital_Markets_US'
    Financial_Markets_US = 'Financial_Markets_US'
    Bond_Markets_US = 'Bond_Markets_US'
    SIFMAUK = 'SIFMAUK'
    SIFMA_UK = 'SIFMA_UK'
    Capital_Markets_UK = 'Capital_Markets_UK'
    Financial_Markets_UK = 'Financial_Markets_UK'
    Bond_Markets_UK = 'Bond_Markets_UK'
    SIFMAJP = 'SIFMAJP'
    SIFMA_JP = 'SIFMA_JP'
    Capital_Markets_JP = 'Capital_Markets_JP'
    Financial_Markets_JP = 'Financial_Markets_JP'
    Bond_Markets_JP = 'Bond_Markets_JP'
    SIX = 'SIX'
    SSE = 'SSE'
    TASE = 'TASE'
    TSX = 'TSX'
    TSXV = 'TSXV'
    AIXK = 'AIXK'
    ASEX = 'ASEX'
    BVMF = 'BVMF'
    CMES = 'CMES'
    IEPA = 'IEPA'
    XAMS = 'XAMS'
    XASX = 'XASX'
    XBKK = 'XBKK'
    XBOG = 'XBOG'
    XBOM = 'XBOM'
    XBRU = 'XBRU'
    XBSE = 'XBSE'
    XBUD = 'XBUD'
    XBUE = 'XBUE'
    XCBF = 'XCBF'
    XCSE = 'XCSE'
    XDUB = 'XDUB'
    XFRA = 'XFRA'
    XETR = 'XETR'
    XHEL = 'XHEL'
    XHKG = 'XHKG'
    XICE = 'XICE'
    XIDX = 'XIDX'
    XIST = 'XIST'
    XJSE = 'XJSE'
    XKAR = 'XKAR'
    XKLS = 'XKLS'
    XKRX = 'XKRX'
    XLIM = 'XLIM'
    XLIS = 'XLIS'
    XLON = 'XLON'
    XMAD = 'XMAD'
    XMEX = 'XMEX'
    XMIL = 'XMIL'
    XMOS = 'XMOS'
    XNYS = 'XNYS'
    XNZE = 'XNZE'
    XOSL = 'XOSL'
    XPAR = 'XPAR'
    XPHS = 'XPHS'
    XPRA = 'XPRA'
    XSAU = 'XSAU'
    XSES = 'XSES'
    XSGO = 'XSGO'
    XSHG = 'XSHG'
    XSTO = 'XSTO'
    XSWX = 'XSWX'
    XTAE = 'XTAE'
    XTAI = 'XTAI'
    XTKS = 'XTKS'
    XTSE = 'XTSE'
    XWAR = 'XWAR'
    XWBO = 'XWBO'
    us_futures = 'us_futures'
    _247 = '24/7'
    TWENTYFOURSEVEN = _247
    _245 = '24/5'
    TWENTYFOURFIVE = _245
