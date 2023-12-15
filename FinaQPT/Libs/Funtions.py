
# Code Description

"""
@author: QuantTeam
In this code you can find all the functions for the TIIE IRS Toolkit.

"""
#-------------
#  Libraries
#-------------

import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import QuantLib as ql

# ----------------
# Global variables
# ----------------

tenor2ql = {'B': ql.Days, 'D': ql.Days, 'M': ql.Months, 'W': ql.Weeks, 
            'Y': ql.Years}


        
def historical_curves(start_date: datetime) -> [ql.DiscountCurve, 
                                                ql.DiscountCurve]: 
    """ Creates closing curves
    

    Parameters
    ----------
    start_date : datetime
        Day of the curve wanted

    Raises
    ------
    Exception
        If OIS curve is no found, it will raise an exception

    Returns
    -------
    List of QuantLib Discount Curves

    """
    try:
        df_OIS = pd.read_excel( '//TLALOC/Cuantitativa/Fixed Income'+\
                               '/Historical OIS TIIE/OIS_'+\
                                   str(start_date.year)+\
                                   str('%02d' % start_date.month) + \
                                   str('%02d' % start_date.day) + \
                                   '.xlsx')
    except:
        print('Missing file: OIS_' + str(start_date.year) + 
              str('%02d' % start_date.month) + str('%02d' % start_date.day) + 
              '.xlsx')
        check_OIS = False
        
        raise Exception('PLease check OIS Files')
    
    try:
        df_TIIE = pd.read_excel( '//TLALOC/Cuantitativa/Fixed Income'+\
                               '/Historical OIS TIIE/TIIE_'+\
                                   str(start_date.year)+\
                                   str('%02d' % start_date.month) + \
                                   str('%02d' % start_date.day) + \
                                   '.xlsx')
    except:
        print('Missing file: TIIE_' + str(start_date.year) + 
              str('%02d' % start_date.month) + str('%02d' % start_date.day) + 
              '.xlsx')
        check_OIS = False
        
        
    period_file = min(len(df_OIS), len(df_TIIE), 11650)
    check_OIS = True 
    
    
    effective_date = ql.Date(start_date.day, start_date.month, start_date.year)
    period = ql.Period(period_file -1, ql.Days)
    termination_date = effective_date + period
    tenor = ql.Period(ql.Daily)
    calendar = ql.Mexico()
    business_convention = ql.Unadjusted
    termination_business_convention = ql.Following
    date_generation = ql.DateGeneration.Forward
    end_of_month = True

    schedule = ql.Schedule(effective_date, termination_date, tenor, calendar,
                           business_convention, 
                           termination_business_convention, date_generation,
                           end_of_month)

    dates = []
    for i, d in enumerate(schedule):
        dates.append(d)

    #QauntLib curves (OIS, TIIE) creation
     
    lstOIS_dfs = [1]
    valores_ois = df_OIS['VALOR'][:min(df_OIS.shape[0]-1,11649)]
    plazos_ois = df_OIS['PLAZO'][:min(df_OIS.shape[0]-1,11649)]
    lstOIS_dfs.extend(
        [1/(1 + r*t/36000) for (r, t) in zip(valores_ois, plazos_ois)])

    # for i in range(0, min(df_OIS.shape[0]-1,11649)):
    #     t,r = df_OIS.iloc[i,[1,2]]
    #     lstOIS_dfs.append(1/(1 + r*t/36000)) 
        
    lstTIIE_dfs = [1]
    valores_tiie = df_TIIE['VALOR'][:min(df_TIIE.shape[0]-1,11649)]
    plazos_tiie = df_TIIE['PLAZO'][:min(df_TIIE.shape[0]-1,11649)]
    lstTIIE_dfs.extend(
        [1/(1 + r*t/36000) for (r, t) in zip(valores_tiie, plazos_tiie)])

    # for i in range(0, min(df_TIIE.shape[0]-1,11649)):
    #     t,r = df_TIIE.iloc[i,[1,2]]
    #     lstTIIE_dfs.append(1/(1 + r*t/36000))
    
    crvTIIE = ql.DiscountCurve(dates, lstTIIE_dfs, ql.Actual360(), ql.Mexico())
    crvMXNOIS = ql.DiscountCurve(dates, lstOIS_dfs, ql.Actual360(), ql.Mexico())
    
    return [crvMXNOIS, crvTIIE]                      

    

# -------
# Helpers
# -------

# USDOIS

def qlHelper_USDOIS(df: pd.DataFrame) -> list:
    """Creates helpers to bootstrap USDOIS curve.
    

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with quotes and tenors for USDOIS curve.

    Returns
    -------
    list
        List of QuantLib objects with USDOIS helpers.

    """
    
    # market calendar
    calendar = ql.UnitedStates(1)
    
    # input data
    tenor = df['Tenors'].str[-1].map(tenor2ql).to_list()
    period = df['Period'].to_list()
    data = (df['Quotes']/100).tolist()
    
    # Deposit rates
    deposits = {(period[0], tenor[0]): data[0]}
    
    # Swap rates
    n = len(period)
    swaps = {}
    for i in range(1,n):
        swaps[(period[i], tenor[i])] = data[i]
        
    # Rate Qauntlib.Quote objects
    ## desposits
    for n, unit in deposits.keys():
        deposits[(n, unit)] = ql.SimpleQuote(deposits[(n, unit)])
    ## swap rates
    for n, unit in swaps.keys():
        swaps[(n, unit)] = ql.SimpleQuote(swaps[(n, unit)])
        
    # Rate helpers deposits
    dayCounter = ql.Actual360()
    settlementDays = 2
    ## deposits
    depositHelpers = [ql.DepositRateHelper(ql.QuoteHandle(deposits[(n, unit)]),
                                           ql.Period(n, unit), 
                                           settlementDays,
                                           calendar, 
                                           ql.ModifiedFollowing, 
                                           False, 
                                           dayCounter) 
                      for (n, unit) in deposits.keys()]
    ## swap rates
    OIS_Index = ql.FedFunds()
    OISHelpers = [ql.OISRateHelper(settlementDays, ql.Period(n, unit),
                                   ql.QuoteHandle(swaps[(n,unit)]),
                                   OIS_Index) 
                  for n, unit in swaps.keys()]
    ## helpers merge
    helpers = depositHelpers + OISHelpers
    
    return(helpers)

# USDSOFR

def qlHelper_SOFR(
        dic_df: dict, 
        discount_curve: ql.RelinkableYieldTermStructureHandle) -> list: 
    """Creates helpers to bootstrap USDSOFR curve.
    
    Uses a dictionary with the necessary DataFrames to bootstrap USDSOFR 
    curve.
    

    Parameters
    ----------
    dic_df : dict
        Dictionary with DataFrames that have the quotes and tenors for 
        USDOIS and USDSOFR.
    discount_curve : ql.RelinkableYieldTermStructureHandle
        Curve used to discount cashflows from swaps.

    Returns
    -------
    list
        List of QuantLib objects with USDSOFR helpers.

    """
    
    # market calendar
    calendar = ql.UnitedStates(1)
    
    # settlement date
    dt_settlement = ql.UnitedStates(1).advance(
            ql.Settings.instance().evaluationDate, ql.Period('2D'))
    # non-futures idx
    df = dic_df['USD_SOFR']
    idx_nonfut = (df['Types'] != 'FUT')
    # input data
    tenor = df['Tenors'][idx_nonfut].str[-1].map(tenor2ql).to_list()
    period = df['Period'][idx_nonfut].to_list()
    data_nonfut = (df['Quotes'][idx_nonfut]/100).tolist()
    data_fut = (df['Quotes'][~idx_nonfut]/100).tolist()
    
    # IborIndex
    swapIndex = ql.Sofr()

    # Deposit rates
    deposits = {(period[0], tenor[0]): data_nonfut[0]}
   
    # Futures rates
    n_fut = len(data_fut)
    imm = ql.IMM.nextDate(dt_settlement)
    imm = dt_settlement
    futures = {}
    for i in range(n_fut):
        imm = ql.IMM.nextDate(imm)
        futures[imm] = 100 - data_fut[i]*100  
    
    # Swap rates
    n = len(period)
    swaps = {}
    for i in range(1, n):
        swaps[(period[i], tenor[i])] = data_nonfut[i]
        
    # Rate Qauntlib.Quote objects
    ## desposits
    
    for n, unit in deposits.keys():
        deposits[(n, unit)] = ql.SimpleQuote(deposits[(n, unit)])
    ## futures
    for d in futures.keys():
        futures[d] = futures[d]
    ## swap rates
    for n, unit in swaps.keys():
        swaps[(n, unit)] = ql.SimpleQuote(swaps[(n, unit)])
        
    # Rate helpers deposits
    dayCounter = ql.Actual360()
    settlementDays = 2
    ## deposits
    depositHelpers = [ql.DepositRateHelper(ql.QuoteHandle(deposits[(n, unit)]),
                                           ql.Period(n, unit), 
                                           settlementDays,
                                           calendar, 
                                           ql.ModifiedFollowing, 
                                           False, 
                                           dayCounter) 
                      for n, unit in deposits.keys()]
    ## futures
    months = 3
    futuresHelpers = [ql.FuturesRateHelper(
        ql.QuoteHandle(ql.SimpleQuote(futures[d])), 
        d, months, calendar, 
        ql.ModifiedFollowing, True, dayCounter
        ) 
        for d in futures.keys()
    ]
    
    ## swap rates
    fixedLegFrequency = ql.Annual
    fixedLegAdjustment = ql.ModifiedFollowing
    fixedLegDayCounter = ql.Actual360()
    ## swaphelper
    swapHelpers = [ql.SwapRateHelper(
        ql.QuoteHandle(swaps[(n,unit)]),
        ql.Period(n, unit), 
        calendar,
        fixedLegFrequency, 
        fixedLegAdjustment,
        fixedLegDayCounter, 
        swapIndex, 
        ql.QuoteHandle(), 
        ql.Period(2, ql.Days),
        discount_curve
        )
        for n, unit in swaps.keys()
    ]

    ## helpers merge
    helpers = depositHelpers + futuresHelpers + swapHelpers

    return(helpers)


#LIBOR 3M

def qlHelper_USD3M(dic_df: dict, 
               discount_curve: ql.RelinkableYieldTermStructureHandle) -> list:
    """Creates helpers to bootstarp USD3M LIBOR.
    
    Uses a dictionary with the necessary DataFrames to bootstrap USD3M 
    LIBOR curve. 
    

    Parameters
    ----------
    dic_df : dict
        Dictionary with DataFrames that have the quotes and tenors for 
        USDOIS and USD3M LIBOR.
    discount_curve : ql.RelinkableYieldTermStructureHandle
        Curve used to discount cashflows from swaps.

    Returns
    -------
    list
        List of QuantLib objects with USD3M LIBOR helpers.

    """
    
    # market calendar
    calendar = ql.UnitedStates(1)
    # settlement date
    dt_settlement = ql.UnitedStates(1).advance(
        ql.Settings.instance().evaluationDate,ql.Period('2D'))
    # non-futures idx
    df = dic_df['USD_LIBOR_3M']
    idx_nonfut = (df['Types'] != 'FUT')
    # input data
    tenor = df['Tenors'][idx_nonfut].str[-1].map(tenor2ql).to_list()
    period = df['Period'][idx_nonfut].to_list()
    data_nonfut = (df['Quotes'][idx_nonfut]/100).tolist()
    data_fut = (df['Quotes'][~idx_nonfut]/100).tolist()
    # IborIndex
    swapIndex = ql.USDLibor(ql.Period(3, ql.Months))

    # Deposit rates
    deposits = {(period[0], tenor[0]): data_nonfut[0]}
    # Futures rates
    n_fut = len(data_fut)
    imm = ql.IMM.nextDate(dt_settlement)
    imm = dt_settlement
    futures = {}
    for i in range(n_fut):
        imm = ql.IMM.nextDate(imm)
        futures[imm] = 100 - data_fut[i]*100  
    # Swap rates
    n = len(period)
    swaps = {}
    for i in range(1,n):
        swaps[(period[i], tenor[i])] = data_nonfut[i]
        
    # Rate Qauntlib.Quote objects
    ## desposits
    for n, unit in deposits.keys():
        deposits[(n,unit)] = ql.SimpleQuote(deposits[(n,unit)])
    ## futures
    for d in futures.keys():
        futures[d] = futures[d]
    ## swap rates
    for n,unit in swaps.keys():
        swaps[(n,unit)] = ql.SimpleQuote(swaps[(n,unit)])
        
    # Rate helpers deposits
    dayCounter = ql.Actual360()
    settlementDays = 2
    ## deposits
    depositHelpers = [ql.DepositRateHelper(
        ql.QuoteHandle(deposits[(n, unit)]),
        ql.Period(n, unit), 
        settlementDays,
        calendar, 
        ql.ModifiedFollowing, 
        False, 
        dayCounter
        ) 
        for n, unit in deposits.keys()
    ]
    ## futures
    months = 3
    futuresHelpers = [ql.FuturesRateHelper(
            ql.QuoteHandle(ql.SimpleQuote(futures[d])), 
            d, months, calendar, 
            ql.ModifiedFollowing, True, dayCounter) 
        for d in futures.keys()]
    
    ## swap rates
    fixedLegFrequency = ql.Semiannual
    fixedLegAdjustment = ql.ModifiedFollowing
    fixedLegDayCounter = ql.Thirty360()
    
    ## swaphelper
    swapHelpers = [ql.SwapRateHelper(
            ql.QuoteHandle(swaps[(n,unit)]), ql.Period(n, unit), calendar,
            fixedLegFrequency, fixedLegAdjustment, fixedLegDayCounter, 
            swapIndex, ql.QuoteHandle(), ql.Period(0, ql.Days),discount_curve)
        for n, unit in swaps.keys()]

    ## helpers merge
    helpers = depositHelpers + futuresHelpers + swapHelpers

    return(helpers)

#LIBOR 1M

def qlHelper_USD1M(dic_df: dict, crv_USD3M: ql.DiscountCurve) -> list:   
    """Creates helpers to bootstrap USDM1M LIBOR curve.
    
    Uses a dictionary with the necessary DataFrames to bootstrap USD1M 
    LIBOR curve.
    

    Parameters
    ----------
    dic_df : dict
        Dictionary with DataFrames that have the quotes and tenors for 
        USDOIS, USD3M LIBOR, and USD1M LIBOR.
    crv_USD3M : ql.DiscountCurve
        USD3M LIBOR bootstrapped curve.

    Returns
    -------
    list
        List of QuantLib objects with USD1M LIBOR helpers.

    """
    
    # market calendar
    calendar = ql.UnitedStates(1)
    
    # dat
    df = dic_df['USD_LIBOR_3Mvs1M_Basis']
    # input data
    tenor = df['Tenor'].str[-1].map(tenor2ql).to_list()
    period = df['Period'].to_list()
    data = (df['Quotes']/100).tolist()
    
    # Deposit rates
    deposits = {(period[0], tenor[0]): data[0]}
    # Basis rates
    n = len(period)
    basis = {}
    for i in range(1, n):
        basis[(period[i], tenor[i])] = data[i]/100
        
    # Rate Qauntlib.Quote objects
    for n, unit in deposits.keys():
        deposits[(n, unit)] = ql.SimpleQuote(deposits[(n, unit)])
    for n, unit in basis.keys():
        basis[(n, unit)] = ql.SimpleQuote(basis[(n, unit)])

    # Deposit rate helpers
    dayCounter = ql.Actual360()
    settlementDays = 2
    depositHelpers = [ql.DepositRateHelper(ql.QuoteHandle(deposits[(n, unit)]),
                                           ql.Period(n, unit), 
                                           settlementDays, calendar, 
                                           ql.ModifiedFollowing, False, 
                                           dayCounter)
                      for n, unit in deposits.keys()]
    
    # Basis helper
    crv_handle = ql.RelinkableYieldTermStructureHandle()
    baseCurrencyIndex = ql.USDLibor(ql.Period('3M'), crv_USD3M)
    quoteCurrencyIndex = ql.USDLibor(ql.Period('1M'), crv_handle)
    crv_ff = ql.FlatForward(0, calendar, 0.01, ql.Actual360())
    crv_handle.linkTo(crv_ff)
    isFxBaseCurrencyCollateralCurrency = False
    isBasisOnFxBaseCurrencyLeg = False
    basis_helper = [ql.CrossCurrencyBasisSwapRateHelper(
            ql.QuoteHandle(basis[(n, unit)]), 
            ql.Period(n, unit), 
            settlementDays, 
            calendar, 
            ql.ModifiedFollowing, 
            False,
            baseCurrencyIndex, 
            quoteCurrencyIndex, 
            crv_handle,
            isFxBaseCurrencyCollateralCurrency, 
            isBasisOnFxBaseCurrencyLeg)
        for n, unit in basis.keys()]
    ## helpers merge
    helpers = depositHelpers + basis_helper

    return(helpers)

# MXNOIS

def qlHelper_MXNOIS(dic_df: dict, 
                    discount_curve: ql.RelinkableYieldTermStructureHandle, 
                    crv_usdswp: ql.RelinkableYieldTermStructureHandle, 
                    crvType: str = 'SOFR') -> list:   
    """Creates helpers to bootstrap MXN_OIS.
    
    Uses a dictionary with necessary quotes to bootstrap MXNOIS, as well
    as a discount curve and a projection curve for swap cashflows.
    

    Parameters
    ----------
    dic_df : dict
        Dictionary with USDMXN_XCCY_Basis, USDMXN_Fwds, and MXN_TIIE
        quotes that are needed to build MXNOIS.
    discount_curve : ql.RelinkableYieldTermStructureHandle
        Curve used to discount cashflows, usually USDOIS.
    crv_usdswp : ql.RelinkableYieldTermStructureHandle
        Curve used project swap cashflows, usually USDSOFR (but could be
        LIBOR or other curve).
    crvType : str, optional
        Indicates if we are using SOFR or other curve, since the sign of
        the basis points changes depending on the curve type. The 
        default is 'SOFR'.

    Returns
    -------
    list
        List of QuantLib objects with MXNOIS helpers.

    """
    
    # Calendars
    calendar_mx = ql.Mexico()

    # Handle data
    spotfx = dic_df['USDMXN_XCCY_Basis']['Quotes'][0]
    df_basis = dic_df['USDMXN_XCCY_Basis']
    df_tiie = dic_df['MXN_TIIE']
    df_fwds = dic_df['USDMXN_Fwds']
    
    # Handle idxs
    idx_fwds = np.where(np.isin(df_fwds['Tenor'], 
                                ['%3M','%6M', '%9M', '%1Y']))[0].tolist()
    lst_tiieT = ['%1L', '%26L', '%39L', '%52L', '%65L', 
                 '%91L', '%130L', '%195L', '%260L', '%390L']
    idx_tiie = np.where(np.isin(df_tiie['Tenor'], 
                                lst_tiieT))[0].tolist()
    # Input data
    tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                'Y': ql.Years}
    # calendar
    # data
    def f(x):
        if x[-1]=='L':
            return int(x[1:-1])*4
        else:
            return int(x[1:-1])
        
    tenors = df_tiie['Tenor'][idx_tiie]
    
    tenor_type = tenors.str[-1].map(tenor2ql).tolist()
    tenor = ql.EveryFourthWeek

    basis_period = df_basis['Period'].tolist()
    tiie_period = tenors.map(lambda x: f(x)).tolist()
    fwds_period = df_fwds['Period'][idx_fwds].to_list()
    data_tiie = (df_tiie['Quotes'][idx_tiie]/100).tolist()
    data_fwds = (df_fwds['Quotes'][idx_fwds]/10000).tolist()
    
    # Change basis sign when using SOFR
    if crvType == 'SOFR':
        data_basis = (-1*df_basis['Quotes']/10000).tolist()
    
    else:
        data_basis = (df_basis['Quotes']/10000).tolist()
    
    # Basis swaps
    basis_usdmxn = {}
    n_basis = len(basis_period)
    
    for i in range(1, n_basis):
        basis_usdmxn[(basis_period[i], tenor)] = data_basis[i]

    # Forward Points
    fwdpts = {}
    n_fwds = len(fwds_period)
    
    for i in range(n_fwds):
        fwdpts[(fwds_period[i], tenor)] = data_fwds[i]

    # Deposit rates
    deposits = {(tiie_period[0], tenor_type[0]): data_tiie[0]}
    
    # TIIE Swap rates
    swaps_tiie = {}
    n_tiie = len(tiie_period)
    
    for i in range(1, n_tiie):
        swaps_tiie[(tiie_period[i], tenor_type[i])] = data_tiie[i]

    # Qauntlib.Quote objects
    for n, unit in basis_usdmxn.keys():
        basis_usdmxn[(n, unit)] = ql.SimpleQuote(basis_usdmxn[(n, unit)])
    
    for n, unit in fwdpts.keys():
        fwdpts[(n, unit)] = ql.SimpleQuote(fwdpts[(n, unit)])
    
    for n, unit in deposits.keys():
        deposits[(n, unit)] = ql.SimpleQuote(deposits[(n, unit)])
    
    for n, unit in swaps_tiie.keys():
        swaps_tiie[(n, unit)] = ql.SimpleQuote(swaps_tiie[(n, unit)])
        
    # Deposit rate helper
    dayCounter = ql.Actual360()
    settlementDays = 1
    depositHelpers = [ql.DepositRateHelper(ql.QuoteHandle(deposits[(n, unit)]),
                                           ql.Period(n, ql.Weeks), 
                                           settlementDays, calendar_mx, 
                                           ql.Following, False,  dayCounter)
                      for n, unit in deposits.keys()]

    # FX Forwards helper
    fxSwapHelper = [ql.FxSwapRateHelper(ql.QuoteHandle(fwdpts[(n,u)]),
                                        ql.QuoteHandle(ql.SimpleQuote(spotfx)),
                                        ql.Period(n*4, ql.Weeks), 2,
                                        calendar_mx, ql.Following,
                                        False, True, discount_curve) 
                    for n,u in fwdpts.keys()]

    # Swap rate helpers
    settlementDays = 2
    fixedLegFrequency = ql.EveryFourthWeek
    fixedLegAdjustment = ql.Following
    fixedLegDayCounter = ql.Actual360()
    
    if crvType == 'SOFR':
        fxIborIndex = ql.Sofr(crv_usdswp)
    
    else:
        fxIborIndex = ql.USDLibor(ql.Period('1M'), crv_usdswp)

    swapHelpers = [ql.SwapRateHelper(ql.QuoteHandle(swaps_tiie[(n, unit)]),
                                     ql.Period(n, ql.Weeks), 
                                     calendar_mx,
                                     fixedLegFrequency, 
                                     fixedLegAdjustment,
                                     fixedLegDayCounter, 
                                     fxIborIndex, 
                                     ql.QuoteHandle(
                                         basis_usdmxn[(n/4, tenor)]), 
                                     ql.Period(0, ql.Days))
                   for n, unit in swaps_tiie.keys()]

    # Rate helpers merge
    helpers = depositHelpers + fxSwapHelper + swapHelpers

    return(helpers)

# MXNTIIE

def qlHelper_MXNTIIE(df: pd.DataFrame, 
                     crv_MXNOIS: ql.RelinkableYieldTermStructureHandle, 
                     updateAll=False, bo_crv: np.array = np.array([])) -> list:
    """Creates helpers to bootstrap MXNTIIE curve.
    
    Uses a DataFrame with MXNTIIE quotes, as well as MXNOIS curve 
    bootstrapped earlier, to bootstrap MXNTIIE.
    

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with MXNTIIE quotes.
    crv_MXNOIS : ql.RelinkableYieldTermStructureHandle
        MXNOIS curve used to discount cashflows.
    updateAll : bool | str, optional
        Flag that indicates if we are using TIIE DESK option or not. 
        When using TIIE DESK, the quotes are calculated as the BID OFFER 
        quotes mean. The default is False.
    bo_crv : array, optional
        Modified MXNTIIE quotes for bid offer calculations. See bid_offer
        function to see how these quotes are calculated. The default is [].

    Returns
    -------
    list
        List of QuantLib objects with MXNTIIE helpers.

    """
    tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                'Y': ql.Years}
    # calendar
    calendar_mx = ql.Mexico()
    # data
    def f(x):
        if x[-1]=='L':
            return int(x[1:-1])*4
        else:
            return int(x[1:-1])
    tenors = df['Tenor']
    period = tenors.map(lambda x: f(x)).tolist()
   
    tenor_type = tenors.str[-1].map(tenor2ql).tolist()
    
    
    # When using desk, calculate mid quotes
        
    if len(bo_crv) != 0:
        data = bo_crv/100
            
    else:
        data = (df['Quotes']/100).tolist()
    
    # Deposit rates
    deposit_index = df[df['Tenor']=='%1L'].index[0]
    deposits = {(period[deposit_index], 
                 tenor_type[deposit_index]): data[deposit_index]}
    # Swap rates
    swap_indexes = [i for i in df.index if i != deposit_index]
    swaps = {}
    for i in swap_indexes:
        swaps[(period[i], tenor_type[i])] = data[i]
        
    # Rate Qauntlib.Quote objects
    ## desposits
    for n, unit in deposits.keys():
        deposits[(n, unit)] = ql.SimpleQuote(deposits[(n, unit)])
    ## swap rates
    for n, unit in swaps.keys():
        swaps[(n, unit)] = ql.SimpleQuote(swaps[(n, unit)])
        
    # Deposit rate helpers
    dayCounter = ql.Actual360()
    settlementDays = 1
    depositHelpers = [ql.DepositRateHelper(
            ql.QuoteHandle(deposits[(n, unit)]), ql.Period(n, unit), 
            settlementDays, calendar_mx, ql.Following, False, dayCounter)
        for n, unit in deposits.keys()]

    # Swap rate helpers
    settlementDays = 1
    fixedLegFrequency = ql.EveryFourthWeek
    fixedLegAdjustment = ql.Following
    fixedLegDayCounter = ql.Actual360()
    ibor_MXNTIIE = ql.IborIndex('TIIE',
                                ql.Period(13), settlementDays, 
                                ql.MXNCurrency(), calendar_mx,
                                ql.Following, False, ql.Actual360(), 
                                crv_MXNOIS)

    swapHelpers = [ql.SwapRateHelper(ql.QuoteHandle(swaps[(n,unit)]),
                                     ql.Period(n, unit), calendar_mx,
                                     fixedLegFrequency, fixedLegAdjustment,
                                     fixedLegDayCounter, ibor_MXNTIIE)
                   for n, unit in swaps.keys()]

    # helpers merge
    helpers = depositHelpers + swapHelpers

    
    return(helpers)

# -------------------
# Curve Bootstrappers
# -------------------

# USDOIS

def btstrap_USDOIS(dic_data: dict, 
                   interpolation: str = 'Linear') -> ql.DiscountCurve:
    """Creates USDOIS curve.
    
    Uses a dictionary with USDOIS quotes and qlHelper_USDOIS 
    function to bootstrap USDOIS.
    

    Parameters
    ----------
    dic_data : dict
        Dictionary where USDOIS quotes and tenors are stored. The key
        to access USDOIS quotes is 'USD_OIS'.
    interpolation : str, optional
        Type of interpolation used. It can be 'Linear' or 'Cubic'. 
        The default is 'Linear'.

    Returns
    -------
    crvUSDOIS : ql.DiscountCurve
        Bootstrapped USDOIS curve.
        
    See Also
    --------
    qlHelper_USDOIS: Creates the helpers needed to bootstrap USDOIS.

    """
    # Helpers
    hlprUSDOIS = qlHelper_USDOIS(dic_data['USD_OIS'])
    
    # Interpolation method
    if interpolation == 'Linear':
        crvUSDOIS = ql.PiecewiseLogLinearDiscount(0, ql.UnitedStates(1), 
                                                  hlprUSDOIS, ql.Actual360())
        
    else: 
        crvUSDOIS = ql.PiecewiseNaturalLogCubicDiscount(0, ql.UnitedStates(1), 
                                                        hlprUSDOIS, 
                                                        ql.Actual360())
    
    crvUSDOIS.enableExtrapolation()
    
    return crvUSDOIS

# USDSOFR

def btstrap_USDSOFR(dic_data: dict, crvUSDOIS: ql.DiscountCurve, 
                    crvType: str ='Cubic') -> ql.NaturalLogCubicDiscountCurve:
    """Bootstraps USDSOFR curve.
    
    Uses a dictionary with SOFR quotes, a discount curve, and 
    ql.Helper_SOFR function to bootstrap USDSOFR.

    Parameters
    ----------
    dic_data : dict
        Dictionary with SOFR quotes and tenors. The key to access SOFR 
        quotes is 'USD_SOFR'.
    crvUSDOIS : ql.DiscountCurve
        Curve used to discount cashflows, usually USDOIS.
    crvType : str, optional
        Indicates type of interpolation. The default is 'Cubic'.

    Returns
    -------
    crvSOFR : ql.NaturalLogCubicDiscountCurve
        Bootstrapped USDSOFR curve.
    
    See Also
    --------
    qlHelper_SOFR: Creates the helpers needed to bootstrap USDSOFR.

    """
    # Discount curve
    crvDiscUSD = ql.RelinkableYieldTermStructureHandle()
    crvDiscUSD.linkTo(crvUSDOIS)
    
    # Helpers    
    hlprSOFR = qlHelper_SOFR(dic_data, crvDiscUSD)
    
    # Interpolation method
    if crvType=='Cubic':
        crvSOFR = ql.PiecewiseNaturalLogCubicDiscount(0, ql.UnitedStates(1), 
                                                      hlprSOFR, 
                                                      ql.Actual360())
        
    else: 
       crvSOFR = ql.PiecewiseLogLinearDiscount(0, ql.UnitedStates(1), 
                                               hlprSOFR, ql.Actual360())
    
    crvSOFR.enableExtrapolation()
    
    return crvSOFR

#LIBOR 3M

def btstrap_USD3M(dic_data: dict, crvUSDOIS: ql.DiscountCurve) -> \
    ql.NaturalLogCubicDiscountCurve:
    """Bootstraps LIBOR3M curve.
    
    Uses a dictionary with LIBOR3M quotes, a discount curve, and 
    qlHelper_USD3M function to bootstrap LIBOR3M.

    Parameters
    ----------
    dic_data : dict
        Dictionary with LIBOR3M quotes and tenors.
    crvUSDOIS : ql.DiscountCurve
        Curve used to discount cashflows, usually USDOIS.

    Returns
    -------
    crvUSD3M : ql.NaturalLogCubicDiscountCurve
        Bootstrapped LIBOR3M curve.
    
    See Also
    --------
    qlHelper_USD3M: Creates the helpers needed to bootstrap LIBOR3M.

    """
    crvDiscUSD = ql.RelinkableYieldTermStructureHandle()
    crvDiscUSD.linkTo(crvUSDOIS)
        
    hlprUSD3M = qlHelper_USD3M(dic_data, crvDiscUSD)
    crvUSD3M = ql.PiecewiseNaturalLogCubicDiscount(0, ql.UnitedStates(1), 
                                                   hlprUSD3M, 
                                                   ql.Actual360())
    crvUSD3M.enableExtrapolation()
    
    return crvUSD3M

#LIBOR1M

def btstrap_USD1M(dic_data: dict, crvUSD3M: ql.NaturalLogCubicDiscountCurve) \
    -> ql.NaturalLogCubicDiscountCurve:
    """Bootstraps LIBOR1M curve.
    

    Parameters
    ----------
    dic_data : dict
        Dictionary with LIBOR3M and LIBOR1M quotes and tenors.
    crvUSD3M : ql.NaturalLogCubicDiscountCurve
        LIBOR3M curve.

    Returns
    -------
    crvUSD1M : ql.NaturalLogCubicDiscountCurve
        Bootstrapped LIBOR1M curve.
    
    See Also
    --------
    qlHelper_USD1M: Creates the helpers needed to bootstrap LIBOR1M.

    """
    
    crv_usd3m = ql.RelinkableYieldTermStructureHandle()
    crv_usd3m.linkTo(crvUSD3M)
    hlprUSD1M = qlHelper_USD1M(dic_data, crv_usd3m)
    crvUSD1M = ql.PiecewiseNaturalLogCubicDiscount(0, ql.UnitedStates(1), 
                                                   hlprUSD1M, 
                                                   ql.Actual360())
    crvUSD1M.enableExtrapolation()
    
    return crvUSD1M

# MXNOIS

def btstrap_MXNOIS(dic_data: dict, crvUSDSWP: ql.NaturalLogCubicDiscountCurve, 
                   crvUSDOIS: ql.DiscountCurve, 
                   crvType: str = 'SOFR') -> \
    ql.PiecewiseNaturalLogCubicDiscount:
    """Bootstraps MXNOIS curve.
    
    Uses a dictionary with necessary quotes, a discount curve, 
    an IBOR curve, and qlHelper_MXNOIS function to bootstrap MXNOIS 
    curve.

    Parameters
    ----------
    dic_data : dict
        Dictionary with quotes for USDMXN_XCCY_Basis, USDMXN_Fwds, and 
        MXN_TIIE.
    crvUSDSWP : ql.NaturalLogCubicDiscountCurve
        Curve used to create IBOR Index.
    crvUSDOIS : ql.DiscountCurve
        Curve used to discount swap cashflows.
    crvType : str, optional
        Could be 'SOFR' or 'LIBOR'. The default is 'SOFR'.

    Returns
    -------
    crvMXNOIS : ql.PiecewiseNaturalLogCubicDiscount
        Bootstrapped MXNOIS curve.
        
    See Also
    --------
    qlHelper_MXNOIS: Creates the helpers needed to bootstrap MXNOIS.

    """
    # USDOIS curve
    crvDiscUSD = ql.RelinkableYieldTermStructureHandle()
    crvDiscUSD.linkTo(crvUSDOIS)
    
    # SOFR curve
    crv_usdswp = ql.RelinkableYieldTermStructureHandle()
    crv_usdswp.linkTo(crvUSDSWP)
    
    # Helpers
    try:
        hlprMXNOIS = qlHelper_MXNOIS(dic_data, crvDiscUSD, crv_usdswp, crvType)
        
        # Curve creation
        crvMXNOIS = ql.PiecewiseNaturalLogCubicDiscount(0, ql.Mexico(), 
                                                       hlprMXNOIS, 
                                                       ql.Actual360())
        crvMXNOIS.enableExtrapolation()
        
    except:
        start_date = ql.Mexico().advance(
            ql.Settings.instance().evaluationDate, 
            ql.Period(-1, ql.Days)).to_date()

        crvMXNOIS, crvTIIE = historical_curves(start_date)
       
    return crvMXNOIS

# MXNTIIE

def btstrap_MXNTIIE(dic_data: dict, 
                    crvMXNOIS: ql.PiecewiseNaturalLogCubicDiscount, 
                    updateAll = False, bo_crvs: np.array = np.array([])) -> \
    ql.PiecewiseNaturalLogCubicDiscount:
    """Bootstraps MXNTIIE curve.
    
    Uses a dictionary with MXNTIIE quotes, MXNOIS curve and 
    qlHelper_MXNTIIE function to bootstrap MXNTIIE curve.

    Parameters
    ----------
    dic_data : dict
        Dictionary with MXNTIIE quotes and tenors. The key to access 
        these quotes is 'MXN_TIIE'.
    crvMXNOIS : ql.PiecewiseNaturalLogCubicDiscount
        Mexican discount curve used to discount cashflows.
    updateAll : bool | str, optional
        Flag that indicates if we are using TIIE DESK option or not. 
        When using TIIE DESK, the quotes are calculated as the BID OFFER 
        quotes mean. The default is False.
    bo_crv : array, optional
        Modified MXNTIIE quotes for bid offer calculations. See bid_offer
        function to see how these quotes are calculated. The default is [].

    Returns
    -------
    crvTIIE : ql.PiecewiseNaturalLogCubicDiscount
        Bootstrapped MXNTIIE curve.
        
    See Also
    --------
    qlHelper_MXNTIIE: Creates the helpers needed to bootstrap MXNTIIE.

    """
    
    # Discount curve
    crv_mxnois = ql.RelinkableYieldTermStructureHandle()
    crv_mxnois.linkTo(crvMXNOIS)
    
    # Helpers
    hlprTIIE = qlHelper_MXNTIIE(dic_data['MXN_TIIE'], crv_mxnois, updateAll,
                                bo_crvs)
    
    # Curve creation
    crvTIIE = ql.PiecewiseNaturalLogCubicDiscount(0, ql.Mexico(), hlprTIIE, 
                                                  ql.Actual360())
    crvTIIE.enableExtrapolation()
    
    return crvTIIE

# -----------------------------
# Curves for Risk Sens by Tenor
# -----------------------------

def crvTenorRisk_TIIE(dic_df: dict, 
                      crvMXNOIS: ql.PiecewiseNaturalLogCubicDiscount, 
                      updateAll, g_crvs = None) -> dict:    
    """Creates curves to calculate DV01 by tenor.
    
    Uses a dictionary with MXNTIIE quotes and tenors, and MXNOIS curve 
    to create a dictionary of curves to calculate bucket risk.

    Parameters
    ----------
    dic_df : dict
        Dictionary with MXNTIIE quotes and tenors. The key to access 
        these quotes is 'MXN_TIIE'.
    crvMXNOIS : ql.PiecewiseNaturalLogCubicDiscount
        Mexican discount curve used to discount cashflows.
    updateAll : bool | str, optional
        Flag that indicates if we are using TIIE DESK option or not. 
        When using TIIE DESK, the quotes are calculated as the BID OFFER 
        quotes mean. The default is False.

    Returns
    -------
    dict
        Dictionary with tenors +-1 as keys and a list with shifted 
        MXNOIS and MXNTIIE curves as values. 
        Example: dict_crvs['%3L+1'] = [MXNOIS, MXNTIIE] with MXNOIS and
        MXNTIIE shifted 1bps in 3L tenor.

    """
    
    # Data
    modic = {k:v.copy() for k,v in dic_df.items() }
    
    # rates data
    df_tiie = dic_df['MXN_TIIE'][['Tenor', 'Quotes']].copy()
    
    # Curves by tenor mod
    dict_crvs = dict({})
    
    for i, r in df_tiie.iterrows():
        tmpdf = df_tiie.copy()
        # Rate mods
        tenor = r['Tenor']
        rate_plus_1bp = r['Quotes'] + 1/100
        rate_min_1bp = r['Quotes'] - 1/100
        # Tenor +1bp
        tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_plus_1bp
        modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
        # Proj Curve
        # if g_crvs:
        #     crvMXNOIS = btstrap_MXNOIS(modic, g_crvs[1], g_crvs[0])
        crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
        # Save
        dict_crvs[tenor+'+1'] = [crvMXNOIS, crvTIIE]
        # Tenor -1bp
        tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_min_1bp
        modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
        # Proj Curve
        # if g_crvs:
        #     crvMXNOIS = btstrap_MXNOIS(modic, g_crvs[1], g_crvs[0])
        crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
        # Save
        dict_crvs[tenor+'-1'] = [crvMXNOIS, crvTIIE]
    
    return(dict_crvs)

# -----------------
# Vanilla TIIE Swap
# -----------------

def tiieSwap(start: ql.Date, maturity: ql.Date, notional: float, 
             ibor_tiie: ql.IborIndex, rate: float, typ: int, 
             rule: int) -> tuple:
    """Creates swap object and IBOR fixing dates for IBOR Index.
    

    Parameters
    ----------
    start : ql.Date
        Start date of the swap.
    maturity : ql.Date
        End date of the swap.
    notional : float
        Notional amount.
    ibor_tiie : ql.IborIndex
        IBOR Index associated with MXNTIIE.
    rate : float
        Rate of swap.
    typ : int
        Indicates the side of the swap. It can be 
        ql.VanillaSwap.Receiver (-1) or ql.VanillaSwap.Payer (1).
    rule : int
        Rule used to generate the dates of the swap schedule. It can be
        ql.DateGeneration.Forward (1) or ql.DateGeneration.Backward (0).

    Returns
    -------
    tuple
        Tuple with ql.VanillaSwap object and list of fixing dates of 
        swap.

    """
    
    # TIIE Swap Schedule Specs
    cal = ql.Mexico()
    legDC = ql.Actual360()
    cpn_tenor = ql.Period(13)
    convention = ibor_tiie.businessDayConvention()
    termDateConvention = ibor_tiie.businessDayConvention()
    rule = rule
    isEndOfMonth = False
    
    # fix-float leg schedules
    fixfltSchdl = ql.Schedule(start, maturity, cpn_tenor, cal, convention,
                            termDateConvention, rule, isEndOfMonth)
    
    # swap
    swap = ql.VanillaSwap(typ, notional, fixfltSchdl, rate, legDC, fixfltSchdl,
                          ibor_tiie, 0, legDC)

    return swap, [ibor_tiie.fixingDate(x) for x in fixfltSchdl][:-1]

# -----------------
# Swap pricing legs
# -----------------

def get_CF_tiieSwap(swp: tuple) -> pd.DataFrame:    
    """Creates a DataFrame with swap cashflows.
    

    Parameters
    ----------
    swp : tuple
        First entry of the tuple should be ql.VanillaSwap object. The 
        second one is the list of fixing dates.

    Returns
    -------
    cf : pd.DataFrame
        DataFrame with cashflow calendar and amounts.

    """
    
    swp_type = swp[0].type()
    
    # Cashflows for fixed leg
    cf1_l1 = pd.DataFrame({'Date': pd.to_datetime(str(cf.date())),
                           'Start_Date': pd.to_datetime(
                               str(cf.accrualStartDate().ISO())),
                           'End_Date': pd.to_datetime(
                               str(cf.accrualEndDate().ISO())),
                           'Fix_Amt': cf.amount()*-1*swp_type} 
                          for cf in map(ql.as_coupon, swp[0].leg(0)))
    
    # Cashflow for float leg
    cf1_l2 = pd.DataFrame({'Date': pd.to_datetime(str(cf.date())),
                           'Fixing_Date': pd.to_datetime(str(fd)),
                           'Start_Date': pd.to_datetime(
                               str(cf.accrualStartDate().ISO())),
                           'End_Date': pd.to_datetime(
                               str(cf.accrualEndDate().ISO())),
                           'Float_Amt': cf.amount()*swp_type} 
                          for (cf, fd) in zip(map(
                                  ql.as_coupon, swp[0].leg(1)), swp[1]))
    
    # Final DataFrame
    cf = cf1_l1.copy()
    cf.insert(1, 'Fixing_Date', cf1_l2['Fixing_Date'])
    cf['Float_Amt'] = cf1_l2['Float_Amt']
    cf['Net_Amt'] = cf['Fix_Amt'] + cf['Float_Amt']
    cf['Fix_Amt'] = cf['Fix_Amt'].map('{:,.0f}'.format)
    cf['Float_Amt'] = cf['Float_Amt'].map('{:,.0f}'.format)
    cf['Net_Amt'] = cf['Net_Amt'].map('{:,.0f}'.format)
    cf['Acc_Days'] = (1*(cf['End_Date'] - cf['Start_Date']) \
                      / np.timedelta64(1, 'D'))
    cf['Acc_Days'] = [int(i) for i in cf['Acc_Days'].values]

    return cf

# ------------------------
# Download Banxico TIIE 1D
# ------------------------

def banxico_download_data(serie: str, banxico_start_date: str, 
                          banxico_end_date :str, token: str) -> pd.DataFrame:
    """Downloads TIIE28 quotes between specified dates.

    Parameters
    ----------
    serie : str
        Series number used to download TIIE28 quotes using Banxico's 
        API.
    banxico_start_date : str
        Start date in '%Y-%m-%d' format.
    banxico_end_date : str
        End date in '%Y-%m-%d' format.
    token : str
        Token to access Banxico's API.

    Returns
    -------
    pd.DataFrame
        DataFrame with dates and quotes for TIIE28.

    """
    # API
    url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/" \
        + serie + "/datos/" + banxico_start_date + "/" + banxico_end_date
    headers={'Bmx-Token':token}
    response = requests.get(url, headers=headers) 
    status = response.status_code 
    
    #Error en la obtención de los datos
    if status != 200:
        return print('Error Banxico TIIE 1D')
    
    raw_data = response.json()
    data = raw_data['bmx']['series'][0]['datos'] 
    df = pd.DataFrame(data) 

    df["dato"] = df["dato"].str.replace(',','')
    df["dato"] = df["dato"].str.replace('N/E','0')
    df['dato'] = df['dato'].apply(lambda x:float(x)) / 100
    df['fecha'] = pd.to_datetime(df['fecha'],format='%d/%m/%Y')

    return df

# -----
# Dates
# -----

# Dates Pricing

def start_end_dates_trading(parameters_trades: pd.Series, 
                            evaluation_date: datetime) -> tuple:
    """Calculates start and end dates of trade.
    

    Parameters
    ----------
    parameters_trades : pd.Series
        Series with details of trade. This includes Start_Tenor, 
        Fwd_Tenor, Start_Date and End_Date.
    evaluation_date : datetime
        Date of trade evaluation.

    Returns
    -------
    tuple
        Tuple with start date (ql.Date), maturity (ql.Date), and 
        flag_mat (bool) which is a flag to indicate if the maturity date
        is a holiday.

    """
    # Calendar
    mx_calendar = ql.Mexico()
    
    # Flags for holidays start or end dates
    date_flag_start = False
    flag_mat = False
    
    #Evaluation date
    todays_date = evaluation_date
    todays_date = ql.Date(todays_date.day, todays_date.month, todays_date.year)

    # Case only Fwd Tenor

    if (parameters_trades.Start_Tenor ==  0 and 
        parameters_trades.Fwd_Tenor !=  0 and 
        parameters_trades.Start_Date ==  0 and 
        parameters_trades.End_Date ==  0):
    
        start = mx_calendar.advance(todays_date, ql.Period(1, ql.Days))

        if mx_calendar.isHoliday(start):
            start = mx_calendar.advance(start, ql.Period(1, ql.Days)) 
            date_flag_start = True
        
        else: 
            date_flag_start = False
            
        maturity = start + ql.Period((int(parameters_trades.Fwd_Tenor) * 28), 
                                     ql.Days) 

    # Case only End Date
    
    elif (parameters_trades.Start_Tenor == 0 and 
          parameters_trades.Fwd_Tenor == 0 and 
          parameters_trades.Start_Date == 0 and 
          parameters_trades.End_Date != 0):

        start = todays_date + ql.Period(1, ql.Days)

        if mx_calendar.isHoliday(start):
            start = mx_calendar.advance(start, ql.Period(1, ql.Days)) 
            date_flag_start = True
        
        else: 
            date_flag_start = False
            
        maturity = ql.Date((pd.to_datetime(parameters_trades.End_Date)).day, 
                           (pd.to_datetime(parameters_trades.End_Date)).month, 
                           (pd.to_datetime(parameters_trades.End_Date)).year)
        
        if mx_calendar.isHoliday(maturity):
            flag_mat = True
 
            
    # Case only Start Date and End Date
    
    elif (parameters_trades.Start_Tenor == 0 and 
          parameters_trades.Fwd_Tenor == 0 and 
          parameters_trades.Start_Date != 0 and 
          parameters_trades.End_Date != 0):
    
        start = ql.Date((pd.to_datetime(parameters_trades.Start_Date)).day, 
                        (pd.to_datetime(parameters_trades.Start_Date)).month, 
                        (pd.to_datetime(parameters_trades.Start_Date)).year)

        if mx_calendar.isHoliday(start):
            start = mx_calendar.advance(start, ql.Period(1 , ql.Days)) 
            date_flag_start = True
            
        else: 
            date_flag_start = False
        
        maturity = ql.Date((pd.to_datetime(parameters_trades.End_Date)).day, 
                           (pd.to_datetime(parameters_trades.End_Date)).month, 
                           (pd.to_datetime(parameters_trades.End_Date)).year)
        
        if mx_calendar.isHoliday(maturity):
            flag_mat = True
    
    # Case Fwd Tenor and Start Date
                
    elif (parameters_trades.Start_Tenor == 0 and 
          parameters_trades.End_Date == 0 and 
          parameters_trades.Fwd_Tenor != 0 and 
          parameters_trades.Start_Date != 0):
        
        start = ql.Date((pd.to_datetime(parameters_trades.Start_Date)).day, 
                        (pd.to_datetime(parameters_trades.Start_Date)).month, 
                        (pd.to_datetime(parameters_trades.Start_Date)).year)

        if mx_calendar.isHoliday(start):
            start = mx_calendar.advance(start, ql.Period(1, ql.Days)) 
            date_flag_start = True
        
        else: 
            date_flag_start = False
            
        maturity = start + ql.Period((int(parameters_trades.Fwd_Tenor) * 28), 
                                     ql.Days)

 
            
    # Case Start and Fwd Tenor    
    
    elif (parameters_trades.Start_Date == 0 and 
          parameters_trades.End_Date == 0 and 
          parameters_trades.Start_Tenor != 0 and 
          parameters_trades.Fwd_Tenor != 0):
        
        start = todays_date + ql.Period(1, ql.Days) + \
            ql.Period((int(parameters_trades.Start_Tenor) * 28), ql.Days) 

        if mx_calendar.isHoliday(start):
            start = mx_calendar.advance(start, ql.Period(1, ql.Days)) 
            date_flag_start = True
        
        else: 
            date_flag_start = False
            
        maturity = start + ql.Period((int(parameters_trades.Fwd_Tenor) * 28), 
                                     ql.Days)

    #Case IMM
    
    elif (parameters_trades.Fwd_Tenor != 0 and 
          parameters_trades.Start_Tenor != 0 and 
          parameters_trades.Start_Date != 0):
        
        start = ql.IMM.nextDate(ql.Date(
            (pd.to_datetime(parameters_trades.Start_Date)).day, 
            (pd.to_datetime(parameters_trades.Start_Date)).month, 
            (pd.to_datetime(parameters_trades.Start_Date)).year)) 

        if mx_calendar.isHoliday(start):
            start = mx_calendar.advance(start, ql.Period(1, ql.Days)) 
            date_flag_start = True
        
        else: 
            date_flag_start = False
            
        maturity = start + ql.Period((int(parameters_trades.Fwd_Tenor) * 28), 
                                     ql.Days) 
        
    return start, maturity, flag_mat


#--------------------
#  Flat Dv01 Curves
#--------------------

def flat_dv01_curves (dic_data: dict, banxico_TIIE28: pd.DataFrame, 
                      crvUSDSOFR: ql.NaturalLogCubicDiscountCurve, 
                      crvUSDOIS: ql.DiscountCurve, updateAll, 
                      boot: str='SOFR') -> list:
    """Creates modified Ibor Index and engines with 
    
    Shifts quotes +- 1 bps to calculate modified Ibor Index and engines
    to calculate flat DV01.
    

    Parameters
    ----------
    dic_data : dict
        Dictionary with MXNTIIE quotes. The access key is 'MXN_TIIE'.
    banxico_TIIE28 : pd.DataFrame
        DataFrame with historical TIIE28 quotes.
    crvUSDSOFR : ql.NaturalLogCubicDiscountCurve
        Curve used to bootstrap MXNOIS (usually SOFR).
    crvUSDOIS : ql.DiscountCurve
        USD discount curve.
    boot : str, optional
        Indicates if we are using SOFR or LIBOR curve. The default is 
        'SOFR'.

    Returns
    -------
    list
    
        List with following elements:
            
        ibor_tiie_plus : ql.IborIndex 
            Ibor Index created with plus shifted MXNTIIE curve.
        tiie_swp_engine_plus : ql.DiscountingSwapEngine 
            Swap engine created with plus shifted MXNTIIE.
        ibor_tiie_minus : ql.IborIndex
            Ibor Index created with minus shifted MXNTIIE curve.
        tiie_swp_engine_minus : ql.DiscountingSwapEngine 
            Swap engine created with minus shifted MXNTIIE.

    """
    
    
    # dic data
    modic = {k: v.copy() for k,v in dic_data.items()}

    # rates data
    data_tiie = modic['MXN_TIIE']
    df_tiie = data_tiie[['Tenor','Quotes']]
    
    if updateAll == 'DESK':
        df_tiie['Quotes'] = (dic_data['MXN_TIIE']['FMX Desk BID']
                             + dic_data['MXN_TIIE']['FMX Desk OFFER']) / 2
    
    tmpdf = df_tiie.copy()

    n = len(df_tiie)
    shift_plus_list = np.array([0.01] * n)
       
    # tenor and rate mods
    tenor = df_tiie['Tenor']
    rate_plus_1bp = df_tiie['Quotes'].tolist() + shift_plus_list

    # data +1bp
    tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_plus_1bp
    modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
    ## disc crv
    crvMXNOIS = btstrap_MXNOIS(modic, crvUSDSOFR, crvUSDOIS, crvType=boot)
   
    crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
  
    # Pricing Swaps
    
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE)
    ibor_tiie_plus = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                  ql.Mexico(), ql.Following, False,
                                  ql.Actual360(), ibor_tiie_crv)
    
    ibor_tiie_plus.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        ibor_tiie_plus.addFixing(ql.Date(
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).day, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).month, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).year), 
            banxico_TIIE28['dato'][h+1])
    
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine_plus = ql.DiscountingSwapEngine(rytsMXNOIS)

    shift_minus_list = np.array([-0.01] * n)
    rate_min_1bp = df_tiie['Quotes'].tolist() + shift_minus_list
    
    # data -1bp
    tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_min_1bp
    modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
    ## disc crv
    crvMXNOIS = btstrap_MXNOIS(modic, crvUSDSOFR, crvUSDOIS, crvType=boot)
    crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
    
    # Pricing Swaps
    
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE)
    ibor_tiie_minus = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                   ql.Mexico(), ql.Following, False,
                                   ql.Actual360(), ibor_tiie_crv)
    
    ibor_tiie_minus.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1): 
        ibor_tiie_minus.addFixing(ql.Date(
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).day, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).month, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).year), 
            banxico_TIIE28['dato'][h+1])
    
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine_minus = ql.DiscountingSwapEngine(rytsMXNOIS)
    
    return [ibor_tiie_plus, tiie_swp_engine_plus, ibor_tiie_minus, 
            tiie_swp_engine_minus]


#--------------------
#  Bid Offer Curves
#--------------------

def bid_offer_crvs(dic_data: dict, banxico_TIIE28:pd.DataFrame, 
                   crvUSDSOFR: ql.NaturalLogCubicDiscountCurve, 
                   crvUSDOIS: ql.DiscountCurve, boot: str='SOFR') -> list:
    """Builds bid offer Ibor Index and engines.
    
    Uses MXNTIIE bid offer quotes to create bid offer curves and build 
    engines to calculate bid offer rates.
    

    Parameters
    ----------
    dic_data : dict
        Dictionary with MXNTIIE quotes and tenors. The access key is 
        'MXN_TIIE'.
    banxico_TIIE28 : pd.DataFrame
        DataFrame with historical TIIE28 quotes.
    crvUSDSOFR : ql.NaturalLogCubicDiscountCurve
        Curve used to bootstrap MXNOIS (usually SOFR).
    crvUSDOIS : ql.DiscountCurve
        USD discount curve.
    boot : str, optional
        Indicates if we are using SOFR or LIBOR curve. The default is 
        'SOFR'.

    Returns
    -------
    list
         
    List with following elements:
        
        ibor_tiie_bid : ql.IborIndex 
            Ibor Index created with bid quotes of MXNTIIE.
        tiie_swp_engine_bid : ql.DiscountingSwapEngine 
            Swap engine created with MXNTIIE bid quotes.
        ibor_tiie_offer : ql.IborIndex
            Ibor Index created with offer quotes of MXNTIIE.
        tiie_swp_engine_offer : ql.DiscountingSwapEngine 
            Swap engine created with MXNTIIE offer quotes.

    """
    # dic data
    modic = {k: v.copy() for k, v in dic_data.items()}

    # rates data
    data_tiie = modic['MXN_TIIE']

    # OFFER
     
    offer = data_tiie['FMX Desk OFFER']
    modic['MXN_TIIE']['Quotes'] = offer
    
    # MXNOIS and MXNTIIE Curves
    crvMXNOIS = btstrap_MXNOIS(modic, crvUSDSOFR, crvUSDOIS, crvType=boot)
    crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
  
    # Pricing Swaps
    
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE)
    ibor_tiie_offer = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                   ql.Mexico(), ql.Following, False,
                                   ql.Actual360(), ibor_tiie_crv)
    
    ibor_tiie_offer.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        ibor_tiie_offer.addFixing(ql.Date(
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).day, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).month, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).year), 
            banxico_TIIE28['dato'][h+1])
    
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine_offer = ql.DiscountingSwapEngine(rytsMXNOIS)
      
    # BID
    
    bid = data_tiie['FMX Desk BID']
    modic['MXN_TIIE']['Quotes'] = bid
    
    # MXNOIS and MXNTIIE curves
    crvMXNOIS = btstrap_MXNOIS(modic, crvUSDSOFR, crvUSDOIS, crvType=boot)
    crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
  
    # Pricing Swaps
    
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE)
    ibor_tiie_bid = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                 ql.Mexico(), ql.Following, False,
                                 ql.Actual360(), ibor_tiie_crv)
    
    ibor_tiie_bid.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        ibor_tiie_bid.addFixing(ql.Date(
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).day, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).month, 
                (pd.to_datetime(banxico_TIIE28['fecha'][h])).year), 
            banxico_TIIE28['dato'][h+1])
    
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine_bid = ql.DiscountingSwapEngine(rytsMXNOIS)
    
    return [ibor_tiie_bid, tiie_swp_engine_bid, ibor_tiie_offer, 
            tiie_swp_engine_offer]


#-------------------------
#  Flat Dv01 Calculation
#-------------------------

def flat_DV01_calc(ibor_tiie_plus: ql.IborIndex, 
                   tiie_swp_engine_plus: ql.DiscountingSwapEngine, 
                   ibor_tiie_minus: ql.IborIndex, 
                   tiie_swp_engine_minus: ql.DiscountingSwapEngine, 
                   start: ql.Date, maturity: ql.Date, notional: float, 
                   rate: float, typ: int, rule: int) -> float:
    """Calculates DV01 in MXN for specific trade.

    Parameters
    ----------
    ibor_tiie_plus : ql.IborIdex
        Ibor Index gotten from shifting MXNTIIE 1 bps.
    tiie_swp_engine_plus : ql.DiscountingSwapEngine
        Engine linked to MXNOIS gotten from shifting MXNTIIE 1 bps.
    ibor_tiie_minus : ql.IborIndex
        Ibor Index gotten from shifting MXNTIIE -1 bps.
    tiie_swp_engine_minus : ql.DiscountingSwapEngine
        Engine linked to MXNOIS gotten from shifting MXNTIIE -1 bps.
    start : ql.Date
        Start date of trade.
    maturity : ql.Date
        End date of trade.
    notional : float
        Notional of trade.
    rate : float
        Rate of trade.
    typ : int
        Indicates the side of the swap. It can be 
        ql.VanillaSwap.Receiver (-1) or ql.VanillaSwap.Payer (1).
    rule : int
        Rule used to generate the dates of the swap schedule. It can be
        ql.DateGeneration.Forward (1) or ql.DateGeneration.Backward (0).

    Returns
    -------
    float
        DV01 in MXN for trade.

    """
    # Plus 1bps swap
    swap_valuation = tiieSwap(start, maturity, abs(notional), ibor_tiie_plus, 
                              rate, typ, rule)
    swap_valuation[0].setPricingEngine(tiie_swp_engine_plus)
    
    # NPV plus
    npv_plus = swap_valuation[0].NPV()
    
    # Minus 1bps swap
    swap_valuation = tiieSwap(start, maturity, abs(notional), ibor_tiie_minus, 
                              rate, typ, rule)
    swap_valuation[0].setPricingEngine(tiie_swp_engine_minus)
    
    # NPV minus
    npv_minus = swap_valuation[0].NPV()  
    
    # DV01
    npv_dv01 = (npv_plus - npv_minus) / 2
     
    return npv_dv01


#------------------
#  KRR calculator
#------------------

def KRR_helper(i: int, values: pd.Series, brCrvs: dict, dic_data: dict, 
               npv_group: dict, start: ql.Date, maturity: ql.Date, 
               notional: float, rate: float) -> tuple:
    """Calculates Key Rate Risk by tenor for given trade.

    Parameters
    ----------
    i : int
        Trade index.
    values : pd.Series
        Details of trade (checks, start_tenor, fwd_tenor, etc. gotten 
        from Excel file).
    brCrvs : dict
        Dictionary with shifted MXNOIS and MXNTIIE curves +-1 bps by 
        tenor.
    dic_data : dict
        Dictionary with quotes and tenors for MXNTIIE.
    npv_group : dict
        Dictionary with trade indices as keys, and a list with NPV Group and 
        NPV as values.
    start : ql.Date
        Start date of trade.
    maturity : ql.Date
        End date of trade.
    notional : float
        Notional of trade.

    Returns
    -------
    tuple
    
        Tuple with following elements:
            
        krrc_f: bool
            Indicates if trade has check for Key_Rate_Risk_Check
        krrg_f: bool
            Indicates if trade has a KRR_Group
        krrl: int
            KRR_Group of trade. If it doesn't have a group it will be
            assigned to 0.
        df_tenorDV01: pd.DataFrame
            DataFrame with risks by tenor for given trade.
            
    See Also
    --------
    crvTenorRisk_TIIE: Creates brCrvs.

    """
    # Flags for KRR and group krr
    krrc_f = False
    krrg_f = False

    # KRR group
    krrl = 0 
    
    
    
    if notional >= 0:
        typ = ql.VanillaSwap.Receiver
    
    else:
        typ = ql.VanillaSwap.Payer
    
    if values.Date_Generation == 'Forward':
        rule = ql.DateGeneration.Forward
    
    else:
        rule = ql.DateGeneration.Backward
        
    
    modNPV = {}    
    
    # Tenor risk
    for tenor in brCrvs.keys():
        # new yieldcurves
        rytsDisc = ql.RelinkableYieldTermStructureHandle()
        rytsForc = ql.RelinkableYieldTermStructureHandle()
        discCrv, forcCrv = brCrvs[tenor]
        rytsDisc.linkTo(discCrv)
        rytsForc.linkTo(forcCrv)
        
        # disc-forc engines 
        discEngine = ql.DiscountingSwapEngine(rytsDisc)
        ibor_tiie = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                 ql.Mexico(), ql.Following, False,
                                 ql.Actual360(), rytsForc)
        swap_list = []
        swap_valuation = tiieSwap(start, maturity, abs(notional), ibor_tiie, 
                                  rate, typ, rule)     
        swap_valuation[0].setPricingEngine(discEngine)  
        swap_list.append(swap_valuation[0].NPV())
        
        modNPV[tenor] = swap_list
        
    df_modNPV = pd.DataFrame(modNPV, index = [i])

    brTenors = dic_data['MXN_TIIE']['Tenor'].tolist()
    df_tenorDV01 = pd.DataFrame(None, index = [i])
    
    # DataFrame with risk by tenor
    for tenor in brTenors:
        df_tenorp1 = df_modNPV[tenor+'+1']
        df_tenorm1 = df_modNPV[tenor+'-1']
        df_deltap1 = df_tenorp1 - npv_group[i][1]
        df_deltam1 = df_tenorm1 - npv_group[i][1]
        df_signs = np.sign(df_deltap1)
        df_tenorDV01[tenor] = df_signs * (abs(df_deltap1)+abs(df_deltam1)) / 2

    krr_group_value = values.KRR_Group
    
    # KRR check and group
    try:
        krr_group_value = int(krr_group_value)
        
    except:
        pass  

    if values.Key_Rate_Risk_Check != 0:
        krrc_f = True
        krrl = krr_group_value
        
    if krr_group_value != 0:
        krrg_f = True
        krrl = krr_group_value
        
    return (krrc_f, krrg_f, krrl, df_tenorDV01)
    
#----------------
#  Print Trades 
#----------------
    
def output_trade(i: int,  start: ql.Date, maturity: ql.Date, notional: float, 
                 rate: float, swap_valuation: list, flag_mat: bool = False, 
                 bo: bool = False, blotter: bool = False) -> pd.DataFrame:
    """Prints details for given trade.
    
    This function only returns a DataFrame with trade details when 
    blotter parameter is True, and it always prints the details of the 
    passed trade.

    Parameters
    ----------
    i : int
        Trade index.
    start : ql.Date
        Start date of trade.
    maturity : ql.Date
        End date of trade.
    notional : float
        Notinal of trade.
    rate : float
        Rate of trade.
    swap_valuation : list
        List with ql.VanillaSwap object and list of fixing dates of 
        trade.
    flag_mat : bool, optional
        Flag that indicates if maturity date is holiday. The default is 
        False.
    bo : bool, optional
        Flag that indicates if trade has Bid_Offer_Check. The default is 
        False. Apparently it is never used.
    blotter : bool, optional
        Flag that indicates if the function is being used for Blotter 
        sheet. The default is False.

    Returns
    -------
    confos_df_a : pd.DataFrame
        DataFrame with details of trade to input to Confos sheet in 
        Excel file. Only works when blotter parameter is true.

    """
    
    # Days until start date
    dts = start - (ql.Settings.instance().evaluationDate + 1)
    
    if dts < 0:
        tenor1 = 'Backdated /'
        dtm = maturity - (ql.Settings.instance().evaluationDate + 1)
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
            
    elif dts == 0:
        tenor1 = 'Spot /'
        dtm = maturity - (ql.Settings.instance().evaluationDate + 1)
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
        
    elif dts < 364:
        tenor1 = "{:,.2f}L /".format(dts/28)
        dtm = maturity - start
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
    
    else:
        tenor1 = "{:,.2f}Y /".format(dts/364)
        dtm = maturity - start
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
    
    # Print details
    if not blotter:
        print('\n')
        print('Output Trade ', i  , '\n')
        
        if flag_mat:
            real_maturity = ql.Mexico().advance(maturity,ql.Period(1,ql.Days))
            print('*******END DATE IS HOLIDAY********')
            print('Real End Date is ', real_maturity,'\n')
            
        print('Tenors:', tenor1, tenor2)
        print('Start Date: ', start)
        print('End Date: ', maturity)
        print('Notional: ', "MXN$ {:,.0f}".format(notional))
        print('Rate: ', "% {:,.4f}".format(rate * 100))
        print('Fair Rate: ', "% {:,.4f}".format(
                                            swap_valuation[0].fairRate()*100))
        print('NPV: ', "MXN$ {:,.0f}".format(swap_valuation[0].NPV()))
    
    # Create DataFrame with details
    else:
        confos_df_a = pd.DataFrame()
        confos_df_a['Trade'] = [f'Trade No. {i}']
        confos_df_a['Time'] = str(datetime.now().time())
        
        if notional > 0: 
            confos_df_a['Side'] = ['Side: PAY Fixed MXN IRS']
        
        else:
            confos_df_a['Side'] = ['Side: REC Fixed MXN IRS']
        
        end_date = ql.Mexico().advance(maturity, ql.Period(0,ql.Days))
        confos_df_a['Tenor'] = ['Tenors: '+ tenor1 + tenor2]
        confos_df_a['Start Date'] = [f'Start Date: {start}']
        confos_df_a['End Date'] = [f'End Date: {end_date}']
        confos_df_a['Notional']=['Notional: ' 
                                 + "MXN$ {:,.0f}".format(notional)]
        confos_df_a['Rate']=['Rate: '+ "% {:,.4f}".format(rate * 100)]
        
        return confos_df_a
        
#--------------------------
#  Print Bid Offer Trades
#--------------------------

def output_bo(flat_dv01: float, mxn_fx: float, bo: bool = False, 
              original_dv01: float = 0, npv_receiver: float = 0, 
              npv_payer: float = 0, fair_rate_bid: float = 0, 
              fair_rate_offer:float = 0) -> None:
    """Prints rest of trade details. 
    
    The extra details include bid offer rates in case the trade has a 
    bid offer check.

    Parameters
    ----------
    flat_dv01 : float
        Trade DV01.
    mxn_fx : float
        MXN/USD exchange rate.
    bo : bool, optional
        Flag to indicate if trade has Bid_Offer_Check. The default is 
        False.
    original_dv01 : float, optional
        Original DV01 of trade. If trade is not backdated it should be 
        very similar to flat_dv01. The default is 0.
    npv_receiver : float, optional
        Swap NPV from the receiver side. The default is 0.
    npv_payer : float, optional
        Swap NPV from the payer side. The default is 0.
    fair_rate_bid : float, optional
        Bid fair rate. The default is 0.
    fair_rate_offer : float, optional
        Offer fair rate. The default is 0.

    Returns
    -------
    None

    """
    # Outputs for Bid Offer Check
    if bo:
        
        if npv_receiver < 0:
            finamex_r = 'receives'
                   
        else:
            finamex_r = 'pays'
           
        if npv_payer < 0:
            finamex_p = 'receives'
           
        else:
            finamex_p = 'pays'
           
        print('BID OFFER Rate: ', "BID % {:,.4f} |".format(fair_rate_bid*100), 
              " OFFER % {:,.4f}".format(fair_rate_offer*100))
        print('FMX '+ finamex_p + ' MXN$ {:,.0f} |'.format(abs(npv_payer)),
              "FMX " + finamex_r + " MXN$ {:,.0f}".format(abs(npv_receiver)))
        print('Flat DV01 :' , "USD$ {:,.0f}".format(flat_dv01 / mxn_fx))
        
        if original_dv01!=0:
            print('Original DV01:', "USD$ {:,.0f}".format(original_dv01)) 
    
    # DV01
    else:
        print('Flat DV01 :' , "USD$ {:,.0f}".format(flat_dv01 / mxn_fx))
        
        if original_dv01!=0:
            print('Original DV01:', "USD$ {:,.0f}".format(original_dv01)) 
    
        
#--------------------------
#  Bid Offer Calculations  
#--------------------------  
    
def bid_offer(start: ql.Date, maturity: ql.Date, fair_rate: float, 
              dic_data: dict, crvMXNOIS: ql.PiecewiseNaturalLogCubicDiscount, 
              banxico_TIIE28: pd.DataFrame, df_tenorDV01: pd.DataFrame, 
              notional: float, rate: float, rule: int, typ: int) -> tuple:
    """Calculates NPV for swap from payer side and reciever side.

    Parameters
    ----------
    start : ql.Date
        Start date of trade.
    maturity : ql.Date
        End date of trade.
    fair_rate : float
        Fair rate of trade.
    dic_data : dict
        Dictionary with quotes and tenors for MXNTIIE.
    crvMXNOIS : ql.PiecewiseNaturalLogCubicDiscount
        Curve used to discount swap cashflows.
    banxico_TIIE28 : pd.DataFrame
        DataFrame with historical TIIE28 quotes..
    df_tenorDV01 : pd.DataFrame
        DataFrame with DV01 by tenor.
    notional : float
        Notional of trade.
    rate : float
        Rate of trade.
    rule : int
        Rule used to generate the dates of the swap schedule. It can be
        ql.DateGeneration.Forward (1) or ql.DateGeneration.Backward (0).
    typ : int
        Indicates the side of the swap. It can be 
        ql.VanillaSwap.Receiver (-1) or ql.VanillaSwap.Payer (1).

    Returns
    -------
    tuple
        Tuple with NPV from receiver side, and NPV from payer side.

    """
    # Days until start date
    dts = start - (ql.Settings.instance().evaluationDate + 1)
    
    if dts < 0:
        tenor1 = 'Backdated /'
        short_tenor = 0
        dtm = maturity - (ql.Settings.instance().evaluationDate + 1)
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
            
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
            
    elif dts == 0:
        tenor1 = 'Spot /'
        short_tenor = 0
        dtm = maturity - (ql.Settings.instance().evaluationDate + 1)
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
        
    elif dts < 364:
        tenor1 = "{:,.2f}L /".format(dts/28)
        dtm = maturity - start
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
        short_tenor = dts/28
    
    else:
        tenor1 = "{:,.2f}Y /".format(dts/364)
        dtm = maturity - start
        
        if dtm < 364:
            tenor2 = "{:,.2f}L".format(dtm/28)
        
        else:
            tenor2 = "{:,.2f}Y".format(dtm/364)
        
        short_tenor = dts/28
    
    long_tenor = dtm/28 + short_tenor
    
    # Mid quotes
    tiie =  dic_data['MXN_TIIE'].copy()
    tiie['MID'] = tiie['Quotes']
    intervals = (tiie['Period'].tolist())[1:]
    
    # Tenors with the most risk
    try:
        short_index = [intervals[k]<=short_tenor<intervals[k+1] 
                       for k in range(len(intervals))].index(True)
    
    except:        
        short_index = 0
        
    try:   
        long_index = [intervals[k]<long_tenor<=intervals[k+1] 
                      for k in range(len(intervals))].index(True) + 1
        
    except:        
        long_index = len(intervals)-1
        
    tenors_cambios = intervals[short_index:long_index + 1]
    tiie_cambios = tiie.copy() 
    
    # Change mid quotes for bid/offer quotes in riskiest tenors
    conditions = [(df_tenorDV01.values>0)[0] * \
                      (tiie_cambios['Period'].isin(tenors_cambios)), 
                  (df_tenorDV01.values<0)[0] * \
                      (tiie_cambios['Period'].isin(tenors_cambios))]
    options1 = [tiie_cambios['FMX Desk BID'], tiie_cambios['FMX Desk OFFER']]
    options2 = [tiie_cambios['FMX Desk OFFER'], tiie_cambios['FMX Desk BID']]
    
    if typ==-1: 
        tiie_cambios['Nueva TIIE Receiver'] = np.select(conditions, options1, 
                                                        tiie_cambios['MID'])
        bo_crvs_rec = tiie_cambios['Nueva TIIE Receiver'].values 
        tiie_cambios['Nueva TIIE Payer'] = np.select(conditions, options2, 
                                                     tiie_cambios['MID'])
        bo_crvs_pay = tiie_cambios['Nueva TIIE Payer'].values
        
    else: 
        tiie_cambios['Nueva TIIE Receiver'] = np.select(conditions, options2, 
                                                        tiie_cambios['MID'])
        bo_crvs_rec = tiie_cambios['Nueva TIIE Receiver'].values    
        tiie_cambios['Nueva TIIE Payer'] = np.select(conditions, options1, 
                                                     tiie_cambios['MID'])    
        bo_crvs_pay = tiie_cambios['Nueva TIIE Payer'].values
        
    # MXNTIIE for receiver side
    crvTIIE_receiver = btstrap_MXNTIIE(dic_data, crvMXNOIS, 'DESK', 
                                       bo_crvs_rec)
    
    # MXNTIIE for payer side
    crvTIIE_payer = btstrap_MXNTIIE(dic_data, crvMXNOIS, 'DESK', bo_crvs_pay)
    
    # Ibor Index for receiver
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE_receiver)
    ibor_tiie_receiver = ql.IborIndex('TIIE', ql.Period(13), 1, 
                                      ql.MXNCurrency(), ql.Mexico(),
                                      ql.Following, False, ql.Actual360(),
                                      ibor_tiie_crv)
    
    # Ibor Index for payer
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE_payer)
    ibor_tiie_payer = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                   ql.Mexico(), ql.Following, False,
                                   ql.Actual360(), ibor_tiie_crv)
    
    # MXNOIS discounting curve
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine_bo = ql.DiscountingSwapEngine(rytsMXNOIS) 
    
    # Swap from receiver side
    swp_receiver = tiieSwap(start, maturity, abs(notional), ibor_tiie_receiver, 
                            rate, -1, rule)
    swp_receiver[0].setPricingEngine(tiie_swp_engine_bo)
    npv_receiver = swp_receiver[0].NPV()
    
    # Swap from payer side
    swp_payer = tiieSwap(start, maturity, abs(notional), ibor_tiie_payer, rate, 
                         1, rule)
    swp_payer[0].setPricingEngine(tiie_swp_engine_bo)
    npv_payer = swp_payer[0].NPV()
     
    return npv_receiver, npv_payer

def KRR_helper_1L(i: int, values: pd.Series, brCrvs: dict, dic_data: dict, 
                npv_group: dict, start: ql.Date, maturity: ql.Date, 
                notional: float, rate: float) -> tuple:

    typ = values.SwpType
    rule = values.mtyOnHoliday
    modNPV = {}  
    
    tenors = ['%1L+1', '%1L-1']
    
    # Tenor risk
    for tenor in tenors:
    # new yieldcurves
        rytsDisc = ql.RelinkableYieldTermStructureHandle()
        rytsForc = ql.RelinkableYieldTermStructureHandle()
        discCrv, forcCrv = brCrvs[tenor]
        rytsDisc.linkTo(discCrv)
        rytsForc.linkTo(forcCrv)
        
        # disc-forc engines 
        discEngine = ql.DiscountingSwapEngine(rytsDisc)
        ibor_tiie = ql.IborIndex('TIIE', ql.Period(13), 1, ql.MXNCurrency(),
                                  ql.Mexico(), ql.Following, False,
                                  ql.Actual360(), rytsForc)
        swap_list = []
        swap_valuation = tiieSwap(start, maturity, abs(notional), ibor_tiie, 
                                  rate, typ, rule)     
        swap_valuation[0].setPricingEngine(discEngine)  
        swap_list.append(swap_valuation[0].NPV())
        
        modNPV[tenor] = swap_list
        
    df_modNPV = pd.DataFrame(modNPV, index = [i])
    brTenors = dic_data['MXN_TIIE']['Tenor'].tolist()
    df_tenorDV01 = pd.DataFrame(None, index = [i])
    
    tenor = '%1L'
    # DataFrame with risk by tenor
    df_tenorp1 = df_modNPV[tenor+'+1']
    df_tenorm1 = df_modNPV[tenor+'-1']
    df_deltap1 = df_tenorp1 - npv_group[i][1]
    df_deltam1 = df_tenorm1 - npv_group[i][1]
    df_signs = np.sign(df_deltap1)
    df_tenorDV01[tenor] = df_signs * (abs(df_deltap1)+abs(df_deltam1)) / 2

    return df_tenorDV01

def crvTenorRisk_TIIE_1L(dic_data, crvDiscUSD, crv_usdswp, crvMXNOIS):
    # Data
    modic = {k:v.copy() for k,v in dic_data.items()}
    # rates data
    df_tiie = modic['MXN_TIIE'].copy()
    
    # df_tiie.loc[10.5]=['MPSW156M Curncy', '%156L', 156, 'SWAP', 
    #                     df_tiie.loc[10:11,'Quotes'].mean(), np.nan]
    
    # df_tiie = df_tiie.sort_index().reset_index(drop=True)
    # Curves by tenor mod
    dict_crvs = dict({})
    (i, r) = [(j, k) for j, k in df_tiie.iterrows()][0]
    
    #for i,r in df_tiie.iterrows():
    tmpdf = df_tiie.copy()
    # Rate mods
    tenor = r['Tenor']
    rate_plus_1bp = r['Quotes'] + 1/100
    rate_min_1bp = r['Quotes'] - 1/100
    # Tenor +1bp
    tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_plus_1bp
    modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
    # Disc Curve
    crvMXNOIS = btstrap_MXNOIS(modic, crv_usdswp, 
                                crvDiscUSD, crvType='SOFR')
    # Proj Curve
    crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
    # Save
    dict_crvs[tenor+'+1'] = [crvMXNOIS, crvTIIE]
    # Tenor -1bp
    tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_min_1bp
    modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
    # Disc Curve
    crvMXNOIS = btstrap_MXNOIS(modic, crv_usdswp, 
                                crvDiscUSD, crvType='SOFR')
    # Proj Curve
    crvTIIE = btstrap_MXNTIIE(modic, crvMXNOIS)
    # Save
    dict_crvs[tenor+'-1'] = [crvMXNOIS, crvTIIE]
    
    return dict_crvs

def banxicoData(evaluation_date: datetime) -> pd.DataFrame:
    '''Downloads the TIIE28 quotes from the last year
    
    Uses banxico's api to download the quotes for the TIIE28 curve

    Parameters
    ----------
    evaluation_date : datetime

    Returns
    -------
    banxico_TIIE28 : DataFrame
        DataFrame with dates and quotes of the year to date TIIE28 curve

    '''
    # Parameters Definition
    token="c1b63f15802a3378307cc2eb90a09ae8e821c5d1ef04d9177a67484ee6f9397c" 
    banxico_start_date = (evaluation_date 
                          - timedelta(days = 3600)).strftime('%Y-%m-%d')
    banxico_end_date = evaluation_date.strftime('%Y-%m-%d')
    
    # Function evaluation
    try:
        
        banxico_TIIE28 = banxico_download_data('SF43783', banxico_start_date, 
                                                  banxico_end_date, token)
        banxico_TIIE28.to_excel('//TLALOC/tiie/HistoricalTIIE.xlsx', 
                                index=False)
        
            
    except:
        banxico_TIIE28 = pd.read_excel('//TLALOC/tiie/HistoricalTIIE.xlsx')
        if evaluation_date not in banxico_TIIE28['fecha']:
            tiie_rates = pd.read_excel('TIIE_IRS_Data.xlsm', 'MXN_TIIE')
            last_rate = tiie_rates.iloc[0]['Quotes']/100
            banxico_TIIE28 = pd.concat(
                [banxico_TIIE28,  
                  pd.DataFrame({'fecha': [evaluation_date], 
                                'dato': [last_rate]})], ignore_index=True)
            banxico_TIIE28.to_excel('//TLALOC/tiie/HistoricalTIIE.xlsx', 
                                    index=False)
        banxico_TIIE28 = \
            banxico_TIIE28[banxico_TIIE28['fecha']<=evaluation_date]
    
        
    return banxico_TIIE28

def set_ibor_TIIE(crvTIIE):
    
    file = 'TIIE_IRS_Data.xlsm'

    future_rates = pd.read_excel(file, 
                                  sheet_name = 'Short_End_Pricing', skiprows = 1, usecols = ['MPC Meetings', 'Fix Eff', 'Rate'])
    
    future_rates = future_rates.iloc[1:26]
    future_rates['Rate'] = future_rates['Rate'].astype(float)/100
    
    # Eval Date
    ql_eval_date = ql.Settings.instance().evaluationDate
    dt_eval_date = datetime(ql_eval_date.year(),
                        ql_eval_date.month(),
                        ql_eval_date.dayOfMonth())
    # Fixings
    banxico_TIIE28 = banxicoData(dt_eval_date)
    
    final_date = dt_eval_date
    
    
    banxico_TIIE28_a = pd.DataFrame({'fecha': pd.date_range(banxico_TIIE28.iloc[-1]['fecha'] + timedelta(days=1),future_rates.iloc[0]['MPC Meetings'],freq='d'), 'dato' : banxico_TIIE28.iloc[-1]['dato']})
    banxico_TIIE28 = pd.concat([banxico_TIIE28, banxico_TIIE28_a], ignore_index = True)
    
    
    for k in range(future_rates.shape[0]-1):
        banxico_TIIE28_a = pd.DataFrame({'fecha': pd.date_range(future_rates.iloc[k]['Fix Eff'],future_rates.iloc[k+1]['MPC Meetings'],freq='d'), 'dato': future_rates.iloc[k]['Rate']})
        banxico_TIIE28 = pd.concat([banxico_TIIE28, banxico_TIIE28_a], ignore_index = True)
    
    banxico_TIIE28['fecha'] = pd.to_datetime(banxico_TIIE28['fecha'])
    banxico_business_dates = [banxico_TIIE28.iloc[k]['fecha'] for k in range(banxico_TIIE28.shape[0]) if ql.Mexico().isBusinessDay(ql.Date().from_date(banxico_TIIE28.iloc[k]['fecha']))]

    banxico_TIIE28 = banxico_TIIE28[(banxico_TIIE28['fecha']<= final_date) & banxico_TIIE28['fecha'].isin(banxico_business_dates)]
    
    
    # TIIE IBOR INDEX
    if type(crvTIIE) != type(ql.RelinkableYieldTermStructureHandle()):
        ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
        ibor_tiie_crv.linkTo(crvTIIE)
    else:
        ibor_tiie_crv = crvTIIE
    ibor_tiie = ql.IborIndex('TIIE',
                  ql.Period(13),
                  1,
                  ql.MXNCurrency(),
                  ql.Mexico(),
                  ql.Following,
                  False,
                  ql.Actual360(),
                  ibor_tiie_crv)
    ###########################################################################
    # Ibor Index Fixings
    ibor_tiie.clearFixings()
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        dt_fixing = pd.to_datetime(banxico_TIIE28.iloc[h]['fecha'])
        ibor_tiie.addFixing(
            ql.Date(dt_fixing.day, dt_fixing.month, dt_fixing.year), 
            banxico_TIIE28.iloc[h+1]['dato']
            )
    

    return(ibor_tiie)

def get_risk_byBucket(df_book, brCrvs, crvMXNOIS, ibor_tiie, fxrate):
    # discounting engine
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine = ql.DiscountingSwapEngine(rytsMXNOIS)
    # swap obj df 1814
    dfbookval = pd.DataFrame(None, 
                              columns=df_book.columns.tolist()+['SwpObj',
                                                              'NPV',
                                                              'evalDate'])
    # CONTROL VAR
    #print(ql.Settings.instance().evaluationDate)
    # Book's Base NPV
    for i,row in df_book.iterrows():
        bookid, tradeid, tradedate, notnl,\
            stdt, mty, cpdt, ctpty, itr, r, swptyp, schdlrule = row
        sdt = ql.Date(stdt.day,stdt.month,stdt.year)
        edt = ql.Date(mty.day,mty.month,mty.year)
        swp = tiieSwap(sdt, edt, notnl, ibor_tiie, r/100, swptyp, schdlrule)
        swp[0].setPricingEngine(tiie_swp_engine)
        npv = swp[0].NPV()
        tmpdict = {'BookID': bookid, 
                    'TradeID': tradeid, 
                    'TradeDate': tradedate, 
                    'Notional': notnl, 
                    'StartDate': stdt, 
                    'Maturity': mty, 
                    'CpDate': cpdt, 
                    'FxdRate': r, 
                    'SwpType': swptyp,
                    'mtyOnHoliday': schdlrule,
                    'SwpObj': swp[0],
                    'NPV': npv,
                    'evalDate': ql.Settings.instance().evaluationDate}
        new_row = pd.DataFrame(tmpdict, index=[0])
        dfbookval = pd.concat([dfbookval.loc[:], new_row])
    dfbookval = dfbookval.reset_index(drop=True) 
    # Book's Bucket Sens NPV
    modNPV = {}    
    for tenor in brCrvs.keys():
        # new yieldcurves
        rytsDisc = ql.RelinkableYieldTermStructureHandle()
        rytsForc = ql.RelinkableYieldTermStructureHandle()
        discCrv, forcCrv = brCrvs[tenor]
        rytsDisc.linkTo(discCrv)
        rytsForc.linkTo(forcCrv)
        # disc-forc engines
        discEngine = ql.DiscountingSwapEngine(rytsDisc)
        ibor_tiie_br = set_ibor_TIIE(rytsForc)
        
        # swaps
        lst_npvs = []
        for i,row in dfbookval.iterrows():
            bookid, tradeid, tradedate, notnl, \
                stdt, mty, cpdt, ctpty, itr, r, \
                swptyp, rule, swpobj, onpv, evDate = row
            sdt = ql.Date(stdt.day,stdt.month,stdt.year)
            edt = ql.Date(mty.day,mty.month,mty.year)
            swp = tiieSwap(sdt, edt, notnl, ibor_tiie_br, r/100, swptyp, rule)
            swp[0].setPricingEngine(discEngine)
            lst_npvs.append(swp[0].NPV())
        modNPV[tenor] = lst_npvs
        modNPV['BookID'] = dfbookval['BookID']

    df_modNPV = pd.DataFrame(
        modNPV,
        index = dfbookval.index,
        )
    
    # Bucket DV01
    brTenors = [x[:-2] for x in brCrvs.keys()]
    
    df_tenorDV01 = pd.DataFrame(None, index = dfbookval.index)
    for tenor in brTenors:
        df_tenorp1 = df_modNPV[tenor+'+1']
        df_tenorm1 = df_modNPV[tenor+'-1']
        df_deltap1 = df_tenorp1 - dfbookval['NPV']
        df_deltam1 = df_tenorm1 - dfbookval['NPV']
        df_signs = np.sign(df_deltap1)
        df_tenorDV01[tenor] = df_signs*(abs(df_deltap1)+abs(df_deltam1))/2
        
    # Book Bucket Risks
    dfbr = pd.Series((df_tenorDV01.sum()/fxrate).sum(), 
                                index = ['OutrightRisk'])
    dfbr = dfbr.append((df_tenorDV01.sum()/fxrate))
    dfbr = dfbr.map('{:,.0f}'.format)
    dic_bookRisks = {
        'NPV_Book': dfbookval.NPV.sum()/fxrate,
        'NPV_Swaps': dfbookval.NPV/fxrate,
        'DV01_Book': dfbr,
        'DV01_Swaps': df_tenorDV01/fxrate
        }
    return(dic_bookRisks)

def blotter_to_posswaps(wb, df_book, banxico_TIIE28, g_engines, dv01_engines):
    
    parameters = wb.sheets('Pricing')
    mxn_fx = parameters.range('F1').value
    evaluation_date = pd.to_datetime(parameters.range('B1').value)
    
    
    ibor_tiie = g_engines[0]
    ibor_tiie.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        dt_fixing = pd.to_datetime(banxico_TIIE28.iloc[h]['fecha'])
        ibor_tiie.addFixing(
            ql.Date(dt_fixing.day, dt_fixing.month, dt_fixing.year), 
            banxico_TIIE28.iloc[h+1]['dato']
            )
        
    tiie_swp_engine = g_engines[1]
    ibor_tiie_plus = dv01_engines[0]
    tiie_swp_engine_plus = dv01_engines[1]
    ibor_tiie_minus = dv01_engines[2]
    tiie_swp_engine_minus = dv01_engines[3]
    
    risk_sheet = wb.sheets('Risk')
    book = risk_sheet.range('B2:B2').value
    blotter = wb.sheets('Blotter')
    if blotter.range('H2').value is None:
        parameters_trades = pd.DataFrame()
        
    else:
        range_trades = blotter.range('A1').end('right').address[:-1] + \
            str(blotter.range('H1').end('down').row)
        parameters_trades = blotter.range('A1',range_trades).options(
            pd.DataFrame, header=1).value
        parameters_trades = parameters_trades[parameters_trades['Book'] == book]
        parameters_trades = parameters_trades.fillna(0)
        
    blotter_book = pd.DataFrame(columns = df_book.columns)
    for i, values in parameters_trades.iterrows():
        
        start, maturity, flag_mat = \
            start_end_dates_trading(values, evaluation_date)
        
        if values.NPV_MXN == 0 and values.DV01_USD == 0:
            
            notional = values.Notional_MXN
            rate = values.Rate
        
            if notional >= 0:
                typ = ql.VanillaSwap.Receiver
                
            else:
                typ = ql.VanillaSwap.Payer
            
            #Date generation rule
            if values.Date_Generation == 'Forward':
                rule = ql.DateGeneration.Forward
                
            else:
                rule = ql.DateGeneration.Backward
        
        elif values.Notional_MXN == 0:
            notional = 100000000
            rate = values.Rate
            
            rule = ql.DateGeneration.Backward
            typ = ql.VanillaSwap.Receiver
           
            # Swaps construction
            swap_valuation = tiieSwap(start, maturity, abs(notional), 
                                          ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # Case rate == 0 
            if rate == 0:
                rate = swap_valuation[0].fairRate()
    
            # Dummy DV01
            flat_dv01 = flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)            
            dv01_100mn = flat_dv01/mxn_fx
            npv_100mn = swap_valuation[0].NPV()
            
            # New Swap Data
            dv01_value = values.DV01_USD
            dv01_value = float(dv01_value)
            
            npv_value = values.NPV_MXN
            npv_value = float(npv_value)
            
            if npv_value == 0:
                notional = (dv01_value * 100000000) / dv01_100mn
                
    
            elif dv01_value == 0:
                notional = (npv_value * 100000000) / npv_100mn
            
            # Swap side definition
            if notional >= 0:
                typ = ql.VanillaSwap.Receiver
                
            else:
                typ = ql.VanillaSwap.Payer
                
            # Date generation rule definition
            if values.Date_Generation == 'Forward':
                rule = ql.DateGeneration.Forward
                
            else:
                rule = ql.DateGeneration.Backward
                
        blotter_book.loc[i] = [book, i, evaluation_date, abs(notional), start.to_date(),
                                maturity.to_date(), 0, 0, 0, rate*100, typ, rule]
    
    return blotter_book




    
    