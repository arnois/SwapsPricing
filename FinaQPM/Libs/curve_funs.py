# last update: 2023/07/24
"""curve functions

This script contains the classes and functions needed to create curves and 
swaps.

This script requires `os`, `pickle`, `requests`, `warnings`, `numpy`
`pandas`, `QuantLib`, and `datetime` be installed within the Python environment 
this script is being run.

This file can also be imported as a module ("cf" is conventional) and 
contains the following classes:
    
    * curve - main curve class
    * USD_OIS - USD OIS curve (cuve child)
    * USD_SOFR - USD SOFR curve (cuve child)
    * MXN_OIS - MXN OIS curve (cuve child)
    * MXN_TIIE - MXN TIIE curve (cuve child)
    * _mxn_curves - class for creating curves necessary for mexican swaps
                    (USD_OIS, USD_SOFR, MXN_OIS, MXN_TIIE)
    * mxn_curves - class for creating curves necessary for mexican swaps, 
                   (_mxn_curves child)
    * tiieSwap - class for making tiie swaps

Also contains the following functions:
    
    * save_obj - saves a pickle of the object wanted
    * load_obj - loads an obje form the pickle wanted
    * import_data - imports the necessary data for creaing mxn_curves
    * pull_data - gets data from the wanted files
    * futures_check - check if the futures form the data imported are updated
    * add_tiie_tenors - adds tenors to he current MXN_TIIE dataframe
    * granular - adds all granular tiie tenors to the current 
                 MXN_TIIE dataframe
    
 
Created by Quant Team

Contact:
    Esteban López Araiza Bravo: elopeza@finamex.com.mx
    Gabriela Abreu Olvera: gabreu@finamex.com.mx

"""

import os
import pickle
import requests
import warnings
import numpy as np
import pandas as pd
import QuantLib as ql
import pdfplumber
from datetime import datetime, timedelta



warnings.filterwarnings("ignore")

#-----------------
#  Curve classes
#-----------------
        
class curve:
    """ QPT Curve class
    
    Parent class of curves
    
    ...
    
    Attributes
    ----------
    
    interpolation: str
        Type of interpolation wanted for the curve
    df: pd.DataFrame, optional
        DataFrame with the inputs needed for the curve
    nodes: tuple, optional
        Tuple with the nodes required for the curve
    
    Methods
    -------
    from_nodes(nodes)
        Interpolates curve by nodes
    
    
    
    """
    def __init__(self, df: pd.DataFrame, interpolation: str = 'Cubic' ):
        """ Attributes definition
        

        Parameters
        ----------
        df : pd.DataFrame, tuple
            DataFrame or nodes required for interpolating the curve 
        interpolation : str, default = 'Cubic'
            Type of interpolation wanted for the curve ('Linear or default')

        Returns
        -------
        None.

        """
        self.interpolation = interpolation
        self.tenor2ql = {'B': ql.Days, 'D': ql.Days, 'M': ql.Months,
                         'W': ql.Weeks, 'Y': ql.Years}
        if type(df) == tuple:
            self.nodes = df
            self.nodes_flag = True
        else:
            self.df = df
            self.nodes_flag = False
        
        
    
    @classmethod
    
    def from_nodes(cls, nodes):
        interpolation = 'Linear'
        return cls(nodes, interpolation)

        
    

    

class USD_OIS(curve):
    """ QPT USD_OIS Curve class
    
    USD OIS Cuve class, curve class child
    
    ...
    
    Attributes
    ----------
    
    interpolation: str
        Type of interpolation wanted for the curve
    df: pd.DataFrame
        DataFrame with the inputs needed for the curve
    nodes: tuple
        Tuple with the nodes required for the curve
    name: str
        Name of the curve
    calendar: ql.Calendar
        Calendar of the curve
    helpers: list, not available using from_nodes method
        list of hlpers needed to bootstrap he curve
    tenors: list, not available using from_nodes method
        List of tenors for the curve
    quotes: list, not available using from_nodes method
        List of quotes for each tenor 
    curve: ql.Discountcurve
        QuantLib Discount Cure object
    
    
    Methods
    -------
    from_nodes(nodes)
        Interpolates curve by nodes
    hlprs(df)
        creates helpers for curve bootstraping
    bootstrap()
        bootstraps curve
    
    
    
    """
    
    def __init__(self, df, interpolation):
        """ Attributes definition
        
    
        Parameters
        ----------
        df : pd.DataFrame, tuple
            DataFrame or nodes required for interpolating the curve 
        interpolation : str, default = 'Cubic'
            Type of interpolation wanted for the curve ('Linear or default')
    
        Returns
        -------
        None.
    
        """
        
        super().__init__(df, interpolation)
        self.name = 'USD_OIS'
    
        
    def hlprs(self, df: pd.DataFrame) -> list:
        """ Helpers creation
        

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame with curve inputs

        Returns
        -------
        list
            List of bootstrapping helpers

        """
        
        # input data
        self.tenors = df['Tenors'].tolist()
        tenor = df['Tenors'].str[-1].map(self.tenor2ql).to_list()
        period = df['Period'].to_list()
        self.quotes = (df['Quotes']/100).tolist()
        
        # Deposit rates
        deposits = {(period[0], tenor[0]): self.quotes[0]}
        
        # Swap rates
        n = len(period)
        swaps = {}
        for i in range(1,n):
            swaps[(period[i], tenor[i])] = self.quotes[i]
            
        # Rate Qauntlib.Quote objects
        
        ## desposits
        for n, unit in deposits.keys():
            deposits[(n,unit)] = ql.SimpleQuote(deposits[(n,unit)])
            
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
            self.calendar, 
            ql.ModifiedFollowing, 
            False, 
            dayCounter)
             
            for (n, unit) in deposits.keys()]
        
        ## swap rates
        OIS_Index = ql.FedFunds()
        OISHelpers = [ql.OISRateHelper(
            settlementDays, ql.Period(n, unit),
            ql.QuoteHandle(swaps[(n,unit)]),
            OIS_Index)
             
            for n, unit in swaps.keys()]
        
        ## helpers merge
        helpers = depositHelpers + OISHelpers
        
        return(helpers)
        
    
    def bootstrap(self):    
        """ Bootstrap curve
        

        Returns
        -------
        None.

        """
        # Bootstrapping by nodes
        try:
            self.calendar = ql.UnitedStates(1)
            nodes_dates = [ql.Date(self.nodes[k][0].strftime('%Y-%m-%d'),
                                   '%Y-%m-%d') for k in range(len(self.nodes))]
            nodes_discount_factors = [self.nodes[k][1] 
                                      for k in range(len(self.nodes))]
            
            # Interpolation
            self.curve = ql.DiscountCurve(nodes_dates, nodes_discount_factors, 
                                          ql.Actual360(), self.calendar)
        
        # Bootstrapping by input
        except AttributeError:
            self.calendar = ql.UnitedStates(1)
            self.helpers = self.hlprs(self.df)
            
            # When Linear
            if self.interpolation == 'Linear':
                crvUSDOIS = ql.PiecewiseLogLinearDiscount(0, self.calendar, 
                                                          self.helpers, 
                                                          ql.Actual360())
                
            else: 
                crvUSDOIS = ql.PiecewiseNaturalLogCubicDiscount(0, 
                                                                self.calendar, 
                                                                self.helpers, 
                                                                ql.Actual360())
            
            crvUSDOIS.enableExtrapolation()
            
            #Attribute definition
            self.curve = crvUSDOIS
            nodes = self.curve.nodes()
            dates = [datetime(nodes[k][0].year(), nodes[k][0].month(), 
                              nodes[k][0].dayOfMonth()) 
                     for k in range(len(nodes))]
            rates = [nodes[k][1] for k in range(len(nodes))]
            self.nodes = tuple(zip(dates, rates))
    

        
        
        

class USD_SOFR(curve):
    """ QPT USD_SOFR Curve class
    
    USD SOFR Cuve class, curve class child
    
    ...
    
    Attributes
    ----------
    
    interpolation: str
        Type of interpolation wanted for the curve
    df: pd.DataFrame
        DataFrame with the inputs needed for the curve
    nodes: tuple
        Tuple with the nodes required for the curve
    name: str
        Name of the curve
    calendar: ql.Calendar
        Calendar of the curve
    helpers: list, not available using from_nodes method
        list of hlpers needed to bootstrap he curve
    tenors: list, not available using from_nodes method
        List of tenors for the curve
    tenors_imm: list, not available using from_nodes method
        List of tenors for the next imm dates
    quotes: list, not available using from_nodes method
        List of quotes for each tenor 
    quotes_imm: list, not available using from_nodes method
        List of quotes for the next imm dates
    curve: ql.Discountcurve
        QuantLib Discount Cure object
    ibor_index: ql.Ibor
        Ibor index used for bootstrapping
    
    
    Methods
    -------
    from_nodes(nodes)
        Interpolates curve by nodes
    hlprs(df)
        creates helpers for curve bootstraping
    bootstrap()
        bootstraps curve
    
    
    
    """
    def __init__(self, df:pd.DataFrame, interpolation: str,
                 discount_curve: curve):
        """ Attributes definition
        
    
        Parameters
        ----------
        df : pd.DataFrame, tuple
            DataFrame or nodes required for interpolating the curve 
        interpolation : str, default = 'Cubic'
            Type of interpolation wanted for the curve ('Linear or default')
        discount_curve: curve
            Curve used for discounting cashflows
    
        Returns
        -------
        None.
    
        """
        super().__init__(df, interpolation)
        self.discount_curve = discount_curve
        self.name = 'USD_SOFR'
    
    @classmethod
    
    def from_nodes(cls, nodes):
        interpolation = 'Linear'
        return cls(nodes, interpolation, None)
    
    def hlprs(self, df: pd.DataFrame, 
              discount_curve: ql.RelinkableYieldTermStructureHandle):
        """ Helpers creation
        

        Parameters
        ----------
        df: pd.DataFrame
            DataFrame with curve inputs
        discount_curve: ql.RelinkableYieldTermStructureHandle
            Curve used for discounting cashflows
        

        Returns
        -------
        list
            List of bootstrapping helpers

        """
        # settlement date
        dt_settlement = ql.UnitedStates(1).advance(
                ql.Settings.instance().evaluationDate,ql.Period('2D'))
        
        # non-futures idx
        idx_nonfut = (df['Types'] != 'FUT')
        
        # input data
        self.tenors = df['Tenors'][idx_nonfut].tolist()
        self.tenors_imm = df['Tenors'][~idx_nonfut].tolist()
        tenor = df['Tenors'][idx_nonfut].str[-1].map(self.tenor2ql).to_list()
        period = df['Period'][idx_nonfut].to_list()
        self.quotes = (df['Quotes'][idx_nonfut]/100).tolist()
        self.quotes_imm = (df['Quotes'][~idx_nonfut]/100).tolist()
        
        # IborIndex
        swapIndex = ql.Sofr()

        # Deposit rates
        deposits = {(period[0], tenor[0]): self.quotes[0]}
       
        # Futures rates
        n_fut = len(self.quotes_imm)
        imm = ql.IMM.nextDate(dt_settlement)
        imm = dt_settlement
        futures = {}
        for i in range(n_fut):
            imm = ql.IMM.nextDate(imm)
            futures[imm] = 100 - self.quotes_imm[i]*100  
        
        # Swap rates
        n = len(period)
        swaps = {}
        for i in range(1,n):
            swaps[(period[i], tenor[i])] = self.quotes[i]
            
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
            self.calendar, 
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
            d, months, self.calendar, 
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
            self.calendar,
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
    
    def bootstrap(self):
        """ Bootstraps curve
        
        When nodes exist, it will bootstrap from nodes
        Returns
        -------
        None.

        """
        
        # when Nodes are used
        if hasattr(self, 'nodes'):
            self.calendar = ql.UnitedStates(1)
            
            nodes_dates = [ql.Date(self.nodes[k][0].strftime('%Y-%m-%d'), 
                                   '%Y-%m-%d') for k in range(len(self.nodes))]
            nodes_discount_factors = [self.nodes[k][1] 
                                      for k in range(len(self.nodes))]
            self.curve =\
                ql.NaturalLogCubicDiscountCurve(nodes_dates, 
                                                nodes_discount_factors, 
                                                ql.Actual360(), self.calendar)
            crv_usdswp = ql.RelinkableYieldTermStructureHandle()
            crv_usdswp.linkTo(self.curve)
            self.ibor_index = ql.Sofr(crv_usdswp)
         

        # When Inputs are used
        else:
            # Relikable Yield Structure cretaion for discounting curve
            crvDiscUSD = ql.RelinkableYieldTermStructureHandle()
            crvDiscUSD.linkTo(self.discount_curve.curve)
            self.calendar = ql.UnitedStates(1)    
            self.helpers = self.hlprs(self.df, crvDiscUSD)

            # Interpolation
            ## When Cubic
            if self.interpolation == 'Cubic':
                crvSOFR = ql.PiecewiseNaturalLogCubicDiscount(0, self.calendar, 
                                                                self.helpers, 
                                                                ql.Actual360())
            ## When Linear
            else: 
               crvSOFR = ql.PiecewiseLogLinearDiscount(0, self.calendar, 
                                                               self.helpers, 
                                                               ql.Actual360())
            ## Extrapolation of curve
            crvSOFR.enableExtrapolation()
            self.curve = crvSOFR
            crv_usdswp = ql.RelinkableYieldTermStructureHandle()
            crv_usdswp.linkTo(self.curve)
            self.ibor_index = ql.Sofr(crv_usdswp)
           
            # Save Nodes
            ## Linear nodes
            try:
                
                crvSOFR_l = ql.PiecewiseLogLinearDiscount(0, self.calendar, 
                                                                self.helpers, 
                                                                ql.Actual360())
                crvSOFR_l.enableExtrapolation()
                nodes = crvSOFR_l.nodes()
                dates=[datetime(nodes[k][0].year(), nodes[k][0].month(), 
                                nodes[k][0].dayOfMonth()) 
                       for k in range(len(nodes))]
                rates = [nodes[k][1] for k in range(len(nodes))]
                self.nodes = tuple(zip(dates,rates))
            
            ## Cubic nodes
            except:
                nodes=self.curve.nodes()
                dates=[datetime(nodes[k][0].year(), nodes[k][0].month(), 
                                nodes[k][0].dayOfMonth()) 
                       for k in range(len(nodes))]
                rates = [nodes[k][1] for k in range(len(nodes))]
                self.nodes = tuple(zip(dates,rates))
            
    
        
   
class MXN_OIS(curve):
    """ QPT MXN_OIS Curve class
    
    MXN OIS Cuve class, curve class child
    
    ...
    
    Attributes
    ----------
    
    interpolation: str
        Type of interpolation wanted for the curve
    dic_df: dict
        Dictionary with dataFrames with the inputs needed for the curve
    nodes: tuple
        Tuple with the nodes required for the curve
    discount_curve: curve
        curve used for discountng
    swap_curve: curve
        curve used for cashflows
    name: str
        Name of the curve
    historical: bool
        When true, historicla curves are made
    calendar: ql.Calendar
        Calendar of the curve
    helpers: list, not available using from_nodes method
        list of hlpers needed to bootstrap he curve
    tenors: list, not available using from_nodes method
        List of tenors for the curve
    tenors_fwds: list, not available using from_nodes method
        List of tenors for the next futures
    quotes: list, not available using from_nodes method
        List of quotes for each tenor 
    quotes_basis_spot: float
        Spot fx for basis swaps
    quotes_fwds: list, not available using from_nodes method
        List of quotes for the next future dates
    quotes_basis: basis cuotes, not available using from_nodes method
        List of basis quotes
    curve: ql.Discountcurve
        QuantLib Discount Cure object
    discounting_engine: ql.DiscountingSwapEngine
        discounting engine for swaps
    
    
    Methods
    -------
    from_nodes(nodes)
        Interpolates curve by nodes
    historical_curve(start_date)
        creates the curve by past information
    hlprs(dic_df)
        creates helpers for curve bootstraping
    bootstrap()
        bootstraps curve
    
    """
    def __init__(self, dic_df: dict, interpolation: str, discount_curve: curve,
                 swap_curve: curve, historical: bool = False):
        """ Attributes definition
        
    
        Parameters
        ----------
        dic_df : pd.DataFrame, tuple
            DataFrame or nodes required for interpolating the curve 
        interpolation : str, default = 'Cubic'
            Type of interpolation wanted for the curve ('Linear or default')
        discount_curve: curve
            Curve used for discounting cashflows
        swap_curve: curve
            curve used for calculating the cahsflow for each swap
    
        Returns
        -------
        None.
    
        """
        super().__init__(dic_df, interpolation)
        self.discount_curve = discount_curve
        self.swap_curve = swap_curve
        self.name = 'MXN_OIS'
        self.historical = historical
    
    def historical_curve(self, start_date: datetime):
       """ Creates closing curves
       only MXN_OIS and MXN_TIIE curves

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
       QuantLib Discount Curve for MXN OIS
       
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
           
           raise Exception('Please check OIS Files')
       
       
           
           
       period_file = min(len(df_OIS), 11650)
       check_OIS = True 
       
       # Schedule for date making
       effective_date = ql.Date(start_date.day, start_date.month, start_date.year)
       period = ql.Period(period_file -1, ql.Days)
       termination_date = effective_date + period
       tenor = ql.Period(ql.Daily)
       calendar = self.calendar
       business_convention = ql.Unadjusted
       termination_business_convention = ql.Following
       date_generation = ql.DateGeneration.Forward
       end_of_month = True

       schedule = ql.Schedule(effective_date, termination_date, tenor, calendar,
                              business_convention, 
                              termination_business_convention, date_generation,
                              end_of_month)
       
       # quotes dates
       dates = []
       for i, d in enumerate(schedule):
           dates.append(d)

       # QuantLib  Discount curve (OIS) creation
         
       lstOIS_dfs = [1]

       for i in range(0, min(df_OIS.shape[0]-1,11649)):
           t,r = df_OIS.iloc[i,[1,2]]
           lstOIS_dfs.append(1/(1 + r*t/36000)) 
           
       
       
       crvMXNOIS = ql.DiscountCurve(dates, lstOIS_dfs, ql.Actual360(), ql.Mexico())
       
       return crvMXNOIS
       
    
    def hlprs(self, dic_df:dict,
              discount_curve: ql.RelinkableYieldTermStructureHandle):
        """ Helpers creation
        

        Parameters
        ----------
        dic_df: dict
            Dictionary with curves inputs
        discount_curve: ql.RelinkableYieldTermStructureHandle
            Curve used for discounting cashflows
        

        Returns
        -------
        list
            List of bootstrapping helpers

        """

        
        # Handle dat
        spotfx = dic_df['USDMXN_XCCY_Basis']['Quotes'][0]
        self.quotes_basis_spot = spotfx
        df_basis = dic_df['USDMXN_XCCY_Basis']
        df_tiie = dic_df['MXN_TIIE']
        df_fwds = dic_df['USDMXN_Fwds']
        
        # Handle idxs
        self.tenors_fwds = ['%3M','%6M', '%9M', '%1Y']
        idx_fwds = np.where(np.isin(df_fwds['Tenor'],
                         ['%3M','%6M', '%9M', '%1Y']))[0].tolist()
        lst_tiieT = ['%1L', '%26L', '%39L', '%52L', '%65L', 
                     '%91L', '%130L', '%195L', '%260L', '%390L']
        self.tenors = lst_tiieT
        idx_tiie = np.where(np.isin(df_tiie['Tenor'],
                         lst_tiieT))[0].tolist()
        # Input data
        tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                    'Y': ql.Years}
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
        self.quotes = data_tiie
        data_fwds = (df_fwds['Quotes'][idx_fwds]/10000).tolist()
        self.quotes_fwds = data_fwds
        
        
        data_basis = (-1*df_basis['Quotes']/10000).tolist()
        

        
        self.quotes_basis = data_basis
        # Basis swaps
        basis_usdmxn = {}
        n_basis = len(basis_period)
        for i in range(1,n_basis):
            basis_usdmxn[(basis_period[i], tenor)] = data_basis[i]

        # Forward Points
        fwdpts = {}
        n_fwds = len(fwds_period)
        for i in range(n_fwds):
            fwdpts[(fwds_period[i], tenor)] = data_fwds[i]

        # Deposit rates
        deposits = {(tiie_period[0], tenor_type[0]): data_tiie[0]}
        
        # TIIE Swap rates]
        swaps_tiie = {}
        n_tiie = len(tiie_period)
        for i in range(1,n_tiie):
            swaps_tiie[(tiie_period[i], tenor_type[i])] = data_tiie[i]

        # Qauntlib.Quote objects
        for n,unit in basis_usdmxn.keys():
            basis_usdmxn[(n,unit)] = ql.SimpleQuote(basis_usdmxn[(n,unit)])
            
        for n,unit in fwdpts.keys():
            fwdpts[(n,unit)] = ql.SimpleQuote(fwdpts[(n,unit)])
            
        for n,unit in deposits.keys():
            deposits[(n,unit)] = ql.SimpleQuote(deposits[(n,unit)])
            
        for n,unit in swaps_tiie.keys():
            swaps_tiie[(n,unit)] = ql.SimpleQuote(swaps_tiie[(n,unit)])
            
            
        # Deposit rate helper
        dayCounter = ql.Actual360()
        settlementDays = 1
        depositHelpers = [ql.DepositRateHelper(
            ql.QuoteHandle(deposits[(n, unit)]),
            ql.Period(n, ql.Weeks), 
            settlementDays,
            self.calendar, 
            ql.Following,
            False, 
            dayCounter
            )
            for n, unit in deposits.keys()
        ]

        # FX Forwards helper
        fxSwapHelper = [ql.FxSwapRateHelper(
            ql.QuoteHandle(fwdpts[(n,u)]),
            ql.QuoteHandle(
                ql.SimpleQuote(spotfx)),
            ql.Period(n*4, ql.Weeks),
            2,
            self.calendar,
            ql.Following,
            False,
            True,
            discount_curve
            ) 
            for n,u in fwdpts.keys()
        ]

        # Swap rate helpers
        settlementDays = 2
        fixedLegFrequency = ql.EveryFourthWeek
        fixedLegAdjustment = ql.Following
        fixedLegDayCounter = ql.Actual360()

        fxIborIndex = self.swap_curve.ibor_index


        swapHelpers = [ql.SwapRateHelper(ql.QuoteHandle(swaps_tiie[(n,unit)]),
                                       ql.Period(n, ql.Weeks), 
                                       self.calendar,
                                       fixedLegFrequency, 
                                       fixedLegAdjustment,
                                       fixedLegDayCounter, 
                                       fxIborIndex, 
                                       ql.QuoteHandle(basis_usdmxn[(n/4,tenor)]), 
                                       ql.Period(0, ql.Days))
                       for n, unit in swaps_tiie.keys() ]

        # Rate helpers merge
        helpers = depositHelpers + fxSwapHelper + swapHelpers

        return(helpers)

    def bootstrap(self):
        """ Bootstraps curve
        

        Returns
        -------
        None.

        """
        self.calendar = ql.Mexico()
        if not self.historical:
            if hasattr(self, 'nodes'):
               
               
               nodes_dates = [ql.Date(self.nodes[k][0].strftime('%Y-%m-%d'), '%Y-%m-%d') 
                                   for k in range(len(self.nodes))]
               nodes_discount_factors = [self.nodes[k][1] 
                                              for k in range(len(self.nodes))]
               self.curve = ql.NaturalLogCubicDiscountCurve(nodes_dates, 
                                                            nodes_discount_factors, 
                                                            ql.Actual360(), 
                                                            self.calendar)
            
            else:
                
                # When there is a usdSWP curve it can be done
                    
                crvDiscUSD = ql.RelinkableYieldTermStructureHandle()
                crvDiscUSD.linkTo(self.discount_curve.curve)
                
                # with Futures
                self.helpers = self.hlprs(self.df, crvDiscUSD)
                crvMXNOIS = ql.PiecewiseNaturalLogCubicDiscount(0, self.calendar, 
                                                               self.helpers, 
                                                               ql.Actual360())
                crvMXNOIS.enableExtrapolation()
                self.curve = crvMXNOIS
                
                nodes = self.curve.nodes()
                dates=[datetime(nodes[k][0].year(), nodes[k][0].month(), 
                                nodes[k][0].dayOfMonth()) 
                       for k in range(len(nodes))]
                rates = [nodes[k][1] for k in range(len(nodes))]
                self.nodes = tuple(zip(dates,rates))
            rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
            rytsMXNOIS.linkTo(self.curve)   
            self.discount_engine = ql.DiscountingSwapEngine(rytsMXNOIS)
            
        else:
            print('MXN OIS interpolation cannot be done done, '+
                  'Historical curve will be used instead')
            # When there is not a USDSWP curve because of USA holiday
            
            try:
                start_date = ql.Settings.instance().evaluationDate.to_date()
                self.curve= self.historical_curve(start_date)
                # print(start_date)
            
            except:
                start_date = self.calendar.advance(
                    ql.Settings.instance().evaluationDate, 
                    ql.Period(-1, ql.Days)).to_date()
                self.curve= self.historical_curve(start_date)
            # print(start_date)
            nodes = self.curve.nodes()
            dates=[datetime(nodes[k][0].year(), nodes[k][0].month(), 
                            nodes[k][0].dayOfMonth()) 
                   for k in range(len(nodes))]
            rates = [nodes[k][1] for k in range(len(nodes))]
            self.nodes = tuple(zip(dates, rates))
                
            rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
            rytsMXNOIS.linkTo(self.curve)
            
            self.discount_engine = ql.DiscountingSwapEngine(rytsMXNOIS)
    
        
class MXN_TIIE(curve):
    """ QPT MXN_TIIE Curve class
    
    MXN TIIE Cuve class, curve class child
    
    ...
    
    Attributes
    ----------
    
    interpolation: str
        Type of interpolation wanted for the curve
    df: pd.DataFrame
        DataFrame with the inputs needed for the curve
    nodes: tuple
        Tuple with the nodes required for the curve
    name: str
        Name of the curve
    calendar: ql.Calendar
        Calendar of the curve
    helpers: list, not available using from_nodes method
        list of hlpers needed to bootstrap he curve
    tenors: list, not available using from_nodes method
        List of tenors for the curve
    quotes: list, not available using from_nodes method
        List of quotes for each tenor 
    curve: ql.Discountcurve
        QuantLib Discount Cure object
    ibor_index: ql.IborIndex
        Ibor index used for bootstrapping
    complete_ibor_tiie: ql.IborIndex
        Ibor index used for bootstrapping with all past fixings
    
    
    Methods
    -------
    from_nodes(nodes)
        Interpolates curve by nodes
    historical_curve(start_date)
        creates curve with past curve data
    banxico_download_data(serie, banxico_start_date, banxico_end_date, token)
        downloads data of a given token from banxico API
    banxicoData(evaluation_date)
        downloads one year data of TIIE 28 from banxico API or from the 
        historical file
    complete_ibor_index()
       creates IborIndex with all fixings
    set_ibor_tiie()
        creates IborIndex without past fixings
    hlprs(df)
        creates helpers for curve bootstraping
    bootstrap()
        bootstraps curve
    
    """
    def __init__(self, df: pd.DataFrame, interpolation: str, 
                 discount_curve: curve, historical: bool = False, 
                 bo_crv: np.array = np.array([])):
        """ Attributes definition
        
    
        Parameters
        ----------
        dic_df : pd.DataFrame, tuple
            DataFrame or nodes required for interpolating the curve 
        interpolation : str, default = 'Cubic'
            Type of interpolation wanted for the curve ('Linear or default')
        discount_curve: curve
            curve iused for dicounting cashflows
        
        bo_crv: array, optional
            Modified MXNTIIE quotes for bid offer calculations. See bid_offer
            class to see how these quotes are calculated. The default is []
            
    
        Returns
        -------
        None.
    
        """
        super().__init__(df, interpolation)
        self.discount_curve = discount_curve
        self.bo_crv = bo_crv
        self.historical = historical
        self.name = 'MXN_TIIE'
        
    
    def banxico_download_data(self, serie: str, banxico_start_date: str, 
                              banxico_end_date :str, 
                              token: str) -> pd.DataFrame:
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
    
    def banxicoData(self, evaluation_date: datetime) -> pd.DataFrame:
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
            
            banxico_TIIE28 = self.banxico_download_data('SF43783', banxico_start_date, 
                                                      banxico_end_date, token)
            
            if banxico_TIIE28.empty:
                banxico_TIIE28 = pd.read_excel('//TLALOC/tiie/HistoricalTIIE.xlsx')
                if evaluation_date not in pd.to_datetime(banxico_TIIE28['fecha']):
                    file_path = r'\\TLALOC\tiie\Remate\REMATE CLOSING PRICES FOR '
                    yesterday_ql = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                                       ql.Period(-1, ql.Days))
                    file = file_path + yesterday_ql.to_date().strftime('%m%d%Y') + '.pdf'
                    
                    with pdfplumber.open(file) as pb:
                        text = pb.pages[0].extract_text()
                    renglones = text.split('\n')
                    
                    row_tiie=renglones.index([i for i in renglones if '28d TIIE' in i][0])
                    tiie_28_yst = float(renglones[row_tiie][
                        len('28d TIIE '): renglones[row_tiie].find('%')])/100
                    banxico_TIIE28 = pd.concat(
                        [banxico_TIIE28,  
                         pd.DataFrame({'fecha': [evaluation_date], 
                                       'dato': [tiie_28_yst]})], ignore_index=True)
                    banxico_TIIE28 = \
                        banxico_TIIE28[banxico_TIIE28['fecha']<=evaluation_date]
                    banxico_TIIE28 = banxico_TIIE28.drop_duplicates(subset='fecha')
                
            banxico_TIIE28.to_excel('//TLALOC/tiie/HistoricalTIIE.xlsx', 
                                    index=False)
            
             
            
        # Use historicalTIIE saved      
        except:
            banxico_TIIE28 = pd.read_excel('//TLALOC/tiie/HistoricalTIIE.xlsx')
            if evaluation_date not in pd.to_datetime(banxico_TIIE28['fecha']):
                file_path = r'\\TLALOC\tiie\Remate\REMATE CLOSING PRICES FOR '
                yesterday_ql = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                                   ql.Period(-1, ql.Days))
                file = file_path + yesterday_ql.to_date().strftime('%m%d%Y') + '.pdf'
                
                with pdfplumber.open(file) as pb:
                    text = pb.pages[0].extract_text()
                renglones = text.split('\n')
                
                row_tiie=renglones.index([i for i in renglones if '28d TIIE' in i][0])
                tiie_28_yst = float(renglones[row_tiie][
                    len('28d TIIE '): renglones[row_tiie].find('%')])/100
                banxico_TIIE28 = pd.concat(
                    [banxico_TIIE28,  
                     pd.DataFrame({'fecha': [evaluation_date], 
                                   'dato': [tiie_28_yst]})], ignore_index=True)
                banxico_TIIE28 = \
                    banxico_TIIE28[banxico_TIIE28['fecha']<=evaluation_date]
                banxico_TIIE28 = banxico_TIIE28.drop_duplicates(subset='fecha')
        
         
        
            
        return banxico_TIIE28

    
    def historical_curve(self, start_date: datetime):
       """ Creates closing curves
       only MXN_OIS and MXN_TIIE curves

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
       QuantLib Discount Curve for MXN OIS
       
       """
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
           check_TIIE = False
           
           raise Exception('Please check TIIE Files')
       
       
           
           
       period_file = min(len(df_TIIE), 11650)
       check_TIIE = True 
       
       # Schedule for date making
       effective_date = ql.Date(start_date.day, start_date.month, start_date.year)
       period = ql.Period(period_file -1, ql.Days)
       termination_date = effective_date + period
       tenor = ql.Period(ql.Daily)
       calendar = self.calendar
       business_convention = ql.Unadjusted
       termination_business_convention = ql.Following
       date_generation = ql.DateGeneration.Forward
       end_of_month = True

       schedule = ql.Schedule(effective_date, termination_date, tenor, calendar,
                              business_convention, 
                              termination_business_convention, date_generation,
                              end_of_month)
       
       # quotes dates
       dates = []
       for i, d in enumerate(schedule):
           dates.append(d)

       # QuantLib  Discount curve (OIS) creation
         
       lstTIIE_dfs = [1]

       for i in range(0, min(df_TIIE.shape[0]-1,11649)):
           t,r = df_TIIE.iloc[i,[1,2]]
           lstTIIE_dfs.append(1/(1 + r*t/36000)) 
           
       
       
       crvMXNTIIE = ql.DiscountCurve(dates, lstTIIE_dfs, ql.Actual360(), ql.Mexico())
       
       return crvMXNTIIE
       
    
    def hlprs(self, df: pd.DataFrame,
              discount_curve: ql.RelinkableYieldTermStructureHandle):
        """ Helpers creation
        

        Parameters
        ----------
        dic_df: pd.DataFrame
            DataFrame with curves inputs
        discount_curve: ql.RelinkableYieldTermStructureHandle
            Curve used for discounting cashflows
        

        Returns
        -------
        list
            List of bootstrapping helpers

        """

        
        tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                     'Y': ql.Years}
        # calendar
        calendar_mx = self.calendar
        # data
        def f(x):
            if x[-1]=='L':
                return int(x[1:-1])*4
            else:
                return int(x[1:-1])
        self.tenors = df['Tenor']
        period = self.tenors.map(lambda x: f(x)).tolist()
       
        tenor_type = self.tenors.str[-1].map(tenor2ql).tolist()
        
        
        # When using desk, calculate mid quotes
            
        if len(self.bo_crv) != 0:
            self.quotes = self.bo_crv/100
                
        else:
            self.quotes = (df['Quotes']/100).tolist()
            
        
        # Deposit rates
        deposit_index = df[df['Tenor']=='%1L'].index[0]
        deposits = {(period[deposit_index], 
                     tenor_type[deposit_index]): self.quotes[deposit_index]}
        # Swap rates
        swap_indexes = [i for i in df.index if i != deposit_index]
        swaps = {}
        for i in swap_indexes:
            swaps[(period[i], tenor_type[i])] = self.quotes[i]
            
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
                                    discount_curve)
   
        swapHelpers = [ql.SwapRateHelper(ql.QuoteHandle(swaps[(n,unit)]),
                                         ql.Period(n, unit), calendar_mx,
                                         fixedLegFrequency, fixedLegAdjustment,
                                         fixedLegDayCounter, ibor_MXNTIIE)
                       for n, unit in swaps.keys()]
   
        # helpers merge
        helpers = depositHelpers + swapHelpers
   
      
   
        return(helpers)
         
    def complete_ibor_index(self) -> ql.IborIndex:
        """ creates complete ibor_tiie 
        

        Returns
        -------
        ibor_tiie : TYPE
            IborIndex QuantLib object for pricing swaps

        """
        
        
        
        # Eval Date
        ql_eval_date = ql.Settings.instance().evaluationDate
        dt_eval_date = datetime(ql_eval_date.year(),
                            ql_eval_date.month(),
                            ql_eval_date.dayOfMonth())
        final_date = dt_eval_date
        
        # Fixings
        banxico_TIIE28 = self.banxicoData(dt_eval_date)
        
        if dt_eval_date > pd.to_datetime(banxico_TIIE28['fecha'].iloc[-1]):
              path = os.getcwd()
              
              path = path.replace('\\DailyPnL', '')
              file = path+'\\TIIE_IRS_Data.xlsm'

              future_rates = pd.read_excel(file, 
                                           sheet_name = 'Short_End_Pricing', 
                                           skiprows = 1, 
                                           usecols = ['MPC Meetings', 
                                                      'Fix Eff', 'Rate'])  
              future_rates = future_rates.iloc[1:26]
              future_rates['Rate'] = future_rates['Rate'].astype(float)/100
              banxico_TIIE28_a = pd.DataFrame(
                  {'fecha': pd.date_range(
                      banxico_TIIE28.iloc[-1]['fecha'] + timedelta(days=1),
                      future_rates.iloc[0]['MPC Meetings'],freq='d'),
                      'dato' : banxico_TIIE28.iloc[-1]['dato']})
              banxico_TIIE28 = pd.concat([banxico_TIIE28, banxico_TIIE28_a], 
                                         ignore_index = True)
              for k in range(future_rates.shape[0]-1):
                  banxico_TIIE28_a = pd.DataFrame(
                      {'fecha': pd.date_range(future_rates.iloc[k]['Fix Eff'],
                                              future_rates.\
                                                  iloc[k+1]['MPC Meetings'],
                                              freq='d'),
                       'dato': future_rates.iloc[k]['Rate']})
                  banxico_TIIE28 = pd.concat(
                      [banxico_TIIE28, banxico_TIIE28_a], ignore_index = True)
              
              banxico_TIIE28['fecha'] = pd.to_datetime(banxico_TIIE28['fecha'])
              banxico_business_dates =\
                  [banxico_TIIE28.iloc[k]['fecha'] 
                   for k in range(banxico_TIIE28.shape[0]) 
                   if ql.Mexico().isBusinessDay(ql.Date().from_date(
                           banxico_TIIE28.iloc[k]['fecha']))]

              banxico_TIIE28 =\
                  banxico_TIIE28[(banxico_TIIE28['fecha']<= final_date) 
                                 & banxico_TIIE28['fecha'].isin(
                                     banxico_business_dates)]
        
        # TIIE IBOR INDEX
        self.banxico_TIIE28 = banxico_TIIE28
        ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
        ibor_tiie_crv.linkTo(self.curve)

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
            try:
                ibor_tiie.addFixing(
                    ql.Date(dt_fixing.day, dt_fixing.month, dt_fixing.year), 
                    banxico_TIIE28.iloc[h+1]['dato']
                    )
            except: 
                pass
        
     
        return ibor_tiie 
    
    def set_ibor_TIIE(self) -> ql.IborIndex:
        """ sets the ibor index wihout fixings
        

        Returns
        -------
        IborIndex QuantLib oject

        """
        
      
        ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
        ibor_tiie_crv.linkTo(self.curve)

        ibor_tiie = ql.IborIndex('TIIE',
                     ql.Period(13),
                     1,
                     ql.MXNCurrency(),
                     ql.Mexico(),
                     ql.Following,
                     False,
                     ql.Actual360(),
                     ibor_tiie_crv)
        
        

        return(ibor_tiie)  
    
    def bootstrap(self):
        """ Bootstraps curve
        

        Returns
        -------
        None.

        """
        self.calendar = ql.Mexico()
        # Discount curve
        if not self.historical:
            crv_mxnois = ql.RelinkableYieldTermStructureHandle()
            crv_mxnois.linkTo(self.discount_curve.curve)
            
            # Helpers
            hlprTIIE = self.hlprs(self.df, crv_mxnois)
            
            # Curve creation
            crvTIIE = ql.PiecewiseNaturalLogCubicDiscount(0, ql.Mexico(), hlprTIIE, 
                                                          ql.Actual360())
            crvTIIE.enableExtrapolation()
            
            self.curve = crvTIIE
            
            self.ibor_index = self.set_ibor_TIIE()
            self.nodes = self.curve.nodes()
        
        else:
            print('MXN TIIE interpolation cannot be done done, '+
                  'Historical curve will be used instead')
            # When there is not a USDSWP curve because of USA holiday
            try:
                start_date = ql.Settings.instance().evaluationDate.to_date()
                self.curve = self.historical_curve(start_date)
            except:
                start_date = self.calendar.advance(
                    ql.Settings.instance().evaluationDate, 
                    ql.Period(-1, ql.Days)).to_date()
                self.curve = self.historical_curve(start_date)
                
            nodes = self.curve.nodes()
            dates=[datetime(nodes[k][0].year(), nodes[k][0].month(), 
                            nodes[k][0].dayOfMonth()) 
                   for k in range(len(nodes))]
            rates = [nodes[k][1] for k in range(len(nodes))]
            self.nodes = tuple(zip(dates, rates))
            
            
            self.ibor_index = self.set_ibor_TIIE()
            
        
        
        
def save_obj(obj, name: str) -> None:
    """Saves object to pickle file

    Parameters
    ----------
    obj : any object
        Object you want to save
    name : str
        The directory and name for saving

    Returns
    -------
    None
        Saves the object in the directory given

    """

    with open(name +'.pickle', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
        


def load_obj(name: str) -> None:
    """Loads object from pickle file
    

    Parameters
    ----------
    name : str
        The directory and name for loading

    Returns
    -------
    None
        loads the object from the directory given

    """
    with open(name + '.pickle', 'rb') as f:
        return pickle.load(f)                 
            
        
#------------------------
#  Curve Creation class
#------------------------
class _mxn_curves:
    """
    """
    
    
    def __init__(self, **kwargs):
        done_keys = []
        for k in kwargs.keys():
            
            if k == 'dic_data':
                self.dic_data = kwargs[k]
                done_keys.append(k)   
            elif 'dic_data' in done_keys:
                pass
            else:
                self.dic_data = None
                
            if k == 'crvUSDOIS':
                self.crvUSDOIS = kwargs[k]
                done_keys.append(k) 
            elif 'crvUSDOIS' in done_keys:
                pass
            else: 
                self.crvUSDOIS = None
                
            if k == 'crvUSDSOFR':
                self.crvUSDSOFR = kwargs[k]
                done_keys.append(k) 
            elif 'crvUSDSOFR' in done_keys:
                pass
            else: 
                self.crvUSDSOFR = None
                
            if k == 'crvMXNOIS':
                self.crvMXNOIS = kwargs[k]
                done_keys.append(k) 
            elif 'crvMXNOIS' in done_keys:
                pass
            else: 
                self.crvMXNOIS = None
                
            if k == 'crvMXNTIIE':
                self.crvMXNTIIE = kwargs[k]
                done_keys.append(k) 
            elif 'crvMXNTIIE' in done_keys:
                pass
            else: 
                self.crvMXNTIIE = None
                     
            if k == 'crvMXNTIIE.complete_ibor_tiie':
                self.crvMXNTIIE.complete_ibor_tiie = kwargs[k]
                done_keys.append(k) 
            elif 'crvMXNTIIE.complete_ibor_tiie' in done_keys:
                pass
            else: 
                try:
                    self.crvMXNTIIE.complete_ibor_tiie =\
                        self.crvMXNTIIE.complete_ibor_index()
                except: 
                    self.crvMXNTIIE.complete_ibor_tiie = None
                 
            if k == 'dv01_engines':
                self.dv01_engines = kwargs[k]
                done_keys.append(k) 
            elif 'dv01_engines' in done_keys:
                pass
            else: 
                self.dv01_engines = None
                 
            if k == 'KRR_curves':
                self.KRR_curves = kwargs[k]
                done_keys.append(k) 
            elif 'KRR_curves' in done_keys:
                pass
            else: 
                self.KRR_curves = None
        
    
    
    def _flat_dv01_engines(self, crvUSDOIS: curve,
                           crvUSDSOFR: curve) -> list:
        """Creates modified Ibor Index and engines with shifted quotes
        
        Shifts quotes +- 1 bps to calculate modified Ibor Index and engines
        to calculate flat DV01.
        Parameters
        ----------
        dic_data : dict
            dictionary of all the inputs
        crvUSDOIS: curve
            USD OIS curve
        crvUSDSOFR: curve
        

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
        historical = False
        if not crvUSDOIS:
            historical = True
        interpolation = 'Cubic'
        
        # dic data
        modic = {k: v.copy() for k,v in self.dic_data.items()}

        # rates data
        data_tiie = modic['MXN_TIIE']
        df_tiie = data_tiie[['Tenor','Quotes']]
        
        
        tmpdf = df_tiie.copy()
        
        #Plus shift
        n = len(df_tiie)
        shift_plus_list = np.array([0.01] * n)
           
        # tenor and rate mods
        tenor = df_tiie['Tenor']
        rate_plus_1bp = df_tiie['Quotes'].tolist() + shift_plus_list

        # data +1bp
        tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_plus_1bp
        modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
        ## disc crv
        crvMXNOIS = MXN_OIS(modic, interpolation, crvUSDOIS, crvUSDSOFR, historical)
        crvMXNOIS.bootstrap()
        
        crvTIIE = MXN_TIIE(modic['MXN_TIIE'], interpolation, crvMXNOIS)
        crvTIIE.bootstrap()

        ibor_tiie_plus = crvTIIE.complete_ibor_index()
        tiie_swp_engine_plus = crvMXNOIS.discount_engine
      
        # Minus shift

        shift_minus_list = np.array([-0.01] * n)
        rate_min_1bp = df_tiie['Quotes'].tolist() + shift_minus_list
        
        # data -1bp
        tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_min_1bp
        modic['MXN_TIIE']['Quotes'] = tmpdf['Quotes']
        ## disc crv
        crvMXNOIS_ = MXN_OIS(modic, interpolation, crvUSDOIS, crvUSDSOFR, historical)
        crvMXNOIS_.bootstrap()
        
        crvTIIE_ = MXN_TIIE(modic['MXN_TIIE'], interpolation, crvMXNOIS_)
        crvTIIE_.bootstrap()
        
        ibor_tiie_minus = crvTIIE_.complete_ibor_index()
        tiie_swp_engine_minus = crvMXNOIS_.discount_engine
        
        return [ibor_tiie_plus, tiie_swp_engine_plus, ibor_tiie_minus, 
                tiie_swp_engine_minus]
    
    def flat_dv01_engines(self, inplace: bool = False) -> list:
        """Creates modified Ibor Index and engines with shifted quotes
        
        Shifts quotes +- 1 bps to calculate modified Ibor Index and engines
        to calculate flat DV01.
        
        Parameters
        ----------
        inplace : bool, default = False
            When true, dv01_engines attribute will be defined
        

        Returns
        -------
        list | None
        
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
        if inplace:
            try:
                self.dv01_engines = self._flat_dv01_engines(self.crvUSDOIS, 
                                                            self.crvUSDSOFR)
                                                            
            except:
                self.dv01_engines = self._flat_dv01_engines(None, None)
                                                             
                                                          
            return None
        
        else:
            try:
                return self._flat_dv01_engines(self.crvUSDOIS, self.crvUSDSOFR)
                                               
            except:
                return self._flat_dv01_engines(None, None)
                                               
    
    def _KRR_crvs(self, **kwargs) -> dict:
        """Creates curves to calculate DV01 by tenor.
        
        Uses a DataFRame with MXNTIIE quotes and tenors, and MXNOIS curve 
        to create a dictionary of curves to calculate bucket risk.
        
        Parameters
        ----------
        dic_data : dict
            dictionary of all the inputs
        **kwargs
        crvMXNOIS: curve, default = None
            if None, new MXNOIS curves are calculated
        crvUSDOIS: curve, default = None
            used for calculating new crvMXNOIS curves
        crvUSDSOFR: curve, default = None
            used for calculating new crvMXNOIS curves
            

        Returns
        -------
        dict
            Dictionary with tenors +-1 as keys and a list with shifted 
            discount_curve and float_rate_curve curves as values. 
            Example: dict_crvs['%3L+1'] = [discount_curve, float_rate_curve] 
            with discount_curve and float_rate_curve shifted 1bps in 3L tenor.

        """
       
        done_keys = []
        for k in kwargs.keys():
            if k == 'crvMXNOIS':
                crvMXNOIS = kwargs[k]
                done_keys.append(k)
                complete = False
            elif 'crvMXNOIS' in done_keys:
                pass
            else:
                crvMXNOIS = None
                
            if k == 'crvUSDOIS':
                crvUSDOIS = kwargs[k]
                done_keys.append(k)
                complete = True
            elif 'crvUSDOIS' in done_keys:
                pass
            
            else:
                crvUSDOIS = None
                
            if k == 'crvUSDSOFR':
                crvUSDSOFR = kwargs[k]
                done_keys.append(k)
            elif 'crvUSDSOFR' in done_keys:
                pass
            else:
                crvUSDSOFR = None
            
        
        
        df = self.dic_data['MXN_TIIE']
        modic = {k:v.copy() for k,v in self.dic_data.items()}
        # rates data
        df_tiie = df[['Tenor', 'Quotes']].copy()
        
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
            modic['MXN_TIIE'] = tmpdf
            
            # Proj Curve
            if complete:
                
                crvMXNOIS = MXN_OIS(modic, 'Cubic', crvUSDOIS, crvUSDSOFR)
                crvMXNOIS.bootstrap()
                
            else:
                crvMXNOIS = self.crvMXNOIS
                
            float_rate_curve = MXN_TIIE(tmpdf, 'Cubic', crvMXNOIS)
            float_rate_curve.bootstrap()
            
            # Save
            dict_crvs[tenor+'+1'] = [crvMXNOIS, float_rate_curve]
            
            # Tenor -1bp
            tmpdf['Quotes'][df_tiie['Tenor'] == tenor] = rate_min_1bp
            modic['MXN_TIIE'] = tmpdf
            
            # Proj Curve
            if complete:
                crvMXNOIS_ = MXN_OIS(modic, 'Cubic', crvUSDOIS, crvUSDSOFR)
                crvMXNOIS_.bootstrap()
                
            else:
                crvMXNOIS_ = self.crvMXNOIS
                
            float_rate_curve_ = MXN_TIIE(tmpdf, 'Cubic', crvMXNOIS_)
            float_rate_curve_.bootstrap()
            # Save
            dict_crvs[tenor+'-1'] = [crvMXNOIS_, float_rate_curve_]
            
        
        return (dict_crvs)
             
    def KRR_crvs(self, complete: bool = False,
                 inplace: bool = False) -> dict:
        
        """Creates curves to calculate DV01 by tenor.
        
        Uses a DataFRame with MXNTIIE quotes and tenors, and MXNOIS curve 
        to create a dictionary of curves to calculate bucket risk.
        
        Parameters
        ----------
        complete : bool, default = False
            When true, crvMXNOIS will always be calculated
        inplace : bool, default = False
            When true, dv01_engines attribute will be defined
            

        Returns
        -------
        dict
            Dictionary with tenors +-1 as keys and a list with shifted 
            discount_curve and float_rate_curve curves as values. 
            Example: dict_crvs['%3L+1'] = [discount_curve, float_rate_curve] 
            with discount_curve and float_rate_curve shifted 1bps in 3L tenor.

        """
        if inplace:
            date = ql.Settings.instance().evaluationDate
            if complete and not ql.UnitedStates(1).isHoliday(date):
                self.KRR_curves = self._KRR_crvs(crvUSDOIS = self.crvUSDOIS, 
                                                 crvUSDSOFR = self.crvUSDSOFR)
                                                 
            else:
                self.KRR_curves = self._KRR_crvs(crvMXNOIS = self.crvMXNOIS)
                
            return None
                
        else:
            if complete and not ql.UnitedStates(1).isHoliday(date):
                KRR_curves = self._KRR_crvs(crvUSDOIS = self.crvUSDOIS, 
                                            crvUSDSOFR = self.crvUSDSOFR)
                                                  
                                                 
            else:
                KRR_curves = self._KRR_crvs(crvMXNOIS = self.crvMXNOIS) 
                                                 
            return KRR_curves
                   
                
         
    

class mxn_curves(_mxn_curves):
    """ MXN, curves, KRR curves and engines and dv01 engines
    
    ...
    
    Attributes
    ----------
    
    dic_data: dict
        diccionary of dataframes with inputs needed for creating MXN_OIS and 
        MXN_TIIE curves
    crvUSDOIS: curve
    crvUSDSOFR: curve
    crvMXNOIS: curve
    crvMXNTIIE: curve
    
    
    
    Methods
    -------
    KRR_curves(df, crvDiscount)
        generates a dictionary with swpa_rate_curves and crvDiscounts for every
        tenor given in df
    flat_dv01_engines(dic_data, crvSWP, crvDiscount, interpolation)
        generates engines for plus and minus quotes
    
    
    """
    def __init__(self, dic_data: dict, picklename: str = None, 
                 historical: list = []):
        """ Main Attributes definition
        

        Parameters
        ----------
        dic_data : dict
            input dictionary with datafranmes for mxn curves construction

        Returns
        -------
        None.

        """
        self.historical = historical
        self.dic_data = dic_data.copy()
        if not historical:
            
            if not picklename:
            
                self.crvUSDOIS = USD_OIS(self.dic_data['USD_OIS'], 'Linear')
                self.crvUSDOIS.bootstrap()
                self.crvUSDSOFR = USD_SOFR(self.dic_data['USD_SOFR'], 'Cubic', 
                                        self.crvUSDOIS)
                self.crvUSDSOFR.bootstrap()
                
            else:
                loadCrvs = load_obj(picklename)
                
                self.crvUSDOIS = USD_OIS.from_nodes(tuple(loadCrvs['crvUSDOIS']))
                self.crvUSDOIS.bootstrap()
                self.crvUSDSOFR = USD_SOFR.from_nodes(tuple(loadCrvs['crvSOFR']))   
                self.crvUSDSOFR.bootstrap()
                
            self.crvMXNOIS =\
                MXN_OIS(self.dic_data, 'Cubic', self.crvUSDOIS, self.crvUSDSOFR)
            self.crvMXNOIS.bootstrap()
            
            self.crvMXNTIIE =\
                MXN_TIIE(self.dic_data['MXN_TIIE'], 'Cubic', self.crvMXNOIS)
            self.crvMXNTIIE.bootstrap()
            
        else:
            if 'MXN_OIS' in historical and 'MXN_TIIE' in historical:
            
                self.crvMXNOIS =\
                    MXN_OIS(self.dic_data, 'Cubic', None, None, True)
                self.crvMXNOIS.bootstrap()
                self.crvUSDOIS = None
                self.crvUSDSOFR = None
            
                self.crvMXNTIIE =\
                    MXN_TIIE(self.dic_data['MXN_TIIE'], 'Cubic',
                             self.crvMXNOIS, True)
                self.crvMXNTIIE.bootstrap()
                
            elif 'MXN_OIS' in historical:
                self.crvMXNOIS =\
                    MXN_OIS(self.dic_data, 'Cubic', None, None, True)
                self.crvMXNOIS.bootstrap()
                self.crvUSDOIS = None
                self.crvUSDSOFR = None
                self.crvMXNTIIE =\
                    MXN_TIIE(self.dic_data['MXN_TIIE'], 'Cubic',
                             self.crvMXNOIS)
                self.crvMXNTIIE.bootstrap()
            
            else:
                self.crvUSDOIS = USD_OIS(self.dic_data['USD_OIS'], 'Linear')
                self.crvUSDOIS.bootstrap()
                self.crvUSDSOFR = USD_SOFR(self.dic_data['USD_SOFR'], 'Cubic', 
                                        self.crvUSDOIS)
                self.crvUSDSOFR.bootstrap()
                self.crvMXNOIS =\
                    MXN_OIS(self.dic_data, 'Cubic', self.crvUSDOIS,
                            self.crvUSDSOFR)
                self.crvMXNOIS.bootstrap()
                self.crvMXNTIIE =\
                    MXN_TIIE(self.dic_data['MXN_TIIE'], 'Cubic',
                             self.crvMXNOIS, True)
                self.crvMXNTIIE.bootstrap()
                
            
        
        self.crvMXNTIIE.complete_ibor_tiie =\
            self.crvMXNTIIE.complete_ibor_index()
        
        
        

    
    
    def change_tiie(self, dftiie: pd.DataFrame) -> None:
        """Changes the MXN_TIIE sheet in dic_data and updates curves
        

        Parameters
        ----------
        dftiie : pd.DataFrame
            New MXN_TIIE dataframe for dic_data dictionary
        

        Returns
        -------
        None.

        """
        
        self.dic_data['MXN_TIIE'] = dftiie
        if self.historical and 'MXN_OIS' in self.historical:
            self.crvMXNOIS = MXN_OIS(self.dic_data, 'Cubic', self.crvUSDOIS, 
                                     self.crvUSDSOFR, self.historical)
        else:
            self.crvMXNOIS = MXN_OIS(self.dic_data, 'Cubic', self.crvUSDOIS, 
                                     self.crvUSDSOFR)
        self.crvMXNOIS.bootstrap()
        self.crvMXNTIIE = MXN_TIIE(dftiie, 'Cubic', self.crvMXNOIS)
        self.crvMXNTIIE.bootstrap()
        self.crvMXNTIIE.complete_ibor_tiie =\
            self.crvMXNTIIE.complete_ibor_index()
            
    
    def to_gcrvs(self):
        
        self.KRR_crvs(False, True)
        
        brCrvs = {k:[v[0].curve, v[1].curve] for k, v in self.KRR_curves.items()}
        
        g_crvs = (self.crvUSDOIS.curve, self.crvUSDSOFR.curve, 
                  self.crvMXNOIS.curve, self.crvMXNTIIE.curve, 
                  brCrvs, 'DESK')
        
        g_engines = (self.crvMXNTIIE.complete_ibor_tiie, 
                     self.crvMXNOIS.discount_engine)
        
        self.flat_dv01_engines(True)
        dv01_engines = tuple(self.dv01_engines)
        
        # Bid y Offer
        
        b_quote = self.dic_data['MXN_TIIE']['FMX Desk BID'].values
        o_quote = self.dic_data['MXN_TIIE']['FMX Desk OFFER'].values
        
        b_dftiie = self.dic_data['MXN_TIIE'].copy()
        o_dftiie = self.dic_data['MXN_TIIE'].copy()
        b_dftiie['Quotes'] = b_quote
        
        self.change_tiie(b_dftiie)
        ibor_tiie_bid = self.crvMXNTIIE.complete_ibor_tiie
        tiie_swp_engine_bid = self.crvMXNOIS.discount_engine
        

        o_dftiie['Quotes'] = o_quote
        
        self.change_tiie(o_dftiie)
        ibor_tiie_offer = self.crvMXNTIIE.complete_ibor_tiie
        tiie_swp_engine_offer = self.crvMXNOIS.discount_engine
        
        bo_engines = (ibor_tiie_bid, tiie_swp_engine_bid,
                      ibor_tiie_offer, tiie_swp_engine_offer)
        
        
        return g_crvs, g_engines, dv01_engines, bo_engines
        
        
        
        
        
 
        
            
        



#-------------------
#  tiie Swap Class
#-------------------

class tiieSwap():
    """ MXN, curves, KRR curves and engines and dv01 engines
    
    ...
    
    Attributes
    ----------
    
    start: datetime
        Start date of the swap
    end: datetime
        End date of the swap
    notional: float
        Notional for valuating the swap
    initial_rate: float
        Rate for the trade
    curves: mxn_curves
        Curves for valuating the swap
    rule: int, default = ql.DateGeneration.Backward 
        Cashflow schedule rule
    initial_npv: float, default = 0
        When different to 0, it is used to calculate the notional for the swap
    initial_dv01: float, default = 0
        When different to 0, it is used to calculate the notional for the swap
    
        

    
    Methods
    -------
    KRR_curves(df, crvDiscount)
        generates a dictionary with swpa_rate_curves and crvDiscounts for every
        tenor given in df
    flat_dv01_engines(dic_data, crvSWP, crvDiscount, interpolation)
        generates engines for plus and minus quotes
    
    
    """
    def __init__(self, start: datetime, end: datetime, initial_notional: float,
                 initial_rate: float, curves: mxn_curves, 
                 rule: int = ql.DateGeneration.Backward, 
                 initial_npv: float = 0, initial_dv01: float = 0) -> None:
        """
        

        Parameters
        ----------
        start : datetime
            DESCRIPTION.
        end : datetime
            DESCRIPTION.
        initial_notional : float
            DESCRIPTION.
        initial_rate : float
            DESCRIPTION.
        curves : mxn_curves
            DESCRIPTION.
        rule : int, optional
            DESCRIPTION. The default is ql.DateGeneration.Backward.
        initial_npv : float, optional
            DESCRIPTION. The default is 0.
        initial_dv01 : float, optional
            DESCRIPTION. The default is 0.

        Returns
        -------
        None
            DESCRIPTION.

        """
        
        
        self.start = start
        self.maturity = end
        self.curves = curves
        self.qlstart = ql.Date().from_date(self.start)
        self.qlmaturity = ql.Date().from_date(self.maturity)
        self.discount_curve = curves.crvMXNOIS
        self.discount_engine = self.discount_curve.discount_engine
        self.float_rate_curve = curves.crvMXNTIIE
        self.crvMXNTIIE = curves.crvMXNTIIE
        self.ibor_index = self.float_rate_curve.complete_ibor_tiie
        self.calendar = ql.Mexico()
        self.initial_npv = initial_npv
        self.initial_dv01 = initial_dv01
        self.rule = rule
        self.rate = self.rate_calc(initial_rate)
        self.notional = self.notional_calc(initial_notional)
        
        swap, self.cpn_dates = self.qlswap(self.qlstart, self.qlmaturity,
                                           self.notional, self.rate, 
                                           self.typ, self.rule, 
                                           self.ibor_index)
        swap.setPricingEngine(self.discount_engine)
        self.swap = swap
       
        
        
    def NPV(self):
        if hasattr(self, 'NPV_'):
            return self.NPV_
        else:
            NPV = self.swap.NPV()
            self.NPV_ = NPV
            return NPV
    
    def fairRate(self):
        if hasattr(self, 'fairRate_'):
            return self.fairRate_
        else:
            fairRate = self.swap.fairRate()
            self.fairRate_ = fairRate
            return fairRate
    
    def flat_dv01(self):
        if hasattr(self, 'flat_dv01_'):
            return self.flat_dv01_
        else:
            flat_dv01 = self.flat_dv01_calc(self.qlstart, self.qlmaturity, 
                                                 self.notional, self.rate, 
                                                 self.typ, self.rule)
            self.flat_dv01_ = flat_dv01
            return flat_dv01
        
    def KRR(self):
        if hasattr(self, 'KRR_'):
            return self.KRR_
        else:
            KRR = self.KRR_calc()
            self.KRR_ = KRR
            return KRR
            
    
    def qlswap(self, start: ql.Date(), maturity: ql.Date(), notional: float, 
               rate: float , typ: int, rule: float, ibor_index):
        cal = self.calendar
        self.legDC = ql.Actual360()
        self.cpn_tenor = ql.Period(13)
        self.convention = ibor_index.businessDayConvention()
        self.termDateConvention = ibor_index.businessDayConvention()
        isEndOfMonth = False
        
        # fix-float leg schedules
        self.fixfltSchdl = ql.Schedule(start, maturity, self.cpn_tenor,
                                       cal, self.convention, self.termDateConvention, 
                                       rule, isEndOfMonth)
        
        # swap
        swap = ql.VanillaSwap(typ, notional, self.fixfltSchdl, 
                                   rate, self.legDC, self.fixfltSchdl,
                                   ibor_index, 0, self.legDC)
        
        cpn_dates =\
            [ibor_index.fixingDate(x) for x in self.fixfltSchdl][:-1]
        
        return [swap, cpn_dates]
     
    def rate_calc(self, initial_rate):
        
        if initial_rate == 0:
            notional = 100000000
            typ = -1
            rule = self.rule
            start = ql.Date().from_date(self.start)
            maturity = ql.Date().from_date(self.maturity)
            rate = 0.5
            swap, cpn_dates = self.qlswap(start, maturity, notional, rate, typ, 
                                          rule, self.ibor_index)
            swap.setPricingEngine(self.discount_engine)
            rate = swap.fairRate()
        else:
            if initial_rate > 1:
                
                rate = initial_rate/100
            else: 
                rate = initial_rate
        
        return rate
            
    def flat_dv01_calc(self, start: ql.Date(), maturity: ql.Date(), 
                       notional: float, rate: float, typ: int, rule: int):
        
        if not hasattr(self.curves, 'dv01_engines'):
            self.curves.flat_dv01_engines(inplace = True)
            self.dv01_engines = self.curves.dv01_engines
        else:
            self.dv01_engines = self.curves.dv01_engines
            
        # Plus side
        ibor_tiie_plus = self.dv01_engines[0]
        tiie_swp_engine_plus = self.dv01_engines[1]
        swap, cpn_dates = self.qlswap(start, maturity, abs(notional), 
                                      rate, typ, rule, ibor_tiie_plus)
        swap.setPricingEngine(tiie_swp_engine_plus)
        
        npv_plus = swap.NPV()
        
        # Minus 1bps swap
        ibor_tiie_minus = self.dv01_engines[2]
        tiie_swp_engine_minus = self.dv01_engines[3]
        
        swap, cpn_dates = self.qlswap(start, maturity, abs(notional), 
                                  rate, typ, rule, ibor_tiie_minus )
        swap.setPricingEngine(tiie_swp_engine_minus)
        
        # NPV minus
        npv_minus = swap.NPV()  
        
        # DV01
        npv_dv01 = (npv_plus - npv_minus) / 2
        
        return npv_dv01
            
        
    def notional_calc(self, initial_notional):
        
        if self.initial_npv == 0 and self.initial_dv01 == 0:
            notional = initial_notional
        
        
            
        
        elif self.initial_dv01 == 0:
            notional_ini = 100000000
            typ = -1
            rule = self.rule
            start = ql.Date().from_date(self.start)
            maturity = ql.Date().from_date(self.maturity)
            rate = self.rate
            
            swap, cpn_dates = self.qlswap(start, maturity, notional_ini, rate, 
                                          typ, rule)
            swap.setPricingEngine(self.discount_engine)
            npv_100mn = swap.NPV()
            notional = (self.initial_npv*100000000) / npv_100mn
        
        else: 
            notional_ini = 100000000
            typ = -1
            rule = self.rule
            start = ql.Date().from_date(self.start)
            maturity = ql.Date().from_date(self.maturity)
            rate = self.rate
            
            dv01_100mn = self.flat_dv01_calc(start, maturity, notional_ini, 
                                             rate, typ, rule)
            notional = (self.initial_dv01*100000000) / dv01_100mn
        
        if notional > 0:
            self.typ = -1
        else:
            self.typ = 1
            
        
        
        return abs(notional)
    
    
    def KRR_calc(self):
        
        modNPV = {}
        if not hasattr(self.curves, 'KRR_curves'):
            self.curves.KRR_crvs(inplace = True)
            self.KRR_curves = self.curves.KRR_curves
        else:
            self.KRR_curves = self.curves.KRR_curves
            
        
        # Tenor risk
        for tenor in self.KRR_curves.keys():
            # new yieldcurves
            discCrv, forcCrv = self.KRR_curves[tenor]

            
            # disc-forc engines 
            discEngine = discCrv.discount_engine
            ibor_tiie = forcCrv.ibor_index
            swap_list = []
            swap, cpn_dates = self.qlswap(self.qlstart, self.qlmaturity, 
                                          self.notional, self.rate, 
                                          self.typ, self.rule, ibor_tiie)     
            swap.setPricingEngine(discEngine)  
            swap_list.append(swap.NPV())
            
            modNPV[tenor] = swap_list
            
        df_modNPV = pd.DataFrame(modNPV, index = [1])

        brTenors = self.crvMXNTIIE.tenors
        df_tenorDV01 = pd.DataFrame(None, index = [1])
        
        # DataFrame with risk by tenor
        for tenor in brTenors:
            df_tenorp1 = df_modNPV[tenor+'+1']
            df_tenorm1 = df_modNPV[tenor+'-1']
            df_deltap1 = df_tenorp1 - self.NPV()
            df_deltam1 = df_tenorm1 - self.NPV()
            df_signs = np.sign(df_deltap1)
            df_tenorDV01[tenor] =\
                df_signs * (abs(df_deltap1)+abs(df_deltam1)) / 2

        
            
        return (df_tenorDV01)
        
    @staticmethod
    def get_step_rate(d: datetime.date, scenario_df: pd.DataFrame) -> float:
        """ Get Step Rate
        

        Parameters
        ----------
        d : datetime.date
            Date of fixing.
        scenario_df : pd.DataFrame
            Datarame of Banxico Monetary Scenario.

        Returns
        -------
        float
            float with the desired rate

        """
        # print(d)
        banxico_date = [bd for bd in scenario_df['Fix_Date'] if d>=bd][-1]
        rate = scenario_df[scenario_df['Fix_Date']==banxico_date]['Rate'].values[0]
        return rate/100
    
    
    def step_FRA(self, flt_scenario: pd.DataFrame) -> pd.DataFrame:
        """ STEP FRA
        

        Parameters
        ----------
        flt_scenario : pd.DataFrame
            Datarame of Banxico Monetary Scenario.

        Returns
        -------
        df_fras : pd.DataFrame
            DESCRIPTION.

        """
        
        evaluation_date = ql.Settings.instance().evaluationDate.to_date()
        
        banxico_dates = flt_scenario[flt_scenario.columns[0]].tolist()
        rates = flt_scenario[flt_scenario.columns[2]].tolist()
        fix_banxico_dates = flt_scenario[flt_scenario.columns[1]].tolist()
        
        scenario_rates = pd.DataFrame(
            {'Fix_Date':fix_banxico_dates, 'Rate': rates})
        
        rates = flt_scenario[flt_scenario.columns[1]].tolist()
        banxico_TIIE28 = self.curves.crvMXNTIIE.banxico_TIIE28
        start_dates = [c.accrualStartDate().to_date() for c in 
                       map(ql.as_coupon ,self.swap.leg(0))]
        end_dates = [c.accrualEndDate().to_date() for c in 
                       map(ql.as_coupon ,self.swap.leg(0))]
        fix_dates = [d.to_date() for d in self.cpn_dates]
        
        prev_start_dates = [d for d in start_dates if d <= evaluation_date]
        prev_end_dates = end_dates[:len(prev_start_dates)]
        
        next_start_dates = [d for d in start_dates if d > fix_banxico_dates[-1]]
        
        
        if len(next_start_dates) > 0:
            step_fix_dates = fix_dates[
                len(prev_start_dates): -len(next_start_dates)]
            step_end_dates = end_dates[
                len(prev_start_dates): -len(next_start_dates)]
        else:
            step_fix_dates = fix_dates[len(prev_start_dates):]
            
            step_end_dates = end_dates[len(prev_start_dates):]
        
        next_end_dates = end_dates[len(prev_end_dates)+len(step_end_dates):]
        
        prev_fras = \
            banxico_TIIE28[
                banxico_TIIE28['fecha'].isin(prev_start_dates)
                ].sort_values(by='fecha')['dato'].tolist()
        mxntiie = self.curves.crvMXNTIIE.curve
        next_fras = [mxntiie.forwardRate(
            ql.Date().from_date(next_start_dates[d]), 
            ql.Date().from_date(next_end_dates[d]), 
            ql.Actual360(), ql.Simple).rate() 
            for d in range(len(next_start_dates))]
        
        # tiie28 = self.curves.dic_data['MXN_TIIE'].iloc[0]['Quotes']/100
        
        step_fras = [self.get_step_rate(d, scenario_rates) 
                     for d in step_fix_dates]
        
        fras = prev_fras + step_fras + next_fras
        mxnois = self.curves.crvMXNOIS.curve
        dfs = [mxnois.discount(ql.Date().from_date(end_dates[p])) 
                  for p in range(0, len(end_dates)) if 
                  end_dates[p] > evaluation_date]
        prev_dfs = [0]*(len(end_dates)-len(dfs))
        
        dfs = prev_dfs + dfs
        
        df_fras = pd.DataFrame({'Fix_Date': fix_dates,'Start_Date': start_dates,
                                'End_Date': end_dates, 'FRA': fras, 'DF': dfs})
        
        return df_fras
     
    
    
    def step_NPV(self, flt_scenario: pd.DataFrame):
        
        df_fras = self.step_FRA(flt_scenario)
        
        df_fras['Days'] = (df_fras['End_Date']-df_fras['Start_Date']).dt.days
        df_fras['Fix_Amnt'] = [-1*self.typ*c.amount() for c in self.swap.leg(0)]
        df_fras['Flt_Amnt'] = self.typ*self.notional*df_fras.FRA*df_fras.Days/360     
        
        df_fras['Net_Amnt'] = df_fras.Fix_Amnt + df_fras.Flt_Amnt
        df_fras['NPV'] = df_fras['Net_Amnt']*df_fras.DF
        
        npv = df_fras.NPV.sum()
        
        return npv



#------------------------------
#  input dictionary functions
#------------------------------



def import_data(str_file: str, s_names: list = []) -> dict:
    """ Import data from the files
    

    Parameters
    ----------
    str_file : str
        String of the file inputs
    s_names : list, optional
        sheet_names of the file. The default is [].

    Returns
    -------
    dict
        

    """
    if not s_names:
        tmpxls = pd.ExcelFile(str_file)
        s_names = tmpxls.sheet_names
    dic_data = {}
    for sheet in s_names:
        dic_data[sheet] = pd.read_excel(str_file, sheet)
    tmpxls.close()

    return dic_data

def futures_check(dic_data: dict, str_file: str) -> dict:
    """Checks the futures updated futures IMM
    

    Parameters
    ----------
    dic_data : dict
        Dictionary with Quotes from file
    str_file : str
        Name of the file

    Returns
    -------
    dict
        dictionary with file and sheets

    """
    
    
    dt_settlement = ql.UnitedStates(1).advance(
            ql.Settings.instance().evaluationDate,ql.Period('2D'))
    
    tday_year = str(dt_settlement.year())
    tday_year_end = str(dt_settlement.year())[-1]
    
    
    dic_months = {'M' : 6, 'U': 9, 'Z' : 12, 'H': 3}
    df = dic_data['USD_SOFR']
    idx_nonfut = (df['Types'] != 'FUT')
    # input data
    data_fut = df[~idx_nonfut]
    month = dic_months[data_fut['Tenors'].iloc[0][3]]
    
    year_end = data_fut['Tenors'].iloc[0][-1]
    
    if year_end != '0' or tday_year_end == year_end:
    
        year = int(str(tday_year)[:3]+year_end)
    
    else:
        next_decade = str(int(str(tday_year)[:3])+1)
        year = int(next_decade+year_end)
    
    
    imm0 = ql.IMM.nextDate(ql.Date(1, month, year))
    imm = ql.IMM.nextDate(imm0)
    
    if imm <= dt_settlement:
        print('\nPlease update futures in USD_SOFR sheet in the '
              'TIIE_CurveCreate Inputs.xlsx located in TIIE IRS Valuation Tool'
              '/Main Codes/Potfolio Management folder and click save and make '
              'sure the db_Curves_mkt.xlsx file located in tiie shared folder '
              'is updated with all current futures.')
        
        fut_flag = input('When done press "c": ')
        while fut_flag != 'c':
            
            fut_flag = input('Please press "c": ') 
            
        dic_data['USD_SOFR'] = pd.read_excel(str_file, 'USD_SOFR')
        
    return dic_data

# DATA UPDATE
def pull_data(str_file: str, dt_today: datetime) -> dict:
    """ Gets updated data fro db_curves 
    

    Parameters
    ----------
    str_file : str
        String of input file
    dt_today : datetime
        today date

    Returns
    -------
    dic_data : TYPE
        dictionary with the curve inputs

    """
    
    dic_data = import_data(str_file)
    
    dic_data = futures_check(dic_data, str_file)
    
    db_cme = pd.read_excel(r'//TLALOC/tiie/db_cme' + r'.xlsx', 'db')\
        .set_index('TENOR')
    db_cme.columns = db_cme.columns.astype(str)
    db_crvs = pd.read_excel(r'//TLALOC/tiie/db_Curves_mkt' + r'.xlsx', 
                            'bgnPull', 
                            skiprows=3).drop([0,1]).\
        reset_index(drop=True).set_index('Date')
    # # USD Curves Data
    datakeys = ['USD_OIS', 'USD_SOFR']
    for mktCrv in datakeys:
        dic_data[mktCrv]['Quotes'] = \
            db_crvs.loc[str(dt_today)+' 00:00:00',dic_data[mktCrv]['Tickers']].\
                fillna(method="ffill").values
    # # MXN Curves Data
    cmenames_mxnfwds = ['FX.USD.MXN.ON', 'FX.USD.MXN.1W', 'FX.USD.MXN.1M', 
                        'FX.USD.MXN.2M', 'FX.USD.MXN.3M', 'FX.USD.MXN.6M', 
                        'FX.USD.MXN.9M', 'FX.USD.MXN.1Y']
    dic_data['USDMXN_Fwds']['Quotes'] = \
        db_cme.loc[cmenames_mxnfwds, str(dt_today) + ' 00:00:00'].values
        
    dic_data['USDMXN_XCCY_Basis']['Quotes'][0] = \
        db_cme.loc['FX.USD.MXN', str(dt_today) + ' 00:00:00']
    
    dic_data['USDMXN_XCCY_Basis']['Quotes'][-9:] = \
        db_cme.iloc[-9:, 
                    np.where(
                        db_cme.columns == str(dt_today) + ' 00:00:00')[0][0]].\
            values*100
    
    dic_data['MXN_TIIE']['Quotes'] = \
        db_cme.iloc[:14, 
                    np.where(
                        db_cme.columns == str(dt_today) + ' 00:00:00')[0][0]].\
                        values 
    dic_data['USD_SOFR']['Quotes'].iloc[1:6] =\
        100 - dic_data['USD_SOFR']['Quotes'].iloc[1:6]
            
    return dic_data


def granular(curves: mxn_curves) -> pd.DataFrame:
    """ make granular dftiie
    

    Parameters
    ----------
    curves : mxn_curves
        DESCRIPTION.

    Returns
    -------
    TYPE
        DESCRIPTION.

    """
    
    tenors = ['%1B', '%1W', '%1L', '%2L', '%3L', '%4L', '%5L', '%6L', '%9L',
            '%13L', '%19L', '%26L', '%39L', '%52L', '%65L', '%78L', '%91L',
            '%104L', '%117L', '%130L', '%143L', '%156L', '%195L', '%260L',
            '%390L']
    dftiie = add_tiie_tenors(curves, tenors)
    
    return  dftiie.copy()


def add_tiie_tenors(curves: mxn_curves, tenors: list,
                    replace: bool = False) -> pd.DataFrame:
    """ makes new dftiie with new tenors
    

    Parameters
    ----------
    curves: mxn_curves
        mxn_curves object with the curves to change
        
    tenors: list
        list of tenors to add, they can be '{number}B': days, '{number}D': days, 
        '{number}W': weeks, '{number}L': 4 weeks, '{number}Y': 364 days
        
    replace: bool
        If True, replaces existing tenors with the new calculated ones,
        if False, only adds new tenors

    Returns
    -------
    dftiie 

    """
    tenors = pd.Series(tenors)

    df_tiie = curves.dic_data['MXN_TIIE'].copy()
    existing_tenors = df_tiie['Tenor'].tolist()
    

    tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                'Y': ql.Years}
    tenor_type = tenors.str[-1].map(tenor2ql).tolist()
    
    def f(x):
        if x[-1]=='L':
            return int(x[1:-1])*4
        else:
            return int(x[1:-1])
        
    period = tenors.map(lambda x: f(x)).tolist()
    
    valuation_date = ql.Settings.instance().evaluationDate
    start = ql.Mexico().advance(valuation_date, ql.Period('1D'))
    
    maturities = [start + ql.Period(period[i], tenor_type[i]) 
                  for i in range(len(tenors))]
    quotes = []
    
    for t in range(len(tenors)):
        
        if tenors[t] in existing_tenors and not replace:
            quotes.append(np.nan)
        
        
        
        elif replace:
            swap = tiieSwap(start.to_date(), maturities[t].to_date(), 
                            1000000000, 0.09, curves)
            
            quotes.append(swap.fairRate()*100)
        
        else:
            df_tiie['Quotes'][df_tiie['Tenor'] == tenors[t]] = np.nan
            
            swap = tiieSwap(start.to_date(), maturities[t].to_date(), 
                            1000000000, 0.09, curves)
            
            quotes.append(swap.fairRate()*100)
    
    granular_tiie = pd.DataFrame({'Tenor': tenors, 
                                  'Period': tenors\
                                      .map(lambda x: x[1:-1]).astype(int), 
                                  'Quotes': quotes})
    if not replace:
        df_tiie_extended = granular_tiie.merge(df_tiie[['Tenor', 'Period',
                                                        'Quotes']], 
                                               how = 'outer', left_on = 'Tenor', 
                                               right_on = 'Tenor')
        
        
        df_tiie_extended['Quotes'] = df_tiie_extended['Quotes_y'].fillna(0)\
            + df_tiie_extended['Quotes_x'].fillna(0)
            
        df_tiie_extended['Period'] = df_tiie_extended.Tenor\
            .map(lambda x: x[1:-1]).astype(int)
            
        
        df_tiie_extended.sort_values(by = 'Period', inplace = True)
        df_tiie_extended.reset_index(inplace = True )
        df_tiie_extended.drop(columns = ['index', 'Quotes_x', 'Quotes_y',
                                         'Period_x', 'Period_y'], inplace = True)
    
    else:
        df_tiie_extended = granular_tiie.copy()
    
        df_tiie_extended['Period'] = df_tiie_extended.Tenor\
            .map(lambda x: x[1:-1]).astype(int)
            
        
        df_tiie_extended.sort_values(by = 'Period', inplace = True)
        df_tiie_extended.reset_index(inplace = True )
    
    
    return  df_tiie_extended.copy()
    



#%%
if __name__ == '__main__':
    ql.Settings.instance().evaluationDate = ql.Date(27,10,2023)
    
    dic_data = pull_data('//tlaloc/Cuantitativa/Fixed Income/'+
                         'TIIE IRS Valuation Tool/Main Codes/'+
                         'Quant Management/OOP codes/'+
                         'TIIE_CurveCreate_Inputs.xlsx', datetime(2023,10,26).date())

    curvas1 = mxn_curves(dic_data)
    
    gran = granular(curvas1)
    
    curvas1.change_tiie(gran)
    
    dftiie2 = add_tiie_tenors(
        curvas1, dic_data['MXN_TIIE']['Tenor'].tolist() + ['%156L'], True)
                              


    
    # g_crvs, g_engines, dv01_engines, bo_engines = curvas1.to_gcrvs()
    #%%
    # swp = tiieSwap(datetime(2023,10,30), datetime(2024,12,5), 100, 0, curvas1)
    
    # npv_leg0 = swp.swap.legNPV(0)

    # ytm = 10.24
    # coupon = 10
    # tdy = datetime(2023,10,30)
    # maturity = datetime(2024,12,5)
    
    # dtm = (maturity - tdy).days
    # n_coupon = np.ceil(dtm/182)
    # accrued = -dtm%182
    
    # DirtyPrice = coupon*182/360*((1-(1/(1+ytm*182/36000)**(n_coupon)))/
    #                              (1-1/(1+ytm*182/36000)))/(1+ytm*182/36000)**(1-accrued/182)
    
 
    
    # dates_bono = [(d, ql.Date.from_date(tdy) + d) for d in range(182-accrued, dtm+182, 182)]
    
    # npv_zero = []
    # zeros = []
    # for dat in dates_bono:
    #     zero = curvas1.crvMXNTIIE.curve.zeroRate(dat[1], ql.Actual360(),
    #                                              ql.Simple, ql.Annual).rate()
        
    #     zeros.append(zero)
        
    #     npv = (coupon*182/360)/(1+dat[0]*(zero-.0025)/360)   
        
    #     npv_zero.append(npv)
    
    # npv_zero_s = sum(npv_zero)
    

    # from scipy.optimize import minimize, rosen, rosen_der
    
    # def pric(ytm, coupon, tdy, maturity, snpv_zero):
    #     tdy = datetime(2023,10,30)
    #     maturity = datetime(2024,12,5)
        
    #     dtm = (maturity - tdy).days
    #     n_coupon = np.ceil(dtm/182)
    #     accrued = -dtm%182
        
    #     DirtyPrice = coupon*182/360*((1-(1/(1+ytm*182/36000)**(n_coupon)))/
    #                                  (1-1/(1+ytm*182/36000)))/(1+ytm*182/36000)**(1-accrued/182)
        
    #     return (DirtyPrice - snpv_zero)**2
    
    # ytm1 = minimize(pric, ytm, args=(coupon, tdy, maturity, npv_zero_s))
   
   
    # def price(ytm, coupon, tdy, maturity):
        
    #     tdy = datetime(2023,10,30)
    #     maturity = datetime(2024,12,5)
        
    #     dtm = (maturity - tdy).days
    #     n_coupon = np.ceil(dtm/182)
    #     accrued = -dtm%182
        
    #     DirtyPrice = coupon*182/360*((1-(1/(1+ytm*182/36000)**(n_coupon)))/
    #                                  (1-1/(1+ytm*182/36000)))/(1+ytm*182/36000)**(1-accrued/182)
        
    #     return DirtyPrice
    
    # new_price = price(ytm1.x[0], coupon, tdy, maturity)
    
    
    # print('First Price: ', DirtyPrice*1000, '\nYTM: ', ytm,
    #       '\nSwap leg0NPV: ', npv_leg0*1000, '\nZero Price: ', npv_zero_s*1000,
    #       '\nNew Price: ', new_price*1000, '\nNew YTM: ', ytm1.x[0])
    
    # def comparable_rate(rate, m, p):
    #     rate_p = p*((1+rate/m)**(m/p)-1)
    #     return rate_p

    
    
    
    
