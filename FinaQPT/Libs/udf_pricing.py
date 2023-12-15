"""Pricing functions

This script contains the functions needed to structure the pricing tool.

This script requires `QuantLib`, `pandas`, `pickle`, `numpy`, `datetime`
`time`, `os`, and `xlwings` be installed within the Python environment 
this script is being run and the user-defined `Funtions` module.

This file can also be imported as a module ("pr" is conventional) and 
contains the following functions:
    
    * save_obj - Saves object to pickle file
    * load_obj - Loads object from pickle file
    * CreateCurves - Creates all curves needed for bootstrapping TIIE
    * banxicoData - Downloads the TIIE28 quotes from the last year
    * engines - Creates the engines for the DiscountCurves given
"""
import os
import sys
import time
import pickle
import numpy as np
import pandas as pd
import datetime as dt
from datetime import timedelta, datetime, date
import xlwings as xw
import QuantLib as ql
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pdfplumber
from scipy.optimize import minimize
from scipy.interpolate import interp1d
import requests
import pyodbc

main_path = '//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/' +\
    'Main Codes/Pricing/'
sys.path.append(main_path)
import Funtions as fn

main_path_oop = '//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/' +\
    'Main Codes/Portfolio Management/OOP codes/'
    
sys.path.append(main_path_oop)
import new_pfolio_funs as pf
import curve_funs as cf

colors = ['firebrick', 'darkred', 'orangered', 'darkorange', 'olivedrab',
          'darkolivegreen', 'forestgreen', 'limegreen', 'green', 'seagreen',
          'lightseagreen', 'darkslategray', 'teal', 'dodgerblue', 'royalblue', 
          'blue', 'rebeccapurple', 'indigo', 'darkviolet', 'purple', 
          'mediumvioletred', 'crimson']
#--------------------------
#  Save object functions 
#--------------------------



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
    # with open(name + '.pickle', 'rb') as f:
    return pd.read_pickle(name + '.pickle')
    
#-----------------------
#  File check Function
#-----------------------

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
        print('\nPlease update futures in USD_SOFR sheet and click save.')
        fut_flag = input('When done press "c": ')
        while fut_flag != 'c':
            
            fut_flag = input('Please press "c": ') 
            
        dic_data['USD_SOFR'] = pd.read_excel(str_file, 'USD_SOFR')
        
    return dic_data


def read_dic_data(str_file, evaluation_date, flag):
    
    input_sheets = ['USD_OIS', 'USD_SOFR', 'USDMXN_XCCY_Basis', 'USDMXN_Fwds', 
                    'MXN_TIIE', 'Granular_Tenors']
    dic_data = {}

    # Saves every input sheet in the excel as DataFrame
    for sheet in input_sheets:
        dic_data[sheet] = pd.read_excel(str_file, sheet)
        
        if sheet != 'Granular_Tenors':
            if (dic_data[sheet]['Quotes'].isna().any() or
                dic_data[sheet]['Quotes'].apply(
                    lambda x: type(x) != float).any()):
                print(f'\nPLEASE CHECK YOUR INPUTS FOR SHEET {sheet}')
                done = 'd'
                while done != 'c':
                    done = input('When Done press "c" and save: ')
                    
                    if done == "c":
                        dic_data[sheet] = pd.read_excel(str_file, sheet)
                        
                
                
            
        if sheet == 'USD_SOFR':
            dic_data = futures_check(dic_data, str_file)
            
    if flag != 'Quant':
        save_obj(dic_data, '//tlaloc/Cuantitativa/Fixed Income/'+
                    'TIIE IRS Valuation Tool/Main Codes/Quant Management/'+
                    'Pricing/dic_data/'+
                    f'dic_data_{evaluation_date.strftime("%Y%m%d")}')
    
    else:
        dic_data = copy_dic_data(evaluation_date, str_file)
        
    
    return dic_data





#------------------------------
#  Fill close prices function
#------------------------------
def close_granular(wb: xw.Book, evaluation_date: datetime) -> list:
    """Gets close prices for granular risk.

    Parameters
    ----------
    wb : xw.Book
        Excel file with granular tenors.
    evaluation_date : datetime
        Date of evaluation.

    Returns
    -------
    list
        List with close prices for granular tenors.

    """
    
    yest_date = (ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                     ql.Period(-1, ql.Days))).to_date()
    yest_date_ql = ql.Date().from_date(yest_date)
    try:
        ql.Settings.instance().evaluationDate = yest_date_ql
    except:
        crvUSDOIS = 0
        crvUSDSOFR = 0
        ql.Settings.instance().evaluationDate = yest_date_ql
        
    crvMXNOIS, crvTIIE = fn.historical_curves(yest_date)
    banxico_TIIE28 = banxicoData(yest_date)
    
    tenors_sheet = wb.sheets('Granular_Tenors')
    tenors_range = str(tenors_sheet.range('A2').end('down').row)
    tenors = pd.Series(tenors_sheet.range('A2:A'+tenors_range).value)
    tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                'Y': ql.Years}
    tenor_type = tenors.str[-1].map(tenor2ql).tolist()
    
    def f(x):
        if x[-1]=='L':
            return int(x[1:-1])*4
        else:
            return int(x[1:-1])
        
    period = tenors.map(lambda x: f(x)).tolist()
    
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE)

    ibor_tiie = ql.IborIndex('TIIE',
                 ql.Period(13),
                 1,
                 ql.MXNCurrency(),
                 ql.Mexico(),
                 ql.Following,
                 False,
                 ql.Actual360(),
                 ibor_tiie_crv)
    
    # Add missing fixings
    ibor_tiie.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        ibor_tiie.addFixing(
            ql.Date((pd.to_datetime(banxico_TIIE28['fecha'][h])).day, 
                    (pd.to_datetime(banxico_TIIE28['fecha'][h])).month, 
                    (pd.to_datetime(banxico_TIIE28['fecha'][h])).year), 
            banxico_TIIE28['dato'][h+1])
    
    # Discount Engine Definition
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine = ql.DiscountingSwapEngine(rytsMXNOIS)
    start = ql.Mexico().advance(ql.Date().from_date(yest_date), 
                                ql.Period(1, ql.Days))
    maturities = [start + ql.Period(period[i], tenor_type[i]) 
                  for i in range(len(tenors))]
    quotes = []
    
    for t in range(len(tenors)):
        
        swap = fn.tiieSwap(start, maturities[t], 1000000000, ibor_tiie, 
                           0.09, -1, 0)[0]
        swap.setPricingEngine(tiie_swp_engine)
        quotes.append(swap.fairRate()*100)
    
    return quotes


def close_price(wb: xw.Book, evaluation_date: datetime, 
                gran: bool=False, granular_closes: list=[]) -> None:
    """Fill close prices in TIIE_IRS_Data Excel file.
    

    Parameters
    ----------
    wb : xw.Book
        TIIE_IRS_Data Excel file.
    evaluation_date : datetime
        Evaluation date. Close prices are from the day before.

    Returns
    -------
    None
        Fills T-1 column in Risk sheet of TIIE_IRS_Data Excel file.

    """
    # Close prices for normal risk are taken from Daily Update file.
    if not gran:
    
        ql_date = ql.Date().from_date(evaluation_date)
        
        # Day before evaluation date
        close_date_ql = ql.Mexico().advance(ql_date, ql.Period(-1, ql.Days))
        close_date = datetime(close_date_ql.year(), close_date_ql.month(), 
                              close_date_ql.dayOfMonth())
        str_date = close_date.strftime('%d-%m-%Y')
        
        # Current year to look for Daily Update file
        current_year = str(evaluation_date.year)
        file_name = r'\\TLALOC\Cuantitativa\Fixed Income\IRS Tenors Daily '\
            r'Update\Daily_Update_' + current_year + '.xlsx'
        
        # Look for Daily Update file
        try:
            close_file = pd.read_excel(file_name)
        except:
            print('Daily Update file not found.')
        
        # Look for sheet of specified date
        try:
            close_file = pd.read_excel(file_name, str_date)
        except:
            print(f'Sheet for {close_date} not found.')
            return None, None
    
        # Risk sheet in TIIE_IRS_Data file
        close_sheet = wb.sheets('Risk')    
        tenors = close_sheet.range('K6:K20').value
        close_range = 'L6:L20'
        tenors = [str(int(t))+' x 1' for t in tenors]
        
        # Close prices
        spot_col = close_file.columns[1]
        close_tenors = close_file[
            close_file['Spot'].isin(tenors)][spot_col].values
        close_sheet.range(close_range).value = close_tenors.reshape(-1,1)
        print('\nCloses OK.')
    
    # Granular closes have to be calculated.
    else:
        granular_closes = close_granular(wb, evaluation_date)
        close_sheet = wb.sheets('Granular_Risk')    
        close_range = 'L6:L30'
        close_tenors = np.array(granular_closes)
        close_sheet.range(close_range).value = close_tenors.reshape(-1,1)
        print('\nCloses OK.')
    
    # Tenors in format of Daily Update file
    
    
    # Fill Risk sheet with close prices

    return granular_closes, close_tenors[0]

def sl_df(evaluation_date, wb):
    
    month = evaluation_date.month
    ql_date = ql.Date().from_date(evaluation_date)
    
    days_list_ql = ql.Mexico().businessDayList(ql.Date(1, month, 2023), ql_date)
    days_list = [d.to_date().strftime('%Y%m%d') for d in days_list_ql]
    
    try:
        sl = load_obj(r'//TLALOC/Cuantitativa/Fixed Income/TIIE IRS '\
                      'Valuation Tool/Blotter/Historical Risks/sl_npv' \
                          + evaluation_date.strftime('%Y%m%d'))
    except:
        sl = pd.DataFrame()
        print('Please run TIIE_PfolioMgmt_Risk_book code to get ' +
              'NPVs for each clearing broker.')
        return None
    
    books = sl.index
    sl_mean_dic = {b: pd.DataFrame() for b in books}
    
    for d in days_list:
        try:
            sl_a = load_obj(r'//TLALOC/Cuantitativa/Fixed Income/TIIE IRS '\
                          'Valuation Tool/Blotter/Historical Risks/sl_npv'+d)
            for b in books:
                try:
                    sl_b = sl_a.loc[[b]].rename(index={b: d})
                except:
                    sl_b = pd.DataFrame({'Citi':np.nan, 'GS':np.nan,
                                         'OTC':np.nan, 'Santa Asigna':np.nan,
                                         'Interno':np.nan}, index = [d])
                sl_mean_dic[b] = pd.concat([sl_mean_dic[b], sl_b])
        except:
            print('No file for clearing brokers found for date: ', d)
            
    sl_mean_df = pd.DataFrame(columns = sl.columns)
    
    for b in books:
        sl_mean_df = pd.concat([sl_mean_df, 
                                pd.DataFrame(
                                    sl_mean_dic[b].mean()).T.rename(index={0:b})])
    
    cols_wo_int = [c for c in sl.columns if c != 'Interno']
    sl_wo_int = sl[cols_wo_int]
    risk_sheet = wb.sheets('Risk')
    end_row = risk_sheet.range('AB6').end('down').row
    end_col = risk_sheet.range('AC5').end('right').end('right')\
        .address.split('$')[1]
    risk_sheet.range('AB5:'+end_col+str(end_row)).clear_contents()
    sl_wo_int['Total'] = sl_wo_int.sum(axis=1)
    sl_wo_int.loc['Total'] = sl_wo_int.sum().tolist()
    risk_sheet['AB5'].options(pd.DataFrame, header=1, 
                              index=True, expand='table').value = sl_wo_int
    risk_sheet.range('AB5').value = 'Book'
    sl_w_int = sl[['Interno']]
    sl_w_int.loc['Total'] = sl_w_int.sum().tolist()
    end_col = risk_sheet.range('AC5').end('right').column
    risk_sheet.range(5, end_col+2).options(pd.DataFrame, header=1, index=False,
                                           expand='table').value = sl_w_int
    
    
    cols_wo_int = [c for c in sl_mean_df.columns if c != 'Interno']
    sl_wo_int = sl_mean_df[cols_wo_int]
    risk_sheet = wb.sheets('Risk')
    end_row = risk_sheet.range('AB20').end('down').row
    end_col = risk_sheet.range('AC19').end('right').end('right')\
        .address.split('$')[1]
    risk_sheet.range('AB19:'+end_col+str(end_row)).clear_contents()
    sl_wo_int['Total'] = sl_wo_int.sum(axis=1)
    sl_wo_int.loc['Total'] = sl_wo_int.sum().tolist()
    risk_sheet['AB19'].options(pd.DataFrame, header=1, 
                              index=True, expand='table').value = sl_wo_int
    risk_sheet.range('AB19').value = 'Book'
    sl_w_int = sl_mean_df[['Interno']]
    sl_w_int.loc['Total'] = sl_w_int.sum().tolist()
    end_col = risk_sheet.range('AC19').end('right').column
    risk_sheet.range(19, end_col+2).options(pd.DataFrame, header=1, index=False,
                                           expand='table').value = sl_w_int
    
    
    
    
    return sl
#--------------------------
#  Update Curve Functions
#--------------------------

def createCurves(dic_data: dict, updateAll, flag=True, save=True) -> list:
    """Creates all curves needed for bootstrapping TIIE
    
    Uses DataFrames with the information needed to create the next 
    curves: USDIOS, SOFR, MXNOIS, MXNTIIE
    Also makes the MXNTIIE Bucket Risk Curves.
    
    Parameters
    ----------
    dic_data : dict
        Dictionary with DataFrames
    updateAll : str | bool
        Flag that decides if it will be full a curve update or just a 
        MXNIIE and MXNOIS one.
    flag : str | bool, optional
        If True it will always Update all curves. The default is True.

    Returns
    -------
    list
        List with all the QuantLib object botstraped curves, the Bucket 
        Risk curves and the updated flag.
    
    See Also
    --------
        fn.btstrap_USDOIS: bootstraps the USDOIS curve
        fn.btstrap_USDSOFR: bootstraps the USDSOFR curve
        fn.btstrap_MXNOIS: bootstraps the MXNOIS curve
        fn.btstrap_MXNTIIE: bootstraps the MXNTIIE curve
        save_obj: saves a pickle object
        load_obj: loads a pickle object
        


    """

    # If Update All or flag = True, then it will Update all curves
    if updateAll == True or flag == True:
        print('Updating all curves...')
        
       # It will try bootstrapping all curves
        try:
            crvUSDOIS = fn.btstrap_USDOIS(dic_data)
            
        except:
            print('Please check you have all inputs for USD_OIS curve.')
            
        try:
            crvUSDSOFR = fn.btstrap_USDSOFR(dic_data, crvUSDOIS)
            
        except:
            print('Please check you have all inputs for USD_SOFR curve.')

        crvMXNOIS = fn.btstrap_MXNOIS(dic_data, crvUSDSOFR, crvUSDOIS)
        
        try:
            crvTIIE = fn.btstrap_MXNTIIE(dic_data, crvMXNOIS, updateAll)
            
        except:
            print('Please check you have all inputs for MXN_TIIE curve.')
        
        # Save all curves nodes in a dictionary pickle
        save_Crvs_Quant = {}
        save_Crvs={}
        names=['crvUSDOIS', 'crvSOFR']
        crvs=[crvUSDOIS, crvUSDSOFR]
        dict_crvs_pk=dict(zip(names, crvs))

        try:
            
            for name in names:
                
                # When curve is SOFR first tries saving linear nodes
                if name == 'crvSOFR':
                    
                    try:
                        
                        nodes = fn.btstrap_USDSOFR(dic_data, crvUSDOIS, 
                                                   'Linear').nodes()
                       
                    except:
                        
                        nodes=dict_crvs_pk[name].nodes()

                        
                else:
                    nodes=dict_crvs_pk[name].nodes()
                    
                dates=[dt.datetime(nodes[k][0].year(), nodes[k][0].month(), 
                    nodes[k][0].dayOfMonth()) for k in range(len(nodes))]
                rates = [nodes[k][1] for k in range(len(nodes))]
                save_Crvs[name] = zip(dates, rates)
                save_Crvs_Quant[name] = zip(dates, rates)
                
                
            if save:
                save_obj(save_Crvs, 'InitialCurves')
            
            # Tries to save it in a pickle file
            if save:
                try:
                    save_obj(save_Crvs_Quant, 
                             '//TLALOC/Cuantitativa/Fixed Income/'
                             + 'TIIE IRS Valuation Tool/Quant Team/Esteban '\
                                 'y Gaby/Historical Curves/InitialCurves' 
                             + datetime.today().strftime('%d-%m-%Y'))
                except:
                    pass
            
                flag = False

        except:
            flag = True
            print('Curves could not be saved. '
                  + 'They will be calculated at all times.')
    
    # In other cases, it will only bootstrap MXNOIS and TIIE curves
    else:
        print(f'Updating TIIE: {updateAll}...')
        names=['crvUSDOIS', 'crvSOFR']
        loadCrvs = load_obj('InitialCurves')
        dic_loadCrvs = {}
        
        # Load all curves form the pickle file
        for i in range(len(names)):
            loadCrv = tuple(loadCrvs[names[i]])
            loadDates = [ql.Date(loadCrv[k][0].strftime('%Y-%m-%d'), 
                                 '%Y-%m-%d') for k in range(len(loadCrv))]
            loadRates = [loadCrv[k][1] for k in range(len(loadCrv))]
            
            # For the USDOIS curve, Discount Curve is needed
            if i==0:
                dic_loadCrvs[names[i]] = ql.DiscountCurve(loadDates, loadRates, 
                                                          ql.Actual360(), 
                                                         ql.UnitedStates(1))
            
            # NaturalLogCubic discount in all other cases
            else:
                dic_loadCrvs[names[i]] = \
                    ql.NaturalLogCubicDiscountCurve(loadDates, loadRates, 
                                                    ql.Actual360(), 
                                                    ql.UnitedStates(1))
        
        crvUSDOIS = dic_loadCrvs['crvUSDOIS']
        crvUSDSOFR = dic_loadCrvs['crvSOFR']
        
        crvMXNOIS = fn.btstrap_MXNOIS(dic_data, crvUSDSOFR, crvUSDOIS, 'SOFR')

        
        try:
            crvTIIE = fn.btstrap_MXNTIIE(dic_data, crvMXNOIS, updateAll)
     
        except:
            raise Exception('Please check you have all inputs for '\
                            'MXN_TIIE curve.')
    
    brCrvs = fn.crvTenorRisk_TIIE(dic_data, crvMXNOIS, updateAll, [crvUSDOIS, crvUSDSOFR])
    
    return [crvUSDOIS, crvUSDSOFR, crvMXNOIS, crvTIIE, brCrvs, flag]

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
        
        banxico_TIIE28 = fn.banxico_download_data('SF43783', 
                                                  banxico_start_date, 
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
        banxico_TIIE28 = banxico_TIIE28.drop_duplicates(subset='fecha')
    
        
    return banxico_TIIE28

def engines(crvMXNOIS: ql.DiscountCurve , crvTIIE: ql.DiscountCurve, 
            banxico_TIIE28: pd.DataFrame) -> list:
    '''Creates the engines for the DiscountCurves given
    
    For the MXNOIS curve it makes a DiscountingSwapEngine object and for
    the MXNTIIE curve it makes an IborIndex object with all of the 
    TIIE's characteristics.

    Parameters
    ----------
    crvMXNOIS : ql.DiscountCurve
        A bootstrapped DiscountCurve of MXNOIS
    crvTIIE : ql.DiscountCurve
        A bootstrapped DiscountCurve of TIIE
    banxico_TIIE28 : pd.DataFrame
        DataFrame with dates and quotes of the year to date TIIE28 curve

    Returns
    -------
    list
        IborIndex object with TIIE's characteristics and 
        DiscountingSwapEngine with MXNOIS characteristics
    
    See Also
    --------
        banxicoData: Creates the banxico_TIIE28 Dataframe

    '''
    # Ibor Index definition
    ibor_tiie_crv = ql.RelinkableYieldTermStructureHandle()
    ibor_tiie_crv.linkTo(crvTIIE)

    ibor_tiie = ql.IborIndex('TIIE',
                 ql.Period(13),
                 1,
                 ql.MXNCurrency(),
                 ql.Mexico(),
                 ql.Following,
                 False,
                 ql.Actual360(),
                 ibor_tiie_crv)
    
    # Add missing fixings
    ibor_tiie.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        ibor_tiie.addFixing(
            ql.Date((pd.to_datetime(banxico_TIIE28['fecha'][h])).day, 
                    (pd.to_datetime(banxico_TIIE28['fecha'][h])).month, 
                    (pd.to_datetime(banxico_TIIE28['fecha'][h])).year), 
            banxico_TIIE28['dato'][h+1])
    
    # Discount Engine Definition
    rytsMXNOIS = ql.RelinkableYieldTermStructureHandle()
    rytsMXNOIS.linkTo(crvMXNOIS)
    tiie_swp_engine = ql.DiscountingSwapEngine(rytsMXNOIS)
    
    return [ibor_tiie, tiie_swp_engine]

def dv01_table(tenor: int, rate: float, evaluation_date: datetime, 
               g_engines: list, dv01_engines: list, mxn_fx: float, 
               dv01_tab: pd.DataFrame) -> pd.DataFrame:
    """Calculates a notional by dv01s in different tenors
    

    Parameters
    ----------
    tenor : int
        The tenor required to calculate the notional
    rate : float
        The rate required to calculate the notional
    evaluation_date : datetime
    
    g_engines : list
        list of QuantLib object necessary to price swaps   
    dv01_engines : list
        list of QuantLib object necessary to calculate dv01s of a 
        certain swap
    mxn_fx : float
        USD-MXN currency exchange rate 
    dv01_tab : pd.DataFrame
        DataFrame with tenors as coluns and notionals as values

    Returns
    -------
    pd.DataFrame
        DataFrame with tenors as columns and notionals calculated by 
        their dv01 in the respected tenor
        
    See Also
    --------
        engines: Creates the g_engines list
        fn.tiieSwap: Creates vanilla swap object and the cashflows list
        fn.flat_DV01_calc: Calculates th dv01 for a certain swap

    """
    # Parameters Definition
    mx_calendar = ql.Mexico()
    todays_date = evaluation_date
    todays_date = ql.Date(todays_date.day, todays_date.month, todays_date.year)
    start = mx_calendar.advance(todays_date, ql.Period(1 , ql.Days))
    maturity = start + ql.Period((int(tenor) * 28) , ql.Days)
    notional = 100000000
    ibor_tiie = g_engines[0]
    tiie_swp_engine = g_engines[1]
    
    
    # Flat Dv01 engines
    ibor_tiie_plus = dv01_engines[0]
    tiie_swp_engine_plus = dv01_engines[1]
    ibor_tiie_minus = dv01_engines[2]
    tiie_swp_engine_minus = dv01_engines[3]
    
    
    # Swap Parameters
    rule = ql.DateGeneration.Backward
    typ = ql.VanillaSwap.Receiver
   
    # Swaps construction
    swap_valuation = fn.tiieSwap(start, maturity, abs(notional), ibor_tiie, 
                                 rate, typ, rule)
    swap_valuation[0].setPricingEngine(tiie_swp_engine)
    flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus,
                                  ibor_tiie_minus, tiie_swp_engine_minus, 
                                  start, maturity, abs(notional), rate, typ, 
                                  rule)

    # DV01 calculation
    dv01_100mn = flat_dv01/mxn_fx
    npv_100mn = swap_valuation[0].NPV()
    
    # New Swap Data
    notional = (float(10000) * notional) / dv01_100mn
    dv01_tab[str(tenor)+'L'] = [abs(notional)]

    return(dv01_tab)

def granular(evaluation_date, crvMXNOIS, g_engines, dic_data):
    
    tenors = dic_data['Granular_Tenors']['Granular Tenors']
    df_tiie = dic_data['MXN_TIIE'].copy()
    existing_tenors = df_tiie['Tenor'].tolist()
    missing_tenors = [t for t in tenors.tolist() if t not in existing_tenors]
    
    dic_granular = {k: v.copy() for (k, v) in dic_data.items()}

    tenor2ql = {'B': ql.Days, 'D': ql.Days, 'L': ql.Weeks, 'W': ql.Weeks, 
                'Y': ql.Years}
    tenor_type = tenors.str[-1].map(tenor2ql).tolist()
    
    def f(x):
        if x[-1]=='L':
            return int(x[1:-1])*4
        else:
            return int(x[1:-1])
        
    period = tenors.map(lambda x: f(x)).tolist()

    ibor_tiie = g_engines[0]
    tiie_swp_engine = g_engines[1]
    start = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                ql.Period(1, ql.Days))
    maturities = [start + ql.Period(period[i], tenor_type[i]) 
                  for i in range(len(tenors))]
    quotes = []
    
    for t in range(len(tenors)):
        
        if tenors[t] in existing_tenors:
            quotes.append(np.nan)
        
        else:
            swap = fn.tiieSwap(start, maturities[t], 1000000000, ibor_tiie, 
                               0.09, -1, 0)[0]
            swap.setPricingEngine(tiie_swp_engine)
            quotes.append(swap.fairRate()*100)
    
    granular_tiie = pd.DataFrame({'Tenor': tenors, 'Period': period, 
                                  'Quotes': quotes})
    df_tiie_extended = granular_tiie.merge(df_tiie[['Tenor', 'Quotes']], 
                                           how = 'outer', left_on = 'Tenor', 
                                           right_on = 'Tenor')
    df_tiie = dic_data['MXN_TIIE'].copy()
    
    existing_tenors = df_tiie['Tenor'].tolist()
    
    df_tiie_extended['Quotes'] = df_tiie_extended['Quotes_y'].fillna(0)\
        + df_tiie_extended['Quotes_x'].fillna(0)
    

    
    dic_granular['MXN_TIIE'] = df_tiie_extended.copy()
    
    
    return dic_granular

    
def proc_CarryCalc(g_engines: list, wb: xw.Book) -> None:
    """Calculates the Carry and Roll of certain swaps  
    

    Parameters
    ----------
    g_engines : list
        list of QuantLib object necessary to price swaps 
    wb : xw.Book
        xlwings Book object that emulates an excel book

    Returns
    -------
    None
        It writes the Carry and rol calculations for the swaps specified 
        in the Carry sheet of the IRS_TIIE_Data.xlsm excel file.
    
    See Also
    --------
     engines: Creates the g_engines list

    """
    carry = wb.sheets('Carry')
    # Read params
    
    params_carry = (carry.range('B2:C6').options(
        pd.DataFrame, index=False, header = False).value).dropna().drop(3)

    carry_shiftL = int(carry.range('C5').value)
    
    ibor_tiie, tiie_swp_engine = g_engines
    
    
    # # Read trades
    row_range = str(carry.range('E2').end('down').row)
    
    
    params_carry_trades = carry.range('E2:H' + row_range).options(
        pd.DataFrame,index=False).value
    # Selected Trades
    params_carry_trades = params_carry_trades[
        ~params_carry_trades[
            ['Start_Tenor','Period_Tenor']].isna().all(axis=1)]
    params_carry_trades['Start_Tenor'] = \
        params_carry_trades['Start_Tenor'].fillna(0)
    # Start Dates
    dt_sttl = params_carry.loc[1,1]
    ql_sttl = ql.Date(dt_sttl.day,dt_sttl.month,dt_sttl.year)
    lst_stdt, lst_eddt, lst_swprate = [], [], []
    for i,r in params_carry_trades.iterrows():
        ql_stdt = ql_sttl+ql.Period(int(r['Start_Tenor']*28),ql.Days)
        lst_stdt.append(ql_stdt)
        ql_eddt = ql_stdt+ql.Period(int(r['Period_Tenor']*28),ql.Days)
        lst_eddt.append(ql_eddt)
        swp0 = fn.tiieSwap(ql_stdt, ql_eddt, 1e9, ibor_tiie, 0.04, -1, 0)[0]
        swp0.setPricingEngine(tiie_swp_engine)
        lst_swprate.append(swp0.fairRate())
    # Swap Rates
    df_trades = params_carry_trades.\
        merge(pd.DataFrame([lst_stdt,lst_eddt,lst_swprate]).T,
              left_index=True, right_index=True)
    df_trades.columns = params_carry_trades.columns.to_list()+\
        ['Start', 'Mty','Rate']
    # Roll
    dt_hrzn = params_carry.loc[4,1]
    ql_hrzn = ql.Date(dt_hrzn.day,dt_hrzn.month,dt_hrzn.year)
    lst_roll = []
    lst_carry = []
    for i,r in df_trades.iterrows():
        if r['Start'] == ql_sttl: # ErodedSwap vs FwdEroded
            period = int(r['Period_Tenor'] - carry_shiftL)*28
            if period <= 0:
                lst_roll.append(0)
                continue
            start = ql_hrzn
            swp1 = fn.tiieSwap(ql_sttl, 
                               ql_sttl+ql.Period(period,ql.Days), 1e9, 
                               ibor_tiie, r['Rate'], -1, 0)[0]
            swp2 = fn.tiieSwap(start, r['Mty'], 1e9, 
                               ibor_tiie, r['Rate'], -1, 0)[0]
            swp1.setPricingEngine(tiie_swp_engine)
            swp2.setPricingEngine(tiie_swp_engine)
            lst_roll.append(1e4*(swp1.fairRate() - swp2.fairRate())*-1)
            lst_carry.append(1e4*(r['Rate']-swp2.fairRate())*-1)
            #print(1e4*(r['Rate']-swp1.fairRate()))
        else:
            if int(r['Start_Tenor']-carry_shiftL) <0:
                   lst_roll.append(0)
                   continue
            period = int(r['Period_Tenor'])*28
            start = ql_sttl + ql.Period(
                int(r['Start_Tenor']-carry_shiftL)*28,ql.Days)
            maturity = start + ql.Period(period,ql.Days)
            swp1 = fn.tiieSwap(start,maturity, 1e9, ibor_tiie, 
                               r['Rate'], -1, 0)[0]
            swp1.setPricingEngine(tiie_swp_engine)
            lst_roll.append(1e4*(swp1.fairRate() - r['Rate'])*-1)
    
    df_trades['CarryRoll'] = lst_roll
    carry.range('K3').value = df_trades[['CarryRoll']].values
    carry.range('I3').value = np.array(lst_carry).reshape(-1,1)
    carry.range('J3:J' + row_range).api.Calculate()

#---------------------
#  Pricing Functions
#---------------------

def tiie_pricing(dic_data: dict, wb: xw.Book, g_crvs: list, 
                 banxico_TIIE28: pd.DataFrame, bo_engines: list, 
                 g_engines: list, dv01_engines: list, 
                 parameters_trades: pd.DataFrame = pd.DataFrame()) -> None:
    """Main function, it wil price all swaps required by the user
    
    This function will price any swap required by the user from the 
    file TIIE_IRS_Data.xlsm in the pricng sheet.

    Parameters
    ----------
    dic_data : dict
        Dictionary with the necessary quotes to price the required swaps
    wb : xw.Book
        xlwings Book object that emulates an excel book
    g_crvs : list
        list of QuantLib DiscountCurve objects 
    banxico_TIIE28 : pd.DataFrame
        Dataframe with TIIE28 quotes from the last 10 years
    bo_engines : list
        List of QuantLib object necessary to price swaps with bid and 
        offer differences
    g_engines : list
        list of QuantLib object necessary to price swaps
        
    dv01_engines : list
        list of QuantLib object necessary to calculate dv01s of swaps
    parameters_trades : pd.DataFrame, optional
        DataFrame of the instructons given by the user to price swaps
        The default is pd.DataFrame().

    Returns
    -------
    None
        This function prints the swaps specifications required by the 
        user, please see the ´output_trade´ and ´output_bo´ functions in
        the *Funtions* module for more information.
    
    See Also
    --------
        createCurves: Creates the g_curves list
        banxicoData: Creates the banxico_TIIE28 DataFrame
        fn.bid_offer_curves: creates the bo_engines list
        engines: Creates the g_engines list
        fn.flat_dv01_curves: Creates the dv01_engines list
        collapse: Can Create parameters_trades DataFrame when required
        fn.start_end_dates_trading: Calculates start and end dates for a
                                    certain swap
        fn.tiieSwap: Creates vanilla swap object and the cashflows list
        fn.flat_DV01_calc: Claculates th dv01 for a certain swap
        fn.KRR_helper: Key Rate Risk calulation for a certain swap
        fn.bid_offer: Calculates the bid offer NPVs for a certain swap
        fn.output_trade: Prints the swap specificatons
        fn.output_bo: Prints the dv01 and bid offer rates and NPVs when 
                      required
        fn.get_CF_tiieSwap: Creates a DataFrame with the swap cashflows
        
    """
    # Read File
    collapse_flag = True
    parameters = wb.sheets('Pricing')
    bad_trades = []
    historical_df = pd.DataFrame()
    updateAll = parameters.range('B2').value
    
    # Check Banxico Dates
    banxico_sheet = wb.sheets('Short_End_Pricing')
    range_banxico_row = str(banxico_sheet.range('B4').end('down').row)
    banxico_df = banxico_sheet.range('B3', 'C' + range_banxico_row).options(
        pd.DataFrame, header=1, index=False).value
    banxico_df.rename(columns = {banxico_df.columns[0]: 'Meeting_Dates'}, 
                      inplace = True)
    evaluation_date = pd.to_datetime(parameters.range('B1').value)
    original_tiie_rates = wb.sheets('MXN_TIIE').range('E2:E16').value
    
    # When last Banxico Date is befor today
    if banxico_df.iloc[0]['Meeting_Dates'] < evaluation_date:
        banxico_flag = True
        
    else: 
        banxico_flag = False
    
    # Input definition
    mxn_fx = parameters.range('F1').value
    original_dftiie = wb.sheets('MXN_TIIE').range('A1:N16')\
        .options(pd.DataFrame,  index = False,header = 1).value
        
    # When parameters_trades is not empty, a collapse is being made
    if parameters_trades.empty:
        range_trades = parameters.range('A4').end('right').address[1] + \
            str(parameters.range('A4').end('down').row)
        parameters_trades = parameters.range('A4', range_trades).options(
            pd.DataFrame, header=1).value
        parameters_trades.index = parameters_trades.index.astype(int)
        parameters_trades.Valuation_Check = \
            parameters_trades.Valuation_Check.astype(str)
        collapse_flag = False
    
    # DataFrame Standarization
    parameters_trades.Valuation_Check = \
        parameters_trades.Valuation_Check.str.lower()
    parameters_trades.Bid_Offer_Check = \
        parameters_trades.Bid_Offer_Check.str.lower()
    
    parameters_trades.Key_Rate_Risk_Check = \
        parameters_trades.Key_Rate_Risk_Check.str.lower()
    parameters_trades.Cashflows = parameters_trades.Cashflows.str.lower()
    parameters_trades = parameters_trades[
        parameters_trades.Valuation_Check != 'none'].fillna(0)
    
    if ('collapse' in 
        parameters_trades['Comment 1'].astype(str).str.lower().tolist()):
            collapse_flag = True
            parameters_trades['CCP'] = 'CME'
    
    # Global Curves
    crvMXNOIS = g_crvs[2]
    brCrvs = g_crvs[4]
    
    # Global Engines
    ibor_tiie = g_engines[0]
    tiie_swp_engine = g_engines[1]
    
    # Bid Offer Engines
    ibor_tiie_bid = bo_engines[0]
    tiie_swp_engine_bid = bo_engines[1]
    ibor_tiie_offer = bo_engines[2]
    tiie_swp_engine_offer = bo_engines[3]
    
    #Flat Dv01 engines
    ibor_tiie_plus = dv01_engines[0]
    tiie_swp_engine_plus = dv01_engines[1]
    ibor_tiie_minus = dv01_engines[2]
    tiie_swp_engine_minus = dv01_engines[3]
    
    # Variable Definition
    krr_group = pd.DataFrame(columns = list(dic_data['MXN_TIIE']['Tenor']))
    krr_list = []
    npv_group = {}
    sf_group = {}
    dic_CF = {}
    bo_group = {}
    dv01s = []
    notionals = []
    abs_dv01s = {}
    fees = []
    banxico_risks = pd.DataFrame()
    
    # Iteration by rows (swap parameters)
    for i, values in parameters_trades.iterrows():
        
        # Check correct input for Valuation_Check
        if values.Valuation_Check != 'x':
            bad_trades.append(str(i))
            continue
        else:
            pass
            
        
        # When last banxico date is before today, break loop
        if banxico_flag:
            print('\n######## Outdated Banxico Meeting Dates #########')
            print('########## CHECK BANXICO MEETING DATES ##########')
            print('              ',banxico_df.iloc[0]['Meeting_Dates'])
            break

        # Base Case
        if values.NPV_MXN == 0 and values.DV01_USD == 0:
                    
            # Swap Data
            
            # Start and end dates definition
            try:
                start, maturity, flag_mat = fn.start_end_dates_trading(
                    values, evaluation_date)
                
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check inputs for trade {i}.')
                # When incorrect, the trade is assigned to bad_trades
                bad_trades.append(str(i))
                continue
            
            # Notional definition and type handling
            notional = values.Notional_MXN
            try:
                
                if type(notional) == str:
                    notional = float(notional.replace(',', ''))
                
                else:
                    notional = float(notional)
                    if notional == 0:
                        print(f'\nOutput Trade  {i}')
                        print(f'Please check notional for trade {i}. '
                              + 'No notional found.')
                        bad_trades.append(str(i))
                        continue
                   
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check notional for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                bad_trades.append(str(i))
                continue
            
            notionals.append(notional)
            
            
                
            rate = values.Rate
            try:  
                rate = float(rate)
            
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check rate for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                bad_trades.append(str(i))
                continue
            
            # Trade side definition
            if notional >= 0:
                typ = ql.VanillaSwap.Receiver
                
            else:
                typ = ql.VanillaSwap.Payer
            
            # Date generation rule definition
            if values.Date_Generation == 'Forward':
                rule = ql.DateGeneration.Forward
                
            else:
                rule = ql.DateGeneration.Backward
            
            # Swaps construction
            swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                         ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # Output varibles definition
            npv = swap_valuation[0].NPV() 
            fair_rate = swap_valuation[0].fairRate()
            swap_dict = {}
            swap_dict[i] = [npv,fair_rate]
            swap_df = pd.DataFrame.from_dict(swap_dict).T
            swap_df = swap_df.rename(columns={0: 'NPV', 1: 'FairRate'})
            
            # DV01 Calculation
            flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)
            dv01s.append(flat_dv01/mxn_fx)
            
            
            # Original DV01 -> DV01 by one fair rate bp movement
            flat_dv01_fr = fn.tiieSwap(start, maturity, abs(notional), 
                                       ibor_tiie, swap_valuation[0].fairRate() 
                                       + 0.0001, typ, rule)
            flat_dv01_fr[0].setPricingEngine(tiie_swp_engine)
            original_dv01 = abs(flat_dv01_fr[0].NPV()/mxn_fx) * \
                np.sign(flat_dv01)
            original_dv01_mxn = abs(flat_dv01_fr[0].NPV()) * np.sign(flat_dv01)
            
            # KRR group inputs definition
            krr_group_value = values.KRR_Group
            try:
                krr_group_value = int(krr_group_value)
                
            except:
                pass
            npv_group[i] = [krr_group_value, npv]
            
            # Bid / Offer 
            if ((values.Bid_Offer_Check == 'x' or values.Bid_Offer_Group != 0) 
                and len(brCrvs) < 40):
                bo = True
                
                # When backdated swap, complete bid/offer curves are used
                if start <= \
                    ql.Mexico().advance(ql.Settings.instance().evaluationDate, 
                                        ql.Period('1D')):
                    swap_receiver = fn.tiieSwap(start, maturity, abs(notional), 
                                                ibor_tiie_offer, rate, -1, 
                                                rule)
                    swap_receiver[0].setPricingEngine(tiie_swp_engine_offer)
                    fair_rate_offer = swap_receiver[0].fairRate()
           
                    swap_payer = fn.tiieSwap(start, maturity, abs(notional), 
                                             ibor_tiie_bid, rate, 1, rule)
                    swap_payer[0].setPricingEngine(tiie_swp_engine_bid)
                    fair_rate_bid = swap_payer[0].fairRate()
                    
                    npv_receiver = swap_receiver[0].NPV()
                    npv_payer = swap_payer[0].NPV()
                    
                # All other cases, only bid/offer tenors are changed
                else:
                    krrc_f, krrg_f, krrl, df_tenorDV01 = fn.KRR_helper(i, 
                        values, brCrvs, dic_data, npv_group, start, maturity, 
                        notional, rate)
                    
                    # NPV calculation
                    npv_receiver, npv_payer = \
                        fn.bid_offer(start, maturity, fair_rate, dic_data, 
                                     crvMXNOIS, banxico_TIIE28, df_tenorDV01, 
                                     notional, rate, rule, typ)
                    
                    # Fair Rate Spread calculation
                    spread_rec = abs((-typ*npv - npv_receiver) / 
                                     original_dv01_mxn) / 10000
                    spread_payer = abs((typ*npv - npv_payer) / 
                                       original_dv01_mxn) / 10000
                    spread_nuevo = (spread_rec + spread_payer)/2
                    
                    # New Fair Rates definition
                    fair_rate_bid = fair_rate - spread_nuevo
                    fair_rate_offer = fair_rate + spread_nuevo
                    
                # Print data
                if values.Bid_Offer_Check =='x':
                    
                    # Only Bid Offer Check Output
                    fn.output_trade(i, start, maturity, notional, rate, 
                                    swap_valuation, flag_mat, bo)
                    fn.output_bo(flat_dv01, mxn_fx, bo, 0, npv_receiver, 
                                 npv_payer, fair_rate_bid, fair_rate_offer)
                    
                    # Bid Offer Group data definition
                    if values.Bid_Offer_Group != 0:
                        if typ < 0:
                            bo_group[i]=[values.Bid_Offer_Group, npv_receiver, 
                                         npv_payer]
                        else:
                            bo_group[i]=[values.Bid_Offer_Group, npv_payer, 
                                         npv_receiver]
                    
                else:
                    # Only Bid Offer group Output (Normal Output)
                    fn.output_trade(i, start, maturity, notional, rate, 
                                    swap_valuation, flag_mat)
                    fn.output_bo(flat_dv01, mxn_fx)
                    
                    # Bid Offer Group data definition
                    if typ < 0:
                        bo_group[i]=[values.Bid_Offer_Group, npv_receiver, 
                                     npv_payer]
                    else:
                        bo_group[i]=[values.Bid_Offer_Group, npv_payer, 
                                     npv_receiver]
                    
            
            else:
                # Normal Output
                fn.output_trade(i, start, maturity, notional, rate, 
                                swap_valuation, flag_mat)
                fn.output_bo(flat_dv01, mxn_fx)
                bo = False
                
            if collapse_flag:
                ccp = values.CCP
                fee = collapse_fee(start, maturity, notional, ccp, 
                                   evaluation_date)
                fees.append(fee)
            # Spread Fly Group Data Definition
            if values.Spread_Fly != 0:
                if values.Bid_Offer_Check == 'x':               
                    sf_group[i] = [values.Spread_Fly, fair_rate_bid, 
                                   fair_rate_offer, start, maturity]
                else:
                    sf_group[i] = [values.Spread_Fly, fair_rate, np.nan, start, 
                                   maturity]
        
            # Swaps Cashflows and Banxico Check
            swap_valuation_CF = fn.get_CF_tiieSwap(swap_valuation)
            swap_CF = swap_valuation_CF.drop(['Date', 'Fix_Amt','Float_Amt'], 
                                             axis=1)
            # After today coupons
            
            # When Cashflows Check, dictionary Definition
            if values.Cashflows == 'x':
                if (swap_CF['Fixing_Date'].iloc[0] - evaluation_date).days > \
                    28:
                    inter_period = (swap_CF['Fixing_Date'].iloc[0] - 
                                    evaluation_date).days//28
                    inter_fixings = [swap_CF['Fixing_Date'].iloc[0] -\
                                  timedelta(days = 28 * k) for k in 
                                  range(inter_period-1, -1, -1)]
                    inter_fras = [g_crvs[3].forwardRate(
                        ql.Date.from_date(inter_fixings[l]), 
                        ql.Date.from_date(inter_fixings[l+1]), ql.Actual360(), 
                        ql.Simple).rate() for l in range(0, 
                                                         len(inter_fixings)-1)]
                    inter_sdates = [(ql.Mexico().advance(
                        ql.Date.from_date(j), ql.Period(1, ql.Days))).to_date() 
                        for j in inter_fixings]
                    inter_edates = [(ql.Date().from_date(d) + 
                                     ql.Period(28, ql.Days)).to_date() for d in 
                                    inter_sdates]
                
                else:
                    inter_period = 0
                    inter_fixings = []
                    inter_fras = []
                    inter_sdates = []
                    inter_edates = []
                
                fixing_dates = [i.strftime('%Y-%m-%d') for i in 
                                swap_CF['Start_Date'] if i > evaluation_date]
                
                # Past coupons
                prev_fixing_dates = [i for i in swap_CF['Start_Date'] 
                                     if i <= evaluation_date]
                
                # FRAs calculation
                prev_fras = \
                    banxico_TIIE28[
                        banxico_TIIE28['fecha'].isin(prev_fixing_dates)
                        ].sort_values(by='fecha')['dato'].values
                #prev_dfs = [0]*len(prev_fras)
                end_dates = swap_valuation_CF['End_Date'].tolist()
                end_dates_1 = end_dates[len(prev_fixing_dates):]
                
                cf_fras = [g_crvs[3].forwardRate(
                    ql.Date(fixing_dates[d], '%Y-%m-%d'), 
                    ql.Date().from_date(end_dates_1[d]), 
                    ql.Actual360(), ql.Simple).rate() 
                    for d in range(len(fixing_dates))]
                end_dates = swap_valuation_CF['End_Date'].tolist()
                fix_dates = swap_valuation_CF['Fixing_Date'].tolist()
                cf_dfs = [g_crvs[2].discount(ql.Date().from_date(end_dates[p])) 
                          for p in range(0, len(end_dates)) if 
                          end_dates[p] > evaluation_date]
                prev_dfs = [0]*(len(end_dates)-len(cf_dfs))
                cf_dfs = [j for i in [prev_dfs, cf_dfs] for j in i]
                cf_fras = [j for i in [prev_fras, cf_fras] for j in i]
                swap_valuation_CF['FRA'] = cf_fras 
                swap_valuation_CF['DF'] = cf_dfs
                swap_valuation_CF['Notional'] = notional
                swap_CF_inter = pd.DataFrame(
                    columns = swap_valuation_CF.columns)
                swap_CF_inter['Date'] = inter_edates[:-1]
                swap_CF_inter['Fixing_Date'] = inter_fixings[:-1]
                swap_CF_inter['Start_Date'] = inter_sdates[:-1]
                swap_CF_inter['End_Date'] = inter_edates[:-1]
                swap_CF_inter['FRA'] = inter_fras
                swap_valuation_CF = pd.concat([swap_CF_inter, 
                                               swap_valuation_CF], 
                                              ignore_index=True)
                
                dic_CF[i] = swap_valuation_CF
            
            # Banxico and Coupon Fixing common dates check 
            ql_dates = [ql.Date(i.strftime('%Y-%m-%d'), '%Y-%m-%d') 
                        for i in banxico_df['Meeting_Dates']]
            banxico_df['Fix_Effect'] = [(ql.Mexico().advance(i, 
                ql.Period(1 , ql.Days))).ISO() for i in ql_dates]
            banxico_dates = banxico_df[['Fix_Effect']]
            swap_CF['Fixing_Date'] = [i.strftime('%Y-%m-%d') 
                                      for i in swap_CF['Fixing_Date']]
            swap_CF_copy = swap_CF.copy()
            common_dates = swap_CF_copy.merge(banxico_dates, 
                                              how='inner', 
                                              left_on = 'Fixing_Date', 
                                              right_on = 'Fix_Effect')
            common_dates['Net_Amt'] = [i.replace(',', '') 
                                       for i in common_dates['Net_Amt']]
            common_dates['Flt Risk'] = [
                round((notional*0.0001*(28/360)) / mxn_fx, 0)] * \
                common_dates.shape[0]
            common_dates['KRR_Group'] = [krr_group_value] * \
                common_dates.shape[0]
            
            # When comon dates are found, FRAs are assigned
            if common_dates.shape[0] != 0:
                fras = [g_crvs[3].forwardRate(ql.Date(d, '%Y-%m-%d'), 
                                              ql.Date(d, '%Y-%m-%d') 
                                              + ql.Period(28, ql.Days), 
                                              ql.Actual360(), ql.Simple).rate() 
                        for d in common_dates['Fixing_Date']]
                common_dates['FRA'] = fras
                risk = common_dates[['Fix_Effect', 'Flt Risk', 'FRA', 
                                     'KRR_Group']]
                risk['FRA'] = risk['FRA'] * 100
                banxico_risks = pd.concat([banxico_risks, risk], 
                                          ignore_index=True)
                format_mapping = {'Flt Risk' : 'USD${:,.0f}', 
                                  'FRA': '{:,.4f}%'}
                
                for k, v in format_mapping.items():
                    risk[k] = risk[k].apply(v.format)
                
                # Common Dates Warning
                print('\n')
                print ('Check Banxico Meeting Dates\n')
                print(risk[['Fix_Effect', 'Flt Risk', 'FRA']])
                
            # Irregular Cashflows check 
            
            # Case last tenor  greater than 28
            if (int(swap_CF['Acc_Days'].iloc[-1]) > 28):
                extra_days = int(swap_CF['Acc_Days'].iloc[-1]) - 28
                holiday  = 0
                for ed in range(1,extra_days+1):
                    
                    if ql.Mexico().isHoliday(ql.Date().from_date(
                            swap_CF['End_Date'].iloc[-1])-ed):
                        holiday += 1
                
                if holiday == extra_days:
                    pass
                
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)
            
            # Case last tenor lower than 28
            elif (int(swap_CF['Acc_Days'].iloc[-1]) < 28):
                if swap_CF['Acc_Days'].iloc[-2] > 28:
                    extra_days = int(swap_CF['Acc_Days'].iloc[-2]) - 28
                    holiday  = 0
                    for ed in range(1,extra_days+1):
                        
                        if ql.Mexico().isHoliday(ql.Date().from_date(
                                swap_CF['End_Date'].iloc[-2])-ed):
                            holiday += 1
                            
                    if holiday == extra_days:
                        pass
                    
                    else:
                        print('\n')
                        print('Irregular Cashflows:')
                        print('\n')
                        print(swap_CF)
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)
            
            # Case first Tenor greater than 28
            if (int(swap_CF['Acc_Days'].iloc[0]) > 28):
                extra_days = int(swap_CF['Acc_Days'].iloc[0]) - 28
                holiday  = 0
                
                for ed in range(1,extra_days+1):
                    
                    if ql.Mexico().isHoliday(ql.Date().from_date(
                            swap_CF['End_Date'].iloc[0])-ed):
                        holiday += 1
                        
                if holiday == extra_days:
                    pass
                
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)   
            
            # Case first tenor lower than 28
            elif (int(swap_CF['Acc_Days'].iloc[0]) < 28):
                
                if swap_CF['Acc_Days'].iloc[1] > 28:
                    extra_days = int(swap_CF['Acc_Days'].iloc[1]) - 28
                    holiday  = 0
                    
                    for ed in range(1,extra_days+1):
                        
                        if ql.Mexico().isHoliday(ql.Date().from_date(
                                swap_CF['End_Date'].iloc[1])-ed):
                            holiday += 1
                            
                    if holiday == extra_days:
                        pass
                    
                    else:
                        print('\n')
                        print('Irregular Cashflows:')
                        print('\n')
                        print(swap_CF)
                        
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)
            confos_df_a = fn.output_trade(i, start, maturity, notional, 
                                          rate, swap_valuation, 
                                          flag_mat, blotter = True)
            
            confos_df_a.insert(2,'Client', 
                               np.select([confos_df_a['Side'].str[6] == 'P'],
                                         ['Finamex: REC Fixed MXN IRS'],
                                         ['Finamex: PAY Fixed MXN IRS']))
            
            confos_df_a = confos_df_a.drop(columns = ['Side'])
            
            
            
            
            if bo == True:
                confos_df_a['Fair_Rate'] =\
                    ["% {:,.4f}".format(fair_rate_bid*100) +' | ' 
                     + "% {:,.4f}".format(fair_rate_offer*100)]
                bid_rates = original_dftiie['FMX Desk BID'].values
                offer_rates = original_dftiie['FMX Desk OFFER'].values
                rates = ["% {:,.4f}".format(bid_rates[i]) + ' | '
                         + "% {:,.4f}".format(offer_rates[i])
                         for i in range(0, len(bid_rates))]
                
                
            else:
                confos_df_a['Fair_Rate'] = ["% {:,.4f}".format(
                                                    swap_valuation[0]\
                                                        .fairRate()*100)]
                    
                
                rates = ["% {:,.4f}".format(r) 
                         for r in original_tiie_rates]

                
            rates_krr = ["% {:,.4f}".format(r) 
                     for r in dic_data['MXN_TIIE']['Quotes'].values]
            tenors = dic_data['MXN_TIIE']['Tenor'].values
            tenor_rates = dict(zip(tenors, rates))
            tenor_rates_df = pd.DataFrame(tenor_rates, index=[0])
            hist_price = pd.concat([confos_df_a, tenor_rates_df], axis=1)
            hist_price['NPV'] = [npv]
            hist_price['DV01'] = [flat_dv01/mxn_fx]
            if not collapse_flag:
                if values['Comment 1'] != 0:
                    hist_price['Comment'] = values['Comment 1']
                else:
                    hist_price['Comment'] = ['']
            else:
                hist_price['Comment'] = ['']
                hist_price['Start Date'] = [start.to_date()]
                hist_price['End Date'] = [maturity.to_date()]
            historical_df = pd.concat([historical_df, hist_price])
            # KRR
            if (values.Key_Rate_Risk_Check == 'x') or (krr_group_value != 0):
                krrc_f, krrg_f, krrl, df_tenorDV01 = \
                    fn.KRR_helper(i, values, brCrvs, dic_data, npv_group, 
                                  start, maturity, notional, rate)
                
                # If Key_Rate_Risk_Check is True
                if krrc_f:
                    print('\n')
                    print('Key Rate Risk USD$:', '\n')
                    print('Outright DV01: '
                          + f'{(df_tenorDV01 / mxn_fx).sum().sum():,.0f}')
                    dv01 = pd.DataFrame((df_tenorDV01/mxn_fx).sum().map(
                        '{:,.0f}'.format))
                    dv01['Quote'] = ['   ' + r for r in rates_krr]
                    print(dv01.to_string(index=True, header=False))
                    
                    
                    
                # If Key_Rate_Risk_Group is True
                if krrg_f:
                    krr_list.append(krrl)
                    krr_group = pd.concat([krr_group, df_tenorDV01])
                    abs_dv01s[i] = [krr_group_value, 
                                    (np.abs((df_tenorDV01 / mxn_fx).sum()\
                                            .sum()))]
    
        # Case Notional Unknown
        elif values.Notional_MXN == 0:
            
            # Swap Data
            
            # Start and end dates definition
            try:
                start, maturity, flag_mat = fn.start_end_dates_trading( 
                    values, evaluation_date)
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check inputs for trade {i}.')
                # When incorrect, the trade is assigned to bad_trades
                bad_trades.append(str(i))
                continue
            
            # Notional and rate definition 
            notional = 100000000
            rate = values.Rate
            try:  
                rate = float(rate)
            
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check rate for trade {i}.')
                bad_trades.append(str(i))
                continue
            
            # Standard d swap date generation rule and side
            rule = ql.DateGeneration.Backward
            typ = ql.VanillaSwap.Receiver
           
            # Standard swap construction
            swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                         ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # Case rate == 0
            if rate == 0:
                rate = swap_valuation[0].fairRate() 
                swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                             ibor_tiie, rate, typ, rule)
                swap_valuation[0].setPricingEngine(tiie_swp_engine)
                
                
            # Dummy Dv01    
            flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)
            dv01_100mn = flat_dv01/mxn_fx
            npv_100mn = swap_valuation[0].NPV()
            
            # New Swap Data
            
            # DV01 definition and data type handling
            dv01_value = values.DV01_USD
            try:
                if type(dv01_value) == str:
                    dv01_value = float(dv01_value.replace(',', ''))
                
                else:
                    dv01_value = float(dv01_value)
                    
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check DV01_USD for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                bad_trades.append(str(i))
                continue
            
            # NPV definition and data type handling
            npv_value = values.NPV_MXN
            
            try:
                if type(npv_value) == str:
                    npv_value = float(npv_value.replace(',', ''))
                
                else:
                    npv_value = float(npv_value)
                    
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check NPV_MXN for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                bad_trades.append(str(i))
                continue
            
            # Notional Unknown cases and notional definition
            if npv_value == 0:
            
                notional = (dv01_value*100000000) / dv01_100mn

            elif dv01_value == 0:
            
                notional = (npv_value*100000000) / npv_100mn
            
            notionals.append(notional)
                       
            # Trade side definition
            if notional >= 0:
                typ = ql.VanillaSwap.Receiver
            else:
                typ = ql.VanillaSwap.Payer
            
            # Date generation rule definition
            if values.Date_Generation == 'Forward':
                rule = ql.DateGeneration.Forward
            else:
                rule = ql.DateGeneration.Backward
            
            # Swaps construction
            swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                         ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # Output varibles definition
            npv = swap_valuation[0].NPV() 
            fair_rate = swap_valuation[0].fairRate()
            swap_dict = {}
            swap_dict[i] = [npv,fair_rate]
            swap_df = pd.DataFrame.from_dict(swap_dict).T
            swap_df = swap_df.rename(columns={0: 'NPV', 1: 'FairRate'})
        
            # DV01 Calculation
            flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)
            dv01s.append(flat_dv01/mxn_fx)
            
            # Original DV01 -> DV01 by one fair rate bp movement
            flat_dv01_fr = fn.tiieSwap(start, maturity, abs(notional), 
                                       ibor_tiie, swap_valuation[0].fairRate() 
                                       + 0.0001, typ, rule)
            flat_dv01_fr[0].setPricingEngine(tiie_swp_engine)
            original_dv01 = abs(flat_dv01_fr[0].NPV()/mxn_fx) * \
                np.sign(flat_dv01)
            original_dv01_mxn = abs(flat_dv01_fr[0].NPV()) * np.sign(flat_dv01)
            
            # KRR group inputs definition
            krr_group_value = values.KRR_Group
            try:
                krr_group_value = int(krr_group_value)
                
            except:
                pass
            npv_group[i] = [krr_group_value, npv]
            
            
            # Bid / Offer 
            if ((values.Bid_Offer_Check == 'x' or values.Bid_Offer_Group != 0) 
                and len(brCrvs)<40):
                bo = True
                
                # When backdated swap, complete bid/offer curves are used
                if start <= ql.Mexico().advance(
                        ql.Settings.instance().evaluationDate, 
                        ql.Period('1D')):
                    swap_receiver = fn.tiieSwap(start, maturity, abs(notional), 
                                                ibor_tiie_offer, rate, -1, 
                                                rule)
                    swap_receiver[0].setPricingEngine(tiie_swp_engine_offer)
                    fair_rate_offer = swap_receiver[0].fairRate()
           
                    swap_payer = fn.tiieSwap(start, maturity, abs(notional), 
                                             ibor_tiie_bid, rate, 1, rule)
                    swap_payer[0].setPricingEngine(tiie_swp_engine_bid)
                    fair_rate_bid = swap_payer[0].fairRate()
                    
                    npv_receiver = swap_receiver[0].NPV()
                    npv_payer = swap_payer[0].NPV()
                    
                # All other cases, only bid/offer tenors are changed
                else:
                    krrc_f, krrg_f, krrl, df_tenorDV01 = \
                        fn.KRR_helper(i, values, brCrvs, dic_data, npv_group, 
                                      start, maturity, notional, rate)
                    # NPV calculation
                    npv_receiver, npv_payer = \
                        fn.bid_offer(start, maturity, fair_rate, dic_data, 
                                     crvMXNOIS, banxico_TIIE28, df_tenorDV01, 
                                     notional, rate, rule, typ)
                    
                    # Fair Rate Spread calculation
                    spread_rec = abs((-typ*npv - npv_receiver) / 
                                     original_dv01_mxn) / 10000
                    spread_payer = abs((typ*npv - npv_payer) /
                                       original_dv01_mxn) / 10000
                    spread_nuevo = (spread_rec + spread_payer)/2
                    
                    # New Fair Rates definition
                    fair_rate_bid = fair_rate - spread_nuevo
                    fair_rate_offer = fair_rate + spread_nuevo
                    
                    
                # Print Data
                if values.Bid_Offer_Check =='x':
                    
                    # Only Bid Offer Check Output
                    fn.output_trade(i, start, maturity, notional, rate, 
                                    swap_valuation, flag_mat, bo)
                    fn.output_bo(flat_dv01, mxn_fx, bo, original_dv01, 
                                 npv_receiver, npv_payer, fair_rate_bid, 
                                 fair_rate_offer)
                    
                    # Bid Offer Group data definition
                    if values.Bid_Offer_Group != 0:
                        if typ < 0:
                            bo_group[i]=[values.Bid_Offer_Group, npv_receiver, 
                                         npv_payer]
                        else:
                            bo_group[i]=[values.Bid_Offer_Group, npv_payer, 
                                         npv_receiver]
                        
                else:
                    # Only Bid Offer group Output (Normal Output)
                    fn.output_trade(i, start, maturity, notional, rate, 
                                    swap_valuation, flag_mat)
                    fn.output_bo(flat_dv01, mxn_fx, original_dv01)
                    
                    # Bid Offer Group data definition
                    if typ < 0:
                        bo_group[i]=[values.Bid_Offer_Group, npv_receiver, 
                                     npv_payer]
                    else:
                        bo_group[i]=[values.Bid_Offer_Group, npv_payer, 
                                     npv_receiver]
                   
            
            else:
                # Normal Output with original dv01
                bo=False
                fn.output_trade(i, start, maturity, notional, rate, 
                                swap_valuation, flag_mat)
                fn.output_bo(flat_dv01, mxn_fx, bo, original_dv01)
                  
            # Spread Fly Group Data Definition
            if values.Spread_Fly != 0:
                if values.Bid_Offer_Check == 'x':               
                    sf_group[i] = [values.Spread_Fly, fair_rate_bid, 
                                   fair_rate_offer, start, maturity]
                else:
                    sf_group[i] = [values.Spread_Fly, fair_rate, np.nan, 
                                   start, maturity]
             
            # Swaps Cashflows and Banxico Check
            swap_valuation_CF = fn.get_CF_tiieSwap(swap_valuation)
            swap_CF = swap_valuation_CF.drop(['Date', 'Fix_Amt','Float_Amt'], 
                                             axis=1)
            
            # When Cashflows Check, dictionary Definition
            if values.Cashflows == 'x':
                if (swap_CF['Fixing_Date'].iloc[0] - evaluation_date).days > \
                    28:
                    inter_period = (swap_CF['Fixing_Date'].iloc[0] - 
                                    evaluation_date).days//28
                    inter_fixings = [swap_CF['Fixing_Date'].iloc[0] -\
                                  timedelta(days = 28 * k) for k in 
                                  range(inter_period-1, -1, -1)]
                    inter_fras = [g_crvs[3].forwardRate(
                        ql.Date.from_date(inter_fixings[l]), 
                        ql.Date.from_date(inter_fixings[l+1]), ql.Actual360(), 
                        ql.Simple).rate() for l in range(0, 
                                                         len(inter_fixings)-1)]
                    inter_sdates = [(ql.Mexico().advance(
                        ql.Date.from_date(j), ql.Period(1, ql.Days))).to_date() 
                        for j in inter_fixings]
                    inter_edates = [(ql.Date().from_date(d) + 
                                     ql.Period(28, ql.Days)).to_date() for d in 
                                    inter_sdates]
                
                else:
                    inter_period = 0
                    inter_fixings = []
                    inter_fras = []
                    inter_sdates = []
                    inter_edates = []
                    
                
                # After today coupons
                
                fixing_dates =[i.strftime('%Y-%m-%d') for i in 
                               swap_CF['Start_Date'] if i > evaluation_date] 
                
                # Past coupons
                prev_fixing_dates = [i for i in swap_CF['Start_Date'] if i <= 
                                     evaluation_date]
                
                # FRAs calculation
                prev_fras = banxico_TIIE28[
                    banxico_TIIE28['fecha'].isin(
                        prev_fixing_dates)].sort_values(
                            by='fecha')['dato'].values
                # prev_dfs = [0]*len(prev_fras)
                end_dates = swap_valuation_CF['End_Date'].tolist()
                end_dates_1 = end_dates[len(prev_fixing_dates):]
                
                cf_fras = [g_crvs[3].forwardRate(
                    ql.Date(fixing_dates[d], '%Y-%m-%d'), 
                    ql.Date().from_date(end_dates_1[d]), 
                    ql.Actual360(), ql.Simple).rate() 
                    for d in range(len(fixing_dates))]
                end_dates = swap_valuation_CF['End_Date'].tolist()
                fix_dates = swap_valuation_CF['Fixing_Date'].tolist()
                cf_dfs = [g_crvs[2].discount(ql.Date().from_date(end_dates[p])) 
                          for p in range(0, len(end_dates)) if 
                          end_dates[p] > evaluation_date]
                prev_dfs = [0]*(len(end_dates)-len(cf_dfs))
                cf_dfs = [j for i in [prev_dfs, cf_dfs] for j in i]
                cf_fras = [j for i in [prev_fras, cf_fras] for j in i]
                swap_valuation_CF['FRA'] = cf_fras
                swap_valuation_CF['DF'] = cf_dfs
                swap_valuation_CF['Notional'] = notional
                swap_CF_inter = pd.DataFrame(
                    columns = swap_valuation_CF.columns)
                swap_CF_inter['Date'] = inter_edates[:-1]
                swap_CF_inter['Fixing_Date'] = inter_fixings[:-1]
                swap_CF_inter['Start_Date'] = inter_sdates[:-1]
                swap_CF_inter['End_Date'] = inter_edates[:-1]
                swap_CF_inter['FRA'] = inter_fras
                swap_valuation_CF = pd.concat([swap_CF_inter, 
                                               swap_valuation_CF], 
                                              ignore_index=True)
                dic_CF[i] = swap_valuation_CF
                
            # Banxico and Coupon Fixing common dates check 
            banxico_dates = pd.to_datetime(
                banxico_df['Meeting_Dates'].to_list())  
            ql_dates = [ql.Date(i.strftime('%Y-%m-%d'), '%Y-%m-%d') 
                        for i in banxico_df['Meeting_Dates']]
            banxico_df['Fix_Effect'] = \
                [(ql.Mexico().advance(i, ql.Period(1 , ql.Days))).ISO() 
                 for i in ql_dates]
            banxico_dates=banxico_df[['Fix_Effect']]
            swap_CF['Fixing_Date'] = [i.strftime('%Y-%m-%d') 
                                      for i in swap_CF['Fixing_Date']]
            swap_CF_copy = swap_CF.copy()
            common_dates = swap_CF_copy.merge(banxico_dates, how='inner', 
                                              left_on = 'Fixing_Date', 
                                              right_on = 'Fix_Effect')
            common_dates['Net_Amt'] = [i.replace(',', '') 
                                       for i in common_dates['Net_Amt']]
            common_dates['Flt Risk'] = [round((notional * .0001 * (28/360)) / 
                                              mxn_fx, 0)]*common_dates.shape[0]
            common_dates['KRR_Group'] = [krr_group_value]*common_dates.shape[0]

            # When comon dates are found, FRAs are assigned
            if common_dates.shape[0]!=0:
                fras = [g_crvs[3].forwardRate(ql.Date(d, '%Y-%m-%d'), 
                                              ql.Date(d, '%Y-%m-%d') 
                                              + ql.Period(28, ql.Days), 
                                              ql.Actual360(), ql.Simple).rate() 
                        for d in common_dates['Fixing_Date']]
                common_dates['FRA'] = fras
                risk = common_dates[['Fix_Effect', 'Flt Risk', 'FRA', 
                                     'KRR_Group']]
                risk['FRA'] = risk['FRA']*100
                banxico_risks = pd.concat([banxico_risks, risk], 
                                          ignore_index=True)
                format_mapping = {'Flt Risk' : 'USD${:,.0f}', 
                                  'FRA': '{:,.4f}%'}
                
                for k, v in format_mapping.items():
                    risk[k] = risk[k].apply(v.format)
            
                # Common Dates Warning
                print('\n')
                print ('Check Banxico Meeting Dates\n')
                print(risk[['Fix_Effect', 'Flt Risk', 'FRA']])
                
            # Irregular Cashflows check 
            
            # Case last tenor  greater than 28
            if (int(swap_CF['Acc_Days'].iloc[-1]) > 28):
                extra_days = int(swap_CF['Acc_Days'].iloc[-1]) - 28
                holiday  = 0
                for ed in range(1,extra_days+1):
                    
                    if ql.Mexico().isHoliday(ql.Date().from_date(
                            swap_CF['End_Date'].iloc[-1])-ed):
                        holiday += 1
                
                if holiday == extra_days:
                    pass
                
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)
            
            # Case last tenor lower than 28
            elif (int(swap_CF['Acc_Days'].iloc[-1]) < 28):
                if swap_CF['Acc_Days'].iloc[-2] > 28:
                    extra_days = int(swap_CF['Acc_Days'].iloc[-2]) - 28
                    holiday  = 0
                    for ed in range(1,extra_days+1):
                        
                        if ql.Mexico().isHoliday(ql.Date().from_date(
                                swap_CF['End_Date'].iloc[-2])-ed):
                            holiday += 1
                            
                    if holiday == extra_days:
                        pass
                    
                    else:
                        print('\n')
                        print('Irregular Cashflows:')
                        print('\n')
                        print(swap_CF)
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)
            
            # Case first Tenor greater than 28
            if (int(swap_CF['Acc_Days'].iloc[0]) > 28):
                extra_days = int(swap_CF['Acc_Days'].iloc[0]) - 28
                holiday  = 0
                
                for ed in range(1,extra_days+1):
                    
                    if ql.Mexico().isHoliday(ql.Date().from_date(
                            swap_CF['End_Date'].iloc[0])-ed):
                        holiday += 1
                        
                if holiday == extra_days:
                    pass
                
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)   
            
            # Case first tenor lower than 28
            elif (int(swap_CF['Acc_Days'].iloc[0]) < 28):
                
                if swap_CF['Acc_Days'].iloc[1] > 28:
                    extra_days = int(swap_CF['Acc_Days'].iloc[1]) - 28
                    holiday  = 0
                    
                    for ed in range(1,extra_days+1):
                        
                        if ql.Mexico().isHoliday(ql.Date().from_date(
                                swap_CF['End_Date'].iloc[1])-ed):
                            holiday += 1
                            
                    if holiday == extra_days:
                        pass
                    
                    else:
                        print('\n')
                        print('Irregular Cashflows:')
                        print('\n')
                        print(swap_CF)
                        
                else:
                    print('\n')
                    print('Irregular Cashflows:')
                    print('\n')
                    print(swap_CF)
            
                
            confos_df_a = fn.output_trade(i, start, maturity, notional, 
                                          rate, swap_valuation, 
                                          flag_mat, blotter = True)
            
            confos_df_a.insert(2,'Client', 
                               np.select([confos_df_a['Side'].str[6] == 'P'],
                                         ['Finamex: REC Fixed MXN IRS'],
                                         ['Finamex: PAY Fixed MXN IRS']))
            
            confos_df_a = confos_df_a.drop(columns = ['Side'])
           
            if (bo == True):
                confos_df_a['Fair_Rate'] =\
                    ["% {:,.4f}".format(fair_rate_bid*100) + ' | ' 
                     + "% {:,.4f}".format(fair_rate_offer*100)]
                bid_rates = original_dftiie['FMX Desk BID'].values
                offer_rates = original_dftiie['FMX Desk OFFER'].values
                rates = ["% {:,.4f}".format(bid_rates[i]) + ' | '
                         + "% {:,.4f}".format(offer_rates[i])
                         for i in range(0, len(bid_rates))]
            else:
                confos_df_a['Fair_Rate'] = [
                    "% {:,.4f}".format(swap_valuation[0].fairRate()*100)]
                
                rates = ["% {:,.4f}".format(r) 
                         for r in  original_tiie_rates]
            
            tenors = dic_data['MXN_TIIE']['Tenor'].values
            tenor_rates = dict(zip(tenors, rates))
            tenor_rates_df = pd.DataFrame(tenor_rates, index=[0])
            hist_price = pd.concat([confos_df_a, tenor_rates_df], axis=1)
            hist_price['NPV'] = [npv]
            hist_price['DV01'] = [flat_dv01/mxn_fx]
            if not collapse_flag:
                if values['Comment 1'] != 0:
                    hist_price['Comment'] = values['Comment 1']
                else:
                    hist_price['Comment'] = ['']
            else:
                hist_price['Comment'] = ['']
            historical_df = pd.concat([historical_df, hist_price])
            rates_krr = ["% {:,.4f}".format(r) 
                     for r in  dic_data['MXN_TIIE']['Quotes'].values]
            
            # KRR
            if (values.Key_Rate_Risk_Check == 'x') or (krr_group_value != 0):
                krrc_f, krrg_f, krrl, df_tenorDV01 = \
                    fn.KRR_helper(i, values, brCrvs, dic_data, npv_group, 
                                  start, maturity, notional, rate)
                
                # If Key_Rate_Risk_Check is True
                if krrc_f:
                    print('\n')
                    print('Key Rate Risk USD$:', '\n')
                    print('Outright DV01: '
                          + f'{(df_tenorDV01/mxn_fx).sum().sum():,.0f}')
                    
                    dv01 = pd.DataFrame((df_tenorDV01/mxn_fx).sum().map(
                        '{:,.0f}'.format))
                    dv01['Quote'] = ['   ' + r for r in rates_krr]
                    print(dv01.to_string(index=True, header=False))
                    
                    
                
                # If Key_Rate_Risk_Group is True
                if krrg_f:
                    krr_list.append(krrl)
                    krr_group = pd.concat([krr_group, df_tenorDV01])
                    abs_dv01s[i] = [krr_group_value, 
                                    np.abs((df_tenorDV01 / mxn_fx).sum()\
                                           .sum())]
        
        # No notional nor NPV nor DV01
        else:
            print(f'\nOutput Trade  {i}')
            print(f'Please check inputs for trade {i}.')
            bad_trades.append(str(i))
    
    # KRR Group Display
    if not krr_group.empty:
        npv_group_df = pd.DataFrame.from_dict(npv_group).T
        npv_group_df = npv_group_df.rename(columns={0: 'KRRG', 1: 'NPV'})
        abs_dv01_group = pd.DataFrame.from_dict(abs_dv01s).T
        abs_dv01_group = abs_dv01_group.rename(columns = {0: 'KRRG', 
                                                          1: 'ABS DV01'})
        
        krr_group.index = krr_list
        krr_group = ((krr_group.groupby(krr_group.index).sum())/mxn_fx).T
        #pd.options.display.float_format = '{:,.0f}'.format
        print('\n')
        print('KRR by Group USD$:')
        krr_group_f = pd.DataFrame(krr_group.applymap('{:,.0f}'.format))
        
        rates = ["% {:,.4f}".format(r) 
                 for r in dic_data['MXN_TIIE']['Quotes'].values]
        krr_group_f['Quote'] = ['   ' + r for r in rates]
        print(krr_group_f.to_string(index=True, header=True))
        #print(krr_group)
        
        for i in list(krr_group.columns):
            npv_group_df_i = npv_group_df.loc[npv_group_df['KRRG'] == i]
            npv_group_df_i = npv_group_df_i['NPV'].sum()
            abs_dv01_group_i = abs_dv01_group.loc[abs_dv01_group['KRRG'] == i]
            abs_dv01_group_i = abs_dv01_group_i['ABS DV01'].sum()
            sum_kkr_g = krr_group[i].sum()
            print('\n')
            print('Outright DV01 Group ' + str(i) + ' USD$: ', 
                  "{:,.0f}".format(sum_kkr_g))
            print('Absolute DV01 Group ' + str(i) + ' USD: ',
                  "{:,.0f}".format(abs_dv01_group_i))
            print('NPV Group ' + str(i) + ' MXN$: ', 
                  "{:,.0f}".format(npv_group_df_i))
            if collapse_flag:
                if 0 in fees:
                    print('\n###### Check CCP. No fees found. ######')
                fee_initial = (max(2000, 
                                   100 * mxn_fx))*parameters_trades.shape[0]
                total_fees = fee_initial + sum(fees)
                upfront_fee = -npv_group_df_i + total_fees
                print('Fees MXN$: ', "{:,.0f}".format(total_fees))
                print('Upfront Fee MXN$: ', "{:,.0f}".format(upfront_fee))
            
        
        # Bad trades in group
        if len(bad_trades)!=0:
            print('\n ###### PLEASE CHECK INPUTS FOR TRADE(S)', 
                  ', '.join(bad_trades), '#######')    
    
    #Spread Fly calculation and display
    if sf_group:
        sf_group_df = pd.DataFrame(sf_group, 
                                   index=['Group', 'FR_BID', 'FR_OFFER', 
                                            'START_DATE', 'END_DATE']).T
        sf_groups = {}
        
        for g in sf_group_df['Group'].unique():
            sf_a = sf_group_df[sf_group_df.Group == g]
            sf_a['Plazo'] = sf_a['END_DATE']-sf_a['START_DATE']
            sf_a = sf_a.sort_values(['END_DATE','Plazo'])
            
            # Spread Calculation
            if sf_a.shape[0] == 2:

                if sf_a['FR_OFFER'].isna().sum() != 0:
                    spread_fr = (sf_a.iloc[1]['FR_BID'] 
                                 - sf_a.iloc[0]['FR_BID'])
                    sf_groups[g] = pd.DataFrame({'Spread': [spread_fr]}, 
                                                index=['Fair Rate'])
                    
                else:
                    spread_fr_bid = (sf_a.iloc[1]['FR_BID'] 
                                     - sf_a.iloc[0]['FR_OFFER'])
                    spread_fr_offer = (sf_a.iloc[1]['FR_OFFER'] 
                                       - sf_a.iloc[0]['FR_BID'])
                    sf_groups[g] = \
                        pd.DataFrame({'Bid Spread': [spread_fr_bid], 
                                      'Offer Spread':[spread_fr_offer]}, 
                                     index=['Fair Rate'])
            
            # Fly calculation
            elif sf_a.shape[0]==3:
    
                if sf_a['FR_OFFER'].isna().sum() != 0:
                    fly_fr = \
                        sf_a.iloc[1]['FR_BID']*2 - \
                            sf_a.iloc[0]['FR_BID'] - sf_a.iloc[2]['FR_BID']

                    sf_groups[g] = pd.DataFrame({'Fly':[fly_fr]}, 
                                                index=['Fair Rate'])
                
                else:
                    fly_fr_bid = \
                        sf_a.iloc[1]['FR_BID']*2 - \
                            sf_a.iloc[0]['FR_OFFER'] - sf_a.iloc[2]['FR_OFFER']
                    fly_fr_offer = \
                        sf_a.iloc[1]['FR_OFFER']*2 - sf_a.iloc[0]['FR_BID'] - \
                            sf_a.iloc[2]['FR_BID']
                    sf_groups[g] = pd.DataFrame({'Bid Fly': [fly_fr_bid], 
                                                 'Offer Fly': [fly_fr_offer]}, 
                                                index=['Fair Rate'])
        # Spread / Fly display            
        if sf_groups:
            sf_groups_keys = list(sf_groups.keys()) 
            sf_groups_keys.sort()
            print('\n')
            print('Spread / Fly by Group \n') 
                  
            for key in sf_groups_keys:   
                print('Group',key)
                sf_d = sf_groups[key].rename(
                    columns={col: "" for col in sf_groups[key]})
                print((sf_groups[key].iloc[0:1,:] * 100).applymap(
                    '%{:,.4f}'.format), '\n')
    
    # Bad Trades Warning Display
    if len(bad_trades)!=0:
        print('\n ###### PLEASE CHECK INPUTS FOR TRADE(S)', 
              ', '.join(bad_trades), '#######')
                 
    # CashFlows excel display
    if collapse_flag:
        cf_sheet = wb.sheets('CF2')
    else:
        cf_sheet = wb.sheets('CF1')
        
    if dic_CF: 
        df_cfOTR = pd.DataFrame()
        
        for k,v in dic_CF.items():
            v['TradeID'] = k
            df_cfOTR = pd.concat([df_cfOTR, v], axis = 0)

        
        #cf_sheet = wb.sheets('CF1')
        range_cf = 'I12:T'+str(cf_sheet.range('I12').end('down').row)
        cf_sheet.range(range_cf).clear_contents()

        df_cfOTR = df_cfOTR.sort_values(by='Date')
        # df_cfOTR['DF'] = np.select([df_cfOTR['End_Date']==evaluation_date],
        #                            [1], default = df_cfOTR['DF'])
        cf_sheet.range('I12').value = df_cfOTR.columns.to_list()
        cf_sheet.range('I13').value = df_cfOTR.values
        cf_sheet.range('C2').value = \
            banxico_TIIE28.iloc[-1]['dato'] * 100
        cf_sheet.range('C3').value = mxn_fx
        
        # print('\n\nCashflows available in CF1 sheet')
    
    
    # Bid Offer Group Display
    if bo_group:
        bo_npv_df = (pd.DataFrame(bo_group).T).rename(
            columns={0:'Group', 1:'NPV Bid', 2:'NPV Offer'}) 
        bo_npv = bo_npv_df.groupby('Group')[['NPV Bid', 'NPV Offer']].sum()
        
        # Receier payer conditions
        conditions = [bo_npv>=0, bo_npv<0]
        choices = [bo_npv.applymap("FMX Pays MXN$ {:,.0f}".format), 
                   (-bo_npv).applymap("FMX Receives MXN$ {:,.0f}".format)]
        
        # Display
        bo_npv_display = pd.DataFrame(np.select(conditions, choices), 
                                      columns=bo_npv.columns, 
                                      index=bo_npv.index)
        
        print('\n\nBid Offer Groups')
        for k,v in bo_npv_display.iterrows():
            try:
                k = int(k)
            except:
                pass
            print(f'{k} ',v['NPV Bid'],' | ',v['NPV Offer'])
        
        # Bad trades warning
        if len(bad_trades)!=0:
            print('\n ###### PLEASE CHECK INPUTS FOR TRADE(S)', 
                  ', '.join(bad_trades), '#######')
    
    # Banxico risks groups warning 
    if not banxico_risks.empty:
        cf_sheet = wb.sheets('CF1')
        scenarios = cf_sheet.range('B5').expand('table').options(
            pd.DataFrame, header=True, index=False).value
        scenarios['Fix Eff'] = scenarios['Fix Eff'].apply(
            lambda t: t.strftime('%Y-%m-%d'))
        scenarios.rename(columns={'Rate': 'Step Rate'}, inplace=True)
        groups = banxico_risks['KRR_Group'].unique()
        groups = [g for g in groups if g!=0]
        groups.sort()
        
        for g in groups:
            banxico_a = banxico_risks[banxico_risks['KRR_Group']==g]
            banxico_group = \
                banxico_a.groupby('Fix_Effect').agg({'Flt Risk':'sum', 
                                                     'FRA':'mean'})
            banxico_group = banxico_group.merge(scenarios[['Fix Eff', 'Step Rate']], 
                                how='left', left_index=True, right_on='Fix Eff')
            banxico_group['Bps'] = (banxico_group['Step Rate']-banxico_group['FRA'])*100
            banxico_group['Banxico Flt Cost'] = banxico_group['Flt Risk']*banxico_group['Bps']
            banxico_group.set_index('Fix Eff', inplace=True)
            banxico_group.drop(columns=['Bps'], inplace=True)
            format_mapping = {'Flt Risk' : 'USD ${:,.0f}', 'FRA': '{:,.4f}%',
                              'Step Rate': '{:,.4f}%', 
                              'Banxico Flt Cost' : 'USD ${:,.0f}'}
            
            for k, v in format_mapping.items():
                banxico_group[k] = banxico_group[k].apply(v.format)
                
            print(f'\nBanxico Net Risk Group {g}\n')
            print(banxico_group.to_markdown())
    
    print('\nNumber of trades: ', parameters_trades.shape[0])
            
    # Historical quotes in Excel file
    if collapse_flag:
        collapse_type = collapse(wb)[1]
        start_date = historical_df['Start Date'].min()
        end_date = historical_df['End Date'].max()
        npv_sum = historical_df['NPV'].sum()
        dv01_sum = historical_df['DV01'].sum()
        dv01_abs = sum([np.abs(i) for i in historical_df['DV01']])
        notional_sum = sum(notionals)
        historical_df = hist_price.copy()
        
        historical_df['Trade'] = ['Collapse: '+str(
            parameters_trades.shape[0]) + ' trades']
        historical_df['Client'] = ['Abs DV01']
        historical_df['Tenor'] = dv01_abs
        historical_df['Start Date'] = [str(ql.Date().from_date(start_date))]
        historical_df['End Date'] = [str(ql.Date().from_date(end_date))]
        historical_df['Notional'] = [notional_sum]
        historical_df['Rate'] = ['']
        historical_df['Fair_Rate'] = ['']
        historical_df['NPV'] = [npv_sum]
        historical_df['DV01'] = [dv01_sum]
        historical_df['Comment'] = collapse_type

        
    hist_pricing = wb.sheets('Historical_Pricing')
    if hist_pricing.range('B3').value is None:
        empty_column = 2
    else:
        empty_column = hist_pricing.range('A3').end('right').column + 1
    now = datetime.now().time()
    now_float = now.hour/24 + now.minute/(24*60) + now.second/(24*3600)
    
    historical_date = pd.to_datetime(hist_pricing.range('B1').value)
    date_flag = False
    if historical_date != evaluation_date:
        date_flag = True
    
    if date_flag:
        hist_pricing.range('B1').value = evaluation_date
        wb.sheets('Confos').range('A1:XFD15').clear_contents()
        end_row = str(hist_pricing.range('A3').end('down').row)
        end_col = hist_pricing.range('A3').end('right').address.split('$')[1]
        end_cell = end_col+end_row
        empty_column = 2
        hist_pricing.range('B3:' + end_cell).clear_contents()
        hist_pricing.range('B3:' + end_cell).color = None
        hist_pricing.range('B3:' + end_cell).font.bold = False
        hist_pricing.range('B1').value = evaluation_date
        
    try:
   
        historical_df['Client'] = historical_df['Client'].apply(
            lambda x: x.replace('Finamex: ', ''))
        if not collapse_flag:
            historical_df['Tenor'] = historical_df['Tenor'].apply(
                lambda x: x.replace('Tenors: ', ''))
            historical_df['Start Date'] = historical_df['Start Date'].apply(
                lambda x: x.replace('Start Date: ', ''))
            historical_df['End Date'] = historical_df['End Date'].apply(
                lambda x: x.replace('End Date: ', ''))
            historical_df['Notional'] = historical_df['Notional'].apply(
                lambda x: x.replace('Notional: MXN$', ''))
        historical_df['Rate'] = historical_df['Rate'].apply(
            lambda x: x.replace('Rate: ', ''))
        hist_pricing.range(3, empty_column).value = historical_df.T.values
    except:
        pass
   

#-------------------------------
#  Short End Pricing Functions
#-------------------------------
           
def proc_ShortEndPricing(crvMXNOIS: ql.DiscountCurve, 
                         crvTIIE: ql.DiscountCurve, wb: xw.Book, 
                         banxico_TIIE28: pd.DataFrame) -> None:
    """Calculates Zero Rates and Discount Factors for TIIE and MXNOIS

    Parameters
    ----------
    crvMXNOIS : ql.DiscountCurve
        MXNOIS DiscountCurve QuantLib object
    crvTIIE : ql.DiscountCurve
        TIIE DiscountCurve QuantLib object
    wb : xw.Book
        excel file book.
    banxico_TIIE28 : pd.DataFrame
        DataFrame with TIIE28 rates of last year

    Returns
    -------
    None
        Displays in the excel file the zero Rates of the TIIE curve and 
        the Discount Factors of the MXNOIS curve in the excel file 
        "TIIE_IRS_Data" in sheet "Short_End_Pricing"
    See Also
    --------
        banxicoData: Creates the banxico_TIIE28 DataFrame

    """

    # Short_End_Pricing sheet
    wb_stir = wb.sheets('Short_End_Pricing')
    wb_stir.activate()
    
     # Check table updating
    date_friday_yst = wb_stir.range('E155').value

    valdt = ql.Settings.instance().evaluationDate

    if date_friday_yst.date != valdt.to_date():

        table_yst = np.array(wb_stir.range('B32').expand('table').value)
        wb_stir.range('B155').value = table_yst

        wb_cf = wb.sheets('CF1')
        wb_cf.api.Calculate()
        cf_yst = np.array(wb_cf.range('B34').expand('table').value)
        wb_cf.range('B59').value = cf_yst
        
    # Read dates data
    # df_StEndDt = pd.DataFrame(wb_stir.range('L72:M72').expand('down').value,
    #                           columns = wb_stir.range('L71:M71').value)
    valdt = ql.Settings.instance().evaluationDate
    std = ql.Mexico().advance(valdt, ql.Period(1, ql.Days))
    end_date = std + 5*13*28
    schdl = ql.Schedule(std, end_date, ql.Period(13), ql.Mexico(), 
                        ql.Following, ql.Following, 0, False)
    
    # Fwd & Discount Rates
    fwdRates = []
    discF = []
    start_dates = []
    end_dates = []
    dates = [d for i, d in enumerate(schdl)]
    # Fwd and Discount Rates calculation
    for d in range(1, len(dates)):
        fwdRates.append(crvTIIE.forwardRate(dates[d-1], 
                                            dates[d],
                                            ql.Actual360(),
                                            ql.Simple).rate())
        discF.append(crvMXNOIS.discount(dates[d]))
        start_dates.append(dates[d-1].to_date())
        end_dates.append(dates[d].to_date())
        
    df_StEndDt = pd.DataFrame({'Start': start_dates, 'End': end_dates})
    df_StEndDt['FltRate'] = fwdRates
    df_StEndDt['DF'] = discF
    # mintiie = df_StEndDt.nsmallest(1,'FltRate')[['End', 'FltRate']]
    # Update Rates
    wb_stir.range('L72').value = df_StEndDt[['Start', 'End']].values
    wb_stir.range('O72').value = df_StEndDt[['FltRate']].values
    wb_stir.range('U72').value = df_StEndDt[['DF']].values
    wb_stir.range('D2').value = banxico_TIIE28.iloc[-1]['dato']*100
    
    wb_stir.api.Calculate()
    wb_cf.api.Calculate()
                             
    mondy_st = wb_stir.range('D114').value
    mondy_ql = ql.Date.from_date(mondy_st)
    ed_ql = mondy_ql + 28*13*3
    
    schdl2 = ql.Schedule(mondy_ql , ed_ql, ql.Period(13), ql.Mexico(), 
                        ql.Following, ql.Following, 0, False)
    
    dates2 = [d.to_date() for i, d in enumerate(schdl2)]
    dates2s = pd.Series(dates2[1:-1])
    dates2e = pd.Series(dates2[1:])
    wb_stir.range('D115').value = dates2s.values.reshape(-1,1)
    wb_stir.range('E114').value = dates2e.values.reshape(-1,1)
    
    wb_stir.api.Calculate()
    
    
def proc_ShortEndPricing_byMPC(crvTIIE: ql.DiscountCurve, wb: xw.Book) -> None:
    '''Calculates Forward Rates for MPC meeting dates
    

    Parameters
    ----------
    crvTIIE : ql.DiscountCurve
        TIIE Discount Curve QuantLib Object
    wb : xw.Book
        excel file book.

    Returns
    -------
    None
        Displays the forward rates of TIIE by MPC meeting date in the 
        excel file "TIIE_IRS_Data" in sheet "Short_End_Pricing"

    '''
    
    # Short_End_Pricing sheet
    wb_stir = wb.sheets('Short_End_Pricing')
    wb_stir.activate()
    
    # Banxico Meeting Dates
    mpcdates = wb_stir.range('B4:B28').value
    
    # Get Fwd TIIE28 Rates
    mx_cal = ql.Mexico()
    lst_ftiie28 = []
    
    for mtngdate in mpcdates:
        qldate = ql.Date(mtngdate.day, mtngdate.month, mtngdate.year)
        stdt = mx_cal.advance(qldate,1,ql.Days)
        eddt = stdt + ql.Period('28D')
        lst_ftiie28.append(
            [crvTIIE.forwardRate(stdt,eddt,ql.Actual360(),ql.Simple).rate()])
        
    # Update FwdRates
    wb_stir.range('E4:E28').value = lst_ftiie28
    
#---------------------
#  Collapse Function
#---------------------

def collapse(wb: xw.Book) -> pd.DataFrame:
    """Creates a DataFrame with the parameters for a swap trade pricing
    

    Parameters
    ----------
    wb : xw.Book
        xlwings Book object that emulates an excel book

    Returns
    -------
    collapse_df : pd.DataFrame
        DataFrame with the instrcutios to price swaps

    """
    # Collapse sheet
    # collapse_sheet = wb.sheets('Collapse')
    
    # # Collapse parameters
    # rango = collapse_sheet.range('A1').end('right').address[:-1] +\
    #     str(collapse_sheet.range('A1').end('down').row)
    # collapse_df = collapse_sheet.range('A1', rango).options(
    #     pd.DataFrame, header=1, index=False).value
    
    folders_dict = {'O': 'Oscar', 'L': 'Leti', 'I': 'Inaki', 'J': 'Maza', 
                    'P': 'Paco', 'B': 'Beto', 'E': 'GabyEsteban', 
                    'G': 'GabyEsteban'}
    
    user_id = wb.sheets('Blotter').range('A2').value[0]
    
    path = r'//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/'\
        r'Collapse/' + folders_dict[user_id]

    files = os.listdir(path)
    good_files = [f for f in files if '.xlsx' in f or '.csv' in f]
    paths = [os.path.join(path, basename) for basename in good_files]
    folder_name = folders_dict[user_id]
    
    try:
        last_collapse = max(paths, key=os.path.getmtime)
    except:
        print('\n###### NO COLLAPSE FILE FOUND. ######')
        return None

    if last_collapse.endswith('.csv'):
        try:
            collapse_df = pd.read_csv(last_collapse)
        except:
            print('Corrupted csv file. Please open and save the file and try '\
                  'again.')
            return None
        
    elif last_collapse.endswith('.xlsx'):
        try:
            collapse_df = pd.read_excel(last_collapse)
        except:
            print('Corrupted Excel file. Please open and save the file and '\
                  'try again.')
            return None
    raro_flag = False
    
    # BBG collapse
    if 'Trade Key' in collapse_df.columns:
        
    # Dictionary to rename columns
        column_names = {'Effective Date': 'Start_Date', 
                        'Maturity Date': 'End_Date', 'Coupon': 'Rate', 
                        'Notional': 'Notional_MXN', 'Bloomberg NPV':'NPV_MXN', 
                        'DV01': 'DV01_USD'}
        collapse_df = collapse_df.rename(columns = column_names)
        # Fill Notional with appropriate sign
        collapse_df['Notional_MXN'] = np.select(
            [collapse_df['Side']=='PAY'], [-collapse_df['Notional_MXN']], 
            default=collapse_df['Notional_MXN'])
        collapse_type = 'BBG'
        
        if (not collapse_df['RECEIVE Stub Type'].isna().all() or 
            not collapse_df['PAY Stub Type'].isna().all()):
            
            raro_flag = True
            
        
    # TradeWeb collapse    
    elif 'TNum' in collapse_df.columns[0]:
        
        
                  
            
        if 'Direction 1' in collapse_df.columns:
            column_names = {'Rate/Spread': 'Rate', 
                            'Effective Date': 'Start_Date',
                            'End Date (Unadjusted)': 'End_Date', 
                            'Direction 1': 'Side',
                            'Notional': 'Notional_MXN', 'Clr Service': 'CCP'}
        else:
        
            # Dictionary to rename columns
            column_names = {'Rate/Spread': 'Rate', 
                            'Effective Date': 'Start_Date',
                            'End Date (Unadjusted)': 'End_Date', 
                            'Pay/Rcv': 'Side',
                            'Notional': 'Notional_MXN', 'Clr Service': 'CCP'}
        collapse_df = collapse_df.rename(columns = column_names)
        # Fill Notional with appropriate sign
        collapse_df['Notional_MXN'] = np.select(
            [collapse_df['Side']=='CRCV'], [-collapse_df['Notional_MXN']], 
            default=collapse_df['Notional_MXN'])
        collapse_type = 'TradeWeb'
        
        if not collapse_df['Fixed Stub Type'].isna().all():
            raro_flag = True
            
            
    
    else:
        print('\n#### PLEASE CHECK YOUR LAST CREATED FILE IS A COLLAPSE '\
              'FILE. ####')
        return None
    
    lch_flag = 'LCH' in collapse_df['CCP'].tolist()    
    collapse_df.index = collapse_df.index + 1
    
    # Convert dates to datetime using month first
    collapse_df['Start_Date'] = pd.to_datetime(collapse_df['Start_Date'], 
                                               dayfirst=False)
    collapse_df['End_Date'] = pd.to_datetime(collapse_df['End_Date'], 
                                             dayfirst=False)
    
    # Fill NPV and DV01 with zero
    collapse_df['NPV_MXN']=0
    collapse_df['DV01_USD']=0
    
    # Standard swap format
    collapse_df['Rate'] = collapse_df['Rate']/100
    collapse_df['Valuation_Check'] = 'x'
    collapse_df['Key_Rate_Risk_Check'] = None
    collapse_df['Bid_Offer_Check'] = 'x'
    collapse_df['Start_Tenor'] = None
    collapse_df['Fwd_Tenor'] = None
    collapse_df['Date_Generation'] = 'Backward'
    
    # Group all trades in same KRR and bid offer group
    collapse_df['KRR_Group'] = 1
    collapse_df['Bid_Offer_Group'] = 1
    
    # Spread/Fly and cashflows
    collapse_df['Spread_Fly'] = None
    collapse_df['Cashflows'] = 'x'
    collapse_df['Comment 1'] = None
    
    if raro_flag and 'TNum' in collapse_df.columns:
        raro_df = collapse_df[~collapse_df['Fixed Stub Type'].isna()][
            ['TNum', 'Start_Date', 'End_Date', 'Notional_MXN', 'Rate',
             'Fixed Stub Type']]
    
    elif raro_flag and 'Leg Index' in collapse_df.columns:
        raro_df = collapse_df[~collapse_df['Fixed Stub Type'].isna()][
            ['Leg Index', 'Start_Date', 'End_Date', 'Notional_MXN', 'Rate',
             'PAY Stub Type', 'RECEIVE Stub Type']]
        
    
    else:
        raro_df = pd.DataFrame(columns = collapse_df.columns)
    
    
    
    return collapse_df, collapse_type, folder_name, lch_flag, raro_df

#--------------------
#  Blotter Function
#--------------------
        
def tiie_blotter(dic_data: dict, wb: xw.Book, g_crvs: list, 
                 banxico_TIIE28: pd.DataFrame, bo_engines: list, 
                 g_engines: list, dv01_engines: list, 
                 gran: bool = False) -> None:
    """It wil price all swaps in the blotter sheet
    
    This function will price any swap required by the user from the 
    file TIIE_IRS_Data.xlsm in the blotter sheet.

    Parameters
    ----------
    dic_data : dict
        Dictionary with the necessary quotes to price the required swaps
    wb : xw.Book
        xlwings Book object that emulates an excel book
    g_crvs : list
        list of QuantLib DiscountCurve objects 
    banxico_TIIE28 : pd.DataFrame
        Dataframe with TIIE28 quotes from the last 10 years
    bo_engines : list
        List of QuantLib object necessary to price swaps with bid and 
        offer differences
    g_engines : list
        list of QuantLib object necessary to price swaps   
    dv01_engines : list
        list of QuantLib object necessary to calculate dv01s of a 
        certain swap
  

    Returns
    -------
    None
        This function writes in the excel file the KRR for every swap 
        and updates the Risk Sheet with the old and new KRRs. 
        
    See Also
    --------   
        createCurves: Creates the g_curves list
        banxicoData: Creates the banxico_TIIE28 Dataframe
        fn.bid_offer_crvs: creates the bo_engines list
        engines: Creates the g_engines list
        fn.start_end_dates_trading: Calculates start and end dates for a
                                    certain swap
        fn.flat_dv01_curves: Creates the dv01_engines list
        fn.tiieSwap: Creates vanilla swap object and the cashflows list
        fn.flat_DV01_calc: Claculates th dv01 for a certain swap
        fn.KRR_helper:  Key Rate Risk calulation for a certain swap
        

    """
    
    # Blotter sheet
    blotter = wb.sheets('Blotter')
    blotter.activate()
    blotter.range('A2:M10000').color = (255,255,255)
    parameters = wb.sheets('Pricing')
    
    # Exchange rate
    mxn_fx = parameters.range('F1').value
    
    num_cols = len(dic_data['MXN_TIIE']['Tenor'])
    
    # Risk sheet and book
    risk_sheet = wb.sheets('Risk')
    book = risk_sheet.range('B2:B2').value
    risk_sheet.range('C4').clear_contents()
    evaluation_date = pd.to_datetime(parameters.range('B1').value)
    print(f'\nLooking for book {book} Initial Key Rate Risk ...')
    risk_flag = False
    
    # Search book risk or future book risk
    try:
        
        if type(book) == str:
            dv01_dict = load_obj('DailyPnL/Future Risks/risk_' + 
                                 evaluation_date.strftime('%Y%m%d'))
        else:
            if not gran:
                dv01_dict = load_obj(r'//tlaloc/Cuantitativa/Fixed Income' 
                                     + '/TIIE IRS Valuation Tool/Blotter' 
                                     + '/Historical Risks/risk_' + 
                                     evaluation_date.strftime('%Y%m%d'))
            else:
                dv01_dict = load_obj(r'//tlaloc/Cuantitativa/Fixed Income' 
                                     + '/TIIE IRS Valuation Tool/Blotter' 
                                     + '/Historical Risks/grisk_' + 
                                     evaluation_date.strftime('%Y%m%d'))
                try:
                    carry = dv01_dict[int(book)]['Carry']
                except:
                    carry = pd.DataFrame(columns=['Carry'])
            book = int(book)
        
        try:
            dv01_df = dv01_dict[book]['DV01_Book']
            dv01_df = pd.DataFrame(dv01_df.iloc[1:])
            print('Risk OK')
        
        except:
            print(f'Book {book} Initial Key Rate Risk not found. ' 
                  + f'Please run TIIE Portfolio Risk code for book {book}, ' 
                  + 'or input values manually.\n')
            dv01_df = pd.DataFrame([0]*num_cols)
            risk_flag = True
            
    except:
        dv01_df = pd.DataFrame([0]*num_cols)
        print('Key Rate Risk file not found. '
              + '\nPlease run the TIIE_PfolioMgmt_Risk_book.py code.')
        risk_flag = True
    
    # String book for further operations
    book_str = str(book)
    
    # Case future risk
    if 'F' in book_str:
        
        try:
            future_date = dv01_dict['FutureDate']
            
        except:
            print('\nFuture date not found.')
            dateIsNotOk = True
            
            # Input future date in case future risk not found
            while dateIsNotOk:
                print('\n\nPlease enter future date: ')
                finput_year = int(input('\tYear: '))
                finput_month = int(input('\tMonth: '))
                finput_day = int(input('\tDay: '))
                
                try:
                    future_date = date(finput_year, finput_month, finput_day)
                    dateIsNotOk = False
                    
                except:
                    print('Wrong date! Try again pls.')
                    dateIsNotOk = True
        
        # Change settings instance to future date
        risk_sheet.range('C4').value = pd.to_datetime(future_date)
        evaluation_date_fut = pd.to_datetime(future_date)
        ql.Settings.instance().evaluationDate = \
            ql.Date().from_date(evaluation_date_fut)
        final_date = pd.to_datetime(future_date)
        
        # Future rates
        future_rates = wb.sheets(
            'Short_End_Pricing').range('B2:I28').options(pd.DataFrame, 
                                                         header=1, 
                                                         index=False).value
        future_rates = future_rates.iloc[1:26]
        future_rates['Rate'] = future_rates['Rate'].astype(float)/100
        future_rates = future_rates[['MPC Meetings', 'Fix Eff', 'Rate']]
        
        # Fill TIIE 28 with future rates
        banxico_TIIE28_a = pd.DataFrame(
            {'fecha': pd.date_range(banxico_TIIE28.iloc[-1]['fecha'] 
                                    + timedelta(days=1), 
                                    future_rates.iloc[0]['MPC Meetings'], 
                                    freq='d'), 
             'dato' : banxico_TIIE28.iloc[-1]['dato']})
        banxico_TIIE28 = pd.concat([banxico_TIIE28, banxico_TIIE28_a], 
                                   ignore_index = True)
        
        for k in range(future_rates.shape[0]-1):
            banxico_TIIE28_a = pd.DataFrame(
                {'fecha': pd.date_range(future_rates.iloc[k]['Fix Eff'], 
                                        future_rates.iloc[k+1]['MPC Meetings'], 
                                        freq='d'), 
                 'dato': future_rates.iloc[k]['Rate']})
            banxico_TIIE28 = pd.concat([banxico_TIIE28, banxico_TIIE28_a], 
                                       ignore_index = True)
        
        banxico_TIIE28['fecha'] = pd.to_datetime(banxico_TIIE28['fecha'])
        
        # Remove non-business days
        banxico_business_dates = \
            [banxico_TIIE28.iloc[k]['fecha'] 
             for k in range(banxico_TIIE28.shape[0]) 
             
             if ql.Mexico().isBusinessDay(
                     ql.Date().from_date(banxico_TIIE28.iloc[k]['fecha']))]
    
        banxico_TIIE28 = banxico_TIIE28[
            (banxico_TIIE28['fecha'] <= final_date) 
            & banxico_TIIE28['fecha'].isin(banxico_business_dates)]
    
    # Case blotter sheet empty
    if blotter.range('H2').value is None:
        columns = blotter.range(
            'A1:' + blotter.range('A1').end('right').address).value
        blotter.range('N2:BI10000').clear_contents()
        parameters_trades = pd.DataFrame(columns = columns[1:])
    
    # Case blotter sheet not empty
    else:
        range_trades = blotter.range('A1').end('right').address[:-1] + \
            str(blotter.range('H1').end('down').row)
        parameters_trades = blotter.range('A1',range_trades).options(
            pd.DataFrame, header=1).value
    
    parameters_trades['Key_Rate_Risk_Check'] = 0
    parameters_trades['KRR_Group'] = 0
    parameters_trades = parameters_trades.fillna(0)
    
    
    # Global Curves
    crvMXNOIS = g_crvs[2]
    brCrvs = g_crvs[4]
    
    # Global Engines
    ibor_tiie = g_engines[0]
    ibor_tiie.clearFixings()
    
    for h in range(len(banxico_TIIE28['fecha']) - 1):
        dt_fixing = pd.to_datetime(banxico_TIIE28.iloc[h]['fecha'])
        ibor_tiie.addFixing(
            ql.Date(dt_fixing.day, dt_fixing.month, dt_fixing.year), 
            banxico_TIIE28.iloc[h+1]['dato']
            )
    
    tiie_swp_engine = g_engines[1]
    
    # Bid Offer Engines
    ibor_tiie_bid = bo_engines[0]
    tiie_swp_engine_bid = bo_engines[1]
    ibor_tiie_offer = bo_engines[2]
    tiie_swp_engine_offer = bo_engines[3]
    
    #Flat Dv01 engines
    ibor_tiie_plus = dv01_engines[0]
    tiie_swp_engine_plus = dv01_engines[1]
    ibor_tiie_minus = dv01_engines[2]
    tiie_swp_engine_minus = dv01_engines[3]
    
    npv_group = {}
    npvs = []
    dv01s = []
    fair_rates = []
    
    # Tenor list
    krrs = pd.DataFrame(columns = dic_data['MXN_TIIE']['Tenor'].tolist())
    
    # Confirmation DataFrame
    confos_df = pd.DataFrame()

    for i, values in parameters_trades.iterrows():
        
        # Base Case
        banxico_flag = False
        
        if values.NPV_MXN == 0 and values.DV01_USD == 0:
                    
            # Swap Data
            try:
                start, maturity, flag_mat = \
                    fn.start_end_dates_trading(values, evaluation_date)
            
            # Highlight wrong inputs
            except:
                print(f'\nOutput Trade  {i}')
                print(f'Please check inputs for trade {i}.')
                row=str(int(i[1:])+1)
                blotter.range('C'+row+':F'+row).color = (247,213,7)
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat(
                    [krrs, 
                     pd.DataFrame(
                         np.array([0]*num_cols).reshape(1,-1), 
                         columns= dic_data['MXN_TIIE']['Tenor'].tolist())
                     ])
                continue
            
            notional = values.Notional_MXN
            
            # Fix notional in case of string value
            try:
                if type(notional) == str:
                    notional = float(notional.replace(',', ''))
                
                else:
                    notional = float(notional)
                    
            # Highlight wrong input        
            except:
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat([krrs, pd.DataFrame(
                    np.array([0]*num_cols).reshape(1,-1), 
                    columns=dic_data['MXN_TIIE']['Tenor'].tolist())])
                print(f'\nOutput Trade  {i}')
                print(f'Please check notional for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                row=str(int(i[1:])+1)
                blotter.range('G'+row).color = (247,213,7)
                continue
          
            rate = values.Rate
            
            # Convert rate to float
            try:  
                rate = float(rate)
            
            # Highlight wrong inputs
            except:
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat([krrs, pd.DataFrame(
                    np.array([0]*num_cols).reshape(1,-1), 
                    columns=dic_data['MXN_TIIE']['Tenor'].tolist())])
                print(f'\nOutput Trade  {i}')
                print(f'Please check rate for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                row=str(int(i[1:])+1)
                blotter.range('H'+row).color = (247,213,7)
                continue
            
            # Swap side definition
            if notional >= 0:
                typ = ql.VanillaSwap.Receiver
                
            else:
                typ = ql.VanillaSwap.Payer
            
            #Date generation rule
            if values.Date_Generation == 'Forward':
                rule = ql.DateGeneration.Forward
                
            else:
                rule = ql.DateGeneration.Backward
            
            # Swaps construction
            swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                         ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # NPV and fair rate
            npv = swap_valuation[0].NPV() 
            fair_rate = swap_valuation[0].fairRate()
            
            npv_group[i] = [0, npv]
            npvs.append(npv)
            fair_rates.append(fair_rate)
            
            # DV01 
            flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)
            
            # Original DV01
            flat_dv01_fr = fn.tiieSwap(start, maturity, abs(notional), 
                                       ibor_tiie, swap_valuation[0].fairRate() 
                                       + .0001, typ, rule)
            flat_dv01_fr[0].setPricingEngine(tiie_swp_engine)
            original_dv01 = abs(flat_dv01_fr[0].NPV()/mxn_fx) * \
                np.sign(flat_dv01)
            original_dv01_mxn = abs(flat_dv01_fr[0].NPV()) * np.sign(flat_dv01)
    
            dv01s.append(flat_dv01/mxn_fx)
            
            # KRR
            krrc_f, krrg_f, krrl, df_tenorDV01 = fn.KRR_helper(
                i, values, brCrvs, dic_data, npv_group, start, maturity, 
                notional, rate)
            krrs = pd.concat([krrs, df_tenorDV01/mxn_fx])
            
            # Confirmation
            confos_df_a=fn.output_trade(i, start, maturity, notional, rate, 
                                        swap_valuation, False, False, True)
            confos_df_a.insert(2,'Client', np.select(
                [confos_df_a['Side'].str[6]=='P'], 
                ['Finamex: REC Fixed MXN IRS'], ['Finamex: PAY Fixed MXN IRS'])
                )
            
            try:
                confos_df_a['Side'] = \
                    confos_df_a['Side'].str.replace('Side',values.Cpty)
            except:
                pass
            
            # Fees
            if values.Upfront_Fee_MXN != 0:
                
                # Case positive fee
                if values.Upfront_Fee_MXN > 0:
                    confos_df_a['Fee'] = [f'Upfront Fee: {values.Cpty} Pays ' 
                                          + '${:,.0f} MXN'.format(
                                              values.Upfront_Fee_MXN)]
                
                # Case negative fee
                else:
                    confos_df_a['Fee'] = [
                        f'Upfront Fee: {values.Cpty} Receives ' 
                        + '${:,.0f} MXN'.format(values.Upfront_Fee_MXN)]
            # Case no fee        
            else:
                confos_df_a['Fee'] = [np.nan]

            confos_df = pd.concat([confos_df, confos_df_a])
            
        # Notional unknown
        elif values.Notional_MXN == 0:
            
            # Swap Data
            try:
                start, maturity, flag_mat = fn.start_end_dates_trading(
                    values, evaluation_date)
            
            # Highlight wrong inputs
            except:
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat([
                    krrs, pd.DataFrame(
                        np.array([0]*num_cols).reshape(1,-1), 
                        columns=dic_data['MXN_TIIE']['Tenor'].tolist())]
                    )
                print(f'\nOutput Trade  {i}')
                print(f'Please check inputs for trade {i}.')
                row=str(int(i[1:])+1)
                blotter.range('C'+row+':F'+row).color = (247,213,7)
                continue
            
            notional = 100000000
            rate = values.Rate
            
            # Rate
            try:  
                rate = float(rate)
            
            # Highlight wrong inputs
            except:
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat(
                    [krrs,pd.DataFrame(
                        np.array([0]*num_cols).reshape(1,-1), 
                        columns= dic_data['MXN_TIIE']['Tenor'].tolist())]
                    )
                print(f'\nOutput Trade  {i}')
                print(f'Please check rate for trade {i}.')
                row=str(int(i[1:])+1)
                blotter.range('H'+row).color = (247,213,7)
                continue
            
            rule = ql.DateGeneration.Backward
            typ = ql.VanillaSwap.Receiver
           
            # Swaps construction
            swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                         ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # Case rate == 0 
            if rate == 0:
                rate = swap_valuation[0].fairRate()
    
            # Dummy DV01
            flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)            
            dv01_100mn = flat_dv01/mxn_fx
            npv_100mn = swap_valuation[0].NPV()
            
            # New Swap Data
            dv01_value = values.DV01_USD
            
            try:
                # Fix DV01 in case it is a string value
                if type(dv01_value) == str:
                    dv01_value = float(dv01_value.replace(',', ''))
                
                else:
                    dv01_value = float(dv01_value)
                    
            # Highlight wrong inputs        
            except:
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat([
                    krrs,pd.DataFrame(
                        np.array([0]*num_cols).reshape(1,-1), 
                        columns=dic_data['MXN_TIIE']['Tenor'].tolist())]
                    )
                print(f'\nOutput Trade  {i}')
                print(f'Please check DV01_USD for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                row=str(int(i[1:])+1)
                blotter.range('J'+row).color = (247,213,7)
                continue
            
            # NPV
            npv_value = values.NPV_MXN
            
            try:
                
                # Fix NPV value in case it is a string
                if type(npv_value) == str:
                    npv_value = float(npv_value.replace(',', ''))
                
                else:
                    npv_value = float(npv_value)
            
            # Highlight wrong inputs
            except:
                npvs.append(0)
                fair_rates.append(0)
                dv01s.append(0)
                npv_group[i]=[0,0]
                krrs = pd.concat(
                    [krrs,pd.DataFrame(
                        np.array([0]*num_cols).reshape(1,-1), 
                        columns= dic_data['MXN_TIIE']['Tenor'].tolist())]
                    )
                print(f'\nOutput Trade  {i}')
                print(f'Please check NPV_MXN for trade {i}. '
                      + 'Maybe there are unnecessary spaces.')
                row=str(int(i[1:])+1)
                blotter.range('I'+row).color = (247,213,7)
                continue
            
            # Notional calculation
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
            
            # Swaps construction
            swap_valuation = fn.tiieSwap(start, maturity, abs(notional), 
                                         ibor_tiie, rate, typ, rule)
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            # NPV and fair rate
            npv = swap_valuation[0].NPV() 
            fair_rate = swap_valuation[0].fairRate()
            fair_rates.append(fair_rate)

            # Flat dv01
            flat_dv01 = fn.flat_DV01_calc(ibor_tiie_plus, tiie_swp_engine_plus, 
                                          ibor_tiie_minus, 
                                          tiie_swp_engine_minus, start, 
                                          maturity, abs(notional), rate, typ, 
                                          rule)
            
            # Original DV01
            flat_dv01_fr = fn.tiieSwap(start, maturity, abs(notional), 
                                       ibor_tiie, swap_valuation[0].fairRate() 
                                       + 0.0001, typ, rule)
            flat_dv01_fr[0].setPricingEngine(tiie_swp_engine)
            original_dv01 = abs(flat_dv01_fr[0].NPV()/mxn_fx) * \
                np.sign(flat_dv01)
            original_dv01_mxn = abs(flat_dv01_fr[0].NPV()) * np.sign(flat_dv01)
            
            npv_group[i] = [0, npv]
            npvs.append(npv)
            dv01s.append(flat_dv01/mxn_fx)
                
             # KRR
            krrc_f, krrg_f, krrl, df_tenorDV01 = fn.KRR_helper(
                i, values, brCrvs, dic_data, npv_group, start, maturity, 
                notional, rate)
            krrs = pd.concat([krrs, df_tenorDV01/mxn_fx])
            
            # Confiramtions
            confos_df_a=fn.output_trade(i, start, maturity, notional, rate, 
                                        swap_valuation, False, False, True)
            confos_df_a.insert(2,'Client', 
                               np.select([confos_df_a['Side'].str[6] == 'P'],
                                         ['Finamex: REC Fixed MXN IRS'],
                                         ['Finamex: PAY Fixed MXN IRS']))
            try:
                confos_df_a['Side'] = \
                    confos_df_a['Side'].str.replace('Side',values.Cpty)
            except:
                pass
            confos_df = pd.concat([confos_df, confos_df_a])
        
        # Bad case
        else:
            npvs.append(0)
            fair_rates.append(0)
            dv01s.append(0)
            npv_group[i]=[0,0]
            krrs = pd.concat(
                [krrs,pd.DataFrame(
                    np.array([0]*num_cols).reshape(1,-1), 
                    columns=dic_data['MXN_TIIE']['Tenor'].tolist())]
                )
            print(f'\nOutput Trade  {i}')
            print(f'Please check inputs for trade {i}.')
            row = str(int(i[1:])+1)
            blotter.range('G'+row).color = (247,213,7)
            blotter.range('I'+row+':J'+row).color = (247,213,7)
            continue
        
        # Highlight trades with holiday maturity date
        if ql.Mexico().isHoliday(maturity):
            row = int(i[1:])+1
            blotter.range('A'+str(row)+':M'+str(row)).color = (255,204,255)
    
    # Add fair rate, NPV, and DV01
    krrs.insert(0, 'Fair Rate', fair_rates)
    krrs.insert(1,'MXN_NPV',npvs)   
    krrs.insert(2,'USD_DV01', dv01s)
    
    # Books numbers
    books = parameters_trades.Book
    
    # Fill blotter sheet
    if not gran:
        blotter.range('N2:AE10000').clear_contents()
        blotter.range('N2').value = krrs.values
    else:
        blotter.range('AH2:BI10000').clear_contents()
        blotter.range('AH2').value = krrs.values
    
    # Add books and fees
    krrs['Book'] = books
    krrs['NPV_Fees'] = krrs.MXN_NPV + parameters_trades.Upfront_Fee_MXN.values
    
    # Case future book
    if type(book) == str:
        if 'F' in book:
            book_f = book
            book = int(book_f[:-1])
    
    # KRR for book
    krrs_book = krrs[krrs['Book'] == book]
    
    # Sum of risk
    risk_sum = krrs_book.sum()
    risk_sum_df = pd.DataFrame(risk_sum[3:-2])
    sum_npv = sum((krrs_book.NPV_Fees).values)
    
    # Other blotters
    key_letter = blotter.range('A2').value[0]
    key_letters =['O','J','I','L','A','P', 'B']
    desk_blotter =pd.DataFrame()
    desk_blotter.index.name = 'Trade_#'
    
    # Concatenate other blotters
    for k in key_letters:
        
        if k != key_letter:
            
            # Check if other blotters exist
            try:
                df_a = pd.read_excel(r'//TLALOC/Cuantitativa/Fixed Income'
                                   + '/TIIE IRS Valuation Tool/Blotter'
                                   + '/desk_blotter_'+ k + '_' 
                                   + evaluation_date.strftime('%Y%m%d') 
                                   + '.xlsx', 'Blotter', index_col=0)
                desk_blotter = pd.concat([desk_blotter, df_a])
        
            except:
                continue
        
    # Fill Desk Blotter sheet    
    dsk_blt = wb.sheets('Desk_Blotter')
    dsk_blt.range('A2:AE10000').clear_contents()
    dsk_blt.activate()
    dsk_blt.range('A1').value = desk_blotter.replace(0, np.nan)   
    
    # Write risk excel
    if not gran:
        risk_sheet = wb.sheets('Risk')
        riskrange = 'D6:D20'
        totrange = 'P21'
        
    else:
        risk_sheet = wb.sheets('Granular_Risk')
        riskrange = 'D6:D30'
        totrange = 'P31'
        risk_sheet.range('R6').value = carry[['Carry']].values
        
        
    time.sleep(2)
    risk_sheet.activate()
    if not risk_flag:
        risk_sheet.range('C6').value = dv01_df.values
    
    # Save last risk
    
    last_risk_row = risk_sheet.range('B5').end('down').row
    last_risk = risk_sheet.range('B5:I'+str(last_risk_row)).options(
        pd.DataFrame, header=1, index=False).value
    with pd.ExcelWriter(r'//TLALOC/Cuantitativa/Fixed Income'
                        + r'/TIIE IRS Valuation Tool/Blotter/desk_blotter_'
                        + key_letter+'_'+evaluation_date.strftime('%Y%m%d')
                        + '.xlsx', engine="openpyxl", mode='w') as writer:  
        
        last_risk.to_excel(writer, sheet_name='Last_risk', index=False)
    
    # Case blotter empty
    if parameters_trades.empty:
        risk_sheet.range(riskrange).value = 0
    
    # Case blotter not empty
    else:  
        risk_sheet.range('D6').value = risk_sum_df.values    
        blotter_self = parameters_trades[
            parameters_trades.columns[:12]].replace(0, np.nan)
        
        # Save blotter values
        with pd.ExcelWriter(r'//TLALOC/Cuantitativa/Fixed Income'
                            + r'/TIIE IRS Valuation Tool/Blotter/desk_blotter_'
                            + key_letter+'_'+evaluation_date.strftime('%Y%m%d')
                            + '.xlsx', engine="openpyxl", mode='a') as writer:  
            blotter_self.to_excel(writer, sheet_name='Blotter')
    
    time.sleep(1)
    
    # Fill sum of NPVs in Excel file and recalculate sheet
    risk_sheet.range(totrange).value = sum_npv/mxn_fx
    risk_sheet.range('A1:AZ10000').api.Calculate()
    
    #Fill confirmations sheet
    confos = wb.sheets('Confos')
    confos.range('A2:XFD15').clear_contents()
    try:
        confos_df = confos_df.drop(columns=['Time'])
    except:
        pass
    confos.range('A2').value = confos_df.T.values
    time.sleep(0.5)
    col = confos.range('A1').end('right').column
    
    if (col > 16300 and confos.range('A1').value is None):
        initial_col = 1
    else:
        if col > 16300:
            col = 1
        initial_col = col + 1
        
    last_col = confos.range('A2').end('right').column
    
    if (last_col > 16300 and confos.range('A2').value is None):
        pass
       
    else:
        if last_col > 16300:
            last_col = 1
        total_cols = range(initial_col, last_col+1)
        for c in total_cols:
            confos.range(1, c).value = str(datetime.now().time())
    
    
    return(krrs, npv_group, dv01s)

#---------------------------
#  Upload Blotter Function
#---------------------------    

def check_date(d):
    try:
        d = d.strftime('%Y-%m-%d')
    except:
        pass
    try:
        d = d.split(' ')[0]
    except:
        pass
    try:
        d.replace('/', '-')
    except:
        pass
    
    return d

def fill_uti(blotter_auto_new, user, evaluation_date):
    
    blotter_auto_new.reset_index(drop=True, inplace=True)
    folders_dict = {'O': 'Oscar', 'L': 'Leti', 'I': 'Inaki', 'J': 'Maza', 
                    'P': 'Paco', 'B': 'Beto', 'E': 'GabyEsteban', 
                    'G': 'GabyEsteban'}
    
    path = r'//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/'\
        r'Collapse/' + folders_dict[user]
    #path = f'C:/Users/{user}/Downloads/'
    files = os.listdir(path)
    good_files = [f for f in files if 'IntraTrades' in f]
    paths = [os.path.join(path, basename) for basename in good_files]
    last_export = max(paths, key=os.path.getmtime)
    
    used_utis = [u for u in blotter_auto_new['UTI'].tolist() if u!=None]
    uti_file = pd.read_csv(last_export)
    #uti_file = uti_file[~(uti_file['Cleared Trade Global UTI'].isna())]
    columns = ['Direction', 'Notional', 'Fixed Rate', 
               'Effective Date', 'Maturity Date', 'UTI', 'SEF Execution Platform']

    columns_dic = {'Fixed Rate': 'Yield(Spot)', 
                   'Effective Date': 'Fecha Inicio', 'Maturity Date': 'Fecha vencimiento',
                   'UTI': 'UTI'}

    uti = uti_file[columns].copy()
    uti.rename(columns=columns_dic, inplace=True)
    uti = uti[~(uti['UTI'].isin(used_utis))]
    uti['Size'] = np.select([uti['Direction']=='Pay'], 
                                [-uti['Notional']/1_000_000],
                                default=uti['Notional']/1_000_000)
    # uti['Tenor'] = uti['Tenor'].apply(
    #     lambda x: str(int(x[:-1])*13)+'m' if x[-1]=='y' else x)

    def fill_dates(t, evaluation_date):
        start_date = ql.Mexico().advance(
            ql.Date().from_date(evaluation_date), ql.Period(1, ql.Days))
        end_date = start_date + 28*t
        
        return start_date.to_date().strftime('%Y-%m-%d'), end_date.to_date().strftime('%Y-%m-%d')
    
    
    

    # blotter_auto_new['UTI'] = np.nan

    columns_id = ['Yield(Spot)', 'Fecha Inicio', 
                  'Fecha vencimiento', 'Size']
    
    uti.dropna(subset='UTI', inplace=True)
    
    
    dic_mont = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12}
    
        
    blotter_auto_new['Fecha Inicio'] = blotter_auto_new['Fecha Inicio'].fillna(0)
    blotter_auto_new['Fecha vencimiento'] = blotter_auto_new['Fecha vencimiento']\
        .fillna(0)
    
    try:
        uti['Fecha Inicio'] = pd.to_datetime(uti['Fecha Inicio'].apply(
            lambda x: x[:3] + str(dic_mont[x[3:6].lower()]) + x[6:]),
            dayfirst = True)
        uti['Fecha Inicio'] = uti['Fecha Inicio'].apply(
            lambda d: d.strftime('%Y-%m-%d'))
    except:
        pass
    
    try:
        uti['Fecha vencimiento'] = pd.to_datetime(
            uti['Fecha vencimiento'].apply(
            lambda x: x[:3] + str(dic_mont[x[3:6].lower()]) + x[6:]),
            dayfirst = True)
        uti['Fecha vencimiento'] = uti['Fecha vencimiento'].apply(
            lambda d: d.strftime('%Y-%m-%d'))
    except:
        pass
        
    try:
        blotter_auto_new['Fecha Inicio'] = blotter_auto_new['Fecha Inicio'].apply(
            lambda d: d.strftime('%Y-%m-%d') if d!= 0 else 0)
    except:
        pass
    
    try:
        blotter_auto_new['Fecha Inicio'] = blotter_auto_new['Fecha Inicio'].apply(
            lambda d: d.split(' ')[0] if d!= 0 else 0)
    except:
        pass
    
    try:
        blotter_auto_new['Fecha vencimiento'] = \
            blotter_auto_new['Fecha vencimiento'].apply(
                lambda d: d.strftime('%Y-%m-%d') if d!=0 else 0)
    except:
        pass
    
    try:
        blotter_auto_new['Fecha vencimiento'] = \
            blotter_auto_new['Fecha vencimiento'].apply(
                lambda d: d.split(' ')[0] if d!=0 else 0)
    except:
        pass

        
    blotter_auto_new['Fecha vencimiento'] = blotter_auto_new[
        'Fecha vencimiento'].astype(str).str.replace('/', '-')
    blotter_auto_new['Fecha Inicio'] = blotter_auto_new[
        'Fecha Inicio'].astype(str).str.replace('/', '-')
    
    
    

    for i, r in blotter_auto_new.iterrows():
        if r.UTI is not None:
            continue
        else:
            r_copy = r.copy()
            if r.Broker == 'bbg':
                uti_temp = uti[uti['SEF Execution Platform'] == 'BloombergSEF']
            elif r.Broker == 'tw':
                uti_temp = uti[uti['SEF Execution Platform'] == 'Tradeweb']
            else:
                uti_temp = uti[~(uti['SEF Execution Platform'].isin(
                    ['BloombergSEF', 'Tradeweb']))]
            if (r['Fecha Inicio'] == '0' or r['Fecha Inicio'] == 'None'):
                # print('aaaaa')
                r_copy['Fecha Inicio'] = fill_dates(int(r.Tenor[:-1]), 
                                               evaluation_date)[0]
                r_copy['Fecha vencimiento'] = fill_dates(int(r.Tenor[:-1]), 
                                                    evaluation_date)[1]
                
            r_copy['Yield(Spot)'] = np.round(r_copy['Yield(Spot)'], 5)
            r_copy['Fecha Inicio'] = check_date(r_copy['Fecha Inicio'])
            r_copy['Fecha vencimiento'] = check_date(r_copy['Fecha vencimiento'])
                
            for j, s in uti_temp.iterrows():
                # print(j,'\n',s[columns_id], r_copy[columns_id])
                s_copy = s.copy()
                s_copy['Yield(Spot)'] = np.round(s_copy['Yield(Spot)'], 5)
                # print('\nBlotter: ', r_copy[columns_id].astype(str))
                # print('\nUTI File: ', s_copy[columns_id].astype(str))
                if (r_copy[columns_id].astype(str) == 
                    s_copy[columns_id].astype(str)).all(): 
                    # print('aaaaa')
                    blotter_auto_new.at[i, 'UTI'] = s.UTI
                    uti.drop(j, axis = 0, inplace=True)
                    break
       
    return blotter_auto_new.replace({'0': None, 'None':None})

def upload_blotter(wb: xw.Book, evaluation_date = datetime) -> None:
    """Saves the blotter in the JAM format


    Parameters
    ----------
    wb : xw.Book
        xlwings Book object that emulates an excel book
    evaluation_date : datetime

 

    Returns
    -------
    None
        saves the file in \\TLALOC\tiie\Blotters


    """
    # MegaBlotter Catalogue
    blotter_catalogue = pd.read_excel(r'//tlaloc/Cuantitativa/Fixed Income'
                                      + r'/TIIE IRS Valuation Tool/Blotter'
                                      +r'/Catalogos/CatalogoMegaBlotter.xlsx')
    
    
    
    
    # Dictionaries to fill Ctpty, Cámara, Socio Liquidador and Broker
    cpty_dic = dict(zip(blotter_catalogue['cat_shortcut'].astype(str).str.lower().values, 
                        blotter_catalogue['cat_clave'].values))
    # print(cpty_dic['1814'])
    camara_dic = dict(zip(blotter_catalogue['cat_shortcut'].astype(str).str.lower().values, 
                          blotter_catalogue['Camara'].values))
    socio_dic = dict(zip(blotter_catalogue['cat_shortcut'].astype(str).str.lower().values, 
                         blotter_catalogue['Socio Liquidador'].values))
    broker_dic = dict(zip(
        blotter_catalogue['cat_shortcut'].astype(str).str.lower().values, 
        blotter_catalogue['Broker'].values))
    regulado_dic = dict(zip(
        blotter_catalogue['cat_shortcut'].astype(str).str.lower().values, 
        blotter_catalogue['regulado'].values))
    blotter_cat = blotter_catalogue.dropna(subset = 'Client')
    client_dic = dict(zip(blotter_cat['cat_shortcut'].astype(str).str.lower().values, 
                          blotter_cat['Client'].values))
    
    # User name
    user_name = input('Please enter your username: ')
    
    # Pricer Blotter
    blotter = wb.sheets('Blotter')
    range_trades = blotter.range('A1').end('right').address[:-1] + \
        str(blotter.range('H1').end('down').row)
    parameters_trades = blotter.range('A1', range_trades).options(
        pd.DataFrame, header=1).value
    
    
    # Data handling
    parameters_trades = parameters_trades.fillna(0)
    # parameters_trades['Cpty_temp'] = \
    #     parameters_trades['Cpty'].copy().astype(str).str.lower()
    parameters_trades['Cpty_temp'] = \
        parameters_trades['Cpty'].copy().apply(lambda x: str(int(x)) 
                                               if (type(x) == float) 
                                               else str(x).lower())
    
    for i, v in parameters_trades.iterrows():
        
        if v['Rate'] == 'fee':
            
            
            mxn_tiie = wb.sheets('MXN_TIIE')
            tenor130 = mxn_tiie.range('E12').value
            
            if v['Cpty'] == 'ventas' or v['Cpty'] == 'santi' \
                or str(int(v['Cpty'])) == '8089':
                parameters_trades.at[i,'Fwd_Tenor'] = 130
                parameters_trades.at[i,'Notional_MXN'] = 130_000_000
                parameters_trades.at[i,'Rate'] = tenor130/100
                
                if v['Cpty'] == 'ventas': 
                    parameters_trades.at[i,'Cpty_temp'] = '8087'
                    v['Cpty'] = '8087'
                    
                elif v['Cpty'] == 'santi':
                    parameters_trades.at[i,'Cpty_temp'] = '8082'
                    v['Cpty'] = '8082'
                
                else:
                    parameters_trades.at[i,'Cpty_temp'] = '8089'
                    v['Cpty'] = '8089'
                    
                parameters_trades.loc[float(i[1:])+.01] =\
                    parameters_trades.loc[i].copy().values
                
                parameters_trades.at[float(i[1:])+.01, 'Notional_MXN'] =\
                    -parameters_trades.loc[i,'Notional_MXN']
                parameters_trades.at[float(i[1:])+.01, 'USD_DV01'] =\
                    -parameters_trades.loc[i,'USD_DV01']
                
                
                
                if v['Cpty'] ==  '8082' or v['Cpty'] == '8089':
                    parameters_trades.loc[float(i[1:])+.02] =\
                        parameters_trades.loc[i].copy().values
                    parameters_trades.at[float(i[1:])+.02, 'Cpty_temp'] =\
                        str(int(v['Book']))
                    parameters_trades.at[float(i[1:])+.02, 'Book'] = v['Cpty']
                    parameters_trades.loc[float(i[1:])+.03] =\
                        parameters_trades.loc[float(i[1:])+.02].copy().values
                    parameters_trades.at[float(i[1:])+.02, 'Upfront_Fee_MXN'] = 0
                    parameters_trades.at[float(i[1:])+.03,
                                          'Upfront_Fee_MXN'] = -v['Upfront_Fee_MXN']
                    
                    parameters_trades.at[float(i[1:])+.02, 'Notional_MXN'] =\
                        -parameters_trades.at[i,'Notional_MXN']
                    parameters_trades.loc[float(i[1:])+.02, 'USD_DV01'] =\
                        -parameters_trades.at[i,'USD_DV01']
            
                parameters_trades.at[i,'Upfront_Fee_MXN'] = 0
                
        if ((v['Start_Date']  == 0) and (v['End_Date'] == 0) 
            and (v['Start_Tenor'] == 0)):
            pass
       
        else:
            start, maturity, flag_mat = \
                fn.start_end_dates_trading(v, evaluation_date)
            parameters_trades['Start_Date'].at[i] = start.to_date()
            parameters_trades['End_Date'].at[i] = maturity.to_date()
            v['Start_Date'] = start.to_date()
            v['End_Date'] = maturity.to_date()
            
            if v['Start_Date'] < evaluation_date.date():
                parameters_trades['Fwd_Tenor'].at[i] = \
                    np.ceil((parameters_trades['End_Date'].loc[i] - \
                             evaluation_date.date()).days/28)
                
            else:
                parameters_trades['Fwd_Tenor'].at[i] = \
                    np.ceil((parameters_trades['End_Date'].loc[i] - \
                             parameters_trades['Start_Date'].loc[i]).days/28)
                
    cme_conditions = [(parameters_trades['Cpty_temp'].astype(str).str.split('_').apply(
        lambda x: len(x)>1)) & (parameters_trades['Cpty_temp'].astype(str).str.split('_').apply(
            lambda x: 'cme' not in x))]
    cme_options = [parameters_trades['Cpty_temp'].astype(str).str.split('_').apply(
        lambda x: x[0])]
    
    parameters_trades['Cpty_temp'] = np.select(cme_conditions, ['cme'],
                                               default=parameters_trades['Cpty_temp'].astype(str))
    # JAM format
    blotter_auto = pd.DataFrame()
    blotter_auto['User'] = [user_name] * parameters_trades.shape[0]
    blotter_auto['Book'] = parameters_trades['Book'].values
    blotter_auto['Tenor'] = parameters_trades['Fwd_Tenor'].values
    blotter_auto['Yield(Spot)'] = (parameters_trades['Rate'].values)*100
    blotter_auto['Yield (IMM)'] = 0
    blotter_auto['DV01s'] = parameters_trades['USD_DV01'].values
    blotter_auto['Size'] = (parameters_trades['Notional_MXN'].values)/1000000
    blotter_auto['Ctpty'] = \
        parameters_trades['Cpty_temp'].astype(str).replace(cpty_dic).values 
    # print(parameters_trades['Cpty_temp'].astype(str).iloc[-5:])
    # print(parameters_trades['Cpty_temp'].astype(str).replace(cpty_dic).iloc[-5:])
    blotter_auto['Fecha Inicio'] = parameters_trades['Start_Date'].values
    blotter_auto['Fecha vencimiento'] = parameters_trades['End_Date'].values
    blotter_auto['Cámara'] = \
        parameters_trades['Cpty_temp'].astype(str).replace(camara_dic).values
    blotter_auto['Socio Liquidador'] = \
        parameters_trades['Cpty_temp'].astype(str).replace(socio_dic).values
    blotter_auto['Broker'] = \
        parameters_trades['Cpty_temp'].astype(str).replace(broker_dic).values  
    blotter_auto['Regulado'] = \
        parameters_trades['Cpty_temp'].astype(str).replace(regulado_dic).values
    blotter_auto['P&L'] = parameters_trades['MXN_NPV'].values
    blotter_auto['Folio Original'] = np.select(cme_conditions, cme_options, 
                                               default='')
    
    
    


    conditio = [((np.ceil(blotter_auto['Tenor']) >= 2) & 
                 (np.ceil(blotter_auto['Tenor']) <= 6) & 
                 (abs(blotter_auto['Size']) >= 2000) & 
                 (blotter_auto['Regulado'] == 'mex')),
                ((np.ceil(blotter_auto['Tenor']) >= 7) & 
                 (np.ceil(blotter_auto['Tenor']) <= 13) & 
                 (abs(blotter_auto['Size']) >= 1000) & 
                 (blotter_auto['Regulado'] == 'mex')),
                ((np.ceil(blotter_auto['Tenor']) >= 14) & 
                 (np.ceil(blotter_auto['Tenor']) <= 52) & 
                 (abs(blotter_auto['Size']) >= 250)  & 
                 (blotter_auto['Regulado'] == 'mex')),
                ((np.ceil(blotter_auto['Tenor']) >= 53) & 
                 (np.ceil(blotter_auto['Tenor']) <= 130) & 
                 (abs(blotter_auto['Size']) >= 130) & 
                 (blotter_auto['Regulado'] == 'mex')),
                ((np.ceil(blotter_auto['Tenor']) >= 131) & 
                 (np.ceil(blotter_auto['Tenor']) <= 390) & 
                 (abs(blotter_auto['Size']) >= 100) & 
                 (blotter_auto['Regulado'] == 'mex'))]
    regulado = np.select(conditio, ['MEX','MEX','MEX','MEX','MEX'], '') 
    blotter_auto['Broker'] = np.select(
        [(parameters_trades['Cpty'].str.lower().isin(
            ['mexder','santa asigna'])) & (regulado != 'MEX')], 
        ['trad'], blotter_auto['Broker'])
    
    # "Regulado" and "Comment" columns
    blotter_auto['Regulado'] = regulado
    
    # Add "Cuota compensatoria" column to formatted blotter
    blotter_auto['Cuota compensatoria / unwind'] = \
        parameters_trades['Upfront_Fee_MXN'].values
        
    fee_rate = {182: 0.00375, 1092: 0.003, 3640: 0.0025, 7280: 0.0018,
                15000: 0.0015}
    
    for i, v in blotter_auto.iterrows():
        if (v['Ctpty'].lower() == 'u2428' or v['Ctpty'].lower() == 'u8085' 
            or v['Ctpty'].lower() == 'u8089' or v['Ctpty'].lower() == 'u8088') :
            blotter_auto.loc[i+.01] = v.copy().values
            blotter_auto.loc[i+.01, 'Book'] = int(v.Ctpty[1:])
            blotter_auto.loc[i+.01, 'Ctpty'] = 'u' + str(int(v.Book))
            blotter_auto.loc[i+.01, 'Size'] = -v.Size
            blotter_auto.loc[i+.01, 'DV01s'] = -v.DV01s
            blotter_auto.loc[i+.01, 'P&L'] = -v['P&L']
            if v['Ctpty'].lower() == 'u8085':
                blotter_auto.loc[i+.01, 'Cuota compensatoria / unwind'] =\
                    -v['Cuota compensatoria / unwind']
            if v['Ctpty'] == 'u2428' and\
                parameters_trades['Cpty_temp'].iloc[i] in client_dic.keys():
                blotter_auto.loc[i+.02] = v.copy().values
                blotter_auto.loc[i+.02, 'Book'] = int(v.Ctpty[1:])
                blotter_auto.loc[i+.02, 'Ctpty'] =\
                    client_dic[parameters_trades['Cpty_temp'].iloc[i]]
                for j,k in fee_rate.items():
                    
                    if blotter_auto.loc[i+.02, 'Tenor']*28 <= j:    
                        blotter_auto.loc[i+.02, 'Yield(Spot)'] =\
                            v['Yield(Spot)'] + np.sign(v['Size'])*k
                        
                        break
        
            
        
    blotter_auto['Comment'] = None
    blotter_auto['UTI'] = None
    blotter_auto['Tenor'] = blotter_auto['Tenor'].astype(int).astype(str)+'m'
    blotter_auto = blotter_auto.replace({np.nan: None, 0: None, 'nan': None})
    
    date_save = evaluation_date.strftime('%Y%m%d')
    
    try: 
        # If there's already a blotter file for today
        blotter_template_file = \
            xw.Book(f'//TLALOC/tiie/Blotters/{date_save[2:]}.xlsx', 
                    update_links = False)
            
        blotter_template_file.save(r'//tlaloc/Cuantitativa/Fixed Income' + \
                                   '/TIIE IRS Valuation Tool/Blotter/Backups'+
                                   f'/{date_save[2:]}_copy.xlsx')
            
        blotter_template_sheet = \
            blotter_template_file.sheets('BlotterTIIE_auto')
        
        # Blotter file info
        blotter_template = blotter_template_sheet.range('C3:S' 
            + str(blotter_template_sheet.range('C4').end('down').row)).options(
                pd.DataFrame, header=1, index = False).value    
        
        # "Regulado" column values
        blotter_regulados = blotter_template_sheet.range('AQ4:AQ' 
            + str(blotter_template_sheet.range('C4').end('down').row)).value
        blotter_template['Regulado'] = blotter_regulados
        
        # "Comment" column values
        blotter_comment = blotter_template_sheet.range('AL4:AL' 
            + str(blotter_template_sheet.range('C4').end('down').row)).value
        blotter_template['Comment'] = blotter_comment
        
        blotter_uti = blotter_template_sheet.range('AR4:AR' 
            + str(blotter_template_sheet.range('C4').end('down').row)).value
        blotter_template['UTI'] = blotter_uti
        
        blotter_pnl = blotter_template_sheet.range('AN4:AN'
            + str(blotter_template_sheet.range('C4').end('down').row)).value
        blotter_template['P&L'] = blotter_pnl
        
        # Trades with folio stay the same
        blotter_folios = blotter_template.dropna(
            subset = ['Folio JAM', 'Comment'], how = 'all')
        
        blotter_folios_tenor = \
            blotter_folios[blotter_folios['Fecha vencimiento'].isna() & 
                           blotter_folios['Fecha Inicio'].isna()]
        blotter_folios_tenor['Fecha Inicio'] = None

        blotter_folios_tenor['Fecha vencimiento'] = None
        blotter_folios_stend = \
            blotter_folios[~(blotter_folios['Fecha vencimiento'].isna() & 
                             blotter_folios['Fecha Inicio'].isna())]
        try:
            blotter_folios_stend['Fecha Inicio'] = \
                blotter_folios_stend['Fecha Inicio'].dt.date
            blotter_folios_stend['Fecha vencimiento'] = \
                blotter_folios_stend['Fecha vencimiento'].dt.date
        except:
            pass
        
        blotter_folios = pd.concat([blotter_folios_stend,blotter_folios_tenor])
        blotter_folios.sort_index(inplace=True)
        blotter_folios.replace({np.nan: None, 0: None, 'nan': None}, 
                               inplace = True)
        blotter_folios_copy = blotter_folios.copy()
        
        
        
    except:
        
        blotter_template_file = xw.Book(
            r'\\tlaloc\Cuantitativa\Fixed Income\TIIE IRS Valuation Tool'
            + r'\Quant Team\Esteban y Gaby\Upload_blotter.xlsx', 
            update_links = False)
        blotter_template_sheet = \
            blotter_template_file.sheets('BlotterTIIE_auto')
        blotter_folios = \
            pd.DataFrame(columns = blotter_template_sheet.range('C3:R3').value + ['UTI'])
        blotter_folios_copy = blotter_folios.copy()
        
        blotter_folios_copy.to_excel('//TLALOC/Cuantitativa/Fixed Income'
                                      + '/TIIE IRS Valuation Tool'
                                      + '/Blotter/Backups'
                                      + f'/{date_save[2:]}_copy.xlsx')
    
    
    columnas = [c for c in blotter_auto.columns[1:-2] if c != 'DV01s' and c != 
                'Regulado' and c != 'Yield (IMM)' and c != 'P&L' 
                and c != 'Folio Original' and c!= 'Comment' and c != 'UTI']
    
     
    blotter_folios['Book'] = blotter_folios['Book'].astype(int).astype(str)
    blotter_auto['Book'] = blotter_auto['Book'].astype(int).astype(str)
    # Check different rows
    for i, v in blotter_auto.iterrows():
        l = 0
        for k, b in blotter_folios.iterrows():
            if (v[columnas].astype(str) == b[columnas].astype(str)).all():
            
                blotter_auto = blotter_auto.drop(i, axis = 0)
                blotter_folios = blotter_folios.drop(k, axis = 0)
                break
    
    
    folio_conditions = [blotter_auto['Folio Original'] != '']
    folio_options = [blotter_auto.index]
    blotter_auto['Folio JAM'] = np.select(folio_conditions, folio_options, 
                                          default='')
    
    #blotter_auto_new = pd.concat([blotter_folios_copy, blotter_auto])
    blotter_auto_new = pd.concat([blotter_folios_copy, blotter_auto])
    
    try:
        user_letter = parameters_trades.index[0][0]
        # folders_dic = {'J': 'jlsanchez', 'O': 'oluna', 'G': 'gabreu', 
        #                'E': 'elopeza'}
        # user = folders_dic[user_letter]
        blotter_auto_new = fill_uti(blotter_auto_new, user_letter, evaluation_date)
    except:
        print('UTI column could not be filled.')
        
    for i, v in blotter_auto_new.iterrows():
        if (((v['Ctpty'].lower() == 'u8082' or v['Ctpty'].lower() == 'u8087' 
              or v['Ctpty'].lower() == 'u8089')
             and v['Cuota compensatoria / unwind'] is not None) or 
            ((v['Book'] == '8082' or v['Book']=='8089') and 
             v['Ctpty'].lower() == 'u1814' and 
             v['Cuota compensatoria / unwind'] is not None)):
            # print(v['Cuota compensatoria / unwind'], v['Cuota compensatoria / unwind'] is not None)
            v['Folio JAM'] = 'Pendiente'+str(i)
    
    # Clear contentes of previous blotter
    blotter_template_sheet.range('C4:S10000').clear_contents()
    
    # Clear Comment, PnL, Regulado and UTI column values
    blotter_template_sheet.range('AL4:AL10000').clear_contents()
    blotter_template_sheet.range('AQ4:AQ10000').clear_contents()
    blotter_template_sheet.range('AN4:AN10000').clear_contents()
    blotter_template_sheet.range('AR4:AR10000').clear_contents()
    
    # Paste values of updated blotter
    blotter_template_sheet.range('C4').value = \
        blotter_auto_new.replace(0, None).drop(
            columns=['Regulado', 'Comment', 'P&L', 'UTI']).values
    
    # Fill Comment and Regulado columns
    regulado = blotter_auto_new['Regulado'].values
    comment = blotter_auto_new['Comment'].values
    pnl = blotter_auto_new['P&L'].values
    uti = blotter_auto_new['UTI'].values
    blotter_template_sheet.range('AQ4').value = regulado.reshape(-1,1)
    blotter_template_sheet.range('AL4').value = comment.reshape(-1, 1)
    blotter_template_sheet.range('AN4').value = pnl.reshape(-1, 1)
    blotter_template_sheet.range('AR4').value = uti.reshape(-1, 1)
    
    save = '0'
    while save.lower() != 'y' and save.lower() != 'n':
        save = input('\nDo you want to save Blotter file? Y/N: ')
        
        if save.lower() == 'y':
            # Save file
            blotter_template_file.save(
                f'//TLALOC/tiie/Blotters/{date_save[2:]}.xlsx')
        elif save.lower() == 'n':
            pass
    
#-------------------------
#  Fwd Starting Function
#-------------------------  
                
def fwd_starting(wb: xw.Book, g_engines: list, close: bool = False) -> None:
    """
    

    Parameters
    ----------
    wb : wx.Book+1
        DESCRIPTION.
    g_engines : list
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    
    # Parameters Definition
    months = {3: 'March', 6: 'June', 9: 'September', 12: 'December'}
    parameters = wb.sheets('Pricing')
    fwd_start = wb.sheets('Fwd_Starting')
    fwd_start.activate()
    evaluation_date = parameters.range('B1').value
    evaluationDate = ql.Date.from_date(evaluation_date)
    ibor_tiie = g_engines[0]
    tiie_swp_engine = g_engines[1]
    
    if close:
        col_num = 4
        col_num_fwd = 17
        
    else:
        col_num = 10
        col_num_fwd = 18
    
    #IMMs
    # IMM dates definition
    next_imm = ql.IMM.nextDate(evaluationDate)
    imm_list = [datetime(next_imm.year(), next_imm.month(), 1)]
    
    for i in range(2):
        next_imm = ql.IMM.nextDate(next_imm + 1)
        imm_list.append(datetime(next_imm.year(), next_imm.month(), 1))
    
    # Fwd tenors 
    end_row = str(fwd_start.range('B4').end('down').row)
    imm_tenor = fwd_start.range('B4:B'+end_row).value
    
    # Fair Rate Calculation    
    for imm in imm_list:
        
        fair_rates = []
        fwd_start.range(3,col_num).value = months[imm.month]
        
        for tenor in imm_tenor:
            parameters_trades = pd.Series(['imm', tenor, imm, 0], index = \
                                          ['Start_Tenor', 'Fwd_Tenor',
                                           'Start_Date', 'End_Date'])
            start, maturity, flag_mat = \
                fn.start_end_dates_trading(parameters_trades, evaluation_date)
            notional = 1000000000
            rate = .10
            typ = ql.VanillaSwap.Receiver
            rule = ql.DateGeneration.Backward
            
            swap_valuation = fn.tiieSwap(start, maturity, notional, ibor_tiie, 
                                         rate, typ, rule)
            
            swap_valuation[0].setPricingEngine(tiie_swp_engine)
            
            fair_rates.append(swap_valuation[0].fairRate())
        
        # Display
        fwd_start.range(4, col_num).value = np.array(fair_rates).reshape(-1,1)
        col_num +=1
    
    
    # FWDs
    
    # Start and End tenors 
    end_row_st = str(fwd_start.range('N4').end('down').row)
    start_tenor = fwd_start.range('N4:N'+end_row_st).value
    
    end_row_ed = str(fwd_start.range('O4').end('down').row)
    fwd_tenor = fwd_start.range('O4:O'+end_row_st).value
    
    # Fair Rates Calculations
    fair_rates_fwd = []
    for stenor, etenor in zip(start_tenor, fwd_tenor):
        try:
            if stenor.strip().lower() == 'spot':
                stenor = 0
        except: 
            pass
        
        parameters_trades = pd.Series([stenor, etenor, 0, 0], index = \
                                      ['Start_Tenor', 'Fwd_Tenor',
                                       'Start_Date', 'End_Date'])
        start, maturity, flag_mat = \
            fn.start_end_dates_trading(parameters_trades, evaluation_date)
        notional = 1000000000
        rate = .10
        typ = ql.VanillaSwap.Receiver
        rule = ql.DateGeneration.Backward
        
        swap_valuation = fn.tiieSwap(start, maturity, notional, ibor_tiie, 
                                     rate, typ, rule)
        swap_valuation[0].setPricingEngine(tiie_swp_engine)
    
        fair_rates_fwd.append(swap_valuation[0].fairRate())
        
    # Display    
    fwd_start.range(4, col_num_fwd).value = \
        np.array(fair_rates_fwd).reshape(-1,1)
    fwd_start.range('S4:S'+ end_row_ed).api.Calculate()
    
    if close:
        save_obj([fair_rates, fair_rates_fwd], r'Closes/close_fwds_' 
                                  + evaluation_date.strftime('%Y%m%d'))
    
    




#--------------------------------------
#  Spreads Paths for Corros Functions
#--------------------------------------

def clean_spreads(spreads_corros: pd.DataFrame) -> pd.DataFrame:
    """Cleans spreads DataFrame gotten from Corros file.
    
    DataFrame gotten from Corros file has missing column names and does not
    have the spread tenors separated. Columns Tenor 1 L and Tenor 2 L will be
    created for easier data handling.
    

    Parameters
    ----------
    spreads_corros : pd.DataFrame
        Spreads DataFrame gotten from Corros file.

    Returns
    -------
    spreads_df : pd.DataFrame
        Spreads DataFrame ready to handle.

    """

    i=1
    cols = []
    for c in spreads_corros.columns:
        if c==None:
            c='Tenor'+str(i)
            i=i+1
        cols.append(c)
            
    spreads_corros.columns = cols   
    
    # Tenor column (3-6, 6-9, etc.)
    tenor_column = spreads_corros.columns[1]
    
    # Separate first tenor and second tenor
    spreads_corros['Tenor 1 L'] = spreads_corros[tenor_column].apply(
        lambda x: x.split('-')[0])
    spreads_corros['Tenor 2 L'] = spreads_corros[tenor_column].apply(
        lambda x: x.split('-')[1])

    # Get rid of bad data and convert tenors to int
    spreads_corros = spreads_corros[(spreads_corros['Tenor 1 L'] != '') & 
                                    (spreads_corros['Tenor 2 L'] != '')]
    spreads_corros['Tenor 1 L'] = spreads_corros['Tenor 1 L'].astype(int)
    spreads_corros['Tenor 2 L'] = spreads_corros['Tenor 2 L'].astype(int)

    # String tenors (3m6m, 6m9m, etc.) rename column
    tenor_str_column = spreads_corros.columns[2]  
    spreads_corros = spreads_corros.rename(
        columns = {tenor_str_column: 'Tenor'})

    # Columns we need  
    spreads_df = spreads_corros[['Tenor', 'Tenor 1 L', 'Tenor 2 L', 'Bid', 
                                 'Offer']]

    # Remove rows that don't have neither bid nor offer values
    spreads_df = spreads_df.set_index('Tenor')
    spreads_df.dropna(axis = 0, subset = ['Bid', 'Offer'], how = 'all', 
                      inplace = True)
    
    return spreads_df

def clean_rates(rates_corros: pd.DataFrame) -> pd.DataFrame:
    """Cleans rates DataFrame gotten from Corros file.
    

    Parameters
    ----------
    rates_corros : pd.DataFrame
        Rates DataFrame gotten from Corros file.

    Returns
    -------
    rates_df : pd.DataFrame
        Clean DataFrame.

    """
    
    # Fill columns with no name
    i=1
    cols = []
    for c in rates_corros.columns:
        if c==None:
            c='Tenor'+str(i)
            i=i+1
        cols.append(c)
    rates_corros.columns = cols
    
    # Tenor (3, 6, 9) and string tenors columns (3m, 6m, 9m)
    tenor_column, tenor_str_column = rates_corros.columns[1:3]
    
    # Drop rows with no tenors
    rates_df = rates_corros.dropna(subset=[tenor_column, tenor_str_column])
    
    # Rename columns
    rates_df = rates_df.rename(columns={tenor_column: 'Tenor_L', 
                                        tenor_str_column: 'Tenor'})
    
    return rates_df

def create_graphs(spreads_df: pd.DataFrame) -> list:
    """
    

    Parameters
    ----------
    spreads_df : pd.DataFrame
        DataFrame with spreads info.

    Returns
    -------
    list
        List with two graphs:
            bG: bid graph
            oG: offer graph.

    """
    
    bid_spreads = spreads_df['Bid'].tolist()
    offer_spreads = spreads_df['Offer'].tolist()
    
    start_tenors = spreads_df['Tenor 1 L'].tolist()
    end_tenors = spreads_df['Tenor 2 L'].tolist()
    
    bG = nx.DiGraph()
    oG = nx.DiGraph()

    for t in range(0, len(start_tenors)):
        
        # Start and end nodes
        v = start_tenors[t]
        s = end_tenors[t]
        
        # Case short to long tenor
        if v < s:
            if not pd.isnull(bid_spreads[t]) and bid_spreads[t] != '':
                bG.add_edge(v, s, weight = bid_spreads[t])
                oG.add_edge(s, v, weight = -bid_spreads[t])
            if not pd.isnull(offer_spreads[t]) and offer_spreads[t] != '':
                bG.add_edge(s, v, weight = -offer_spreads[t])
                oG.add_edge(v, s, weight = offer_spreads[t])
            
        # Case long to short tenor    
        elif s < v:
            if not pd.isnull(offer_spreads[t]) and offer_spreads[t] != '':
                bG.add_edge(v, s, weight = -offer_spreads[t])
                oG.add_edge(s, v, weight = offer_spreads[t])
            if not pd.isnull(bid_spreads[t]) and bid_spreads[t] != '':
                bG.add_edge(s, v, weight = bid_spreads[t])
                oG.add_edge(v, s, weight = -bid_spreads[t])
    
    return bG, oG

def get_all_paths(graph: nx.DiGraph, side: str) -> dict:
    """Get all paths from one tenor to another given a graph with spreads info.
    
    Uses a directed graph with spreads as edge's weight and tenors as nodes to
    get all possible paths between all nodes.
    

    Parameters
    ----------
    graph : nx.DiGraph
        Directed Graph with spreads information.
    side : str
        Indicates if we are on the Bid side or on the Offer side.

    Returns
    -------
    dict
        Dictionary with all possible paths. The keys are the tenors, and the
        values are DataFrames with all possible paths starting from the key
        tenor to the rest.

    """
    
    # Dictionaries to save paths for each starting node
    paths_dict = {} 
    
    # For each node we get all the paths starting in that node to all others
    for n in graph.nodes():
        
        # Lists for all paths starting at n and all the weights
        all_paths = []
        all_weights = []
        
        for v in graph.nodes():
            
            # Paths and weights going from n to v
            paths = [path for path in nx.all_simple_paths(graph, n, v)]
            weights = [nx.path_weight(graph, path, 'weight') for path in paths]
            
            # Save paths and weights to list
            all_paths.extend(paths)
            all_weights.extend(weights)
            
        # Save all paths starting at n in dictionary
        paths_dict[n] = pd.DataFrame({side + ' Path': all_paths, 
                                      side + ' Spread': all_weights})
    
    return paths_dict

def best_paths_fn(rates_df: pd.DataFrame, graph: nx.DiGraph, side: str, 
                  paths_dict: dict) -> dict:
    """
    

    Parameters
    ----------
    rates_df : pd.DataFrame
        DataFrame with rates.
    graph : nx.DiGraph
        Directed Graph with spreads information.
    side : str
        Indicates if we are on the Bid side or on the Offer side.
    paths_dict : dict
        Dictionary with all possible paths. The keys are the tenors, and the
        values are DataFrames with all possible paths starting from the key
        tenor to the rest.

    Returns
    -------
    dict
        Dictionary with best paths. The keys are the tenors, and the
        values are DataFrames with the best paths starting from the key
        tenor to the rest.

    """
    
    best_paths_dic = {}
    
    # Get best bid/offer rates starting from known node (tenor, bid)
    for i in range(0, rates_df.shape[0]):
        
        row = rates_df.iloc[i]
        tenor = row.Tenor_L
        
        if side == 'Bid':
            rate = row.Bid
            
        elif side == 'Offer':
            rate = row.Offer
        
        
        # If there were no spreads from known node the graph will not have it 
        # as a node
        try:
            paths_start = paths_dict[tenor]
        except:
            rate_start = rates_df[rates_df['Tenor_L']==tenor][side].values[0]
            best_paths_dic[tenor] = pd.DataFrame(
                {'Start': tenor, 'End': tenor, side + ' Path': [[int(tenor)]], 
                 side: [rate_start], side + ' Spread': [0], 'Length': [0]})
            continue
        
        # If bid rate is nan we can't use it as a known node
        if not pd.isnull(rate) and rate != '':
            
            # Dictionary to save bid paths starting in known node
            paths = {}
            
            # Iterate over all nodes
            for k in paths_dict.keys():
                
                # Paths starting in known node and ending in specific node k
                paths_k = [(path, nx.path_weight(graph, path, 'weight')) for 
                           path in paths_start[side + ' Path'] if path[-1]==k]
                
                # DataFrame with all paths starting in known node and ending in 
                # specific node with sum of spreads
                k_df = pd.DataFrame({side + ' Path': [p for (p, w) in paths_k], 
                                      side + ' Spread': [w for (p, w) 
                                                         in paths_k]})
                
                # Bid rate of end node will be start bid plus sum of spreads
                k_df[side]  = [np.round(rate + w/100, 7) for w in 
                               k_df[side + ' Spread']]
                
                # Save DataFrame in dictionary. DataFrame with bid paths 
                # starting in known node and ending in k.
                paths[k] = k_df
            
            # DataFrame to save best paths for all nodes starting from known 
            # node
            best_paths = pd.DataFrame(columns = ['Start','End', side + ' Path', 
                                                 side])
            
            # Bid path keys are the nodes gotten from the spreads
            for k in paths.keys():
                
                # Bid paths that end in node k.
                options = paths[k]
                # Best path is the one with maximum bid.
                if side == 'Bid':
                    best_path = options[options[side]==np.round(
                        options[side].max(), 7)]
                elif side == 'Offer':
                    best_path = options[options[side]==np.round(
                        options[side].min(), 7)]
                # Get length of best paths to get the shortest one
                best_path['Length'] = best_path[side + ' Path'].apply(
                    lambda x: len(x)-1)
                best_path = best_path[best_path['Length'] == \
                                      best_path['Length'].min()]
                
                # Column with end node
                best_path.insert(0, 'End', k)
                # Column with start node
                best_path.insert(0, 'Start', tenor)
                
                # Concat DataFrame to save all best paths starting in known 
                # node
                best_paths = pd.concat([best_paths, best_path])
            
            # Add trivial path in case it is the best
            best_paths = pd.concat([best_paths, 
                                    pd.DataFrame(
                                        {'Start': [tenor], 'End': [tenor], 
                                         side + ' Path': [[int(tenor)]], 
                                         side: [rate], side + ' Spread': [0], 
                                         'Length': [0]})])
            
            # Drop duplicates to get only one path
            best_paths = best_paths.drop_duplicates(subset = 'End')
            
            # Save to dictionary
            best_paths_dic[tenor] = best_paths.sort_values(by = 'End')
    
    return best_paths_dic

def format_paths(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gives acceptable format to paths DataFrame to write it in Excel file.
    
    Since xlwings can't write a list as values in Excel, you have to convert
    lists with paths into strings.'
    

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with bid and offer paths.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with only string values to write it in Corros Excel file.

    """
    df = df.fillna('')
    df['Bid Path'] = [', '.join(map(str, df['Bid Path'].tolist()[i])) 
                                   for i in range(0, len(df['Bid Path']))]

    df['Offer Path'] = [', '.join(map(str, df['Offer Path'].tolist()[i])) 
                                     for i in range(0, len(df['Offer Path']))]

    df['Bid Path'] = ['['+s+']' for s in df['Bid Path']]
    df['Offer Path'] = ['['+s+']' for s in df['Offer Path']]
    df['Bid Path'] = np.select([df['Bid']==''], [''], df['Bid Path'])
    df['Offer Path'] = np.select([df['Offer']==''], [''], df['Offer Path'])
    df['Bid Spread'] = np.select([df['Bid Path']==''], [''], df['Bid Spread'])
    df['Offer Spread'] = np.select([df['Offer Path']==''], [''], 
                                   df['Offer Spread'])
    
    return df

def corros_fn(corros_book: xw.Book) -> tuple:
    """Fills Corros Excel file with best bid/offer spreads and paths.
    

    Parameters
    ----------
    corros_book : xw.Book
        Corros Excel file.

    Returns
    -------
    tuple
        Tuple of three elements that include the following:
            best_spreads_copy: pd.DataFrame with best bid/offer spreads and 
            paths.
            paths_data_copy: pd.DataFrame with all the paths starting from each
            tenor to the rest.
            closes_df: pd.DataFrame with close rates.

    """
    
    best_sheet = corros_book.sheets('BEST')
    best_sheet.activate()
    best_sheet.api.Calculate()

    row_spreads = best_sheet.range('V52').end('down').row
    
    # Check if there is already any data
    if row_spreads < 75:
        spreads_corros = best_sheet.range('U51:Y'+str(row_spreads)).options(
            pd.DataFrame, header=1, index=False).value
        spreads_df = clean_spreads(spreads_corros)
    
    # Case no data available
    else:
        spreads_df = pd.DataFrame(
            columns = ['Tenor 1 L', 'Tenor 2 L', 'Bid', 'Offer'])

    row_rates = best_sheet.range('V28').end('down').row
    rates_corros = best_sheet.range('U27:Y'+str(row_rates)).options(
        pd.DataFrame, header=1, index=False).value

    # DataFrame of bid and offer starting rates
    rates_df = clean_rates(rates_corros)

    bG, oG = create_graphs(spreads_df)
                
    # Dictionaries to save paths for each starting node            
    bid_paths_dict = get_all_paths(bG, 'Bid')
    offer_paths_dict = get_all_paths(oG, 'Offer')

        
    # Get best bid/offer paths and merge them in same dictionary
    best_paths_bid_dic = best_paths_fn(rates_df, bG, 'Bid', bid_paths_dict)
    best_paths_offer_dic = best_paths_fn(rates_df, oG, 'Offer', 
                                         offer_paths_dict)

    all_keys = set().union(best_paths_bid_dic.keys(), 
                           best_paths_offer_dic.keys())
    best_paths_dic = {}

    for k in all_keys:
        try:
            b_df = best_paths_bid_dic[k]
        except:
            o_df = best_paths_offer_dic[k]
            b_df = pd.DataFrame({'Start': [k]*o_df.shape[0], 
                                 'End': o_df['End'].tolist(), 
                                 'Bid Path': ['']*o_df.shape[0], 
                                 'Bid': ['-']*o_df.shape[0], 
                                 'Bid Spread': ['-']*o_df.shape[0], 
                                 'Length': ['']*o_df.shape[0]})
        try:    
            o_df = best_paths_offer_dic[k]
        except:
            b_df = best_paths_bid_dic[k]
            o_df = pd.DataFrame({'Start': [k]*b_df.shape[0], 
                                 'End': b_df['End'].tolist(), 
                                 'Offer Path': ['']*b_df.shape[0], 
                                 'Offer': ['-']*b_df.shape[0], 
                                 'Offer Spread': ['-']*b_df.shape[0], 
                                 'Length': ['']*b_df.shape[0]})
            
        best_df = b_df.merge(o_df, how = 'outer', left_on = 'End', 
                             right_on = 'End')
        best_df['Start_x'] = np.select([best_df['Start_x'].isna()], 
                                       [best_df['Start_y']], 
                                       best_df['Start_x'])
        best_df['Start_y'] = np.select([best_df['Start_y'].isna()], 
                                       [best_df['Start_x']], 
                                       best_df['Start_y'])
        best_paths_dic[k] = best_df
    
    # Create DataFrame with all paths
    paths_df = pd.DataFrame()

    for k in best_paths_dic.keys():
        paths_df = pd.concat([paths_df, best_paths_dic[k]])
    
    
    paths_data = paths_df[['Start_x', 'End', 'Bid Path', 'Bid', 
                           'Bid Spread', 'Offer Path', 'Offer', 
                           'Offer Spread']]

    paths_data = paths_data.rename(columns = {'Start_x': 'Start'})
    
    tenors = [str(int(paths_data['Start'].tolist()[i])) + 'm' + \
              str(int(paths_data['End'].tolist()[i]))  + 'm' for i in 
              range(0, paths_data.shape[0])]
        
    paths_data.insert(2, 'Spread Tenor', tenors)
    
    # Create DataFrames with best bid and offer rates
    best_spreads_bid = pd.DataFrame()
    best_spreads_offer = pd.DataFrame()  
      
    for v in paths_df['End'].unique():
        
        df_a = paths_df[paths_df['End']==v]
        
        # Get maximum bid (ignoring blank rates)
        df_b = df_a[df_a['Bid'] != '-']
        best_b = df_b[df_b['Bid']==df_b['Bid'].max()]
        
        # If there are two paths with same rate, get the shortest one
        best_b['Length'] = best_b['Bid Path'].apply(lambda x: len(x))
        best_b = best_b[best_b['Length'] == best_b['Length'].min()]
        best_b = best_b.drop_duplicates(subset = 'End')
        
        # Get minimum offer (ignoring blank rates)
        df_o = df_a[df_a['Offer'] != '-']
        best_o = df_o[df_o['Offer']==df_o['Offer'].min()]
        
        # If there are two paths with same rate, get the shortest one
        best_o['Length'] = best_o['Offer Path'].apply(lambda x: len(x))
        best_o = best_o[best_o['Length'] == best_o['Length'].min()]
        best_o = best_o.drop_duplicates(subset = 'End')
        
        best_spreads_bid = pd.concat([best_spreads_bid, best_b])
        best_spreads_offer = pd.concat([best_spreads_offer, best_o])
    
    # Create one unified DataFrame
    best_spreads_df = best_spreads_bid[['End', 'Bid Path', 'Bid', 
                                        'Bid Spread']]\
        .merge(best_spreads_offer[['End', 'Offer Path', 'Offer', 
                                   'Offer Spread']],  how='outer', 
               left_on='End', right_on='End')
        
    best_spreads_df = best_spreads_df.rename(columns={'End': 'Tenor'})
    best_spreads_df.sort_values(by = 'Tenor', inplace = True)
    rates = rates_df[['Tenor', 'Bid', 'Offer']]


    best_spreads_copy = best_spreads_df.copy()
    best_spreads_copy = format_paths(best_spreads_copy)

    complete_tenors_range = best_sheet.range('AF28').end('down').row
    complete_tenors = best_sheet.range('AF28:AF'+
                                       str(complete_tenors_range)).value

    missing_tenors = [t for t in complete_tenors if t not in 
                      best_spreads_copy['Tenor'].tolist()]
    missing_df = pd.DataFrame({'Tenor': missing_tenors, 'Bid Path': '', 
                               'Bid': '', 'Bid Spread': '', 'Offer Path': '', 
                               'Offer': '', 'Offer Spread': ''})

    best_spreads_copy = pd.concat([best_spreads_copy, missing_df], 
                                  ignore_index=True)
    best_spreads_copy = best_spreads_copy.sort_values(by='Tenor')

    best_sheet.range('AG52:AH74').clear_contents()
    best_sheet.range('AE52:AE74').clear_contents()
    best_sheet.range('AL52:AM74').clear_contents()
    
    # Replace blanks with "-" so Excel doesn't count them as zeroes
    best_sheet.range('AG52').value = best_spreads_copy[['Bid', 'Offer']]\
        .replace('', '-').values
        
    # Write results in Corros Excel file
    best_sheet.range('AE52').value = best_spreads_copy[['Tenor']].values
    best_sheet.range('AL52').value = best_spreads_copy[['Bid Path']]\
        .replace('[]', '').values
    best_sheet.range('AM52').value = best_spreads_copy[['Offer Path']].\
        replace('[]', '').values

    # Write best paths in Corros Excel file
    paths_data.index = range(0, paths_data.shape[0])
    paths_data_copy = paths_data.copy()
    paths_data_copy = format_paths(paths_data_copy)

    paths_sheet = corros_book.sheets('Paths')
    paths_sheet.clear_contents()
    paths_sheet['A1'].options(pd.DataFrame, header=1, index=False, 
                              expand='table').value = \
        paths_data_copy.replace('[]', '').sort_values(by='Start')
    tenors_start=paths_data_copy['Start'].unique()
    tenors_start.sort()
    paths_sheet.range('K2').value = tenors_start.reshape(-1, 1)
    best_sheet.api.Calculate()
    
    # Get close rates
    close_row = best_sheet.range('AE52').end('down').row
    tenors_close = best_sheet.range('AE52:AE'+str(close_row)).value   
    closes = best_sheet.range('AJ52:AJ'+str(close_row)).value
    closes_df = pd.DataFrame({'Tenor': tenors_close, 'Close': closes})
    
    return best_spreads_copy, paths_data_copy, closes_df

def fill_rates(wb: xw.Book, best_spreads: pd.DataFrame, 
               closes_df: pd.DataFrame) -> None:
    """Fills rates in TIIE_IRS_Data with best rates gotten from spreads.
    

    Parameters
    ----------
    wb : xw.Book
        TIIE_IRS_Data Excel file.
    best_spreads : pd.DataFrame
        DataFrame with best bid/offer rates gotten from spreads.
    closes_df : pd.DataFrame
        DataFrame with close rates in case some tenors are empty.

    Returns
    -------
    None

    """
    
    rates_sheet = wb.sheets('Notional_DV01')
    rates_sheet.activate()
    prev_rates = rates_sheet.range('B3:D18').options(
        pd.DataFrame, header=1, index=False).value
    prev_rates.set_index('Tenor', inplace=True)
    best_spreads_copy = best_spreads.copy()
    best_spreads_copy.set_index('Tenor', inplace=True)
    
    new_rates = prev_rates.merge(best_spreads_copy[['Bid', 'Offer']], 
                                 how='left', left_index=True, right_index=True)
    
    new_rates = new_rates.reset_index()
    
    # Fill tenors greater than 10Y that didn't have a rate via spreads using 
    # close rates
    close_rates = new_rates.merge(closes_df, how='left', left_on='Tenor', 
                                  right_on='Tenor')
    rate130_close = close_rates[close_rates['Tenor']==130]['Close'].values[0]
    spread130 = []
    
    # Non essential tenors don't have to be filled
    for i in range(0, close_rates.shape[0]):
        if close_rates['Tenor'][i] in prev_rates.index:
            try:
                spread130.append(close_rates['Close'][i] - rate130_close)
            except:
                spread130.append('')
        else:
            pass
    # Spreads between 10Y and the rest of the tenors
    new_rates['Spread130'] = spread130
    
    # Bid rate for 10Y
    rate130_b = new_rates[new_rates['Tenor']==130]['Bid_y'].values[0]
    rate130_o = new_rates[new_rates['Tenor']==130]['Offer_y'].values[0]
    
    if rate130_b != '':
        if rate130_o == '':
            rate130_o = rate130_b
    
        conditions_b = [new_rates['Tenor']==1, 
                      (new_rates['Tenor']<=130) & (new_rates['Bid_y']==''), 
                      (new_rates['Tenor']<=130) & (new_rates['Bid_y'].isna()),
                      (new_rates['Tenor']>130) & (new_rates['Bid_y'].isna()) & (~new_rates['Offer_y'].isna()),
                      (new_rates['Tenor']>130) & (new_rates['Bid_y']=='') & (new_rates['Offer_y']!=''),
                      (new_rates['Tenor']>130) & (new_rates['Bid_y']=='') & (new_rates['Offer_y']==''),
                      (new_rates['Tenor']>130) & (new_rates['Bid_y'].isna()) & (new_rates['Offer_y'].isna())]
    
        options_b = [new_rates['Bid_x'], np.nan, np.nan,
                     new_rates['Offer_y'].fillna(0).replace('', 0) - rate130_o - .02 + rate130_b,
                     new_rates['Offer_y'].fillna(0).replace('', 0) - rate130_o - .02 + rate130_b,
                     rate130_b+new_rates['Spread130'], 
                     rate130_b+new_rates['Spread130']]
    
    else:
        conditions_b = [new_rates['Tenor']==1, 
                      (new_rates['Tenor']<=130) & (new_rates['Bid_y']==''), 
                      (new_rates['Tenor']<=130) & (new_rates['Bid_y'].isna())]
        
        options_b = [new_rates['Bid_x'], np.nan, np.nan]
    
    # Offer rate for 10Y

    if rate130_o != '':
        if rate130_b == '':
            rate130_b = rate130_o
    
        conditions_o = [
            new_rates['Tenor']==1, 
            (new_rates['Tenor']<=130) & (new_rates['Offer_y']==''), 
            (new_rates['Tenor']<=130) & (new_rates['Offer_y'].isna()),
            (new_rates['Tenor']>130) & (new_rates['Offer_y']=='') & (new_rates['Bid_y']!=''),
            (new_rates['Tenor']>130) & (new_rates['Offer_y'].isna()) & (~new_rates['Bid_y'].isna()),
            (new_rates['Tenor']>130) & (new_rates['Offer_y']=='') & (new_rates['Bid_y']==''),
            (new_rates['Tenor']>130) & (new_rates['Offer_y'].isna()) & (new_rates['Bid_y'].isna())]
    
        options_o = [new_rates['Offer_x'], np.nan, np.nan, 
                     new_rates['Bid_y'].fillna(0).replace('', 0) - rate130_b + .02 + rate130_o,
                     new_rates['Bid_y'].fillna(0).replace('', 0) - rate130_b + .02 + rate130_o,
                     rate130_o+new_rates['Spread130'], 
                     rate130_o+new_rates['Spread130']]
    
    else:
        conditions_o = [
            new_rates['Tenor']==1, 
            (new_rates['Tenor']<=130) & (new_rates['Offer_y']==''), 
            (new_rates['Tenor']<=130) & (new_rates['Offer_y'].isna())]
        
        options_o = [new_rates['Offer_x'], np.nan, np.nan]
    
    new_rates['Bid'] = np.select(conditions_b, options_b, new_rates['Bid_y'])
    new_rates['Offer'] = np.select(conditions_o, options_o, 
                                   new_rates['Offer_y'])
    
    rates_sheet.range('C4').value = new_rates[['Bid', 'Offer']].values
    
    # Activate TIIE_DESK macro in Excel file
    app=wb.app
    macro = app.macro('TIIE_IRS_Data.xlsm!TIIEDesk')
    macro()
    
#-------------------
#  Graph functions
#-------------------

def time_series_fn(start_date: datetime, end_date: datetime, 
                   graph_params: pd.DataFrame, historical_dates: list, 
                   mean = False, std = False) -> list:
    """Generates a DataFrame with all the time series included in the graph.
    

    Parameters
    ----------
    start_date : datetime
        Start date of the time series.
    end_date : datetime
        End date of the time series.
    graph_params : pd.DataFrame
        DataFrame with all the tenors of the time series you want to graph.
    historical_dates : list
        List with available historical dates.
    mean : TYPE, optional
        If you want to include the mean in the graph. The default is False.
    std : TYPE, optional
        Number of standard deviations you want to see in the graph. The 
        default is False.

    Returns
    -------
    list
        List with the following elements:
            time_series_df: DataFrame with the data of each time series 
            included in the graph. The columns are the different time series.
            graph_type: string that indicates if the type of graph

    """
    time_series_list = []

    for i, r in graph_params.iterrows():
        time_series = get_time_series(start_date, end_date, i, r, 
                                       historical_dates)
        time_series_list.append(time_series)
    time_series_df, graph_type = get_graph_df(time_series_list, mean, std,
                                             'Time Series')    
    return time_series_df, graph_type

def time_series_graph(time_series_df: pd.DataFrame, ax, mean: bool, std, 
                      rho_df=pd.DataFrame()) -> None:
    """
    

    Parameters
    ----------
    time_series_df : pd.DataFrame
        DataFrame with the data for each time series included in the graph.
    ax : TYPE
        Axis for the graph.
    mean : bool
        If you want to graph the mean or not.
    std : TYPE
        Number of standar deviations you want to graph. If set to false, no 
        standard deviations will appear on the graph.
    rho_df : pd.DataFrame, optional
        DataFrame with correlations. Only applies in spread graph. The default 
        is pd.DataFrame().

    Returns
    -------
    None

    """
    col_list = time_series_df.columns.tolist()
    
    for k in col_list:
        num = np.random.randint(0,len(colors))
        numlist = [num]
        while num in numlist:
            num = np.random.randint(0,len(colors))
            
        plt.plot(time_series_df[k], color=colors[num])
        
    ax.set_ylabel("Rate", color=colors[num])
    ax.set_xlabel("Date")
    cols = np.floor(len(col_list)/4)
    ax.legend(col_list, title='Time Series', loc='upper left', 
              bbox_to_anchor=(1.1, 0.45), ncol=1, fancybox=True, shadow=True)
    
    vals = ax.get_yticks()
    ax.set_yticklabels(['{:,.2%}'.format(x) for x in vals])
    

def spread_series_fn(start_date: datetime, end_date: datetime, 
                     graph_params: pd.DataFrame, historical_dates: list, 
                     mean=False, std=False) -> tuple:
    """Generates a DataFrame with time series for spreads included in graph.
    
    Calculates the spread time series for pairs of tenors and includes the mean
    and standard deviation in case the user indicates it.
    

    Parameters
    ----------
    start_date : datetime
        Start date of the time series.
    end_date : datetime
        End date of the time series.
    graph_params : pd.DataFrame
        DataFrame with the tenors used to calculate the spreads. The spreads
        will be calculated by pairs of tenors following the order of the
        DataFrame.
    historical_dates : list
        List of historical dates available in the data bases.
    mean : TYPE, optional
        Indicates if the mean will be included in the graph or not. The default 
        is False.
    std : float or bool, optional
        Indicates the number of standard deviations to show in the graph. If no
        standard deviation is to be displayed it is set to false. The default 
        is False.

    Returns
    -------
    tuple
        Tuple with the following elements:
            spread_df: DataFrame with the time series of the spreads (and mean 
            and standard deviation if indicated by the user)
            graph_type: string with the type of graph, in this case the graph
            type is "Spread".
            rho_df: DataFrame with correlations between tenors for each spread.

    """
    spread_list= []
    rho_df = pd.DataFrame()
    spread_pairs = [g for (i, g) 
                    in graph_params.groupby(graph_params.index // 2)]
    shapes = [g.shape[0] for g in spread_pairs if g.shape[0]!=2]
    
    if shapes:
        print('\nPlease make sure you have groups of two for Spread option.')
        return [pd.DataFrame(columns=['WARNING']), 'Spread']
    
    three_months = (ql.Date().from_date(end_date) +\
                    ql.Period(-3, ql.Months)).to_date()
    one_month = (ql.Date().from_date(end_date) +\
                 ql.Period(-1, ql.Months)).to_date()
    one_week = (ql.Date().from_date(end_date) +\
                ql.Period(-1, ql.Weeks)).to_date()
    
    
    for pair in spread_pairs:
        spread_pair = []
        start_tenors = []
        fwd_tenors = []
        pair = order_tenors(pair)
        for i, r in pair.iterrows():
            if r.Instrument == 'Tiie':
                start_tenor = r.Start_Tenor
                fwd_tenor = r.Forward_Tenor
            elif r.Instrument == 'Usd':
                start_tenor = r.Instrument + ' ' + r.Start_Tenor
                fwd_tenor = r.Forward_Tenor
            else:
                start_tenor = ''
                fwd_tenor = r.Instrument + ' ' + r.Forward_Tenor
            time_series = get_time_series(start_date, end_date, i, r, 
                                           historical_dates)
            spread_pair.append(time_series)
            start_tenors.append(start_tenor)
            fwd_tenors.append(fwd_tenor)
        time_series1, time_series2 = spread_pair
        start_tenor1, start_tenor2 = start_tenors
        fwd_tenor1, fwd_tenor2 = fwd_tenors
        
        real_timeseries1 = time_series1.dropna()
        real_timeseries2 = time_series2.dropna()
        common_dates = [d for d in real_timeseries1.index if d in 
                        real_timeseries2.index]
        
        three_month_dates = [d for d in common_dates if d > three_months]
        one_month_dates = [d for d in common_dates if d > one_month]
        one_week_dates = [d for d in common_dates if d > one_week]
        
        # If a Bono or Udi does not have info past a certain date, we have to 
        # calculate the corrrelation starting from the date the info starts.
        try:
            
            rho = time_series1.loc[common_dates].astype(float).corr(
                time_series2.loc[common_dates].astype(float))
            rho_df_a = pd.DataFrame(
                {'Corr ' + start_tenor1 + fwd_tenor1 + ' / ' + start_tenor2 + \
                 fwd_tenor2 + ' = ' +'{:,.2f}'.format(rho): rho}, 
                    index=time_series1.index)
            
            if pd.to_datetime(start_date) < pd.to_datetime(three_months):
                rho_three_month = time_series1.loc[three_month_dates]\
                    .astype(float).corr(time_series2.loc[three_month_dates]\
                                        .astype(float))
                rho_df_a['3 Month = ' + '{:,.2f}'.format(rho_three_month)] =\
                    rho_three_month
            
            if pd.to_datetime(start_date) < pd.to_datetime(one_month):
                rho_one_month = time_series1.loc[one_month_dates]\
                    .astype(float).corr(time_series2.loc[one_month_dates]\
                                        .astype(float))
                rho_df_a['1 Month = ' + '{:,.2f}'.format(rho_one_month)] =\
                    rho_one_month
                    
            if pd.to_datetime(start_date) < pd.to_datetime(one_week):
                rho_one_week = time_series1.loc[one_week_dates]\
                    .astype(float).corr(time_series2.loc[one_week_dates]\
                                        .astype(float))
                rho_df_a['1 Week = ' + '{:,.2f}'.format(rho_one_week)] =\
                    rho_one_week

            
            rho_df_a[' '] = ''
            rho_df = pd.concat([rho_df, rho_df_a], axis=1)
        
        except:
            print('Correlation could not be calculated.')
        
        spread_series = time_series1 - time_series2
        spread_series = spread_series * 10000
        spread_series.name =  start_tenor1+fwd_tenor1 + ' vs ' \
            + start_tenor2 + fwd_tenor2 + '  =  ' +\
                '{:,.2f}'.format(spread_series[-1]) 
        spread_series = spread_series.dropna()
        spread_list.append(spread_series)
        
    spread_df, graph_type = get_graph_df(spread_list, mean, std, 'Spread')
    
    return spread_df, graph_type, rho_df

def spread_graph(spread_df: pd.DataFrame, ax, mean: bool, std, 
                 rho_df: pd.DataFrame) -> None:
    """Plots spread graph.
    

    Parameters
    ----------
    spread_df : pd.DataFrame
        DataFrame with spread time series.
    ax : 
        Graph axis.
    mean : bool
        Indicates if mean will be graphed.
    std : float or bool
        Number of standard deviations that will be graphed. If False, no 
        standard deviations will be graphed.
    rho_df : pd.DataFrame
        DataFrame with correlations between time series.

    Returns
    -------
    None

    """
    
    col_list = spread_df.columns.tolist()
  
    for k in col_list:    
        num = np.random.randint(0,len(colors))
        numlist = [num]
        while num in numlist:
            num = np.random.randint(0,len(colors))
            
        plt.plot(spread_df[k], color=colors[num])
        
    ax.set_ylabel("Bps", color=colors[num])
    ax.set_xlabel("Date")
    
    cols = np.floor(len(col_list)/4)
        
    rho_text = '\n'.join(rho_df.columns[:-1])
    ax.legend(col_list+rho_df.columns.tolist(), title='Spread', 
              loc='upper left', bbox_to_anchor=(1.1, 0.25),
          ncol=1, fancybox=True, shadow=True)
    props = dict(boxstyle='round', alpha=0.5, facecolor='lightsteelblue')
    ax.text(1.1, 0.95, rho_text, transform=ax.transAxes, fontsize=11,
        verticalalignment='bottom', bbox=props)
    
   



def butterfly_series_fn(start_date: datetime, end_date: datetime, 
                        graph_params: pd.DataFrame, historical_dates: list, 
                        mean=False, std=False) -> tuple:
    """Creates a DataFrame with butterfly time series.
    

    Parameters
    ----------
    start_date : datetime
        Start date of the time seres.
    end_date : datetime
        End date of the time series.
    graph_params : pd.DataFrame
        DataFrame with tenors that will be used to calculate butterfly.
    historical_dates : list
        List with historical dates available in data bases.
    mean : bool, optional
        Indicates if mean will be graphed. The default is False.
    std : float or bool, optional
        Indicates the number of standard deviations to show in the graph. If no
        standard deviation is to be displayed it is set to false. The default 
        is False.

    Returns
    -------
    tuple
        Tuple with the following elements:
            butterfly_df: pd.DataFrame with butterfly time series
            graph_type: string that indicates the type of graph, in this case
            it is "Butterfly"

    """
    fly_list = []
    fly_pairs = [g for (i, g) in graph_params.groupby(graph_params.index // 3)]
    shapes = [g.shape[0] for g in fly_pairs if g.shape[0]!=3]
    if shapes:
        print('\nPlease make sure you have groups of three for Butterfly '\
              'option.')
        return [pd.DataFrame(columns=['WARNING']), 'Butterfly']
    else:
        for pair in fly_pairs:
            butterfly_list = []
            start_tenors = []
            fwd_tenors = []
            pair = order_tenors(pair)
            for i, r in pair.iterrows():
                if r.Instrument == 'Tiie':
                    start_tenor = r.Start_Tenor
                    fwd_tenor = r.Forward_Tenor
                elif r.Instrument == 'Usd':
                    start_tenor = r.Instrument + ' ' + r.Start_Tenor
                    fwd_tenor = r.Forward_Tenor
                else:
                    start_tenor = ''
                    fwd_tenor = r.Instrument + ' ' + r.Forward_Tenor
                time_series = get_time_series(start_date, end_date, i, r, 
                                               historical_dates)
                butterfly_list.append(time_series)
                start_tenors.append(start_tenor)
                fwd_tenors.append(fwd_tenor)
            
            time_series1, time_series2, time_series3 = butterfly_list
            start_tenor1, start_tenor2, start_tenor3 = start_tenors
            fwd_tenor1, fwd_tenor2, fwd_tenor3 = fwd_tenors
            butterfly_series = 2 * time_series2 - time_series1 - time_series3 
            butterfly_series = butterfly_series*10000
            butterfly_series.name = start_tenor3+fwd_tenor3 \
                + '/'+ start_tenor2+fwd_tenor2 + '/'+start_tenor1+fwd_tenor1 +\
                    '  =  ' + '{:,.2f}'.format(butterfly_series[-1])
            butterfly_series = butterfly_series.dropna()
            fly_list.append(butterfly_series)
        
        
        butterfly_df, graph_type = get_graph_df(fly_list, mean, std, 
                                                'Butterfly')
        return butterfly_df, graph_type
    
def butterfly_graph(butterfly_df: pd.DataFrame, ax, mean: bool, std, 
                    rho_df: pd.DataFrame) -> None:
    """Plots butterfly graph.
    

    Parameters
    ----------
    butterfly_df : pd.DataFrame
        DataFrame with butterfly time series.
    ax : 
        Graph axis.
    mean : bool
        Indicates if mean will be graphed.
    std : float or bool, optional
        Indicates the number of standard deviations to show in the graph. If no
        standard deviation is to be displayed it is set to false.
    rho_df : pd.DataFrame
        DataFrame with correlations. Only applies in spread graph. For
        butterfly graph this DataFrame is empty.

    Returns
    -------
    None

    """
    col_list = butterfly_df.columns.tolist()
    
    for k in col_list:
        num = np.random.randint(0,len(colors))
        numlist = [num]
        while num in numlist:
            num = np.random.randint(0,len(colors))
            
        plt.plot(butterfly_df[k], color=colors[num])
        
    ax.set_ylabel("Bps", color=colors[num])
        
        
    ax.set_xlabel("Date")
    
    cols = np.floor(len(col_list)/4)
    ax.legend(col_list, title='Butterfly', loc='upper left', 
              bbox_to_anchor=(1.1, 0.75), ncol=1, fancybox=True, shadow=True)

def spr_btfly_graph(tenors_df, ax):
    col_list = tenors_df.columns.tolist()
    for k in col_list:
        num = np.random.randint(0,len(colors))
        numlist = [num]
        while num in numlist:
            num = np.random.randint(0,len(colors))
            
        plt.plot(tenors_df[k], color=colors[num])
        
    ax.set_ylabel("Bps", color=colors[num])
    ax.set_xlabel("Date")
    cols = np.floor(len(col_list)/4)
    ax.legend(col_list, title='Spread/Btfly', loc='upper left', 
              bbox_to_anchor=(1.07, 0.25), ncol=1, fancybox=True, shadow=True)
   

    
def cross_sectional_series_fn(historical_dates: list, tenors: list, 
                              cs_params: pd.DataFrame) -> None:
    """Finds necessary data for cross sectional graph.

    Parameters
    ----------
    historical_dates : list
        List of historical dates available in data bases.
    tenors : list
        List of available tenors in Historical_IRS_Parameters Excel file.
    cs_params : pd.DataFrame
        DataFrame with the dates and tenors that will be graphed in cross
        sectional graph.

    Returns
    -------
    None

    """
    source_server = "//TLALOC/Cuantitativa"
    path_wdir = source_server +\
        "/Fixed Income/IRS Historical Tenors/Historical_IRS_Parameters.xlsx"
    cs_list = []
    
    for i, r in cs_params.iterrows():
        tenor = r.Tenor
        date = r.Date
        try:
            cs_sheet = pd.read_excel(path_wdir, tenor, index_col='Date', 
                                        parse_dates=True)
            
        except:
            print('Please check tenor value')
            return None
        
        date = check_dates(historical_dates, date, i=i)[0]
        
        if date < cs_sheet.index[0]:
            date = cs_sheet.index[0]
            
        while date not in cs_sheet.index:
            date = date + timedelta(-1)
        
        cs_data_df = pd.DataFrame()
        
        if tenor != 'Spot': 
            cs_data_df_a = pd.DataFrame(
                {date: [pd.read_excel(path_wdir, 'Spot', index_col='Date', 
                                      parse_dates=True).loc[date, tenor]]}, 
                index=['0m'])
            cs_data_df = pd.concat([cs_data_df, cs_data_df_a])
        
            for t in tenors:
                cs_data_df_a = pd.DataFrame(
                    {date: pd.read_excel(path_wdir, t, index_col='Date', 
                                         parse_dates=True).loc[date, tenor]}, 
                    index=[t])
                cs_data_df = pd.concat([cs_data_df, cs_data_df_a])       
                
            cs_data = cs_data_df[date] 
        
        else:  
            cs_data = cs_sheet.loc[date].T
            
        cs_data.name = date.strftime('%d/%m/%Y') + ' ' + tenor
        cs_data.index = list(map(lambda x:x.replace('Spot', '0m'),
                                 cs_data.index))
        index_months = np.select([cs_data.index.str[-1]=='y'], 
                                  [cs_data.index.str[:-1].astype(int)*13], 
                                  cs_data.index.str[:-1].astype(int))
        cs_data.index = index_months
        cs_list.append(cs_data)
    
    cross_sectional_graph(tenors, cs_list)
        
def cross_sectional_graph(tenors: list, cs_list: list) -> None:
    """Plots cross sectional graph.

    Parameters
    ----------
    tenors : list
        List of available tenors in Historical_IRS_Parameters Excel file.
    cs_list : list
        List with DataFrames with cross sectional data for each tenor indicated
        by the user.

    Returns
    -------
    None

    """

    num = np.random.randint(0,len(colors))
    
    
    tenors_df = pd.DataFrame({'Tenor': tenors})
    tenors_df = tenors_df.replace({'Spot': '0m'})
    index_months = np.select([tenors_df['Tenor'].str[-1]=='y'], 
                              [tenors_df['Tenor'].str[:-1].astype(int)*13], 
                              tenors_df['Tenor'].str[:-1].astype(int))
    
    # Ticks for x axis
    ticks = [i for i in index_months 
             if i not in [0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
    
    cs_df = pd.DataFrame(index=index_months)
    
    for i in range(0, len(cs_list)):
        cs_df[cs_list[i].name] = cs_list[i]
    
    col_list = cs_df.columns.tolist()
    
    fig, ax = plt.subplots(figsize=(15,7.5))
    plt.plot(cs_df, color = colors[num])
    plt.xlabel("Tenor")
    plt.ylabel("Rate")

    cols = np.floor(len(col_list)/4)
    plt.legend(col_list, loc='upper left', bbox_to_anchor=(1, 0.5),
          ncol=1, fancybox=True, shadow=True)
    plt.title("IRS TIIE Cross Sectional Analysis")
    vals = ax.get_yticks()
    ax.set_yticklabels(['{:,.1%}'.format(x) for x in vals])
    plt.xticks(ticks, rotation=90)
    plt.grid()
    plt.show()
    print('\nGraph done!')

def sql_query_gen(st_date, ed_date, instruments, stenors, ftenors):
    

    # st_date = '2023-01-01'
    # ed_date = '2023-10-31'
    months_dic = {'01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr', '05': 'May', 
                  '06': 'Jun', '07': 'Jul', '08': 'Ago', '09': 'Sep', '10': 'Oct', 
                  '11': 'Nov', '12': 'Dic'}

    rev_months = {v:k for k, v in months_dic.items()} 

    # instrument = ['TIIE', 'TIIE', 'TIIE', 'TIIE', 'Bono', 'Udi']
    # stenors = ['1y', '1y', '3m', 'Spot', 'None', 'None']
    # ftenors = ['1y', '2y', '3m', '1y', 'Mar27', 'Nov31']
    cont  = 0

    if instruments[0] == 'Tiie' or instruments[0] == 'Usd':
        froms = f" FROM [dbo].[Derivatives] AS D{cont}"
        varas = f"SELECT *, D{cont}.br_rate AS '{stenors[0]+ftenors[0]}'"
        cols = [f'{stenors[0]+ftenors[0]}']
        if instruments[0] == 'Tiie':

            wheres = f" WHERE D{cont}.dv_date >= '{st_date}' AND D{cont}.dv_date <= '{ed_date}'"+\
                f" AND D{cont}.dv_stenor = '{stenors[0]}' AND D{cont}.dv_ftenor = '{ftenors[0]}'"
        else:
            if stenors[0] == 'Spot':
                wheres = f" WHERE D{cont}.dv_date >= '{st_date}' AND D{cont}.dv_date <= '{ed_date}'"+\
                    f" AND D{cont}.dv_stenor = 'SOFR' AND D{cont}.dv_ftenor = '{ftenors[0].upper()}'"
            
            else:
                wheres = f" WHERE D{cont}.dv_date >= '{st_date}' AND D{cont}.dv_date <= '{ed_date}'"+\
                        f" AND D{cont}.dv_stenor = 'SOFR' AND D{cont}.dv_ftenor ="+\
                            f" ' {stenors[0].upper() + ftenors[0].upper()}'"
                
                
        
    
    else:
        froms = f" FROM [dbo].[BondRates] AS D{cont} INNER JOIN "+\
            f"[dbo].[BondData] AS B{cont} ON D{cont}.br_isin = B{cont}.tic_isin"
        varas = f"SELECT *, D{cont}.br_rate/100 AS '{ftenors[0]}'"
        cols = [f'{ftenors[0]}']
        if instruments[0] == 'Bono':
            
            wheres = f" WHERE D{cont}.br_date >= '{st_date}' AND D{cont}.br_date <= '{ed_date}'"+\
                f" AND B{cont}.tic_name = 'Mbono'  AND"+\
                    f" LEFT (B{cont}.tic_maturityDate, 6) ="+\
                        f" '{'20' + ftenors[0][3:] + rev_months[ftenors[0][:3]]}'"
        else:
            wheres = f" WHERE D{cont}.br_date >= '{st_date}' AND D{cont}.br_date <= '{ed_date}'"+\
                f" AND B{cont}.tic_name = 'Sbono'  AND"+\
                    f" LEFT (B{cont}.tic_maturityDate, 6) ="+\
                        f" '{'20' + ftenors[0][3:] + rev_months[ftenors[0][:3]]}'"
        


    for i,s,f in zip(instruments[1:], stenors[1:], ftenors[1:]):
        cont += 1
        
        
        if ((i == 'Tiie' or i == 'Usd') and 
            (instruments[cont-1] == 'Tiie' or instruments[cont-1] == 'Usd')):
            
            
            
            froms = froms + f" INNER JOIN [dbo].[Derivatives] AS D{cont} ON "+\
                f"D{cont-1}.dv_date = D{cont}.dv_date"
            if i == 'Tiie':    
                wheres = wheres + f" AND D{cont}.dv_stenor = '{s}' AND"+\
                    f" D{cont}.dv_ftenor = '{f}'"
            
            else:
                if s == 'Spot':
                    wheres = wheres + f" AND D{cont}.dv_stenor = 'SOFR' AND"+\
                        f" D{cont}.dv_ftenor = '{f.upper()}'"
                else:
                    wheres = wheres + f" AND D{cont}.dv_stenor = 'SOFR' AND"+\
                        f" D{cont}.dv_ftenor = ' {s.upper()+f.upper()}'"
                    
                    
                    
            
            var = f", D{cont}.br_rate AS '{s+f}'"
            varas = varas + var
            
            cols.append(f'{s+f}')
            
            
            
                
        elif ((i == 'Tiie' or i == 'Usd') and 
              (instruments[cont-1] == 'Bono' or instruments[cont-1] == 'Udi')):
            
            froms = froms + f" INNER JOIN [dbo].[Derivatives] AS D{cont} ON "+\
                f"D{cont-1}.br_date = D{cont}.dv_date"
            
            if i == 'Tiie':    
                wheres = wheres + f" AND D{cont}.dv_stenor = '{s}' AND"+\
                    f" D{cont}.dv_ftenor = '{f}'"
            
            else:
                if s == 'Spot':
                    wheres = wheres + f" AND D{cont}.dv_stenor = 'SOFR' AND"+\
                        f" D{cont}.dv_ftenor = '{f.upper()}'"
                else:
                    wheres = wheres + f" AND D{cont}.dv_stenor = 'SOFR' AND"+\
                        f" D{cont}.dv_ftenor = ' {s.upper()+f.upper()}'"
                
            var = f", D{cont}.br_rate AS '{s+f}'"
            varas = varas + var
            cols.append(f'{s+f}')
        
        elif ((i == 'Bono' or i == 'Udi') and 
              (instruments[cont-1] == 'Bono' or instruments[cont-1] == 'Udi')):
            
            froms = froms + f" INNER JOIN [dbo].[BondRates] AS D{cont} ON "+\
                f"D{cont-1}.br_date = D{cont}.br_date"+\
                    f" INNER JOIN [dbo].[BondData] AS B{cont} ON "+\
                        f"D{cont}.br_isin = B{cont}.tic_isin"
            
            var = f", D{cont}.br_rate/100 AS '{f}'"
            varas = varas + var
            cols.append(f'{f}')
            
            if i == 'Bono':
                wheres = wheres + f" AND B{cont}.tic_name = 'Mbono' AND"+\
                    f" LEFT (B{cont}.tic_maturityDate, 6) ="+\
                        f" '{'20' + f[3:] + rev_months[f[:3]]}'"
            else:
                wheres = wheres + f" AND B{cont}.tic_name = 'Sbono' AND"+\
                    f" LEFT (B{cont}.tic_maturityDate, 6) ="+\
                        f" '{'20' + f[3:] + rev_months[f[:3]]}'"
        
        else:
            froms = froms + f" INNER JOIN [dbo].[BondRates] AS D{cont} ON "+\
                f"D{cont-1}.dv_date = D{cont}.br_date"+\
                    f" INNER JOIN [dbo].[BondData] AS B{cont} ON "+\
                        f"D{cont}.br_isin = B{cont}.tic_isin"
            
            var = f", D{cont}.br_rate/100 AS '{f}'"
            varas = varas + var
            cols.append(f'{f}')
            
            if i == 'Bono':
                wheres = wheres + f" AND B{cont}.tic_name = 'Mbono' AND"+\
                    f" LEFT (B{cont}.tic_maturityDate, 6) ="+\
                        f" '{'20' + f[3:] + rev_months[f[:3]]}'"
            else:
                wheres = wheres + f" AND B{cont}.tic_name = 'Sbono' AND"+\
                    f" LEFT (B{cont}.tic_maturityDate, 6) ="+\
                        f" '{'20' + f[3:] + rev_months[f[:3]]}'"
    
    query = varas + froms + wheres
    
    return query, cols

def get_time_series(start_date: datetime, end_date: datetime, i: int, 
                     r: pd.Series, historical_dates: list) -> pd.Series:
    """Gets time series for specific dates and tenor.

    Parameters
    ----------
    start_date : datetime
        Start date of time series.
    end_date : datetime
        End date of time series.
    i : int
        Index in given DataFrame of the series.
    r : pd.Series
        Series with tenor info.
    historical_dates : list
        List of historical dates available in data bases.

    Returns
    -------
    time_series: pd.Series
        Series with time series data.

    """
    

    instrument = [r.Instrument]
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    if instrument[0] == 'Tiie':
     
        start_tenor = [r.Start_Tenor]
        fwd_tenor = [r.Forward_Tenor]
        
        query, cols = sql_query_gen(start_date, end_date, instrument,
                                    start_tenor, fwd_tenor)
        try:
            time_series = pd.read_sql_query(query, conn,
                                                  index_col='dv_date', 
                                                  parse_dates=True)[cols[0]]
            
        except:
            print(f'Please check start tenor value for trade {i+1}.')
            return pd.Series()
        
        time_series.name = cols[0] + "  =  " +\
            "{:,.2f}%".format(time_series.iloc[-1]*100)
        dates = [d for d in historical_dates if d in time_series.index]
        time_series = time_series.loc[dates]
        
    
    elif instrument[0] == 'Bono' or instrument[0] == 'Udi':
        
        start_tenor = [r.Start_Tenor]
        fwd_tenor = [r.Forward_Tenor]
        
        query, cols = sql_query_gen(start_date, end_date, instrument,
                                    start_tenor, fwd_tenor)

        time_series = pd.read_sql_query(query, conn,
                                              index_col='br_date', 
                                              parse_dates=True)[cols[0]]
        
        time_series.name = instrument[0] + ' ' + cols[0] + "  =  " +\
            "{:,.2f}%".format(time_series[-1]*100)
        
        dates = [d for d in historical_dates if d in time_series.index]
        time_series = time_series.loc[dates]
        
            
    elif instrument[0] == 'Usd':
        start_tenor = [r.Start_Tenor]
        fwd_tenor = [r.Forward_Tenor]
        
        query, cols = sql_query_gen(start_date, end_date, instrument,
                                    start_tenor, fwd_tenor)
        
        time_series = pd.read_sql_query(query, conn,
                                             index_col='dv_date', 
                                             parse_dates=True)[cols[0]]/100
       
        time_series.name = 'Usd' + ' ' + cols[0] + "  =  " +\
            "{:,.2f}%".format(time_series[-1]*100)
        
        dates = [d for d in historical_dates if d in time_series.index]
        time_series = time_series.loc[dates]  
    
    all_dates = [d for d in historical_dates 
                 if d >= start_date and d <= end_date]
    
    complete_series = pd.Series(data=np.nan, index=all_dates)
    complete_series.loc[dates] = time_series.loc[dates]
    
    if instrument[0] != 'Tiie' and instrument[0] != 'Usd':
        time_series = interp(complete_series, instrument, start_tenor, fwd_tenor)
    
    return time_series




def get_graph_df(graph_list: list, mean: bool, std, graph_type: str) -> tuple:
    """Concatenates all series in a single DataFrame to graph.
    
    Assigns names to the columns of the DataFrame to show them in the legend
    of the graph. This function also adds the mean and standard deviation to 
    the DataFrame to display them in the graph.

    Parameters
    ----------
    graph_list : list
        List with time series that will be graphed.
    mean : bool
        Indicates if the mean will be shown in the graph.
    std : float or bool
        Indicates the number of standard deviations to show in the graph. If no
        standard deviation is to be displayed it is set to false.
    graph_type : str
        Indicates if the graph is for normal time series, spread or butterfly.

    Returns
    -------
    tuple
        Tuple with the following elements:
            graph_df: DataFrame with columns that will be graphed.
            graph_type: string that indicades the type of graph.

    """
    
    graph_df = pd.DataFrame()
    
    for i in range(0, len(graph_list)):
        graph_df[graph_list[i].name] = graph_list[i]
        cut = graph_list[i].name.find('=')
        series_name = graph_list[i].name[:cut]
        
        if mean:
            
            if graph_type == 'Time Series':
                mean_name = 'Mean '+series_name +'  =  ' +\
                    '{:,.2f}%'.format(np.average(graph_list[i])*100)

            else:
                mean_name = 'Mean '+series_name +'  =  ' +\
                    '{:,.2f}'.format(np.average(graph_list[i]))
                    
            graph_df[mean_name] = np.average(graph_list[i])
        
        if std:
            std_plus = np.average(graph_list[i]) + std * np.std(graph_list[i])
            std_minus = np.average(graph_list[i]) - std * np.std(graph_list[i])
            
            if graph_type == 'Time Series':
                std_plus_name = f'+{str(int(std))} Std '+ series_name + \
                          '  =  ' + '{:,.2f}%'.format(std_plus*100)
                std_minus_name = f'-{str(int(std))} Std '+ series_name + \
                          '  =  ' + '{:,.2f}%'.format(std_minus*100)
            
            else:
                std_plus_name = f'+{str(int(std))} Std '+ series_name + \
                          '  =  ' + '{:,.2f}'.format(std_plus)
                std_minus_name = f'-{str(int(std))} Std '+ series_name + \
                          '  =  ' + '{:,.2f}'.format(std_minus)
            
            graph_df[std_plus_name] = std_plus
            graph_df[std_minus_name] = std_minus
    
    return graph_df, graph_type

def check_dates(historical_dates: list, start_date: datetime, end_date=False, 
                i=0) -> tuple:
    """Checks that the dates given by the user are in the historical database.
    
    Parameters
    ----------
    historical_dates : list
        List of historical dates available in data bases.
    start_date : datetime
        Start Date given by the user.
    end_date : datetime or bool, optional
        End Date given by the user. For cross sectional graphs there is no end 
        date so it is set to False. The default is False.
    i : int, optional
        Index of the tenor that will be graphed. It is only used for cross 
        sectional graph. The default is 0.

    Returns
    -------
    tuple
        Tuple with the following elements:
            start_date: Modified start date (datetime)
            end_date: Modified end date (datetime)

    """
    
    if start_date < historical_dates[0]:
        start_date = historical_dates[0]
        str_start0 = historical_dates[0].strftime('%Y/%m/%d')
        print(f'\nStart Date will be set to {str_start0}.')
    
    flag_start = False
    while start_date not in historical_dates:
        start_date = start_date + timedelta(-1)
        flag_start = True
    
    if flag_start:
        if end_date:
            str_start = start_date.strftime('%Y/%m/%d')
            print(f'\nStart Date will be set to {str_start}.')
        else:
            str_start = start_date.strftime('%Y/%m/%d')
            print(f'\nStart Date for trade {i+1} will be set to {str_start}.')
    
    if end_date:
        flag_end = False
        while end_date not in historical_dates:
            end_date = end_date + timedelta(-1)
            flag_end = True
    
    
        if flag_end:
            str_end = end_date.strftime('%Y/%m/%d')
            print(f'End Date will be set to {str_end}.')
    
    else:
        end_date = 0
        
    return start_date, end_date
    

def order_tenors(graph_params: pd.DataFrame) -> pd.DataFrame:
    """Orders DataFrame by tenor to calculate spread or butterfly correctly.
    
    Orders tenors in descending order to know which is the long, short or belly
    tenor.

    Parameters
    ----------
    graph_params : pd.DataFrame
        DataFrame with tenors that will be graphed.

    Returns
    -------
    graph_order : pd.DataFrame
        Ordered DataFrame.

    """
    
    graph_order = graph_params.copy()
    set_instruments = set(graph_params['Instrument'].unique())
    len_params = graph_params.shape[0]
    
    # Swaps are ordered in descending order
    if set_instruments.issubset({'Tiie', 'Usd'}):
        graph_order.replace({'Spot': '0m'}, inplace=True)
        graph_order['Start_Period'] =\
            graph_order['Start_Tenor'].apply(lambda x : x[-1])
        graph_order['End_Period'] =\
            graph_order['Forward_Tenor'].apply(lambda x : x[-1])
        
        try:
            graph_order['Start_Tenor_Num'] =\
                graph_order['Start_Tenor'].apply(lambda x : int(x[:-1]))
            graph_order['End_Tenor_Num'] =\
                graph_order['Forward_Tenor'].apply(lambda x : int(x[:-1]))
        except:
            print('Please check your tenors. Use "m" for months and "y" for' +
                  ' years.')
            graph_order.replace({'0m': 'Spot'}, inplace=True)
            return graph_order
        
        graph_order['Start_Plazo'] = np.select(
            [graph_order['Start_Period']=='y'], 
            [graph_order['Start_Tenor_Num']*13], 
            graph_order['Start_Tenor_Num'])
        
        graph_order['End_Plazo'] = np.select(
            [graph_order['End_Period']=='y'], 
            [graph_order['End_Tenor_Num']*13], 
            graph_order['End_Tenor_Num'])
        
        graph_order['Total_Plazo'] = graph_order['Start_Plazo'] +\
            graph_order['End_Plazo']
        
        graph_order = graph_order.sort_values(by = ['Total_Plazo', 
                                                    'End_Plazo'], 
                                              ascending=False)
        
        graph_order.replace({'0m': 'Spot'}, inplace=True)
    
    # For spreads between swaps and bonos, swaps always are the long tenor
    elif (len_params == 2 and 'Tiie' in set_instruments):
        
        if 'Bono' in set_instruments:
            graph_order = graph_order.sort_values(by = 'Instrument', 
                                                  ascending=False)
        elif 'Udi' in set_instruments:
            graph_order = graph_order.sort_values(by = 'Instrument', 
                                                  ascending=True)
    
    # For spreads between bonos and udis, bonos are always the long tenor
    elif (len_params == 2 and set_instruments == {'Bono', 'Udi'}):
        graph_order = graph_order.sort_values(by = 'Instrument', 
                                              ascending=True)
        
    # Spreads between bonos of the same kind are calculated as always, long 
    # minus short
    elif (len_params == 2 and set_instruments.issubset({'Bono', 'Udi'})):
        meses_dic = {'Ene': 1, 'Feb': 2, 'Mar': 3, 'Abr': 4,
                     'May': 5, 'Jun': 6, 'Jul': 7, 'Ago': 8,
                     'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dic': 12}
        graph_order['Year'] = graph_order['Forward_Tenor'].apply(
            lambda x: int(x[-2:]))
        graph_order['Month'] = graph_order['Forward_Tenor'].apply(
            lambda x: meses_dic[x[:3]])
        graph_order = graph_order.sort_values(by = ['Year', 'Month'], 
                                              ascending=False)
    
    return graph_order

def graph_display(dflist: list, graph_type_list: list, mean: bool, std: float, 
                  rho_df: pd.DataFrame = pd.DataFrame()) -> None:
    """Displays complete graph.

    Parameters
    ----------
    dflist : list
        List of DataFrames that will be graphed. This can include time series, 
        spread and butterfly series.
    graph_type_list : list
        List with different types of graphs that will be included in the final
        graph.
    mean : bool
        Indicates if the mean will be shown in the graph.
    std : float or bool
        Indicates the number of standard deviations to show in the graph. If no
        standard deviation is to be displayed it is set to false.
    rho_df : pd.DataFrame, optional
        DataFrame with correlations between time series. Only applies for 
        spreads. The default is pd.DataFrame().

    Returns
    -------
    None

    """
    dic_types = {'Time Series': time_series_graph, 'Spread': spread_graph,
                 'Butterfly': butterfly_graph}
    
    if len(dflist) == 1:
        fig, ax = plt.subplots(figsize=(15,7.5))
        plt.title(graph_type_list[0]+' Analysis')
        dic_types[graph_type_list[0]](dflist[0], ax, mean, std, rho_df)
        plt.grid()
        plt.show()
        print('\nGraph done!')
        
    elif len(dflist)==2:
        mean = False
        std = False
        fig, ax1 = plt.subplots(figsize=(15,7.5))
        plt.title(' / '.join(graph_type_list)+' Analysis')
        dic_types[graph_type_list[0]](dflist[0], ax1, mean, std, rho_df)
        ax2 = ax1.twinx()
        dic_types[graph_type_list[1]](dflist[1], ax2, mean, std, rho_df)
        ax1.grid(True, axis = 'x')
        ax2.grid(True, axis = 'y')
        plt.show()
        print('\nGraph done!')
    
    elif len(dflist)==3:
        mean = False
        std = False
        fig, ax1 = plt.subplots(figsize=(15,7.5))
        plt.title(' / '.join(graph_type_list)+' Analysis')
        dic_types[graph_type_list[0]](dflist[0], ax1, mean, std, rho_df)
        ax2 = ax1.twinx()
        dic_types[graph_type_list[1]](dflist[1], ax2, mean, std, rho_df)
        ax1.grid(True, axis = 'x')
        ax2.grid(True, axis = 'y')
        ax3 = ax1.twinx()
        ax3.spines.right.set_position(("axes", 1.05))
        dic_types[graph_type_list[2]](dflist[2], ax3, mean, std, rho_df)
        plt.show()
        print('\nGraph done!')
    
        
        
def graph_option(graph_file: str) -> None:
    """Function that will be called when graphing option is selected.
    

    Parameters
    ----------
    graph_file : str
        Name of Excel file with parameters for graph. Usually the file is 
        "IRS_Parameters.xlsm".

    Returns
    -------
    None

    """

    source_server = "//TLALOC/Cuantitativa/Fixed Income/IRS Historical Tenors"
    path_wdir_tiie = source_server +\
        "/Historical_IRS_Parameters.xlsx"
    path_wdir_bonos = source_server + "/Historical MbonoUdibonoUSSwaps.xlsx"
    
    # historical_dates_tiie = pd.read_excel(path_wdir_tiie, '3m', 
    #                                       index_col='Date', parse_dates=True)
    # historical_dates_bonos = pd.read_excel(path_wdir_bonos, skiprows=5, 
    #                                        index_col = 0)
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    historical_dates_tiie = pd.read_sql_query(
        "SELECT DISTINCT(dv_date) AS 'Dates' FROM [dbo].[Derivatives] ORDER "+\
            "BY 'Dates'", conn)['Dates'].tolist()
    
    historical_dates_bonos = pd.read_sql_query(
        "SELECT DISTINCT(br_date) AS 'Dates' FROM [dbo].[BondRates] ORDER "+\
            "BY 'Dates'", conn)['Dates'].tolist()
    
    # historical_dates = [d for d in historical_dates_tiie.index if d 
    #                     in historical_dates_bonos.index]
    
    historical_dates = [d for d in historical_dates_tiie if d 
                        in historical_dates_bonos]
    
    #parameters_file = pd.read_excel(graph_file, header=None, usecols=[0, 1])
    parameters_book = xw.Book(graph_file)
    graph_sheet = parameters_book.sheets('Graphs')
    # graph_type = parameters_file.iloc[0, 1]
    graph_type = graph_sheet.range('B1').value
    
    try:
        # start_date = parameters_file.iloc[1, 1]
        # end_date = parameters_file.iloc[2, 1]
        start_date = graph_sheet.range('B2').value
        end_date = graph_sheet.range('B3').value
    
    except:
        start_date = 0
        end_date = 0
    
    try:
        #mean = parameters_file.iloc[3, 1]
        mean = graph_sheet.range('B4').value
    
    except:
        mean = 0
    
    try:
        std = graph_sheet.range('B5').value
        #std = parameters_file.iloc[4, 1]
    
    except:
        std = 0
    
    if std==None or std==0  or np.isnan(std):
        std=False
    
    # Time series parameters
    # graph_params_ts = pd.read_excel(graph_file, usecols=[3, 4, 5]).dropna(
    #     subset='TS Instrument')
    graph_params_ts = graph_sheet.range('D1').expand('table').options(
        pd.DataFrame, header=True, index=False).value
    graph_params_ts = graph_params_ts.dropna(subset='TS Instrument')
    
    # Spread parameters
    # graph_params_sp = pd.read_excel(graph_file, usecols=[7, 8, 9]).dropna(
    #     subset='Spread Instrument')
    graph_params_sp = graph_sheet.range('H1').expand('table').options(
        pd.DataFrame, header=True, index=False).value
    graph_params_sp = graph_params_sp.dropna(subset='Spread Instrument')
    
    # Butterfly parameters
    # graph_params_bt = pd.read_excel(graph_file, usecols=[11, 12, 13]).dropna(
    #     subset='Btfly Instrument')
    graph_params_bt = graph_sheet.range('L1').expand('table').options(
        pd.DataFrame, header=True, index=False).value
    graph_params_bt = graph_params_bt.dropna(subset='Btfly Instrument')
    
    dflist = []
    graph_type_list = []
    rho_df = pd.DataFrame()
    
    total_series = int(graph_params_ts.shape[0]>0) +\
        int(graph_params_sp.shape[0]>0) + int(graph_params_bt.shape[0]>0)
    
    # If more than one type of series will be graphed, mean and std are set to 
    # False to avoid cluttering the graph.     
    if total_series>1:
        mean = False
        std = False
        
                                              
    if graph_type=='Time Series':
        start_date, end_date = check_dates(historical_dates, start_date,
                                           end_date)
        if not graph_params_ts.empty:
            graph_params_ts['TS Instrument'] =\
                graph_params_ts['TS Instrument'].astype(str).str.capitalize()
            graph_params_ts['TS Start_Tenor'] =\
                graph_params_ts['TS Start_Tenor'].astype(str).str.capitalize()
            graph_params_ts['TS Forward_Tenor'] =\
                graph_params_ts['TS Forward_Tenor'].astype(str).str\
                    .capitalize()
            graph_params_ts.columns = ['Instrument', 'Start_Tenor',
                                       'Forward_Tenor']
            try:
                time_series_df, graph_type =\
                    time_series_fn(start_date, end_date, graph_params_ts, 
                                   historical_dates, mean, std)
            except:
                print('Something went wrong. Please check your inputs for '\
                      'time series.')
                return None
            
            dflist.append(time_series_df)
            graph_type_list.append(graph_type)
            
        if not graph_params_sp.empty:
            graph_params_sp['Spread Instrument'] =\
                graph_params_sp['Spread Instrument'].astype(str).str\
                    .capitalize()
            graph_params_sp['Spread Start_Tenor'] =\
                graph_params_sp['Spread Start_Tenor'].astype(str).str\
                    .capitalize()
            graph_params_sp['Spread Fwd_Tenor'] =\
                graph_params_sp['Spread Fwd_Tenor'].astype(str).str\
                    .capitalize()
            graph_params_sp.columns = ['Instrument', 'Start_Tenor',
                                       'Forward_Tenor']
            
            try:
                spread_df, graph_type1, rho_df =\
                    spread_series_fn(start_date, end_date, graph_params_sp, 
                                     historical_dates, mean, std)
            except:
                print('Something went wrong. Please check your inputs for '\
                      'spread series.')
                return None
                
            dflist.append(spread_df)
            graph_type_list.append(graph_type1)
            
        if not graph_params_bt.empty:
            graph_params_bt['Btfly Instrument'] =\
                graph_params_bt['Btfly Instrument'].astype(str).str\
                    .capitalize()
            graph_params_bt['Btfly Start_Tenor'] =\
                graph_params_bt['Btfly Start_Tenor'].astype(str).str\
                    .capitalize()
            graph_params_bt['Btfly Fwd_Tenor'] =\
                graph_params_bt['Btfly Fwd_Tenor'].astype(str).str.capitalize()
            graph_params_bt.columns = ['Instrument', 'Start_Tenor',
                                       'Forward_Tenor']
            
            try:
                btfly_df, graph_type2 =\
                    butterfly_series_fn(start_date, end_date, graph_params_bt, 
                                        historical_dates, mean, std)
            except:
                print('Something went wrong. Please check your inputs for '\
                      'butterfly series.')
                return None
                
            dflist.append(btfly_df)
            graph_type_list.append(graph_type2)
        
        graph_display(dflist, graph_type_list, mean, std, rho_df)
        
    # Cross sectional graph    
    elif graph_type=='Cross Sectional':
        tenors_sheet = pd.read_excel('IRS_Parameters.xlsm', 'Tenor')
        tenors = tenors_sheet['tenor label'].tolist()
        cs_params = pd.read_excel(graph_file, usecols=[4, 5])
        cs_params = cs_params.dropna()
        
        try:
            cs_params['Date']=pd.to_datetime(
                (cs_params['Date'] - 25569) * 86400.0, unit='s')
        
        except:
            pass
        
        cs_params = cs_params.sort_values(by='Date')
        cs_params['Tenor'] = cs_params['Tenor'].astype(str).str.capitalize()
        cross_sectional_series_fn(historical_dates, tenors, cs_params)
    

        
def collapse_blotter(wb: xw.Book) -> None:
    """Fills blotter sheet from collapse file.

    Parameters
    ----------
    wb : xw.Book
        TIIE_IRS_Data Excel file.

    Returns
    -------
    None

    """
    
    collapse_sheet = wb.sheets('Collapse')
    rango = collapse_sheet.range('A1').end('right').address[:-1]+str(
        collapse_sheet.range('A1').end('down').row)
    collapse_df = collapse_sheet.range('A1', rango).options(
        pd.DataFrame, header=1, index=False).value
    
    if 'Trade Key' in collapse_df.columns:
        
    # Dictionary to rename columns
        column_names = {'Effective Date': 'Start_Date', 
                        'Maturity Date': 'End_Date', 'Coupon': 'Rate', 
                        'Notional': 'Notional_MXN', 'Bloomberg NPV':'NPV_MXN', 
                        'DV01': 'DV01_USD'}
        collapse_df = collapse_df.rename(columns = column_names)
        # Fill Notional with appropriate sign
        collapse_df['Notional_MXN'] = np.select(
            [collapse_df['Side']=='PAY'], [-collapse_df['Notional_MXN']], 
            default=collapse_df['Notional_MXN'])
        
    elif 'TNum' in collapse_df.columns:
        
        # Dictionary to rename columns
        column_names = {'Rate/Spread': 'Rate', 'Effective Date': 'Start_Date',
                          'End Date (Unadjusted)': 'End_Date', 
                          'Pay/Rcv': 'Side','Notional': 'Notional_MXN'}
        collapse_df = collapse_df.rename(columns = column_names)
        
        # Fill Notional with appropriate sign
        collapse_df['Notional_MXN'] = np.select(
            [collapse_df['Side']=='CRCV'], [-collapse_df['Notional_MXN']], 
            default=collapse_df['Notional_MXN'])
        
    def change_date(d):
        try: 
            c = pd.to_datetime(d.strftime('%d-%m-%Y'), dayfirst=False).date()
        except:
            c = pd.to_datetime(d, dayfirst=False).date()
        
        return c
            
  
        
    collapse_df['Start_Date'] = collapse_df['Start_Date'].apply(change_date)
    collapse_df['End_Date'] = collapse_df['End_Date'].apply(change_date)
    
    


    book = int(input('Book: '))
    cpty = input('Counterparty: ')
    fee = float(input('Fee: '))
    
    blotter_sheet = wb.sheets('Blotter')
    blotter_sheet.activate()
    last_row = blotter_sheet.range('B1').end('down').row + 1
    if last_row > 10000:
        last_row = 2
    blotter_sheet.range('B'+ str(last_row)).value = np.array(
        [book]*collapse_df.shape[0]).reshape(-1, 1)
    blotter_sheet.range('E'+ str(last_row)).value = \
        collapse_df[['Start_Date']].values
        
    blotter_sheet.range('F'+ str(last_row)).value = \
        collapse_df[['End_Date']].values
    blotter_sheet.range('G'+ str(last_row)).value = \
        collapse_df[['Notional_MXN']].values
    blotter_sheet.range('H'+ str(last_row)).value = \
        collapse_df[['Rate']].values/100
    blotter_sheet.range('K'+ str(last_row)).value = np.array(
        [cpty]*collapse_df.shape[0]).reshape(-1, 1)
    blotter_sheet.range('L'+ str(last_row)).value = fee
    collapse_sheet.clear_contents()
    
def collapse_fee(start: ql.Date, maturity: ql.Date, notional: float, ccp: str, 
                 evaluation_date: datetime) -> float:
    """Calculates fees for swaps in collapse option.
    

    Parameters
    ----------
    start : ql.Date
        Swap start date.
    maturity : ql.Date
        Swap end date.
    notional : float
        Swap notional.
    ccp : str
        Clearing service, could be CME or LCH.
    evaluation_date : datetime
        Date of evalutaion.

    Returns
    -------
    float
        Fee associated with given swap.

    """
    # Fees for CME
    if ccp == 'CME':
        fees = {3: .25, 6:.5, 13: 1, 39: 2.5, 78: 4.5, 117: 6, 156: 8, 208: 10, 
                    273: 12.5, 338: 15, 403: 17.5, 663: 24}
    
    # Fees for LCH
    elif ccp == 'LCH':
        fees = {13: .9, 39: 2.25, 65: 4.05, 91: 5.4, 130: 7.2, 156: 8.1, 
                195: 9, 260: 13.5, 325: 16.2, 663: 18}
    
    else:
        print('CCP fees not found.')
        return 0
    

    tenor = (maturity-ql.Date().from_date(evaluation_date))/28
    
    tenors = list(fees.keys())
    tenors.sort()
    
    for t in tenors:
        if tenor < t:
            fee = fees[t]*abs(notional)/1000000
            break
    
    return fee

def remate_closes(wb: xw.Book, evaluation_date: datetime) -> None:
    """Fills Remate closes in TIIE_IRS_Data Excel file.

    Parameters
    ----------
    wb : xw.Book
        TIIE_IRS_Data Excel file.
    evaluation_date : datetime
        Date of evaluation.
    
    Returns
    -------
    None

    """
    
    # Look for pdf file with Remate closes
    file_path = r'\\TLALOC\tiie\Remate\REMATE CLOSING PRICES FOR '
    yesterday_ql = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                       ql.Period(-1, ql.Days))
    file = file_path + yesterday_ql.to_date().strftime('%m%d%Y') + '.pdf'
    
    try:
        with pdfplumber.open(file) as pb:
            text = pb.pages[0].extract_text()
    
    except:
        print('Remate file not found.')
        wb.sheets('Risk').range('V6:V20').clear_contents()
        return None
        
    renglones = text.split('\n')
    
    row_tiie=renglones.index([i for i in renglones if '28d TIIE' in i][0])
    tiie_28_yst = float(renglones[row_tiie][
        len('28d TIIE '): renglones[row_tiie].find('%')])
    
    # Start row
    irs_r = renglones.index([i for i in renglones 
                             if 'SOFR Basis Swaps' in i][0])
    
    # End row
    options_r = renglones.index([i for i in renglones if 'Vols' in i][0])    
    
    cols=['Tenor', 'Bid', 'Offer', 'Chg']
    irs_df = pd.DataFrame(columns=cols)

    for r in range(irs_r+2, options_r):
        datos = renglones[r].split(' ')
        tenor = datos[0]
        bid = datos[1]
        offer = datos[3]
        chg = datos[4]
        irs_df_a = pd.DataFrame({'Tenor': [tenor], 'Bid': [bid],
                                 'Offer': [offer],'Chg': [chg]})
        irs_df = pd.concat([irs_df, irs_df_a], ignore_index=True)

    irs_df['Mid'] = (irs_df['Bid'].astype(float) 
                     + irs_df['Offer'].astype(float))/2
    
    risk_sheet = wb.sheets('Risk')
    risk_sheet.range('V6').value = tiie_28_yst
    risk_sheet.range('V7').value = irs_df[['Mid']].values
    print('Remate OK.')
    
    return irs_df
    #%% Sin DOCUMENTAR :(
def setPfolio_tiieSwps(str_posswps_file):
    # posswaps file import
    ## str_posswps_file = r'E:\posSwaps\PosSwaps20230213.xlsx'
    df_posSwps = pd.read_excel(str_posswps_file, 'Hoja1')
    
    # posswaps filter columns
    lst_selcols = ['swp_usuario', 'swp_ctrol', 'swp_fecop', 'swp_monto', 
                   'swp_fec_ini', 'swp_fec_vto', 'swp_fec_cor', 'swp_val_i_pa',
                   'swp_val_i', 'swp_serie', 'swp_emisora', 'swp_pzoref', 
                   'swp_nombre', 'swp_nomcte']
    lst_selcols_new = ['BookID','TradeID','TradeDate','Notional',
                       'StartDate','Maturity','CpDate','RateRec',
                       'RatePay','PayIndex','RecIndex','CouponReset', 
                       'Counterparty', 'Intern']
    df_posSwps = df_posSwps[lst_selcols]
    df_posSwps.columns = lst_selcols_new
    df_posSwps['Counterparty'] = df_posSwps['Counterparty'].str.strip()
    df_posSwps['Intern'] = df_posSwps['Intern'].str.strip()
    df_posSwps[['RecIndex','PayIndex']] = df_posSwps[['RecIndex',
                                        'PayIndex']].replace(' ','',regex=True)
    df_tiieSwps = df_posSwps[df_posSwps['RecIndex'].isin(['TIIE28','TF.MN.'])]
    df_tiieSwps = df_posSwps[df_posSwps['PayIndex'].isin(['TIIE28','TF.MN.'])]
    df_tiieSwps = df_tiieSwps[df_tiieSwps['CouponReset'] == 28]
    df_tiieSwps['FxdRate'] = df_tiieSwps['RateRec'] + df_tiieSwps['RatePay']
    df_tiieSwps['SwpType'] = -1
    df_tiieSwps['SwpType'][df_tiieSwps['RecIndex']=='TF.MN.'] = 1
    
    df_tiieSwps = df_tiieSwps.drop(['RecIndex','PayIndex',
                                    'CouponReset','RatePay','RateRec'], 
                     axis=1)
    df_tiieSwps[['TradeDate','StartDate',
                 'Maturity','CpDate']] = df_tiieSwps[['TradeDate','StartDate',
                 'Maturity','CpDate']].apply(lambda t: 
                                             pd.to_datetime(t,format='%Y%m%d'))
    df_tiieSwps = df_tiieSwps.reset_index(drop=True)
    # SPOTTING NON-IRREGULAR-TURNED-IRREGULAR CPNS
    # schdl generation rule
    endOnHoliday = {}
    for i,row in df_tiieSwps.iterrows():
        bookid, tradeid, tradedate, notnl, stdt, mty, cpdt, ctpty, itr,\
            r, swptyp = row
        mod28 = (mty - stdt).days%28
        omty = mty - timedelta(days=mod28)
        omtyql = ql.Date(omty.day,omty.month,omty.year)
        swpEndInHolyDay = ql.Mexico().isHoliday(omtyql)*1
        endOnHoliday[tradeid] = swpEndInHolyDay
    endOnHoliday = pd.Series(endOnHoliday)
    df_tiieSwps['mtyOnHoliday'] = 0
    df_tiieSwps['mtyOnHoliday'][
        np.where(df_tiieSwps['TradeID'].isin(endOnHoliday.index))[0]
        ] = endOnHoliday
    
    df_tiieSwps['Counterparty'] = df_tiieSwps['Counterparty'].str.strip()
    return(df_tiieSwps)

def risk_byMeet(wb: xw.Book) -> None:
    
    bookID = wb.sheets('Risk').range('B2').value

    banxico_sheet = wb.sheets('Short_End_Pricing')
    range_banxico_row = str(banxico_sheet.range('B4').end('down').row)
    banxico_df = banxico_sheet.range('B3', 'C' + range_banxico_row).options(
        pd.DataFrame, header=1, index=False).value
    
    pricing = wb.sheets('Pricing')
    mxn_fx = pricing.range('F1').value
    
    banxic_df1 = banxico_df.copy().iloc[:9]
    banxic_df1['fltRisk']=0 
    banxic_df1.set_index('Fix Eff', inplace = True)
    banxic_df1.rename(columns={None:'MeetingDate'}, inplace = True)
    
    blotter = wb.sheets('Blotter')
    
    if blotter.range('H2').value == None:
        parameters_trades = pd.DataFrame()
    else:
        range_trades = blotter.range('A1').end('right').address[:-1] + \
            str(blotter.range('H1').end('down').row)
        parameters_trades = blotter.range('A1',range_trades).options(
            pd.DataFrame, header=1).value
    
    
    ql_today = ql.Settings.instance().evaluationDate
    
    dt_yst = ql.Mexico().advance(ql_today, ql.Period(-1, ql.Days)).to_date()
    dfpfl = setPfolio_tiieSwps('//TLALOC/tiie/posSwaps/PosSwaps' +
                               dt_yst.strftime('%Y%m%d') + '.xlsx')
    for i,v in banxic_df1.iterrows():
        tenor = (ql.Date().from_date(v.MeetingDate) - ql_today)//28 
        banxic_df1.at[i,'tenor'] = tenor
        last_d = ql.Date().from_date(i) - ql.Period(tenor*28, ql.Days)
        
        if ql.Mexico().isHoliday(last_d):
            last_d = ql.Mexico().advance(last_d, ql.Period(-1, ql.Days))
        
        banxic_df1.at[i, 'LastDay'] = last_d.to_date()
    
    dfpflbook = dfpfl[dfpfl['BookID'] == bookID]
    #dfpflbook = pd.DataFrame()
    for i, v in dfpflbook.iterrows():
        qlst = ql.Date().from_date(v.StartDate)
        qlmty = ql.Date().from_date(v.Maturity)
        schdule = ql.Schedule(qlst, qlmty, ql.Period(13), ql.Mexico(),
                              ql.Following, ql.Following, v.mtyOnHoliday, 
                              False)
        
        fixdates = [ql.Mexico().advance(d, ql.Period(-1, ql.Days)).to_date() 
                    for i,d in enumerate(schdule)]
        common = [pd.to_datetime(d)  for d in fixdates 
                  if pd.to_datetime(d) in banxic_df1.index]
        if common:
            banxic_df1.loc[common, 'fltRisk'] =\
                banxic_df1.loc[common, 'fltRisk']+\
                    v.Notional*(-1) * v.SwpType * 28/360 * .0001/mxn_fx
                
    for i, v in parameters_trades.iterrows():
        start, maturity, flag_mat = \
            fn.start_end_dates_trading(v.fillna(0), ql_today.to_date())
            
        
        schdule = ql.Schedule(start, maturity, ql.Period(13), ql.Mexico(),
                              ql.Following, ql.Following, 0, False)
        
        fixdates = [ql.Mexico().advance(d, ql.Period(-1, ql.Days)).to_date() 
                    for i,d in enumerate(schdule)]
        common = [pd.to_datetime(d)  for d in fixdates 
                  if pd.to_datetime(d) in banxic_df1.index]
        if common:
            banxic_df1.loc[common, 'fltRisk'] =\
                banxic_df1.loc[common, 'fltRisk'] +\
                    v.Notional_MXN * 28/360 * .0001/mxn_fx    
                
    banxic_df1['Notional'] = banxic_df1.fltRisk/.0001*360/28*mxn_fx    
    colum = ['fltRisk', 'Notional', 'tenor', 'LastDay']
    wb.sheets('Fix_Dates_Analysis').range('E3:H11').value =\
        banxic_df1[colum].values
        
def banxico_risk(dic_data, wb, g_engines, dv01_engines, future_date):
    
    parameters = wb.sheets('Pricing')
    evaluation_date = pd.to_datetime(parameters.range('B1').value)
    yest_day = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(-1, ql.Days)).to_date()
    str_dt_posswaps = yest_day.strftime('%Y%m%d')
    str_posswps_file = r'//TLALOC/tiie/posSwaps/PosSwaps' + str_dt_posswaps + \
        '.xlsx'
    
    risk_sheet = wb.sheets('Risk')
    book = risk_sheet.range('B2:B2').value
    df_tiieSwps_tot = setPfolio_tiieSwps(str_posswps_file)
    df_book = df_tiieSwps_tot[df_tiieSwps_tot['BookID']==book]
    #df_book = pd.DataFrame(columns = df_book.columns)
    banxico_TIIE28 = banxicoData(future_date)
    
    blotter_book = fn.blotter_to_posswaps(wb, df_book, banxico_TIIE28, 
                                          g_engines, dv01_engines)
    
    ql.Settings.instance().evaluationDate = \
        ql.Date().from_date(future_date)
    

    updateAll = parameters.range('B2').value
    flag = updateAll
    
    g_crvs = createCurves(dic_data, updateAll, flag, save=False)
    brCrvs = fn.crvTenorRisk_TIIE_1L(dic_data, g_crvs[0], g_crvs[1], g_crvs[2])
    df_book = pd.concat([df_book, blotter_book], ignore_index=True)
    
    ibor_tiie = fn.set_ibor_TIIE(g_crvs[3])
    mxn_fx = wb.sheets('Pricing').range('F1').value
    dic_val = fn.get_risk_byBucket(df_book, brCrvs, g_crvs[2], ibor_tiie, 
                                   mxn_fx)
    krr_1L = float(dic_val['DV01_Book'].loc['%1L'].replace(',', ''))
    
    return krr_1L

def banxico_risk_option(dic_data, wb, g_engines, dv01_engines):
    
    banxico_sheet = wb.sheets('Short_End_Pricing')
    range_banxico_row = str(banxico_sheet.range('B4').end('down').row)
    banxico_dates = banxico_sheet.range('B3', 'C' + range_banxico_row).options(
        pd.DataFrame, header=1, index=False).value
    banxico_dates.rename(columns = {banxico_dates.columns[0]: 'Meeting_Dates'}, 
                      inplace = True)

    future_dates = banxico_dates['Fix Eff']

    krrs_dict = {}
    krrs_df = pd.DataFrame()
    
    for future_date in future_dates[:9]:
        krrs_df.loc[future_date, 'KRR %1L'] = banxico_risk(
            dic_data, wb, g_engines, dv01_engines, future_date)
        
    fix_sheet = wb.sheets('Fix_Dates_Analysis')
    fix_sheet.activate()
    fix_sheet.range('I3:I11').value = krrs_df[['KRR %1L']].values
    
    #return krrs_df

def closes_vector(wb, corros_file):
    
    meses_dic = {'en': '01', 'fb': '02', 'mz': '03', 'ab': '04', 'my': '05',
                 'jn': '06', 'jl': '07', 'ag': '08', 'sp': '09', 'ot': '10',
                 'nv': '11', 'dc': '12'}
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    

    
    parameters = wb.sheets('Pricing')
    evaluation_date = pd.to_datetime(parameters.range('B1').value)
    
    irs_df = remate_closes(wb, evaluation_date)
    
    yesterday_ql = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                       ql.Period(-1, ql.Days))
    str_date = yesterday_ql.to_date().strftime('%Y-%m-%d')
    
    
    flag_vector = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] "+
                                    f"WHERE  br_date = '{str_date}'",
                                    conn).empty
                                    

            
    corros_book = xw.Book(corros_file)
    best_sheet = corros_book.sheets('BEST')
    bonos_range = best_sheet.range('W4').end('down').row
    bonos = best_sheet.range('W4:W'+str(bonos_range)).value
    bonos_b = ['20'+b[2:]+meses_dic[b[:2]] for b in bonos]
    close_bonos = []
    
    bono_flag = False
    udi_flag = False
    
    if not flag_vector:

        for b in bonos_b:
            try:
                close_b = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] "+
                                            "INNER JOIN [dbo].[BondData] ON "+
                                            "br_isin = tic_isin WHERE "+
                                            f"br_date = '{str_date}' "+
                                            "AND LEFT(tic_maturityDate,6) = "+
                                            f"'{b}' AND tic_name = 'Mbono'",
                                            conn)['br_rate'].iloc[0]

                close_bonos.append(close_b)
            except:
                bono_flag = True
                close_bonos.append(0)
                print('Bono '+ b + ' not found. Please fill its close value manually.')
        
        best_sheet.range('AA4').value = np.array(close_bonos).reshape(-1, 1)
    
        udis_range = best_sheet.range('W100').end('down').row
        
        if 'UDIBONO' in best_sheet.range('W78'):
            udis_range = best_sheet.range('W80').end('down').row
            udis = best_sheet.range('W80:W'+str(udis_range)).value
            cell = 'AA80'
            
        else:
            udis = best_sheet.range('W100:W'+str(udis_range)).value
            cell = 'AA100'
        
        if udis_range > 1000000:
            udi_flag = True
        
        udis_b = ['20'+u[2:]+meses_dic[u[:2]] for u in udis]
        udibono_flag = False
        try:
            if not udi_flag:
                close_udis = []
            
                for u in udis_b:
                    try:
                        close_u =pd.read_sql_query("SELECT * FROM "+
                                                   "[dbo].[BondRates] "+
                                                    "INNER JOIN "+
                                                    "[dbo].[BondData] ON "+
                                                    "br_isin = tic_isin WHERE"+
                                                    f" br_date = '{str_date}'"+
                                                    " AND "+
                                                    "LEFT(tic_maturityDate,6)"+
                                                    f" = '{u}' AND "+
                                                    "tic_name = 'Sbono'",
                                                    conn)['br_rate'].iloc[0]
                        close_udis.append(close_u)
                    
                    except:
                        udibono_flag = True
                        close_udis.append(0)
                        print('Udibono ' + u + ' not found. Please fill its close value manually.')
                best_sheet.range(cell).value = np.array(close_udis).reshape(-1, 1)
        except:
            print('Udibonos failed. Please fill them manually.')
         
    tiie_range = best_sheet.range('V28').end('down').row
    tiies = best_sheet.range('V28:V'+str(tiie_range)).value
    
    irs_df['Tenor_num'] = irs_df['Tenor'].apply(lambda x: float(x[:-2]))
    tiie_df = pd.DataFrame(index = tiies)
    
    for i in tiie_df.index:
        try:
            tiie_df.loc[i, 'Close'] = irs_df[irs_df['Tenor_num']
                                             ==i]['Mid'].values[0]
        except:
            tiie_df.loc[i, 'Close'] = ''
        
    best_sheet.range('AA28').value = irs_df[['Mid']].values
    
    corros_flag = True
    if not flag_vector and not bono_flag and not udi_flag and not udibono_flag:
        print('Corros Closes OK.')
        corros_flag = False

    else:
        print('TIIE Corros Closes OK.')
        
    if udi_flag:
        print('No values for Udibonos found. Please fill Udibonos closes manually.')
    
    if not bono_flag and corros_flag and not flag_vector:
        print('Bono closes OK.')
        
        
    if flag_vector:
        print('No vector found. Please fill bonos and udis closes manually.')
        

def intraday_pnl(wb, dic_data_c, df_tiieSwps=pd.DataFrame(), book_flag = False, 
                 npv_yst=False, npv_tdyst=False, cf_sum=False):
    
    # Parameters definition
    parameters = wb.sheets('Pricing')
    today_date = pd.to_datetime(parameters.range('B1').value)
    risk_sheet = wb.sheets('Risk')
    book = risk_sheet.range('B2').value
    
    dic_data = {k:v.copy() for (k, v) in dic_data_c.items()}
    df_tiie = dic_data['MXN_TIIE']
    dic_data['MXN_TIIE'] = df_tiie[df_tiie['Period']!=156].reset_index(
        drop=True)
    
    str_inputsFileName = '//tlaloc/Cuantitativa/Fixed Income/TIIE '\
        'IRS Valuation Tool/Main Codes/Quant Management/Pricing/'\
            'TIIE_CurveCreate_Inputs'
    str_inputsFileExt = '.xlsx'
    str_file = str_inputsFileName + str_inputsFileExt

    last_date = \
        ql.Mexico().advance(ql.Date().from_date(today_date), 
                            ql.Period(-1, ql.Days)).to_date()
        
    # last working day as string
    last_date_str = last_date.strftime('%Y%m%d')
    
    # Swaps File
    str_posswps_file = r'//TLALOC/tiie/posSwaps/PosSwaps' + last_date_str +\
        '.xlsx' # PosSwaps file 
    
    if df_tiieSwps.shape[0] == 0:
        df_tiieSwps = pd.read_excel(str_posswps_file)
    
    # Yesterday and today valuation date
    dt_today = today_date.date()
    dt_val_yst, dt_val_tdy = last_date, today_date.date()
    
    # Check if today or yesterday are holidays
    historical_tdy = []
    historical_yst = []
    
    if ql.UnitedStates(1).isHoliday(ql.Date().from_date(dt_val_tdy)):
        historical_tdy = ['MXN_OIS']
        
    if (not npv_yst) or book_flag:
        
        df_tiieSwps = pd.read_excel(str_posswps_file)
        
        # Yesterday Valuation
        dic_data_yst = pf.cf.pull_data(str_file, dt_val_yst)
        
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(dt_val_yst)):
            historical_yst = ['MXN_OIS']
        
        
        try:
            ql.Settings.instance().evaluationDate = ql.Date().from_date(
                dt_val_yst)
        except:
            ql.Settings.instance().evaluationDate = ql.Date().from_date(
                dt_val_yst)
        
        curves_yst = pf.cf.mxn_curves(dic_data_yst, None, historical_yst)
        
        if historical_yst:
            tiie = pf.cf.MXN_TIIE(dic_data_yst['MXN_TIIE'], 'Cubic', 
                                curves_yst.crvMXNOIS, True)
            tiie.bootstrap()
            curves_yst.crvMXNTIIE = tiie
        
        if historical_tdy:
            curves_yst.crvUSDOIS = None
            curves_yst.crvUSDSOFR = None
            curves_yst.crvMXNOIS.swap_curve = None
            curves_yst.crvMXNOIS.discount_curve = None
        
        # Yesterday's portfolio creation
        pfolio_yst = pf.pfolio.from_posSwaps(df_tiieSwps, bookID = book)
        pfolio_yst.get_book_npv(dt_val_yst, curves_yst, inplace = True)
        npv_yst = pfolio_yst.dfbookval['NPV'].sum()
        
        # Coupon Payments Cash flows
        dt_cf = dt_val_tdy
        tdyCF = pfolio_yst.get_pfolio_CF_atDate(dt_cf)
        cf_sum = tdyCF.sum().values[0]
        
        # Yesterday's portfolio with today's evaluation date
        pfolio_tdyst = pf.pfolio.from_posSwaps(df_tiieSwps, bookID = book)
        
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(dt_val_tdy)):
            dic_data = curves_yst.dic_data
            del curves_yst
            try:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(dt_val_tdy)
            except:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(dt_val_tdy)
            
            
            curves_yst = pf.cf.mxn_curves(dic_data, None, ['MXN_OIS'])
            curves_yst.crvMXNTIIE.complete_ibor_tiie =\
                curves_yst.crvMXNTIIE.complete_ibor_index()
        
        else:
            try:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(dt_val_tdy)
            except:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(dt_val_tdy)
                    
            curves_yst.crvMXNTIIE.complete_ibor_tiie =\
                curves_yst.crvMXNTIIE.complete_ibor_index()
        
        pfolio_tdyst.get_book_npv(dt_val_tdy, curves_yst, inplace = True)
        npv_tdyst = pfolio_tdyst.dfbookval['NPV'].sum()
    
    # Today Valuation
    
    if not historical_tdy:
        try:
            ql.Settings.instance().evaluationDate = ql.Date().from_date(
                dt_val_tdy)
        except:
            ql.Settings.instance().evaluationDate = ql.Date().from_date(
                dt_val_tdy)
        
        #curves_yst = pf.cf.mxn_curves(dic_data_yst, None, historical_tdy)
    
    curves_tdy = pf.cf.mxn_curves(dic_data, None, historical_tdy)
    pfolio_tdy = pf.pfolio.from_posSwaps(df_tiieSwps, bookID = book)
    pfolio_tdy.get_book_npv(dt_val_tdy, curves_tdy, inplace = True)
    npv_tdy = pfolio_tdy.dfbookval['NPV'].sum()
    
    ibor_tiie = curves_tdy.crvMXNTIIE.ibor_index
    carry_df = pd.DataFrame()
    for i, r in pfolio_tdy.dfbookval.iterrows():
        fixed_rate = r.FxdRate/100
        if r.CpDate == dt_val_tdy:
            prev_cp_date = ql.Date().from_date(r.CpDate)
        else:
            prev_cp_date = ql.Date().from_date(r.CpDate) - ql.Period(
                28, ql.Days)
        
        if ql.Mexico().isHoliday(prev_cp_date):
            prev_cp_date = ql.Mexico().advance(prev_cp_date, ql.Period(
                1, ql.Days))
        
        fix_date = ibor_tiie.fixingDate(prev_cp_date)
        if fix_date <= ql.Date().from_date(dt_val_tdy):
            float_rate = ibor_tiie.fixing(fix_date)
            carry = (fixed_rate - float_rate)*r.Notional/360
        else:
            float_rate = 0
            carry = 0
        
        carry_df_a = pd.DataFrame({'Carry': [carry], 
                                   'FxdRate': [fixed_rate*100],
                                   'FltRate': [float_rate*100], 
                                   'Notional': [r.Notional],
                                   'SwpType': [r.SwpType]}, index=[r.TradeID])
        carry_df = pd.concat([carry_df, carry_df_a])
    
    carry = carry_df['Carry'].sum()
    
    blotter = wb.sheets('Blotter')
    
    if blotter.range('H2').value is None:
        parameters_trades = pd.DataFrame()
        fees = 0
    
    else:
        range_trades = blotter.range('A1').end('right').address[:-1] + \
            str(blotter.range('H1').end('down').row)
        parameters_trades = blotter.range('A1',range_trades).options(
            pd.DataFrame, header=1).value
        parameters_trades = parameters_trades.fillna(0)
        parameters_trades = parameters_trades[parameters_trades['Book']==book]
        parameters_trades = parameters_trades[
            parameters_trades['Rate'] != 'fee']
        fees = parameters_trades['Upfront_Fee_MXN'].sum()
        parameters_trades['Rate'] = parameters_trades['Rate'].astype(float)
    
    bltnpv = 0
    for i, values in parameters_trades.iterrows():
        
        start, maturity, flag_mat = \
            fn.start_end_dates_trading(values, today_date)
            
        notional = values.Notional_MXN
        rate = values.Rate
        npv = values.NPV_MXN
        dv01 = values.DV01_USD
        if values.Date_Generation == 'Forward':
            rule = ql.DateGeneration.Forward
            
        else:
            rule = ql.DateGeneration.Backward
        
        start = pd.to_datetime(start.to_date())
        end = pd.to_datetime(maturity.to_date())
        
        swp = pf.cf.tiieSwap(start, end, notional, rate, curves_tdy, rule, 
                             npv, dv01)
        
        npv = swp.NPV()
        bltnpv = bltnpv + npv
    
    #bltnpv = parameters_trades['MXN_NPV'].sum()
    #fees = parameters_trades['Upfront_Fee_MXN'].sum()
    
    date_blotter = today_date.strftime('%y%m%d')
    
    try:
        blotter_day = pd.read_excel(
            f'//TLALOC/tiie/Blotters/{date_blotter}.xlsx', 'BlotterTIIE_auto',
            skiprows=2)
        international_fees = blotter_day[(blotter_day['Book']==book) & 
                                         (blotter_day['Ctpty'] == 'u8082')]\
            ['Cuota compensatoria / unwind'].sum()
        local_fees = blotter_day[(blotter_day['Book']==book) & 
                                 (blotter_day['Ctpty'] == 'u8087')]\
            ['Cuota compensatoria / unwind'].sum()
        
    except:
        international_fees = 0
        local_fees = 0
    
    
    total_fees = fees + international_fees + local_fees
    delta_npv = npv_tdy + bltnpv - npv_yst
    cf1 = cf_sum
    carry_roll = npv_tdyst - npv_yst + cf1
    roll = carry_roll - carry
    mktmvmnt = npv_tdy - npv_tdyst
    
    pnl = delta_npv + cf1 + fees

    # Display
    # print(f'\n{book}\nFrom {str(dt_val_yst)} to {str(dt_val_tdy)}'+\
    #       f'\nPnL: {pnl:,.0f}\n\tNPV Chg: {delta_npv:,.0f}'+\
    #       f'\n\tCF: {cf1:,.0f}'+\
    #           f'\n\tFees: {fees:,.0f}')
    #print('\n------------------------------------------------------\n')
    print('\n')
    print(f'PnL for Book {book:.0f}'.center(52, '-'))
    # print(f'\nPnL for Book {book:.0f}:'+ f'\n\nTotal: {pnl:,.0f}' +
    #       f'\n\tCarry: {carry:,.0f}' +
    #       f'\n\tPosition PnL: {mktmvmnt:,.0f}' +
    #       f'\n\tTrading PnL: {bltnpv+fees:,.0f}' +
    #       f'\n\t\tToday\'s trades: {bltnpv:,.0f}' +
    #       f'\n\t\tFees: {fees:,.0f}')
    
    print(f'\n\nTotal: {pnl:,.0f}' +
          f'\n\tCarry Roll: {carry_roll:,.0f}' +
          f'\n\t\tCarry: {carry:,.0f}' +
          f'\n\t\tRoll: {roll:,.0f}' +
          f'\n\tPosition PnL: {mktmvmnt:,.0f}' +
          f'\n\tTrading PnL: {bltnpv+total_fees:,.0f}' +
          f'\n\t\tToday\'s trades: {bltnpv:,.0f}' +
          f'\n\t\tTotal Fees: {total_fees:,.0f}' + 
          f'\n\t\t\tFees: {fees:,.0f}' +
          f'\n\t\t\tInternational: {international_fees:,.0f}' + 
          f'\n\t\t\tLocal: {local_fees:,.0f}')
    
    return df_tiieSwps, npv_yst, npv_tdyst, cf_sum, book

    
def risky_tenors_fn1(krr):
    krr = pd.DataFrame(krr)
    total_risk = krr.sum(axis=0).sum()
    #print(total_risk)
    krr.drop('%1L', inplace=True)
    krr = krr.rename(columns={krr.columns[0]: 'Risk'})
    
    risky_tenors = []
    krr['Abs_Risk'] = np.abs(krr['Risk'])
    krr.sort_values(by='Abs_Risk', ascending=False, inplace=True)
    # krr['Distance'] = np.abs(krr['Risk'] - total_risk)
    # krr.sort_values(by='Distance', inplace=True)
    risky_tenor = krr.iloc[0].name
    risky_tenors.append(risky_tenor)
    acc_risk = krr.iloc[0].Risk
    krr.drop(risky_tenor, inplace=True)
    risk = total_risk
    
    while ((1.1 < np.abs(acc_risk/total_risk)) or 
           (np.abs(acc_risk/total_risk) < .9)):
        if krr.shape[0] == 0:
            break
        risk = total_risk - acc_risk
        krr['Distance'] = np.abs(krr['Risk'] - risk)
        krr.sort_values(by='Distance', inplace=True)
        acc_risk = acc_risk + krr.iloc[0].Risk
        # print(krr.Distance)
        # print(acc_risk)
        # print(np.abs(acc_risk/total_risk))
        risky_tenor = krr.iloc[0].name
        risky_tenors.append(risky_tenor)
        # print(risky_tenor)
        krr.drop(risky_tenor, inplace=True)
        
            
    return risky_tenors

def risky_tenors_fn(krr):
    krrcopy = krr.copy()
    krr = pd.DataFrame(krr)
    
    total_risk = krr.sum(axis=0).sum()
    krr.drop('%1L', inplace=True)
    krr = krr.rename(columns={krr.columns[0]: 'Risk'})
    risky_tenors = []
    krr['Abs_Risk'] = np.abs(krr['Risk'])
    krr.sort_values(by='Abs_Risk', ascending=False, inplace=True)
    risky_tenor = krr.iloc[0].name
    # risky_tenors.append(risky_tenor)
    acc_risk = krr.iloc[0].Risk
    max_risk = krr.iloc[0].Risk
    
    num_tenors = krr[krr['Abs_Risk']/abs(max_risk) >= .3].shape[0]
    # print(num_tenors)
    
    if num_tenors > 3:
        # print('normal')
        risky_tenors = risky_tenors_fn1(krrcopy)
        if len(risky_tenors)>10:
            krrcopy = krr.copy()
            risky_tenors = []
            n = 1
            for i,v in krrcopy[krrcopy['Abs_Risk']/abs(max_risk) >= .3 ].iterrows():
                risky_tenors.append(i)
                n += 1
        
    else:
        # print('nuevo')
        n = 1
        for i,v in krr[krr['Abs_Risk']/abs(max_risk) >= .3 ].iterrows():
            risky_tenors.append(i)
            n += 1
        
    return risky_tenors
        
        

def check_parameters(parameters_trades):
    flag = False
    for i, values in parameters_trades.iterrows():
        if values.Rate == 0:
            print('Please fill Rate column for trade ', 
                  int(i))
            flag = True        
        if values.NPV_MXN != 0:
            if values.Notional_MXN == 0:
                print('Please fill Notional_MXN column for trade ' + 
                      str(int(i)) + '.\n')
                flag = True
    
    return flag


        

def simulation(wb, dic_data):
    
    sim_sheet = wb.sheets('Fwd_Start_Sim')
    sim_sheet.activate()
    final_row = sim_sheet.range('A2').end('down').row
    final_col = sim_sheet.range('H2').end('right').address.split('$')[1]

    final_row_clear = sim_sheet.range('H2').end('down').row

    parameters_trades = sim_sheet.range(
        'A2:I'+str(final_row_clear)).options(pd.DataFrame, header=1, 
                                       index=False).value
    parameters_trades = parameters_trades.fillna(0)
    
    evaluation_date = wb.sheets('Pricing').range('B1').value
    mxn_fx = wb.sheets('Pricing').range('F1').value

    initial_quotes = dic_data['MXN_TIIE']['Quotes'].copy().values
    quotes_df = dic_data['MXN_TIIE'].copy()
    quotes_df.set_index('Tenor', inplace=True)
    
    bounds_df = quotes_df['Quotes'].apply(lambda x: (x, x))

    results_df = pd.DataFrame(index=quotes_df.index)
    results_df['Initial'] = initial_quotes.tolist()

    # krr_df = pd.DataFrame(index=quotes_df.index)

    flag = check_parameters(parameters_trades)
    
    rem_cme_f = False
    group = False
    
    if parameters_trades['Valuation_Check'].isin(['x', 'X']).any():
        parameters_trades = parameters_trades[
            parameters_trades['Valuation_Check'].isin(['x','X'])]
        
        parameters_trades.set_index('Trade_#', inplace=True)
        
        
        params_trades = [g for (i,g) in
                         parameters_trades.groupby(parameters_trades.index)]
    
    elif parameters_trades['Valuation_Check'].isin(['CME', 'REM',
                                                    'cme', 'rem']).any():
        parameters_trades_cme = parameters_trades[
            parameters_trades['Valuation_Check'].isin(['CME','cme'])]
        
        parameters_trades_cme.set_index('Trade_#', inplace=True)
        
        parameters_trades_rem = parameters_trades[
            parameters_trades['Valuation_Check'].isin(['REM','rem'])]
        
        parameters_trades_rem.set_index('Trade_#', inplace=True)
        params_trades = [parameters_trades_cme, parameters_trades_rem]
        rem_cme_f = True
    
    else:
        # nums = parameters_trades['Valuation_Check'].unique()
        parameters_trades_1 = parameters_trades[
            parameters_trades['Valuation_Check']!=0]
        parameters_trades_1.set_index('Trade_#', inplace=True)
        params_trades = [g for (i,g) in
                         parameters_trades_1.groupby('Valuation_Check')]
        group = True
    
    def fair_rate_l2(tiie_array, params_pf, curves):
        
        dftiie = curves.dic_data['MXN_TIIE'].copy()
        
        dftiie['Quotes'] = tiie_array
        curves.change_tiie(dftiie)
        
        swap = pf.cf.tiieSwap(params_pf.dfbook.StartDate.iloc[0], 
                              params_pf.dfbook.Maturity.iloc[0], 
                              params_pf.dfbook.Notional.iloc[0],
                              params_pf.dfbook.FxdRate.iloc[0], 
                              curves)
        swap_rate = swap.fairRate()*100
        
        return 1e4*(target_rate*100 - swap_rate)**2
    
    def npv_l2(tiie_array, params_df, curves):
        
        dftiie = curves.dic_data['MXN_TIIE'].copy()
        # print(dftiie['Quotes'])
        #dftiie = dic_data['MXN_TIIE'].copy().drop(index=11).reset_index(drop=True)
        dftiie['Quotes'] = tiie_array
        curves.change_tiie(dftiie)
        npv = params_df.get_book_npv(evaluation_date, curves)
        swap_npv = npv.NPV.sum()
        npv_ratio = swap_npv/target_npv
        
        return 1e2*(npv_ratio - 1)**2
    if ql.UnitedStates(1).isHoliday(ql.Settings.instance().evaluationDate):
        historical = ['MXN_OIS']
    else: 
        historical = []
    
    optimals = []
    
    if not flag:
        at = datetime.now()
        n = 0
        for params in params_trades:
            if params.empty:
                n+=1
                continue
            params_pf = pf.pfolio.from_pricing(params, evaluation_date, None, 1814)
            a = datetime.now()
            if rem_cme_f and n == 0:
                dic_data_cme = {k: v.copy() for (k, v) in dic_data.items()}
                risk = wb.sheets('Risk')
                quotes = risk.range('U6:U20').value
                dic_data_cme['MXN_TIIE']['Quotes'] = quotes
                quotes_df = dic_data_cme['MXN_TIIE'].copy()
                quotes_df.set_index('Tenor', inplace=True)
                bounds_df = quotes_df['Quotes'].apply(lambda x: (x, x))
                curves = pf.cf.mxn_curves(dic_data_cme, None, historical)
                curves.KRR_crvs(False, True)
            
            elif rem_cme_f and n == 1:
                dic_data_rem = {k: v.copy() for (k, v) in dic_data.items()}
                risk = wb.sheets('Risk')
                quotes = risk.range('V6:V20').value
                dic_data_rem['MXN_TIIE']['Quotes'] = quotes
                quotes_df = dic_data_rem['MXN_TIIE'].copy()
                quotes_df.set_index('Tenor', inplace=True)
                bounds_df = quotes_df['Quotes'].apply(lambda x: (x, x))
                curves = pf.cf.mxn_curves(dic_data_rem, None, historical)
                curves.KRR_crvs(False, True)
            
            else:
                curves = pf.cf.mxn_curves(dic_data, None, historical)
                curves.KRR_crvs(False, True)
            n += 1
            notional_flag = False
            # if not group:
                
            if params.iloc[0].Notional_MXN == 0:
                notional = 1_000_000_000
                params_pf.dfbook['Notional'] = notional
                notional_flag = True
            else:
                notional_flag = False
                
            target_rate = params.Rate.sum()
            target_npv = params.NPV_MXN.sum()
                    
            dics = params_pf.get_risk_byBucket(evaluation_date,
                                               curves, mxn_fx)
            
            original_fr = params_pf.dfbookval.SwpObj.iloc[0].fairRate()
            original_npv = dics['NPV_Book']
            
            krr = dics['DV01_Swaps'].sum(axis=0)
            
            krr_print = dics['DV01_Book'].iloc[1:].rename({0:'Risk'})
            
            outright_risk =\
                float(dics['DV01_Book']['OutrightRisk'].replace(',', ''))
                
             
                
            risky_tenors = risky_tenors_fn(krr)
            
            bounds = bounds_df.copy()
            for t in risky_tenors:
                bounds.at[t] = (bounds_df.loc[t][0] - 
                                .05, bounds_df.loc[t][1] + .05)
            
            bounds = bounds.tolist()
            
            if (params.Valuation_Check == 'x').any():
                print(f'Output Trade {params.index[0]}'.center(52, '-'))
            else:
                print(f'Output Group {params.Valuation_Check.iloc[0]}'.center(52, '-'))
            print('\nTenors to change: ', ', '.join(risky_tenors))
                
            if target_npv != 0:
                print('Original NPV: ', "MXN$ {:,.0f}".format(original_npv))
                optimal_rates = minimize(npv_l2, initial_quotes,
                                         args=(params_pf, curves),
                                         method='L-BFGS-B', 
                                         bounds=bounds,
                                         options = {'maxiter': 275},
                                         tol = 1e-5)               
                
            else:
                print('Original Fair Rate: ', "% {:,.6f}".format(
                    original_fr * 100))   
                optimal_rates = minimize(fair_rate_l2, initial_quotes, 
                                         args=(params_pf, curves),
                                         method='L-BFGS-B', 
                                         bounds=bounds,
                                         options = {'maxiter': 275},
                                         tol = 1e-5)               
            
            b = datetime.now()
                
                
            # else:
                
                
            #     target_npv = params.NPV_MXN.sum()
                
            #     dics = params_pf.get_risk_byBucket(evaluation_date, curves,
            #                                        mxn_fx)
    
            #     krr = dics['DV01_Swaps'].sum(axis=0)
                
            #     krr_print = dics['DV01_Book'].iloc[1:].rename({0:'Risk'})
                
            #     outright_risk =\
            #         float(dics['DV01_Book']['OutrightRisk'].replace(',', ''))

            #     original_npv = dics['NPV_Book']
                
    
            #     risky_tenors = risky_tenors_fn(krr)
                
            #     print('Output Trade Group'
            #           f' {params.Valuation_Check.iloc[0]}'.center(52, '-'))
            #     print('\nTenors to change: ', ', '.join(risky_tenors))
            #     print('Original NPV: ', "MXN$ {:,.0f}".format(original_npv))
    
            #     bounds = bounds_df.copy()
            #     for t in risky_tenors:
            #         bounds.at[t] = (bounds_df.loc[t][0] - 
            #                         .05, bounds_df.loc[t][1] + .05)
                
            #     bounds = bounds.tolist()
                
            #     optimal_rates = minimize(npv_l2, initial_quotes,
            #                              args=(params_pf, curves),
            #                              method='L-BFGS-B', 
            #                              bounds=bounds,
            #                              options = {'maxiter': 275},
            #                              tol = 1e-5)                    
                    
            #     b = datetime.now()
    
            optimal_tiies = optimal_rates.x
            
            optimals.append(optimal_tiies)
            optimal_dftiie = dic_data['MXN_TIIE'].copy()
            #optimal_dftiie = dic_data['MXN_TIIE'].copy().drop(index=11).reset_index(drop=True)
            optimal_dftiie['Quotes'] = optimal_tiies
            curves.change_tiie(optimal_dftiie)
            
            npvdf2 = params_pf.get_book_npv(evaluation_date, curves)
            
            new_fair_rates = [swp.fairRate() for swp in npvdf2.SwpObj]
            
            if target_npv != 0:
                
                print('New NPV: ', "MXN$ {:,.0f}".format(npvdf2.NPV.sum()))
            
            else:
                print('New Fair Rate: ', "% {:,.6f}".format(
                    new_fair_rates[0] * 100))
        
            if not notional_flag:
                print('\n', krr_print, '\n')
                print('Outright Risk: ', '{:,.0f}'.format(outright_risk))
            
            
            fr = 0
            for i,v in params_pf.dfbook.iterrows():
                
                # print(i)
                
                sim_sheet.range('J' + str(int(i+2)) + ':' +  final_col + 
                                str(int(i+2))).clear_contents()
                sim_sheet.range('J' + str(int(i+2)) + ':' + final_col + 
                                str(int(i+2))).color = (255, 255, 255)
                
                for t in risky_tenors:
                    for c in range(9, 
                                   sim_sheet.range('G2').end('right').column + 1):
                        if sim_sheet.range(2, c + 1).value == t:
                            sim_sheet.range(int(i + 2), c + 1).color = (196, 215, 
                                                                        155)
                
                sim_sheet.range('J'+str(int(i+2))).value = new_fair_rates[fr]
                sim_sheet.range('K'+str(int(i+2))).value = optimal_tiies
                
                
                
                fr += 1
            print('Calculation time: ', b-a, '\n')
            del params_pf
            del curves
                #curves = pf.cf.mxn_curves(dic_data)
        
        
        
        bt = datetime.now()
        print('\nTotal calculation time: ', bt-at)
        yn = input('Do you want to save the optimal rates (y/n)?: ').lower()
        if yn == 'y':
            notional_sht = wb.sheets('Notional_DV01')
            if group:
                num_trades = [int(par['Valuation_Check'].iloc[0])
                              for par in params_trades]
            
            else:
                num_trades = [int(par.index[0]) for par in params_trades]
            
            
            
            if len(optimals) > 1 :
                print(num_trades)
                num_trad = int(input('Which optimal rates would you want to use?: '))
                
                num = num_trades.index(num_trad)
                notional_sht.activate()
                
                
                notional_sht.range('C4').value = optimals[num].reshape(-1,1)
                notional_sht.range('D4').value = optimals[num].reshape(-1,1)
            else:
                notional_sht.activate()
                notional_sht.range('C4').value = optimals[0].reshape(-1,1)
                notional_sht.range('D4').value = optimals[0].reshape(-1,1)
                

#%%
# ASW Functions
def comparable_rate(rate, m, p):
    rate_p = p*((1+rate/m)**(m/p)-1)
    return rate_p

def eq_fair_rate(maturity, curvas, notional, evaluation_date):
    
    start = ql.Mexico().advance(ql.Date().from_date(evaluation_date),
                                ql.Period(1, ql.Days)).to_date()
    
    if maturity.date() > start:
        
        start = ql.Mexico().advance(ql.Date().from_date(evaluation_date),
                                    ql.Period(1, ql.Days)).to_date()
        
        swp = cf.tiieSwap(start, maturity, notional, 0, curvas, 0)
        rate = swp.fairRate() * 100
        
            
    else:
        rate = 0
        print('Please check your maturity dates in ASW_Bonos or ASW_Cetes '+\
              'sheet. Outdated maturity date found.')
    
    return rate

def step_rate(tiie28: float, scenario: pd.DataFrame, 
              evaluation_date: datetime) -> pd.DataFrame:
    """Fill TIIE rates with given Banxico scenario.
    

    Parameters
    ----------
    tiie28 : float
        Current value for TIIE28.
    scenario : pd.DataFrame
        DataFrame with Banxico meeting dates and decisions.
    evaluation_date : datetime
        Evaluation date.

    Returns
    -------
    scenario : pd.DataFrame
        DataFrame filled with step rates for each meeting date.

    """
    
    scenario['CumBasis'] = np.cumsum(scenario)
    scenario['TIIE'] = scenario['CumBasis']/100 + tiie28
    scenario['EffDate'] = [ql.Mexico().advance(
        ql.Date().from_date(d), ql.Period(1, ql.Days)).to_date() 
        for d in scenario.index]
    scenario['EffDate'] = pd.to_datetime(scenario['EffDate'])
    scenario.loc[evaluation_date] = [0 , 0, tiie28, evaluation_date]
    scenario.sort_index(inplace=True)
    
    return scenario

# def fondeo_cost(tiie_fondeo, scenario, evaluation_date, last_date):
    
#     dt_settle = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
#                                     ql.Period(2, ql.Days)).to_date()
    
#     dt_plus_one = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
#                                     ql.Period(1, ql.Days)).to_date()
    
#     if dt_settle < scenario.index[0].date():
#         first_fix = scenario.index[0] + timedelta(1)
#         fondeo_df = pd.DataFrame({'Date': pd.date_range(
#             evaluation_date, scenario.index[0], freq='d'), 
#             'Rate': tiie_fondeo})
#     else:
#         fondeo_df = pd.DataFrame()
        
#     for i in range(scenario.shape[0]-1):
#         fondeo_df_a = pd.DataFrame({'Date': pd.date_range(
#             scenario.EffDate[i], scenario.index[i+1], freq='d'),
#             'Rate': scenario.iloc[i].TIIE})
#         fondeo_df = pd.concat([fondeo_df, fondeo_df_a])
        
#     fondeo_df_a = pd.DataFrame({'Date': pd.date_range(scenario.EffDate[-1],
#                                                       last_date, freq='d'),
#                                 'Rate': scenario.TIIE[-1]})
    
#     fondeo_df = pd.concat([fondeo_df, fondeo_df_a])
        
#     fondeo_0 = pd.DataFrame({'Date': [pd.to_datetime(evaluation_date)], 
#                               'Rate': [tiie_fondeo]})
    
#     fondeo_df = fondeo_df[fondeo_df['Date'] >= pd.to_datetime(dt_plus_one)]
#     fondeo_df = pd.concat([fondeo_0, fondeo_df])
#     fondeo_df['Factor'] = [1] + \
#         [1 + (fondeo_df.iloc[i].Date - fondeo_df.iloc[i-1].Date).days\
#          *fondeo_df.iloc[i].Rate/36000 for i in range(1, fondeo_df.shape[0])]
    
#     fondeo_df['CumProd'] = np.cumprod(fondeo_df['Factor'])
#     fondeo_df['Days'] = (fondeo_df['Date'] - pd.to_datetime(evaluation_date)).dt.days
#     fondeo_df['Funding'] = (fondeo_df['CumProd'] - 1)*36000/fondeo_df['Days']
#     fondeo_df.fillna(tiie_fondeo, inplace=True)
#     fondeo_df['Date'] = fondeo_df['Date'].dt.date
    
#     return fondeo_df

def cash_flows(plazo, rate, scenario, notional, evaluation_date):
    
    if plazo <= 0:
        return 0, 0
    else:
        
        n_coupons = plazo//28
        acc_coupon = plazo%28
        cf = notional*rate*28/36000*n_coupons + \
            notional*rate*acc_coupon/36000
            
        cal = ql.Mexico()
        start = cal.advance(ql.Date().from_date(evaluation_date), 
                            ql.Period(1, ql.Days))
        
        maturity = start + ql.Period(plazo, ql.Days)
        legDC = ql.Actual360()
        cpn_tenor = ql.Period(13)
        convention = ql.Following
        termDateConvention = ql.Following
        rule = 0
        isEndOfMonth = False
        fixfltSchdl = ql.Schedule(start, maturity, cpn_tenor, cal, convention,
                                termDateConvention, rule, isEndOfMonth)
        
        fix_schedule = [cal.advance(d, ql.Period(-1, ql.Days)).to_date() 
                        for d in fixfltSchdl]
        schedule = [d.to_date() for d in fixfltSchdl]
        
        meetings = [[s < m.date() for m in scenario.EffDate].index(True) - 1 
                    for s in fix_schedule[:-1] if s < scenario.EffDate[-1].date()]
        
        float_rates = scenario.iloc[meetings].TIIE.values
        last_rate = scenario.iloc[-1].TIIE
        float_rates = np.append(float_rates, 
                                [last_rate]*(len(
                                    fix_schedule[:-1])-len(float_rates)))
        
        float_cf = pd.DataFrame({'FixDate': fix_schedule[:-1], 
                                 'EffDate': schedule[:-1], 
                                 'EndDate': schedule[1:], 'TIIE': float_rates})
        float_cf['CF'] = (float_cf['EndDate'] - float_cf['EffDate']).apply(
            lambda x: x.days)*float_cf['TIIE']*notional/36000
            
    
    return -cf, float_cf['CF'].sum()

def bono_price_fn(maturity, ytm, coupon, evaluation_date, dirty=True):
    
    settle_date = ql.Mexico().advance(
        ql.Date().from_date(evaluation_date), ql.Period(1, ql.Days))
    maturity_ql = ql.Date().from_date(maturity)
    dtm = maturity_ql - settle_date
    n = np.ceil(dtm/182)
    accrued = -dtm % 182
    price = (182*coupon/36000 + coupon/ytm + (1-(coupon/ytm))\
             /(1+182*ytm/36000)**(n-1))*100/(1+182*ytm/36000)**(1-accrued/182)
        
    if not dirty:
        current_coupon = accrued * coupon /360
        price = price - current_coupon
    
    # final_return = (182-accrued)*coupon*100/36000 + (n-1)*182*coupon*100/36000 \
    #     + 100
    final_return = n*182*coupon*100/36000 \
        + 100
    
    return price, final_return

def pnl_df(df: pd.DataFrame, evaluation_date: datetime, curvas: cf.mxn_curves, 
           notional: float, step_tiie28: pd.DataFrame, bond_type: str, 
           fondeo_df: pd.DataFrame, cols: list, live=False) -> pd.DataFrame:
    """Creates DataFrame with detailed PnL of ASW.
    

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with instruments and their details (maturity, rate, coupon, 
                                                      etc.).
    evaluation_date : datetime
        Evaluation date.
    curvas : cf.mxn_curves
        Curves used to calculate TIIE Fair Rate for each maturity date.
    notional : float
        Notional.
    step_tiie28 : pd.DataFrame
        TIIE Rates given a Banxico scenario.
    bond_type : str
        CETES or Mbono.
    fondeo_df : pd.DataFrame
        DataFrame with daily funding rates.
    cols : list
        List of columns.
    live : bool, optional
        If you are calculating live PnL or close PnL. The default is False.

    Returns
    -------
    df : pd.DataFrame
        DataFrame with PnL details.

    """
    
    spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(1, ql.Days)).to_date()
    
    # Fill non-existent or outdated bonds with old date
    df['Maturity'] = df['Maturity'].fillna(datetime(1901, 1, 1))
    
    # In case date formats don't match
    try:
        df['Plazo'] = (df['Maturity'] - pd.to_datetime(spot_date)).dt.days
    except:
        try:
            df['Maturity'] = pd.to_datetime(df['Maturity'])
            df['Plazo'] = (df['Maturity'] - pd.to_datetime(spot_date)).dt.days
        except:
            print('Date formats do not match in pnl_df function.')
    
    # Fill cashflows with equivalent TIIE fair rate
    df['TIIE'] = df['Maturity'].apply(lambda m: eq_fair_rate(
        m, curvas, notional, evaluation_date))
    
    df['Fix'] = [cash_flows(int(df.iloc[i]['Plazo']), df.iloc[i]['TIIE'], 
                              step_tiie28, notional, evaluation_date)[0] 
                   for i in range(0, df.shape[0])]

    df['Float'] = [cash_flows(int(df.iloc[i]['Plazo']), 
                                           df.iloc[i]['TIIE'], 
                                           step_tiie28, notional, 
                                           evaluation_date)[1] 
                   for i in range(0, df.shape[0])]
    
    if bond_type=='Mbono':
        df['Price'] = [bono_price_fn(m, y, c, evaluation_date)[0] 
                       for (m, y, c) in zip(df['Maturity'], df['br_rate'], 
                                            df['tic_coupon'])]
        titles = notional/100
        df['Settlement'] = df['Price']*titles
        df['Return'] = [bono_price_fn(m, y, c, evaluation_date)[1]*titles 
                              for (m, y, c) in zip(df['Maturity'], 
                                                df['br_rate'], 
                                                df['tic_coupon'])]

        df['P&L'] = df['Return'] - df['Settlement']
        
    elif bond_type=='CETES':
        
        df['Price'] = 10/(1+df['Rendimiento']*df['Plazo']/36000)
        titles = notional/10
        df['Settlement'] = df['Price']*titles
        df['P&L'] = (10-df['Price'])*titles
    
    # In case date formats don't match
    try:
        df['Maturity'] = df['Maturity'].dt.date
    except:
        pass
    try:
        fondeo_df['Date'] = fondeo_df['Date'].dt.date
    except:
        pass
    
    try:
        df = df.merge(fondeo_df[['Date', 'Funding']], how='left',
                                  left_on='Maturity', right_on='Date')
    except:
        print('Fondeo dataframe has a different date format than maturities '+
              'in pnl_df.')
    
    df['FundingCost'] = -df['Funding']*df['Settlement']\
        *df['Plazo']/36000
        
    df['Final_P&L'] = df['P&L'] + df['FundingCost']
    df['Swap_P&L'] = df['Fix'] + df['Float']
    
    if not live:
        df.sort_values(by='Maturity', inplace=True)
        df = df[df['Plazo']<=1460]
        
    df = df[cols]
    
    return df

def cashflows(swp, stp_tiie):

    
    cf = sum([c.amount() for c in swp.swap.leg(0)])
    
    fix_schedule = [d.to_date() for d in swp.cpn_dates]
    meetings = [[s < m.date() for m in stp_tiie.EffDate].index(True) - 1 
                for s in fix_schedule if s < stp_tiie.EffDate[-1].date()]
    
    schedule = [ql.Mexico().advance(d, ql.Period(1, ql.Days)).to_date()
                for d in swp.cpn_dates] + [swp.maturity]
    
    
    float_rates = stp_tiie.iloc[meetings].TIIE.values
    float_cf = pd.DataFrame({'FixDate': fix_schedule,
                             'EffDate': schedule[:-1], 'EndDate': schedule[1:], 
                             'TIIE': float_rates})
    
    float_cf['CF'] = (float_cf['EndDate'] - float_cf['EffDate']).apply(
        lambda x: x.days)*float_cf['TIIE']*swp.notional/36000


    return -cf, float_cf['CF'].sum()

def standard_cete(cetes: pd.DataFrame, tdy: datetime, tiie: pd.DataFrame, 
                  live=False, live_cetes=pd.DataFrame()):
    
    plazos_standar = [7,28,56,91,133,182,210,280,364,448,560,644,728]
    
    min_plz = [0,14,35,70,110,150,190,250,300,400,500,600,700]  
    
    max_plz = min_plz[1:] + [800]
    
    
    cetes = cetes[['TV', 'Emisora', 'Serie', 'PrecioSucio', 'PrecioLimpio',
                   'DiasPorVencer', 'Rendimiento']]
    
    if live:
        for i, r in live_cetes.iterrows():
            index = cetes[cetes['Serie']==str(int(r.Serie))].index[0]
            cetes.at[index, 'Rendimiento'] = r.br_rate
    
    cetes = cetes[cetes['DiasPorVencer'] > 1]
    
    cetes['DiasPorVencer'] = [(datetime.strptime(d, '%y%m%d') - tdy).days-1
                              for d in cetes['Serie']]
    condi_stnd = [((cetes['DiasPorVencer'] >= mi) & (cetes['DiasPorVencer'] < ma))
                   for (mi, ma) in zip(min_plz,max_plz)]
    
    cetes['plz_stnd'] = np.select(condi_stnd, plazos_standar)
    
    
    
    inter = interp1d(cetes['DiasPorVencer'].values, 
                      cetes['Rendimiento'].values, kind='linear', axis = -1,
                      copy=True, bounds_error = None, fill_value='extrapolate')
    
    
    
    cetes['Rendimiento_inter'] = inter(np.array(cetes['plz_stnd']))
    cetes_stand = cetes.groupby('plz_stnd')[['plz_stnd','Rendimiento_inter']].mean()                        
    cetes_stand['TV'] = 'BI'
    cetes_stand['Emisora'] = 'CETES'
    cetes_stand['Maturity'] = cetes_stand['plz_stnd'].apply(
        lambda x: ql.Mexico().advance(ql.Date().from_date(tdy) + int(x)+1, 
                                      ql.Period(0, ql.Days)).to_date())
    
    cols = ['TV', 'Emisora', 'plz_stnd', 'Maturity', 'Rendimiento_inter',
            'TIIE_Zero']
    
    cetes_stand['TIIE_Zero'] = tiie[tiie['VALOR'].isin(
        (cetes_stand['plz_stnd']+1).tolist())]['PLAZO']
                                    
    
    
    return cetes_stand[cols]

def standard_cetes_live(cetes_vec, live_cetes, evaluation_date):
    
    spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(1, ql.Days)).to_date()
    
    plazos_standar = [7,28,56,91,133,182,210,280,364,448,560,644,728]
    min_plz = [0,14,35,70,110,150,190,250,300,400,500,600,700]  
    max_plz = min_plz[1:] + [800]
    
    cetes_vec = cetes_vec[['TV', 'Emisora', 'Serie', 'PrecioSucio', 'PrecioLimpio',
                   'DiasPorVencer', 'Rendimiento']]
    
    all_durations = list(set(cetes_vec['DiasPorVencer'].tolist() + plazos_standar))
    all_durations.sort()
    n = len(all_durations) - 1
    live_cetes['Node'] = [False]*live_cetes.shape[0]
    
    for i, r in live_cetes.iterrows():
        try:
            index = cetes_vec[cetes_vec['Serie']==str(int(r.Serie))].index[0]
            cetes_vec.at[index, 'Rendimiento'] = r.br_rate
        except:
            continue
        # dtm = cetes_vec[cetes_vec['Serie']==str(int(r.Serie))]['DiasPorVencer'].values[0]
        # duration_index = all_durations.index(dtm)
        
        # if all_durations[duration_index] in plazos_standar:
        #     live_cetes.at[i, 'Node'] =  True
        
        # elif duration_index < n:
        #     if all_durations[duration_index + 1] in plazos_standar:
        #         live_cetes.at[i, 'Node'] =  True
        #     elif all_durations[duration_index - 1] in plazos_standar:
        #         live_cetes.at[i, 'Node'] =  True
            
        # elif duration_index > 0:
        #     if all_durations[duration_index - 1] in plazos_standar:
        #         live_cetes.at[i, 'Node'] =  True
        #     if all_durations[duration_index + 1] in plazos_standar:
        #         live_cetes.at[i, 'Node'] =  True
            
            
        
    
    live_cetes['Maturity'] = pd.to_datetime(live_cetes['Serie'], 
                                            format='%y%m%d')
    live_cetes['DiasPorVencer'] = live_cetes['Maturity'].apply(
        lambda m: (m - pd.to_datetime(spot_date)).days)    
    
    cetes_vec = cetes_vec[cetes_vec['DiasPorVencer'] > 1]
    
    cetes_vec['DiasPorVencer'] = [(datetime.strptime(d, '%y%m%d') - evaluation_date).days-1
                              for d in cetes_vec['Serie']]
    cetes_vec['Maturity'] = pd.to_datetime(cetes_vec['Serie'], format='%y%m%d')
    condi_stnd = [((cetes_vec['DiasPorVencer'] >= mi) & (cetes_vec['DiasPorVencer'] < ma))
                   for (mi, ma) in zip(min_plz,max_plz)]
    
    cetes_vec['Plazo'] = np.select(condi_stnd, plazos_standar)
    
    inter = interp1d(cetes_vec['DiasPorVencer'].values, 
                      cetes_vec['Rendimiento'].values, kind='linear', axis = -1,
                      copy=True, bounds_error = None, fill_value='extrapolate')
    
    cetes_vec['br_rate'] = inter(np.array(cetes_vec['Plazo']))
    cetes_stand = cetes_vec.groupby('Plazo')['Plazo','br_rate'].mean()
    
    extra_cetes = live_cetes[live_cetes['Node']==False][['DiasPorVencer', 'br_rate']]
    extra_cetes.rename(columns={'DiasPorVencer': 'Plazo'}, inplace=True)
    
    cetes_stand = pd.concat([cetes_stand, extra_cetes])
    cetes_stand.drop_duplicates(subset='Plazo', inplace=True)
    cetes_stand = cetes_stand[cetes_stand['Plazo']>2]
    cetes_stand.sort_values(by='Plazo', inplace=True)
    cete_zip = [(cetes_stand.iloc[i]['br_rate']/100, int(cetes_stand.iloc[i]['Plazo'])) 
                for i in range(0, cetes_stand.shape[0])]
    
    return cete_zip
    

def pnl_stndcetes_tiie(cetes, tdy, tiie_zero, stp_tiie, funding_df, curves, 
                       notional):
       
    stand_cete = standard_cete(cetes, tdy, tiie_zero)
    
    stand_cete.drop('TIIE_Zero', axis = 1, inplace = True)
    
    stand_cete['TIIE_FairRate'] = 0
    stand_cete['Fix_Amount'] = 0
    stand_cete['Float_Amount'] = 0
    stand_cete['Dirty_Price'] = 0
    stand_cete['Funding'] = 0
    # try:
    #     stand_cete['Maturity'] = stand_cete['Maturity'].dt.date
    # except:
    #     pass
    # try:
    #     funding_df['Date'] = pd.to_datetime(funding_df['Date'])
    #     funding_df['Date'] = funding_df['Date'].dt.date
    # except:
    #     pass



    for i, v in stand_cete.iterrows():
        start_date = ql.Mexico().advance(
            ql.Date().from_date(tdy), ql.Period(1, ql.Days)).to_date()
        
            
        swp = cf.tiieSwap(start_date, v['Maturity'], 
                          notional, 0, curves, 0)
        
        fix_amnt, flt_amnt = cashflows(swp, stp_tiie)
    
        # [print(i,sw.amount()) for sw in swp.swap.leg(0)] 
        stand_cete.at[i, 'TIIE_FairRate'] = swp.fairRate()*100
        stand_cete.at[i, 'Fix_Amount'] = fix_amnt
        stand_cete.at[i, 'Float_Amount'] = flt_amnt
            
            
       
     
        stand_cete.at[i, 'TIIE_Zero'] = tiie_zero[
            tiie_zero['VALOR'] == 
            (v['Maturity']-start_date).days]['PLAZO'].iloc[0]
        

        stand_cete.at[i, 'Dirty_Price'] = 10/(
            1+v['Rendimiento_inter']*(v['Maturity']-start_date).days/36000)
        
        try:
            stand_cete.at[i, 'Funding'] = funding_df[
                funding_df['Date'] == v['Maturity']]['Funding'].iloc[0]
        except:
            print('Problem with fondeo dataframe date format in Standard '+
                  'CETES PnL.')
        
    stand_cete['Settlement'] = stand_cete['Dirty_Price']*notional/10
    stand_cete['P&L Cete'] = notional - stand_cete['Settlement']
    stand_cete['Funding_Cost'] = -stand_cete['Settlement']\
        *stand_cete['Funding']*stand_cete['plz_stnd']/36000
    stand_cete['Tot P&L CETE'] = stand_cete['P&L Cete'] +\
        stand_cete['Funding_Cost']
    
    stand_cete['Tot P&L TIIE'] = stand_cete['Fix_Amount'] +\
        stand_cete['Float_Amount']
        
    stand_cete['Serie'] = stand_cete['Maturity'].apply(
        lambda d: d.strftime('%y%m%d'))
        
    
    cols = ['plz_stnd', 'Serie', 'Maturity', 'Rendimiento_inter', 'Dirty_Price',
            'Settlement', 'P&L Cete', 'Funding', 'Funding_Cost', 'Tot P&L CETE',
            'TIIE_FairRate', 'Fix_Amount', 'Float_Amount', 'Tot P&L TIIE']
    
    return stand_cete[cols]

def spread_at_t(date: date, curvas: cf.mxn_curves, bond_type:str, 
                notional: float=1_000_000_000) -> pd.DataFrame:
    """Calculates spread at time T between bonds and TIIE.
    

    Parameters
    ----------
    date : date
        Date at which you want to calculate spread.
    curvas : cf.mxn_curves
        Curves used to price TIIE.
    bond_type : str
        CETES or Mbono.
    notional : float, optional
        Notional. The default is 1_000_000_000.

    Returns
    -------
    bonos_t : pd.DataFrame
        DataFrame with spread data.

    """
    
    m = 360/182
    p = 360/28
    
    months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 6: 'jun', 
                  7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 
                  12: 'dic'}
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    close_date = ql.Mexico().advance(ql.Date().from_date(date), 
                              ql.Period(-1, ql.Days)).to_date()
    spot_date = ql.Mexico().advance(ql.Date().from_date(date), 
                              ql.Period(1, ql.Days)).to_date()

    # SQL Query
    bonos_t = pd.read_sql_query(
        "SELECT * FROM [dbo].[BondRates] INNER JOIN [dbo].[BondData] on "+
        f"br_isin=tic_isin WHERE br_bondType='{bond_type}' AND "+
        f"br_date='{close_date}'", conn)
    
    if bond_type == 'Mbono':
        bonos_t['Maturity'] = pd.to_datetime(
            bonos_t['tic_maturityDate'], format='%Y%m%d')
        bonos_t['Bond_Name'] = bonos_t['Maturity'].apply(
            lambda x: months_dic[x.month]+str(x.year)[-2:])
        bonos_t['Plazo'] = (bonos_t['Maturity'] - 
                                      pd.to_datetime(spot_date)).dt.days
    
    elif bond_type == 'CETES':
        bonos_t['Plazo'] = bonos_t['tic_maturityDate'].astype(int)
        bonos_t['Maturity'] = bonos_t['Plazo'].apply(
            lambda d: ql.Mexico().advance(
                ql.Date().from_date(spot_date + timedelta(int(d))), 
                ql.Period(0, ql.Days)).to_date())
        bonos_t['Maturity'] = pd.to_datetime(bonos_t['Maturity'])
        bonos_t['Bond_Name'] = bonos_t['Plazo'].astype(str)
    
    # Interpolation to fill missing bonds values
    locs = bonos_t[~(bonos_t['br_rate'].isna())].index
    x = bonos_t.loc[locs]['Plazo']
    y = bonos_t.loc[locs]['br_rate']
    bono_recta = interp1d(x, y, fill_value='extrapolate')
    bonos_t['br_rate'] = np.select(
        [bonos_t['br_rate'].isna()], 
        [bonos_t['Plazo'].apply(lambda p: bono_recta(p))],
        default = bonos_t['br_rate'])
    
    # Equivalent TIIE rate for each maturity date
    bonos_t['TIIE'] = bonos_t['Maturity'].apply(
        lambda m: eq_fair_rate(m, curvas, notional, date))
    
    if bond_type == 'Mbono':
        bonos_t['Bond_Name'] = ["'" + n for n in bonos_t['Bond_Name']]
        bonos_t['TIIE Comp'] = [comparable_rate(r/100, p, m)*100 
                                  for r in bonos_t['TIIE']]
    
    else:
        tenor_list = tuple(str(p)+'d' for p in bonos_t['Plazo'])
        tiie_zero = pd.read_sql_query("SELECT * FROM [dbo].[Derivatives] "+
                                      "WHERE dv_stenor='Zero' AND "+
                                      f"dv_date='{close_date}' AND "+
                                      f"dv_ftenor IN {tenor_list}", conn)
        
        tiie_zero['Plazo'] = tiie_zero['dv_ftenor'].str.replace(
            'd', '').astype(int)
        tiie_zero.rename(columns={'br_rate': 'zero_rate'}, inplace=True)
        bonos_t = bonos_t.merge(tiie_zero[['Plazo', 'zero_rate']], how='left', 
                      left_on='Plazo', right_on='Plazo')
        bonos_t['TIIE Comp'] = bonos_t['zero_rate']*100

    bonos_t.sort_values(by='Maturity', inplace=True)
    bonos_t = bonos_t[['Bond_Name', 'br_rate', 'TIIE', 'TIIE Comp', 
                       'Maturity', 'tic_coupon']]
    
    # Save spread in pickle file
    closes_path = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
        'Tool/Blotter/Closes/'
    date_str = date.strftime('%d%m%y')
    
    if bond_type == 'Mbono':
        save_obj(bonos_t, closes_path + 'bond_' + date_str)
        
    elif bond_type == 'CETES':
        save_obj(bonos_t, closes_path + 'cete_' + date_str)
    
    return bonos_t

def live_pnl(pricing_file: str, graph_file: str, 
             tiie28: float) -> cf.mxn_curves:
    """Calculates live PnL for ASWW.
    

    Parameters
    ----------
    pricing_file : str
        TIIE_IRS_Data file name.
    graph_file : str
        IRS_Parameters file name.
    tiie28 : float
        Current TIIE28 rate.

    Returns
    -------
    curvas : cf.mxn_curves
        Updated curves.

    """
    
    graph_book = xw.Book(graph_file)
    
    m = 360/182
    p = 360/28
    
    months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 6: 'jun', 
                  7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 
                  12: 'dic'}
    
    # Important sheets
    parameters_sheet = graph_book.sheets('Scenarios')
    parameters_sheet.api.Calculate()
    cetes_sheet = graph_book.sheets('ASW_Cetes')
    cetes_sheet.api.Calculate()
    bonos_sheet = graph_book.sheets('ASW_Bonos')
    bonos_sheet.api.Calculate()
    
    
    # Evaluation parameters
    evaluation_date = parameters_sheet.range('C2').value
    yst_date = ql.Mexico().advance(
        ql.Date().from_date(evaluation_date), ql.Period(-1, ql.Days)).to_date()
    notional = parameters_sheet.range('C6').value
    cete_spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(1, ql.Days)).to_date()
    
    # Banxico Scenarios
    scen_row = parameters_sheet.range('F5').end('down').row
    scenarios = parameters_sheet.range('E5:I'+str(scen_row)).options(
        pd.DataFrame, index=False, header=1).value
    scenarios.rename(columns={scenarios.columns[0]: 'Meeting_Date'}, 
                     inplace=True)
    scenarios.set_index('Meeting_Date', inplace=True)
    scenario_letter = parameters_sheet.range('G27').value[-1]
    scenario = scenarios[[scenario_letter]]
    
    # Step rates
    step_tiie28 = step_rate(tiie28, scenario.copy(), evaluation_date)
    fondeo_df = parameters_sheet.range('L4').expand('table').options(
        pd.DataFrame, index=False, header=1).value
    
    # Live curves calculation
    input_sheets = ['USD_OIS', 'USD_SOFR', 'USDMXN_XCCY_Basis', 'USDMXN_Fwds', 
                    'MXN_TIIE', 'Granular_Tenors']
    dic_data = {}
    for sheet in input_sheets:
        dic_data[sheet] = pd.read_excel(pricing_file, sheet)
    
    historical_tdy = []
    if ql.UnitedStates(1).isHoliday(ql.Date().from_date(evaluation_date)):
        historical_tdy = ['MXN_OIS']
        
    print('Calculating Live Spreads and Live PnL...')
    curvas = cf.mxn_curves(dic_data, 'InitialCurves', historical_tdy)
    
    # CETES Live Spread
    cetes = cetes_sheet.range('B6').expand('table').options(
        pd.DataFrame, header=True, index=False).value
    cetes['CETE'] = cetes['CETE'].astype(int)
    cetes['Maturity'] = cetes['CETE'].apply(
        lambda d: ql.Mexico().advance(
            ql.Date().from_date(cete_spot_date + timedelta(int(d))), 
            ql.Period(0, ql.Days)).to_date())
    cetes['Maturity'] = pd.to_datetime(cetes['Maturity'])
    cete_spread = cetes[['CETE', 'Maturity']].copy()
    cete_spread['TIIE'] = cete_spread['Maturity'].apply(
        lambda m: eq_fair_rate(m, curvas, notional, evaluation_date))
    cete_spread['TIIE Comp'] = cete_spread['Maturity'].apply(
        lambda m: curvas.crvMXNTIIE.curve.zeroRate(ql.Date().from_date(m), 
                                                   ql.Actual360(), ql.Simple, 
                                                   ql.Annual).rate()*100)
    
    cetes_sheet.range('B24:B36').clear_contents()
    cetes_sheet.range('D24:E36').clear_contents()
    cetes_sheet.range('B24').value = cete_spread[['CETE']].values
    cetes_sheet.range('D24').value = cete_spread[['TIIE', 'TIIE Comp']].values
    
    # CETES Live PnL
    live_cetes = cetes_sheet.range('C95').expand('down').options(
        pd.DataFrame, header=True, index=False).value
    live_yields = cetes_sheet.range('E95').expand('down').options(
        pd.DataFrame, header=True, index=False).value
    cetes_live = live_cetes.merge(live_yields, how='left', left_index=True, 
                                  right_index=True)
    # cetes_row = cetes_sheet.range('C95').end('down').row
    # live_cetes = cetes_sheet.range('C96:C'+str(cetes_row)).value
    # cetes_rates = cetes_sheet.range('E96:E'+str(cetes_row)).value
    # cetes_live = pd.DataFrame({'Serie': live_cetes, 
    #                            'Rendimiento': cetes_rates})
    cetes_live.rename(columns={'YTM': 'Rendimiento'}, inplace=True)
    cetes_live['Maturity'] = pd.to_datetime(cetes_live['Serie'], 
                                            format='%y%m%d')
    
    cetes_cols = ['Plazo', 'Serie', 'Maturity', 'Rendimiento', 'Price', 
                  'Settlement', 'P&L', 'Funding', 'FundingCost', 'Final_P&L', 
                  'TIIE', 'Fix', 'Float', 'Swap_P&L']
    cetes_live = pnl_df(cetes_live, evaluation_date, curvas, notional, 
                        step_tiie28, 'CETES', fondeo_df, cetes_cols, True)
    
    cetes_sheet.range('B96:B112').clear_contents()
    cetes_sheet.range('D96:D112').clear_contents()
    cetes_sheet.range('F96:O112').clear_contents()
    cetes_sheet.range('B96').value = cetes_live[['Plazo']].values
    cetes_sheet.range('D96').value = cetes_live[['Maturity']].values
    cetes_sheet.range('F96').value = cetes_live[cetes_cols[4:]].values
    
    
    # Bonds Live Spread
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    bonds = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] LEFT JOIN "+
                              "[dbo].[BondData] ON br_isin=tic_isin WHERE "+
                              f"br_date='{yst_date}' AND br_bondType='Mbono'", 
                              conn)
    
    bonds['Maturity'] = pd.to_datetime(
        bonds['tic_maturityDate'], format='%Y%m%d')
    
    bonds.sort_values(by='Maturity', inplace=True)
     
    bonds['Bond_Name'] = bonds['Maturity'].apply(
        lambda x: months_dic[x.month]+str(x.year)[-2:])
    bonds['Bond_Name'] = ["'" + n for n in bonds['Bond_Name'].tolist()]
    
    bonds_spread = bonds[['Bond_Name', 'Maturity']].copy()
    bonds_spread['TIIE'] = bonds_spread['Maturity'].apply(
        lambda m: eq_fair_rate(m, curvas, notional, evaluation_date))
    bonds_spread['TIIE Comp'] = [comparable_rate(r/100, p, m)*100 
                              for r in bonds_spread['TIIE']]
    
    bonos_sheet.range('B33:B54').clear_contents()
    bonos_sheet.range('D33:E54').clear_contents()
    bonos_sheet.range('B33').value = bonds_spread[['Bond_Name']].values
    bonos_sheet.range('D33').value = bonds_spread[['TIIE', 'TIIE Comp']].values
    
    # Bonds Live PnL
    live_bonos = bonos_sheet.range('B71').expand('down').options(
        pd.DataFrame, header=True, index=False).value
    live_yields = bonos_sheet.range('D71').expand('down').options(
        pd.DataFrame, header=True, index=False).value
    bonos_live = live_bonos.merge(live_yields, how='left', left_index=True, 
                                  right_index=True)
    bonos_live.rename(columns={'Mbono': 'Bond_Name', 'YTM': 'br_rate'}, 
                      inplace=True)
    # bonos_row = bonos_sheet.range('B71').end('down').row
    # live_bonos = bonos_sheet.range('B72:B'+str(bonos_row)).value
    # bonos_rates = bonos_sheet.range('D72:D'+str(bonos_row)).value
    # bonos_live = pd.DataFrame({'Bond_Name': live_bonos, 
    #                            'br_rate': bonos_rates})
    bonds['Bond_Name'] = bonds['Bond_Name'].str.replace("'", "")
    bonos_live = bonos_live.merge(bonds[['Bond_Name', 'Maturity', 
                                              'tic_coupon']], 
                              how='left', left_on='Bond_Name', 
                              right_on='Bond_Name')
    
    bond_cols = ['Bond_Name', 'Maturity', 'br_rate', 'Price',
                 'Settlement', 'Return', 'P&L', 'Funding', 'FundingCost', 
                 'Final_P&L',
                 'TIIE', 'Fix', 'Float', 'Swap_P&L']
    
    bonos_live = pnl_df(bonos_live, evaluation_date, curvas, notional, 
                        step_tiie28, 'Mbono', fondeo_df, bond_cols, True)
    
    bonos_sheet.range('C72:C80').clear_contents()
    bonos_sheet.range('E72:O80').clear_contents()
    bonos_sheet.range('C72').value = bonos_live[['Maturity']].values
    bonos_sheet.range('E72').value = bonos_live[bond_cols[3:]].values
    
    return curvas
    

# def pnl_live(pricing_file, graph_book, tiie28, bonds_data, cetes_data, curvas):
    
#     m = 360/182
#     p = 360/28
    
#     # Coupons dictionary
#     bond_dic = {'dic23': 8.0, 'sep24': 8, 'dic24': 10, 'mar25': 5,
#                 'mar26': 5.75, 'sep26': 7, 'mar27': 5.5, 'jun27': 7.5,
#                 'mar29': 8.5, 'may29': 8.5, 'may31': 7.75, 'may33': 7.5,
#                 'nov34': 7.75, 'nov36': 10, 'nov38': 8.5, 'nov42': 7.75,
#                 'nov47': 8, 'jul53': 8, 'may35': 8}
    
#     cetes_sheet = graph_book.sheets('ASW_Cetes')
#     cetes_sheet.api.Calculate()
    
#     # Evaluation parameters
#     parameters_sheet = graph_book.sheets('Scenarios')
#     evaluation_date = parameters_sheet.range('C2').value
#     tiie_fondeo = parameters_sheet.range('C5').value
#     notional = parameters_sheet.range('C6').value
#     cete_spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
#                                     ql.Period(1, ql.Days)).to_date()
    
#     # Banxico Scenarios
#     scen_row = parameters_sheet.range('F5').end('down').row
#     scenarios = parameters_sheet.range('E5:I'+str(scen_row)).options(
#         pd.DataFrame, index=False, header=1).value
#     scenarios.rename(columns={scenarios.columns[0]: 'Meeting_Date'}, 
#                      inplace=True)
#     scenarios.set_index('Meeting_Date', inplace=True)
#     scenario_letter = parameters_sheet.range('G27').value[-1]
#     scenario = scenarios[[scenario_letter]]
    
#     # Step rates
#     step_fondeo = step_rate(tiie_fondeo, scenario.copy(), evaluation_date)
#     step_tiie28 = step_rate(tiie28, scenario.copy(), evaluation_date)
#     scenario_rates = step_fondeo.rename(columns={'TIIE': 'Fondeo'}).merge(
#         step_tiie28[['TIIE']], how='left', left_index=True, right_index=True)
    
#     fondeo_df = parameters_sheet.range('L4').expand('table').options(
#         pd.DataFrame, index=False, header=1).value
    
#     # Today's curves with closing prices
#     yst_date = ql.Mexico().advance(
#         ql.Date().from_date(evaluation_date), ql.Period(-1, ql.Days)).to_date()
#     str_yst = yst_date.strftime('%Y%m%d')
#     main_path = '//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
#         'Tool/Main Codes/Portfolio Management/OOP codes/'
#     str_file = main_path + 'TIIE_CurveCreate_Inputs.xlsx'
#     tiie_zero = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/Historical '+
#                               f'OIS TIIE/TIIE_{yst_date.strftime("%Y%m%d")}'+
#                               '.xlsx')
    
#     # CETES PnL with closing prices
#     vector_file = f'//minerva/Applic/finamex/gbs/vector_precios_{str_yst}.csv'
#     vector_precio = pd.read_csv(vector_file)
#     cetes_vec = vector_precio[vector_precio['TV']=='BI'].copy()
#     cetes = cetes_vec.copy()
#     cetes['Maturity'] = pd.to_datetime(cetes['Serie'], format='%y%m%d')
#     cetes = cetes[cetes['Maturity'] > pd.to_datetime(cete_spot_date)]
#     cetes = cetes[['Instrumento', 'Rendimiento', 'Maturity', 'Serie']]
#     cetes_cols = ['Plazo', 'Serie', 'Maturity', 'Rendimiento', 'Price', 
#                   'Settlement', 'P&L', 'Funding', 'FundingCost', 'Final_P&L', 
#                   'TIIE', 'Fix', 'Float', 'Swap_P&L']
#     cetes = pnl_df(cetes, evaluation_date, curvas, notional, step_tiie28, 
#                    'CETES', fondeo_df, cetes_cols)
    
#     stand_cetes_pnl = pnl_stndcetes_tiie(cetes_vec, evaluation_date, tiie_zero, 
#                                          step_tiie28, fondeo_df,  curvas, 
#                                          notional)
    
#     cetes_sheet.range('B42:O54').clear_contents()
#     cetes_sheet.range('B57:O94').clear_contents()
#     cetes_sheet.range('B42').value = stand_cetes_pnl.values
#     cetes_sheet.range('B57').value = cetes.values
    
#     # Bonds PnL with closing prices
#     bonos_df = bonds_data[['Bond_Name', 'Maturity', 'br_rate', 
#                            'tic_coupon']].copy()
    
#     bond_cols = ['Bond_Name', 'Maturity', 'br_rate', 'Price',
#                  'Settlement', 'Return', 'P&L', 'Funding', 'FundingCost', 
#                  'Final_P&L',
#                  'TIIE', 'Fix', 'Float', 'Swap_P&L']
    
#     bonos_df = pnl_df(bonos_df, evaluation_date, curvas, notional, step_tiie28, 
#                       'Mbono', fondeo_df, bond_cols)
    
#     bonos_sheet = graph_book.sheets('ASW_Bonos')
#     bonos_sheet.range('B60:O68').clear_contents()
#     bonos_sheet.range('B60').value = bonos_df[bond_cols].values
#     bonos_sheet.api.Calculate()
    
#     # Live PnL
#     pricing_book = xw.Book(pricing_file)
#     dftiie = pricing_book.sheets(
#         'MXN_TIIE').range('A1').expand('table').options(pd.DataFrame, header=1, 
#                                                         index=False).value
#     dic_data = curvas.dic_data
#     dic_data['MXN_TIIE'] = dftiie
#     curvas = cf.mxn_curves(dic_data, 'InitialCurves')
    
#     # CETES Live Spread
#     cete_spread = cetes_data[['Bond_Name', 'Maturity']].copy()
#     cete_spread['TIIE'] = cete_spread['Maturity'].apply(
#         lambda m: eq_fair_rate(m, curvas, notional, evaluation_date))
#     cete_spread['TIIE Comp'] = cete_spread['Maturity'].apply(
#         lambda m: curvas.crvMXNTIIE.curve.zeroRate(ql.Date().from_date(m), 
#                                                    ql.Actual360(), ql.Simple, 
#                                                    ql.Annual).rate()*100)
    
#     cetes_sheet.range('B24:B36').clear_contents()
#     cetes_sheet.range('D24:E36').clear_contents()
#     cetes_sheet.range('B24').value = cete_spread[['Bond_Name']].values
#     cetes_sheet.range('D24').value = cete_spread[['TIIE', 'TIIE Comp']].values
    
#     # CETES Live PnL
#     cetes_row = cetes_sheet.range('C95').end('down').row
#     live_cetes = cetes_sheet.range('C96:C'+str(cetes_row)).value
#     cetes_rates = cetes_sheet.range('E96:E'+str(cetes_row)).value
#     cetes_live = pd.DataFrame({'Serie': live_cetes, 
#                                'Rendimiento': cetes_rates})
#     cetes_live['Maturity'] = pd.to_datetime(cetes_live['Serie'], 
#                                             format='%y%m%d')
#     cetes_live = pnl_df(cetes_live, evaluation_date, curvas, notional, 
#                         step_tiie28, 'CETES', fondeo_df, cetes_cols, True)
    
#     cetes_sheet.range('B96:B112').clear_contents()
#     cetes_sheet.range('D96:D112').clear_contents()
#     cetes_sheet.range('F96:O112').clear_contents()
#     cetes_sheet.range('B96').value = cetes_live[['Plazo']].values
#     cetes_sheet.range('D96').value = cetes_live[['Maturity']].values
#     cetes_sheet.range('F96').value = cetes_live[cetes_cols[4:]].values
    
    
#     # Bonds Live Spread
#     bonds_spread = bonds_data[['Bond_Name', 'Maturity']].copy()
#     bonds_spread['TIIE'] = bonds_spread['Maturity'].apply(
#         lambda m: eq_fair_rate(m, curvas, notional, evaluation_date))
#     bonds_spread['TIIE Comp'] = [comparable_rate(r/100, p, m)*100 
#                               for r in bonds_spread['TIIE']]
    
#     bonos_sheet.range('B33:B54').clear_contents()
#     bonos_sheet.range('D33:E54').clear_contents()
#     bonos_sheet.range('B33').value = bonds_spread[['Bond_Name']].values
#     bonos_sheet.range('D33').value = bonds_spread[['TIIE', 'TIIE Comp']].values
    
#     # Bonds Live PnL
#     bonos_row = bonos_sheet.range('B71').end('down').row
#     live_bonos = bonos_sheet.range('B72:B'+str(bonos_row)).value
#     bonos_rates = bonos_sheet.range('D72:D'+str(bonos_row)).value
#     bonos_live = pd.DataFrame({'Bond_Name': live_bonos, 
#                                'br_rate': bonos_rates})
#     bonds_data['Bond_Name'] = bonds_data['Bond_Name'].str.replace("'", "")
#     bonos_live = bonos_live.merge(bonds_data[['Bond_Name', 'Maturity', 
#                                               'tic_coupon']], 
#                               how='left', left_on='Bond_Name', 
#                               right_on='Bond_Name')
#     bonos_live = pnl_df(bonos_live, evaluation_date, curvas, notional, 
#                         step_tiie28, 'Mbono', fondeo_df, bond_cols, True)
    
#     bonos_sheet.range('C72:C80').clear_contents()
#     bonos_sheet.range('E72:O80').clear_contents()
#     bonos_sheet.range('C72').value = bonos_live[['Maturity']].values
#     bonos_sheet.range('E72').value = bonos_live[bond_cols[3:]].values
    
#     return curvas
    
def hist_asw(graph_book, long_leg, short_leg):
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    # TIIE vs BONO ASW
    if long_leg == 'TIIE' and short_leg == 'BONO':
        months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 
                      6: 'jun', 7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 
                      11: 'nov', 12: 'dic'}
                      
        
        sheet = graph_book.sheets('ASW_Bonos')
        bond_type = 'Mbono'
        start_date = sheet.range('C2').value
        end_date = sheet.range('C3').value
        
        # Short Leg Data
        short_df = pd.read_sql_query("SELECT * FROM [dbo].[BondRates]"+
                                    " INNER JOIN [dbo].[BondData] on "+
                                    "br_isin=tic_isin WHERE "+
                                    "br_bondType='Mbono' AND "+
                                     f"br_date>='{start_date}' AND "+
                                     f"br_date<='{end_date}'", conn)
        
        short_data = short_df.copy()
        short_data['Date'] = short_data['br_date'].dt.date
        short_data['Maturity'] = pd.to_datetime(short_data['tic_maturityDate'], 
                                              format='%Y%m%d').dt.date
        short_data = short_data[short_data['Maturity'] > end_date.date()]
        short_data['Plazo'] = (
            short_data['Maturity'] - short_data['Date'].apply(
                lambda d: ql.Mexico().advance(
                    ql.Date.from_date(d),
                    ql.Period(1, ql.Days)).to_date())).apply(lambda x: x.days)
        short_data['Name'] = short_data['Maturity'].apply(
            lambda x: months_dic[x.month]+str(x.year)[-2:])
        
        # Long Leg Data
        long_df = pd.read_sql_query("SELECT * FROM [dbo].[Derivatives] WHERE"+
                                    " dv_stenor<>'SOFR' AND dv_stenor='Spot' "+
                                    f"AND dv_date>='{start_date}' AND "+
                                    f"dv_date<='{end_date}'", conn)
        
        long_data = long_df.copy()
        long_data['Date'] = long_data['dv_date'].dt.date
        long_data['Plazo'] = long_data['dv_ftenor'].apply(
            lambda x: int(x[:-1])*28 if x[-1]=='m' else int(x[:-1])*364)
        
        # Short Dates
        dates = short_data['Date'].unique().tolist()
        dates.sort()
        print_rang = 'I33'
        clear_rang = 'L54'
    
    # TIIE vs CETE ASW
    elif long_leg == 'TIIE' and short_leg == 'CETES':
        
        sheet = graph_book.sheets('ASW_Cetes')
        bond_type = 'CETES'
        start_date = sheet.range('C2').value
        end_date = sheet.range('C3').value
        
        # Short Leg Data
        short_df = pd.read_sql_query("SELECT * FROM [dbo].[BondRates]"+
                                     " INNER JOIN [dbo].[BondData] on "+
                                     "br_isin=tic_isin WHERE "+
                                     "br_bondType='CETES' AND "+
                                     f"br_date>='{start_date}' AND "+
                                     f"br_date<='{end_date}'", conn)
        
        short_data = short_df.copy()
        short_data['Date'] = short_data['br_date'].dt.date
        short_data['Plazo'] = short_data['tic_maturityDate'].astype(int)
        short_data['Name'] = short_data['Plazo'].copy()
        
        # Long Leg Data
        long_df = pd.read_sql_query("SELECT * FROM [dbo].[Derivatives] WHERE"+
                                    " dv_stenor<>'SOFR' AND dv_stenor='Spot' "+
                                    f"AND dv_date>='{start_date}' AND "+
                                    f"dv_date<='{end_date}'", conn)
        
        long_data = long_df.copy()
        long_data['Date'] = long_data['dv_date'].dt.date
        long_data['Plazo'] = long_data['dv_ftenor'].apply(
            lambda x: int(x[:-1])*28 if x[-1]=='m' else int(x[:-1])*364)
        
        # Short Dates
        dates = short_data['Date'].unique().tolist()
        dates.sort()
        print_rang = 'I24'
        clear_rang = 'L36'
        
    else:
        print(' UNDER CONSTRUCTION '.center(52,'#'))
        raise Exception('Module is under construction')
    
    isins = short_data['br_isin'].unique().tolist()
    short_data_copy = short_data.copy()
    hist_df = pd.DataFrame()
    for d in dates:
        
        hist_a = pd.DataFrame()
        long_d = long_data[long_data['Date'] == d].copy()
        
        if long_d.empty:
            continue
        
        short_d = short_data[short_data['Date'] == d].copy()
        missing_isins = [i for i in isins if i not in 
                         short_d['br_isin'].unique()]
        n = len(missing_isins)
        isin_df = short_data_copy.drop_duplicates(subset='br_isin')
        missing_df = isin_df[isin_df['br_isin'].isin(missing_isins)]
        
        missing_df['br_date'] = d
        missing_df['Date'] = d
        missing_df['br_rate'] = np.nan
        
        short_d = pd.concat([short_d, missing_df])

        # Short Rate Calculation
        locs = short_d[~(short_d['br_rate'].isna())].index
        x = short_d.loc[locs]['Plazo']
        y = short_d.loc[locs]['br_rate']
        bono_recta = interp1d(x, y, fill_value='extrapolate')
        
        hist_a['Short_Rate'] = np.select(
            [short_d['br_rate'].isna()], 
            [short_d['Plazo'].apply(lambda p: bono_recta(p))],
            default = short_d['br_rate'])
        
        # Interpolate long_d
        recta_d = interp1d(long_d['Plazo'], long_d['br_rate'], 
                           fill_value='extrapolate')
            
        # Long Rate Calculation
        hist_a['Long_Rate'] = short_d['Plazo'].apply(
            lambda p: recta_d(p)*100).values
        
        # Spread Calculation
        hist_a[d] = hist_a['Long_Rate'] - hist_a['Short_Rate']
        
        # Hist DataFrame concat
        hist_a.index = short_d['tic_maturityDate'].str.strip().tolist()
        hist_df = pd.concat([hist_df, hist_a[[d]].T])
    
    hist_df.columns = hist_df.columns.astype(int)
    hist_df = hist_df.T.sort_index().T
    
    stats = hist_df.max().to_frame(name = 'Max')*100
    stats['Min'] = hist_df.min()*100
    stats['Avg'] = hist_df.mean()*100
    
    if short_leg == 'BONO':
        dic_name = dict(zip(short_data['tic_maturityDate'].unique(),
                            short_data['Name'].unique()))
        stats.index = pd.Series(stats.index).astype(str).apply(
            lambda x: dic_name[x])
        stats['Name'] = "'" + stats.index
    
    else:
        stats.index = stats.index.astype(int)
        stats.sort_index(inplace = True)
        stats['Name'] = stats.index
        
    cols = ['Name', 'Max', 'Min', 'Avg']
    
    sheet.range(print_rang + ':' + clear_rang).clear_contents()
    sheet.range(print_rang).value = stats[cols].values
    

def tiie_spreads(pricing_file: str, graph_file: str, 
                 tiie28: float) -> cf.mxn_curves:
    """Calculates spreads for T and T-1.
    
    This function calculates Bonds vs. TIIE and CETES vs. TIIE spreads for T 
    and T-1, as well as historical data like min, max and average.
    

    Parameters
    ----------
    pricing_file : str
        Name of TIIE_IRS_Data Excel file.
    graph_file : str
        Name of IRS_Parameteres file.
    tiie28 : float
        Current TIIE28 value.

    Returns
    -------
    close_curves : cf.mxn_curves
        Curves created with close prices for close PnL calculation.

    """
    
    main_path = '//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/' +\
        'Main Codes/Portfolio Management/OOP codes/'
    str_file = main_path + 'TIIE_CurveCreate_Inputs.xlsx'
    
    graph_book = xw.Book(graph_file)
    parameters_sheet = graph_book.sheets('Scenarios')
    parameters_sheet.api.Calculate()
    parameters_sheet.range('C4').value = tiie28
    evaluation_date = parameters_sheet.range('C2').value
    
    bonos_sheet = graph_book.sheets('ASW_Bonos')
    cetes_sheet = graph_book.sheets('ASW_Cetes')
    
    print('Calculating Historical Stats...')
    hist_asw(graph_book, 'TIIE', 'BONO')
    hist_asw(graph_book, 'TIIE', 'CETES')
    cetes_sheet.api.Calculate()
    bonos_sheet.api.Calculate()

    print('Calculating T and T-1 Close Spreads...')
    yst_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                              ql.Period(-1, ql.Days)).to_date()
    
    yst_date_str = yst_date.strftime('%d%m%y')
    closes_path = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
        'Tool/Blotter/Closes/'
    
    yst_flag = False
        
    try:
        bond_t_minus_one = load_obj(closes_path + 'bond_' + yst_date_str)
        cete_t_minus_one = load_obj(closes_path + 'cete_' + yst_date_str)
    
    except:
        print('No data for T-1 found. Calculating T-1 closes...')
        ql.Settings.instance().evaluationDate = ql.Date().from_date(
            yst_date)
        yst_close = ql.Mexico().advance(ql.Date().from_date(yst_date), 
                                  ql.Period(-1, ql.Days)).to_date()
        
        # Check if yesterday was holiday
        historical_yst = []
        
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(yst_date)):
            historical_yst = ['MXN_OIS']
            
        dic_data_yst = cf.pull_data(str_file, yst_close)
        curvas_yst = cf.mxn_curves(dic_data_yst, None, historical_yst)
        
        bond_t_minus_one = spread_at_t(yst_date, curvas_yst, 'Mbono')
        cete_t_minus_one = spread_at_t(yst_date, curvas_yst, 'CETES')
        
        yst_flag = True
    
    if yst_flag:
        ql.Settings.instance().evaluationDate = ql.Date().from_date(
            evaluation_date)
    
    curvas_yst = None
    historical_tdy = []
    if ql.UnitedStates(1).isHoliday(ql.Date().from_date(evaluation_date)):
        historical_tdy = ['MXN_OIS']
    dic_data_close = cf.pull_data(str_file, yst_date)
    close_curves = cf.mxn_curves(dic_data_close, None, historical_tdy)
    eval_date_str = evaluation_date.strftime('%d%m%y')
    
    try:
        bond_t = load_obj(closes_path + 'bond_' + eval_date_str)
        cete_t = load_obj(closes_path + 'cete_' + eval_date_str)
    
    except:
        bond_t = spread_at_t(evaluation_date, close_curves, 'Mbono')
        cete_t = spread_at_t(evaluation_date, close_curves, 'CETES')
    
    
    cols = ['Bond_Name', 'br_rate', 'TIIE', 'TIIE Comp']
    
    bonos_sheet.range('B7:E29').clear_contents()
    bonos_sheet.range('I7:K29').clear_contents()
    bonos_sheet.range('B7').value = bond_t[cols].values
    bonos_sheet.range('I7').value = bond_t_minus_one[['br_rate', 'TIIE', 
                                                      'TIIE Comp']].values
    
    cetes_sheet.range('B7:E19').clear_contents()
    cetes_sheet.range('I7:K19').clear_contents()
    cetes_sheet.range('B7').value = cete_t[cols].values
    cetes_sheet.range('I7').value = cete_t_minus_one[['br_rate', 'TIIE', 
                                                      'TIIE Comp']].values
    cetes_sheet.api.Calculate()
    bonos_sheet.api.Calculate()
    
    return close_curves

def change_scenario(graph_file: str, tiie28: float) -> None:
    """Updates close PnL with given Banxico scenario.
    

    Parameters
    ----------
    graph_file : str
        Name of IRS_Parameteres file.
    tiie28 : float
        Current TIIE28 value.

    Returns
    -------
    None

    """
    
    graph_book = xw.Book(graph_file)
    cetes_sheet = graph_book.sheets('ASW_Cetes')
    bonos_sheet = graph_book.sheets('ASW_Bonos')
    
    # Evaluation parameters
    parameters_sheet = graph_book.sheets('Scenarios')
    parameters_sheet.api.Calculate()
    evaluation_date = parameters_sheet.range('C2').value
    tiie_fondeo = parameters_sheet.range('C5').value
    notional = parameters_sheet.range('C6').value
    cete_spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(1, ql.Days)).to_date()
    
    # Banxico Scenarios
    scen_row = parameters_sheet.range('F5').end('down').row
    scenarios = parameters_sheet.range('E5:I'+str(scen_row)).options(
        pd.DataFrame, index=False, header=1).value
    scenarios.rename(columns={scenarios.columns[0]: 'Meeting_Date'}, 
                     inplace=True)
    scenarios.set_index('Meeting_Date', inplace=True)
    scenario_letter = parameters_sheet.range('G27').value[-1]
    scenario = scenarios[[scenario_letter]]
    
    # Step rates
    step_fondeo = step_rate(tiie_fondeo, scenario.copy(), evaluation_date)
    step_tiie28 = step_rate(tiie28, scenario.copy(), evaluation_date)
    scenario_rates = step_fondeo.rename(columns={'TIIE': 'Fondeo'}).merge(
        step_tiie28[['TIIE']], how='left', left_index=True, right_index=True)
    
    fondeo_df = parameters_sheet.range('L4').expand('table').options(
        pd.DataFrame, index=False, header=1).value
    
    standard_cetes_df = cetes_sheet.range('B41').expand(
        'table').options(pd.DataFrame, header=True, index=False).value
    
    standard_cetes_df = standard_cetes_df.merge(fondeo_df, how='left', 
                                                left_on='Maturity', 
                                                right_on='Date')
    
    standard_cetes_df['Funding Rate'] = standard_cetes_df['Funding']
    
    standard_cetes_df['Float Amount'] = [cash_flows(
        int(standard_cetes_df.iloc[i].Duration), 
        standard_cetes_df.iloc[i]['TIIE Fair Rate'], 
        step_tiie28, notional, evaluation_date)[1] 
        for i in range(0, standard_cetes_df.shape[0])]
    
    standard_cetes_df['Funding Cost'] = -standard_cetes_df['Funding']*\
        standard_cetes_df['Settlement']*standard_cetes_df['Duration']/36000
        
    standard_cetes_df['Tot P&L CETE'] = standard_cetes_df['P&L Cete'] + \
        standard_cetes_df['Funding Cost']
    standard_cetes_df['Tot P&L TIIE'] = standard_cetes_df['Fix Amount'] + \
        standard_cetes_df['Float Amount']
        
    cetes_sheet.range('B42').value = standard_cetes_df[
        standard_cetes_df.columns[:-4]].values
    
    cetes_df = cetes_sheet.range('B56').expand(
        'table').options(pd.DataFrame, header=True, index=False).value
    
    cetes_df = cetes_df.merge(fondeo_df, how='left', left_on='Maturity', 
                              right_on='Date')
    
    cetes_df['Funding Rate'] = cetes_df['Funding']
    
    cetes_df['Float Amount'] = [cash_flows(int(cetes_df.iloc[i].Duration), 
                                    cetes_df.iloc[i]['TIIE Fair Rate'],
                                    step_tiie28, notional, evaluation_date)[1] 
                         for i in range(0, cetes_df.shape[0])]
    
    cetes_df['Funding Cost'] = -cetes_df['Funding']*cetes_df['Settlement']\
        *cetes_df['Duration']/36000
        
    cetes_df['Tot P&L CETE'] = cetes_df['P&L Cete'] + cetes_df['Funding Cost']
    cetes_df['Tot P&L TIIE'] = cetes_df['Fix Amount'] + cetes_df['Float Amount']
    
    cetes_sheet.range('B57').value = cetes_df[cetes_df.columns[:-6]].values
    
    bonos_df = bonos_sheet.range('B59').expand(
        'table').options(pd.DataFrame, header=True, index=False).value
    bonos_df['Mbono'] = bonos_df['Mbono'].str.replace("'", "")
    bonos_df['Mbono'] = ["'" + n for n in bonos_df['Mbono'].tolist()]
    
    bonos_df = bonos_df.merge(fondeo_df, how='left', left_on='Maturity', 
                              right_on='Date')
    bonos_df['Funding Rate'] = bonos_df['Funding']
    
    bonos_df['Duration'] = bonos_df['Maturity'].apply(
        lambda x: (x-pd.to_datetime(cete_spot_date)).days)
    bonos_df['Float Amount'] = [cash_flows(int(bonos_df.iloc[i].Duration), 
                                    bonos_df.iloc[i]['TIIE Fair Rate'],
                                    step_tiie28, notional, evaluation_date)[1] 
                         for i in range(0, bonos_df.shape[0])]
    
    bonos_df['Funding Cost'] = -bonos_df['Funding']*bonos_df['Settlement']\
        *bonos_df['Duration']/36000
        
    bonos_df['Tot P&L Mbono'] = bonos_df['P&L Mbono'] + bonos_df['Funding Cost']
    bonos_df['Tot P&L TIIE'] = bonos_df['Fix Amount'] + bonos_df['Float Amount']
    
    bonos_sheet.range('B60').value = bonos_df[bonos_df.columns[:-7]].values
    

    

def close_pnl(graph_file: str, close_curves: cf.mxn_curves, 
              tiie28: float) -> None:
    """Calculates close PnL of Bonds vs. TIIE and CETES vs. TIIE.
    
    This function uses the given Banxico scenario to calculate PnL for ASW 
    using close prices.
    

    Parameters
    ----------
    graph_file : str
        Name of IRS_Parameters file.
    close_curves : cf.mxn_curves
        Curves created with close prices.
    tiie28 : float
        Current TIIE28 value.

    Returns
    -------
    None

    """
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 6: 'jun', 
                  7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 
                  12: 'dic'}
    
    graph_book = xw.Book(graph_file)
    
    # Important sheets
    parameters_sheet = graph_book.sheets('Scenarios')
    cetes_sheet = graph_book.sheets('ASW_Cetes')
    bonos_sheet = graph_book.sheets('ASW_Bonos')
    parameters_sheet.api.Calculate()
    cetes_sheet.api.Calculate()
    bonos_sheet.api.Calculate()
    
    # Evaluation parameters
    evaluation_date = parameters_sheet.range('C2').value
    tiie_fondeo = parameters_sheet.range('C5').value
    notional = parameters_sheet.range('C6').value
    cete_spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(1, ql.Days)).to_date()
    
    yst_date = ql.Mexico().advance(
        ql.Date().from_date(evaluation_date), ql.Period(-1, ql.Days)).to_date()
    str_yst = yst_date.strftime('%Y%m%d')
    tiie_zero = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/Historical '+
                              f'OIS TIIE/TIIE_{yst_date.strftime("%Y%m%d")}'+
                              '.xlsx')
    
    # Banxico Scenarios
    scen_row = parameters_sheet.range('F5').end('down').row
    scenarios = parameters_sheet.range('E5:I'+str(scen_row)).options(
        pd.DataFrame, index=False, header=1).value
    scenarios.rename(columns={scenarios.columns[0]: 'Meeting_Date'}, 
                     inplace=True)
    scenarios.set_index('Meeting_Date', inplace=True)
    scenario_letter = parameters_sheet.range('G27').value[-1]
    scenario = scenarios[[scenario_letter]]
    
    # TIIE Step rates
    step_tiie28 = step_rate(tiie28, scenario.copy(), evaluation_date)
    
    # Funding rates
    fondeo_df = parameters_sheet.range('L4').expand('table').options(
        pd.DataFrame, index=False, header=1).value
    
    #--------------------------
    #  CETES PnL
    #--------------------------
    
    # Existing CETES
    print('Calculating PnL...')
    vector_file = f'//minerva/Applic/finamex/gbs/vector_precios_{str_yst}.csv'
    vector_precio = pd.read_csv(vector_file)
    cetes_vec = vector_precio[vector_precio['TV']=='BI'].copy()
    cetes = cetes_vec.copy()
    
    # Data Handling
    cetes['Maturity'] = pd.to_datetime(cetes['Serie'], format='%y%m%d')
    cetes = cetes[cetes['Maturity'] > pd.to_datetime(cete_spot_date)]
    cetes = cetes[['Instrumento', 'Rendimiento', 'Maturity', 'Serie']]
    cetes_cols = ['Plazo', 'Serie', 'Maturity', 'Rendimiento', 'Price', 
                  'Settlement', 'P&L', 'Funding', 'FundingCost', 'Final_P&L', 
                  'TIIE', 'Fix', 'Float', 'Swap_P&L']
    cetes = pnl_df(cetes, evaluation_date, close_curves, notional, step_tiie28, 
                   'CETES', fondeo_df, cetes_cols)
    
    # Standard CETES PnL
    stand_cetes_pnl = pnl_stndcetes_tiie(cetes_vec, evaluation_date, tiie_zero, 
                                         step_tiie28, fondeo_df,  close_curves, 
                                         notional)
    
    # Copy values to sheet
    cetes_sheet.range('B42:O54').clear_contents()
    cetes_sheet.range('B57:O94').clear_contents()
    cetes_sheet.range('B42').value = stand_cetes_pnl.values
    cetes_sheet.range('B57').value = cetes.values
    
    #--------------------------
    #  Bonds PnL
    #--------------------------
    
    # Existing Bonds
    #print('Calculating Bonds Close PnL...')
    bonds = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] LEFT JOIN "+
                              "[dbo].[BondData] ON br_isin=tic_isin WHERE "+
                              f"br_date='{yst_date}' AND br_bondType='Mbono'", 
                              conn)
    
    # Data Handling
    bonds['Maturity'] = pd.to_datetime(
        bonds['tic_maturityDate'], format='%Y%m%d')
     
    bonds['Bond_Name'] = bonds['Maturity'].apply(
        lambda x: months_dic[x.month]+str(x.year)[-2:])
    
    bonds['Bond_Name'] = ["'" + n for n in bonds['Bond_Name'].tolist()]
    
    bonos_df = bonds[['Bond_Name', 'Maturity', 'br_rate', 
                           'tic_coupon']].copy()
    
    bond_cols = ['Bond_Name', 'Maturity', 'br_rate', 'Price',
                 'Settlement', 'Return', 'P&L', 'Funding', 'FundingCost', 
                 'Final_P&L',
                 'TIIE', 'Fix', 'Float', 'Swap_P&L']
    
    bonos_df = pnl_df(bonos_df, evaluation_date, close_curves, notional, step_tiie28, 
                      'Mbono', fondeo_df, bond_cols)
    
    # Copy values to sheet
    bonos_sheet.range('B60:O68').clear_contents()
    bonos_sheet.range('B60').value = bonos_df[bond_cols].values
    bonos_sheet.api.Calculate()
    
    return cetes_vec
    
def snapshot(graph_file):
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    wb = xw.Book(graph_file)
    snap_sheet = wb.sheets('Snapshot')
    snap_sheet.activate()
    scen_sheet = wb.sheets('Scenarios')
    scen_sheet.api.Calculate()
    bonos = snap_sheet.range('B4').expand('down').value
    
    eval_date = scen_sheet.range('C2').value
    ql_eval_date = ql.Date.from_date(eval_date)
    
    yest =  ql.Mexico().advance(ql_eval_date, ql.Period(-1, ql.Days)).to_date()
    
    last_week = ql.Mexico().advance(ql_eval_date- ql_eval_date.weekday()-1 , 
                                    ql.Period(0, ql.Days),
                                    ql.Preceding).to_date()
    
    last_month = ql.Mexico().advance(ql.Date(1, ql_eval_date.month(),
                                             ql_eval_date.year()), 
                                     ql.Period(-1, ql.Days),
                                     ql.Preceding).to_date()
    
    last_year = ql.Mexico().advance(ql.Date(1, 1, ql_eval_date.year()), 
                                     ql.Period(-1, ql.Days),
                                     ql.Preceding).to_date()
    
    df_bonosnap = pd.DataFrame({'YTD': [last_year], 'MTD': [last_month], 
                               'WTD': [last_week], '1d': [yest]})
    
    months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 
                  6: 'jun', 7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 
                  11: 'nov', 12: 'dic'}
    
    # BONOS
    
    sql_bono = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] "+
                                 "INNER JOIN [dbo].[BondData] "+
                                 "ON br_isin = tic_isin "+
                                 f"WHERE br_date IN ('{yest}', '{last_week}'"+
                                 f", '{last_month}', '{last_year}') "+
                                 "AND br_bondType= 'Mbono'", conn)
    
    sql_bono_copy = sql_bono.copy()
    
    isins = sql_bono['tic_isin'].unique().tolist()
    


    for d in sql_bono['br_date'].unique():
        sql_bono_d = sql_bono[sql_bono['br_date']==d]
        missing_isins = [i for i in isins if i not in 
                         sql_bono_d['br_isin'].unique()]
        n = len(missing_isins)
        isin_df = sql_bono_copy.drop_duplicates(subset='br_isin')
        missing_df = isin_df[isin_df['br_isin'].isin(missing_isins)]
        missing_df['br_date'] = d
        missing_df['br_rate'] = np.nan
        sql_bono_d = pd.concat([sql_bono_d, missing_df])
        
        if sql_bono_d['br_rate'].isna().any():
            
            sql_bono_d['Maturity'] = sql_bono_d['tic_maturityDate'].apply(
                lambda t: datetime.strptime(t, '%Y%m%d'))
            
            sql_bono_d['Days'] = (sql_bono_d['Maturity'] - 
                                  sql_bono_d['br_date']).dt.days
            
            rates = sql_bono_d['br_rate'][
                ~sql_bono_d['br_rate'].isna()].values
            
            days = sql_bono_d['Days'][
                ~sql_bono_d['br_rate'].isna()].values
            
            recta = interp1d(days, rates, fill_value = 'extrapolate')

            sql_bono_d['br_rate'] = np.select([sql_bono_d['br_rate'].isna()],
                                              [sql_bono_d['Days'].apply(
                                                  lambda x: recta(x))], 
                                              sql_bono_d['br_rate'])
            
            sql_bono = sql_bono[sql_bono['br_date']!=d]
            
            sql_bono = pd.concat([sql_bono, sql_bono_d[sql_bono.columns]])
     
    sql_bono['BondName'] = sql_bono['tic_maturityDate'].apply(
        lambda d: months_dic[int(d[4:6])] + d[2:4])
            
    
    col_ord = ['1d', 'WTD', 'MTD', 'YTD']
    dic_date = dict(zip([yest, last_week, last_month, last_year], col_ord))
    for b in bonos:
        
        
        sql_bono_a = sql_bono[sql_bono['BondName'] == b]
        
        sql_bono_a.sort_values(by='br_date', ascending=True, inplace = True)
        
        sql_bono_a['type'] = sql_bono_a['br_date'].dt.date.apply(
            lambda d: dic_date[d])
        
        dic_bono_a = dict(zip(sql_bono_a['type'], sql_bono_a['br_rate']))
        
        df_bonosnap_a = pd.DataFrame(dic_bono_a, index=[b])
        
        df_bonosnap = pd.concat([df_bonosnap, df_bonosnap_a])
        
    df_bonosnap = df_bonosnap[col_ord]  
    
    if df_bonosnap.isna().any().any():
        col_mis = df_bonosnap.isna().any()[df_bonosnap.isna().any()].index
        
        col_mis_date = df_bonosnap[col_mis].iloc[0].values
        
        col_good = (df_bonosnap.iloc[0].isin(col_mis_date) &
                    ~df_bonosnap.isna()).all()[
                        (df_bonosnap.iloc[0].isin(col_mis_date) &
                         ~df_bonosnap.isna()).all()].index[0]
        
        for c in col_mis:
            
            df_bonosnap[c] = df_bonosnap[col_good]
        
    snap_sheet.range('J3').value = df_bonosnap.values
    
    # TIIEs
    
    tiies = snap_sheet.range('B37').expand('down').value
    tiie_tup = tuple(tiies)
    
    sql_tiie = pd.read_sql_query("SELECT * FROM [dbo].[Derivatives] "+
                                 f"WHERE dv_date IN ('{yest}', '{last_week}'"+
                                 f", '{last_month}', '{last_year}') "+
                                 "AND dv_stenor= 'Spot' "+
                                 f"AND dv_ftenor IN {tiie_tup}", conn)   
    
    df_tiiesnap = pd.DataFrame({'YTD': [last_year], 'MTD': [last_month], 
                               'WTD': [last_week], '1d': [yest]})

    
    for t in tiies:
        
        sql_tiie_a = sql_tiie[sql_tiie['dv_ftenor'] == t]
        
        sql_tiie_a.sort_values(by='dv_date', ascending=True, inplace = True)
    
        sql_tiie_a['type'] = sql_tiie_a['dv_date'].dt.date.apply(
            lambda d: dic_date[d])
        
        dic_tiie_a = dict(zip(sql_tiie_a['type'], sql_tiie_a['br_rate']*100))
        
        df_tiiesnap_a = pd.DataFrame(dic_tiie_a, index=[t])
        
        df_tiiesnap = pd.concat([df_tiiesnap, df_tiiesnap_a])
    
    df_tiiesnap = df_tiiesnap[col_ord]
    
    snap_sheet.range('J36').value = df_tiiesnap.values
    
    if df_tiiesnap.isna().any().any():
        col_mis = df_tiiesnap.isna().any()[df_tiiesnap.isna().any()].index
        
        col_mis_date = df_tiiesnap[col_mis].iloc[0].values
        
        col_good = (df_tiiesnap.iloc[0].isin(col_mis_date) &
                    ~df_tiiesnap.isna()).all()[
                        (df_tiiesnap.iloc[0].isin(col_mis_date) &
                         ~df_tiiesnap.isna()).all()].index[0]
        
        for c in col_mis:
            
            df_tiiesnap[c] = df_tiiesnap[col_good]
    
    snap_sheet.range('J36').value = df_tiiesnap.values
    
    # UDIBONOS
    
    udis = snap_sheet.range('P4').expand('down').value
    
    sql_udi = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] "+
                                 "INNER JOIN [dbo].[BondData] "+
                                 "ON br_isin = tic_isin "+
                                 f"WHERE br_date IN ('{yest}', '{last_week}'"+
                                 f", '{last_month}', '{last_year}') "+
                                 "AND br_bondType= 'Sbono'", conn)  
    
    sql_udi_copy = sql_udi.copy()
    
    isins = sql_udi['tic_isin'].unique().tolist()
    
    sql_udi['BondName'] = sql_udi['tic_maturityDate'].apply(
        lambda d: months_dic[int(d[4:6])] + d[2:4])
    
    df_udisnap = pd.DataFrame({'YTD': [last_year], 'MTD': [last_month], 
                               'WTD': [last_week], '1d': [yest]})
    
    
    for d in sql_udi['br_date'].unique():
        sql_udi_d = sql_udi[sql_udi['br_date']==d]
        missing_isins = [i for i in isins if i not in 
                         sql_udi_d['br_isin'].unique()]
        n = len(missing_isins)
        isin_df = sql_udi_copy.drop_duplicates(subset='br_isin')
        missing_df = isin_df[isin_df['br_isin'].isin(missing_isins)]
        missing_df['br_date'] = d
        missing_df['br_rate'] = np.nan
        sql_bono_d = pd.concat([sql_udi_d, missing_df])
        
        if sql_udi_d['br_rate'].isna().any():
            
            sql_udi_d['Maturity'] = sql_udi_d['tic_maturityDate'].apply(
                lambda t: datetime.strptime(t, '%Y%m%d'))
            
            sql_udi_d['Days'] = (sql_udi_d['Maturity'] - 
                                 sql_udi_d['br_date']).dt.days
            
            rates = sql_udi_d['br_rate'][
                ~sql_udi_d['br_rate'].isna()].values
            
            days = sql_udi_d['Days'][
                ~sql_udi_d['br_rate'].isna()].values
            
            recta = interp1d(days, rates, fill_value = 'extrapolate')

            sql_udi_d['br_rate'] = np.select([sql_udi_d['br_rate'].isna()],
                                             [sql_udi_d['Days'].apply(
                                                 lambda x: recta(x))], 
                                              sql_udi_d['br_rate'])
            
            sql_udi = sql_udi[sql_udi['br_date']!=d]
            
            sql_udi = pd.concat([sql_udi, sql_udi_d])

    
    for u in udis:
        
        sql_udi_a = sql_udi[sql_udi['BondName'] == u]
        
        sql_udi_a.sort_values(by='br_date', ascending=True, inplace = True)
    
        sql_udi_a['type'] = sql_udi_a['br_date'].dt.date.apply(
            lambda d: dic_date[d])
        
        dic_udi_a = dict(zip(sql_udi_a['type'], sql_udi_a['br_rate']))
        
        df_udisnap_a = pd.DataFrame(dic_udi_a, index=[u])
        
        df_udisnap = pd.concat([df_udisnap, df_udisnap_a])
    
    df_udisnap = df_udisnap[col_ord]
    
    if df_udisnap.isna().any().any():
        col_mis = df_udisnap.isna().any()[df_udisnap.isna().any()].index
        
        col_mis_date = df_udisnap[col_mis].iloc[0].values
        
        col_good = (df_udisnap.iloc[0].isin(col_mis_date) &
                    ~df_udisnap.isna()).all()[
                        (df_udisnap.iloc[0].isin(col_mis_date) &
                         ~df_udisnap.isna()).all()].index[0]
        
        for c in col_mis:
            
            df_udisnap[c] = df_udisnap[col_good]
    
    
    snap_sheet.range('X3').value = df_udisnap.values
    
    snap_sheet.api.Calculate()
    
def pricing_bonds(graph_file, pricing_file, MXN_TIIE, live=False):
    
    months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 6: 'jun', 
                  7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 
                  12: 'dic'}
    
    # Parameters
    wb = xw.Book(graph_file)
    parameters_sheet = wb.sheets('Scenarios')
    cetes_sheet = wb.sheets('ASW_Cetes')
    bonos_sheet = wb.sheets('ASW_Bonos')
    depo_rate = parameters_sheet.range('C5').value/100
    settlementDays = 1
    depo_settle_days = 2
    calendar = ql.Mexico()
    dayCounter = ql.Actual360()

    evaluation_date = wb.sheets('Scenarios').range('C2').value
    evaluation_date_ql = ql.Date().from_date(evaluation_date)
    ql.Settings.instance().evaluationDate = evaluation_date_ql
    yst_date = ql.Mexico().advance(
        ql.Date().from_date(evaluation_date), ql.Period(-1, ql.Days)).to_date()
    yst_date_str = yst_date.strftime('%Y%m%d')
    
    # SQL reading
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')

    bond_data = pd.read_sql_query(
        "SELECT * FROM [dbo].[BondRates] LEFT JOIN [dbo].[BondData] "+\
            f"ON br_isin=tic_isin WHERE br_date='{yst_date}' AND "+\
                "br_bondType='Mbono'", conn)
        
    if live:
        cete_data = cetes_sheet.range('B23').expand('table').options(
            pd.DataFrame, header=True, index=False).value
        cete_data.rename(columns={'CETE': 'tic_maturityDate', 
                                  'Live': 'br_rate'}, inplace=True)
        cete_data.dropna(subset='br_rate', inplace=True)

    else:
        cete_data = pd.read_sql_query(
            "SELECT * FROM [dbo].[BondRates] LEFT JOIN [dbo].[BondData] "+\
                f"ON br_isin=tic_isin WHERE br_date='{yst_date}' AND "+\
                    "br_bondType='CETES'", conn)
    
        cete_data['tic_maturityDate'] = cete_data['tic_maturityDate'].str.strip()\
            .astype(int)
            
    # Data Handling    
    bond_data['Maturity'] = pd.to_datetime(bond_data['tic_maturityDate'],
                                                format='%Y%m%d')
    bond_data['Bond_Name'] = bond_data['Maturity'].apply(
        lambda x: months_dic[x.month]+str(x.year)[-2:])
    
    if live:
    
        live_bonos = bonos_sheet.range('B32').expand('down').options(
            pd.DataFrame, header=True, index=False).value
        live_yields = bonos_sheet.range('C32').expand('down').options(
            pd.DataFrame, header=True, index=False).value
        bonos_live = live_bonos.merge(live_yields, how='left', left_index=True, 
                                      right_index=True)
        bonos_live.rename(columns={'Mbono': 'Bond_Name', 'Live': 'br_rate'}, 
                          inplace=True)
        
        bond_data = bond_data.merge(
            bonos_live, how='left', left_on='Bond_Name', right_on='Bond_Name')
        
        bond_data['br_rate'] = np.select([bond_data['br_rate_y'].isna()],
                                          [bond_data['br_rate_x']],
                                          default = bond_data['br_rate_y'])
    

    bond_data['Price'] = [bono_price_fn(m, y, c, evaluation_date, False)[0] 
                   for (m, y, c) in zip(bond_data['Maturity'], 
                                        bond_data['br_rate'], 
                                        bond_data['tic_coupon'])]
    

    bond_data.sort_values(by='Maturity', inplace=True)
    
    # Zip creation
    bond_zip = [(bond_data.iloc[i]['Price'], 
                 bond_data.iloc[i]['tic_coupon']/100, 
                 bond_data['Maturity'].apply(
                     lambda m: ql.Date().from_date(m)).iloc[i]) 
                for i in range(bond_data.shape[0])]

    cete_zip = [(cete_data['br_rate'][i]/100, 
                 int(cete_data['tic_maturityDate'][i]))
                for i in range(cete_data.shape[0])]

    # Curve helpers creation
        
    depositHelpers = [ql.DepositRateHelper(
        ql.QuoteHandle(ql.SimpleQuote(depo_rate)), ql.Period(1, ql.Days), 1, 
        calendar, ql.Following, False, dayCounter)]
    
    for r, p in cete_zip:
        final_date = evaluation_date_ql + ql.Period(p, ql.Days)
        days = ql.Mexico().businessDaysBetween(evaluation_date_ql, 
                                               final_date)
        helper = ql.DepositRateHelper(ql.QuoteHandle(ql.SimpleQuote(r)),
                                                ql.Period(days, ql.Days),
                                                settlementDays,
                                                calendar,
                                                ql.Following,
                                                False,
                                                dayCounter)
        depositHelpers.append(helper)

    bond_subzip = [(p, c, m) for (p, c, m) in bond_zip 
                   if m.to_date() > depositHelpers[-1].maturityDate().to_date()]
    
    #bond_subzip.extend(missing_bond_zip)

    bondHelpers = []
    couponFreq = ql.Period(26, ql.Weeks)

    for p, c, m in bond_subzip:
        schedule = ql.Schedule(evaluation_date_ql, m,
                               couponFreq, ql.Mexico(), 0, 0, ql.Following, 
                               False)
        
        helper = ql.FixedRateBondHelper(ql.QuoteHandle(ql.SimpleQuote(p)),
                                        settlementDays, 100, schedule, [c],
                                        dayCounter, ql.Following)
        
        bondHelpers.append(helper)
        
    rate_helpers = depositHelpers + bondHelpers
    
    # Curve Creation
    yieldCurve = ql.PiecewiseLogCubicDiscount(0, ql.Mexico(), rate_helpers,
                                              dayCounter)
    
    # Banxico Dates Funding FRAs
    
    banxico_dates = parameters_sheet.range('E29').expand('table').options(
        pd.DataFrame, header=False, index=False).value
    
    banxico_dates[banxico_dates.columns[1]] = banxico_dates[
        banxico_dates.columns[1]].dt.date

    banxico_list = banxico_dates[banxico_dates.columns[0]].tolist()
    # banxico_eff = [ql.Mexico().advance(
    #     ql.Date().from_date(d), ql.Period(1, ql.Days)) for d in banxico_list]
    banxico_eff = banxico_dates[banxico_dates.columns[1]].tolist()

    dates = []
    fondeos = []
    
    for i in range(1, (banxico_eff[-1]-yst_date).days):
        d = evaluation_date_ql + i
        fondeo = yieldCurve.forwardRate(d, d + ql.Period('1D'), 
                                       ql.Actual360(), ql.Simple).rate()
        dates.append(d.to_date())
        fondeos.append(fondeo)
        
    fondeo_df = pd.DataFrame({'Date': dates, 'Rate': fondeos})
    banxico_df = fondeo_df[fondeo_df['Date'].isin(
        [d for d in banxico_eff])]
    # banxico_df['Pricing'] = [(banxico_df.iloc[0]['Rate']-depo_rate)*10000] + \
    #     [(banxico_df.iloc[i+1]['Rate'] - banxico_df.iloc[i]['Rate'])*10000 
    #      for i in range(0, banxico_df.shape[0] - 1)]
    
    # Merge with TIIE 28
    tiie_28 = [MXN_TIIE.forwardRate(ql.Date().from_date(d), 
                                    ql.Date().from_date(d) + ql.Period('28D'), 
                                    ql.Actual360(), ql.Simple).rate() 
               for d in banxico_eff]
    
    tiie_pricing = pd.DataFrame({'Date': banxico_eff, 'TIIE': tiie_28})

    
    # pricing_book = xw.Book(pricing_file)
    # short_end_sheet = pricing_book.sheets('Short_End_Pricing')
    # tiie_pricing = short_end_sheet.range('C3:E3').expand(
    #     'down').options(pd.DataFrame, header=True, index=False).value
    # tiie_pricing.rename(columns = {tiie_pricing.columns[-1]: 'TIIE'}, 
    #                     inplace=True)
    # tiie_pricing['Fix Eff'] = tiie_pricing['Fix Eff'].dt.date
    
    banxico_df = banxico_df.merge(tiie_pricing, how='left', left_on='Date',
                                  right_on='Date')
    
    parameters_sheet.range('U6').value = banxico_df[['Rate']].values
    parameters_sheet.range('W6').value = banxico_df[['TIIE']].values
    
    
    cetes_series = parameters_sheet.range('S28').expand(
        'table').options(pd.DataFrame, header=True, index=False).value
    #cetes_series['Serie'] = cetes_series['Serie'].astype(int).astype(str)
    # cetes_series['Maturity'] = pd.to_datetime(cetes_series['Serie'],
    #                                           format='%y%m%d')
    cetes_series['Rate'] = cetes_series['Maturity'].apply(
        lambda m: yieldCurve.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m), 
                                             ql.Period(1, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    cetes_series['TIIE'] = cetes_series['Maturity'].apply(
        lambda m: MXN_TIIE.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m) + 28, 
                                             ql.Period(0, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    if live:
        parameters_sheet.range('Y29').value = cetes_series[['Rate']].values
    else:
        parameters_sheet.range('U29').value = cetes_series[['Rate']].values
    parameters_sheet.range('W29').value = cetes_series[['TIIE']].values
    
    bonos_series = parameters_sheet.range('S73').expand(
        'table').options(pd.DataFrame, header=True, index=False).value
    
    bonos_series['Rate'] = bonos_series['Maturity'].apply(
        lambda m: yieldCurve.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m), 
                                             ql.Period(1, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    bonos_series['TIIE'] = bonos_series['Maturity'].apply(
        lambda m: MXN_TIIE.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m) + 28, 
                                             ql.Period(0, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    if live:
        parameters_sheet.range('Y74').value = bonos_series[['Rate']].values
    else:
        parameters_sheet.range('U74').value = bonos_series[['Rate']].values
    
    parameters_sheet.range('W74').value = bonos_series[['TIIE']].values
    
    parameters_sheet.api.Calculate()
    bonos_sheet.api.Calculate()
    cetes_sheet.api.Calculate()
    
def pricing_bonds_live(graph_file, cetes_vec, MXN_TIIE):
    
    
    months_dic = {1: 'ene', 2: 'feb', 3: 'mar', 4:'abr', 5: 'may', 6: 'jun', 
                  7: 'jul', 8: 'ago', 9: 'sep', 10: 'oct', 11: 'nov', 
                  12: 'dic'}
    
    # Parameters
    wb = xw.Book(graph_file)
    parameters_sheet = wb.sheets('Scenarios')
    parameters_sheet.api.Calculate()
    cetes_sheet = wb.sheets('ASW_Cetes')
    bono_sheet = wb.sheets('ASW_Bonos')
    depo_rate = parameters_sheet.range('C5').value/100
    settlementDays = 1
    depo_settle_days = 2
    calendar = ql.Mexico()
    dayCounter = ql.Actual360()
    
    # Today and yesterday dates
    evaluation_date = wb.sheets('Scenarios').range('C2').value
    evaluation_date_ql = ql.Date().from_date(evaluation_date)
    ql.Settings.instance().evaluationDate = evaluation_date_ql
    yst_date = ql.Mexico().advance(
        ql.Date().from_date(evaluation_date), ql.Period(-1, ql.Days)).to_date()
    yst_date_str = yst_date.strftime('%Y%m%d')
    spot_date = ql.Mexico().advance(ql.Date().from_date(evaluation_date), 
                                    ql.Period(1, ql.Days)).to_date()
    
    # SQL reading
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    # CETES
    cete_data = pd.read_sql_query(
        "SELECT * FROM [dbo].[BondRates] LEFT JOIN [dbo].[BondData] "+\
            f"ON br_isin=tic_isin WHERE br_date='{yst_date}' AND "+\
                "br_bondType='CETES'", conn)
        
    cete_data['tic_maturityDate'] = cete_data['tic_maturityDate'].str.strip()\
        .astype(int)
    
    # cete_data = cetes_sheet.range('B56').expand('table').options(
    #     pd.DataFrame, header=True, index=False).value
    # cete_data.rename(columns={'Duration': 'tic_maturityDate', 
    #                           'YTM': 'br_rate'}, inplace=True)
    
    
    # Bonds
    bond_data_sql = pd.read_sql_query(
        "SELECT * FROM [dbo].[BondRates] LEFT JOIN [dbo].[BondData] "+\
            f"ON br_isin=tic_isin WHERE br_date='{yst_date}' AND "+\
                "br_bondType='Mbono'", conn)
        
    bond_data_sql['Maturity'] = pd.to_datetime(
        bond_data_sql['tic_maturityDate'], format='%Y%m%d')
     
    bond_data_sql['Bond_Name'] = bond_data_sql['Maturity'].apply(
        lambda x: months_dic[x.month]+str(x.year)[-2:])
    
    # Live Bonds
    # bond_data = bono_sheet.range('B32:C32').expand('down').options(
    #     pd.DataFrame, header=True, index=False).value
    
    # bond_data = bond_data.merge(bond_data_sql[['Maturity', 'Bond_Name', 
    #                                            'tic_coupon']], 
    #                             how='left', left_on='Mbono', 
    #                             right_on='Bond_Name')
    
    bond_data = bond_data_sql.copy()
    bond_data['Price'] = [bono_price_fn(m, y, c, evaluation_date, False)[0] 
                   for (m, y, c) in zip(bond_data['Maturity'], 
                                        bond_data['br_rate'], 
                                        bond_data['tic_coupon'])]
    
    # bond_data['Price'] = [bono_price_fn(m, y, c, evaluation_date, False)[0] 
    #                for (m, y, c) in zip(bond_data['Maturity'], 
    #                                     bond_data['Live'], 
    #                                     bond_data['tic_coupon'])]
    
    bond_data.sort_values(by='Maturity', inplace=True)
    
    # Live CETES
    live_cetes = cetes_sheet.range('C95').expand('down').options(
        pd.DataFrame, header=True, index=False).value
    live_yields = cetes_sheet.range('E95').expand('down').options(
        pd.DataFrame, header=True, index=False).value
    live_cetes = live_cetes.merge(live_yields, how='left', left_index=True, 
                                  right_index=True)
    live_cetes['Maturity'] = pd.to_datetime(live_cetes['Serie'], 
                                            format='%y%m%d')
    live_cetes['tic_maturityDate'] = live_cetes['Maturity'].apply(
        lambda m: (m - pd.to_datetime(spot_date)).days)
    live_cetes.rename(columns={'YTM': 'br_rate'}, inplace = True)
    
    # Live CETES and close CETES unification
    cete_data = pd.concat([cete_data, live_cetes]).reset_index(drop=True)
    cete_data.drop_duplicates(subset='tic_maturityDate', keep='last', 
                              inplace=True)
    cete_data = cete_data[cete_data['tic_maturityDate'] > 2].reset_index(
        drop=True)
    
    # Curve inputs
    cete_zip = [(cete_data['br_rate'][i]/100, 
                 int(cete_data['tic_maturityDate'][i]))
                for i in range(cete_data.shape[0])]
    
    if cetes_vec.empty:
        vector_file = f'//minerva/Applic/finamex/gbs/vector_precios_{yst_date_str}.csv'
        vector_precio = pd.read_csv(vector_file)
        cetes_vec = vector_precio[vector_precio['TV']=='BI'].copy()
    
    liquidez = pd.read_excel('//tlaloc/gabreu/CETES_Liquidez.xlsx', 
                             'Historical_data')

    volumen = pd.pivot_table(liquidez, values='Compras más Ventas', index='Día', 
                             columns='Emisión', aggfunc='sum')

    titulos = pd.pivot_table(liquidez, values='Número de Títulos en Circulación', 
                             index='Día', columns='Emisión', aggfunc='mean')
    titulos.replace(0, np.nan, inplace=True)

    percentage_table = volumen/titulos
    #percentage_table.replace(np.inf, 0, inplace=True)

    mean_volume = pd.DataFrame(percentage_table.mean())
    mean_volume.rename(columns={mean_volume.columns[0]: 'Volume'}, inplace=True)
    mean_volume.insert(0, 'Serie', [b[-6:] for b in mean_volume.index])

    good_cetes = mean_volume[mean_volume['Volume']>.05].copy()
    #cetes_vec = cetes_vec[cetes_vec['Serie'].isin(good_cetes['Serie'])]
    
    
    cete_zip = standard_cetes_live(cetes_vec, live_cetes, evaluation_date)
    
    bond_zip = [(bond_data.iloc[i]['Price'], 
                 bond_data.iloc[i]['tic_coupon']/100, 
                 bond_data['Maturity'].apply(
                     lambda m: ql.Date().from_date(m)).iloc[i]) 
                for i in range(bond_data.shape[0])]

    # Curve helpers creation
    depositHelpers = [ql.DepositRateHelper(
        ql.QuoteHandle(ql.SimpleQuote(depo_rate)), ql.Period(1, ql.Days), 1, 
        calendar, ql.Following, False, dayCounter)]
    
    settle_date = ql.Mexico().advance(evaluation_date_ql, 
                                      ql.Period(1, ql.Days))
    
    # CETES Helpers
    settlementDays = 1
    for r, p in cete_zip:
        
        final_date = ql.Mexico().advance(settle_date + ql.Period(p, ql.Days), 
                                         ql.Period('0D'))
        days = ql.Mexico().businessDaysBetween(settle_date, 
                                               final_date)
        helper = ql.DepositRateHelper(ql.QuoteHandle(ql.SimpleQuote(r)),
                                                ql.Period(days, ql.Days),
                                                settlementDays,
                                                calendar,
                                                ql.Following,
                                                False,
                                                dayCounter)
        depositHelpers.append(helper)
    
    # Bond Helpers
    bond_subzip = [(p, c, m) for (p, c, m) in bond_zip 
                   if m.to_date() > depositHelpers[-1].maturityDate().to_date()]

    bondHelpers = []
    couponFreq = ql.Period(26, ql.Weeks)

    for p, c, m in bond_subzip:
        schedule = ql.Schedule(evaluation_date_ql, m,
                               couponFreq, ql.Mexico(), 0, 0, ql.Following, 
                               False)
        
        helper = ql.FixedRateBondHelper(ql.QuoteHandle(ql.SimpleQuote(p)),
                                        settlementDays, 100, schedule, [c],
                                        dayCounter, ql.Following)
        
        bondHelpers.append(helper)
        
    rate_helpers = depositHelpers + bondHelpers
    
    # Curve Creation
    yieldCurve = ql.PiecewiseLogCubicDiscount(0, ql.Mexico(), rate_helpers,
                                              dayCounter)
    
    # Banxico Dates Funding FRAs
    banxico_dates = parameters_sheet.range('E29').expand('table').options(
        pd.DataFrame, header=False, index=False).value
    
    banxico_dates[banxico_dates.columns[1]] = banxico_dates[
        banxico_dates.columns[1]].dt.date
    banxico_eff = banxico_dates[banxico_dates.columns[1]].tolist()

    dates = []
    fondeos = []
    
    for i in range(1, (banxico_eff[-1] - yst_date).days):
        d = evaluation_date_ql + i
        fondeo = yieldCurve.forwardRate(d, d + ql.Period('1D'), 
                                       ql.Actual360(), ql.Simple).rate()
        dates.append(d.to_date())
        fondeos.append(fondeo)
        
    fondeo_df = pd.DataFrame({'Date': dates, 'Rate': fondeos})
    banxico_df = fondeo_df[fondeo_df['Date'].isin(
        [d for d in banxico_eff])]
    
    # Merge with TIIE 28
    tiie_28 = [MXN_TIIE.forwardRate(ql.Date().from_date(d), 
                                    ql.Date().from_date(d) + ql.Period('28D'), 
                                    ql.Actual360(), ql.Simple).rate() 
               for d in banxico_eff]
    
    tiie_pricing = pd.DataFrame({'Date': banxico_eff, 'TIIE': tiie_28})

    banxico_df = banxico_df.merge(tiie_pricing, how='left', left_on='Date',
                                  right_on='Date')
    
    # Copy values in Excel file
    parameters_sheet.range('T6').value = banxico_df[['Rate']].values
    parameters_sheet.range('W6').value = banxico_df[['TIIE']].values
    
    
    cetes_series = parameters_sheet.range('S28').expand(
        'down').options(pd.DataFrame, header=True, index=False).value
    #cetes_series['Serie'] = cetes_series['Serie'].astype(int).astype(str)
    # cetes_series['Maturity'] = pd.to_datetime(cetes_series['Serie'],
    #                                           format='%y%m%d')
    cetes_series['Rate'] = cetes_series['Maturity'].apply(
        lambda m: yieldCurve.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m), 
                                             ql.Period(1, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    cetes_series['TIIE'] = cetes_series['Maturity'].apply(
        lambda m: MXN_TIIE.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m) + 28, 
                                             ql.Period(0, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    parameters_sheet.range('T29').value = cetes_series[['Rate']].values
    parameters_sheet.range('W29').value = cetes_series[['TIIE']].values
    
    bonos_series = parameters_sheet.range('S73').expand(
        'down').options(pd.DataFrame, header=True, index=False).value
    
    bonos_series['Rate'] = bonos_series['Maturity'].apply(
        lambda m: yieldCurve.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m), 
                                             ql.Period(1, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    bonos_series['TIIE'] = bonos_series['Maturity'].apply(
        lambda m: MXN_TIIE.forwardRate(ql.Date().from_date(m), 
                                         ql.Mexico().advance(
                                             ql.Date().from_date(m) + 28, 
                                             ql.Period(0, ql.Days)), 
                                         ql.Actual360(), ql.Simple).rate())
    
    parameters_sheet.range('T74').value = bonos_series[['Rate']].values
    parameters_sheet.range('W74').value = bonos_series[['TIIE']].values
    
    parameters_sheet.api.Calculate()
    cetes_sheet.api.Calculate()
    bono_sheet.api.Calculate()
    
                
def copy_dic_data(evaluation_date, str_file):
    
    new_dic_data = pd.read_pickle('//tlaloc/Cuantitativa/Fixed Income/TIIE '
                                  'IRS Valuation Tool/Main Codes/Quant '
                                  'Management/Pricing/dic_data/dic_data_'
                                  f'{evaluation_date.strftime("%Y%m%d")}'+
                                  '.pickle')
    wb = xw.Book(str_file)
    for k in new_dic_data.keys():
        if k != 'Granular_Tenors':
            sheet = wb.sheets(k)
            sheet.range('E2').value = new_dic_data[k][['Quotes']].values
            
            if k == 'MXN_TIIE':
                sheet.range('L2').value = new_dic_data[k][[
                    'FMX Desk BID']].values
                sheet.range('M2').value = new_dic_data[k][[
                    'FMX Desk OFFER']].values
                
    path = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
        'Tool/Quant Team/Esteban y Gaby/Historical Curves/'
    new_dic_data['USD_OIS'].to_excel(path + 'CurveInputs_'+
                                     f'{evaluation_date.strftime("%Y%m%d")}'+
                                     '.xlsx')
    
    for k in new_dic_data.keys():
        if k != 'Granular_Tenors' and k != 'USD_OIS':
            with pd.ExcelWriter(
                    path + f'CurveInputs_{evaluation_date.strftime("%Y%m%d")}'+
                    '.xlsx', 
                    engine="openpyxl", mode='a') as writer:  
                new_dic_data[k].to_excel(writer, sheet_name=k, index=False)
    
    return new_dic_data

def interp(time_series, instrument, stenor, ftenor):
    
    conn = pyodbc.connect('Driver={SQL Server};'
                           'Server=Donaji;'
                           'Database=Historical_FI;'
                           'Trusted_Connection=yes;')
    
    months_dic = {'01': 'Ene', '02': 'Feb', '03': 'Mar', '04': 'Abr', '05': 'May', 
                  '06': 'Jun', '07': 'Jul', '08': 'Ago', '09': 'Sep', '10': 'Oct', 
                  '11': 'Nov', '12': 'Dic'}

    rev_months = {v:k for k, v in months_dic.items()} 
    
    a = datetime.now()
    # first_date = time_series[~time_series.isna()].index[0]
    dates = list(time_series[time_series.isna()].index)
    
    if len(dates)>0:
        print(f'Rates from {dates[0].date()} to {dates[-1].date()}'+
              f' for {instrument[0]} {ftenor[0]} will be interpolated.')
    
    all_bonds = pd.read_sql_query("SELECT * FROM [dbo].[BondData]", conn)
    for d in dates:
        str_d = d.strftime('%Y-%m-%d')
        if instrument[0] == 'Bono':
            
            
            sql_data = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] "+
                                         "INNER JOIN [dbo].[BondData] ON "+
                                         "br_isin = tic_isin WHERE "+
                                         f"br_date = '{str_d}' AND "+
                                         "br_bondType = 'Mbono'", conn)
            
        elif instrument[0] == 'Udi':
            sql_data = pd.read_sql_query("SELECT * FROM [dbo].[BondRates] "+
                                         "INNER JOIN [dbo].[BondData] ON "+
                                         "br_isin = tic_isin WHERE "+
                                         f"br_date = '{str_d}' AND "+
                                         "br_bondType = 'Sbono'", conn)
            
        
        else:
            break
        
        try:
            maturity = pd.to_datetime(
                sql_data[sql_data['tic_maturityDate'].str[:6] == 
                         '20' + ftenor[0][3:] + 
                         rev_months[ftenor[0][:3]]]['tic_maturityDate'].iloc[0], 
                format='%Y%m%d')
        except:
            missing_bond = all_bonds[
                all_bonds['tic_maturityDate'].str[:6]=='20' + ftenor[0][3:] \
                    + rev_months[ftenor[0][:3]]].reset_index(drop=True)
            missing_bond.at[0, 'br_date'] = d
            missing_bond.at[0, 'br_rate'] = np.nan
            sql_data = pd.concat([sql_data, missing_bond], ignore_index=True)
            maturity = pd.to_datetime(
                sql_data[sql_data['tic_maturityDate'].str[:6] == 
                         '20' + ftenor[0][3:] + 
                         rev_months[ftenor[0][:3]]]['tic_maturityDate'].iloc[0], 
                format='%Y%m%d')
        
        sql_datana = sql_data[~sql_data['br_rate'].isna()]
        
        sql_datana['Maturity'] = pd.to_datetime(
            sql_datana['tic_maturityDate'], format='%Y%m%d')
        
        sql_datana['days'] = (sql_datana['Maturity'] - 
                              sql_datana['br_date']).dt.days
        
        recta = interp1d(sql_datana['days'], sql_datana['br_rate'])
        
        days = (maturity-d).days
        time_series[d] = recta(days)/100
            
    
    return time_series 
# if __name__ == '__main__':
#     wb = xw.Book('TIIE_IRS_Data.xlsm')
    
    
