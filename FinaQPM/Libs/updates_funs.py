# -*- coding: utf-8 -*-
""" Portfolio_code functions

Functions necesssary to run the portfolio main code
"""
# Python Libraries
import os
import sys
import pyodbc
import requests
import openpyxl
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import numpy as np
import pandas as pd
import xlwings as xw
import QuantLib as ql
from datetime import date as dt
from datetime import datetime, timedelta
from scipy.interpolate import CubicSpline, interp1d

# Quant Team Libraries
import new_pfolio_funs as pf
import curve_funs as cf

#---------------------------
#        Main Updates
#---------------------------


def cubic_spline(date: datetime, curve: str) -> pd.DataFrame:
    """ Cubic Spline
    Creates Cubic Spline curve

    Parameters
    ----------
    date : datetime
        Date of curve
    curve : str
        Type of curve
        
    Returns
    -------
    df : pd.DataFrame
        DataFrame with the interpolated curve 

    """
    str_date = date.strftime('%Y%m%d')
    # Curve inputs
    if curve == 'TIIE':
        file = r'//tlaloc/Cuantitativa/Fixed Income/File Dump/'+\
            'Historical OIS TIIE/IRSDFR_TIIE28D_'+ str_date +'.csv'
         
    elif curve == 'OIS':
        file = r'//tlaloc/Cuantitativa/Fixed Income/File Dump/'+\
            'Historical OIS TIIE/IRSDFR_TIIE_'+ str_date +'.csv'
        
    try:
        data = pd.read_csv(file, index_col = 0)
    except:
        print(f'File for {curve} not found.')
        return None
    
    # Interpolation
    points = data.values
    cs = CubicSpline(points[1:, 0], points[1:, 1], bc_type='natural')
    p = np.linspace(1, 16310, 16310)
    disc_fact = cs(p)
    yields = (1/disc_fact-1)*36000/p
    
    df = pd.DataFrame({'VALOR': p, 'PLAZO': yields})
    df.insert(0, 'FECHA', date.strftime('%d/%m/%Y'))
    
    path = r'//tlaloc/Cuantitativa/Fixed Income/File Dump/'+\
            'Historical OIS TIIE/'
    df.to_excel(path + f'\{curve}_{str_date}.xlsx', index=False)
    print(f'{curve} saved successfully.')
    
    return df

def db_cme(date: datetime) -> None:
    """Updates db_cme file
    

    Parameters
    ----------
    date : datetime
        date for updating

    Raises
    ------
    Exception
        If the column exists already

    Returns
    -------
    Saves the db_cme file
        

    """
    
    str_date = date.strftime('%Y%m%d')
    file = r'\\TLALOC\tiie\Historical_MXN_Curves\IRS_MXN_CURVE_'+str_date+'.csv'
    
    try:
        cme_close = pd.read_csv(file, header=None)
    except:
        print('File for CME not found.')
        return None
    
    wb = xw.Book(r'\\TLALOC\tiie\db_cme.xlsx')
    db_sheet = wb.sheets('db')
    last_date = db_sheet.range('A1').end('right').value
    columns = db_sheet.range('A1').expand('right').value
    
    if len(columns) > len(set(columns)):
        raise Exception('DUPLICATE COLUMNS FOUND IN db_cme FILE,'+
                        ' PLEASE MAKE SURE ALL DATES ARE UNIQUE')
                          
    closes = cme_close[cme_close.columns[1]].values
    
    if last_date == date:
        last_col = db_sheet.range('A1').end('right').column
        db_sheet.range(2, last_col).value = closes.reshape(-1, 1)
        tmrw = ql.Mexico().advance(
            ql.Date().from_date(date), ql.Period(1, ql.Days)).to_date()
        db_sheet.range(1, last_col+1).value = tmrw
        db_sheet.range(2, last_col+1).value = closes.reshape(-1, 1)
        
    elif date in columns:
        pass
    
    else:
        last_col = db_sheet.range('A1').end('right').column + 1
        db_sheet.range(1, last_col).value = date
        db_sheet.range(2, last_col).value = closes.reshape(-1, 1)
        tmrw = ql.Mexico().advance(
            ql.Date().from_date(date), ql.Period(1, ql.Days)).to_date()
        db_sheet.range(1, last_col+1).value = tmrw
        db_sheet.range(2, last_col+1).value = closes.reshape(-1, 1)
        
    wb.save()
    wb.close()
    print('db_cme file saved successfully.')

def daily_update(start_date: datetime) -> None:
    """ Daily Update
    
    Calculates Fair Rates in the daily Update file 

    Parameters
    ----------
    start_date : datetime
        Date for calculation 

    Returns
    -------
    File with the updated info

    """
    
    tenors = np.linspace(1,390,390).astype(int).tolist()
    days_list = (np.linspace(1,390,390)*28).astype(int).tolist()
    path_wdir = '\\\\tlaloc\\cuantitativa\\Fixed Income\\'
    
    year = str(start_date.year)

    daily_file_name = r'\\TLALOC\Cuantitativa\Fixed Income'+\
        '\IRS Tenors Daily Update\Daily_Update_' + year +'.xlsx'
    daily_file = pd.ExcelFile(daily_file_name)
    sheet = start_date.strftime('%d-%m-%Y')
    if sheet in daily_file.sheet_names:
        print(f'Daily Update file for {sheet} already calculated.')
        return None
    
   
    ql.Settings.instance().evaluationDate = ql.Date.from_date(start_date)
    
    # Read OIS and TIIE files
    try:
        df_OIS = pd.read_excel(path_wdir + 'Historical OIS TIIE/OIS_'+
                               str(start_date.year) + 
                               str('%02d' % start_date.month) + 
                               str('%02d' % start_date.day) + '.xlsx')
        check_OIS = True 
    except:
        print('Missing file: OIS_'+str(start_date.year) +
              str('%02d' % start_date.month) +
              str('%02d' % start_date.day) + '.xlsx')
        check_OIS = False 
    try:
        df_TIIE = pd.read_excel(path_wdir + 'Historical OIS TIIE/TIIE_' + 
                                str(start_date.year) + 
                                str('%02d' % start_date.month) + 
                                str('%02d' % start_date.day) + '.xlsx')
        check_TTIE = True
    except:
        print('Missing file: TIIE_'+ str(start_date.year) + 
              str('%02d' % start_date.month) + 
              str('%02d' % start_date.day) + '.xlsx')
        check_TTIE = False
        
    if  check_OIS == False or check_TTIE == False:
        pass
        
    period_file = min(len(df_OIS), len(df_TIIE), 11650)
    
    #Dates list creation
    
    effective_date = ql.Date(start_date.day, start_date.month, start_date.year)
    period = ql.Period(period_file -1, ql.Days)
    termination_date = effective_date + period
    tenor = ql.Period(ql.Daily)
    calendar = ql.Mexico()
    business_convention = ql.Unadjusted
    termination_business_convention = ql.Following
    date_generation = ql.DateGeneration.Forward
    end_of_month = True
    
    schedule = ql.Schedule(effective_date,
    termination_date,tenor,
    calendar,
    business_convention,
    termination_business_convention,
    date_generation,
    end_of_month)
    
    dates = []
    for i, d in enumerate(schedule):
        dates.append(d)
    
    #QauntLib curves (OIS, TIIE) creation
     
    lstOIS_dfs = [1]
    
    for i in range(0, min(df_OIS.shape[0]-1,11649)):
        t,r = df_OIS.iloc[i,[1,2]]
        lstOIS_dfs.append(1/(1+r*t/36000)) 
    
    crvOIS = ql.DiscountCurve(dates, lstOIS_dfs, ql.Actual360(), calendar)
    
    lstTIIE_dfs = [1]
    
    for i in range(0, min(df_TIIE.shape[0]-1,11649)):
        t,r = df_TIIE.iloc[i,[1,2]]
        lstTIIE_dfs.append(1/(1+r*t/36000))
    
    crvTIIE = ql.DiscountCurve(dates, lstTIIE_dfs, ql.Actual360(), calendar)
    
    # Build term structures for discount and forecast
    
    discountTermStructure = ql.RelinkableYieldTermStructureHandle(crvOIS)
    forecastTermStructure = ql.RelinkableYieldTermStructureHandle(crvTIIE)
    
    def FairRates(start_date: datetime, t1: bool) -> (list, list):
        """ Fair Rate 
        Fair Rate and Dv01 Calculation

        Parameters
        ----------
        start_date : datetime
            date of swap
        t1 : bool
            If True, next day will be used

        Returns
        -------
        list_fair_rates : list
        list_dv01 : list

        """
        # Fair Rates
        calendar = ql.Mexico()
        start_date_q=ql.Date(start_date.day, start_date.month ,start_date.year)
        
        # Fair Rates
        if t1:
            start_date_t1 = calendar.advance(start_date_q, ql.Period('1D'))
        else:
            start_date_t1 = start_date_q
        settlement_date = start_date_t1 
        
    
        list_fair_rates = []
        list_dv01 = []
        for day in days_list:
            
            
            'Matuirity'
            maturity_date = settlement_date + ql.Period(day,ql.Days)
            
            'Define fixed leg coupons'
            fixed_leg_tenor = ql.Period(28, ql.Days)
            
            'Creating the schedule of the fixed leg of the swap'
            fixed_schedule = ql.Schedule(settlement_date, maturity_date, 
                                          fixed_leg_tenor, calendar,
                                          ql.Following, ql.Following,
                                          ql.DateGeneration.Backward, False)
                                          
                                          
            
            'Define float leg coupons'
            float_leg_tenor = ql.Period(28, ql.Days)
            
            'Creating the schedule of the variable leg of the swap'
            float_schedule = ql.Schedule(settlement_date, maturity_date, 
                                          float_leg_tenor, calendar,
                                          ql.Following, ql.Following,
                                          ql.DateGeneration.Backward, False)
                                          
                                          
            
            'Notional'
            notional = 10000000
            
            'Fixed rate'
            fixed_rate = 0.05
            fixed_leg_daycount = ql.Actual360()
            
            'In case you want to add a spread'
            float_spread = 0.0
            float_leg_daycount = ql.Actual360()
            
            TIIE28D_index = ql.IborIndex('TIIE', ql.Period('28d'), 1, 
                                         ql.MXNCurrency(), ql.Mexico(), 
                                         ql.Following, True, ql.Actual360(),
                                         forecastTermStructure)
            
            'Swap structure'
            ir_swap_TIIE = ql.VanillaSwap(ql.VanillaSwap.Receiver , notional, 
                                          fixed_schedule, fixed_rate, 
                                          fixed_leg_daycount, float_schedule,
                                          TIIE28D_index, float_spread, 
                                          float_leg_daycount )
                
            swap_engine = ql.DiscountingSwapEngine(discountTermStructure)
            
            ir_swap_TIIE.setPricingEngine(swap_engine) 
            try:
                r = ir_swap_TIIE.fairRate()*100
    
            except:
                r = np.nan
                
            # DV01 Calc
            ir_swap_TIIE1 = ql.VanillaSwap(ql.VanillaSwap.Receiver , notional, 
                                           fixed_schedule, r + 0.0001, 
                                           fixed_leg_daycount, float_schedule,
                                           TIIE28D_index, float_spread, 
                                           float_leg_daycount)
                                            
                                            
            ir_swap_TIIE2 = ql.VanillaSwap(ql.VanillaSwap.Receiver , notional, 
                                           fixed_schedule, r - 0.0001, 
                                           fixed_leg_daycount, float_schedule,
                                           TIIE28D_index, float_spread, 
                                           float_leg_daycount)
            
            ir_swap_TIIE1.setPricingEngine(swap_engine) 
            ir_swap_TIIE2.setPricingEngine(swap_engine) 
            
            npv1 = ir_swap_TIIE1.NPV()
            npv2 = ir_swap_TIIE2.NPV()

            list_fair_rates.append(r)
            list_dv01.append(abs((npv1-npv2)/2))
            
        return list_fair_rates, list_dv01
    
    
    
    eval_dates=[effective_date]
    for i in range(4):
        ini_date = eval_dates[i]
        ini_date = ql.IMM.nextDate(ini_date+ql.Period(1,ql.Days))
        eval_dates.append(ini_date)
    
    tenor_label={0:'Spot', 1:'IMM 1', 2: 'IMM_2',3: 'IMM_3',4: 'IMM_4',}
    
    fair_rates = {}
    for i in range(len(eval_dates)):
        fair_rates[tenor_label[i]]=[str(k)+" x 1" for k in tenors]
        if i == 0:
            rates, dvo1s = FairRates(
                datetime(eval_dates[i].year(), eval_dates[i].month(), 
                         eval_dates[i].dayOfMonth()), True)
            fair_rates[eval_dates[i]]=rates
            fair_rates['DV01_'+str(i)]=dvo1s
        else:
            rates, dvo1s = FairRates(
                datetime(eval_dates[i].year(), eval_dates[i].month(),
                         eval_dates[i].dayOfMonth()), False)
                                              
                                            
            fair_rates[eval_dates[i]]=rates
            fair_rates['DV01_'+str(i)]=dvo1s
        fair_rates['space_'+str(i)]=['']*len(fair_rates[eval_dates[i]])
            
    fair_rates_df=pd.DataFrame(fair_rates)   
    
    fair_rates_df.to_excel(r'\\TLALOC\Cuantitativa\Fixed Income'+
                           '\IRS Tenors Daily Update\prueba1.xlsx',index=False) 
    
    with pd.ExcelWriter(r'\\TLALOC\Cuantitativa\Fixed Income'+
                        '\IRS Tenors Daily Update\Daily_Update_' + year +
                        '.xlsx', engine="openpyxl", mode='a') as writer:  
        fair_rates_df.to_excel(writer, 
                               sheet_name=start_date.strftime('%d-%m-%Y'), 
                               index=False)

    wb = openpyxl.load_workbook(r'\\TLALOC\Cuantitativa\Fixed Income'+
                                '\IRS Tenors Daily Update\Daily_Update_' + 
                                year + '.xlsx')
    sheet = wb[start_date.strftime('%d-%m-%Y')]
    
    sheet['D1']=None
    sheet['H1']=None
    sheet['L1']=None
    sheet['P1']=None
    sheet['T1']=None
    
    
    wb.save(r'\\TLALOC\Cuantitativa\Fixed Income\IRS Tenors Daily Update'+
            '\Daily_Update_' + year + '.xlsx') 
    wb.close()
    print('Daily Update file saved successfully.')

def prepare_blotter(blotter: pd.DataFrame) -> pd.DataFrame:
    """Prepare Blotter
    
    Parameters
    ----------
    blotter : pd.DataFrame
        Blotter DataFrame

    Returns
    -------
    blotter : pd.DataFrame
        Prepared Blotter

    """
    
    blotter.rename(columns={'Yield(Spot)': 'Yield', 
                            'Fecha Inicio': 'Start_Date',
                            'Fecha vencimiento': 'End_Date',
                            'Folio JAM': 'Folio'}, inplace=True)
    blotter = blotter.dropna(subset='Yield')
    blotter['Emission'] = 'SWAP'
    blotter['Comment'] = np.nan
    
    return blotter

def check_blotter(file: str, nan_cols: list, cols: list, cols_dic: dict,
                  blotter_name: str) -> pd.DataFrame:
    """Checks desired blotter
    

    Parameters
    ----------
    file : str
        File directory.
    nan_cols : list
        Columns with nan.
    cols : list
        Blotter columns.
    cols_dic : dict
        Dictionary to rename columns.
    blotter_name : str
        Type of blotter to check

    Returns
    -------
    blotter : pd.DataFrame
        DataFrame with checked blotter

    """
    # Try to read blotter 
    try:
        if blotter_name == 'TIIE' or blotter_name == 'CME':
            try:
                blotter = pd.read_excel(file, skiprows=2)
            except:
                blotter_book = xw.Book(file)
                blotter_book.save()
                blotter_book.close()
                blotter = pd.read_excel(file, skiprows=2)
        else:
            blotter = pd.read_excel(file)
        blotter_flag = False
    
    except:
        print(f'NO BLOTTER {blotter_name.upper()} FOUND'.center(52, '-'))
        y = input('Continue? (Y/N): ')
        
        bl = 'z'
        if y.lower() == 'n':
            while bl.lower() != 'c':
                bl = input(f'Please save Blotter {blotter_name} and press "c" '+
                           'to continue: ')
            
            blotter = pd.read_excel(file)
            blotter_flag = False
        
        else:
            blotter_flag = True
            blotter = pd.DataFrame(columns=cols)
    
    if not blotter_flag:
        if blotter_name == 'Mesa':
            blotter['USUARIO'] = blotter['USUARIO'].str.replace(
                'U', '').astype(int)
        
        blotter = blotter.rename(columns=cols_dic)
        blotter = blotter.dropna(subset='Yield')
        
        for c in nan_cols:
            blotter[c] = np.nan
        
        if blotter_name == 'FX':
            blotter['Emission'] = 'FX'
        
        elif blotter_name == 'TIIE' or blotter_name == 'CME':
            blotter['Emission'] = 'SWAP'
        
        blotter['Blotter'] = blotter_name
    
    return blotter

def create_super_blotter(date: datetime) -> bool:
    """Create Super Blotter
    
    Creates super blotter and asks user for new spread strategies

    Parameters
    ----------
    date : datetime
        Date of super blotter

    Returns
    -------
    bool
        If True, new spread is needed.

    """
    
    str_tiie = date.strftime('%y%m%d')
    str_cme = date.strftime('%Y%m%d')
    path = '//tlaloc/tiie/Blotters/'
    
    cols = ['Book', 'Emission', 'Tenor', 'Folio', 'Folio Original', 'Size', 
            'Yield', 'Start_Date', 'End_Date', 'Comment', 'Blotter']
    
    tiie = (f'{path}{str_tiie}.xlsx',
            ['Comment'], cols, 
            {'Yield(Spot)': 'Yield', 'Fecha Inicio': 'Start_Date',
             'Fecha vencimiento': 'End_Date', 
             'Folio JAM': 'Folio'}, 'TIIE')
    
    tiie_cme = (f'{path}blotter_tiie_cme_{str_cme}.xlsx', ['Comment'],
                cols, {'Yield(Spot)': 'Yield', 'Fecha Inicio': 'Start_Date',
                 'Fecha vencimiento': 'End_Date', 'Folio JAM': 'Folio'},
                'CME')
    
    bonos = (f'{path}Blotter_mesa_{str_tiie}.xlsx', 
             ['Folio', 'Folio Original', 'Start_Date', 'End_Date', 'Comment'],
             cols, {'USUARIO': 'Book', 'SERIE': 'Tenor', 'EMISORA': 'Emission', 
                    'TASA': 'Yield','TITULOS': 'Size'}, 'Mesa')
    
    fx = (f'{path}Blotter Carga FX {str_tiie}.xlsx',
          ['Tenor', 'Folio', 'Folio Original', 'Start_Date', 'End_Date',
           'Comment'], cols, {'Usuario': 'Book', 'Monto': 'Size', 
                              'Precio':'Yield'}, 'FX')
    
    citi = (f'{path}CITI_{str_cme}.xlsx',
            ['Folio', 'Folio Original', 'Comment'], cols,
            {'Unnamed: 0': 'Book', 'Contract': 'Emission', 'Delivery': 'Tenor',
            'Lots': 'Size', 'Trade Price': 'Yield'}, 'Citi')
    
    blotters = [tiie, tiie_cme, bonos, fx, citi]
    blotters_list = []
    
    # Read blotters
    for b in blotters:
        blotter = check_blotter(b[0], b[1], b[2], b[3], b[4])
        blotters_list.append(blotter[cols])
    
    super_blotter = pd.concat(blotters_list)
    
    super_blotter_book = xw.Book()
    str_hoja1_name = super_blotter_book.sheets[0].name
    super_blotter_sheet = super_blotter_book.sheets[str_hoja1_name]
    
    super_blotter_sheet.range('A1').options(
        pd.DataFrame, header=1, index=False, 
        expand='table').value = super_blotter
    
    # Filter by 8085
    blotter_8085 = super_blotter[super_blotter['Book']==8085]
    new_spread = False
    
    if blotter_8085.shape[0] != 0:
        super_blotter_sheet.activate()
        super_blotter_sheet.api.Range('A:K').AutoFilter(
            Field=1, Criteria1=8085)
        
        
        month_dict = {1: 'ENE', 2: 'FEB', 3: 'MAR', 4: 'ABR', 5: 'MAY', 
                      6: 'JUN', 7: 'JUL', 8: 'AGO', 9: 'SEP', 10: 'OCT', 
                      11: 'NOV', 12: 'DIC'}
        
        catalogue_file = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income'+
                                       '/IRS Tenors Daily Update/SPREADS'+
                                       '/Spreads Code/Spreads_Catalogue.xlsx',
                                      'Catalogue_' + str(date.year))
                
        spreads_rows = catalogue_file[catalogue_file['Strategy'].str.contains(
            'Spread')].copy()
        
        try:
            spreads_nums = spreads_rows['Strategy'].apply(
                lambda x: int(x.split('_')[-1])).tolist()
            
            spreads_nums.sort()
            next_spread = spreads_nums[-1] + 1
        
        except:
            next_spread = False
        
        catalogue = catalogue_file[catalogue_file[
            'Dead or Alive'].str.upper()=='A'].copy()
        
        catalogue = catalogue.drop(
            columns=['Mercado', 'Dead or Alive']).sort_values(
            by=['Strategy', 'Blotter Name'])
                
        print(catalogue.to_markdown(index=False))
        
        print("\nPlease fill the comment column with the corresponding "+
              "strategies' name located in Blotter Name column "+
              "(only for new trades and/or bonos).")
        
        if next_spread:
            print(f'\nFor a new spread please use the number {next_spread}.')
        
        done = 'z'
        
        while done.lower() != 'c' and done.lower() != 's':
            
            done = input('\nIf you need to update the spreads catalogue '+
                         'please press "s". '+
                         'Otherwise, press "c": ')
            
            if done.lower() == 's':
                
                new_spread = True
                print('\nPlease add the new spread in the catalogue file.')
                
                catalogue_book = xw.Book('//tlaloc/Cuantitativa/Fixed '+
                                         'Income/IRS Tenors Daily Update/'+
                                         'SPREADS/Spreads Code/'+
                                         'Spreads_Catalogue.xlsx')
                
                catalogue_sheet = catalogue_book.sheets(
                    'Catalogue_' + str(date.year))
                
                existing_names = catalogue_sheet.range(
                    'E2:E'+str(catalogue_sheet.range('E2').end('down').row))
                
                catalogue_sheet.activate()
                done = input('\nWhen done filling the catalogue and the '+
                             'blotter press "c": ')
                
                existing_names = catalogue_sheet.range(
                    'E2:E'+str(catalogue_sheet.range('E2').end('down').row))
                
                while len(existing_names) > len(set(existing_names)):
                    print('Duplicate names found. Make sure you use new '+
                          'names for new spreads.')
                    done = input('\nWhen done filling the catalogue and the '+
                                 'blotter press "c": ')
                    existing_names = catalogue_sheet.range(
                        'E2:E'+str(catalogue_sheet.range('E2').end('down').row))
                
                catalogue_book.save()
                catalogue_book.close()
                
                super_blotter_sheet.activate()
                
                super_blotter_sheet.api.AutoFilterMode = False
                
                super_blotter_book.save('//tlaloc/Cuantitativa/Fixed '+
                                        'Income/IRS Tenors Daily Update/'+
                                        'SPREADS/Spreads Code/Blotters/'+
                                        f'Super_blotter_{str_tiie}.xlsx')
            
            elif done.lower() == 'c':
                
                super_blotter_sheet.api.AutoFilterMode = False
                
                super_blotter_book.save('//tlaloc/Cuantitativa/Fixed '+
                                        'Income/IRS Tenors Daily Update/'+
                                        'SPREADS/Spreads Code/Blotters/'+
                                        f'Super_blotter_{str_tiie}.xlsx')
                
            
    
    else:
        
        super_blotter_book.save('//tlaloc/Cuantitativa/Fixed '+
                                'Income/IRS Tenors Daily Update/'+
                                'SPREADS/Spreads Code/Blotters/'+
                                f'Super_blotter_{str_tiie}.xlsx')
     
    lf = input("\nWere there any new LF's today? (Y/N): ")
    if lf.lower() == 'y':
        print('\nPlease add the new LF in the catalogue file. Use '+
              'consecutive numbers for Blotter Name.')
        
        catalogue_book = xw.Book('//tlaloc/Cuantitativa/Fixed '+
                                 'Income/IRS Tenors Daily Update/'+
                                 'SPREADS/Spreads Code/'+
                                 'Spreads_Catalogue.xlsx')
        
        catalogue_sheet = catalogue_book.sheets(
            'Catalogue_' + str(date.year))
        
        catalogue_sheet.activate()
        done = input('\nWhen done filling the catalogue press "c": ')
        catalogue_book.save()
        catalogue_book.close()
        
        super_blotter_sheet.activate()
        c = 'z'
        while c.lower() != 'c':
            c = input("Please add LF's to super blotter. In emission write "+
                      '"BONDESF" and in Tenor write its series. In Comment '+
                      'column write the name you gave it in the catalogue. '+
                      'When done press "c": ')
        
        
    super_blotter_book.save()    
    super_blotter_book.close()
    return new_spread


def bond_details(bono: str, vector_file:pd.DataFrame, date: datetime,
                 monto: float, resumen_mercado: pd.DataFrame, udi: bool=False,
                 fondeo: float = .1125) -> (float, float, float, float):
    """ Desired Bond Details
    

    Parameters
    ----------
    bono : str
        Bond name
    vector_file : pd.DataFrame
        DataFrame of vector
    date : datetime
        Date of valuation
    monto : float
        Volume for bond
    resumen_mercado : pd.DataFrame
        Resumen de mercado file
    udi : bool, optional
        if is and UDI. The default is False.
    fondeo : float, optional
        Funding yield. The default is .1125.

    Returns
    -------
    (float, float, float, float)
        DESCRIPTION.

    """
    
    date_yst = ql.Mexico().advance(ql.Date().from_date(date), 
                                   ql.Period(-1, ql.Days)).to_date()
    
    tv = bono[0]
    serie = bono[2:]
    maturity = ql.Date(int(bono[6:]), int(bono[4:6]), int('20'+bono[2:4]))
    days_accrued = (ql.Date().from_date(date) - maturity)%182
    
    bono_close_yst = vector_file[(vector_file['Serie'] == serie) & 
                        (vector_file['TV']==tv)]['Rendimiento'].sum()
    
    bono_coupon = vector_file[(vector_file['Serie'] == serie) & 
                         (vector_file['TV']==tv)]['TasaCuponVigente'].sum()*100
    
    if udi:
        udi_tdy = vector_file[(vector_file['Emisora']=='MXPUDI') & 
                              (vector_file['Serie']=='V24')][
                                  'PrecioSucio'].values[0]
                                  
        udi_yst = vector_file[(vector_file['Emisora']=='MXPUDI') & 
                              (vector_file['Serie']=='1D')][
                                  'PrecioSucio'].values[0]
        
        price = bond_price_fn(maturity, bono_close_yst, bono_coupon, date)*udi_tdy
        
        price_yst = bond_price_fn(maturity, bono_close_yst, bono_coupon, 
                                  date_yst)*udi_yst
        
        titles = monto*1_000_000/price
    
    else:
        price = bond_price_fn(maturity, bono_close_yst, bono_coupon, date)
        price_yst = bond_price_fn(maturity, bono_close_yst, bono_coupon, 
                                  date_yst)
        titles = monto*10_000
    
    carry = price - price_yst

    
    if days_accrued == 0:
        cf = bono_coupon*182/360
    else:
        cf = 0
        
    fc = titles * price_yst * fondeo * (ql.Date().from_date(date)-
                                        ql.Date().from_date(date_yst))/360
                                     
    final_carry = (carry + cf)*titles
    
    
    if resumen_mercado.shape[0] != 0:
        try:
            if bono[0] == 'M':
                right_col = resumen_mercado.columns[1]
                price_col = resumen_mercado.columns[3]
                right_row = resumen_mercado[right_col].tolist().index(
                    bono.replace('M ', 'M_BONOS_'))
                bono_close_tdy = resumen_mercado.loc[right_row, price_col]
                
            elif bono[0] == 'S':
                right_col = resumen_mercado.columns[12]
                price_col = resumen_mercado.columns[14]
                right_row = resumen_mercado[right_col].tolist().index(
                    bono.replace('S ', 'S_UDIBONO_'))
                bono_close_tdy = resumen_mercado.loc[right_row, price_col]
        
        except:
            print(f'Bono {bono} not found.')
            resumen_mercado = pd.DataFrame()
    
    elif resumen_mercado.shape[0] == 0:
        
        bono_close_tdy = input(f'\nPlease enter close price for bono {bono}: ')
        bono_close_tdy = float(bono_close_tdy)
        
    #fc = funding_cost*titles
        
    return bono_close_yst, bono_close_tdy, final_carry, fc 

def swaps_details(tenor: str, daily_update_: pd.DataFrame, 
                  daily_update_yst: pd.DataFrame,
                  data8085: pd.DataFrame) ->(float, float, float):
    """Detail of swaps
    

    Parameters
    ----------
    tenor : str
        Tenor to update
    daily_update_ : pd.DataFrame
        Daily Update DataFrame
    daily_update_yst : pd.DataFrame
        Yesterday's Daily Update DataFrame'
    data8085 : pd.DataFrame
        8085 PnL DataFrame

    Returns
    -------
    (tiie_close_yst: float, tiie_close: float, swap_pnl: float)
        DESCRIPTION.

    """
    
    tiie_close = daily_update_[
        daily_update_['Spot'] == tenor.lower()][daily_update_.columns[1]].sum()
    
    tiie_close_yst = daily_update_yst[
        daily_update_yst['Spot'] == tenor.lower()][daily_update_yst.columns[1]].sum()
    
    swap_pnl = data8085.loc['IRS ' + tenor].PnL
    
    return tiie_close_yst, tiie_close, swap_pnl

def spreads_pnl(dt_today: datetime, data8085: pd.DataFrame,
                new_spread: bool = False) -> None:
    """Spread strategies PnL calculation
    

    Parameters
    ----------
    dt_today : datetime
        Date of evaluation
    data8085 : pd.DataFrame
        PnL DataFrame for book 8085
    new_spread : bool, optional
        IF a new spread is made. The default is False.

    Returns
    -------
    None
        DESCRIPTION.

    """
    
    ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
    ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
    dt_yst = dt(ql_dt_yest.year(), ql_dt_yest.month(), ql_dt_yest.dayOfMonth())

    month_dict = {1: 'ENE', 2: 'FEB', 3: 'MAR', 4: 'ABR', 5: 'MAY', 6: 'JUN', 
                    7: 'JUL', 8: 'AGO', 9: 'SEP', 10: 'OCT', 11: 'NOV', 
                    12: 'DIC'}

    file = '//tlaloc/Cuantitativa/Fixed Income/IRS Tenors Daily '+\
        f'Update/SPREADS/Spreads Code/SPREADS_{dt_yst.year}.xlsx'

    spreads_df = pd.read_excel(file, f'RESUMEN {month_dict[dt_yst.month]} 23', 
                             skiprows=2, usecols=[3, 5, 6, 7])

    spreads_df.rename(columns={spreads_df.columns[0]: 'Spread_Name'}, 
                      inplace=True)

    # Yesterday and today dates in string file formats
    str_dt = dt_yst.strftime('%Y%m%d')
    str_dt_2 = dt_yst.strftime('%d-%m-%Y')
    str_dt_tdy = dt_today.strftime('%d-%m-%Y')
    str_dt_tdy_2 = dt_today.strftime('%Y%m%d')

    vector_file = pd.read_csv(
        r'\\MINERVA\Applic\finamex\gbs\vector_precios_'+str_dt+'.csv')

    resumen_file = '//tlaloc/GPO-Bloomberg/Trading/Blotters '+\
        f'Trading/Valuaciones/Resumen_de_Mercado_{str_dt_tdy_2}.xlsx'

    try:
        resumen_mercado = pd.read_excel(resumen_file, skiprows=14)

    except:
        print('\nNo file for Resumen de Mercado found.')
        resumen_mercado = pd.DataFrame()
    
    daily_update_yst = pd.read_excel(r'\\TLALOC\Cuantitativa\Fixed Income\IRS'+
                                 r' Tenors Daily Update\Daily_Update_2023.xlsx', 
                                 str_dt_2)

    try:
        daily_update_ = pd.read_excel(r'\\TLALOC\Cuantitativa\Fixed Income\IRS'+
                                     r' Tenors Daily Update\Daily_Update_2023.xlsx', 
                                     str_dt_tdy)
    except:
        print(f"No daily update sheet found for {str_dt_tdy}. "+\
              "Yesterday's sheet will be used instead.")
        daily_update_ = daily_update_yst

    # Set sprads_df columns for Price Change and PnL
    spreads_df = spreads_df.dropna(subset='DVO1 MXN')
    spreads_df = spreads_df[spreads_df['SPREAD'].str.contains('BONDESF')==False]
    spreads_df = spreads_df[spreads_df['DVO1 MXN'] != 0]
    spreads_df['T-1'] = np.nan
    spreads_df['T'] = np.nan
    spreads_df['Carry'] = np.nan
    spreads_df['Funding Cost'] = np.nan
    spreads_df['PnL'] = np.nan

    for i, r in spreads_df.iterrows():
        name = r.SPREAD
        monto = r.MONTO
        spread = r.Spread_Name
        dv01 = r['DVO1 MXN']
        if name[0] == 'M' or (name[0] == 'S' and name != 'Swaps'):
            
            close_yst, close_tdy, carry, fc = bond_details(name, vector_file, 
                                                           dt_today, monto, 
                                                           resumen_mercado, 
                                                           name[0]=='S', 
                                                           .1125)
            spreads_df.at[i, 'T-1'] = close_yst
            spreads_df.at[i, 'T'] = close_tdy
            spreads_df.at[i, 'Carry'] = carry
            spreads_df.at[i, 'Funding Cost'] = fc
            spreads_df.at[i, 'PnL'] = dv01 * (close_tdy - close_yst)\
                * 100 + carry - fc
        
        elif name[0] == 'I':
            
            tenor = name[4:]
            close_yst, close, swap_pnl = swaps_details(tenor, daily_update_, 
                                                   daily_update_yst, data8085)
            spreads_df.at[i, 'T-1'] = close_yst
            spreads_df.at[i, 'T'] = close
            spreads_df.at[i, 'PnL'] = swap_pnl

    spreads_df['Price Change'] = (spreads_df['T'] - spreads_df['T-1'])*100
    jumanji_pnl = data8085.loc['Swaps'].PnL
    
    # Add jumanji PnL
    spreads_df.loc[spreads_df.shape[0]+1] = ['DU', 'Swaps'] + [np.nan]*6 \
        + [jumanji_pnl] + [np.nan]
            

    spreads_book = xw.Book(file, update_links=False)
    spreads_sheet = spreads_book.sheets(f'RESUMEN {month_dict[dt_yst.month]} 23')

    spreads = spreads_df[['SPREAD', 'Price Change', 'PnL']].rename(
        columns={'SPREAD': 'Name'})

    spreads.loc[spreads.shape[0]+1, 'Name'] = 'Total'
    spreads.loc[spreads.shape[0], 'PnL'] = spreads['PnL'].sum()

    spreads_sheet['R4'].options(pd.DataFrame, header=1, index=False, 
                                expand='table').value = spreads

def update_spreads_book(dt_today: datetime) -> None:
    """ Spreads Book Update
    

    Parameters
    ----------
    dt_today : datetime
        Date of valuation

    Returns
    -------
    None
        DESCRIPTION.

    """
    
    file = '//tlaloc/Cuantitativa/Fixed Income/IRS Tenors Daily '+\
        f'Update/SPREADS/Spreads Code/SPREADS_{dt_today.year}.xlsx'
        
    catalogue = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/IRS '+
                                  r'Tenors Daily Update/SPREADS/Spreads '+
                                  r'Code/Spreads_Catalogue.xlsx',
                                  'Catalogue_' + str(dt_today.year))
    
    str_date = dt_today.strftime('%y%m%d')
    
    super_blotter = pd.read_excel('//tlaloc/Cuantitativa/Fixed '+
                                  'Income/IRS Tenors Daily Update/'+
                                  'SPREADS/Spreads Code/Blotters/'+
                                  f'Super_blotter_{str_date}.xlsx')
    
    blotter_8085 = super_blotter[super_blotter['Book']==8085]
    blotter_8085.dropna(subset='Comment', inplace=True)
        
    excel_book = xw.Book(file)
    blotter_8085 = blotter_8085.merge(catalogue, how='left', left_on='Comment', 
                                      right_on='Blotter Name')
    
    sheet_names = [s.name for s in excel_book.sheets if '_data' in s.name]
    
    for s in blotter_8085['Strategy'].unique():
        if s + '_data' not in sheet_names:
            df_a = blotter_8085[blotter_8085['Strategy']==s]
            df_a.sort_values(by='Blotter Name', inplace=True)
            spread_df = df_a[['Folio', 'Emission', 'Global Name']]
            spread_df.rename(columns={'Global Name': 'Name'}, inplace=True)
            spread = s
            excel_book.sheets.add(s + '_data', before='DU_data')
            sheet = excel_book.sheets(s + '_data')
            sheet.range('A2').value = 'Folio'
            sheet.range('B2').value = 'Type'
            sheet.range('C2').value = 'Name'
            sheet.range('D2').value = 'Monto'
            sheet.range('E1').value = 'Utilidad'
            sheet.range('AK1').value = 'Monto'
            sheet.range('E2').value = datetime(dt_today.year, 
                                               dt_today.month, 1)
            sheet.range('AK2').value = datetime(dt_today.year, 
                                                dt_today.month, 1)
            
            formula = '=E2+1'
            sheet.range('F2').formula=formula
            sheet.range('F2:AI2').formula=sheet.range('F2').formula
            sheet.range('AL2:BO2').formula=sheet.range('F2').formula
        
            excel_book.sheets.add(spread, before='DU_data')
        try:
            template_name = [s.name for s in excel_book.sheets if 'Spread' 
                             in s.name and 'data' not in s.name and 
                             s.name!=spread][0]
            summary_sheet = excel_book.sheets(spread)
            excel_book.sheets(template_name).range('A1:AA100').copy(
                summary_sheet.range('A1'))
            
            summary_sheet.range('B1').value = spread.replace(
                '_', ' ').capitalize()
            spread_name = (',').join(spread_df['Name'].apply(
                lambda x: x[:4].replace(' ', '')).unique())
            summary_sheet.range('B2').value = spread_name.replace(
                ',', ' VS ')
            
            
            leg1 = df_a['Blotter Name'].unique()[0]
            summary_sheet.range('B3').value = df_a[
                df_a['Blotter Name']==leg1]['Global Name'].values[0]
            
            leg2 = df_a['Blotter Name'].unique()[1]
            summary_sheet.range('F3').value = df_a[
                df_a['Blotter Name']==leg2]['Global Name'].values[0]
            
            summary_sheet.range('B5:I27').clear_contents()
            
        except:
            print(f'Summary sheet for {spread} could not be added, '+
                  'please add it manually.')
    
#----------------------------
#        Spreads Code
#----------------------------

def fill_missing_swaps(spreads_blotter: xw.Book.sheets,
                       spreads_file: pd.DataFrame, dt_files: dt) -> None:
    """ Missing Swaps 
    

    Parameters
    ----------
    spreads_blotter : xw.Book.sheets
        Spread Sheet
    spreads_file : pd.DataFrame
        DataFrame of spread Strategy
    dt_files : dt
        Date for files

    Returns
    -------
    None
        DESCRIPTION.

    """
    # Read PosSwaps
    str_dt = dt_files.strftime('%Y%m%d')
    posswps_str = '//tlaloc/tiie/posSwaps/PosSwaps' + str_dt + '.xlsx'
    posswps = pd.read_excel(posswps_str)
    
    # Get missing swaps
    asw_swaps = spreads_file[spreads_file['Global Name'] 
                             != 'Swaps']['Folio'].tolist()
    swaps = spreads_file[spreads_file['Global Name'] 
                         == 'Swaps']['Folio'].tolist()
    posswps_swaps = posswps[
        (~(posswps['swp_ctrol'].isin(asw_swaps))) & 
        (posswps['swp_usuario']==8085)]['swp_ctrol'].tolist()
    
    missing_swaps = [s for s in posswps_swaps if s not in swaps]
    n = len(missing_swaps)
    
    missing_df = pd.DataFrame({'Estrategia': ['DU']*n, 'Mercado': ['SWAP']*n,
                               'Folio': missing_swaps, 
                               'Global Name': ['Swaps']*n, 
                               'Type': ['DURATION']*n, 
                               'Blotter Name': ['DS']*n})
    
    # Add missing swaps
    blotter_row = spreads_blotter.range('A1').end('down').row + 1
    spreads_blotter.range('A' + str(blotter_row)).value = missing_df.values
    
def get_mxnfx(dt_files: dt) -> float:
    """ Get USDMXN FX
    

    Parameters
    ----------
    dt_files : dt
        Date for file reading

    Returns
    -------
    float
        USD MXN FX

    """
    
    serie = 'SF43718'
    str_date = dt_files.strftime('%Y-%m-%d')
    token="c1b63f15802a3378307cc2eb90a09ae8e821c5d1ef04d9177a67484ee6f9397c" 
    
    url = "https://www.banxico.org.mx/SieAPIRest/service/v1/series/" \
        + serie + "/datos/" + str_date + "/" + str_date
    headers={'Bmx-Token':token}
    response = requests.get(url, headers=headers) 
    status = response.status_code 
    
    # Error en la obtención de los datos 
    if status != 200:
        return print('Error Banxico TIIE 1D')
    
    raw_data = response.json()
    data = raw_data['bmx']['series'][0]['datos'] 
    df = pd.DataFrame(data)
    mxn_fx = float(df['dato'].values[0])
    
    return mxn_fx

def find_folio(pnl_sif: pd.DataFrame, emission: str, serie: str,
               book: int = 8085) -> int:
    """ Find Folio 
    

    Parameters
    ----------
    pnl_sif : pd.DataFrame
        SIF trades
    emission : str
        Kind of instrument.
    serie : str
        Instrument series.
    book : int, optional
        Portfolio book. The default is 8085.

    Returns
    -------
    int
        Folio of the trade.

    """
    
    folio_df = pnl_sif[(pnl_sif['Book']==book) & 
                       (pnl_sif['Emission'] == emission.upper()) & 
                       (pnl_sif['Serie'] == serie)]
    
    if folio_df.shape[0] > 1:
        print(f'More than one folio found for {emission} {serie}. Please '+
              'fill folio manually in Spreads file.')
        folio = np.nan
    elif folio_df.shape[0] == 0:
        print(f'No record found for {emission} {serie}. Please fill folio '+
              'manually in Spreads file.')
        folio = np.nan
    else:
        folio = folio_df.index[0]
    
    return folio

def update_spreads(excel_book: xw.Book, dt_files: dt, catalogue: pd.DataFrame,
                   pnl_sif: pd.DataFrame):
    """
    

    Parameters
    ----------
    excel_book : xw.Book
        Spreads file.
    dt_files : dt
        Date for file reading.
    catalogue : pd.DataFrame
        Catalogue of names.
    pnl_sif : pd.DataFrame
        DataFrame of SIF trades.

    Returns
    -------
    None.

    """
    # Super Blotter read
    str_date = dt_files.strftime('%y%m%d')
    
    super_blotter = pd.read_excel('//tlaloc/Cuantitativa/Fixed '+
                                  'Income/IRS Tenors Daily Update/'+
                                  'SPREADS/Spreads Code/Blotters/'+
                                  f'Super_blotter_{str_date}.xlsx')
    
    blotter_8085 = super_blotter[(super_blotter['Book']==8085) 
                                 | (super_blotter['Book'] == 8084)]
    
    missing_folios = blotter_8085[blotter_8085['Folio'].isna()].copy()
    
    for i, b in missing_folios.iterrows():
        emission = b.Emission
        serie = b.Tenor
        if emission == 'BONDESF':
            folio = find_folio(pnl_sif, emission, str(serie), 8084)
        else:
            folio = find_folio(pnl_sif, emission, str(serie), 8085)
        
        blotter_8085.at[i, 'Folio'] = folio
    
    blotter_8085.drop_duplicates(subset='Folio', inplace=True)
        
    blotter_8085.dropna(subset='Comment', inplace=True)
    blotter_8085 = blotter_8085.merge(catalogue, how='left', left_on='Comment', 
                                      right_on='Blotter Name')
    
    # Add new strategies
    for s in blotter_8085['Strategy'].unique():
        
        df_a = blotter_8085[blotter_8085['Strategy']==s]
        spread_df = df_a[['Folio', 'Emission', 'Global Name']]
        spread = s
        try:
            sheet = excel_book.sheets(spread + '_data')
            last_row = sheet.range('A2').end('down').row
            if last_row > 1_000_000:
                last_row = 2
                folios = []
            else:
                folios = sheet.range('A3:A'+str(last_row)).value
            spread_df = spread_df[~(spread_df['Folio'].isin(folios))]
            sheet.range('A'+str(last_row+1)).value = spread_df.values
        except:
            
            print(f'\nNo sheet for {spread}_data found. The code will add it, but '+
                  'make sure the values are correct: \n')
            spread_df.rename(columns={'Global Name': 'Name'}, inplace=True)
            
            spread_df['Folio'] = spread_df['Folio'].fillna(0)
            spread_df['Folio'] = spread_df['Folio'].astype(int)
            spread_df['Folio'] = spread_df['Folio'].apply(
                lambda x: '{:.0f}'.format(x))
            print(spread_df.to_markdown(index=False))
            new_sheet_name = spread + '_data'
            excel_book.sheets.add(new_sheet_name, before='DU_data')
            sheet = excel_book.sheets(new_sheet_name)
            sheet.range('A2').value = 'Folio'
            sheet.range('B2').value = 'Type'
            sheet.range('C2').value = 'Name'
            sheet.range('D2').value = 'Monto'
            sheet.range('E1').value = 'Utilidad'
            sheet.range('AK1').value = 'Monto'
            sheet.range('E2').value = datetime(dt_files.year, 
                                               dt_files.month, 1)
            sheet.range('AK2').value = datetime(dt_files.year, 
                                                dt_files.month, 1)
            
            formula = '=E2+1'
            sheet.range('F2').formula=formula
            sheet.range('F2:AI2').formula=sheet.range('F2').formula
            sheet.range('AL2:BO2').formula=sheet.range('F2').formula
            sheet.range('A3').value = spread_df.values
            
            excel_book.sheets.add(spread, before='DU_data')
            try:
                template_name = [s.name for s in excel_book.sheets if 'Spread' 
                                 in s.name and 'data' not in s.name and 
                                 s.name!=spread][0]
                summary_sheet = excel_book.sheets(spread)
                excel_book.sheets(template_name).range('A1:AA100').copy(
                    summary_sheet.range('A1'))
                
                summary_sheet.range('B1').value = spread.replace(
                    '_', ' ').capitalize()
                spread_name = (',').join(spread_df['Name'].apply(
                    lambda x: x[:4].replace(' ', '')).unique())
                summary_sheet.range('B2').value = spread_name.replace(
                    ',', ' VS ')
                
                df_a.sort_values(by='Blotter Name', inplace=True)
                leg1 = df_a['Blotter Name'].unique()[0]
                summary_sheet.range('B3').value = df_a[
                    df_a['Blotter Name']==leg1]['Global Name'].values[0]
                
                leg2 = df_a['Blotter Name'].unique()[1]
                summary_sheet.range('F3').value = df_a[
                    df_a['Blotter Name']==leg2]['Global Name'].values[0]
                
                summary_sheet.range('B5:I27').clear_contents()
                
                #excel_book.save()
            except:
                print(f'Summary sheet for {spread} could not be added, '+
                      'please add it manually.')
            
    
    excel_book.save()
    
    
    
#--------------
#  Valuations
#--------------

def bond_price_fn(maturity: ql.Date, ytm: float, coupon: float, 
                  date: datetime) -> float:
    """ Bond Price Calc
    

    Parameters
    ----------
    maturity : ql.Date
    ytm : float
    coupon : float
    date : datetime
        Date of valuation

    Returns
    -------
    price: float
        Price of bond

    """

    dtm = maturity - ql.Date().from_date(date)
    n = np.ceil(dtm/182)
    accrued = -dtm % 182
    price = (182*coupon/36000 + coupon/ytm + (1-(coupon/ytm))\
             /(1+182*ytm/36000)**(n-1))*100/(1+182*ytm/36000)**(1-accrued/182)
    
    return price

def cete_price_fn(maturity: ql.Date, ytm: float, dt_files: dt) -> float:
    """ Cete Price Calc
    
    
    Parameters
    ----------
    maturity : ql.Date
    ytm : float
    date : datetime
        Date of valuation
    
    Returns
    -------
    price: float
        Price of cete
    
    """
    
    dtm = maturity - ql.Date().from_date(dt_files)
    price = 10/((ytm*dtm/36000) + 1)
    
    return price

def bonde_price_fn(maturity: ql.Date, tiie28: float, coupon_ac: float,
                   surcharge: float, dt_files: dt) -> float:
    """ Bonde Price Calc
    
    
    Parameters
    ----------
    maturity : ql.Date
    tiie28 : float
    coupon_ac: float
    surcharge: float
    dt_files : datetime
        Date of valuation
    
    Returns
    -------
    price: float
        Price of bonde
    
    """

    dtm = maturity - ql.Date().from_date(dt_files)
    n = np.ceil(dtm/28)
    accrued = -dtm % 28
    
    
    schdl = ql.Schedule(ql.Date().from_date(dt_files)-accrued, maturity,
                        ql.Period(13), ql.Mexico(), ql.Preceding, 
                        ql.Preceding, 0, False)
    
    
    
    days = []
    dates = []
    for i, d in enumerate(schdl):
        dates.append(d)
        if d > ql.Date().from_date(dt_files):
           day = d-ql.Date().from_date(dt_files)
           days.append(day)
 
    days = np.array(days)
    
    tenors = []
    for i in range(1,len(dates)):
        if dates[i] > ql.Date().from_date(dt_files):
            tenors.append(dates[i]-dates[i-1])
    
    tenors = np.array(tenors)
    ytm_28 = ((1 + (tiie28+surcharge)/36000)**28-1)*36000/28
    last_coupon = \
        ((1 + coupon_ac*accrued/36000)*(1+(tiie28)/36000)**(tenors[0]-accrued)-1)*\
            36000/28
    
    coupon = ((1 + (tiie28)/36000)**tenors[1:]-1)*36000/28
    
    
    
    price = last_coupon*28/360*1/(1+ytm_28*28/36000)**(days[0]/28) +\
        (coupon*28/360*1/(1+ytm_28*28/36000)**(days[1:]/28)).sum() +\
            100*1/(1+ytm_28*28/36000)**(days[-1]/28)
        
        

    return price


def bono_details(bono: str, vector_file: pd.DataFrame, dt_files: dt,
                 udi: bool = False) -> (float, float):
    """ Calculate Bond Details
    

    Parameters
    ----------
    bono : str
        Bond Name.
    vector_file : pd.DataFrame
        Vector.
    dt_files : dt
        date of valuation.
    udi : bool, optional
        if udi is being made. The default is False.

    Returns
    -------
    (float, float)
        returns bono close yield and bono dv01.

    """
    
    tv = bono[0]
    serie = bono[2:]
    maturity = ql.Date(int(bono[6:]), int(bono[4:6]), int('20'+bono[2:4]))
    
    bono_close = vector_file[(vector_file['Serie'] == serie) & 
                        (vector_file['TV']==tv)]['Rendimiento'].sum()
    
    bono_coupon = vector_file[(vector_file['Serie'] == serie) & 
                         (vector_file['TV']==tv)]['TasaCuponVigente'].sum()*100
    
    bono_dv01 = (bond_price_fn(maturity, bono_close, bono_coupon, dt_files) - \
        bond_price_fn(maturity, bono_close - .01, bono_coupon, dt_files))
    
    if udi:
        price = bond_price_fn(maturity, bono_close, bono_coupon, dt_files)
        bono_dv01 = bono_dv01*100/price
    
    
    return bono_close, bono_dv01

def cete_details(cete: str, vector_file: pd.DataFrame, 
                 dt_files: dt) -> (float, float):
    """Calculate Cete details
    

    Parameters
    ----------
    cete : str
        Cete name.
    vector_file : pd.DataFrame
        Vector.
    dt_files : dt
        date of valuation.

    Returns
    -------
    (float, float)
        Cete close ytm and DV01.

    """
    
    
    tv = cete[0:2]
    serie = cete[2:]
    maturity = ql.Date(int(cete[7:]), int(cete[5:7]), int('20'+cete[3:5]))
    
    cete_close = vector_file[(vector_file['Serie'] == serie) & 
                        (vector_file['TV']==tv)]['Rendimiento'].sum()
    
    cete_dv01 = (cete_price_fn(maturity, cete_close, dt_files) - \
        cete_price_fn(maturity, cete_close - .01, dt_files))
        
    return cete_close, cete_dv01

def bonde_details(bono: str, vector_file: pd.DataFrame, 
                  dt_files: dt) -> (float, float):
    """Calculate Bonde details 
    

    Parameters
    ----------
    bono : str
        Bonde name.
    vector_file : pd.DataFrame
        Vector.
    dt_files : dt
        Date of valuation.

    Returns
    -------
    (float, float)
        Surcharge and dv01.

    """
    
    emisora = bono.split()[0]
    serie = bono.split()[1]
    maturity = ql.Date(int(serie[4:]), int(serie[2:4]), int('20'+serie[:2]))
    
    bono_close = vector_file[(vector_file['Serie'] == serie) & 
                        (vector_file['Emisora']==emisora)]['Rendimiento'].sum()
    
    surcharge = vector_file[(vector_file['Serie'] == serie) & 
                        (vector_file['Emisora']==emisora)]['Sobretasa'].sum()
    
    bono_coupon =\
        vector_file[(vector_file['Serie'] == serie) & 
                    (vector_file['Emisora']==emisora)]\
            ['TasaCuponVigente'].sum()*100
    
        
    tiie28 = bono_close - surcharge
    
    
    
    bono_dv01 = ((bonde_price_fn(maturity, tiie28, bono_coupon, surcharge + .01,
                                dt_files) -
        bonde_price_fn(maturity, tiie28, bono_coupon, surcharge - .01, 
                       dt_files)))/2
    
    return surcharge, bono_dv01

def swap_details(tenor: str, daily_update: pd.DataFrame, dfpos: pd.DataFrame, 
                 controls: list, dt_today: dt,
                 curves: pf.cf.mxn_curves) -> (float, float):
    """
    

    Parameters
    ----------
    tenor : str
    daily_update : pd.DataFrame
        Daily Update file.
    dfpos : pd.DataFrame
        PosSwaps File.
    controls : list
        list of controls.
    dt_today : dt
        date of valuation.
    curves : pf.cf.mxn_curves
        curves for valuation.

    Returns
    -------
    (float, float)
        close yiled and dv01.

    """
    
    tiie_close = daily_update[
        daily_update['Spot'] == tenor.lower()][daily_update.columns[1]].sum()
    
    try:
        
        dfpos_ctrols = dfpos[dfpos['swp_ctrol'].isin(controls)]
        pfolio_ctrols= pf.pfolio.from_posSwaps(dfpos_ctrols)
        date_tdy = datetime(dt_today.year, dt_today.month, dt_today.day)
        dics = pfolio_ctrols.get_risk_byBucket(date_tdy, curves)
        tiie_dv01 = (dics['DV01_Swaps'].T.sum() - 
                     dics['DV01_Swaps']['%1L']).sum()
    except:
        tiie_dv01 = 0
    
    return tiie_close, tiie_dv01

def cme_fn(pnl_sif: pd.DataFrame) -> (float, float):
    """ Get CME utility
    

    Parameters
    ----------
    pnl_sif : pd.DataFrame
        SIF trades file.

    Returns
    -------
    (float, float)
        Aggregate utility and day utility.

    """
    
    cme_df = pnl_sif[(pnl_sif['Book']==8085) & (pnl_sif['Market'].isin(
        ['BRO', 'DIV']))]
    cme_acc = cme_df['Acc_Utility'].sum()
    cme_day = cme_df['Utility'].sum()
    
    return cme_acc, cme_day


#------------------
#  Main Functions
#------------------

def spreads_main(dt_today: dt,
                 curves: pf.cf.mxn_curves = None) -> pf.cf.mxn_curves:
    """ Spreads Main
    
    Gets trades utility and notionals for updatng the spread code

    Parameters
    ----------
    dt_today : dt
        Date of valuation.
    
    curves: pf.cf.mxn_curves
        Curves for swaps
    Returns
    -------
    None
        Spreads File.

    """
    
    ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
    ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
    dt_files = dt(ql_dt_yest.year(), ql_dt_yest.month(), ql_dt_yest.dayOfMonth())


    # REad files

    # Read necessary files

    month_dict = {1: 'ENE', 2: 'FEB', 3: 'MAR', 4: 'ABR', 5: 'MAY', 6: 'JUN', 
                    7: 'JUL', 8: 'AGO', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12: 'DIC'}


    # Dates to find files
    str_dt = dt_files.strftime('%Y%m%d')
    str_dt_2 = dt_files.strftime('%d-%m-%Y')

    # Files to fill close prices
    try:
        vector_file = pd.read_csv(
            r'\\MINERVA\Applic\finamex\gbs\vector_precios_' + str_dt + '.csv')
    except: 
        vector_file = pd.read_csv('//tlaloc/Cuantitativa/Fixed Income/TIIE '+
                                      'IRS Valuation Tool/GabyEsteban'+
                                      '/vector_precios_' + str_dt + '.csv')

    daily_update = pd.read_excel(r'\\TLALOC\Cuantitativa\Fixed Income\IRS'+
                                 r' Tenors Daily Update\Daily_Update_2023.xlsx', 
                                 str_dt_2)

        
    try:
        dfpos = pd.read_excel('//tlaloc/tiie/posSwaps'+
                              f'/PosSwaps{dt_files.strftime("%Y%m%d")}.xlsx')
    except: 
        print('No posSwaps file found.')
        

    # Read SIF file

    try:
        pnl_sif_complete = pd.read_excel(r'\\TLALOC\Cuantitativa\Fixed Income\IRS' +
                                         r' Tenors Daily Update\PnL '+
                                         r'SIF\UtilidadPerdida_' + str_dt + 
                                         '.xlsx')
    except:
        pnl_sif_book = xw.Book(r'\\TLALOC\Cuantitativa\Fixed Income\IRS' +
                                          r' Tenors Daily Update\PnL '+
                                          r'SIF\UtilidadPerdida_' + str_dt + 
                                          '.xlsx')
        pnl_sif_book.save()
        pnl_sif_book.close()
        pnl_sif_complete = pd.read_excel(r'\\TLALOC\Cuantitativa\Fixed Income'+
                                         r'\IRS Tenors Daily Update\PnL '+
                                         r'SIF\UtilidadPerdida_' + str_dt + 
                                         '.xlsx')

    # Data Handling
    pnl_sif = pnl_sif_complete[['lvRes_Usuario', 'lvRes_Mercado', 'lvRes_Emisora', 
                                'lvRes_num_futuro', 'lvRes_Serie', 
                                'lvRes_PosCierreDia', 'lvRes_VectorCierre',
                                'lvRes_UtilidadFin',
                                'lvRes_UtilidadDia']].copy()

    pnl_sif = pnl_sif.rename(columns={'lvRes_Usuario': 'Book', 
                                      'lvRes_Mercado': 'Market',
                                      'lvRes_Emisora':'Emission', 
                                      'lvRes_num_futuro': 'Folio',
                                      'lvRes_PosCierreDia': 'Position',
                                      'lvRes_VectorCierre': 'Close',
                                      'lvRes_Serie': 'Serie',
                                      'lvRes_UtilidadFin': 'Acc_Utility',
                                      'lvRes_UtilidadDia': 'Utility'})

    pnl_sif['Emission'] = pnl_sif['Emission'].str.strip()
    pnl_sif['Market'] = pnl_sif['Market'].str.strip()
    pnl_sif['Serie'] = pnl_sif['Serie'].str.strip()

    pnl_sif = pnl_sif.set_index('Folio')

    catalogue = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/IRS '+
                                  r'Tenors Daily Update/SPREADS/Spreads '+
                                  r'Code/Spreads_Catalogue.xlsx',
                                  'Catalogue_' + str(dt_files.year))


    # Excel file
        
    file_name = '//tlaloc/Cuantitativa/Fixed Income/IRS Tenors Daily '+\
        'Update/SPREADS/Spreads Code/'+\
        f'SPREADS_{dt_files.year}.xlsx'

    excel_book = xw.Book(file_name, update_links=False)

    # Update spreads
    update_spreads(excel_book, dt_files, catalogue, pnl_sif)

    # Find sheets for spreads
    spread_sheets = [s.name for s in excel_book.sheets if 'data' in s.name 
                     and 'Spread' in s.name]
    summary_sheets = [s.replace('_data', '') for s in spread_sheets]
    final_df = pd.DataFrame()

    # Fill spreads data
    inpt_str = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool'+\
        '/Main Codes/Portfolio Management/OOP Codes/TIIE_CurveCreate_Inputs.xlsx'

    
    
    if not curves:
        ql.Settings.instance().evaluationDate = ql_dt_today
        dic_data = pf.cf.pull_data(inpt_str, dt_today)
        fx_rate = dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
        if ql.UnitedStates(1).isHoliday(ql_dt_today):
            curves = pf.cf.mxn_curves(dic_data, None, ['MXN_OIS'])

        else:
            curves = pf.cf.mxn_curves(dic_data)
        
        dftiie12 = pf.cf.add_tiie_tenors(curves, ['%156L'])
        curves.change_tiie(dftiie12)
        curves.KRR_crvs(True, True)
    
    else:
        ql.Settings.instance().evaluationDate = ql_dt_today
        

    for s in spread_sheets:
        sheet = excel_book.sheets(s)

        spread_data = pd.read_excel(file_name, s, skiprows=1)
        spread_data = spread_data[['Folio', 'Type', 'Name']].merge(
            pnl_sif, how = 'left', left_on = 'Folio', right_index = True)
        
            
        spread_data['Monto'] = np.select([spread_data['Emission']=='BONOS', 
                                          spread_data['Emission']=='UDIBONO',
                                          spread_data['Emission']=='BONDESF'], 
                                         [spread_data['Position']/10000, 
                                          spread_data['Position']*spread_data[
                                              'Close']/1_000_000,
                                          spread_data['Position']/10000],
                                         spread_data['Position']/1000000)
        
        dead_spread = catalogue[
            catalogue['Strategy']==s.replace('_data', '')]['Dead or Alive']\
            .values[0]
            
        if dead_spread.upper() == 'D':
            sheet.range('A1').value = 'D'
        
        if str(sheet.range('A1').value).upper() == 'D':
            spread_data['Utility'] = np.select([spread_data['Type']=='Bond'], 
                                               [spread_data['Utility']], default=0)
            
            spread_data['Monto'] = np.select([spread_data['Type']=='Bond'], 
                                               [spread_data['Monto']], default=0)
        
        
        col_range = sheet.range('A2').end('right').column

        date_flag = True
        for i in range(1, col_range+1):
            if sheet.range(2, i).value == pd.to_datetime(dt_files):
                right_col = i
                date_flag = False
                break

        if date_flag:
            print('Date not found in columns.', s)
            
            
         
        sheet.range(3, right_col).value = spread_data[['Utility']].values
        sheet.range(3, right_col+32).value = spread_data[['Monto']].values
        sheet.range(3, 4).value = spread_data[['Monto']].values
        
        summary_sheet = excel_book.sheets(s.replace('_data', ''))
        row_range = summary_sheet.range('A5').end('down').row
        
        for i in range(5, row_range + 1):
            if summary_sheet.range(i, 1).value == pd.to_datetime(dt_files):
                right_row = i
                if i==5:
                    prev_flag = True
                    prev_row = i-1
                else:
                    prev_row = i-1
                    prev_flag = False
                break
        
        leg_1 = summary_sheet.range('B3').value
        leg_2 = summary_sheet.range('F3').value
        
        tenor = leg_2[4:]
        
        if (leg_1[0] == 'M' or leg_1[0] == 'S'):
            bono = leg_1
            bono_close, bono_dv01 = bono_details(bono, vector_file, dt_files, 
                                                 leg_1[0] == 'S')
            
            monto_leg1 = spread_data[spread_data['Name']==leg_1]['Monto'].sum()
            uti_leg1 = spread_data[spread_data['Name']==leg_1]['Utility'].sum()
            bono_dv01 = bono_dv01*monto_leg1*10000
            bono_row = np.array([monto_leg1, bono_close, uti_leg1, bono_dv01])
            summary_sheet.range('B'+str(right_row)).value = bono_row
            
            acc_uti_leg1 = spread_data[
                spread_data['Name']==leg_1]['Acc_Utility'].sum()
            
            if not prev_flag:
                prev_close_bono = summary_sheet.range('C'+str(prev_row)).value
            else:
                prev_close_bono = 0
            
            if prev_close_bono is None:
                prev_close_bono = 0
            
            
        
        controls = spread_data[spread_data['Name'] != leg_1]['Folio'].tolist()
        
        tiie_close, tiie_dv01 = swap_details(tenor, daily_update, dfpos, 
                                             controls, dt_today, curves)
        
        tiie_dv01 = tiie_dv01*fx_rate
        
        monto_leg2 = spread_data[spread_data['Name']==leg_2]['Monto'].sum()
        uti_leg2 = spread_data[spread_data['Name']==leg_2]['Utility'].sum()
        if not prev_flag:
            prev_close_tiie = summary_sheet.range('G'+str(prev_row)).value
        else:
            prev_close_tiie = 0
        
        if prev_close_tiie is None:
            prev_close_tiie = 0
            
        acc_uti_leg2 = spread_data[spread_data['Name']==leg_2]['Acc_Utility'].sum()
        
        if str(sheet.range('A1').value).upper() == 'D':
            monto_leg2 = 0
            uti_leg2 = 0
            tiie_dv01 = 0
            
        swap_row = np.array([monto_leg2, tiie_close, uti_leg2, tiie_dv01])
            
        summary_sheet.range('F'+str(right_row)).value = swap_row
        
        spread_df = pd.DataFrame({'Name': [s.replace('_data', '')]*2,
                                  'Type': ['ASW']*2, 'Spread': [leg_1, leg_2], 
                                  'Monto': [monto_leg1, monto_leg2],
                                  'DV01_MXN': [bono_dv01, tiie_dv01],
                                  'Yst_Valuation': 
                                      [(prev_close_bono-prev_close_tiie)*100]*2,
                                  'Valuation': [(tiie_close-bono_close)*100]*2,
                                  'Acc_PnL':[acc_uti_leg1, acc_uti_leg2],
                                  'PnL': [uti_leg1, uti_leg2]})
        
        final_df = pd.concat([final_df, spread_df])
        
       
        
        
    # Fill duration data

    tenors = range(2, 391)
    irs = {'IRS ' + str(t) + ' X 1': t for t in tenors}
    duration_order = {'S 231116': 1, 'UDI-TIIE 3y': 400, 'Swaps': 401}
    duration_order.update(irs)

    duration_sheet = excel_book.sheets('DU_data')
    duration_data = pd.read_excel(file_name, 'DU_data', skiprows=1)

    # duration_data['Order'] = duration_data['Name'].apply(lambda x: 
    #                                                      duration_order[x])
    # duration_data = duration_data.sort_values(by='Order')

    duration_data = duration_data[['Folio', 'Name']].merge(pnl_sif, how = 'left', 
                                               left_on = 'Folio', 
                                               right_index = True)

    duration_data['Monto'] = np.select([duration_data['Emission']=='BONOS', 
                                      duration_data['Emission']=='UDIBONO',
                                      duration_data['Emission']=='BONDESF'], 
                                     [duration_data['Position']/10000, 
                                      duration_data['Position']*duration_data[
                                          'Close']/1_000_000,
                                      duration_data['Position']/10000],
                                     duration_data['Position']/1000000)

    col_range = duration_sheet.range('A2').end('right').column

    date_flag = True
    for i in range(1, col_range+1):
        if duration_sheet.range(2, i).value == pd.to_datetime(dt_files):
            right_col = i
            date_flag = False
            break

    if date_flag:
        duration_sheet.range(2, col_range + 1).value = dt_files
        right_col = col_range + 1
        

    duration_sheet.range(3, right_col).value = duration_data[['Utility']].values
    duration_sheet.range(3, right_col+32).value = duration_data[['Monto']].values
    duration_sheet.range(3, 4).value = duration_data[['Monto']].values

    duration_summary = excel_book.sheets('DU')
    end_col = duration_summary.range('B4').end('right').column

    legs = duration_data['Name'].unique().tolist()
    montos = []
    closes = []
    utis = []
    dv01s = []
    du_df = pd.DataFrame()

    for l in legs:
        
        monto = duration_data[duration_data['Name']==l]['Monto'].sum()
        uti = duration_data[duration_data['Name']==l]['Utility'].sum()
        acc_uti = duration_data[duration_data['Name']==l]['Acc_Utility'].sum()
        if ((l[0] == 'M' or l[0] == 'S') and l != 'Swaps'):
            
            bono = l
            close, dv01 = bono_details(bono, vector_file, dt_files, l[0] == 'S')
            dv01 = dv01*monto*10000
        
        elif l[0] == 'B':
            
            cete = l
            close, dv01 = cete_details(cete, vector_file, dt_files)
            
        elif l[0] == 'I':
            
            tenor = l[4:]
            controls = duration_data[
                (duration_data['Name']==l)]['Folio'].tolist()
            close, dv01 = swap_details(tenor, daily_update, dfpos, controls, 
                                       dt_today, curves)
            dv01 = dv01*fx_rate
            #dv01 = risk_control(curves, controls, dt_files)
        
        else:
            close, dv01 = np.nan, np.nan
            
        closes.append(close)
        dv01s.append(dv01)
        
        l_row = np.array([monto, close, uti, dv01])
        
        for c in range(1, end_col+1):
            if duration_summary.range(3, c).value == l:
                right_col = c
                break
        
        duration_summary.range(right_row, right_col).value = l_row
        prev_close = duration_summary.range(prev_row, right_col + 1).value
        du_a = pd.DataFrame({'Name': ['DU'], 'Type': ['Duration'],
                             'Spread': [l],
                             'Monto': [monto], 'DV01_MXN': [dv01], 
                             'Yst_Valuation': [prev_close],
                             'Valuation': [close],
                             'Acc_PnL': [acc_uti],
                             'PnL': [uti]})
        
        du_df = pd.concat([du_df, du_a])
        #final_df = pd.concat([final_df, du_a])

    # duration_data['Order'] = duration_data['Name'].apply(lambda x: 
    #                                                      duration_order[x])
    # duration_data = duration_data.sort_values(by='Order')    
    du_df['Order'] = du_df['Spread'].apply(lambda x: duration_order[x])
    du_df = du_df.sort_values(by='Order')

    final_df = pd.concat([final_df, du_df])

    # Fill Tesoreria data
        
    tesoreria_df = pnl_sif[pnl_sif['Book']==8084]  .copy()  
    tesoreria_df['Name'] = tesoreria_df['Emission'].apply(lambda x: x + ' ') + \
        tesoreria_df['Serie']

    tesoreria_sheet = excel_book.sheets('T_data')
    tesoreria_data = pd.read_excel(file_name, 'T_data', skiprows=1)

    missing_bonds = [n for n in tesoreria_df['Name'].tolist() if n not in 
                     tesoreria_data['Name'].tolist()]

    if len(missing_bonds) > 0:
        print('MISSING BONDS: ', missing_bonds) 


    tesoreria_data = tesoreria_data[['Folio', 'Name']].merge(pnl_sif, how = 'left', 
                                               left_on = 'Folio', 
                                               right_index = True)

    tesoreria_data['Monto'] = np.select([tesoreria_data['Emission']=='BONOS', 
                                      tesoreria_data['Emission']=='UDIBONO',
                                      tesoreria_data['Emission']=='BONDESF'], 
                                     [tesoreria_data['Position']/10000, 
                                      tesoreria_data['Position']*tesoreria_data[
                                          'Close']/1_000_000,
                                      tesoreria_data['Position']/10000],
                                     tesoreria_data['Position']/1000000)

    col_range = tesoreria_sheet.range('A2').end('right').column

    date_flag = True
    for i in range(1, col_range+1):
        if tesoreria_sheet.range(2, i).value == pd.to_datetime(dt_files):
            right_col = i
            date_flag = False
            break

    if date_flag:
        print('Date not found.')

    tesoreria_sheet.range(3, right_col).value = tesoreria_data[['Utility']].values
    tesoreria_sheet.range(3, right_col+32).value = tesoreria_data[['Monto']].values
    tesoreria_sheet.range(3, 4).value = tesoreria_data[['Monto']].values

    tesoreria_summary = excel_book.sheets('T')
    end_col = tesoreria_summary.range('B4').end('right').column

    legs = tesoreria_data['Name'].unique().tolist()

    for l in legs:
        
        monto = tesoreria_data[tesoreria_data['Name']==l]['Monto'].sum()
        uti = tesoreria_data[tesoreria_data['Name']==l]['Utility'].sum()
        acc_uti = tesoreria_data[tesoreria_data['Name']==l]['Acc_Utility'].sum()
        if l.split(' ')[0] == 'BONDESF':
            bono = l
            surcharge, dv01 = bonde_details(bono, vector_file, dt_files)
            dv01 = dv01*monto*10000
        
        else:
            print('Bond different than BONDESF found.')
        
        l_row = np.array([monto, surcharge, uti, dv01])
        
        for c in range(1, end_col+1):
            if tesoreria_summary.range(3, c).value == l:
                right_col = c
                break
        
        tesoreria_summary.range(right_row, right_col).value = l_row
        
        prev_surcharge = tesoreria_summary.range(prev_row, right_col+1).value
        t_a = pd.DataFrame({'Name': ['T'], 'Type': ['Fondeo Tesorería'],
                            'Spread': [l],
                             'Monto': [monto], 'DV01_MXN': [dv01], 
                             'Yst_Valuation': [prev_surcharge],
                             'Valuation': [surcharge],
                             'Acc_PnL': [acc_uti],
                             'PnL': [uti]})
        
        final_df = pd.concat([final_df, t_a])


    # CME fill
    cme_acc, cme_day = cme_fn(pnl_sif)
    cme_df = pd.DataFrame({'Name': ['CME'], 'Acc_PnL': [cme_acc],
                          'PnL': [cme_day]})

    # Fill summary sheet
    year_str = str(dt_files.year)[-2:]
    summary_sheet = excel_book.sheets('PRE_RESUMEN '+ month_dict[dt_files.month] + 
                                      ' ' + year_str)

    summary_sheet.range('D5:H1000000').clear_contents()
    summary_sheet.range('K5:L1000000').clear_contents()
    summary_sheet.range('O5:O1000000').clear_contents()
    #summary_sheet.range('N5').clear_contents()

    summary_sheet.range('D4').value = 'CME'
    summary_sheet.range('E4').value = 'CME'
    summary_sheet.range('F4').value = 'CME'
    prev_cme = summary_sheet.range('N4').value
    summary_sheet.range('N4').value = prev_cme + cme_day
    summary_sheet.range('O4').value = cme_day

    summary_sheet.range('B2').value = dt_files
    summary_sheet.range('D5').value = final_df[['Name', 'Type', 'Spread', 'Monto', 
                                                'DV01_MXN']].values
    summary_sheet.range('K5').value = final_df[['Yst_Valuation', 
                                                'Valuation']].values
    summary_sheet.range('O5').value = final_df[['PnL']].values

    try:
        mxn_fx = get_mxnfx(dt_files)
        summary_sheet.range('B3').value = mxn_fx
        
    except:
        print('MXN_FX could not be downloaded. Please fill it manually in '+
              'PRE_RESUMEN sheet cell B3.')
    
    return curves
    


def main_updates(dt_today: datetime) -> pd.DataFrame:
    """Main Updates Funciton
    

    Parameters
    ----------
    dt_today : datetime
        Date of valuation.

    Returns
    -------
    None
        DESCRIPTION.

    """
    print('Interpolating Curves...')
    df_tiie = cubic_spline(dt_today, 'TIIE')
    df_ois = cubic_spline(dt_today, 'OIS')
    
    print('\nFilling db_cme file...')
    db_cme(dt_today)
    
    print('\nCalculating Daily Update...')
    daily_update(dt_today)


    print('\nPnL Calculation...')
    data8085 = pf.PnL(dt_today)

    print('\nSpreads Update and PnL...')
    new_spread = create_super_blotter(dt_today)

    try:
       
        spreads_pnl(dt_today, data8085)
    except:
        print('Something went wrong with spreads PnL.')

        

    if new_spread:
        update_spreads_book(dt_today)
        print('Spreads Book has been updated with new spreads.')
        
    print('\nAll done!')
    return data8085
#----------
#  Curves
#----------

def get_curves(dt_today: datetime, curves: cf.mxn_curves = None):
    """ Get curves
    

    Parameters
    ----------
    dt_today : datetime
        day of today.
    curves : cf.mxn_curves, optional
        curves. The default is None.

    Returns
    -------
    curves: mxn_curves

    """
    ql_evaldate = ql.Date().from_date(dt_today)
    if not curves:
        
        
        a = datetime.now()
        
        
        print(f'Calculating Curves for {dt_today.date()}...')
        str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
            'Tool/Main Codes/Portfolio Management/OOP Codes/'
        str_inputsFileName = 'TIIE_CurveCreate_Inputs'
        str_inputsFileExt = '.xlsx'
        str_input = str_dir + str_inputsFileName + str_inputsFileExt
        dic_data = pf.cf.pull_data(str_input, dt_today.date())
        
        if ql.UnitedStates(1).isHoliday(ql_evaldate):
            curves = pf.cf.mxn_curves(dic_data, None, ['MXN_OIS'])
    
        else:
            curves = pf.cf.mxn_curves(dic_data)
        
        dftiie12 = pf.cf.add_tiie_tenors(curves, ['%156L'])
        curves.change_tiie(dftiie12)
        print('Calculating KRR curves...')
        curves.KRR_crvs(True, True)
        b = datetime.now()
        print('Curves Calculated!')
        print(b-a)
        
        
        
    
    else:
        ql.Settings.instance().evaluationDate = ql_evaldate
        
        if curves.dic_data['MXN_TIIE'].shape[0] > 15:
            print('Calculating KRR curves...')
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
                'Tool/Main Codes/Portfolio Management/OOP Codes/'
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_input = str_dir + str_inputsFileName + str_inputsFileExt
            tenors = pd.read_excel(str_input, sheet_name = 'MXN_TIIE', 
                                   usecols = ['Tenor'])['Tenor'].tolist()
            
            dftiie_12y = pf.cf.add_tiie_tenors(curves, tenors + ['%156L'], True)
            curves.change_tiie(dftiie_12y)
            curves.KRR_crvs(True, True)
            print('Done!')
        
    
    return curves
    
    
    



#--------------
#  By Control  
#--------------

def by_ctrol(dt_today: datetime, wb: xw.Book, dfpos: pd.DataFrame,
             curves: pf.cf.mxn_curves = None) -> pf.cf.mxn_curves:
             
    """ Key rate Risk Calculation
    

    Parameters
    ----------
    dt_today : datetime
        DESCRIPTION.
    wb : xw.Book
        File to get parameters from
    dfpos : pd.DataFrame
        PosSwaps File to get swap details
    curves : pf.cf.mxn_curves, optional
        curves for valuating. The default is None.

    Returns
    -------
    Curves.

    """
    
    # Parameters
    
    byctrol = wb.sheets('By_Control')
    dv01_book = wb.sheets('DV01_Book')
    
    book = int(byctrol.range('B1').value)
    dv01 = byctrol.range('B2').value
    ctrol_flag = byctrol.range('B3').value
    
    ctrols = []
    if ctrol_flag:
        if dv01:
            print(f'\nKRR Risk for Controls will be calculated {dv01}')
        else:
            print(f'\nPnL for Controls will be calculated')
            
        ctrols = [int(c) for c in byctrol.range('A4').expand('down').value]
        book = None
        dfpos_c = dfpos[dfpos['swp_ctrol'].isin(ctrols)]
    else:  
        if dv01:
            print(f'\nKRR Risk for {book} will be calculated {dv01}')
        else:
            print(f'\nPnL for {book} will be calculated')
        dfpos_c = dfpos.copy()
    
    ql_evaldate = ql.Date().from_date(dt_today)
    
    if dv01 == 'By Code':
        yst_date = ql.Mexico().advance(ql_evaldate, ql.Period(-1, ql.Days)).to_date()
        

        curves = get_curves(dt_today, curves)

    
        pfolio_ctrols = pf.pfolio.from_posSwaps(dfpos_c, book)
        
        print('\nRunning DV01 risk in ' + 'PosSwaps' + 
              yst_date.strftime('%Y%m%d') + '...')
        print('\nKey Rate Risk...')
        dic_book_valrisk = pfolio_ctrols.get_risk_byBucket(dt_today, curves)


        print(f"\nAt {dt_today}\nNPV (MXN): "+\
              f"{dic_book_valrisk['NPV_Book']:,.0f}")
        # Bucket Risk (KRR)
        dfbook_br = dic_book_valrisk['DV01_Book']
        dfbook_br1 = dfbook_br.str.replace(',','').astype(float)
        outrisk = dfbook_br1[2:].sum()
        dfbook_br['OutrightRisk'] = '{:,.0f}'.format(outrisk)
        ## Display KRR
        dic_data = curves.dic_data
        print(dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0])
        print(dfbook_br)
        byctrol.range('D4').expand('table').clear_contents()
        byctrol.range('D4').value = np.array(dfbook_br.index[1:]).reshape(-1,1)
        byctrol.range('E4').value = dfbook_br.values[1:].reshape(-1,1)
        byctrol.range('E23').value = 0
        byctrol.range('E23').value = dic_book_valrisk['NPV_Book']
        
        dv01_book.range('A1').expand('table').clear_contents()
        dv01_book.range('A1').value = dic_book_valrisk['DV01_Swaps']
        dv01_book.range('A1').value = 'Controls'
        dv01_book.range('A2').value = dic_book_valrisk['Book'][
            ['TradeID']].values
        
        yst_date = pd.to_datetime(
            ql.Mexico().advance(ql_evaldate, ql.Period(-1, ql.Days)).to_date())
        ystyst_date = pd.to_datetime(
            ql.Mexico().advance(ql.Date().from_date(yst_date), 
                                ql.Period(-1, ql.Days)).to_date())
        

        yn = input('Do PnL calculation? (Y/N): ').lower()
        if yn == 'y':
            pnl = pfolio_ctrols.get_PnL(ystyst_date, yst_date)
            byctrol.range('G4').expand('table').clear_contents()
            byctrol.range('G4').value = np.array(pnl.index).reshape(-1,1)
            byctrol.range('H4').value = pnl.values
            
            save = input('Save PnL? (Y/N): ')
            
            if save.lower() == 'y':
                user_cwd = os.getcwd()
                pnl.to_excel(user_cwd + '/Detailed Files/' +
                             f'pfolio_pnl_{dt_today.strftime("%Y%m%d")}.xlsx')
    
    elif dv01 == 'By UAIR':
        # curves = None
        
        curves = get_curves(dt_today, curves)
        fxrate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
        
        
        try:
            uair = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/'+
                                 'IRS Tenors Daily Update/PnL SIF/'+
                                 f'Resumen DV01 {dt_today.strftime("%d-%m-%Y")}'+
                                 '.xlsx', sheet_name = 'DV01 Swaps Operables')
            
            if ctrol_flag:
                uair_c = uair[
                    uair['Control'].isin(ctrols)].set_index('Control')
            else:
                uair_c = uair[
                    uair['Usuario'] == '00'+str(book)].set_index('Control')
                
            uair_c.drop(['Usuario', 'Total'], axis = 1, inplace = True)  
            uair_c['OutRightRisk'] = uair_c.sum(axis=1)
            uair_c = uair_c[['OutRightRisk'] + uair_c.columns[:-1].tolist()]
            dfbook_br = (uair_c.sum()/fxrate).apply(
                lambda x : '{:,.0f}'.format(x))
            
            pfolio_ctrols = pf.pfolio.from_posSwaps(dfpos_c, book)
            npv = pfolio_ctrols.get_book_npv(dt_today, curves)['NPV'].sum()
            
            # Display
            print('\nKey Rate Risk...')
            print(f"\nAt {dt_today}\nNPV (MXN): "+\
                  f"{npv:,.0f}")
            print(fxrate)
            print(dfbook_br)
            
            byctrol.range('D4').expand('table').clear_contents()
            byctrol.range('D4').value = np.array(
                dfbook_br.index[1:]).reshape(-1,1)
            byctrol.range('E4').value = dfbook_br.values[1:].reshape(-1,1)
            byctrol.range('E23').value = 0
            byctrol.range('E23').value = npv
            
            dv01_book.range('A1').expand('table').clear_contents()
            dv01_book.range('A1').value = uair_c
            
            
            yst_date = pd.to_datetime(
                ql.Mexico().advance(ql_evaldate, ql.Period(-1, ql.Days)).to_date())

            ystyst_date = pd.to_datetime(
                ql.Mexico().advance(ql.Date().from_date(yst_date), 
                                    ql.Period(-1, ql.Days)).to_date())
            
            yn = input('Do PnL calculation? (Y/N): ').lower()
            if yn == 'y':
                pnl = pfolio_ctrols.get_PnL(ystyst_date, yst_date)
                byctrol.range('G4').expand('table').clear_contents()
                byctrol.range('G4').value = np.array(pnl.index).reshape(-1,1)
                byctrol.range('H4').value = pnl.values
                
                save = input('Save PnL? (Y/N): ')
                
                if save.lower() == 'y':
                    user_cwd = os.getcwd()
                    pnl.to_excel(user_cwd + '/Detailed Files/' +
                                 'pfolio_pnl_'+
                                 f'{dt_today.strftime("%Y%m%d")}.xlsx')
            
            
        except:
            print(f'Resumen DV01 {dt_today.strftime("%d-%m-%Y")}'+
                  ' file not found')
    
    else:

        curves = get_curves(dt_today, curves)
        fxrate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]

        pfolio_ctrols = pf.pfolio.from_posSwaps(dfpos_c, book)
        yst_date = pd.to_datetime(
            ql.Mexico().advance(ql_evaldate, ql.Period(-1, ql.Days)).to_date())
        ystyst_date = pd.to_datetime(
            ql.Mexico().advance(ql.Date().from_date(yst_date), 
                                ql.Period(-1, ql.Days)).to_date())
        
        
        pnl = pfolio_ctrols.get_PnL(ystyst_date, yst_date)
        
        byctrol.range('G4').expand('table').clear_contents()
        byctrol.range('G4').value = np.array(pnl.index).reshape(-1,1)
        byctrol.range('H4').value = pnl.values
        byctrol.range('E23').value = pnl['NPV_tdy'].sum()
    
        save = input('Save PnL? (Y/N): ')
        
        if save.lower() == 'y':
            user_cwd = os.getcwd()
            pnl.to_excel(user_cwd + '/Detailed Files/' +
                         f'pfolio_pnl_{dt_today.strftime("%Y%m%d")}.xlsx')
        
        
    return curves
    
    
#---------
#  GAMMA  
#---------
    
def gamma_function(pf_file, evaluation_date):
    
    str_file = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
        'Tool/Main Codes/Portfolio Management/OOP Codes/'+\
            'TIIE_CurveCreate_Inputs.xlsx'
            
    wb = xw.Book(pf_file)
    gamma_sheet = wb.sheets('Gamma')
    # evaluation_date = parameters_sheet.range('B1').value
    book = gamma_sheet.range('B1').value
    controls_flag = gamma_sheet.range('B2').value
    if controls_flag:
        controls = gamma_sheet.range('A3').expand('table').options(
            pd.DataFrame, header=True, index=False).value
    
    ql_dt_today = ql.Date().from_date(evaluation_date)
    ql.Settings.instance().evaluationDate = ql_dt_today
    ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
    dt_posswaps = ql_dt_yest.to_date()
    
    str_dt_posswaps = dt_posswaps.strftime('%Y%m%d')
    str_posswps_file = r'//TLALOC/tiie/posSwaps/PosSwaps'+str_dt_posswaps+'.xlsx'
    posSwaps_df = pd.read_excel(str_posswps_file)
    
    dic_data = cf.pull_data(str_file, evaluation_date.date())
    curvas = cf.mxn_curves(dic_data)
    curvas.KRR_crvs(True, True)
    
    if controls_flag:
        posSwaps_df = posSwaps_df[posSwaps_df['swp_ctrol'].isin(
            controls['Controls'].tolist())]
        book_pfolio = pf.pfolio.from_posSwaps(posSwaps_df)
    
    else:
        book_pfolio = pf.pfolio.from_posSwaps(posSwaps_df, book)

    dic_bookRisk = book_pfolio.get_risk_byBucket(evaluation_date, curvas)
    dv01_df = pd.DataFrame(dic_bookRisk['DV01_Book'].rename('Risk'))
    
    for i in range(0, dic_data['MXN_TIIE'].shape[0]):
        
        tenor = dic_data['MXN_TIIE'].iloc[i]['Tenor']
        dic_data_plus = {k: v.copy() for (k, v) in dic_data.items()}
        dic_data_plus['MXN_TIIE'].at[i, 'Quotes'] = \
            dic_data['MXN_TIIE'].iloc[i]['Quotes'] + 1/100
        curvas.change_tiie(dic_data_plus['MXN_TIIE'])
        curvas.KRR_crvs(True, True)
        dic_bookRisk_plus = book_pfolio.get_risk_byBucket(
            evaluation_date, curvas)
        dv01_df_a = pd.DataFrame(dic_bookRisk_plus['DV01_Book'].rename(tenor))
        dv01_df = dv01_df.merge(dv01_df_a, how='left', left_index=True, 
                                right_index=True)
        
        results_df = pd.DataFrame(
            dic_bookRisk['DV01_Book'].rename('Risk').drop(
                index='OutrightRisk'))
        results_df['Risk'] = results_df['Risk'].str.replace(
            ',', '').astype(float)
        out_risk = float(
            dic_bookRisk['DV01_Book'].loc['OutrightRisk'].replace(',', ''))
        risk_changes = []
        tenor_changes = []

    for t in dv01_df.columns:
        if t != 'Risk':
            risk = float(dv01_df.loc[t, t].replace(',', ''))
            risk_changes.append(risk)
            out_change = out_risk - float(dv01_df.loc[
                'OutrightRisk', t].replace(',', ''))
            tenor_changes.append(out_change)
                
    results_df['DV01_plus'] = risk_changes
    results_df['OutrightRisk Change'] = tenor_changes
    results_df['Tenor Change'] = results_df['Risk'] - results_df['DV01_plus']
    
    
    gamma_sheet['D3'].options(pd.DataFrame, header=1, index=True, 
                              expand='table').value = results_df
        
    return results_df

#---------------------
#  Analysis By Tenor
#---------------------

def analysis_byTenor(pf_file: str) -> None:
    """
    

    Parameters
    ----------
    pf_file : str
        Quant Portfolio Management File.

    Returns
    -------
    None
        DESCRIPTION.

    """
    wb = xw.Book(pf_file)

    analysis = wb.sheets('Analysis_byTenor')
    
    
    start_date = analysis.range('B1').value
    end_date = analysis.range('B2').value
    typ_dv01 = analysis.range('B3').value
    book = int(analysis.range('B4').value)
    flag_ctrol = analysis.range('B5').value
    
    if flag_ctrol:
        ctrols = [int(c) for  c in analysis.range('A6').expand('down').value]
    
    
    schdl = ql.Schedule(ql.Date.from_date(start_date), ql.Date.from_date(end_date),
                        ql.Period(1, ql.Days), ql.Mexico(), ql.Following,
                        ql.Following, 0, False)
    
    datesdt = [d.to_date() for i,d in enumerate(schdl)]
    dates = [datetime.strptime(d.strftime('%d%m%Y'), '%d%m%Y') for d in datesdt]
    
    conn = pyodbc.connect('Driver={SQL Server};'
                            'Server=Donaji;'
                            'Database=Historical_FI;'
                            'Trusted_Connection=yes;')

    
    str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
        'Tool/Main Codes/Portfolio Management/OOP Codes/'
    str_inputsFileName = 'TIIE_CurveCreate_Inputs'
    str_inputsFileExt = '.xlsx'
    str_input = str_dir + str_inputsFileName + str_inputsFileExt
    
    krs = pd.DataFrame()
    tenor2dt = {'B': 1, 'W': 7, 'L': 28}
    # tenors = ['%1L','%3L','%6L','%9L','%13L','%26L','%39L','%52L','%65L','%91L',
    #           '%130L','%156L','%195L','%260L','%390L']
    # tenors = pd.Series(tenors)
    cols = 0
    for d in dates:
        
        analysis.range(2, 6+cols).value = d
        cols += 5
        pos = pd.read_excel(f'//tlaloc/tiie/posSwaps/PosSwaps{d.strftime("%Y%m%d")}.xlsx')

        curves = None
        qd = ql.Date.from_date(d)
        
        
        yst_d = ql.Mexico().advance(qd, ql.Period(-1, ql.Days)).to_date()
        
        if typ_dv01 == 'By Code':
            tenorsql= ('1m', '3m', '6m', '9m', '1y', '2y', '3y', '4y', '5y', '7y', 
                      '10y', '12y','15y', '20y' ,'30y')
            
            try: 
                dics_comp = pf.cf.load_obj('//tlaloc/Cuantitativa/Fixed Income/'+
                                      'TIIE IRS Valuation Tool/Blotter/'+
                                      'Historical Risks/'+
                                      f'risk_{d.strftime("%Y%m%d")}')
                dic_data = pf.cf.pull_data(str_input, yst_d)
                fx_rate = dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
                dv01 = dics_comp[book]['DV01_Swaps'].copy()
                dv01.index = dics_comp[book]['Book'].TradeID.tolist()
                
                if flag_ctrol:
                    dv01_c = dv01[dv01.index.isin(ctrols)]
                    
                else:
                    dv01_c = dv01.copy()
                
            except:
                print(f'KRR Risk for {d} will be calculated')
                curves = get_curves(d, curves)
                fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
                
                portfolio_ctrol = pf.pfolio.from_posSwaps(pos, book)
                dics = portfolio_ctrol.get_risk_byBucket(d, curves)
                
               
                
                dv01 = dics['DV01_Swaps'].copy()
                dv01.index = dics['Book'].TradeID.tolist()
                 
                if flag_ctrol:
                    dv01_c = dv01[dv01.index.isin(ctrols)]
                    
                else:
                    dv01_c = dv01.copy()
                
                try:
                    dics_comp[book] = dics
                except:
                    dics_comp = {book: dics}
                
                pf.cf.save_obj(dics_comp,'//tlaloc/Cuantitativa/Fixed Income/'+
                               'TIIE IRS Valuation Tool/Blotter/'+
                               f'Historical Risks/risk_{d.strftime("%Y%m%d")}')
                
            krs = pd.concat([krs, dv01_c.sum()*fx_rate], axis = 1)
        
        else:
            tenorsql= ('1m', '3m', '6m', '9m', '1y', '2y', '3y', '4y', '5y', '7y', 
                      '10y', '15y', '20y' ,'30y')
            try:
                dv01 = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/'+
                                      'IRS Tenors Daily Update/PnL SIF/'+
                                      f'Resumen DV01 {d.strftime("%d-%m-%Y")}'+
                                      '.xlsx', sheet_name = 'DV01 Swaps Operables')
                if flag_ctrol:
                    dv01_c = dv01[dv01['Control'].isin(ctrols)].set_index('Control')
                else:
                    dv01_c = dv01.set_index('Control')
                dv01_c.drop(['Usuario', 'Total'], axis = 1, inplace = True)  
                krs = pd.concat([krs, dv01_c.sum()], axis = 1) 
                   
            
            except:
                print(f'Resumen DV01 {d.strftime("%d-%m-%Y")} file not found')
                print(f'KRR Risk for {d} will be calculated')
                tenorsdv = ['1X1', '3X1', '6X1', '9X1', '13X1', '26X1', '39X1', 
                            '52X1', '65X1',  '91X1', '130X1', '195X1', '260X1', 
                            '390X1'] 
    
                curves = get_curves(d, curves)
                fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
                
                portfolio_ctrol = pf.pfolio.from_posSwaps(pos, book)
                dics = portfolio_ctrol.get_risk_byBucket(d, curves)
                
               
                
                dv01 = dics['DV01_Swaps'].copy()
                dv01.index = dics['Book'].TradeID.tolist()
                 
                if flag_ctrol:
                    dv01_c = dv01[dv01.index.isin(ctrols)].copy()
                    
                else:
                    dv01_c = dv01.copy()
                
                dic_tendv = dict(zip(dv01_c.columns, tenorsdv))
                
                dv01_c.columns = [dic_tendv[dv] for dv in dv01_c.columns]
    
                krs = pd.concat([krs, dv01_c.sum()*fx_rate], axis = 1)
                
                
        try:
            det_file = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/'+
                                     'IRS Tenors Daily Update/Detailed Files/'+
                                     f'book{book}_{d.strftime("%Y%m%d")}_npv.xlsx')
    
        except:
            print(f'book{book}_{d.strftime("%Y%m%d")}_npv file not found')
            print('PnL will be calculated by Code')
            curves = None
            yst_date = datetime.strptime(yst_d.strftime('%d%m%Y'), '%d%m%Y')
            tdy_date = datetime.strptime(d.strftime('%d%m%Y'), '%d%m%Y')
            
            pf.get_pfolio_PnL(yst_date, tdy_date, pos, book)
                
            det_file = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/'+
                                     'IRS Tenors Daily Update/Detailed Files/'+
                                     f'book{book}_{d.strftime("%Y%m%d")}_npv.xlsx')   
        
        if flag_ctrol:
            
            det_file_c = det_file[det_file['TradeID'].isin(ctrols)]
        
        else:
            det_file_c = det_file.copy()

                
        det_file_c['term'] = (det_file_c.Maturity.dt.date -\
            d.date()).dt.days
            
        det_file_c.sort_values(by='term', inplace=True)
        tenors = pd.Series(krs.index.tolist())
        # print(tenors)
        if 'X' in tenors.iloc[0]:
            condi = tenors.apply(lambda x: int(x[:-2])*28).tolist()
            # print(condi)
        else:  
            condi = tenors.apply(lambda x: int(x[1:-1]) * tenor2dt[x[-1]]).tolist()
            
        condis = [(condi[t] + condi[t-1])/2 for t in range(1, len(condi))] 
        conditions = [det_file_c.term <= condis[0]] +\
            [(condis[t-1] <= det_file_c.term) & (det_file_c.term <= condis[t]) 
             for t in range(1, len(condis))] +\
                [det_file_c.term > condis[-1]]
        det_file_c['Bucket'] = np.select(conditions, tenors.tolist(), ':)')
        dfbucketcarry = pd.DataFrame(index = tenors.tolist())
        dfbucketcarry = pd.concat([dfbucketcarry, 
                                   pd.pivot_table(det_file_c, 
                                                  values = 'Carry_Roll', 
                                                  index = 'Bucket', 
                                                  aggfunc = np.sum)], axis = 1)
        

        dfbucketcarry = dfbucketcarry.fillna(0)
    
        # print(dfbucketcarry)
        
        dic_ten = dict(zip(tenorsql, krs.index))
        sql_tiie = pd.read_sql_query("SELECT * FROM [dbo].[Derivatives] WHERE "+
                                      "dv_stenor = 'Spot' AND dv_date IN "+
                                      f" ('{d.strftime('%Y-%m-%d')}', "+
                                      f"'{yst_d.strftime('%Y-%m-%d')}') AND "+
                                      f"dv_ftenor IN {tenorsql}", conn)
        
        sql_tiie['dv_date'] = sql_tiie['dv_date'].dt.date
        sql_tiie['dv_ftenor'] = sql_tiie['dv_ftenor'].replace(dic_ten)
        change_tiie = pd.pivot_table(sql_tiie, values = 'br_rate', 
                                      index = 'dv_ftenor', columns = 'dv_date')
        
        change_tiie['Change'] = (change_tiie[d.date()]- change_tiie[yst_d])*10000
        
        krs = pd.concat([krs, change_tiie[['Change']]], axis = 1)
        
        krs['Position PnL '+ str(d.day)] = krs[0]*krs['Change']
        
        krs.rename(columns = {0: 'DV01 '+ str(d.day),
                              'Change': 'Change ' + str(d.day)}, inplace = True)
        
        krs['Carry_Roll ' + str(d.day)] = dfbucketcarry['Carry_Roll']
        krs['PnL '+str(d.day)] =\
            krs['Carry_Roll ' + str(d.day)] + krs['Position PnL '+str(d.day)]
    
    krs.insert(0,'Pnl Acumulado',
               krs[['PnL ' + str(d.day) for d in dates]].sum(axis = 1))
    
    analysis.range('D3').expand('table').clear_contents()
    analysis.range('D3').value = krs
    analysis.range('D3').value = 'Tenor'
    
#-------------
#  Trioptima
#-------------


def trioptima(dt_today: datetime, pos: pd.DataFrame, wb: xw.Book,
              curves: cf.mxn_curves = None):
    
    
    tri = wb.sheets('Trioptima')
    
    book = tri.range('B1').value
    
    
    
    tri_df = tri.range('D1').options(pd.DataFrame, expand = 'table',
                                     header = 1, index = False).value


    ctrols_tot = tri_df['Folio Original'][tri_df['Comment'].str.lower() 
                                          == 'total'].astype(int).tolist()

    ctrols_par = tri_df['Folio Original'][tri_df['Comment'].str.lower() 
                                          == 'parcial'].astype(int).tolist()

    notion_par = (abs(tri_df['Size'][tri_df['Comment'].str.lower() 
                                          == 'parcial']*1000000)).tolist()

    posswaps_tot = pos[~pos['swp_ctrol'].isin(ctrols_tot)]
    
    ql_evaldate = ql.Date().from_date(dt_today)
    
    pos_date = ql.Mexico().advance(ql_evaldate, ql.Period(-1, ql.Days))
    
    for k in range(len(ctrols_par)):
        
        ind = posswaps_tot[posswaps_tot['swp_ctrol']==ctrols_par[k]].index[0]
        
        notional = posswaps_tot.loc[ind]['swp_monto']
        posswaps_tot.at[ind, 'swp_monto'] = notional - notion_par[k]
    
    
    
    curves = get_curves(dt_today, curves)
    fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
        
        
        
    pfolio_tri = pf.pfolio.from_posSwaps(posswaps_tot, book)

    dics = pfolio_tri.get_risk_byBucket(dt_today, curves)
    

    print('\nRunning DV01 risk for Trioptima Unwinds...')
    print('\nKey Rate Risk...')
    print(f"\nAt {dt_today}\nNPV (MXN): "+\
          f"{dics['NPV_Book']:,.0f}")
    dfbook_br = dics['DV01_Book']
    print(f'\n{dics["USDMXN_XCCY_Basis"]["Quotes"].iloc[0]}')
    print(dfbook_br)
        
    yn = input('Do you want to save KRR curves?')
    
    if yn.lower() == 'y':
        risk_path = '//tlaloc/Cuantitativa/Fixed Income/File Dump/Blotters/' +\
            'TIIE/Historial Risks/'
        try:
            dics_old = pd.read_pickle(risk_path +
                                      f'risk_{dt_today.strftime("%Y%m%d")}')
            dics_old[book] = dics
            cf.save_obj(dics_old, dt_today.strftime("%Y%m%d"))
        
        except:
            dics_old = {}
            dics_old[book] = dics
            cf.save_obj(dics_old, dt_today.strftime("%Y%m%d"))
        
    
    return curves

#----------------------
#  Prestamos Internos
#----------------------

def nomCaptu(clave,catalog,Columna1,Columna2):

    claveind=list(catalog[Columna1]).index(clave)
    Nombre = catalog[Columna2][claveind]
    return Nombre
  #Me da el nombre del capturista del programa    
def dfexcel(directorio):
    cat = pd.read_excel(directorio)
    cat1 =pd.DataFrame(cat)
    return cat1
   
# hacerlo con arrays para que sea más rápido
def cargalist(catalog,catv,lista,clavecaptu,tipo,num,usuario,dic_usu, vecto):   
    
        
    Ht = np.array([['MD','24HR','48HR','72HR'],[0,1,2,3]])
    pdias = pd.DataFrame(Ht.T,columns=["H","t"])
    carga = np.zeros(46).tolist()
    blot_cash = np.zeros(12).tolist()
    
    hoy = datetime.now()
    dhoy = hoy.date()
    thoy= hoy.time()
    t = int(nomCaptu(lista[8],pdias,'H','t'))
    
    f_liq = ql.Mexico().advance(ql.Date.from_date(dhoy), 
                                ql.Period(t, ql.Days)).to_date()
    
    
    
    # Trading Blotter
    
    carga[0]=tipo
    carga[1]="U"+str(usuario)
    carga[2]=nomCaptu("U"+str(usuario),catalog,'Clave','Nombre')
    carga[3]=dic_usu[usuario]+num   
    carga[5]="FOLIO CAPTURA"
    carga[8]=clavecaptu
    try:
        carga[9]=(nomCaptu(clavecaptu,catalog,'Clave','Nombre'))
    except:
        # print('Agregar la clave ',clavecaptu,' a catálogo_contrapartes_2022')
        carga[9]='SIN NOMBRE'
        pass
    carga[10]=(dhoy.strftime('%d/%m/%Y'))   
    carga[11]=(f_liq.strftime('%d/%m/%Y'))
    carga[12]=t
    carga[13]=thoy.strftime('%H:%M:%S')
    try:
        carga[14] = catv['TV'][catv['Serie']==lista[3]].iloc[0] 
        # print((catv['Serie'] == lista[3]).any())
        #Tipo de Bono
        carga[15]=catv['Emisora'][catv['Serie']==lista[3]].iloc[0]  #Emisora del bno
        carga[16]=int(float(lista[3])) #Serie el Bono
        carga[17]=catv['ISIN'][catv['Serie']==lista[3]].iloc[0]#ISIN del bono
        carga[18]=(int((catv['Maturity'][
            catv['Serie']==lista[3]].iloc[0].date()-f_liq).days+1))
        # print('wuuu')# dias a vencimiento
    except:
        print('Agregar el bono con serie ',lista[3],' al catálogo Valuación 2022')
        carga[14]='BONO FALTANTE'
        carga[15]='BONO FALTANTE'
        carga[16]=lista[3]
        carga[17]='BONO FALTANTE'
        carga[18]='BONO FALTANTE'
        pass
    carga[19]=(lista[6])
    carga[21]=tipo
    carga[23]="Indeval" 
    if lista[0]==usuario: carga[26]=-lista[5]; carga[20]=lista[1] #aqui se pone la contraparte
    else: carga[26]=lista[5]; carga[20]=lista[0]
    carga[32]=1
    carga[34]="MXN"
    carga[35]="MXN"      
    
    
    # Blotter_cash
    
    dic_ser_b, dic_ser_u, udi, udi24 = dic_create(vecto)
    
    
    blot_cash[0] = t
    blot_cash[1] = None
    blot_cash[2] = usuario
    blot_cash[5] = lista[6]
    if lista[2] == 'M':
        blot_cash[3] = 'BONOS'
        blot_cash[4] = dic_ser_b[lista[3]]
        blot_cash[6] = lista[4]/1_000_000
        
    elif lista[2] == 'S':
        blot_cash[3] = 'UDIBONO'
        blot_cash[4] = dic_ser_u[lista[3]]
        # print(lista[3])
        coupon = vecto[(vecto['TV'] == lista[2]) &
                       (vecto['Serie'] == str(int(lista[3])))
                       ]['TasaCuponVigente'].iloc[0]*100
        price = udibono_price_fn(
            datetime.strptime(str(20000000+int(lista[3])), '%Y%m%d').date(), 
            lista[6], coupon, f_liq)
            
        if t == 0:
            blot_cash[6] = udi*price*lista[5]/1_000_000
            # print(blot_cash[4], price)
        else:
            blot_cash[6] = udi24*price*lista[5]/1_000_000
            # print(blot_cash[4], price)
        
    else:
        blot_cash[3] = 'CETES'
        blot_cash[4] = dic_ser_b[lista[3]]
        blot_cash[6] = lista[4]/1_000_000
        
    blot_cash[7] = 'int'
    
    if lista[0] == usuario: 
        blot_cash[8] = lista[1]
        blot_cash[6] = -blot_cash[6]
    else: blot_cash[8] = lista[0]
    
    dic_trad = {'U3233': 'l', 'U2428': 'p', 'U2935': 't', 'U3236': 'e',
                'U3246': 'g', 'U3312': 'a'}
    blot_cash[11] = dic_trad[clavecaptu.capitalize()]
    

    
    
    return carga, blot_cash

def dic_create(vecto):
    cols = ['TV', 'Emisora', 'Serie', 'PrecioSucio', 'PrecioLimpio',
            'PrecioSucio24Hrs', 'PrecioLimpio24Hrs', 'Rendimiento', 
            'TasaCuponVigente']
    
    vecto = vecto[cols]
    
    bonos_df = vecto[vecto['TV'] == 'M']
    udibonos_df = vecto[vecto['TV'] == 'S']
    
    udi = vecto[(vecto['Emisora'] == 'MXPUDI') &
                (vecto['Serie'] == 'V24')]['PrecioSucio'].iloc[0]
    udi24H = vecto[(vecto['Emisora'] == 'MXPUDI') &
                   (vecto['Serie'] == 'V48')]['PrecioSucio'].iloc[0]
    
    serie_b = bonos_df['Serie'].tolist()
    
    l_s = [int(serie_b[0][:2])]
    for s in range(1, len(serie_b)):
        if int(serie_b[s][:2]) == l_s[s-1]:
            l_s.append(float(serie_b[s][:2])+.1)
        
        else:
            l_s.append(int(serie_b[s][:2]))
            
    serie_b = [int(s) for s in serie_b]    
    
    dic_ser_b = dict(zip(serie_b, l_s))
    
    serie_u = udibonos_df['Serie'].tolist()
    
    l_s_u = [int(serie_u[0][:2])]
    for s in range(1, len(serie_u)):
        if int(serie_u[s][:2]) == l_s_u[s-1]:
            l_s_u.append(float(serie_u[s][:2])+.1)
        
        else:
            l_s_u.append(int(serie_u[s][:2]))
            
    serie_u = [int(s) for s in serie_u]    
    
    dic_ser_u = dict(zip(serie_u, l_s_u))
    
    return dic_ser_b, dic_ser_u, udi, udi24H
    
   
def dfcarga(catalog, catv, dfPI, clavecaptu, columnas, usuario, dic_usu, vecto):
    l_PI=dfPI.to_numpy().tolist()
    cols_blot = ['SD_0', 'SD_1', 'User', 'Emisora', 'Security', 'Yield', 
                 'Size', 'load', 'Ctpy/Sales', 'Folio Brkr', 'Pos/Agr', 
                 'Trader']
    lcarga = []
    blot = []
    num=0
    for i in l_PI:    
        #print(i[0],i[1])
        if i[0] == usuario or i[1] == usuario:
            carga, blot_cash = cargalist(catalog, catv, i, clavecaptu, 
                                         'Interno', num, usuario, dic_usu, 
                                         vecto)
                                         
            lcarga.append(carga)
            blot.append(blot_cash)
            num+=1  
        #else:
            #raise Exception('PORFAVOR PONGA UN USUARIO VÁLIDO \n No se encuentra el usuario en el Blotter')
            
            
    blotcarga = pd.DataFrame(blot, columns = cols_blot)       
    dfcarga = pd.DataFrame(lcarga,columns=columnas)
    
    
    return(dfcarga, blotcarga)


def udibono_price_fn(maturity, ytm, coupon, dt_files):

    dtm = (maturity - dt_files).days
    n = np.ceil(dtm/182)
    accrued = -dtm % 182
    
    price = (182*coupon/36000 + coupon/ytm + (1-(coupon/ytm))\
             /(1+182*ytm/36000)**(n-1))*100/(1+182*ytm/36000)**(1-accrued/182)
        
        
    return price


    
    

def cargaexcel(clavecaptu, l_usu, dt_today):
    
    # Catalogues reading
    cat_path = '//tlaloc/Cuantitativa/Fixed Income/File Dump/Catalogues/'
    catalog = dfexcel(cat_path + 'catalogo_contrapartes_2022.xlsx')
    catv = dfexcel(cat_path + 'Valuacion2022.xlsx')
    blot_path = '//tlaloc/Cuantitativa/Fixed Income/File Dump/Blotters/Bonos/'
    dfPI = dfexcel(blot_path + 'PRESTAMOS INTERNOS Blotter.xlsx')
    
    usus = dfPI['VENDE'].unique()
    
    if not all(k in usus for k in l_usu):
        no_esta = list(set(l_usu).difference(usus))
        
        print('\nPORFAVOR PONGA UN USUARIO VÁLIDO \n '
              'No se encuentra el/los usuario(s)'
              f' {no_esta} en el Blotter.')
        l_usu = list(set(l_usu).difference(no_esta))
        
    l_num=(np.linspace(1, len(usus)+1, len(usus)+1)*1000).astype(int).tolist()
    dic_usu=dict(zip(usus,l_num))
    

    columnas = dfexcel(cat_path + 'CargaPI.xlsx').columns.values.tolist()
    
    vecto_date = ql.Mexico().advance(
        ql.Date.from_date(dt_today), ql.Period(-1, ql.Days)).to_date()

    vecto = pd.read_csv('//tlaloc/Cuantitativa/Fixed Income/File Dump/'+
                        'Vectores/vector_precios_'+
                        f'{vecto_date.strftime("%Y%m%d")}.csv')
        
    dfcarg = pd.DataFrame()
    blotcar = pd.DataFrame()
    for usuario in l_usu:       
        df_a, blot_a = dfcarga(catalog, catv, dfPI, clavecaptu, columnas, 
                               usuario, dic_usu, vecto)
        dfcarg = pd.concat([dfcarg, df_a])
        blotcar = pd.concat([blotcar, blot_a])
        
    user_path = os.getcwd()
    
    dfcarg.to_excel(user_path + '/Cargas/carga.xlsx')
    blotcar.to_excel(user_path + '/Cargas/blot_cash.xlsx')
                              
        
    return(dfcarg)


def prestamos_internos(dt_today):
    
    clavecaptu=input('Escriba la clave de Capurista: ')

    usu=True
    print('Escriba los usuarios que desea (sólo el número, cuando acabe escriba "0"): ')
    l_usu=[]
    l=1
    while usu:
        usuario=int(input(f'         {l}. '))
        l=l+1
        if usuario == 0:
            break
        else:
            l_usu.append(usuario)
        
    if len(set(l_usu)) != len(l_usu):
        print('\nFAVOR DE PONER USUARIOS DIFERENTES, se quitará el duplicado')
        l_usu = list(set(l_usu))

    dfcar=cargaexcel(clavecaptu, l_usu, dt_today)
    
    return dfcar

#--------------
#  BucketRisk
#--------------

def bucketRisk(dt_today: datetime, df_tiieSwps: pd.DataFrame,
               curves_tdy: cf.mxn_curves) -> dict:
    """Key Rate Risk by book
    

    Parameters
    ----------
    dt_today : datetime
        DESCRIPTION.
    pos : pd.DataFrame
        DESCRIPTION.

    Returns
    -------
    dics
        DESCRIPTION.

    """
    
    
    ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
    ql.Settings.instance().evaluationDate = ql_dt_today
    ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
    
    try:
        cf.load_obj(r'//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool' +\
                 '/Blotter/Historical Risks/sl_npv'+dt_today.strftime('%Y%m%d'))
            
    except:
        df_swps_total = df_tiieSwps[['swp_usuario', 'swp_nombre', 
                                     'swp_utilidad', 'swp_nomcte', 
                                     'swp_emisora', 'swp_serie']]
        emisoras = ['TIIE28', 'TF.MN.']    
        socios = ['CITIBANK SWAPS CHICAGO', 'GOLDMAN SACHS SWAPS CHICAGO', 
                  'FINAMEX OPERACIONES SWAPS', 'SANTANDER SWAPS ASIGNA']
        df_swps_total['swp_nombre'] = df_swps_total['swp_nombre'].str.strip()
        df_swps_total['swp_nomcte'] = df_swps_total['swp_nomcte'].str.strip()
        df_swps_total['swp_emisora'] = df_swps_total['swp_emisora'].str.strip()
        df_swps_total['swp_serie'] = df_swps_total['swp_serie'].str.strip()
        
        df_swaps_sl = df_swps_total[(df_swps_total['swp_nombre'].isin(socios)) & 
                                    (df_swps_total['swp_emisora'].isin(emisoras)) 
                                    & (df_swps_total['swp_serie'].isin(emisoras))]
        
        socios_totales = socios.copy()
        socios_totales.append('INTERNO')
        sl_df = pd.DataFrame(columns = socios_totales, 
                             index = df_swaps_sl['swp_usuario'].unique())
        for socio in socios:
            for book in df_swaps_sl['swp_usuario'].unique():
                df_a = df_swaps_sl[(df_swaps_sl['swp_usuario'] == book) & 
                                   (df_swaps_sl['swp_nombre']==socio) & 
                                   (df_swaps_sl['swp_nomcte'] != 'INTERNO')]
                sl_df.loc[book, socio] = -df_a['swp_utilidad'].sum()
        
        for book in df_swaps_sl['swp_usuario'].unique():
            df_a = df_swaps_sl[(df_swaps_sl['swp_usuario'] == book) & 
                               (df_swaps_sl['swp_nomcte'] == 'INTERNO')]
            sl_df.loc[book, 'INTERNO'] = -df_a['swp_utilidad'].sum()
        
                
        cols_names = {'CITIBANK SWAPS CHICAGO': 'Citi', 
                      'GOLDMAN SACHS SWAPS CHICAGO': 'GS',
                      'FINAMEX OPERACIONES SWAPS': 'OTC', 
                      'SANTANDER SWAPS ASIGNA': 'Santa Asigna',
                      'INTERNO': 'Interno'}
        
        sl_df = sl_df.rename(columns = cols_names)
        cf.save_obj(
            sl_df,
            r'//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool' +\
                '/Blotter/Historical Risks/sl_npv'+dt_today.strftime('%Y%m%d'))
    
        
    usu=True
    print('\nPlease write the books you want to evaluate '+
          '(Press "0" when finished, add "G" for granular risk, '+
          'or "F" for future risk evaluation): ')
    
    l_usu=[]
    l_gusu = []
    l_fusu = []

    l=1

    while usu:
        usuario = input(f'         {l}. ')
        if usuario == "0":
            break
        
        elif usuario.lower()[-1] == "g":
            l_gusu.append(int(usuario[:-1]))
            continue
        
        elif usuario.lower()[-1] == 'f':
            l_fusu.append(int(usuario[:-1]))
            continue
        
        else:
            try: 
                l_usu.append(int(usuario))
            except:
                l_usu.append(usuario.lower())
            l = l+1

        if len(set(l_usu)) != len(l_usu):
            print('\nPLEASE WRITE DIFFERENT BOOKS, '+
                  'the duplicate will be removed')
            l_usu = list(set(l_usu))
        
    str_today = dt_today.strftime("%Y%m%d")
    
    
    if l_usu:
        krr_type = 'normal'
        try:
            dic_risks = cf.load_obj(r'//TLALOC/Cuantitativa/Fixed Income'
                                 '/TIIE IRS Valuation Tool/Blotter/Historical'
                                 ' Risks/risk_' + str_today)
            book_found = set(dic_risks.keys())
            
        except:
            dic_risks = {}
            book_found = set()
            pass
        
        if set(l_usu).difference(book_found):
            
            if not curves_tdy:
                
                print(f'\nCalculating Curves for {dt_today.date()}...')
                str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                    'TIIE IRS Valuation Tool/Main Codes/'+\
                        'Portfolio Management/OOP Codes/'
                    
                    
                str_inputsFileName = 'TIIE_CurveCreate_Inputs'
                str_inputsFileExt = '.xlsx'
                str_file = str_dir + str_inputsFileName + str_inputsFileExt
                ql.Settings.instance().evaluationDate = ql.Date(
                    ).from_date(dt_today)
                dic_data = cf.pull_data(str_file, dt_today.date())
                if ql.UnitedStates(1).isHoliday(ql_dt_today):
                    curves_tdy = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
                    
                else:   
                    curves_tdy = cf.mxn_curves(dic_data)
                    
                dftiie_12y = cf.add_tiie_tenors(curves_tdy, ['%156L'])
                
                curves_tdy.change_tiie(dftiie_12y)
                curves_tdy.KRR_crvs(True, True)
            
            elif curves_tdy.dic_data['MXN_TIIE'].shape[0]>15:
                str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                    'TIIE IRS Valuation Tool/Main Codes/'+\
                        'Portfolio Management/OOP Codes/'
                    
                    
                str_inputsFileName = 'TIIE_CurveCreate_Inputs'
                str_inputsFileExt = '.xlsx'
                str_file = str_dir + str_inputsFileName + str_inputsFileExt
                    
                dic_data = cf.pull_data(str_file, dt_today.date())
                ql.Settings.instance().evaluationDate = ql.Date(
                    ).from_date(dt_today)
                tenors = dic_data['MXN_TIIE']['Tenor'].tolist()
                dftiie_12y = cf.add_tiie_tenors(curves_tdy, tenors + ['%156L'],
                                                True)
                
                curves_tdy.change_tiie(dftiie_12y)
                curves_tdy.KRR_crvs(True, True)
            
            else:
                if curves_tdy.KRR_curves == None:
                    ql.Settings.instance().evaluationDate = ql.Date(
                        ).from_date(dt_today)
                    curves_tdy.KRR_crvs(True, True)
        
        for book in l_usu:
            
            dics = get_bucket_risk(df_tiieSwps, curves_tdy, dt_today, krr_type,
                                   book)
            
    
    if l_gusu:
        krr_type = 'granular'
        try:
            dic_grisks = cf.load_obj(r'//TLALOC/Cuantitativa/Fixed Income'
                                 '/TIIE IRS Valuation Tool/Blotter/Historical'
                                 ' Risks/grisk_' + str_today)
            gbook_found = set(dic_grisks.keys())
            
        except:
            dic_grisks = {}
            gbook_found = set()
            pass
        
        if set(l_gusu).difference(gbook_found):
            
            if not curves_tdy:
                
                print(f'\nCalculating Curves for {dt_today.date()}...')
                str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                    'TIIE IRS Valuation Tool/Main Codes/'+\
                        'Portfolio Management/OOP Codes/'
                    
                    
                str_inputsFileName = 'TIIE_CurveCreate_Inputs'
                str_inputsFileExt = '.xlsx'
                str_file = str_dir + str_inputsFileName + str_inputsFileExt
                    
                dic_data = cf.pull_data(str_file, dt_today.date())
                if ql.UnitedStates(1).isHoliday(ql_dt_today):
                    ql.Settings.instance().evaluationDate = ql.Date(
                        ).from_date(dt_today)
                    curves_tdy = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
                    
                else:  
                    ql.Settings.instance().evaluationDate = ql.Date(
                        ).from_date(dt_today)
                    curves_tdy = cf.mxn_curves(dic_data)
                    
                granular = cf.granular(curves_tdy)
                
                curves_tdy.change_tiie(granular)
                curves_tdy.KRR_crvs(True, True)
            
            elif curves_tdy.dic_data['MXN_TIIE'].shape[0]<16:
                str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                    'TIIE IRS Valuation Tool/Main Codes/'+\
                        'Portfolio Management/OOP Codes/'
                    
                    
                str_inputsFileName = 'TIIE_CurveCreate_Inputs'
                str_inputsFileExt = '.xlsx'
                str_file = str_dir + str_inputsFileName + str_inputsFileExt
                    
                dic_data = cf.pull_data(str_file, dt_today.date())
                ql.Settings.instance().evaluationDate = ql.Date(
                    ).from_date(dt_today)
                granular = cf.granular(curves_tdy)
                
                curves_tdy.change_tiie(granular)
                curves_tdy.KRR_crvs(True, True)
            
            elif curves_tdy.KRR_curves == None:
                ql.Settings.instance().evaluationDate = ql.Date(
                    ).from_date(dt_today)
                curves_tdy.KRR_crvs(True, True)
        
        for book in l_gusu:
            
            dics = get_bucket_risk(df_tiieSwps, curves_tdy, dt_today, krr_type, 
                                   book)
        
    if l_fusu:
        
        krr_type = 'future'
        # Date Assignment
        dateIsNotOk = True
        while dateIsNotOk:
            print('\n\nFuture Date')
            input_year = int(input('\tYear: '))
            input_month = int(input('\tMonth: '))
            input_day = int(input('\tDay: '))
            print('\n')
            try:
                dt_fut = datetime(input_year, 
                                  input_month, input_day).date()
                dateIsNotOk = False
            except:
                print('Wrong date! Try again pls.')
                dateIsNotOk = True
        
        
        try:
            dic_frisks = cf.load_obj('Future Risks/risk_'+str_today)
            fbook_found = set(dic_frisks.keys())
            future_date = dic_frisks['FutureDate']
            
            if future_date == dt_fut.strfime('%Y-%m-%d'):
                date_flag = False
                
            else:
                date_flag = True
            
        except:
            dic_frisks = {}
            fbook_found = set()
            pass
        curves_fut = None
        if set(l_fusu).difference(fbook_found) or date_flag:
            
            ql_dt_fut = ql.Date().from_date(dt_fut)
        
                
            print(f'\nCalculating Curves for {dt_fut}...')
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                'TIIE IRS Valuation Tool/Main Codes/'+\
                    'Portfolio Management/OOP Codes/'
                
                
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_file = str_dir + str_inputsFileName + str_inputsFileExt
                
            dic_data = cf.pull_data(str_file, dt_today.date())
            if ql.UnitedStates(1).isHoliday(ql_dt_fut):
                curves_tdy = None
                ql.Settings.instance().evaluationDate = ql_dt_fut
                curves_fut = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
                
            else:   
                curves_fut = cf.mxn_curves(dic_data)
                
            dftiie_12y = cf.add_tiie_tenors(curves_fut, ['%156L'])
            
            curves_fut.change_tiie(dftiie_12y)
            curves_fut.KRR_crvs(True, True)
        
        
        for book in l_fusu:
            
            dics = get_bucket_risk(df_tiieSwps, curves_fut, dt_fut, krr_type,
                                   book)
                            
    
    if (not l_usu and not l_gusu and not l_fusu):
        return None, None
    
    else:
        return dics, curves_tdy
            
            
   
                    
def get_bucket_risk(df_tiieSwps: pd.DataFrame, curves: cf.mxn_curves,
                    dt_today: datetime, krr_type: str, bookID: int):
    """
    

    Parameters
    ----------
    df_tiieSwps : pd.DataFrame
        DESCRIPTION.
    curves : cf.mxn_curves
        DESCRIPTION.
    dt_today : datetime
        DESCRIPTION.
    krr_type : str
        DESCRIPTION.
    bookID : int
        DESCRIPTION.
    dt_fut : datetime, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    None.

    """
    str_today =  dt_today.strftime('%Y%m%d')
    if krr_type == 'normal':
        
        try:
            dic_risks = cf.load_obj(r'//TLALOC/Cuantitativa/Fixed Income'
                                 '/TIIE IRS Valuation Tool/Blotter/Historical'
                                 ' Risks/risk_' + str_today)
            book_found = set(dic_risks.keys())
            
        except:
            dic_risks = {}
            book_found = set()
            pass
        
        if bookID not in book_found:
            if type(bookID) == int:
                fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
                book_pfolio = pf.pfolio.from_posSwaps(df_tiieSwps, 
                                                      bookID = bookID)
                
                print(f'\nRunning DV01 risk for {bookID} book in '+'PosSwaps'+
                      str_today+'...')
                print(f'\nBook {bookID} Key Rate Risk...')
                dic_book_valrisk = book_pfolio.get_risk_byBucket(dt_today, curves)
                
                dic_risks[bookID] = dic_book_valrisk
                
                print(f"\nAt {dt_today}\nNPV (MXN): "+\
                      f"{dic_book_valrisk['NPV_Book']:,.0f}")
                # Bucket Risk (KRR)
                dfbook_br = dic_book_valrisk['DV01_Book']
                ## Display KRR
                print(fx_rate)
                
                
                print(dfbook_br)
            
            else:
                cbs = ['CITIBANK SWAPS CHICAGO', 
                       'GOLDMAN SACHS SWAPS CHICAGO',
                       'FINAMEX OPERACIONES SWAPS',
                       'SANTANDER SWAPS ASIGNA']
                
                if bookID == 'fcm':
                    for b in cbs:
                        
                        posswaps1 = df_tiieSwps.copy()
                        posswaps1['swp_nombre'] =\
                            posswaps1['swp_nombre'].str.strip()
                        dfbook = posswaps1\
                            [posswaps1['swp_nombre']==b]\
                                .reset_index(drop=True)
                        if b == 'FINAMEX OPERACIONES SWAPS':
                            
                            dfbook['swp_nomcte'] = dfbook['swp_nomcte'].str.strip()
                            
                            dfbook = dfbook[dfbook['swp_nomcte'] != 'INTERNO']\
                                .reset_index(drop=True)

                        book_pfolio = pf.pfolio.from_posSwaps(dfbook, 
                                                              curves, dt_today)
                        
                        print(f'\nRunning DV01 risk for {b} book in '+'PosSwaps'+
                              str_today+'...')
                        print(f'\nBook {b} Key Rate Risk...')
                        dic_book_valrisk = book_pfolio.get_risk_byBucket()
                        
                        print(f"\nAt {dt_today}\nNPV (MXN): "+\
                              f"{dic_book_valrisk['NPV_Book']:,.0f}")
                        dfbook_br = dic_book_valrisk['DV01_Book']
                        ## Display KRR
                        fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
                        print(fx_rate)
                        
                        print(dfbook_br)
                
                else:
                    print(f'{bookID} does not exist')
                    
        else:
            dic_book_valrisk = dic_risks[bookID]
            book_pfolio = None
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                'TIIE IRS Valuation Tool/Main Codes/'+\
                    'Portfolio Management/OOP Codes/'
                
                
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_file = str_dir + str_inputsFileName + str_inputsFileExt
                
            dic_data = cf.pull_data(str_file, dt_today.date())
            fxrate = dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            
            print(f'\nBook {bookID} Key Rate Risk...')
            print(f"\nAt {dt_today}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']*fxrate:,.0f}")
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            ## Display KRR
            print(fxrate)
            
            
            print(dfbook_br)
        
        cf.save_obj(dic_risks, r'//TLALOC/Cuantitativa/Fixed Income'\
                 '/TIIE IRS Valuation Tool/Blotter/Historical Risks'\
                     '/risk_' + str_today)
        dic_risks['obj'] = {bookID:  book_pfolio}
        
        return dic_risks
    
    elif krr_type == 'granular':
        
        try:
            dic_grisks = cf.load_obj(r'//TLALOC/Cuantitativa/Fixed Income'
                                     '/TIIE IRS Valuation Tool/Blotter'
                                     '/Historical Risks/grisk_' + str_today)
            gbook_found = set(dic_grisks.keys())
        except:
            dic_grisks = {}
            gbook_found = set()
            pass
        
        if bookID not in gbook_found:
            print(f'\nRunning DV01 risk for {bookID} book in ' + 
                  'PosSwaps' + str_today+'...')
            
            book_pfolio = pf.pfolio.from_posSwaps(df_tiieSwps, bookID = bookID)
            df_book = book_pfolio.dfbook.copy()
            print(f'\nBook {bookID} Granular Key Rate Risk...')
            dic_book_valrisk = book_pfolio.get_risk_byBucket(dt_today, curves)
            print(f"\nAt {dt_today}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']:,.0f}")
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            ## Display KRR
            fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            print(fx_rate)
            
            print(dfbook_br)
                
            dt_ystday = ql.Mexico().advance(
                ql.Settings.instance().evaluationDate, 
                ql.Period(-1, ql.Days)).to_date()
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
                'Tool/Main Codes/Portfolio Management/OOP Codes/'
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_file = str_dir + str_inputsFileName + str_inputsFileExt
            
            print('Calculating Carry...')
            bucketcarry = pf.carry(str_file, df_book, dt_today, dt_ystday)
            
            dic_book_valrisk['Carry'] = bucketcarry
            dic_grisks[bookID] = dic_book_valrisk
        
        else:
            book_pfolio = None
            dic_book_valrisk = dic_grisks[bookID]
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                'TIIE IRS Valuation Tool/Main Codes/'+\
                    'Portfolio Management/OOP Codes/'
                
                
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_file = str_dir + str_inputsFileName + str_inputsFileExt
            dic_data = cf.pull_data(str_file, dt_today.date())
            fx_rate = dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            
            print(f'\nBook {bookID} Granular Key Rate Risk...')
            
            print(f"\nAt {dt_today}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']:,.0f}")
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            ## Display KRR
            print(fx_rate)
            
            print(dfbook_br)
    

        cf.save_obj(dic_grisks, r'//TLALOC/Cuantitativa/Fixed Income'
                 '/TIIE IRS Valuation Tool/Blotter/Historical Risks/grisk_'
                 + str_today)
        dic_grisks['obj'] = book_pfolio
        return dic_grisks
    
    
    elif krr_type == 'future':
        
        try:
            dic_frisks = cf.load_obj('Future Risks/risk_'+str_today)
            book_found_f = set(dic_frisks.keys())

        except: 
            book_found_f = set()
            dic_frisks = {}
            
        if bookID not in book_found_f:
            book_int = int(bookID)
            
                   
            print(f'\nRunning DV01 risk for {bookID} book in '+'PosSwaps'+str_today+'...')
            print(f'\nBook {book_int} Future Key Rate Risk...')
            
            
            book_pfolio = pf.pfolio.from_posSwaps(df_tiieSwps, 
                                                  bookID = book_int)
            dic_book_valrisk = book_pfolio.get_risk_byBucket(dt_today, curves)
            #print(book_pfolio.curves.crvMXNTIIE.nodes)
            fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            print(f"\nAt {dt_today}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']*fx_rate:,.0f}")
            
            dic_frisks[bookID] = dic_book_valrisk
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            print(fx_rate)
         
            print(dfbook_br)
            
        else:
            dic_book_valrisk = dic_grisks[bookID]
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
                'TIIE IRS Valuation Tool/Main Codes/'+\
                    'Portfolio Management/OOP Codes/'
                
                
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_file = str_dir + str_inputsFileName + str_inputsFileExt
            dic_data = cf.pull_data(str_file, dt_today.date())
            fxrate = dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            book_pfolio = None
            
            print(f'\nBook {bookID} Future Key Rate Risk...')
            
            print(f"\nAt {dt_today}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']:,.0f}")
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            ## Display KRR
            print(fx_rate)
             
            print(dfbook_br)
        
        dic_frisks['FutureDate'] = str(dt_today)
    
        cf.save_obj(dic_frisks, 'Future Risks/risk_'+str_today)
        dic_frisks['obj'] = book_pfolio
        return dic_frisks
    

#------------  
#  Step NPV
#------------

    
def get_step_NPV(wb: xw.Book, dt_today: datetime, df_tiieSwps: pd.DataFrame,
                 curves: cf.mxn_curves):
    
    
    sheet_scenario = wb.sheets('Scenario')
    sheet_npv = wb.sheets('NPV')
    
    ql_dt_today = ql.Date().from_date(dt_today)
    yst_day = datetime.strptime(ql.Mexico().advance(
        ql_dt_today, ql.Period(-1,ql.Days)).ISO(), '%Y-%m-%d')
    
    if not curves:
        
        print(f'\nCalculating Curves for {dt_today.date()}...')
        str_dir = '//tlaloc/Cuantitativa/Fixed Income/'+\
            'TIIE IRS Valuation Tool/Main Codes/'+\
                'Portfolio Management/OOP Codes/'
            
            
        str_inputsFileName = 'TIIE_CurveCreate_Inputs'
        str_inputsFileExt = '.xlsx'
        str_file = str_dir + str_inputsFileName + str_inputsFileExt
        ql.Settings.instance().evaluationDate = ql_dt_today
        dic_data = cf.pull_data(str_file, dt_today.date())
        if ql.UnitedStates(1).isHoliday(ql_dt_today):
            curves = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
            
        else:   
            curves = cf.mxn_curves(dic_data)
            
        dftiie_12y = cf.add_tiie_tenors(curves, ['%156L'])
        
        curves.change_tiie(dftiie_12y)
    
    else:
        ql.Settings.instance().evaluationDate = ql_dt_today
    
    
    scenario = sheet_npv.range('B2').value
    sheet_scenario.range('H5').value = scenario
    sheet_scenario.api.Calculate()
    
    flt_scenario = sheet_scenario.range('B5:H5').options(
        pd.DataFrame, expand = 'down', header = 1, index = False).value
    flt_scenario.columns = ['MPC', 'FIX', 'A', 'B', 'C', 'D', 'Scenario']
    
    flt_scenario = flt_scenario[['MPC', 'FIX', 'Scenario']]
    
    tiie28 = sheet_scenario.range('H4').value
    
    flt_scenario = pd.concat([pd.DataFrame(
        {'MPC': [yst_day],
         'FIX': [dt_today],
         'Scenario': [tiie28]}),
        flt_scenario])
    
    
    
    ctrols_flag = sheet_npv.range('B3').value
    
    book = sheet_npv.range('B1').value
    
    
    if ctrols_flag:
        ctrols = sheet_npv.range('A4').expand('down').value
        pos = df_tiieSwps[df_tiieSwps['swp_ctrol'].isin(ctrols)]
        pfolio_ctrols = pf.pfolio.from_posSwaps(pos)
    
    else:
        pos = df_tiieSwps.copy()
        pfolio_ctrols = pf.pfolio.from_posSwaps(pos, book)
        
    step_npv = pfolio_ctrols.get_book_step_npv(dt_today, curves, flt_scenario)
    step_npv.drop(['SwpObj', 'index', 'evalDate', 'Fees', 'Counterparty'],
                  axis=1, inplace = True)
    sheet_npv.range('G3').value = step_npv.values 
    
    sheet_npv.range('E2').value = step_npv.NPV.sum()
    sheet_npv.range('E3').value = step_npv.Step_NPV.sum()
    
    
    return curves
    

#----------------
#  Optimization
#----------------


def risk_control(curves, controls: list, posswps: pd.DataFrame, 
                 eval_date: datetime, end=False) -> (pd.DataFrame, float):
    """Calculates the risk for the given swaps.
    
    Each swap is identified by its control number. 

    Parameters
    ----------
    curves : TYPE
        All the curves needed to calculate the risk.
    controls : list
        List of controls for given swaps.
    posswps : pd.DataFrame
        DataFrame with posSwaps data.
    eval_date : datetime
        Evaluation date.
    end : TYPE, optional
        Indicates if the calculated risk is final or if it is a middle step. 
        The default is False.

    Returns
    -------
    dfbook_br : pd.DataFrame
        DataFrame with outright risk and risks by tenor.
    mxn_fx :  float
        Exchange rate.

    """
    
    ql_evaldate = ql.Date().from_date(eval_date)
    yst_date = ql.Mexico().advance(ql_evaldate, ql.Period(-1, ql.Days)).to_date()
    
    df_tiieSwps1 = posswps[posswps['swp_ctrol'].isin(controls)]
    pfolio_ctrols = pf.pfolio.from_posSwaps(df_tiieSwps1)

    fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
    if end:
        print('\nRunning DV01 risk in ' + 'PosSwaps' + yst_date.strftime('%Y%m%d')+'...')
        print('\nKey Rate Risk...')
        
    dic_book_valrisk = pfolio_ctrols.get_risk_byBucket(eval_date, curves)
    
    if end:
        print(f"\nAt {eval_date}\nNPV (MXN): "+\
              f"{dic_book_valrisk['NPV_Book']*fx_rate:,.0f}")
    
    # Bucket Risk (KRR)
    dfbook_br = dic_book_valrisk['DV01_Book']
    dfbook_br1=dfbook_br.str.replace(',','').astype(float)
    outrisk = dfbook_br1[1:].sum()
    dfbook_br['OutrightRisk'] = '{:,.0f}'.format(outrisk)
    
    ## Display KRR
    if end:
        print(fx_rate)
        print(dfbook_br)
        
    return dfbook_br, fx_rate
    
def minimizing_function(swap: pd.Series, missing: float, x: float, 
                        df: pd.DataFrame, variable: str, 
                        results_df: pd.DataFrame, tenor = None):
    """Main function to minimize the difference between actual and target.
    
    Takes an initial missing amount and a starting amount to find swaps that
    get closer to the missing amount. 

    Parameters
    ----------
    swap : pd.Series
        Starting swap to try to match the missing amount. When trying to 
        minimize the risk in the final step, an empty list is passed.
    missing : float
        Missing amount.
    x : float
        Starting amount.
    df : pd.DataFrame
        DataFrame with swaps to choose from.
    variable : str
        Could be 'Utility' or 'Notional'.
    results_df : pd.DataFrame
        DataFrame with resulting swaps.
    tenor : TYPE, optional
        When trying to minimize the risk, tenor where risk is accumulated 
        (in days). The default is None.

    Returns
    -------
    results_df : pd.DataFrame
        DataFrame with the results.
    alternative_df : TYPE
        DataFrame with alternative results. It is never used.
    df : TYPE
        DataFrame without the swaps that were already chosen.

    """
    
    if tenor:
        df_swap = df[(np.abs(df['DTM'] - tenor) < 100)].copy()
        n = 2
        while df_swap.shape[0] == 0:
            df_swap = df[np.abs(df['DTM'] - tenor) < 100 * n].copy()
            n = n + 1
            if n*100 > max(df['DTM']):
                break
    
    else:
        tenor = swap.DTM
        side = swap.Side
        if variable == 'Notional':
            df_swap = df[(np.abs(df['DTM'] - tenor) < 100) & (df['Side'] != side)].copy()
            n = 2
            while df_swap.shape[0] == 0:
                df_swap = df[(np.abs(df['DTM'] - tenor)) < 100 * n & (df['Side'] != side)].copy()
                n = n + 1
                if n*100 > max(df['DTM']):
                    break
        
        elif variable == 'Utility':
            df_swap = df.copy()
    
    alternative_df = results_df.copy()
    
    if missing < 0:
        if x <= missing:
            pass
        else:
            while x > missing:
                if df_swap.shape[0] != 0:
                    missing = missing - x
                    df_swap['Distance'] = np.abs(df_swap[variable] - missing)
                    df_swap = df_swap.sort_values(by='Distance')
                    new_swap = df_swap.iloc[[0]]
                    alternative_df = results_df.copy()
                    results_df = pd.concat([results_df, new_swap])
                    x = new_swap[variable].sum()
                    df_swap.drop(new_swap.index, inplace=True)
                    df.drop(new_swap.index, inplace=True)
                else:
                    break
    else:
        if missing <= x:
            pass
        else:
            while missing > x:
                if df_swap.shape[0] != 0:
                    missing = missing - x
                    df_swap['Distance'] = np.abs(df_swap[variable] - missing)
                    df_swap = df_swap.sort_values(by = 'Distance')
                    new_swap = df_swap.iloc[[0]]
                    alternative_df = results_df.copy()
                    results_df = pd.concat([results_df, new_swap])
                    x = new_swap[variable].sum()
                    df_swap.drop(new_swap.index, inplace=True)
                    df.drop(new_swap.index, inplace=True)
                else:
                    break
    
    return results_df, alternative_df, df
        
    
def min_function_v1(missing_amount: float, utility: float, swap: pd.Series, 
                    missing: float, x: float, df: pd.DataFrame,
                    results_df: pd.DataFrame, tenor: int = None): 
    """
    

    Parameters
    ----------
    missing_amount : float
        DESCRIPTION.
    utility : float
        DESCRIPTION.
    swap : pd.Series
        DESCRIPTION.
    missing : float
        DESCRIPTION.
    x : float
        DESCRIPTION.
    df : pd.DataFrame
        DESCRIPTION.
    results_df : pd.DataFrame
        DESCRIPTION.
    tenor : int, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    results_df : TYPE
        DESCRIPTION.
    alternative_df : TYPE
        DESCRIPTION.
    df : TYPE
        DESCRIPTION.

    """
                    
    

    alternative_df = results_df.copy()
    
    if not tenor:
        target_notional = -swap.Notional
        tenor = swap.DTM
        side = swap.Side
        df_swap = df[(np.abs(df['DTM'] - tenor) < 100) & 
                     (df['Side'] != side) & 
                     (np.abs(df['Utility'] + utility - missing_amount) < \
                      np.abs(missing_amount * .1))].copy()
        
        n = 2
        while df_swap.shape[0] == 0:
            df_swap = df[(np.abs(df['DTM'] - tenor) < 100*n) & 
                         (df['Side'] != side) & 
                         (np.abs(df['Utility'] + utility - missing_amount) < \
                          missing_amount * .25)].copy()
            n = n + 1
            if n*100 > max(df['DTM']):
                break
            
    else:
        df_swap = df[(np.abs(df['DTM'] - tenor) < 100)].copy()
        n = 2
        while df_swap.shape[0] == 0:
            df_swap = df[(np.abs(df['DTM'] - tenor) < 100*n)].copy()
            n = n + 1
            if n*100 > max(df['DTM']):
                break

            
        
    if missing < 0:
        if x <= missing:
            pass
        else:
            while x > missing:
                if df_swap.shape[0] != 0:
                    missing = missing - x
                    df_swap['Distance'] = np.abs(df_swap['Notional'] - missing)
                    df_swap = df_swap.sort_values(by='Distance')
                    new_swap = df_swap.iloc[[0]]
                    alternative_df = results_df.copy()
                    results_df = pd.concat([results_df, new_swap])
                    utility = results_df['Utility'] + utility
                    x = new_swap['Notional'].sum()
                    df_swap.drop(new_swap.index, inplace=True)
                    df.drop(new_swap.index, inplace=True)
                else:
                    break
    else:
        if missing <= x:
            pass
        else:
            while missing > x:
                if df_swap.shape[0] != 0:
                    missing = missing - x
                    df_swap['Distance'] = np.abs(df_swap['Notional'] - missing)
                    df_swap = df_swap.sort_values(by = 'Distance')
                    new_swap = df_swap.iloc[[0]]
                    alternative_df = results_df.copy()
                    results_df = pd.concat([results_df, new_swap])
                    utility = results_df['Utility'] + utility
                    x = new_swap['Notional'].sum()
                    df_swap.drop(new_swap.index, inplace=True)
                    df.drop(new_swap.index, inplace=True)
                else:
                    break
    
    return results_df, alternative_df, df
    
    
# Standardized distance
def min_function_v2(missing_amount: float, utility: float, swap: pd.Series, 
                    missing: float, x: float, df: pd.DataFrame, 
                    results_df: pd.DataFrame, tenor: int = None):
                    
    
    alternative_df = results_df.copy()
    
    if not tenor:
        target_notional = -swap.Notional
        tenor = swap.DTM
        side = swap.Side
        df_swap = df[(np.abs(df['DTM'] - tenor) < 100) & 
                     (df['Side'] != side) & 
                     (np.abs(df['Utility'] + utility - missing_amount) < \
                      missing_amount * .1)].copy()
        
        n = 2
        while df_swap.shape[0] == 0:
            df_swap = df[(np.abs(df['DTM'] - tenor) < 100*n) & 
                         (df['Side'] != side) & 
                         (np.abs(df['Utility'] + utility - missing_amount) < \
                          missing_amount * .1)].copy()
            n = n + 1
            if n*100 > max(df['DTM']):
                break
            
    else:
        df_swap = df[(np.abs(df['DTM'] - tenor) < 100)].copy()
        n = 2
        while df_swap.shape[0] == 0:
            df_swap = df[(np.abs(df['DTM'] - tenor) < 100*n)].copy()
            n = n + 1
            if n*100 > max(df['DTM']):
                break
        
    y = utility
    
    if missing < 0:
        if x <= missing:
            pass
        else:
            while x > missing:
                if df.shape[0] != 0:
                    missing = missing - x
                    missing_amount = missing_amount - y
                    
                    standard_notional = \
                        (missing - df['Notional'].mean())/df['Notional'].std()
                        
                    standard_amount = \
                        (missing_amount - df['Utility'].mean())/df['Utility']\
                            .std()
                            
                    standard_tenor = (tenor - df['DTM'].mean())/df['DTM'].std()
                    
                    df['Standard Utility'] = (df['Utility'] - df['Utility']\
                                              .mean())/df['Utility'].std()
                    df['Standard DTM'] = (df['DTM'] - df['DTM'].mean())\
                        /df['DTM'].std()
                    df['Standard Notional'] = (df['Notional'] - df['Notional']\
                                               .mean())/df['Notional'].std()
                    
                    df['Distance'] = \
                        (np.abs(df['Standard Notional'] - standard_notional)**2) + \
                        (np.abs(df['Standard Utility'] - standard_amount)**2)+ \
                        (np.abs(df['Standard DTM'] - standard_tenor)**2)*1.2
                    
                    #df['Distance'] = np.abs(df['Notional'] - missing)
                    df = df.sort_values(by='Distance')
                    new_swap = df.iloc[[0]]
                    alternative_df = results_df.copy()
                    results_df = pd.concat([results_df, new_swap])
                    x = new_swap['Notional'].sum()
                    y = new_swap['Utility'].sum()
                    df.drop(new_swap.index, inplace=True)
                else:
                    break
    else:
        if missing <= x:
            pass
        else:
            while missing > x:
                if df.shape[0] != 0:
                    missing = missing - x
                    missing_amount = missing_amount - y
                    
                    standard_notional = (missing - df['Notional'].mean())\
                        /df['Notional'].std()
                        
                    standard_amount = (missing_amount - df['Utility'].mean())\
                        /df['Utility'].std()
                        
                    standard_tenor = (tenor - df['DTM'].mean())/df['DTM'].std()
                    
                    df['Standard Utility'] = (df['Utility'] - df['Utility']\
                                              .mean())/df['Utility'].std()
                    df['Standard DTM'] = (df['DTM'] - df['DTM'].mean())\
                        /df['DTM'].std()
                    df['Standard Notional'] = (df['Notional'] - df['Notional']\
                                               .mean())/df['Notional'].std()
                    
                    df['Distance'] = \
                        (np.abs(df['Standard Notional'] - standard_notional)**2)*.2 + \
                        (np.abs(df['Standard Utility'] - standard_amount)**2)*.1 + \
                            (np.abs(df['Standard DTM'] - standard_tenor)**2)*.7

                    df = df.sort_values(by='Distance')
                    new_swap = df.iloc[[0]]
                    alternative_df = results_df.copy()
                    results_df = pd.concat([results_df, new_swap])
                    x = new_swap['Notional'].sum()
                    y = new_swap['Utility'].sum()
                    df.drop(new_swap.index, inplace=True)
                else:
                    break
    
    return results_df, alternative_df, df
    
            
        
def risky_tenor_notional(risk_df: pd.DataFrame, evaluation_date: datetime, 
                         curves: pf.cf.mxn_curves, risky_tenor: int = None):
    """ Risky Tenor Notional
    

    Parameters
    ----------
    risk_df : pd.DataFrame
        KRR Risk of Portfolio.
    evaluation_date : datetime
    curves : pf.cf.mxn_curves
        Curves for vauating
    risky_tenor : int, optional
        Tenor to evaluate. The default is None.

    Returns
    -------
    risky_tenor : TYPE
        Tenor with risk.
    risky_notional : TYPE
        Notional of risk.

    """
    
    
    
    if not risky_tenor:
        risk = float(risk_df.OutrightRisk.replace(',', ''))
        closest_risk_df = pd.DataFrame(risk_df.iloc[1:], columns = ['Risk'])
        closest_risk_df['Risk'] = closest_risk_df['Risk'].str.replace(
            ',', '').astype(float)
        closest_risk_df['Distance'] = np.abs(closest_risk_df['Risk'] - risk)
        #closest_risk_df['Distance'] = np.abs(closest_risk_df['Risk'])
        #closest_risk_df = closest_risk_df.sort_values(by='Distance', ascending=False)
        closest_risk_df = closest_risk_df.sort_values(by='Distance')
        risky_tenor = closest_risk_df.index[0].replace('%', '')
        risky_tenor = int(risky_tenor.replace('L', ''))
    
    else:
        risky_tenor_l = '%' + str(risky_tenor) + 'L'
        risk = float(risk_df.loc[risky_tenor_l].replace(',', ''))
        
    
    
    mx_calendar = ql.Mexico()
    todays_date = evaluation_date
    todays_date = ql.Date(todays_date.day, todays_date.month, todays_date.year)
    start = mx_calendar.advance(todays_date, ql.Period(1 , ql.Days))
    maturity = start + ql.Period((risky_tenor * 28) , ql.Days)
    notional = 100000000
    dic_data = {k:v.copy() for k,v in curves.dic_data.items()}
    rule = ql.DateGeneration.Backward
    rate = dic_data['MXN_TIIE'][
        dic_data['MXN_TIIE']['Period'] == risky_tenor]['Quotes'].values[0]/100
    
    mxn_fx = dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
   
    # Swaps construction
    swap_valuation = pf.cf.tiieSwap(start.to_date(), maturity.to_date(),
                                    notional, rate, curves, rule)
    flat_dv01 = swap_valuation.flat_dv01()
    
    dv01_100mn = flat_dv01/mxn_fx
    # npv_100mn = swap_valuation[0].NPV()
    
    risky_notional = risk * notional / dv01_100mn
    
    return  risky_tenor, risky_notional

def manual_tenor_risk(wb: xw.Book, k: str, final_results_df: pd.DataFrame,
                      swaps_book: pd.DataFrame, final_swaps: pd.DataFrame, 
                      evaluation_date: datetime, df_tiieSwps1: pd.DataFrame,
                      curves: pf.cf.mxn_curves, row_range: int):
    
    optim_sheet = wb.sheets('Optimization')
    initial_amount = optim_sheet.range('B1').value

    better_risk = input('\nWhich tenor risk would you like to make better? If none, press "0": ')
    
    df_a = swaps_book.copy()
    df_a.drop(final_swaps.index, inplace=True)
    # final_results_df = pd.DataFrame()
    risk_df = final_results_df.drop(['Number of trades', 'Final position'])[k]
    o = k[7:]
    print('\nOption '+o+' before adjusted risk: ')
    print('\nTotal Notional: {:,.0f}'.format(final_swaps['Notional'].sum()))
    print('Final Position: {:,.0f}'.format(initial_amount + 
                                            final_swaps['Utility'].sum()))
    print('Number of trades: ', final_swaps.shape[0])
    
    print(risk_df)
    
    if len(o) == 1:
        o = k[7:] + '.0.1'
    else:
        o = k[7:] + '.1'
    
    final_results = {k + '.0': final_swaps}
    while better_risk != '0':
        
        risky_tenor = int(better_risk)
        
        risky_tenor, risky_notional = risky_tenor_notional(
            risk_df, evaluation_date, curves, risky_tenor)
        
        target_notional = risky_notional
        notional = 0
        missing_notional = target_notional - notional
        notional_swaps = pd.DataFrame()
    
        
        results_df, alternative_df, df_a = minimizing_function(
            [], missing_notional, notional, df_a, 'Notional', notional_swaps, 
            risky_tenor*28)
        
        final_swaps = pd.concat([final_swaps, results_df])
        
        position = [initial_amount + final_swaps['Utility'].sum()]
        trade_no = [final_swaps.shape[0]]
        final_df = pd.DataFrame(columns = ['Option ' + str(o)])
        final_df.loc['Final position'] = position
        final_df.loc['Number of trades'] = trade_no
        
        print('\nOption '+o+' ')
        print('\nTotal Notional: {:,.0f}'.format(final_swaps['Notional'].sum()))
        print('Final Position: {:,.0f}'.format(position[0]))
        print('Number of trades: ', final_swaps.shape[0])
        
        
        
        risk_df, mxn_fx = risk_control(curves, final_swaps['Control'].to_list(), 
                                       df_tiieSwps1, evaluation_date, True)
        
        
        
        
        final_df = pd.concat([final_df, pd.DataFrame(risk_df.rename('Option ' + str(o)))])
        final_results_df = pd.concat([final_results_df, final_df], axis=1)
        
        final_results['Option ' + str(o)] = final_swaps
        
        o = o[:3] + str(round(float(o[-2:])+.1,1))[1:]
        
        better_risk = input('\nWhich tenor risk would you like to make better? If none, press "0": ')
            
        
        
    column_order = final_results_df.columns.to_list()
    column_order.sort()
    final_results_df = final_results_df[column_order]
        
    
    optim_sheet.range('D2').expand('table').clear_contents()
    
    optim_sheet.range('D2').value = final_results_df
    optim_sheet.range('D2').value = 'Choose'    
    
    final_list = []
    a = 1
    a = (input('\nWould you like to save any of the options? Y/N: ')).lower()
    
    if a == 'y':
        print('\nPlease indicate the options you want to save. '+
              'If you want to save all press "a". When done press "0".')
        o = 1
        while (o != '0' and o != 'a'):
            o = input('Option: ')
            if o.lower() == 'a':
                final_list = list(final_results.keys())
            elif o != '0': 
                final_list.append('Option ' + o)
    
    accepted_results = {o: final_results[o] for o in final_list}
    xw_columns = ['Control', 'Book', 'Monto', 'swp_nombre', 'Start_Date', 
                  'End_Date', 'Side', 'DTM', 'Utility', 'Rate']
    
    
        
    
    for k in accepted_results.keys():
        if optim_sheet.range(row_range, 1).value != None:
            optim_sheet.range((row_range, 1), 
                              (row_range,2)).expand('down').clear_contents()
            optim_sheet.range(row_range, 3).expand('table').clear_contents()
            
        optim_sheet.range((row_range, 1), 
                          (row_range,2)).expand('down').value = final_results_df[[k]]
        optim_sheet.range(row_range, 1).value = 'Detailed'
        

        # results_book.sheets.add(k)
        sheet_df = accepted_results[k][xw_columns].set_index('Control')
        sheet_df['Start_Date'] = pd.to_datetime(sheet_df['Start_Date'], 
                                                format='%Y%m%d')
        sheet_df['End_Date'] = pd.to_datetime(sheet_df['End_Date'], 
                                              format='%Y%m%d')
        optim_sheet.range(row_range, 3).value = sheet_df
        
        row_range = max(optim_sheet.range(row_range,1).end('down').row, 
                        optim_sheet.range(row_range,3).end('down').row)+2
    
    return accepted_results, row_range
    
  
    
def optimization(wb: xw.Book, dt_today: datetime, df_tiieSwps: pd.DataFrame, 
                 curves: cf.mxn_curves):
    
    curves = get_curves(dt_today, curves)
    fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'].iloc[0]
    
    optim_sheet = wb.sheets('Optimization')
    
    initial_amount = optim_sheet.range('B1').value
    target_amount = optim_sheet.range('B2').value
    book = optim_sheet.range('B3').value
    
    if type(book) == float or type(book) == int:
        book = int(book)
        books = [book]
        
    else:
        books = [2342, 1814, 8085]
    
    socios = ['CITIBANK SWAPS CHICAGO', 'GOLDMAN SACHS SWAPS CHICAGO']
    
    final_results = {}
    
    posswps_df_complete  = df_tiieSwps.copy()
    
    posswps_df_complete['Start_Date'] = pd.to_datetime(
        posswps_df_complete['swp_fec_ini'], format='%Y%m%d')
    posswps_df_complete = posswps_df_complete[
        posswps_df_complete['Start_Date'] < datetime(2023, 12, 21)]
    cols = [posswps_df_complete.columns[i] 
            for i in range(len(posswps_df_complete.columns)) 
            if i in [2, 4, 5, 8, 9, 10, 18, 22, 23, 19, 32]]
    
    posswps_df = posswps_df_complete[cols].copy()
    posswps_df['Rate'] = posswps_df['swp_val_i_pa'] + posswps_df['swp_val_i']

    columns_dic = {'swp_ctrol': 'Control', 'swp_usuario': 'Book', 
                   'swp_monto': 'Monto', 'swp_fec_ini': 'Start_Date', 
                   'swp_fec_vto': 'End_Date', 'swp_cpa_vta': 'Side', 
                   'swp_dvto': 'DTM', 'swp_utilidad': 'Utility'}

    posswps_df = posswps_df.rename(columns=columns_dic)
    posswps_df['swp_nombre'] = posswps_df['swp_nombre'].str.strip()

    swaps_book = posswps_df[(posswps_df['Book'].isin(books)) & 
                            (posswps_df['swp_nombre'].isin(socios))].copy()

    swaps_book['Notional'] = np.select([swaps_book['Side']=='VTA'], 
                                       [swaps_book['Monto']*(-1)], 
                                       swaps_book['Monto'])
    
    missing_amount = target_amount - initial_amount
    df = swaps_book.copy()
    df['Distance'] = np.abs(df['Utility'] - missing_amount)
    df = df.sort_values(by='Distance')
    start_swap = df.iloc[[0]]
    df.drop(start_swap.index, inplace=True)
    
    utility = start_swap['Utility'].sum()
    
    # Initial swaps that match target utility
    initial_swaps, alternative_initial, df = minimizing_function(
        start_swap.iloc[0], missing_amount, utility, df, 'Utility', start_swap)
    
    options_dic = {1: minimizing_function, 2: min_function_v1, 3: min_function_v2}
    final_results = {}
    
    final_results_df = pd.DataFrame()
    # Calculate the rest of the swaps to match the initial notionals and tenors
    for k in options_dic.keys():
        final_swaps = initial_swaps.copy()
        utility = initial_swaps['Utility'].sum()
        df_a = swaps_book.copy()
        df_a.drop(initial_swaps.index, inplace=True)
        for i, swap in initial_swaps.iterrows():
            target_notional = -swap.Notional
            tenor = swap.DTM
            side = swap.Side
            notional = 0
            missing_notional = target_notional - notional
            notional_swaps = pd.DataFrame()
            if k == 1:
                results_df, alternative_df, df_a = options_dic[k](
                    swap, missing_notional, notional, df_a, 'Notional', 
                    notional_swaps)
            else:
                results_df, alternative_df, df_a = options_dic[k](
                    missing_amount, utility, swap, missing_notional, notional, df_a, 
                    notional_swaps)
            final_swaps = pd.concat([final_swaps, results_df])
            utility = final_swaps['Utility'].sum()
        
        print('\nOption '+str(k)+' before adjusted risk: ')
        print('\nTotal Notional: {:,.0f}'.format(final_swaps['Notional'].sum()))
        print('Final Position: {:,.0f}'.format(initial_amount + 
                                                final_swaps['Utility'].sum()))
        print('Number of trades: ', final_swaps.shape[0])
        risk, mxn_fx = risk_control(curves, final_swaps['Control'].to_list(), 
                                    posswps_df_complete, dt_today, True)
        
        risky_tenor, risky_notional = risky_tenor_notional(risk, dt_today, 
                                                           curves)
        
        target_notional = risky_notional
        notional = 0
        missing_notional = target_notional - notional
        notional_swaps = pd.DataFrame()
        
        df_b = df_a.copy()
        if k == 1:
            results_df_alt, alternative_df, df_b = options_dic[k](
                [], missing_notional, notional, df_b, 'Notional', 
                notional_swaps, risky_tenor*28)
            
        else:
            results_df_alt, alternative_df, df_b = options_dic[k](
                missing_amount, utility, [], missing_notional, notional, df_b, 
                notional_swaps, risky_tenor*28)
        
        results_df, alternative_df, df_a = minimizing_function(
            [], missing_notional, notional, df_a, 'Notional', notional_swaps, 
            risky_tenor*28)
        
        final_swaps_alt = pd.concat([final_swaps.copy(), results_df_alt])
        final_swaps = pd.concat([final_swaps, results_df])
        #final_swaps_alt = pd.concat([final_swaps, results_df_alt])
        
        final_results['Option ' + str(k) + '.1'] = final_swaps_alt
        
        print('\nOption '+str(k)+' alternative')
        # print('\nTotal Notional: {:,.0f}'.format(final_swaps_alt['Notional'].sum()))
        print('Final Position: {:,.0f}'.format(initial_amount + 
                                               final_swaps_alt['Utility'].sum()))
        print('Number of trades: ', final_swaps_alt.shape[0])
        risk_f_alt, mxn_fx = risk_control(curves, final_swaps_alt['Control'].to_list(), 
                                      posswps_df_complete, dt_today, True)
        
        final_results['Option ' + str(k)] = final_swaps
        print('\nOption '+str(k))
        # print('\nTotal Notional: {:,.0f}'.format(final_swaps['Notional'].sum()))
        print('Final Position: {:,.0f}'.format(initial_amount + 
                                               final_swaps['Utility'].sum()))
        print('Number of trades: ', final_swaps.shape[0])
        risk_f, mxn_fx = risk_control(curves, final_swaps['Control'].to_list(), 
                                      posswps_df_complete, dt_today, True)
        
        positions = [initial_amount + final_swaps_alt['Utility'].sum(), 
                     initial_amount + final_swaps['Utility'].sum()]
        trade_no = [final_swaps_alt.shape[0], final_swaps.shape[0]]
        final_df = pd.DataFrame(columns = ['Option ' +str(k)+'.1', 'Option '+str(k)])
        final_df.loc['Final position'] = positions
        final_df.loc['Number of trades'] = trade_no
        risk_df = pd.DataFrame(risk_f_alt).merge(pd.DataFrame(risk_f), how='left', 
                                                 left_index=True, right_index=True)
        risk_df.rename(columns = {risk_df.columns[0]: 'Option ' +str(k)+'.1',
                                  risk_df.columns[1]: 'Option '+str(k)}, inplace=True)
        final_df = pd.concat([final_df, risk_df])
        final_results_df = pd.concat([final_results_df, final_df], axis=1)
        
    
    final_results_alt = {}
    
    final_results_alt_df = pd.DataFrame()
    # Calculate the rest of the swaps to match the initial notionals and tenors
    for k in options_dic.keys():
        final_swaps = alternative_initial.copy()
        utility = alternative_initial['Utility'].sum()
        df_a = swaps_book.copy()
        df_a.drop(alternative_initial.index, inplace=True)
        for i, swap in alternative_initial.iterrows():
            target_notional = -swap.Notional
            tenor = swap.DTM
            side = swap.Side
            notional = 0
            missing_notional = target_notional - notional
            notional_swaps = pd.DataFrame()
            if k == 1:
                results_df, alternative_df, df_a = options_dic[k](
                    swap, missing_notional, notional, df_a, 'Notional', 
                    notional_swaps)
            else:
                results_df, alternative_df, df_a = options_dic[k](
                    missing_amount, utility, swap, missing_notional, notional, df_a, 
                    notional_swaps)
            final_swaps = pd.concat([final_swaps, alternative_df])
            utility = final_swaps['Utility'].sum()
        
        # print('\nOption '+str(k)+' before adjusted risk: ')
        # print('\nTotal Notional: {:,.0f}'.format(final_swaps['Notional'].sum()))
        # print('Final Position: {:,.0f}'.format(initial_amount + 
        #                                        final_swaps['Utility'].sum()))
        # print('Number of trades: ', final_swaps.shape[0])
        risk, mxn_fx = risk_control(curves, final_swaps['Control'].to_list(), 
                                    posswps_df_complete, dt_today)
        
        risky_tenor, risky_notional = risky_tenor_notional(risk, dt_today, 
                                                           curves)
        
        target_notional = risky_notional
        notional = 0
        missing_notional = target_notional - notional
        notional_swaps = pd.DataFrame()
        
        df_b = df_a.copy()
        if k == 1:
            results_df_alt, alternative_df_alt, df_b = options_dic[k](
                [], missing_notional, notional, df_b, 'Notional', 
                notional_swaps, risky_tenor*28)
            
        else:
            results_df_alt, alternative_df, df_b = options_dic[k](
                missing_amount, utility, [], missing_notional, notional, df_b, 
                notional_swaps, risky_tenor*28)
        
        results_df, alternative_df, df_a = minimizing_function(
            [], missing_notional, notional, df_a, 'Notional', notional_swaps, 
            risky_tenor*28)
        
        final_swaps_alt = pd.concat([final_swaps.copy(), alternative_df_alt])
        final_swaps = pd.concat([final_swaps, alternative_df])
        #final_swaps_alt = pd.concat([final_swaps, results_df_alt])
        
        final_results['Option ' + str(k) + '.2'] = final_swaps_alt
        
        print('\nOption '+str(k)+' alternative')
        # print('\nTotal Notional: {:,.0f}'.format(final_swaps_alt['Notional'].sum()))
        print('Final Position: {:,.0f}'.format(initial_amount + 
                                               final_swaps_alt['Utility'].sum()))
        print('Number of trades: ', final_swaps_alt.shape[0])
        risk_f_alt, mxn_fx = risk_control(curves, final_swaps_alt['Control'].to_list(), 
                                      posswps_df_complete, dt_today, True)
        
        final_results['Option ' + str(k)+'.3'] = final_swaps
        print('\nOption '+str(k))
        # print('\nTotal Notional: {:,.0f}'.format(final_swaps['Notional'].sum()))
        print('Final Position: {:,.0f}'.format(initial_amount + 
                                               final_swaps['Utility'].sum()))
        print('Number of trades: ', final_swaps.shape[0])
        risk_f, mxn_fx = risk_control(curves, final_swaps['Control'].to_list(), 
                                      posswps_df_complete, dt_today, True)
        
        positions = [initial_amount + final_swaps_alt['Utility'].sum(), 
                     initial_amount + final_swaps['Utility'].sum()]
        trade_no = [final_swaps_alt.shape[0], final_swaps.shape[0]]
        final_df = pd.DataFrame(columns = ['Option ' +str(k)+'.2', 'Option '+str(k) + '.3'])
        final_df.loc['Final position'] = positions
        final_df.loc['Number of trades'] = trade_no
        risk_df = pd.DataFrame(risk_f_alt).merge(pd.DataFrame(risk_f), how='left', 
                                                 left_index=True, right_index=True)
        risk_df.rename(columns = {risk_df.columns[0]: 'Option ' +str(k)+'.2',
                                  risk_df.columns[1]: 'Option '+str(k)+'.3'}, inplace=True)
        final_df = pd.concat([final_df, risk_df])
        final_results_df = pd.concat([final_results_df, final_df], axis=1)
    
    column_order = final_results_df.columns.to_list()
    column_order.sort()
    final_results_df = final_results_df[column_order]
        
    optim_sheet.range('D2').expand('table').clear_contents()
    
    optim_sheet.range('D2').value = final_results_df
    optim_sheet.range('D2').value = 'Choose'
    
    # results_book_sheet.range('A1').value = final_results_df    
    
    final_list = []
    a = 1
    a = (input('\nWould you like to save any of the options? Y/N: ')).lower()
    
    if a == 'y':
        print('\nPlease indicate the options you want to save. '+
              'If you want to save all press "a". When done press "0".')
        o = 1
        while (o != '0' and o != 'a'):
            o = input('Option: ')
            if o.lower() == 'a':
                final_list = list(final_results.keys())
            elif o != '0': 
                final_list.append('Option ' + o)
    
    accepted_results = {o: final_results[o] for o in final_list}
    
    optim_sheet.range('A43:L1000').clear_contents()
    complete_accepted_results = {}
    row_range = 23
    
    for k in accepted_results.keys():
        final_results_df_k = final_results_df[[k]]
        
        final_swaps = accepted_results[k].copy()
        print(final_results_df_k)
        new_accepted_results, row_range = manual_tenor_risk(wb, k,
                                                 final_results_df_k, 
                                                 swaps_book, final_swaps, 
                                                 dt_today, posswps_df_complete, 
                                                 curves, row_range)

        for k in new_accepted_results:
            complete_accepted_results[k] = new_accepted_results[k].copy()
            
    return curves








