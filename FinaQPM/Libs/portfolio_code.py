"""Portfolio Management Code

This script contains the he main code for Portfolio Management

Main Code for oprtfolio Management
"""


import os
import sys
import time
import numpy as np
import pandas as pd
import xlwings as xw
from datetime import datetime
import QuantLib as ql

# Quant management functions
import updates_funs as uf



        
def menu() -> int:
    """Displays user menu
    
    Returns
    -------
    int
        Option for operating

    """
    print('\nPlease choose an option: ')
    print('\n1) Book KRR            8) Trioptima' +
          '\n2) Book PnL            9) Main Updates' +
          '\n3) By Control          10) Morning Spreads' +
          '\n4) NPV                 11) Spreads Monthly Update' +
          '\n5) Analysis By Tenor   12) Optimization'+
          '\n6) Book gamma          13) Change Date'+
          '\n7) Prestamos Internos  14) End Session')

    option = int(float(input('\nOption:  ')))
    return option

def main(pf_file):
    
    # Date Assignment
    dateIsNotOk = True
    while dateIsNotOk:
        print('\n\nToday Date')
        input_year = int(input('\tYear: '))
        input_month = int(input('\tMonth: '))
        input_day = int(input('\tDay: '))
        print('\n')
        try:
            dt_today = datetime(input_year, input_month, input_day)
            dateIsNotOk = False
        except:
            print('Wrong date! Try again pls.')
            dateIsNotOk = True
    
    ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
    ql.Settings.instance().evaluationDate = ql_dt_today
    ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
    dt_posswaps = ql_dt_yest.to_date()
    
    wb = xw.Book(pf_file)
    
    ###############################################################################
    
    # Portfolios
    # Swaps File
    str_dt_posswaps = dt_posswaps.strftime('%Y%m%d')
    str_posswps_file = r'//TLALOC/tiie/posSwaps/PosSwaps'+str_dt_posswaps+'.xlsx' # PosSwaps file 
    
    try:
        print('Searching PosSwaps'+str_dt_posswaps+'.xlsx...')
        df_tiieSwps = pd.read_excel(str_posswps_file)# Swaps' Portfolio
        print('PosSwaps'+str_dt_posswaps+'.xlsx successfully found!')
    except:
        print('\n\nPosSwaps'+str_dt_posswaps+'.xlsx not found!')
        sys.exit('Please make sure PosSwaps'+str_dt_posswaps+' file exists')
    
    curves = None
    
    option = 0
    while option != 14:
        option = menu()
        
        # Book KRR
        if option == 1:
            dics, curves = uf.bucketRisk(dt_today, df_tiieSwps, curves)
        
        # Book PnL
        if option == 2:
            data8085 = uf.pf.PnL(dt_today)
        
        # Control KRR
        if option == 3:

            curves = uf.by_ctrol(dt_today, wb, df_tiieSwps, curves)
            
        # Analysis By tenor
        if option == 4: 
            curves = uf.get_step_NPV(wb, dt_today, df_tiieSwps, curves)
             
        # Analysis By tenor
        if option == 5: 
            uf.analysis_byTenor(pf_file)
        
        # Gamma
        if option == 6:
            gamma_df = uf.gamma_function(pf_file, dt_today)
        
        # Prestamos Internos
        if option == 7: 
            uf.prestamos_internos(dt_today)
        
        # Prestamos Internos
        if option == 8: 
            uf.trioptima(dt_today, df_tiieSwps, wb)

        # Main Updates  
        if option == 9:
            data8085 = uf.main_updates(dt_today)
        
        
        # Morning Spreads
        if option == 10:
            curves = uf.spreads_main(dt_today.date(), curves)
        
        # Spreads Monthly Update
        if option == 11:
            print(' Under Construction '.center(52,'#')+
                  '\n' + '    ,               '.center(52,'#')+
                  '\n' + '   /(  ___________  '.center(52,'#')+
                  '\n' + '  |  >:===========` '.center(52,'#')+
                  '\n' + '   )(               '.center(52,'#')+
                  '\n' + '   ""               '.center(52,'#'))
        
        # Optimization
        if option == 12:
            
            curves = uf.optimization(wb, dt_today, df_tiieSwps, curves)
        
        
        # Change Date
        if option == 13:
            
            # Date Assignment
            dateIsNotOk = True
            while dateIsNotOk:
                print('\nToday Date')
                input_year = int(input('\tYear: '))
                input_month = int(input('\tMonth: '))
                input_day = int(input('\tDay: '))
                print('\n')
                try:
                    dt_today = datetime(input_year, input_month, input_day)
                    dateIsNotOk = False
                except:
                    print('Wrong date! Try again pls.')
                    dateIsNotOk = True
            ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
            ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
            dt_posswaps = ql_dt_yest.to_date()
            str_dt_posswaps = dt_posswaps.strftime('%Y%m%d')
            try:
                print('Searching PosSwaps'+str_dt_posswaps+'.xlsx...')
                df_tiieSwps = pd.read_excel(str_posswps_file)# Swaps' Portfolio
                print('PosSwaps'+str_dt_posswaps+'.xlsx successfully found!')
            except:
                print('\n\nPosSwaps'+str_dt_posswaps+'.xlsx not found!')
                sys.exit('Please make sure PosSwaps'+str_dt_posswaps+' file exists')
                
                
            ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
            
            curves = None
            ql.Settings.instance().evaluationDate = ql_dt_today
            ql_dt_yest = ql.Mexico().advance(ql_dt_today,-1,ql.Days)
            a = datetime.now()
            print(f'\nCalculating Curves for {dt_today.date()}...')
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
                'Tool/Main Codes/Portfolio Management/OOP Codes/'
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_input = str_dir + str_inputsFileName + str_inputsFileExt
            dic_data = uf.pf.cf.pull_data(str_input, dt_today.date())

            if ql.UnitedStates(1).isHoliday(ql_dt_today):
                curves = uf.pf.cf.mxn_curves(dic_data, None, ['MXN_OIS'])

            else:
                curves = uf.pf.cf.mxn_curves(dic_data)
            dftiie12 = uf.pf.cf.add_tiie_tenors(curves, ['%156L'])
            curves.change_tiie(dftiie12)
            print('Calculating KRR curves...')
            curves.KRR_crvs(True, True)
            b = datetime.now()
            print('Curves Calculated!')
            print(b-a)
        
        
        
        if option == 14:
            ruleta = np.random.randint(1,10)
            
            if ruleta == 7:
                print('\n')
                print('#'.center(52, '#'))
                print(' Lets dance! '.center(52, '#'))
                print('#'.center(52, '#'))
                time.sleep(1)
                for i in range(10):
                    print('\n  (•_•)  '+
                          '\n  <) )╯  '+
                          '\n  / \    '+
                          '\n\n\n\n\n\n')
                    time.sleep(.35)
                    print('\n  (•_•)  '+
                          '\n  \( (>  '+
                          '\n   / \   '+
                          '\n\n\n\n\n\n')
                    time.sleep(.35)
        
        
        
        
            
            
            
#%%

if __name__ == '__main__':

    main('//tlaloc/Cuantitativa/Fixed Income/Quant Team/Esteban/FinaQPM/Quant Portfolio Management.xlsx')      
        
        