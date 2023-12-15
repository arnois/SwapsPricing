#---------------
#  Description
#---------------

# Last update 24/07/2023
"""Main code for IRS TIIE trading

Code oriented for day to day user operation. The excel file named 
"TIIE_IRS_Data" is needed for its correct functionality.

When run, it will update all curves with the data available in the 
input sheets (black colored) of the excel file and then evaluates the 
swaps selected in the "Pricing" sheet


It has 16 options for operating:
    
    1. Pricing: Evaluation for the swaps selected in the "Pricing" sheet
            of the excel file.       
    2. Update Curves: Updates all curves and the evaluates the swaps 
            selected in the "Pricing" sheet.
    3. Corros: Fills Corros file with best bid offer rates via spreads.      
    4. Collapse: Evaluates the swaps given in the "Collapse" sheet.
    5. Blotter: Evaluates the swaps given in the "Blotter" sheet which Book
            is defined in the "Risk" Sheet, inputs the Key Rate Risk for 
            each swap in the "Blotter" sheet, Updates the Risk taken on 
            the "Risk" sheet and updates the "Desk Blotter" with the swaps
            evluated by other users.      
    6. Upload Blotter: Transforms the swpas given in the bloter to the 
            gven Blotter format needed for confirmed traded swaps.  
    7. Fwd Starting: Calculates rates for IMM's and forward starting trades.
    8. Pricing Granular: Prices specified swaps with granular risk.
    9. Update Granular: Updates curves with granularity.
    10. Collapse Granular: Calculates collapse trades with granularity.
    11. Blotter Granular: Calculates blotter with granularity.
    12. Short End Pricing: Inputs the forward implied TIIE rates and Discount
            Factors in the "Short_End_Pricng" sheet. 
    13. Graph: Graphs the given time series, spreads, and/or butterflies.
    14. Collapse Blotter: Saves collapse trades in Blotter sheet.
    15. Banxico Risk: Calculates 1L risk for future Banxico meeting dates.
    16. End Session: Ends sessions and stops the running code.
    
"""


#-------------
#  Libraries
#-------------
import sys
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
import xlwings as xw
import QuantLib as ql
main_path = '//TLALOC/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/' +\
    'Main Codes/Pricing/'
sys.path.append(main_path)
import Funtions as fn
import udf_pricing as pr

warnings.filterwarnings("ignore")

#-------------
#  Functions
#-------------

def menu() -> int:
    """Displays user menu
    
    Returns
    -------
    int
        Option for operating

    """
    print('Please choose an option: ')
    print('\n1) Pricing             12) Short End Pricing' +
          '\n2) Update Curves       13) Graph' +
          '\n3) Corros              14) Collapse Blotter' +
          '\n4) Collapse            15) Banxico Risk' +
          '\n5) Blotter             16) Refresh All' +
          '\n6) Upload Blotter      17) Intraday PnL' +
          '\n7) Fwd Starting        18) Fwd Start Simulation' +
          '\n8) Pricing Granular    19) Bono TIIE Close'+
          '\n9) Update Granular     20) Bono TIIE Live'+
          '\n10) Collapse Granular  21) Snapshot'+
          '\n11) Blotter Granular   22) End Session')

    option = int(float(input('\nOption:  ')))
    if option != 17:
        print('\n')
    return option

#---------------------
#  Evaluation Inputs  
#---------------------
def main(pricing_file: str, graph_file: str, corros_file: str):
    start_time = datetime.now()
    str_file = pricing_file
    wb = xw.Book(str_file)
    parameters = wb.sheets('Pricing')
    parameters.range('B1:G1').api.Calculate()
    evaluation_date = pd.to_datetime(parameters.range('B1').value)
    if datetime.now().hour < 8:
        updateAll =  True
    else:
        updateAll = parameters.range('B2').value
    flag = updateAll

    granular_closes, tiie_28_yst = pr.close_price(wb, evaluation_date)
    try:
        pr.closes_vector(wb, corros_file)
    except:
        print('Closes could not be filled in Corros file. Please enter them '\
              'manually.')
    #irs_df = pr.remate_closes(wb, evaluation_date)
    sl = pr.sl_df(evaluation_date, wb)

    # USDMXN Currency Exchange Rate check
    mxn_fx = parameters.range('F1').value
    while type(mxn_fx) is not float:
        try:
            mxn_fx = float(parameters.range('F1').value)
        except:
            print('\nPlease check Cell "F1" of Pricing Sheet')
            c=input('When done press "c": ')
            if c == 'c':
                
                parameters = wb.sheets('Pricing')
                mxn_fx = parameters.range('F1').value
            else:
                continue
            
    # Valuation Date definition   
    print('\nValuation Date: ', evaluation_date)
    ql.Settings.instance().evaluationDate = ql.Date(evaluation_date.day, 
                                                    evaluation_date.month, 
                                                    evaluation_date.year)

    while (ql.UnitedStates(1).isHoliday(ql.Date().from_date(evaluation_date)) 
           and updateAll == True):
        print('Please make sure cell "B2" in Pricing sheet is not True since' \
              ' today is a US holiday.')
        input('When done press "c": ')
        updateAll = parameters.range('B2').value
        flag = updateAll
    
    


    #---------------------
    #  Inputs dictionary
    #---------------------

    dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
    
    
    tenors = dic_data['MXN_TIIE']['Period'][1:].tolist()
    rates = (dic_data['MXN_TIIE']['Quotes'][1:]/100).tolist()

    #-----------------------
    #  Curves Bootstraping
    #-----------------------

    g_crvs = pr.createCurves(dic_data, updateAll, flag)
    flag = g_crvs[-1]
    banxico_TIIE28 = pr.banxicoData(evaluation_date)
    tiie28 = banxico_TIIE28.iloc[-1].dato * 100
    bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, g_crvs[1], 
                                   g_crvs[0])
    g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
    dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, g_crvs[1], 
                                        g_crvs[0], updateAll)
    dic_granular = 0
    gg_crvs = 0
    gg_engines = 0
    
    df_tiieSwps = pd.DataFrame()
    book = 1814
    npv_yst = False
    npv_tdyst = False
    cf_sum = False
    spreads_flag = True
    bonds_data = pd.DataFrame()
    close_curves = None
    cetes_vec = pd.DataFrame()

    #---------------------
    #  Trades Evaluation
    #---------------------

    print('\nCalculating trades...')
    try:
        pr.tiie_pricing(dic_data, wb, g_crvs, banxico_TIIE28, bo_engines, 
                        g_engines, dv01_engines)
    except:
        raise Exception('\n#################################################'
                        '\nPlease make sure you have all inputs for MXN_TIIE'
                        '\n#################################################\n')
       
    
    end_time= datetime.now()

    # Carry Roll calculation
    pr.proc_CarryCalc(g_engines, wb)

    # DV01 reference table calculation
    dv01_tab = pd.DataFrame()
    for k in range(0, len(tenors)):
        dv01_tab = pr.dv01_table(tenors[k], rates[k], evaluation_date, 
                                 g_engines, dv01_engines, mxn_fx, dv01_tab)
    dv01_tab.index = ['DV01']
    wb.sheets['Notional_DV01'].range('J4').value = dv01_tab.T.values

    print('\n',end_time - start_time)
    print('\n----------------------------------------------------\n')

    #-------------
    #  Main Loop  
    #-------------

    # When 16, loop is broken
    option = 0
    while option != 22:   
        
        # Menu display
        while True:
            try:
                option = menu()
                break
            except:
                print('\n###### Please write a number ######\n')
                continue
        
        # When 1, swaps given are evaluated
        if option == 1:
            
            # Defined parameters
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            print('Calculating trades...')
            print('Valuation Date: ', evaluation_date)
            
            # Swaps evaluation
            pr.tiie_pricing(dic_data, wb, g_crvs, banxico_TIIE28, bo_engines, 
                            g_engines, dv01_engines)
        
        # When 2, Curves are updated and swpas given are evaluated
        if option == 2:
            
            # Defined parameters
            start_time = datetime.now()
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            updateAll = parameters.range('B2').value
            
            # Saves every input sheet in the excel as DataFrame
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
            
            # USDMXN Currency Exchange Rate check
            mxn_fx = parameters.range('F1').value
            while type(mxn_fx) is not float:
                try:
                    mxn_fx = float(parameters.range('F1').value)
                except:
                    print('\nPlease check Cell "F1" of Pricing Sheet')
                    c=input('When done press "c": ')
                    if c == 'c':
                        
                        parameters = wb.sheets('Pricing')
                        mxn_fx = parameters.range('F1').value
                    else:
                        continue
                    
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, g_crvs[1], 
                                           g_crvs[0])
            g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
            dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                               g_crvs[1], g_crvs[0], updateAll)
            try:
                pr.proc_CarryCalc(g_engines, wb)
            except:
                print('\n#################################################'
                      '\nPlease make sure you have all inputs for MXN_TIIE'
                      '\n#################################################\n')
                continue
            end_time=datetime.now()
            print(end_time - start_time)
            
            # Swaps given evaluation
            print('\nValuation Date: ', evaluation_date)
            print('Calculating trades...')
            pr.tiie_pricing(dic_data, wb, g_crvs, banxico_TIIE28, bo_engines, 
                            g_engines, dv01_engines)
            
            # DV01 refeence table calculation
            dv01_tab = pd.DataFrame()
            
            for k in range(0, len(tenors)):
                dv01_tab = pr.dv01_table(tenors[k], rates[k], evaluation_date, 
                                         g_engines, dv01_engines, mxn_fx, 
                                         dv01_tab)
            dv01_tab.index = ['DV01']
            wb.sheets['Notional_DV01'].range('J4').value = dv01_tab.T.values

        # When 3, Corros file will be updated with best bids and offers
        if option == 3:
            
            corros_book = xw.Book(corros_file, update_links=False)
            
            sheets = ['ENMX', 'RTMX', 'SIFM', 'TMEX6', 'GFIM', 'MEI', 'VAR', 
                      'SIPO', 'Spreads']
            for s in sheets:
                corros_book.sheets(s).api.Calculate()
                
            best_spreads, paths_data, closes_df = pr.corros_fn(corros_book)
            pr.fill_rates(wb, best_spreads, closes_df)
            
            
            
        
        # When 4, swaps given in the "Collapse" sheet calculation
        if option == 4:
            
            # Defined Parameters 
            start_time = datetime.now()
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            
            updateAll = parameters.range('B2').value
            mxn_fx = parameters.range('F1').value
            while type(mxn_fx) is not float:
                try:
                    mxn_fx = float(parameters.range('F1').value)
                except:
                    print('\nPlease check Cell "F1" of Pricing Sheet')
                    c=input('When done press "c": ')
                    if c == 'c':
                        
                        parameters = wb.sheets('Pricing')
                        mxn_fx = parameters.range('F1').value
                    else:
                        continue
                    
            # Saves every input sheet in the excel as DataFrame
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
                
            
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, g_crvs[1], 
                                           g_crvs[0])
            g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
            dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                               g_crvs[1], g_crvs[0], updateAll)
            end_time = datetime.now()
            print('\n', end_time - start_time)
            
            # Swaps given evaluation
            try:
                parameters_trades, collapse_type, folder_name, lch_flag, raro_df = \
                    pr.collapse(wb)
                print('\nCalculating trades...')
                print(f'Collapse: {collapse_type}')
                print(f'Folder: {folder_name}')
                pr.tiie_pricing(dic_data, wb, g_crvs, banxico_TIIE28, 
                                bo_engines, g_engines, dv01_engines, 
                                parameters_trades)
                if lch_flag:
                    print('\n########## Clearing Broker: LCH ##########')
                
                if not raro_df.empty:
                    print('\nPlease Check the Following Trades:')
                    print(raro_df)
                    
            except:
                print('Could not perform collapse. Please check files'+
                      ' or check MXN_TIIE inputs.')
        
            
        # When 5, "Blotter" sheet trades are calculated
        if option==5:
            
            # Defined Parameters
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            
            # Swaps given evaluation
            print('Valuation Date: ', evaluation_date)
            krrs, npv_group, dv01s = pr.tiie_blotter(dic_data, wb, g_crvs, 
                                                     banxico_TIIE28, 
                                                     bo_engines, g_engines, 
                                                     dv01_engines)
            print('Blotter done!')
            
        # When 6, Blotter will be uploaded to a different format
        if option == 6:
            
            # Defined Parameters
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            
            # Blotter Upload
            print('Valuation Date: ', evaluation_date)
            print('\nUploading Blotter...')
            pr.upload_blotter(wb, evaluation_date)
            print('Upload complete!')
        
        # When 7, Fwd Starting swaps will be calculated
        if option == 7:
            # Defined Parameters
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            updateAll = parameters.range('B2').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
            
            
            try:                                                
                close_rates = pr.load_obj(r'Closes/close_fwds_' 
                                          + evaluation_date.strftime('%Y%m%d'))
                
            except:
                close = True
                
                # Saves every input sheet in the excel as DataFrame
                risk_sheet = wb.sheets('Risk')
                close_quotes = risk_sheet.range('L6:L20').value
                        
                modic = {k: v.copy() for k, v in dic_data.items()}
                modic['MXN_TIIE']['Quotes'] = close_quotes
                        
                # Updating Curves
                g_crvs = pr.createCurves(modic, updateAll, flag)
                flag = g_crvs[-1]
                banxico_TIIE28 = pr.banxicoData(evaluation_date)
                g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
                
                # Calculating close fwds
                pr.fwd_starting(wb, g_engines, close)
            
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
            
            # Calculating live fwds
            pr.fwd_starting(wb, g_engines)
        
        if option == 8:
            
            # Defined parameters
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            print('Calculating trades...')
            print('Valuation Date: ', evaluation_date)
            
            # Swaps evaluation
            try:
                pr.tiie_pricing(dic_granular, wb, gg_crvs, banxico_TIIE28, 
                                bo_engines, gg_engines, dv01_engines)
            except:
                if gg_crvs == 0:
                    print('\n########## Please Update Granular Curves'\
                          ' (Option 9) ##########')
                else:
                    print('\n########## Please check you do not have bid '\
                          'offer options in Pricing sheet. ##########')
        
        # Update Granular
        if option == 9:
            
            #Fill close prices for granular risk
            if not granular_closes:
                print('Calculating granular closes...')
                a1 = datetime.now()
                granular_closes, tiie_28_yst = pr.close_price(wb, 
                                                              evaluation_date, 
                                                              gran = True)
                b1 = datetime.now()
                print(b1-a1)
            # Defined parameters
            start_time = datetime.now()
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            updateAll = parameters.range('B2').value
            
            # Saves every input sheet in the excel as DataFrame
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
            
            # USDMXN Currency Exchange Rate check
            mxn_fx = parameters.range('F1').value
            while type(mxn_fx) is not float:
                try:
                    mxn_fx = float(parameters.range('F1').value)
                except:
                    print('\nPlease check Cell "F1" of Pricing Sheet')
                    c=input('When done press "c": ')
                    if c == 'c':
                        
                        parameters = wb.sheets('Pricing')
                        mxn_fx = parameters.range('F1').value
                    else:
                        continue        
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            try:
                dic_granular = pr.granular(evaluation_date, g_crvs[2], 
                                                    g_engines, dic_data)
            except:
                print('\n#################################################'
                      '\nPlease make sure you have all inputs for MXN_TIIE'
                      '\n#################################################\n')
                continue
            
            gg_crvs = pr.createCurves(dic_granular, updateAll, flag)
            
            
            
            
            
            wb.sheets('Granular_Risk').range('M6').value = \
                dic_granular['MXN_TIIE']['Quotes'].values.reshape(-1,1) 
            
            
            
            #gg_crvs = g_crvs.copy()
            #gg_crvs[4] = gbrCrvs
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, 
                                           gg_crvs[1], gg_crvs[0])
            gg_engines = pr.engines(gg_crvs[2], gg_crvs[3], banxico_TIIE28)
            dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                               gg_crvs[1], gg_crvs[0], 
                                               updateAll)
            #pr.proc_CarryCalc(gg_engines, wb)
            end_time=datetime.now()
            print(end_time - start_time)
            
            # Swaps given evaluation
            print('\nValuation Date: ', evaluation_date)
            print('Calculating trades...')
            try:
                pr.tiie_pricing(dic_granular, wb, gg_crvs, banxico_TIIE28, 
                                bo_engines, gg_engines, dv01_engines)
            except:
                print('\n########## Please check you do not have bid offer '\
                      'options in Pricing sheet. ##########')
            
            # DV01 refeence table calculation
            dv01_tab = pd.DataFrame()
            
            for k in range(0, len(tenors)):
                dv01_tab = pr.dv01_table(tenors[k], rates[k], evaluation_date, 
                                          g_engines, dv01_engines, mxn_fx, 
                                          dv01_tab)
            dv01_tab.index = ['DV01']
            wb.sheets['Notional_DV01'].range('J4').value = dv01_tab.T.values
            
        if option == 10:
            
            start_time = datetime.now()
            
            #Fill close prices for granular risk
            if not granular_closes:
                print('Calculating granular closes...')
                a1 = datetime.now()
                granular_closes, tiie_28_yst = pr.close_price(wb, 
                                                              evaluation_date, 
                                                              gran = True)
                b1 = datetime.now()
                print(b1-a1)
                
            # Defined parameters
            start_time = datetime.now()
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            updateAll = parameters.range('B2').value
            
            # Saves every input sheet in the excel as DataFrame
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
            
            # USDMXN Currency Exchange Rate check
            mxn_fx = parameters.range('F1').value
            while type(mxn_fx) is not float:
                try:
                    mxn_fx = float(parameters.range('F1').value)
                except:
                    print('\nPlease check Cell "F1" of Pricing Sheet')
                    c=input('When done press "c": ')
                    if c == 'c':
                        
                        parameters = wb.sheets('Pricing')
                        mxn_fx = parameters.range('F1').value
                    else:
                        continue        
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            try:
                dic_granular = pr.granular(evaluation_date, g_crvs[2], 
                                                    g_engines, dic_data)
            except:
                print('\n#################################################'
                      '\nPlease make sure you have all inputs for MXN_TIIE'
                      '\n#################################################\n')
                continue
            
            gg_crvs = pr.createCurves(dic_granular, updateAll, flag)
            wb.sheets('Granular_Risk').range('M6').value = \
                dic_granular['MXN_TIIE']['Quotes'].values.reshape(-1,1) 
            
            
            
            #gg_crvs = g_crvs.copy()
            #gg_crvs[4] = gbrCrvs
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, 
                                           gg_crvs[1], gg_crvs[0])
            gg_engines = pr.engines(gg_crvs[2], gg_crvs[3], banxico_TIIE28)
            dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                               gg_crvs[1], gg_crvs[0], 
                                               updateAll)
            #pr.proc_CarryCalc(gg_engines, wb)
            end_time=datetime.now()
            print(end_time - start_time)
            
            # Swaps given evaluation
            try:
                parameters_trades, collapse_type, folder_name = pr.collapse(wb)
                print('\nCalculating trades...')
                print(f'Collapse: {collapse_type}')
                print(f'Folder: {folder_name}')
                pr.tiie_pricing(dic_granular, wb, gg_crvs, banxico_TIIE28, 
                                bo_engines, gg_engines, dv01_engines, 
                                parameters_trades)
            except:
                print('Could not perform collapse. Please check files.')
        
        if option == 11:
            
            # Defined Parameters
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = ql.Date(
                evaluation_date.day, evaluation_date.month, 
                evaluation_date.year)
            
            # Swaps given evaluation
            print('Valuation Date: ', evaluation_date)
            try:
                krrs, npv_group, dv01s = pr.tiie_blotter(dic_granular, wb, 
                                                         gg_crvs, 
                                                         banxico_TIIE28, 
                                                         bo_engines,
                                                         gg_engines, 
                                                         dv01_engines, 
                                                         gran = True)
                print('Blotter done!')
            except:
                print('Please Update Granular Curves (Option 9)')
        
        if option == 12:
            
            # Defined Paramters
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = ql.Date(
                evaluation_date.day, evaluation_date.month, 
                evaluation_date.year)
            updateAll = parameters.range('B2').value
            
            # Saves every input sheet in the excel as DataFrame
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
            
            # USDMXN Currency Exchange Rate check
            mxn_fx = parameters.range('F1').value
            while type(mxn_fx) is not float:
                try:
                    mxn_fx = float(parameters.range('F1').value)
                except:
                    print('\nPlease check Cell "F1" of Pricing Sheet')
                    c=input('When done press "c": ')
                    if c == 'c':
                        
                        parameters = wb.sheets('Pricing')
                        mxn_fx = parameters.range('F1').value
                    else:
                        continue
                    
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, g_crvs[1], 
                                           g_crvs[0])
            g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
            dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                               g_crvs[1], g_crvs[0], updateAll)
            
            # Holiday List 
            sep = wb.sheets('Short_End_Pricing')
            holist = ql.Mexico().holidayList(
                ql.Date().from_date(evaluation_date),
                ql.Date().from_date(evaluation_date) + ql.Period(4,3))
            sep.range('J114').value =\
                np.array([d.to_date() for d in holist]).reshape(-1,1)
                
            # Short End Pricing Calculation   
            print('\nShort End Pricing...')
            # try:
            pr.proc_ShortEndPricing(g_crvs[2], g_crvs[3], wb, banxico_TIIE28)
            # except:
            #     print('\n#################################################'
            #           '\nPlease make sure you have all inputs for MXN_TIIE'
            #           '\n#################################################\n')
            #     continue
            
            
            print('\tTenor Fwd TIIE28 Done!')
            pr.proc_ShortEndPricing_byMPC(g_crvs[3], wb)
            print('\tMPC Date Fwd TIIE28 Done!')    
        
        if option == 13:
            pr.graph_option(graph_file)
        

        if option == 14:
            try:
                pr.collapse_blotter(wb)
            except:
                print('No values found for collapse blotter.')
        
        if option == 15:
            
            sheet_names = [s.name for s in wb.sheets]
            if 'Fix_Dates_Analysis' in sheet_names:
                pr.risk_byMeet(wb)
                pr.banxico_risk_option(dic_data, wb, g_engines, dv01_engines)
                
                parameters = wb.sheets('Pricing')
                parameters.range('B1:G1').api.Calculate()
                evaluation_date = parameters.range('B1:B1').value
                ql.Settings.instance().evaluationDate = ql.Date(
                    evaluation_date.day, evaluation_date.month, 
                    evaluation_date.year)
                
                dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
                
                tenors = dic_data['MXN_TIIE']['Period'][1:].tolist()
                rates = (dic_data['MXN_TIIE']['Quotes'][1:]/100).tolist()
    
                #-----------------------
                #  Curves Bootstraping
                #-----------------------
    
                g_crvs = pr.createCurves(dic_data, updateAll, flag)
                flag = g_crvs[-1]
                banxico_TIIE28 = pr.banxicoData(evaluation_date)
                bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, 
                                               g_crvs[1], g_crvs[0])
                g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
                dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                                   g_crvs[1], g_crvs[0], 
                                                   updateAll)
            else:
                continue
        
        if option == 16:
            # Refresh All
            corros_book = xw.Book(corros_file, update_links=False)
            
            sheets = ['ENMX', 'RTMX', 'SIFM', 'TMEX6', 'GFIM', 'MEI', 'VAR', 
                      'SIPO', 'Spreads']
            for s in sheets:
                corros_book.sheets(s).api.Calculate()
                
            best_spreads, paths_data, closes_df = pr.corros_fn(corros_book)
            pr.fill_rates(wb, best_spreads, closes_df)
            
            # Update curves
            
            # Defined parameters
            start_time = datetime.now()
            wb = xw.Book(str_file)
            parameters = wb.sheets('Pricing')
            parameters.range('B1:G1').api.Calculate()
            evaluation_date = parameters.range('B1:B1').value
            ql.Settings.instance().evaluationDate = \
                ql.Date(evaluation_date.day, evaluation_date.month, 
                        evaluation_date.year)
            updateAll = parameters.range('B2').value
            
            # Saves every input sheet in the excel as DataFrame
            dic_data = pr.read_dic_data(str_file, evaluation_date, flag)
            
            # USDMXN Currency Exchange Rate check
            mxn_fx = parameters.range('F1').value
            while type(mxn_fx) is not float:
                try:
                    mxn_fx = float(parameters.range('F1').value)
                except:
                    print('\nPlease check Cell "F1" of Pricing Sheet')
                    c=input('When done press "c": ')
                    if c == 'c':
                        
                        parameters = wb.sheets('Pricing')
                        mxn_fx = parameters.range('F1').value
                    else:
                        continue
                    
            # Updating Curves
            g_crvs = pr.createCurves(dic_data, updateAll, flag)
            flag = g_crvs[-1]
            banxico_TIIE28 = pr.banxicoData(evaluation_date)
            bo_engines = fn.bid_offer_crvs(dic_data, banxico_TIIE28, g_crvs[1], 
                                           g_crvs[0])
            g_engines = pr.engines(g_crvs[2], g_crvs[3], banxico_TIIE28)
            dv01_engines = fn.flat_dv01_curves(dic_data, banxico_TIIE28, 
                                               g_crvs[1], g_crvs[0], updateAll)
            try:
                pr.proc_CarryCalc(g_engines, wb)
            except:
                print('\n#################################################'
                      '\nPlease make sure you have all inputs for MXN_TIIE'
                      '\n#################################################\n')
                continue
                
            
            end_time=datetime.now()
            print(end_time - start_time)
            
            # Defined Parameters
            # wb = xw.Book(str_file)
            # parameters = wb.sheets('Pricing')
            # parameters.range('B1:G1').api.Calculate()
            # evaluation_date = parameters.range('B1:B1').value
            # ql.Settings.instance().evaluationDate = \
            #     ql.Date(evaluation_date.day, evaluation_date.month, 
            #             evaluation_date.year)
            
            # Swaps given evaluation
            print('Valuation Date: ', evaluation_date)
            krrs, npv_group, dv01s = pr.tiie_blotter(dic_data, wb, g_crvs, 
                                                     banxico_TIIE28, 
                                                     bo_engines, g_engines, 
                                                     dv01_engines)
            print('Blotter done!')
            
        if option == 17:
            ready = input('Have you run Corros, Update Curves and Blotter ' + 
                          'functions? (Y/N): ').lower()
            
            if ready == 'y':
                print('\n')
                if book != wb.sheets('Risk').range('B2').value:
                    book_flag = True
                else:
                    book_flag = False
                print('Calculating intraday PnL...')
                df_tiieSwps, npv_yst, npv_tdyst, cf_sum, book = \
                    pr.intraday_pnl(wb, dic_data, df_tiieSwps, book_flag, 
                                    npv_yst, npv_tdyst, cf_sum)
            else:
                print('\n')
                continue
        
        if option == 18:
            # try:
            pr.simulation(wb, dic_data)
            # except:
            #     print('\nSomething went wrong. '+
            #           'Please make sure you have valuation checks in '+
            #           'Fwd_Start_Sim sheet and your are not editing the '+
            #           'Excel file while the process is running.\n')
            #     continue
            #pr.simulation(wb, dic_data)
            
        if option == 19:
            try:
                close_curves = pr.tiie_spreads(pricing_file, graph_file, 
                                               tiie28)
                cetes_vec = pr.close_pnl(graph_file, close_curves, tiie28)
                print('Calculating Bonds Short End Pricing...')
                pr.pricing_bonds(graph_file, pricing_file, g_crvs[3])
                print('\nBono TIIE Close complete.')
            except:
                print('\nASW function could not be performed.')

        if option == 20:
            #try:
                # updated_curves = pr.live_asw(pricing_file, graph_file, tiie28)
                # g_crvs, g_engines, dv01_engines, bo_engines = \
                #     updated_curves.to_gcrvs()
            pr.change_scenario(graph_file, tiie28)
            updated_curves = pr.live_pnl(pricing_file, graph_file, tiie28)
            try:
                g_crvs, g_engines, dv01_engines, bo_engines = \
                    updated_curves.to_gcrvs()
            except:
                print('Updated curves could not be saved.')
            print('Calculating Bonds Short End Pricing...')
            try:
                pr.pricing_bonds(graph_file, pricing_file, g_crvs[3], True)
            except:
                print('Short End Pricing could not be completed.')
            print('\nBono TIIE Live and update curves complete!')
            # except:
            #     print('ASW could not be completed.')
           
        # Snapshot    
        if option == 21:
            pr.snapshot(graph_file)
            print('\nComplete!')
                
            
            
        print('\n----------------------------------------------------\n')