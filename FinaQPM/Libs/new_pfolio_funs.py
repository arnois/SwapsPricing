
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 09:56:23 2023

@author: EstebanLopezAraiza
"""
import pandas as pd
import numpy as np
import QuantLib as ql
import pickle
from datetime import datetime , timedelta
import requests
import os
import sys
import time
import warnings
warnings.filterwarnings("ignore")
import curve_funs as cf


class pfolio():
    """ pfolio class
    
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
    
    def __init__(self, dfbook: pd.DataFrame):
        self.dfbook = dfbook
        
        
    @classmethod 
    def from_posSwaps(cls, posswps_file: str,
                      bookID: int = None) -> pd.DataFrame:
        """ Make portfolio object from PosSwaps file
        

        Parameters
        ----------
        str_posswps_file : str | pd.DataFrame
            file directory with name, or full dataframe
        bookID: int, default = None
            bookID to filter, when None, nothing will be filtered

        Returns
        -------
        dfbook: pd.DataFrame with the portfolio info
        

        """
        lst_selcols = ['swp_usuario', 'swp_ctrol', 'swp_fecop', 'swp_monto', 
                       'swp_fec_ini', 'swp_fec_vto', 'swp_fec_cor', 'swp_val_i_pa',
                       'swp_val_i', 'swp_serie', 'swp_emisora', 'swp_pzoref', 
                       'swp_nombre']
        
        if type(posswps_file) == str:
            try:
                df_posSwps = pd.read_excel(posswps_file, 'Hoja1')
                print(f'PosSwaps file {posswps_file} found!')
            except:
                df_posSwps = pd.DataFrame([], columns = lst_selcols)
                print(f'PosSwaps file {posswps_file} NOT found, please check')
                sys.exit('Please make sure'+posswps_file+' file exists')
            
        
        else:
            df_posSwps = posswps_file.copy()
        
        # posswaps filter columns
        lst_selcols = ['swp_usuario', 'swp_ctrol', 'swp_fecop', 'swp_monto', 
                       'swp_fec_ini', 'swp_fec_vto', 'swp_fec_cor', 'swp_val_i_pa',
                       'swp_val_i', 'swp_serie', 'swp_emisora', 'swp_pzoref', 
                       'swp_nombre']
        lst_selcols_new = ['BookID','TradeID','TradeDate','Notional',
                           'StartDate','Maturity','CpDate','RateRec',
                           'RatePay','PayIndex','RecIndex','CouponReset', 
                           'Counterparty']
        df_posSwps = df_posSwps[lst_selcols]
        df_posSwps.columns = lst_selcols_new
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
            bookid, tradeid, tradedate, notnl, stdt, mty, cpdt, r, swptyp, \
                ctpty = row
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
        df_tiieSwps['Fees'] = 0
        df_tiieSwps['Counterparty'] = df_tiieSwps['Counterparty'].str.strip()
        
        lst_cols= ['BookID','TradeID','TradeDate','Notional', 'StartDate', 
                   'Maturity', 'CpDate', 'FxdRate', 'SwpType', 'mtyOnHoliday',
                   'Fees', 'Counterparty']
        if bookID:
            dfbook = df_tiieSwps[df_tiieSwps['BookID'] == bookID][lst_cols].\
                reset_index(drop=True)
            
        else:
            dfbook = df_tiieSwps[lst_cols].reset_index(drop=True)
        return cls(dfbook)
    
    @classmethod 
    def from_blotter(cls, str_blotter_file: str, val_date: datetime,
                     bookID: int) -> pd.DataFrame:
                     
        """ Make portfolio object from blotter file
        

        Parameters
        ----------
        str_blotter_file : str
            file directory with name
        curves : cf.mxn_curves
            curves used for evaluating the portfolio
        bookID: int
            bookID to filter
        Returns
        -------
        dfbook: pd.DataFrame with the portfolio info
        

        """
        lst_blttrcols = ['Book','Tenor','Yield(Spot)','Size', 'Fecha Inicio', 
                         'Fecha vencimiento','Cuota compensatoria / unwind', 
                         'Folio JAM', 'Folio Original', 'Ctpty']
        if type(str_blotter_file) == str:
            try:
                df = pd.read_excel(str_blotter_file, sheet_name = 'BlotterTIIE_auto', 
                                   skiprows = 2)
                print(f'Blotter file {str_blotter_file} found!')
                
            except: 
                df = pd.DataFrame({}, columns= lst_blttrcols)
                print(f'Blotter file {str_blotter_file} NOT found, please check')
                sys.exit('Please make sure'+str_blotter_file+' file exists')
                
        else:
            df = str_blotter_file.copy()
            
            
        dayblotter = df.loc[~df.Book.isna(), lst_blttrcols]
        dayblotter[['Fecha Inicio', 'Fecha vencimiento']] = \
            dayblotter[['Fecha Inicio', 'Fecha vencimiento']].\
            astype('datetime64[ns]')
        
        tenor2ql = {'m': 4, 'y': 52}
        period = dayblotter['Tenor'].str[-1].map(tenor2ql).to_numpy()
        tenor = dayblotter['Tenor'].str[:-1].astype(int).to_numpy()
        weeks = tenor*period
        dayblotter['L'] = weeks
        lst_NAtypes = [pd._libs.tslibs.nattype.NaTType,
                       np.nan]
        for i,r in dayblotter.iterrows():
            if type(r['Fecha Inicio']) in lst_NAtypes:
                idt = ql.Mexico().advance(ql.Date(val_date.day,
                                                  val_date.month,
                                                  val_date.year),
                                          ql.Period(1,ql.Days))
                fdt = idt + ql.Period(r['L'],ql.Weeks)
                dayblotter.loc[i,'Fecha Inicio'] = idt.to_date()
                dayblotter.loc[i,'Fecha vencimiento']  = fdt.to_date()
                
        dayblotter['Fees'] = \
            dayblotter['Cuota compensatoria / unwind'].fillna(0)
        # DayBlotter Swaps Book
        dayblotter['BookID'] = dayblotter['Book'].astype(int)
        dayblotter['TradeID'] = dayblotter['Folio JAM']
        dayblotter['TradeDate'] = val_date
        dayblotter['Notional'] = abs(dayblotter['Size'])*1e6
        dayblotter['StartDate'] = dayblotter['Fecha Inicio']
        dayblotter['Maturity'] = dayblotter['Fecha vencimiento']
        dayblotter['CpDate'] = dayblotter['Fecha Inicio'] + timedelta(days=28)
        dayblotter['FxdRate'] = dayblotter['Yield(Spot)']
        dayblotter['SwpType'] = np.sign(dayblotter['Size']).astype(int)*-1
        dayblotter['mtyOnHoliday'] = 0
        dayblotter['Counterparty'] = dayblotter['Ctpty']
        
        lst_cols= ['BookID','TradeID','TradeDate','Notional', 'StartDate', 
                   'Maturity', 'CpDate', 'FxdRate', 'SwpType', 'mtyOnHoliday',
                   'Fees', 'Counterparty', 'Folio Original']
        
        dfbook = dayblotter[dayblotter['BookID'] == bookID].\
            reset_index(drop=True)[lst_cols]
        
        return cls(dfbook)
    
    @classmethod
    def from_pricing(cls, str_pricing_file: str, val_date: datetime, 
                     sheet : str, bookID: int) -> pd.DataFrame:
        """ Creates portfolio class fom pricing file
        

        Parameters
        ----------
        str_pricing_file : str | DataFrame
            DataFrame or string from where the portfolo will be created 
        val_date : datetime
            date of trades
        bookID : int
            bookID to evaluate

        Returns
        -------
        dfbook: DataFrame with the info necessary
        
        """
        cols = ['Start_Tenor', 'Fwd_Tenor', 'Start_Date', 'End_Date',
                'Notional_MXN', 'Rate']
        
        if type(str_pricing_file) == str:
            
            try: 
                pric = pd.read_excel(str_pricing_file, sheet_name = sheet).fillna(0)
                print('Pricing file found!')
            
            except:
                pric = pd.DataFrame([], columns = cols)
                print('Pricing file not found')
        
        else:
            pric = str_pricing_file.copy().fillna(0)
            
        
        # Dates: 
        for i, v in pric.iterrows():
            # Case only Fwd
            if ((v.Start_Tenor == 0) and (v.Start_Date == 0) 
                and (v.End_Date == 0)) and v.Fwd_Tenor != 0:
                
                start = ql.Mexico().advance(ql.Date().from_date(val_date),
                                            ql.Period(1, ql.Days))
                maturity = (start + 28*int(v.Fwd_Tenor)).to_date()
                start = start.to_date()
            
            #Case Only End Date
            elif ((v.Start_Date == 0) and (v.Start_Tenor == 0) and
                  (v.End_Date != 0)) and (v.Fwd_Tenor == 0):
                
                start = ql.Mexico().advance(ql.Date().from_date(val_date),
                                            ql.Period(1, ql.Days)).to_date()
                maturity = v.End_Date
            # Case Start and End Dates
            elif ((v.Start_Date != 0) and (v.Start_Tenor == 0) and
                  (v.End_Date != 0)) and (v.Fwd_Tenor == 0):
                
                continue
            # Case Start Date and Fwd tenor
            elif ((v.Start_Date != 0) and (v.Start_Tenor == 0) and
                  (v.End_Date == 0)) and (v.Fwd_Tenor != 0):
                
                start = ql.Date().from_date(v.Start_Date)
                
                maturity = (start + 28*int(v.Fwd_Tenor)).to_date()
                start = start.to_date()
                
            # Case Start and Fwd Tenor
            elif ((v.Start_Date == 0) and (v.Start_Tenor != 0) and
                  (v.End_Date == 0)) and (v.Fwd_Tenor != 0):
                
                valql = ql.Date().from_date(val_date)
                start = ql.Mexico().advance(valql + 28*int(v.Start_Tenor) + 1,
                                            ql.Period(0, ql.Days))
                maturity = (start + int(28*v.Fwd_Tenor)).to_date()
                start = start.to_date()
            
            pric.at[i, 'Start_Date'] = start
            pric.at[i, 'End_Date'] = maturity
            
        if 'Book' in pric.columns:
            diccols = {'Start_Date': 'StartDate', 'End_Date': 'Maturity',
                       'Notional_MXN': 'Notional', 'Rate': 'FxdRate',
                       'Book': 'BookID'}
            pric.rename(diccols, axis = 1, inplace = True)
        
        else:
            diccols = {'Start_Date': 'StartDate', 'End_Date': 'Maturity',
                       'Notional_MXN': 'Notional', 'Rate': 'FxdRate'}
            pric.rename(diccols, axis = 1, inplace = True)
            
            pric['BookID'] = 1814
            
        
        pric['TradeDate'] = val_date
        pric['CpDate'] = pric['StartDate'] + timedelta(28) 
        pric['SwpType'] = np.sign(pric['Notional'])*-1
        pric['Notional'] = abs(pric['Notional'])
        pric['mtyOnHoliday'] = 0
        pric['Fees'] = 0
        pric['Counterparty'] = np.nan
        pric['Folio Original'] = np.nan
        pric['FxdRate'] = pric['FxdRate']*100
        pric['TradeID'] = pric.index + 6000
        
        lst_cols= ['BookID','TradeID','TradeDate','Notional', 'StartDate', 
                   'Maturity', 'CpDate', 'FxdRate', 'SwpType', 'mtyOnHoliday',
                   'Fees', 'Counterparty', 'Folio Original']
        
        dfbook = pric[lst_cols].copy()
        
        return cls(dfbook)
            
        
    
    def get_book_npv(self, dt_today: datetime, curves: cf.mxn_curves, 
                     inplace: bool = False):
        """Calculates book npv
        
    
        Parameters
        ----------
        inplace : bool, optional
            If true dfbokval attribute is added. The default is False.
    
        Returns
        -------
        dfbookval: DataFrame 
            if inplace = False
        None 
            if inplace = True
    
        """
        try: 
            ql.Settings.instance().evaluationDate =\
                ql.Date().from_date(dt_today)
        
        except:
            ql.Settings.instance().evaluationDate =\
                ql.Date().from_date(dt_today)
        
        curves.crvMXNTIIE.complete_ibor_tiie =\
            curves.crvMXNTIIE.complete_ibor_index()
            
        if 'Folio Original' in self.dfbook.columns:
            self.dfbook.drop(columns = ['Folio Original'], inplace = True) 
        # Swap's book dataframe
        dfbookval = pd.DataFrame(None, 
                                 columns = self.dfbook.columns.tolist() +\
                                     ['SwpObj', 'NPV', 'evalDate'])
        # CONTROL VAR
        #print(ql.Settings.instance().evaluationDate)
        # Book's Base NPV
        for i,row in self.dfbook.iterrows():
            bookid, tradeid, tradedate, notnl,\
                stdt, mty, cpdt, r, swptyp, schdlrule, fees, ctpty = row
            
            if swptyp < 0:
                pass
            else:
                notnl = -notnl
            swp = cf.tiieSwap(stdt, mty, notnl, r/100, curves, schdlrule)
           
            npv = swp.NPV()
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
                       'SwpObj': swp,
                       'NPV': npv,
                       'Fees': fees,
                       'evalDate': ql.Settings.instance().evaluationDate.to_date()}
            new_row = pd.DataFrame(tmpdict, index=[i])
            dfbookval = pd.concat([dfbookval.loc[:], new_row])
        
        if inplace:
            self.dfbookval = dfbookval.reset_index()
            return None
        else:
            return dfbookval.reset_index()    
        
        
    def get_book_step_npv(self, dt_today: datetime, curves: cf.mxn_curves, 
                          flt_scenario: pd.DataFrame) -> pd.DataFrame:
        """ Gets book Step NPV
        

        Parameters
        ----------
        dt_today : datetime
        curves : cf.mxn_curves
        flt_scenario : pd.DataFrame
            Banxico Scenario

        Returns
        -------
        df_npv : pd.DataFrame
            DataFrame with every swap

        """
        
        df_npv = self.get_book_npv(dt_today, curves)
        
        df_npv['Step_NPV'] = df_npv.SwpObj.apply(lambda x: x.step_NPV(flt_scenario))
        
        return df_npv
        
        
        
        
    def get_risk_byBucket(self, dt_today: datetime = None, 
                          curves: cf.mxn_curves = None, fx_rate = None) -> dict:
        """ Calcualtes the KRR for every trade in the portfolio
        

        Returns
        -------
        dict
            Dictionary with the KRR and the NPV

        """
       
        if curves:
            self.get_book_npv(dt_today, curves, True)
            
  
        
        df_tenorDV01 = pd.DataFrame(columns =\
                                    self.dfbookval.SwpObj.iloc[0].KRR().columns)
        for swp in self.dfbookval.SwpObj:
            df_tenorDV01 = pd.concat([df_tenorDV01, swp.KRR()])
        
        if not fx_rate:
            fx_rate = swp.curves.dic_data['USDMXN_XCCY_Basis'].iloc[0]['Quotes']
        
     
        dfbr = pd.Series((df_tenorDV01.sum()/fx_rate).sum(), 
                                   index = ['OutrightRisk'])
        dfbr = dfbr.append((df_tenorDV01.sum()/fx_rate))
        dfbr = dfbr.map('{:,.0f}'.format)
        dic_bookRisks = {
            'NPV_Book': self.dfbookval.NPV.sum(),
            'NPV_Swaps': self.dfbookval.NPV,
            'DV01_Book': dfbr,
            'DV01_Swaps': df_tenorDV01/fx_rate,
            'Book':self.dfbook
            }
        return(dic_bookRisks)
    
    def get_pfolio_CF_atDate(self, val_date: datetime, 
                             dfbookval: pd.DataFrame = pd.DataFrame([]), 
                             inplace: bool = False) -> pd.DataFrame:
        # Payment Date
        
        ql_dt_pmt = ql.Date().from_date(val_date)
        
        if dfbookval.empty:
            dfbookval = self.dfbookval
            
  
            
        # Coupon Payments by Swap
        dic_cf = {}
        for i,r in dfbookval.iterrows():
            # Swap
            tmpswp = r['SwpObj']
        
            # Swap payment at date
            tmpcf = self.get_CF_tiieSwap_atDate(tmpswp, ql_dt_pmt)
            
            # Payment at date by swap ID
            dic_cf[r['TradeID']] =\
                tmpcf[tmpcf['date'] == str(ql_dt_pmt.to_date())]['netAmt']
           
        df_cf = pd.DataFrame.from_dict(dic_cf, orient='index')
        
        if df_cf.empty:
            df_cf = pd.DataFrame([0], columns = ['CF_'+str(ql_dt_pmt.to_date())])
        
        else:
            df_cf.columns = ['CF_'+str(ql_dt_pmt.to_date())]
            
        if inplace:
            self.CF = df_cf
            return None
        else:   
            return df_cf
        
    @staticmethod
    def get_CF_tiieSwap_atDate(swp, ql_tdy):
        #ql_tdy = ql.Settings.instance().evaluationDate
        lst_fxngs = swp.cpn_dates
        swp_type = swp.typ
        swp_leg0 = tuple([swpleg0 for swpleg0 in swp.swap.leg(0) 
                          if swpleg0.date() == ql_tdy])
        swp_leg1 = tuple([swpleg1 for swpleg1 in swp.swap.leg(1) 
                          if swpleg1.date() == ql_tdy])
        n_fxngs = len(swp_leg1)
        fxngs_alive = lst_fxngs[-n_fxngs:]
        cf1_l1 = pd.DataFrame({
            'date': pd.to_datetime(str(cf.date())),
            'accStartDate': pd.to_datetime(str(cf.accrualStartDate().ISO())),
            'accEndDate': pd.to_datetime(str(cf.accrualEndDate().ISO())),
            'fixAmt': cf.amount()*-1*swp_type
            } for cf in map(ql.as_coupon, swp_leg0)
            )
        cf1_l2 = pd.DataFrame({
            'date': pd.to_datetime(str(cf.date())),
            'fixingDate': pd.to_datetime(str(fd)),
            'accStartDate': pd.to_datetime(str(cf.accrualStartDate().ISO())),
            'accEndDate': pd.to_datetime(str(cf.accrualEndDate().ISO())),
            'fltAmt': cf.amount()*swp_type
            } for (cf, fd) in zip(map(ql.as_coupon, swp_leg1), fxngs_alive)
            )

        cf1 = cf1_l1.copy()
        if cf1.empty:
            return cf1.append({'date': None, 'netAmt': None}, ignore_index=True)
        else:
            cf1.insert(1, 'fixingDate',cf1_l2['fixingDate'])
            cf1['fltAmt'] = cf1_l2['fltAmt']
            cf1['netAmt'] = cf1['fixAmt'] + cf1['fltAmt']
            cf1['fixAmt'] = cf1['fixAmt']
            cf1['fltAmt'] = cf1['fltAmt']
            cf1['netAmt'] = cf1['netAmt']
            cf1['accDays'] = 1*(cf1['accEndDate'] - \
                               cf1['accStartDate'])/np.timedelta64(1, 'D')
            return cf1
        
    def get_carry_roll(self, start_date: datetime, end_date: datetime,
                  curves: cf.mxn_curves = None,
                  inplace: bool = False) -> pd.DataFrame:
        """
        

        Parameters
        ----------
        start_date : datetime
            start date of carry
        end_date : datetime
            end_date of carry
        curves : cf.mxn_curves, optional
            curves used for carry, default, 'same' as in same curves
            of the portfolio, if start, new curves wiil be made for the start
            date
        inplace : bool, optional
            When True, the DataFrae is stored as an attribute. 
            The default is False.

        Returns
        -------
        carry : pd.DataFrame
            DataFrame with the portfolio details and Carry
            

        """
        
        # When None, new curves are created
        if not curves:
           
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_file = str_inputsFileName + str_inputsFileExt
            try: 
                dt_start_date = start_date.date()
            except:
                dt_start_date = start_date
            dic_data = cf.pull_data(str_file, dt_start_date)
            
            if ql.UnitedStates(1).isHoliday(ql.Date().from_date(start_date)):
                self.curves.crvUSDOIS = None
                self.curves.crvUSDSOFR = None
                self.curves.crvMXNOIS.discount_curve = None
                self.curves.crvMXNOIS.swap_curve = None
                try:
                    ql.Settings.instance().evaluationDate =\
                        ql.Date().from_date(start_date)
                
                except:
                    ql.Settings.instance().evaluationDate =\
                        ql.Date().from_date(start_date)
                
                curves = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
            
           
            else:   
                
                try:
                    ql.Settings.instance().evaluationDate =\
                        ql.Date().from_date(start_date)
                except:
                    ql.Settings.instance().evaluationDate =\
                        ql.Date().from_date(start_date)
                
                curves = cf.mxn_curves(dic_data)
                    
            
            
         # Outside curves are used    
        else:
            
            try:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(start_date)
            except:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(start_date)

            pass
            
        
            
        npv_yst = self.get_book_npv(start_date, curves, False)
        # print(npv_yst['NPV'].sum())
            
        npv_yst.rename(columns = {'NPV': 'NPV_yst'}, inplace = True)
        
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(end_date)):
            # print('wuu')
            dic_data = curves.dic_data
            del curves
            try:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(end_date)
            except:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(end_date)
            
            
            curves = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
            curves.crvMXNTIIE.complete_ibor_tiie =\
                curves.crvMXNTIIE.complete_ibor_index()
        
        else:
            dic_data = curves.dic_data
            try:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(end_date)
            except:
                ql.Settings.instance().evaluationDate =\
                    ql.Date().from_date(end_date)
            curves = cf.mxn_curves(dic_data, None)
            curves.crvMXNTIIE.complete_ibor_tiie =\
                curves.crvMXNTIIE.complete_ibor_index()
                    
        
        npv_tdy = self.get_book_npv(end_date, curves, False)
        # print(npv_tdy['NPV'].sum())
        
        cf1 = self.get_pfolio_CF_atDate(end_date, npv_tdy)
        cf1.rename({cf1.columns[0]: 'CF'}, axis = 1, inplace = True)
        
        npv_tdy.rename(columns = {'NPV': 'NPV_tdyst'}, inplace = True)
        
        carry = npv_yst.merge(npv_tdy[['TradeID', 'NPV_tdyst']], how = 'left', 
                              left_on = 'TradeID', right_on = 'TradeID')
        carry.drop('index', axis = 1, inplace = True)
        
        
        carry = carry.merge(cf1, how= 'left', left_on = 'TradeID',
                            right_index = True)
        
        ibor_tiie = curves.crvMXNTIIE.ibor_index
        carry_df = pd.DataFrame()
        for i, r in npv_tdy.iterrows():
            fixed_rate = r.FxdRate/100
            if r.CpDate == end_date:
                prev_cp_date = ql.Date().from_date(r.CpDate)
            else:
                prev_cp_date = ql.Date().from_date(r.CpDate) - ql.Period(28, ql.Days)
            
            if ql.Mexico().isHoliday(prev_cp_date):
                prev_cp_date = ql.Mexico().advance(prev_cp_date, ql.Period(1, ql.Days))
            
            fix_date = ibor_tiie.fixingDate(prev_cp_date)
            if fix_date <= ql.Date().from_date(end_date):
                float_rate = ibor_tiie.fixing(fix_date)
                carry_i = (fixed_rate - float_rate)*r.Notional/360
            else:
                float_rate = 0
                carry_i = 0
            
            carry_df_a = pd.DataFrame({'Carry': [carry_i], 
                                       'FxdRate': [fixed_rate*100],
                                       'FltRate': [float_rate*100], 
                                       'Notional': [r.Notional],
                                       'SwpType': [r.SwpType]}, index=[r.TradeID])
            carry_df = pd.concat([carry_df, carry_df_a])
        
        # carry_df.index = carry_df.index.astype(str)
        
        self.carry_df = carry_df
        
        
        carry['Carry_Roll'] = carry['NPV_tdyst'] - carry['NPV_yst'] +\
            carry['CF'].fillna(0)
        
        carry = carry.merge(carry_df[['Carry']], how = 'left', 
                                      left_on='TradeID', right_index=True)
    
        carry['Roll'] = carry['Carry_Roll'] - carry['Carry']
        
        carry.drop('SwpObj', axis = 1, inplace = True)
        
        if inplace:
            self.carry = carry
            return None
        
        else:
            return carry
          
         
    def get_dv01(self, dt_today: datetime = None, curves: cf.mxn_curves = None,
                 inplace: bool = False) -> pd.DataFrame:
        if curves:
            self.get_book_npv(dt_today, curves, True)
        
        dv01s = [swp.flat_dv01() for swp in self.dfbookval.SwpObj]
        dv01s_df = pd.DataFrame(dv01s, columns = ['DV01'],
                                index = self.dfbookval.TradeID)
        if inplace:
            self.dv01_df = dv01s_df
            return None
        else:
            return dv01s_df
        
    def get_PnL(self, start_date: datetime, end_date: datetime, **kwargs):
        
        """ Get PnL 
        

        Parameters
        ----------
        
        start_date : datetime
            date for evaluating last portfolio
        end_date : datetime
            date for evaluating current portfolio


        Returns
        -------
        None
            prints PnL and detailed PnL

        """
        if 'historical' in kwargs.keys():
            historical = kwargs['historical']
        else:
            historical = False
        
        
            
        
        
        historical_tdy = []
        historical_yst = []
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(end_date)) \
            or historical:
            historical_tdy = ['MXN_OIS']
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(start_date)) \
            or historical:
            historical_yst = ['MXN_OIS']
        
        # Data input for start date's curves
        
        str_inputsFileName = 'TIIE_CurveCreate_Inputs'
        str_inputsFileExt = '.xlsx'
        str_file = str_inputsFileName + str_inputsFileExt
        
        if 'start_dic' in kwargs.keys():
            dic_data_yst = kwargs['start_dic']
        else:
            dic_data_yst = cf.pull_data(str_file, start_date.date())
        dt_cf = end_date
                
        try:
            ql.Settings.instance().evaluationDate = ql.Date().from_date(start_date)
        except:
            ql.Settings.instance().evaluationDate = ql.Date().from_date(start_date)
        
        curves_yst = cf.mxn_curves(dic_data_yst, None, historical_yst)
        
        if historical:
            tiie = cf.MXN_TIIE(dic_data_yst['MXN_TIIE'], 'Cubic', 
                                curves_yst.crvMXNOIS, True)
            tiie.bootstrap()
            curves_yst.crvMXNTIIE = tiie
        
        carry1 = self.get_carry_roll(start_date, end_date, curves_yst)
        
        if historical_tdy:
            curves_yst.crvUSDOIS = None
            curves_yst.crvUSDSOFR = None
            curves_yst.crvMXNOIS.swap_curve = None
            curves_yst.crvMXNOIS.discount_curve = None
            
        # Data input for today's curves
        if 'end_dic' in kwargs.keys():
            dic_data_tdy = kwargs['end_dic']
        else:
            dic_data_tdy = cf.pull_data(str_file, end_date.date())
            
        
        if not historical:
            
            try:
                ql.Settings.instance().evaluationDate = ql.Date().from_date(end_date)
            except:
                ql.Settings.instance().evaluationDate = ql.Date().from_date(end_date)
                
            curves_tdy = cf.mxn_curves(dic_data_tdy, None, historical_tdy)
        
        # # Curve creation
        # curves_tdy = cf.mxn_curves(dic_data_tdy, None, historical_tdy)
        
        tdy_npv = self.get_book_npv(end_date, curves_tdy)
        
        
        
        tdy_npv.drop(columns = 'SwpObj', inplace = True)
        
        carry1.set_index('TradeID', inplace = True)
        tdy_npv.set_index('TradeID', inplace = True)
        columns_compare = ['NPV_yst', 'NPV_tdyst', 'CF', 'Carry_Roll',
                           'Carry', 'Roll']
        tdy_npv.rename({'NPV': 'NPV_tdy'}, axis = 1, inplace = True)
        # print(tdy_npv.columns)
        dfbook_pnl = pd.concat([tdy_npv, carry1[columns_compare].fillna(0)], axis = 1)
        
        # print(dfbook_pnl.columns)
        
        dfbook_pnl['NPV_Change'] = dfbook_pnl['NPV_tdy'] - dfbook_pnl['NPV_yst']
        
        dfbook_pnl['NPV_Change_w/CF'] = dfbook_pnl['NPV_Change'] +\
            dfbook_pnl['CF']
            
        dfbook_pnl['Market_Movement'] = dfbook_pnl['NPV_Change_w/CF'] -\
            dfbook_pnl['Carry_Roll']
        
        dfbook_pnl['PnL'] = dfbook_pnl['NPV_Change_w/CF'] + dfbook_pnl['Fees']
        
        columns= ['BookID', 'TradeDate', 'Notional', 'StartDate', 'Maturity',
                  'CpDate', 'FxdRate', 'SwpType', 'mtyOnHoliday', 'NPV_yst', 
                  'NPV_tdyst', 'NPV_tdy', 'CF', 'NPV_Change', 'NPV_Change_w/CF',
                  'Carry_Roll', 'Carry', 'Roll', 'Market_Movement', 'Fees', 
                  'PnL']
        
        return (dfbook_pnl[columns])
    
    
def get_pfolio_PnL(start_date: datetime, end_date: datetime,
                   dfposswaps: pd.DataFrame, bookID: int):
    """
    

    Parameters
    ----------
    start_date : datetime
        Start date for PnL.
    end_date : datetime
        DESCRIPTION.
    dfposswaps : pd.DataFrame
        DESCRIPTION.
    bookID : int
        DESCRIPTION.

    Returns
    -------
    pd_strats : TYPE
        DESCRIPTION.

    """
    
    ql.Settings.instance().evaluationDate = ql.Date().from_date(start_date)
    
    input_cwd = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/'+\
        'Main Codes/Quant Management/OOP codes/'
    input_file = 'TIIE_CurveCreate_Inputs.xlsx'
    
    str_file = input_cwd + input_file
    
    
    
    port_pos = pfolio.from_posSwaps(dfposswaps, bookID)
    
    pnl = port_pos.get_PnL(start_date, end_date)
    
    try:
       
        blotter_dir = '//TLALOC/tiie/Blotters/'
        blotter_str = end_date.strftime('%y%m%d') + '.xlsx'
        blotter = pd.read_excel(blotter_dir + blotter_str, 'BlotterTIIE_auto', 
                                skiprows=2)
        international_fees = blotter[(blotter['Book']==bookID) & 
                                     (blotter['Ctpty'] == 'u8082')]\
            ['Cuota compensatoria / unwind'].sum()
        local_fees = blotter[(blotter['Book']==bookID) & 
                             (blotter['Ctpty'] == 'u8087')]\
            ['Cuota compensatoria / unwind'].sum()
        blotter_tdy = pfolio.from_blotter(blotter,
                                          end_date, bookID = bookID)

        historical_tdy = []
        if ql.UnitedStates(1).isHoliday(ql.Date().from_date(end_date)):
            historical_tdy = ['USD_OIS']
            ql.Settings.instance().evaluationDate = ql.Date().from_date(end_date)
        
        dic_data = cf.pull_data(str_file, end_date.date())
        
        curves = cf.mxn_curves(dic_data, None, historical_tdy)
        
        blotter_day = blotter_tdy.dfbook.copy()

        blotter_tdy.get_book_npv(end_date, curves, inplace = True)

        blotter_day['TradeID'] =\
            blotter_day['TradeID'].astype(str).str.strip()
    # dic_data = cf.pull_data(str_file, start_date.date())
        blotter_day['Folio Original'][~blotter_day['Folio Original'].isna()] =\
            blotter_day['Folio Original']\
                [~blotter_day['Folio Original'].isna()].astype(int).astype(str)
        
        pnl.index = pnl.index.astype(str).str.strip()
        blotter_tdy.dfbookval.TradeID =\
            blotter_tdy.dfbookval.TradeID.astype(str).str.strip()
        # Unwind handling
        
        ## Same Day
        unwind_index = list(blotter_day[blotter_day['Folio Original']\
                                        .isin(blotter_day['TradeID'])].index)
            
        blotter_tdy.dfbookval['NPV'][blotter_tdy.dfbookval['TradeID'].isin(
            blotter_day.loc[unwind_index]['Folio Original'])] = 0
        blotter_tdy.dfbookval['Fees'][blotter_tdy.dfbookval['TradeID'].isin(
            blotter_day.loc[unwind_index]['Folio Original'])] =\
            blotter_day.loc[unwind_index]['Fees'].values
        blotter_tdy.dfbookval = blotter_tdy.dfbookval.drop(unwind_index)
        
        ## Past Trades
        unwind_index = list(
            blotter_day[blotter_day['Folio Original'].isin(
                pnl.index)].index)
         

        pnl['NPV_tdy'].loc[blotter_day.loc[unwind_index]['Folio Original']] =\
           pnl['NPV_yst'].loc[blotter_day.loc[unwind_index]['Folio Original']]
        

        
        pnl['Fees'].loc[blotter_day.loc[unwind_index]['Folio Original']] =\
            blotter_day.loc[unwind_index].set_index('Folio Original')\
                ['Fees'].values - pnl['NPV_tdy'].loc[
                    blotter_day.loc[unwind_index]['Folio Original']]    
        
        blotter_tdy.dfbookval = blotter_tdy.dfbookval.drop(unwind_index)
        blotter_tdy.dfbookval = blotter_tdy.dfbookval.set_index('TradeID')
        blotter_tdy.dfbookval.rename({'NPV': 'NPV_tdy'}, axis = 1,
                                     inplace = True)
        
        # Detailed file creation
        pnl = pd.concat([pnl, blotter_tdy.dfbookval]).fillna(0)
        pnl['NPV_Change'] = pnl['NPV_tdy'] - pnl['NPV_yst']
        pnl['NPV_Change_w/CF'] = pnl['NPV_Change'] + pnl['CF']
        
        pnl['PnL'] = pnl['NPV_Change_w/CF'] + pnl['Fees']
 
    except:
        international_fees = 0
        local_fees = 0
        pass
    
    carry_roll = pnl['Carry_Roll'].sum()
    carry = pnl['Carry'].sum()
    roll = carry_roll - carry
    mktmvmnt = pnl['Market_Movement'].sum()
    
    try:
        bltnpv = blotter_tdy.dfbookval['NPV_tdy'].sum()
        
    except:
        bltnpv = 0
    
    pnl_s = pnl['PnL'].sum()
    delta_npv = pnl['NPV_Change'].sum()
    total_fees =  pnl['Fees'].sum()
    fees = total_fees - international_fees - local_fees
    cf1 = pnl['CF'].sum()
    
    print('\n')
    if bookID:
        print(f'PnL for Book {bookID:.0f}'.center(52, '-'))
    
    print(f'\n\nTotal: {pnl_s:,.0f}' +
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
    
    if bookID == 8085:
        year = end_date.year
        str_spread = '//tlaloc/Cuantitativa/Fixed Income'+\
                                     '/IRS Tenors Daily Update/SPREADS'+\
                                     f'/Spreads Code/SPREADS_{year}.xlsx'
        spreads_data = pd.ExcelFile(str_spread)
        dic_strats = {}
        colums = ['Folio', 'Type', 'Name']
   
        #Spreads
        spread_names = [sht for sht in spreads_data.sheet_names 
                        if 'Spread' in sht and 'data' in sht]
        nums = [int(name[7:9]) for name in spread_names]
        nums.sort()
        print('\n')
        print('PnL by Strategy'.center(52, '-'))
        print(f'\nTotal PnL: ${pnl_s:,.0f}\n')
        for n in nums:
            sprds = pd.read_excel(str_spread, sheet_name = f'Spread_{n}_data',
                                  skiprows=1)[colums]
            
            ctrols = sprds[sprds['Type'] == 'SWAP']['Folio'].astype(str).tolist()
            
            pnl_sprd = pnl[pnl.index.isin(ctrols)]['PnL'].sum()
            
            print(f'\tPnL Spread_{n}: ${pnl_sprd:,.0f}')
            
            dic_strats[f'Spread_{n}'] = pnl_sprd
        
        # Duration
        
        dur = pd.read_excel(str_spread, sheet_name = 'DU_data',
                              skiprows=1)[colums]
        
        ctrols = dur[dur['Type'] == 'SWAP']['Folio'].astype(str).tolist()
        pnl_dur = pnl[pnl.index.isin(ctrols)]['PnL'].sum()
        
        print(f'\n\tPnL Duration Total: {pnl_dur:,.0f}')
        dur_data = dur[dur['Type'] == 'SWAP']
        for nam in dur_data['Name'].sort_values().unique():
            ctrols = dur_data[dur_data['Name']==nam]['Folio'].astype(str).tolist()
            pnl_dur_a = pnl[pnl.index.isin(ctrols)]['PnL'].sum()
            print(f'\t\t{nam}: ${pnl_dur_a:,.0f}')
            dic_strats[f'{nam}'] = pnl_dur_a
            
        print(f'\n\tTrading PnL: {bltnpv+total_fees:,.0f}')
        
        
        spreads_data.close()
        pd_strats = pd.DataFrame(dic_strats, index = ['PnL']).T
        return pd_strats
    
    # Save Excel
    columns_order = ['BookID', 'TradeDate', 'Notional', 'StartDate', 
                     'Maturity', 'CpDate', 'FxdRate', 'SwpType', 
                     'mtyOnHoliday',  'NPV_yst', 'NPV_tdyst', 'NPV_tdy', 'CF', 
                     'NPV_Change', 'NPV_Change_w/CF', 'Carry_Roll', 'Carry', 'Roll', 
                     'Market_Movement', 'Fees', 'PnL']

    dt_today_str = end_date.strftime('%Y%m%d')
    
    if bookID:
        str_cwd_paco = '//TLALOC/Cuantitativa/Fixed Income' +\
            '/IRS Tenors Daily Update/Detailed Files/'
        pnl[columns_order].to_excel(str_cwd_paco + f'book{bookID}_' + 
                                           dt_today_str + '_npv1.xlsx')
    else:
        
        str_cwd_paco = '//TLALOC/Cuantitativa/Fixed Income' +\
            '/TIIE IRS Valuation Tool/Paco D/Detailed files/'
        pnl[columns_order].to_excel(str_cwd_paco + 'pfolio_' + 
                                           dt_today_str + '_npv.xlsx')

def countdown(t: int) -> None:
    """Begins a countdown

    Parameters
    ----------
    t : int
        number of seconds 

    Returns
    -------
    Countdown in seconds

    """
    
    while t:
        mins, secs = divmod(t, 60)
        timer = '{:02d}:{:02d}'.format(mins, secs)
        print("\n",timer, end="\r")
        time.sleep(1)
        t -= 1
    print('\n Comenzamos')

def PnL(today_date: datetime, historical: bool = False, t: int = 0) -> None:
    """PnL function
    Gets the desired books to evaluateand starts the evaluation
    

    Parameters
    ----------
    today_date : datetime
        date of today
    t: int
        number of seconds to wait

    Returns
    -------
    None
        Prints the PnL and saves a detaile file

    """

    
    print('Please write the desired books to evaluate, '
          +'when finished, press "0": ')
    l_book=[]
    l=1
    usu = True
    while usu:
        book = int(input(f'         {l}. '))
        l=l+1
        if book == 0:
            break
        else:
            l_book.append(book)
    
    
    if t > 0:
        # function call
        countdown(int(t))
    
    if len(l_book) != 0:
        # Last workig day
        last_date = \
            ql.Mexico().advance(ql.Date().from_date(today_date), 
                                ql.Period(-1, ql.Days)).to_date()
        
        last_date = datetime.strptime(last_date.strftime('%Y%m%d'), '%Y%m%d')
        
        # last working day as string
        last_date_str = last_date.strftime('%Y%m%d')
        
        # Swaps File
        str_posswps_file = r'//TLALOC/tiie/posSwaps/PosSwaps' + last_date_str +\
            '.xlsx' # PosSwaps file 
        
        # Valuation date
        dt_today = today_date.date()
        ql_dt_today = ql.Date(dt_today.day, dt_today.month, dt_today.year)
        ql.Settings.instance().evaluationDate = ql_dt_today
        
        # Portfolios
        df_tiieSwps = pd.read_excel(str_posswps_file)
    
    
        
        # PnL Run
        dt_val_yst, dt_val_tdy = last_date, today_date
        datalist = []
        for book in l_book:
            data=get_pfolio_PnL(dt_val_yst, dt_val_tdy, df_tiieSwps, book)  
            datalist.append(data)
    
        return datalist
        

def get_bucket_risk(posswaps: pd.DataFrame, curves: cf.mxn_curves, 
                     dt_today: datetime, krr_type: str, bookID: int,
                     dt_fut: datetime = None) -> None:
    
    str_today = dt_today.strftime('%Y%m%d')
                    
    if krr_type == 'granular':
        
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
            
            book_pfolio = pfolio.from_posSwaps(posswaps, bookID = bookID)
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
            bucketcarry = carry(str_file, df_book, dt_today, dt_ystday)
            
            dic_book_valrisk['Carry'] = bucketcarry
            dic_grisks[bookID] = dic_book_valrisk
        
        else:
            book_pfolio = None
            dic_book_valrisk = dic_grisks[bookID]
            fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            
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
            future_date = dic_frisks['FutureDate']
            
            if future_date == dt_fut.strfime('%Y-%m-%d'):
                date_flag = False
                
            else:
                date_flag = True

        except: 
            dic_frisks = {}
            date_flag = True
            
        if date_flag or bookID not in book_found_f:
            book_int = int(bookID[:-1])
            
                   
            print(f'\nRunning DV01 risk for {bookID} book in '+'PosSwaps'+str_today+'...')
            print(f'\nBook {book_int} Future Key Rate Risk...')
            
            if ql.UnitedStates(1).isHoliday(ql.Date().from_date(dt_fut)):
                dic_data_fut = {k:v.copy() for k,v in curves.dic_data.items()}
                curves_fut = cf.mxn_curves(dic_data_fut)
                curves_fut.crvUSDOIS = None
                curves_fut.crvUSDSOFR = None
                ql.Settings.instance().evaluationDate = ql.Date().from_date(dt_fut)
                curves_fut.KRR_crvs(False, True)
                
            else:
                ql.Settings.instance().evaluationDate = ql.Date().from_date(dt_fut)
                dic_data_fut = {k:v.copy() for k,v in curves.dic_data.items()}
                curves_fut = cf.mxn_curves(dic_data_fut)
                curves_fut.KRR_crvs(True, True)
            
            
            book_pfolio = pfolio.from_posSwaps(posswaps, bookID = book_int)
            dic_book_valrisk = book_pfolio.get_risk_byBucket(dt_fut, curves_fut)
            #print(book_pfolio.curves.crvMXNTIIE.nodes)
            fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            print(f"\nAt {dt_fut}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']*fx_rate:,.0f}")
            
            dic_frisks[bookID] = dic_book_valrisk
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            print(fx_rate)
         
            print(dfbook_br)
            
        else:
            dic_book_valrisk = dic_grisks[bookID]
            fx_rate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            book_pfolio = None
            
            print(f'\nBook {bookID} Future Key Rate Risk...')
            
            print(f"\nAt {dt_fut}\nNPV (MXN): "+\
                  f"{dic_book_valrisk['NPV_Book']:,.0f}")
            # Bucket Risk (KRR)
            dfbook_br = dic_book_valrisk['DV01_Book']
            ## Display KRR
            print(fx_rate)
             
            print(dfbook_br)
        
        dic_frisks['FutureDate'] = str(dt_fut)
    
        cf.save_obj(dic_frisks, 'Future Risks/risk_'+str_today)
        dic_frisks['obj'] = book_pfolio
        return dic_frisks

        
    else:
        try:
            # print('wuuuuuu')
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
                book_pfolio = pfolio.from_posSwaps(posswaps, bookID = bookID)
                
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
                        
                        posswaps1 = posswaps.copy()
                        posswaps1['swp_nombre'] =\
                            posswaps1['swp_nombre'].str.strip()
                        dfbook = posswaps1\
                            [posswaps1['swp_nombre']==b]\
                                .reset_index(drop=True)
                        if b == 'FINAMEX OPERACIONES SWAPS':
                            
                            dfbook['swp_nomcte'] = dfbook['swp_nomcte'].str.strip()
                            
                            dfbook = dfbook[dfbook['swp_nomcte'] != 'INTERNO']\
                                .reset_index(drop=True)

                        book_pfolio = pfolio.from_posSwaps(dfbook, curves, dt_today)
                        
                        print(f'\nRunning DV01 risk for {b} book in '+'PosSwaps'+
                              str_today+'...')
                        print(f'\nBook {b} Key Rate Risk...')
                        dic_book_valrisk = book_pfolio.get_risk_byBucket()
                        
                        print(f"\nAt {dt_today}\nNPV (MXN): "+\
                              f"{dic_book_valrisk['NPV_Book']:,.0f}")
                        dfbook_br = dic_book_valrisk['DV01_Book']
                        ## Display KRR
                        print(book_pfolio.fx_rate)
                        
                        print(dfbook_br)
                
                else:
                    print(f'{bookID} does not exist')
            

        else:
            dic_book_valrisk = dic_risks[bookID]
            book_pfolio = None
            fxrate = curves.dic_data['USDMXN_XCCY_Basis']['Quotes'][0]
            
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
    


def future_risk (l_gusu: list, ql_dt_fut: ql.Date) -> (list, list, ql.Date):
    
    """ Gets future book list
    

    Parameters
    ----------
    l_gusu : list
        list of granular books
    ql_dt_fut : ql.Date
        Date of valuation

    Returns
    -------
    l_gusu : list
        list of granular books
    l_fusu : list
       list of future books
    ql_dt_fut : ql.Date
        Date of valuation

    """
    
    l_fusu=[]
    l=1
    fusu = True
    dateIsNotOk = True
    
    while dateIsNotOk:
        print('\n\nFuture Date')
        try:
            finput_year = int(input('\tYear: '))
            finput_month = int(input('\tMonth: '))
            finput_day = int(input('\tDay: '))
            
        except:
            print('Wrong date! Try again pls.')
            dateIsNotOk = True
            
        try:
            dt_fut = datetime(finput_year, finput_month, finput_day).date()
            dateIsNotOk = False
            
        except:
            print('Wrong date! Try again pls.')
            dateIsNotOk = True
            
    ql_dt_fut = ql.Date(dt_fut.day, dt_fut.month, dt_fut.year)
    
    if l_gusu:
        print('\nPlease write the books you want to evaluate '
              + '(Press "0" when finished): ')
        while fusu:
            fusuario=input(f'         {l}. ')
            l=l+1
            if fusuario == "0":
                break
    
            else:
                l_fusu.append(fusuario+'F')
            
            if len(set(l_fusu)) != len(l_fusu):
                print('\nPLEASE WRITE DIFFERENT BOOKS,'
                      ' the duplicate will be removed')
                l_fusu = list(set(l_fusu))

    else:
        print('\nPlease write the books you want to evaluate '
              + '(Press "0" when finished or "G" for granular risk): ')
        while fusu:
            fusuario=input(f'         {l}. ')
            l=l+1
            if fusuario == "0":
                break
            elif fusuario.lower() == 'g':
                l_gusu, l_fusu, ql_dt_fut = granular_risk(l_fusu, ql_dt_fut)
                break
                
            else:
                l_fusu.append(fusuario+'F')
    
            if len(set(l_fusu)) != len(l_fusu):
                print('\nPLEASE WRITE DIFFERENT BOOKS,'
                      ' the duplicate will be removed')
                l_fusu = list(set(l_fusu))
            
    return l_gusu, l_fusu, ql_dt_fut
    
def granular_risk(l_fusu: list, ql_dt_fut: list) -> (list, list, ql.Date):
    
    l_gusu=[]
    l=1
    gusu = True
    
    if l_fusu:
        print('\nPlease write the books you want to evaluate '
              + '(Press "0" when finished): ')
        while gusu:
            gusuario=int(input(f'         {l}. '))
            l=l+1
            if gusuario == 0:
                break
    
            else:
                l_gusu.append(gusuario)
        
            if len(set(l_gusu)) != len(l_gusu):
                print('\nPLEASE WRITE DIFFERENT BOOKS,'
                      ' the duplicate will be removed')
                l_gusu = list(set(l_gusu))

    
    else:
        print('\nPlease write the books you want to evaluate '
              +'(Press "0" when finished or "F" for future risk evaluation): ')
        
        while gusu:
            gusuario=input(f'         {l}. ')
            l=l+1
            if gusuario == "0":
                break
            elif gusuario.lower() == 'f':
                l_gusu, l_fusu, ql_dt_fut = future_risk(l_gusu, ql_dt_fut)
                break
            else:
                l_gusu.append(int(gusuario))
        
            if len(set(l_gusu)) != len(l_gusu):
                print('\nPLEASE WRITE DIFFERENT BOOKS,'
                      ' the duplicate will be removed')
                l_gusu = list(set(l_gusu))
            
    return l_gusu, l_fusu, ql_dt_fut
        

def carry(str_file: str, df_book: pd.DataFrame,
          dt_today: datetime, dt_yst: datetime, 
          tenors: list  = []) -> pd.DataFrame:
    
    """Gets Bucket Carry of a portfolio
     
    
     Parameters
     ----------
     str_file : str
         file of nodes
     df_book : pd.DataFrame
         Swap portflio DataFrame
     dt_today : datetime
         Day of valuation
     dt_yst : datetime
         Day before valuation
    
     Returns
     -------
     bucketCarry: pd.Series
         Carry of trades by bucket
         
    
     """
    historical_tdy = False
    historical_yst = False
    if ql.UnitedStates(1).isHoliday(ql.Date().from_date(dt_today)):
        historical_tdy = ['MXN_OIS']
        
    if ql.UnitedStates(1).isHoliday(ql.Date().from_date(dt_yst)):
        historical_yst = ['MXN_OIS']
        
    dic_data = cf.pull_data(str_file, dt_yst)
    curves = cf.mxn_curves(dic_data, None, historical_tdy)
 
    tenor2dt = {'B': 1, 'W': 7, 'L': 28}
    if not tenors:
        tenors = pd.Series(['%1B', '%1W', '%1L', '%2L', '%3L', '%4L', '%5L', 
                            '%6L', '%9L','%13L', '%19L', '%26L', '%39L', '%52L',
                            '%65L', '%78L', '%91L', '%104L', '%117L', '%130L', 
                            '%143L', '%156L', '%195L', '%260L', '%390L'])
    else:
        tenors = pd.Series(tenors)
                            
                            
                            
    
    pfolio_book_ystdy = pfolio(df_book)
    
    dfbookval = pfolio_book_ystdy.get_carry_roll(dt_yst, dt_today, curves)

    
    
    dfbookval['term'] = (dfbookval.Maturity.dt.date -\
        dfbookval.evalDate).dt.days
    dfbookval['term'] = (dfbookval.Maturity.dt.date -\
        dfbookval.evalDate).dt.days
    
    condi = tenors.apply(lambda x: int(x[1:-1]) * tenor2dt[x[-1]]).tolist()
    condis = [(condi[t] + condi[t-1])/2 for t in range(1, len(condi))] 
    conditions = [dfbookval.term <= condis[0]] +\
        [(condis[t-1] <= dfbookval.term) & (dfbookval.term <= condis[t]) 
         for t in range(1, len(condis))] +\
            [dfbookval.term > condis[-1]]
    
    dfbookval['Bucket'] = np.select(conditions, tenors.tolist())
    
    dfbucketcarry = pd.pivot_table(dfbookval, values = 'Carry', 
                                   index = 'Bucket', aggfunc = np.sum)
    
    notenors = list(set(tenors.tolist()).difference(set(dfbucketcarry.index)))
    
    dfbucketcarry = pd.concat([dfbucketcarry, 
                               pd.DataFrame([0]*len(notenors), 
                                            index = notenors, 
                                            columns = ['Carry'])])
    
    
    return dfbucketcarry.loc[tenors.tolist()]



def bucketRisk(dt_today: datetime, df_tiieSwps: pd.DataFrame,
               curves_tdy: cf.mxn_curves = None) -> dict:
    """Gets Bucket Risk for the desired books
    

    Parameters
    ----------
    dt_today : datetime
        Day of valuation
    dfpos: pd.DataFrame
        PosSwaps file
    curves: cf.mxn_curves
        cuves for valuation

    Returns
    -------
    dict
        dictionary with the risks by book

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
    print('\nPlease write the books you want to evaluate (Press "0" when finished,'
          + ' "G" for granular risk, or "F" for future risk evaluation): ')
    l_usu=[]
    l_gusu = []
    l_fusu = []

    l=1
    granular_flag = False
    future_flag = False
    while usu:
        usuario = input(f'         {l}. ')
        if usuario == "0":
            break
        
        elif usuario.lower() == "g":
            ql_dt_fut = 0
            l_gusu, l_fusu, ql_dt_fut = granular_risk(l_fusu, ql_dt_fut)
            break
        
        elif usuario.lower() == 'f':
            ql_dt_fut = 0
            l_gusu, l_fusu, ql_dt_fut = future_risk(l_gusu, ql_dt_fut)
            break
        
        else:
            try: 
                l_usu.append(int(usuario))
            except:
                l_usu.append(usuario.lower())
            l = l+1

        if len(set(l_usu)) != len(l_usu):
            print('\nPLEASE WRITE DIFFERENT BOOKS, the duplicate will be removed')
            l_usu = list(set(l_usu))
    
    if len(l_usu) == 0 and len(l_fusu) == 0 and len(l_gusu) == 0:
        return None, None
    
    if not curves_tdy:
        print(f'\nCalculating Curves for {dt_today.date()}...')
        str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
            'Tool/Main Codes/Portfolio Management/OOP Codes/'
            
            
        str_inputsFileName = 'TIIE_CurveCreate_Inputs'
        str_inputsFileExt = '.xlsx'
        str_file = str_dir + str_inputsFileName + str_inputsFileExt
            
        dic_data = cf.pull_data(str_file, dt_today.date())
        if ql.UnitedStates(1).isHoliday(ql_dt_today):
            curves_tdy = cf.mxn_curves(dic_data, None, ['MXN_OIS'])
            
        else:   
            curves_tdy = cf.mxn_curves(dic_data)
            
        dftiie_12y = cf.add_tiie_tenors(curves_tdy, ['%156L'])
        
        curves_tdy.change_tiie(dftiie_12y)
        gran = True
    
    else:
        if curves_tdy.dic_data['MXN_TIIE'].shape[0] > 15:
            
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
                'Tool/Main Codes/Portfolio Management/OOP Codes/'
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_input = str_dir + str_inputsFileName + str_inputsFileExt
            tenors = pd.read_excel(str_input, sheet_name = 'MXN_TIIE', 
                                   usecols = ['Tenor'])['Tenor'].tolist()
            
            dftiie_12y = cf.add_tiie_tenors(curves_tdy, tenors + ['%156L'], True)
            curves_tdy.change_tiie(dftiie_12y)
            gran = True
        else:
            gran = False
            

    dics = {}
    
    if l_usu:
        if not curves_tdy or gran:
            print('Calculating KRR curves...')
            curves_tdy.KRR_crvs(True, True)
            print('Done!')
            
        for usu in l_usu:
            
            dic_risk = get_bucket_risk(df_tiieSwps, curves_tdy, dt_today,
                                       'normal', usu)
        dics['normal'] = dic_risk
        
    if l_gusu: 
        print('Calculating Granular KRR curves...')
        df_tiie_gran = cf.granular(curves_tdy)
        curves_tdy.change_tiie(df_tiie_gran)
        curves_tdy.KRR_crvs(True, True)
        print('Done!')
        for usu in l_gusu:
            dic_grisk = get_bucket_risk(df_tiieSwps, curves_tdy, dt_today,
                                        'granular', usu)
        dics['granular'] = dic_grisk
        
    if l_fusu: 
        if curves_tdy.dic_data['MXN_TIIE'].shape[0] > 15:
            # print('Calculating KRR curves...')
            str_dir = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation '+\
                'Tool/Main Codes/Portfolio Management/OOP Codes/'
            str_inputsFileName = 'TIIE_CurveCreate_Inputs'
            str_inputsFileExt = '.xlsx'
            str_input = str_dir + str_inputsFileName + str_inputsFileExt
            tenors = pd.read_excel(str_input, sheet_name = 'MXN_TIIE', 
                                   usecols = ['Tenor'])['Tenor'].tolist()
            
            dftiie_12y = cf.add_tiie_tenors(curves_tdy, tenors + ['%156L'], True)
            curves_tdy.change_tiie(dftiie_12y)
            # curves_tdy.KRR_crvs(True, True)
            # print('Done!')
            
        for usu in l_fusu:
            dic_frisk = get_bucket_risk(df_tiieSwps, curves_tdy, dt_today, 
                            'future', usu, ql_dt_fut.to_date())
        
        dics['future'] = dic_frisk
    
    return dics, curves_tdy
    





#%%

if __name__ == '__main__':
    st_dt = datetime(2023,12,7)
    ql.Settings.instance().evaluationDate = ql.Date().from_date(st_dt)
    
    input_cwd = '//tlaloc/Cuantitativa/Fixed Income/TIIE IRS Valuation Tool/'+\
        'Main Codes/Quant Management/OOP codes/'
    input_file = 'TIIE_CurveCreate_Inputs.xlsx'
    str_file = input_cwd + input_file
        
    # input_sheets = ['USD_OIS', 'USD_SOFR', 'USDMXN_XCCY_Basis', 'USDMXN_Fwds', 
    #                 'MXN_TIIE', 'Granular_Tenors']
    
    dic_data = cf.pull_data(str_file, st_dt.date())
        
    controls = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/'+
                                 'Quant Team/Esteban/FinaQPM/'+
                                 'Quant Portfolio Management.xlsx', 
                                 sheet_name = 'NPV', skiprows = 2, 
                                 usecols = [0])['Controls'].tolist()
    pos = pd.read_excel('//tlaloc/tiie/posSwaps/PosSwaps20231206.xlsx')
    
    curves = cf.mxn_curves(dic_data)#, ['MXN_OIS'])
    # # # curves.KRR_crvs(True, True)
    #%%
    
    a = datetime.now()
    port_pos = pfolio.from_posSwaps(pos[pos['swp_ctrol'].isin(controls)], 1814) 
    # port_pos = pfolio.from_posSwaps(pos, 1814) 
    
    flt_scenario = pd.read_excel('//tlaloc/Cuantitativa/Fixed Income/'+
                                 'Quant Team/Esteban/FinaQPM/'+
                                 'Quant Portfolio Management.xlsx', 
                                 sheet_name = 'Scenario', skiprows = 4, 
                                 usecols = [1,2, 6])
    flt_scenario = pd.concat([pd.DataFrame({flt_scenario.columns[0]: [datetime(2023,12,6)],
                                            flt_scenario.columns[1]: [11.504]}),
                              flt_scenario])
    
    # pnl = port_pos.get_PnL(datetime(2023,10,9), datetime(2023,10,10))
                            
    # carry1 = port_pos.get_carry_roll(st_dt, datetime(2023,10,13), curves)

    step_npv = port_pos.get_book_step_npv(st_dt, curves, flt_scenario)
    step_npv.drop(['SwpObj', 'index', 'evalDate'] ,axis=1, inplace = True)
    
    # get_pfolio_PnL(datetime(2023,10,5), datetime(2023,10,6), pos, 1814)
    # datalist=PnL(st_dt)
    b = datetime.now()
    print(b-a)
    
