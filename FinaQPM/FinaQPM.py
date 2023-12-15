# -*- coding: utf-8 -*-
"""
Created on Tue Nov  7 14:48:59 2023

@author: elopeza
"""

import os
import sys
user_path = os.getcwd()
main_path = user_path + '/Libs/'

# Mater Code import
sys.path.append(main_path)
import portfolio_code as pc

sys.path.append(user_path)


pf_file = 'Quant Portfolio Management.xlsx'

pc.main(pf_file) 

