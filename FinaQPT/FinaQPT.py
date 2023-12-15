# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 10:12:37 2023

@author: QuantTeam

User code for Pricing Tool.

master_code, udf_pricing and Funtions codes are required for this code to work.
"""
# Global Imports
import os
import sys
user_path = os.getcwd()
main_path = user_path + '/Libs/'

# Mater Code import
sys.path.append(main_path)
import master_code as mc

sys.path.append(user_path)

# File names
pricing_file = 'TIIE_IRS_Data.xlsm'
graph_file = 'Bono_TIIE.xlsm'
corros_file = 'Corros.xlsx'


mc.main(pricing_file, graph_file, corros_file)



