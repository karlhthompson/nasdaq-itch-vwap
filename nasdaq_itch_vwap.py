#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# =============================================================================
# Created By  : Karl Thompson
# Created Date: Mon March 25 17:34:00 CDT 2019
# =============================================================================
"""nasdaq_itch_vwap - Generate a table of running volume-weighted average price [VWAP] 
for NASDAQ stocks at every trading hour based on Nasdaq TotalView-ITCH 5.0 data.
Data available at: ftp://emi.nasdaq.com/ITCH/01302019.NASDAQ_ITCH50.gz"""
# =============================================================================
# Imports
# =============================================================================
import pandas as pd
import xlsxwriter
import struct
import gzip

# opening the data file:
itch_data = gzip.open('01302019.NASDAQ_ITCH50.gz','rb')

# reading the first byte of each message:
msg_header = itch_data.read(1)

# initializing the Excel workbook that will store all parsed trade data:
workbook =xlsxwriter.Workbook('trade_data.xlsx')
worksheet = workbook.add_worksheet()
row = 0

# iterating over all messages in the data file:
while msg_header:

    # selecting trade messages:
    if msg_header == b'P':

        # parsing trade messages:
        message = itch_data.read(43)
        un_pkd = struct.unpack('>4s6sQcI8cIQ',message)
        re_pkd = struct.pack('>s4s2s6sQsI8sIQ',msg_header,un_pkd[0],b'\x00\x00',un_pkd[1],
            un_pkd[2],un_pkd[3],un_pkd[4],b''.join(list(un_pkd[5:13])),un_pkd[13],un_pkd[14])
        trade_data = list(struct.unpack('>sHHQQsI8sIQ',re_pkd))

        # filtering for valid trade data with the correct Order Reference Number
        # and Buy/Sell Indicator:
        if trade_data[4] == 0 and trade_data[5] == b'B':
            
            # writing the parsed trade message to the Excel workbook, along with
            # a new field containing the product of trade price and shares:
            worksheet.write(row, 0, trade_data[7].decode()) # stock
            worksheet.write(row, 1, trade_data[3]) # timestamp
            worksheet.write(row, 2, trade_data[6]) # shares
            worksheet.write(row, 3, float(trade_data[8])/1e4) # price
            worksheet.write(row, 4, trade_data[6]*float(trade_data[8])/1e4) # product
            row += 1

    # advancing the file position to the next message:
    msg_header = itch_data.read(1)

# closing the data and workbook files:
itch_data.close()
workbook.close()

# importing the parsed trade data into a Pandas dataframe:
trade_df = pd.read_excel('trade_data.xlsx', index_col=0, names=['Stock', 'Timestamp', 'Shares', 'Price', 'Product'])

# creating a dataframe for hourly running VWAP values:
vwap_df = trade_df.groupby(['Stock']).all().drop(columns=['Timestamp', 'Shares', 'Price', 'Product']) 

# creating a list of trading hours in nanoseconds:
hour_list = [3.6e12 * i for i in [9.5, 10, 11, 12, 13, 14, 15, 16]]

# iterating over the trading hours list:
for hour in hour_list:

    # extracting data for trades that occurred before the specified hour:
    trade_df_copy = trade_df[trade_df.Timestamp <= hour] 

    # grouping the trade dataframe by stock:
    trade_df_groups = trade_df_copy.groupby(['Stock'])
    
    # calculating the mean for all trade data:
    trade_df_mean = trade_df_groups.mean(numeric_only=False)

    # calculating the VWAP for all stocks:
    trade_df_mean['VWAP'] = trade_df_mean['Product'] / trade_df_mean['Shares']

    # merging the calculated VWAP fields into the VWAP dataframe:
    vwap_df = pd.merge(vwap_df, trade_df_mean['VWAP'], on=['Stock'], how='left') 

# adjusting the column names in the VWAP dataframe:
vwap_df.columns = ['VWAP at 09:30AM','VWAP at 10:00AM','VWAP at 11:00AM', 'VWAP at 12:00PM',
                   'VWAP at 01:00PM','VWAP at 02:00PM','VWAP at 03:00PM', 'VWAP at 04:00PM']

# saving the hourly VWAP table in Excel format:
vwap_df.to_excel("NASDAQ_VWAP_01_30_2019.xlsx")