#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==============================================================================
# Created By  : Karl Thompson
# Created Date: Mon March 25 17:34:00 CDT 2019
# ==============================================================================
"""nasdaq_itch_vwap - Generate a table of running volume-weighted average price
[VWAP] for NASDAQ stocks at trading hours based on Nasdaq TotalView-ITCH 5 data.
Data available at: ftp://emi.nasdaq.com/ITCH/01302019.NASDAQ_ITCH50.gz"""
# ==============================================================================
# Imports
# ==============================================================================
import pandas as pd
import struct
import gzip
import csv

# function to parse select messages in ITCH data:
def parse_itch_data(itch_data):

    # read the first byte of each message in the data file:
    msg_header = itch_data.read(1)

    # initialize the csv file that will store parsed Add Order and Add Order 
    # with MPID messages:
    add_order_data = open('add_order_data.csv','w')
    add_order_wrtr = csv.writer(add_order_data)

    # initialize the csv file that will store parsed Order Executed messages:
    ord_exec_data = open('ord_exec_data.csv','w')
    ord_exec_wrtr = csv.writer(ord_exec_data)

    # initialize the csv file that will store parsed Order Executed With Price 
    # messages:
    ord_exec_pr_data = open('ord_exec_pr_data.csv','w')
    ord_exec_pr_wrtr = csv.writer(ord_exec_pr_data)

    # initialize the csv file that will store parsed Trade messages:
    trade_data = open('trade_data.csv','w')
    trade_wrtr = csv.writer(trade_data)

    # iterate over all messages in the data file:
    while msg_header:

        # process Add Order and Add Order with MPID messages:
        if msg_header == b'A' or msg_header == b'F':
            message = itch_data.read(35)
            if len(message) < 35: break
            un_pkd = struct.unpack('>4s6sQcI8cI',message)
            re_pkd = struct.pack('>s4s2s6sQsI8sI',msg_header,un_pkd[0],
                b'\x00\x00',un_pkd[1],un_pkd[2],un_pkd[3],un_pkd[4],
                b''.join(list(un_pkd[5:13])),un_pkd[13])
            parsed_ao = list(struct.unpack('>sHHQQsI8sI',re_pkd))
            # filter for data with valid Buy/Sell Indicators:
            if parsed_ao[5] == b'B' or parsed_ao[5] == b'S':
                # further filter for data with plausible field values:
                if (parsed_ao[4] < 1e14 and parsed_ao[6] < 1e8):
                    # write the parsed message to the csv file:
                    try:
                        sto = parsed_ao[7].decode() # stock
                    except:
                        sto = '0' # Write 0 if stock byte decode fails
                    tim = parsed_ao[3] # timestamp
                    ref = parsed_ao[4] # order reference number
                    sha = parsed_ao[6] # shares
                    pri = float(parsed_ao[8])/1e4 # price
                    add_order_wrtr.writerow([sto, tim, ref, sha, pri])

        # process Order Executed messages:
        if msg_header == b'E':
            message = itch_data.read(30)
            if len(message) < 30: break
            un_pkd = struct.unpack('>4s6sQIQ',message)
            re_pkd = struct.pack('>s4s2s6sQIQ',msg_header,un_pkd[0],b'\x00\x00',
                un_pkd[1],un_pkd[2],un_pkd[3],un_pkd[4])
            parsed_oe = list(struct.unpack('>sHHQQIQ',re_pkd))
            # filter for data with plausible field values:
            if (parsed_oe[4] < 1e14 and parsed_oe[5] < 1e8 and parsed_oe[6] < 1e11):
                # write the parsed message to the csv file:
                ref = parsed_oe[4] # order reference number
                sha = parsed_oe[5] # shares
                ord_exec_wrtr.writerow([ref, sha])

        # process Order Executed With Price messages:
        if msg_header == b'C':
            message = itch_data.read(35)
            if len(message) < 35: break
            un_pkd = struct.unpack('>4s6sQIQcI',message)
            re_pkd = struct.pack('>s4s2s6sQIQsI',msg_header,un_pkd[0],
                b'\x00\x00',un_pkd[1],un_pkd[2],un_pkd[3],un_pkd[4],un_pkd[5],
                un_pkd[6])
            parsed_oewp = list(struct.unpack('>sHHQQIQsI',re_pkd))
            # filter for data with plausible field values:
            if (parsed_oewp[4] < 1e14 and parsed_oewp[5] < 1e6 and
                parsed_oewp[6] < 1e10 and parsed_oewp[7] == b'Y'):
                # write the parsed message to the csv file:
                ref = parsed_oewp[4] # order reference number
                sha = parsed_oewp[5] # shares
                pri = float(parsed_oewp[8])/1e4 # new price
                ord_exec_pr_wrtr.writerow([ref, sha, pri])

        # process Trade messages:
        if msg_header == b'P':
            message = itch_data.read(43)
            if len(message) < 43: break
            un_pkd = struct.unpack('>4s6sQcI8cIQ',message)
            re_pkd = struct.pack('>s4s2s6sQsI8sIQ',msg_header,un_pkd[0],
                b'\x00\x00',un_pkd[1],un_pkd[2],un_pkd[3],un_pkd[4],
                b''.join(list(un_pkd[5:13])),un_pkd[13],un_pkd[14])
            parsed_t = list(struct.unpack('>sHHQQsI8sIQ',re_pkd))
            # filter for data with valid Order Reference Numbers
            # and Buy/Sell Indicators:
            if parsed_t[4] == 0 and parsed_t[5] == b'B':
                # write the parsed message to the csv file:
                sto = parsed_t[7].decode() # stock
                tim = parsed_t[3] # timestamp
                sha = parsed_t[6] # shares
                pri = float(parsed_t[8])/1e4 # price
                pro = parsed_t[6]*float(parsed_t[8])/1e4 # product
                trade_wrtr.writerow([sto, tim, sha, pri, pro])

        # advance the file position to the next message:
        msg_header = itch_data.read(1)

    # close the csv files:
    add_order_data.close()
    ord_exec_data.close()
    ord_exec_pr_data.close()
    trade_data.close()


# function to calculate the hourly VWAP based on parsed ITCH data:
def calculate_vwap():

    # import the parsed Add Order data into a Pandas dataframe:
    add_order_df = pd.read_csv('add_order_data.csv', index_col = None, 
            names = ['Stock', 'Timestamp', 'Reference', 'Shares', 'Price'])

    # import the parsed Order Executed data into a Pandas dataframe:
    ord_exec_df = pd.read_csv('ord_exec_data.csv', index_col = None,  
            names = ['Reference', 'Shares'])

    # import the parsed Order Executed With Price data into a Pandas dataframe:
    ord_exec_pr_df = pd.read_csv('ord_exec_pr_data.csv', index_col = None,
            names = ['Reference', 'Shares', 'Price'])

    # import the parsed Trade data into a Pandas dataframe:
    trade_1_df = pd.read_csv('trade_data.csv', index_col = 0,
            names=['Stock', 'Timestamp', 'Shares', 'Price', 'Product'])
    
    # merge the Order Executed data with the Add Order data to extract
    # the executed trades data within:
    trade_2_df = ord_exec_df.merge(add_order_df,on=['Reference'],how='inner')
    trade_2_df = trade_2_df[trade_2_df['Stock']!='0']
    trade_2_df = trade_2_df[['Stock', 'Timestamp', 'Shares_x', 'Price']].set_index('Stock')
    trade_2_df = trade_2_df.rename(columns={"Shares_x": "Shares"})
    trade_2_df['Product'] = trade_2_df['Price']*trade_2_df['Shares']

    # merge the Order Executed With Price data with the Add Order data
    # to extract the executed trades data within:
    trade_3_df = ord_exec_pr_df.merge(add_order_df,on=['Reference'],how='inner')
    trade_3_df = trade_3_df[trade_3_df['Stock']!='0']
    trade_3_df = trade_3_df[['Stock', 'Timestamp', 'Shares_x', 'Price_x']].set_index('Stock')
    trade_3_df = trade_3_df.rename(columns={"Shares_x": "Shares", "Price_x": "Price"})
    trade_3_df['Product'] = trade_3_df['Price']*trade_3_df['Shares']

    # concatenate all three trade dataframes (trades from Trade messages,
    # trades from Executed Order messages, and trades from Executed Order
    # With Price messages) into a comprehensive dataframe:
    trade_df = pd.concat([trade_1_df, trade_2_df, trade_3_df])

    # create a dataframe for hourly running VWAP values:
    vwap_df = trade_df.groupby(['Stock']).all().drop(
            columns=['Timestamp', 'Shares', 'Price', 'Product'])

    # create a list of trading hours in nanoseconds:
    hour_list = [3.6e12 * i for i in [9.5, 10, 11, 12, 13, 14, 15, 16]]

    # iterate over the trading hours list:
    for hour in hour_list:
        # extract data for trades that occurred before the specified hour:
        trade_df_copy = trade_df[trade_df.Timestamp <= hour]
        # group the trade dataframe by stock:
        trade_df_groups = trade_df_copy.groupby(['Stock'])
        # calculate the mean for all trade data:
        trade_df_mean = trade_df_groups.mean(numeric_only=False)
        # calculate the VWAP for all stocks:
        trade_df_mean['VWAP'] = trade_df_mean['Product']/trade_df_mean['Shares']
        # merge the calculated VWAP fields into the VWAP dataframe:
        vwap_df = pd.merge(vwap_df,trade_df_mean['VWAP'],on=['Stock'],how='left')

    # adjust the column names in the VWAP dataframe:
    vwap_df.columns = ['VWAP at 09:30AM','VWAP at 10:00AM','VWAP at 11:00AM',
                       'VWAP at 12:00PM','VWAP at 01:00PM','VWAP at 02:00PM',
                       'VWAP at 03:00PM', 'VWAP at 04:00PM']

    # save the hourly VWAP table in Excel format:
    vwap_df.to_excel("NASDAQ_VWAP_01_30_2019.xlsx")

if __name__ == '__main__':
    
    # open the ITCH data file:
    itch_data = gzip.open('01302019.NASDAQ_ITCH50.gz','rb')

    # parse the data:
    parse_itch_data(itch_data)

    # close the ITCH data file:
    itch_data.close()

    # calculate the hourly VWAP for all stocks:
    calculate_vwap()
