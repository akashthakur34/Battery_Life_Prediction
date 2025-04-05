import requests
import json
import time
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

import re
import collections
import sys


# GLOBAL CONSTANTS
# Timeseries Parameters
tower_id = 'f0223050-778c-11e9-890b-6ff3fd245275'
start_time = '2019-08-01 17:30:00'

interval = 300000 # 600000 = 10 mins interval, which means 180000 = 30 mins interval
time_increment_unix = 86400
time_format = '%Y-%m-%d %H:%M:%S'

req_keys = [
    'time',
    'v_1',
    'current1'
]

# Auth Parameters
auth_username = '*****'
auth_password = '*****'

auth_params = { 
    "username": auth_username, 
    "password": auth_password 
}

# URLs
auth_url = 'http://34.204.179.179:8080/api/auth/login'
timeseries_url = 'http://34.204.179.179:8080/api/plugins/telemetry/DEVICE/{}/values/timeseries?keys={}&startTs={}&endTs={}&interval={}&agg=AVG'

class CycleCalculator():
    '''
        filepath: Path to the excel file to be read. Mandatory Parameter
        battery_id: Battery ID from the file. Mandatory Parameter
        install_datetime: Battery Install DateTime. Default: First datetime of first cycle provided 
        total_cycles: Total number of cycles (charge, discharged combined). Default: 200
        cycle_limit: Battery's cycle limit in Amp-hours. Default: 50
        charge_weight: Weightage given to charge cycle. Default: 0.5. "Discharge weight = 1 - charge weight". 
        
    '''
    
    def __init__(self, install_datetime=None, total_cycles=200, cycle_limit=100, charge_weight=0.5, rated_voltage=48, C=0.5):
        self.total_cycles = total_cycles
        self.cycle_limit = cycle_limit
        self.charge_weight = charge_weight
        self.discharge_weight = 1 - self.charge_weight
        self.rated_voltage = rated_voltage
        self.ideal_energy = self.rated_voltage * self.cycle_limit / 1000 #TODO Check once again
        self.C = C
        
        if install_datetime is not None:
            self.install_datetime = datetime.strptime(install_datetime, time_format) # '%b %d %Y %I:%M%p')
        else:
            self.install_datetime = None
        
        self.data = None
        self.btry = None
        self.days_per_cycle = 0
        # print('Parameter: {}\nInstall Date: {}'.format(install_datetime, self.install_datetime))
        self.URL = "http://64.79.106.106:8080/api/v1/A1_TEST_TOKEN/telemetry"
        
    def get_tabular_data(self, data):
        final_df = pd.DataFrame()

        for key, val in data.items():
            temp_value_list = []
            temp_ts_list = []

            for v in val:
                ts = v.get('ts') / 1000
                value = v.get('value')

                temp_ts_list.append(datetime.utcfromtimestamp(ts).strftime(time_format))
                temp_value_list.append(value)

            final_df[key + '_ts'] = temp_ts_list
            final_df[key] = temp_value_list

            final_df[key + '_ts'] = pd.to_datetime(final_df[key + '_ts'], format=time_format)
            try:
                final_df[key] = pd.to_numeric(final_df[key])
            except Exception as e:
                pass
        
        return final_df   
    
    def get_auth_header(self):
        auth_response = requests.post(auth_url, data=json.dumps(auth_params))
        auth_token = 'Bearer ' + dict(auth_response.json()).get('token')

        header_params = {
            'Content-Type': 'application/json',
            'X-Authorization': auth_token
        }

        return header_params
    
    def build_data(self):
        time_increment_unix = 86400
        time_format = '%Y-%m-%d %H:%M:%S'
        current_time = int(datetime.timestamp(datetime.now()))

        start_time_unix = int(datetime.timestamp(datetime.strptime(start_time, time_format)) * 1000)
        end_time_unix = start_time_unix + (time_increment_unix * 1000)

        df_list = []

        while end_time_unix <= (current_time * 1000):
            timeseries_tower1_response = requests.get(timeseries_url.format(
                tower_id, ','.join(req_keys), 
                start_time_unix, end_time_unix, 
                interval), headers=self.get_auth_header())

            print('Timeseries Response from {} to {}: {}'.format(
                datetime.utcfromtimestamp(start_time_unix/1000).strftime(time_format),
                datetime.utcfromtimestamp(end_time_unix/1000).strftime(time_format),
                timeseries_tower1_response))

            temp_df = self.get_tabular_data(timeseries_tower1_response.json())
            # display(temp_df.head())
            df_list.append(temp_df)

            start_time_unix = start_time_unix + (time_increment_unix * 1000)
            end_time_unix = end_time_unix + (time_increment_unix * 1000)
        print('--- Finished Reading ---')

        df = pd.concat(df_list, ignore_index=True)

        col_list = []
        current_col = None
        voltage_col = None
        for col in df.columns:
            if col == 'time_ts':
                col_list.append(col)
            elif '_ts' not in col and col != 'time':
                col_list.append(col)
                if col.startswith('current'):
                    current_col = col
                elif col.startswith('v_'):
                    voltage_col = col

        df = df[col_list]

        # ['Date_Time', 'Status', 'CycleNum', 'Current_I', 'Total_Voltage', 'Capacity']

        df['Date_Time'] = df['time_ts']
        df['Current_I'] = df[current_col] * -1000
        df['Total_Voltage'] = df[voltage_col] * 1000

        df.drop(['time_ts', current_col, voltage_col], axis=1, inplace=True)

        df['CurrentCharge'] = df['Current_I'] / np.abs(df['Current_I'])
        df['PreviousCharge'] = df['CurrentCharge'].shift(1)
        df['PreviousCharge'].at[0] = df['CurrentCharge'].iloc[0]
        
        df['Status'] = df['CurrentCharge'].map({1: 'Charging', -1: 'Discharging'})
        
        curr_status = None
        prev_status = None

        cycle = 0

        cycle_list = []

        for i, row in df.iterrows():
            curr_status = row['Status']

            if prev_status != curr_status:
                cycle = cycle + 1

            cycle_list.append(cycle)

            prev_status = curr_status
        
        df['CycleNum'] = np.ceil(np.array(cycle_list)/2)
        
        df.drop(['CurrentCharge', 'PreviousCharge'], axis=1, inplace=True)
        
        print('Writing tabular data to tabular_output.xlsx')
        xw = pd.ExcelWriter('output/{}_{}_tabular_output.xlsx'.format(tower_name, battery_id))
        df.to_excel(xw, index=False)
        xw.close()
        
        return df
    
    def get_aggregated_data(self):
        grouped_data = None
        col_map = {
            'CycleNum': 'cycle_no',
            'Status': 'cycle_type',
            'Current_I': 'current',
            'Total_Voltage': 'voltage',
            'Date_Time': 'min_time'
        }
        
        try:
            # Read file
            self.data = self.build_data()
            
            # Get specified battery data
            self.btry = self.data.copy()
            self.btry.reset_index(drop=True, inplace=True)
            
            # Get required columns
            self.btry = self.btry[['Date_Time', 'Status', 'CycleNum', 'Current_I', 'Total_Voltage']].copy()
            
            # Sort data by cycle and datetime
            self.btry.sort_values(by=['CycleNum', 'Date_Time'], inplace=True)
            
            # Aggregate the data
            func = {'Current_I': 'mean', 'Total_Voltage': 'mean', 'Date_Time': 'min'}
            grouped_data = self.btry.groupby(['CycleNum', 'Status']).agg(func).reset_index()
            
            # Renaming Columns
            grouped_data.columns = grouped_data.columns.map(col_map)
            
            # Claculating Max Time
            grouped_data['max_time'] = grouped_data['min_time'].shift(-1)
            grouped_data['max_time'].at[len(grouped_data['max_time']) - 1]  = self.btry['Date_Time'].max()
            
            # Consider only charging and discharging
            grouped_data = grouped_data[grouped_data['cycle_type'].isin(['Charging', 'Discharging'])].copy().reset_index(drop=True)
            
            # Convert the milli-amps to amps and milli-volts to volts
            grouped_data['current'] = grouped_data['current'] / 1000
            grouped_data['voltage'] = grouped_data['voltage'] / 1000
            
            # Hours per cycle and cycle_type
            grouped_data['hours'] = (grouped_data['max_time'] - grouped_data['min_time']).astype('timedelta64[s]') / 3600
            
            # Amp-hours
            grouped_data['AMP_HOUR'] = np.abs(grouped_data['current']) * grouped_data['hours']
            
            # Ratio
            grouped_data['ratio'] = grouped_data['AMP_HOUR'] / (self.cycle_limit * self.C)
            grouped_data['ratio_energy'] = grouped_data['AMP_HOUR'] / self.cycle_limit
            
            # Energy
            grouped_data['energy'] = ((np.abs(grouped_data['current']) * grouped_data['voltage']) * grouped_data['hours']) / 1000
            
            # AFC
            grouped_data['AFC'] = grouped_data['energy']/grouped_data['ratio_energy'] #TODO Check once again
            grouped_data['AFC'].fillna(0.0, inplace=True)
            
            # Previous AFC
            grouped_data['prev_AFC'] = grouped_data['AFC'].shift(2)
            grouped_data['prev_AFC'].fillna(self.ideal_energy, inplace=True)
            grouped_data['prev_AFC'].replace(to_replace=0, value=self.ideal_energy)
            
            # remaining_energy = Previous AFC - present energy 
            grouped_data['remaining_energy'] = grouped_data['prev_AFC'] - grouped_data['energy']
            
            
            ### SOH Calculation ###
            # SOH Numerator
            grouped_data['soh_numerator'] = np.abs(grouped_data['voltage'] * grouped_data['current'] * grouped_data['hours'])
            
            # SOH Denominator
            grouped_data['delta_soc'] = np.abs(grouped_data['current'] * grouped_data['hours'] / self.cycle_limit)
            grouped_data['soh_denominator'] = self.cycle_limit * self.rated_voltage * grouped_data['delta_soc']
            
            # SOH
            grouped_data['soh'] = grouped_data['soh_numerator'] / grouped_data['soh_denominator']
            
            print('Saving Aggregated Data to aggregated_data.xlsx')
            xw = pd.ExcelWriter('output/{}_{}_aggregated_data.xlsx'.format(tower_name, battery_id))
            grouped_data.to_excel(xw, index=False)
            xw.close()
            
        except Exception as e:
            print('Error: {}'.format(e))
        
        return grouped_data
    
    
    def get_diff_days(self, td_big, td_small):
        hrs_in_a_day = 24 # TODO Change to 24 with large data
        td = td_big - td_small
        days = td.days
        seconds_in_days = td.seconds / (3600 * hrs_in_a_day)
        return days + seconds_in_days
    
    
    #
    # Forecast the end date by multiplying with total remaining cycles and days per cycle
    #
    def forecast_end_datetime(self, remaining_cycles):
        remaining_days = remaining_cycles * self.days_per_cycle
        end_date = self.btry['Date_Time'].max() + timedelta(remaining_days)
        return end_date
    
    
    # 
    # Get total completed cycles by using hidsight
    #
    def get_total_completed_cycles(self, completed_cycles):
        
        # Getting total days from given data and calculating days per cycle
        btry_total_days = self.get_diff_days(self.btry['Date_Time'].max(), self.btry['Date_Time'].min())
        self.days_per_cycle = btry_total_days / completed_cycles # self.btry['CycleNum'].nunique()
        
        # Get total days from install date to the first date of the battery data 
        if self.install_datetime is not None:
            total_days_from_install = self.get_diff_days(self.btry['Date_Time'].min(), self.install_datetime)
        else:
            total_days_from_install = 0
        
        # Getting total completed cycles
        total_estimated_cycles_completed = round(total_days_from_install / self.days_per_cycle) + completed_cycles # self.btry['CycleNum'].nunique()
        
        return total_estimated_cycles_completed
    
    
    
    #
    # Calculate remaining cycles
    #
    def calcuate_remaining_cycles(self, df):
        
        # Calculate Remaining Cycles as per the data
        grouped = df.groupby('cycle_type')['ratio'].sum()
        cycles_completed_by_data = round(grouped['Charging'] * self.charge_weight + grouped['Discharging'] * self.discharge_weight, 2)
        
        # Get total completed cycles (assuming the same usage of battery in past unknown cycles)
        completed_cycles = self.get_total_completed_cycles(cycles_completed_by_data)
        
        # Remaining cycles and end date
        remaining_cyclces = round(self.total_cycles - completed_cycles)
        end_date = self.forecast_end_datetime(remaining_cyclces)
            
        return remaining_cyclces, cycles_completed_by_data, completed_cycles, end_date

    #
    # Calculate energy delivered and predict future energy
    #
    def calculate_energy_profile(self, df, remaining_cyclces, cycles_completed_by_data, completed_cycles):
        
        # Calculate Average energy
        grouped = df.groupby('cycle_type')['energy'].sum()
        grouped_2 = df.groupby('cycle_type')['remaining_energy'].sum()
        
        # Calculate energy consumed and delivered as per data per cycle
        energy_consumed = round(grouped['Charging'] / cycles_completed_by_data, 2)
        energy_delivered = round(grouped['Discharging'] / cycles_completed_by_data, 2)
        
        # Calculate total energy consumed and delivered
        total_energy_consumed = energy_consumed * completed_cycles
        total_energy_delivered = energy_delivered * completed_cycles
        
        # Future energy profile
        future_energy_consumption = energy_consumed * remaining_cyclces
        future_energy_delivery = energy_delivered * remaining_cyclces
        
        # Remaining energy profile
        remaining_energy_consumption = round(grouped_2['Charging'] / cycles_completed_by_data, 2)
        remaining_energy_delivery = round(grouped_2['Discharging']/ cycles_completed_by_data, 2)
        
        # Calculate total remaining energy for consumption and delivery
        total_remaining_energy_consumption = remaining_energy_consumption * completed_cycles
        total_remaining_energy_delivery = remaining_energy_delivery * completed_cycles
        
        # Future Reaming energy
        future_remaining_energy_consumption = remaining_energy_consumption * remaining_cyclces
        future_remaining_energy_delivery = remaining_energy_delivery * remaining_cyclces
        
        return energy_consumed, energy_delivered, total_energy_consumed, total_energy_delivered, future_energy_consumption, future_energy_delivery, remaining_energy_consumption, remaining_energy_delivery, total_remaining_energy_consumption, total_remaining_energy_delivery, future_remaining_energy_consumption, future_remaining_energy_delivery
    
    #
    # Get latest discharge soh
    #
    def get_latest_soh(self, df):
        latest_soh = df[df['cycle_type'] == 'Discharging'].tail(1)['soh'].values[0]
        # print('Latest SOH: {}'.format(latest_soh))
        return latest_soh
        
if __name__ == '__main__':
    
    # global tower_id, start_time, req_keys
    
    final_output_list = []
    
    # Reading Input
    input_df = pd.read_excel('input.xlsx')
    
    for i, row in input_df.iterrows():
        tower_id = row['Tower Id']
        tower_name = row['Tower Name']
        battery_id = row['Battery Id']
        current_key = row['Current Key']	
        voltage_key = row['Voltage Key']	
        install_datetime = row['Installation DateTime'].strftime(time_format)
        start_time = row['Data Start DateTime'].strftime(time_format)
        total_cycles = row['Total Cycles']
        cycle_limit = row['Cycle Limit']
        charge_weight = row['Charge Weight']	
        rated_voltage = row['Rated Voltage']
        c = row['C']
        output_url = row['Output URL']
        suffix = row['Suffix']
        
        req_keys = [
            'time',
            voltage_key,
            current_key
        ]
        
        print('Calculating for {} - {}'.format(tower_name, battery_id))
        
        cc = CycleCalculator(install_datetime=install_datetime, total_cycles=total_cycles, cycle_limit=cycle_limit,
                             charge_weight=charge_weight, rated_voltage=rated_voltage, C=c)

        df = cc.get_aggregated_data()

        print()
        print('-----------------------------------------------------------')
        print('Input Parameters:')
        print('-----------------------------------------------------------')

        print('install_datetime = {} \ntotal_cycles = {} \ncycle_limit = {} \ncharge_weight = {} \ndischarge_weight = {}'.format(cc.install_datetime, cc.total_cycles, cc.cycle_limit, cc.charge_weight, cc.discharge_weight))

        # Output Calc 
        rc, data_cc, total_cc, end_date = cc.calcuate_remaining_cycles(df)
        ec, ed, tec, ted, fec, fed, rec, red, trec, tred, frec, fred = cc.calculate_energy_profile(df, rc, data_cc, total_cc)
        latest_soh = cc.get_latest_soh(df)

        print()
        print('-----------------------------------------------------------')
        print('Output:')
        print('-----------------------------------------------------------')
        print('Cycles Completed as per data: {}'.format(data_cc))
        print('Days per cycle: {}'.format(round(cc.days_per_cycle, 2)))
        print('Total Completed cycles (estimated hindsight + as per data): {}'.format(total_cc))
        print('Predicted Remaining Cycles: {}'.format(rc))
        print('Forecast End Date: {}'.format(end_date))
        print()
        print('Energy Consumption per Cycle: {}'.format(ec))
        print('Energy Delivered per Cycle: {}'.format(ed))
        print('Total Energy Consumed till date: {}'.format(tec))
        print('Total Energy Delivered till date: {}'.format(ted))
        print('Future Energy Consuption Forecasted: {}'.format(fec))
        print('Future Energy Delivery Forecasted: {}'.format(fed))

        print()
        print('----- Remaining Energy Calculation as per new method ------')
        print('Remaining Energy at Consumption as per given Cycle: {}'.format(rec))
        print('Remaining Energy at Delivery as per Cycle: {}'.format(red))
        print('Total Remaining Energy at Consumption till date: {}'.format(trec))
        print('Total Remaining Energy at Delivery till date: {}'.format(tred))
        print('Future Remaining Energy at Consuption Forecasted: {}'.format(frec))
        print('Future Remaining Energy at Delivery Forecasted: {}'.format(fred))

        print()
        print('Latest SOH: {}'.format(latest_soh))
        print()
        
        df_params = {
            'tower_name': tower_name,
            'tower_id': tower_id, 
            'battery_id': battery_id,
            'install_datetime': install_datetime,
            'total_cycles': cc.total_cycles,
            'cycle_limit': cc.cycle_limit,
            'charge_weight': cc.charge_weight,
            'discharge_weight': cc.discharge_weight,

            'completed_cycles_as_per_data': data_cc,
            'days_per_cycle': cc.days_per_cycle,
            'total_completed_cycles': total_cc,
            'remaining_cycles': rc,
            'end_date': str(end_date),
            
            'energy_consumption_per_cycle': ec,
            'energy_delivered_per_cycle': ed,
            'total_energy_consumed_till_date': tec,
            'total_energy_delivered_till_date': ted,
            'future_energy_consuption_forecasted': fec,
            'future_energy_delivery_forecasted': fed,

            'remaining_energy_at_consumption_as_per_given_cycle': rec,
            'remaining_energy_at_delivery_as_per_cycle': red,
            'total_remaining_energy_at_consumption_till_date': trec,
            'total_remaining_energy_at_delivery_till_date': tred,
            'future_remaining_energy_at_consuption_forecasted': frec,
            'future_remaining_energy_at_delivery_forecasted': fred,
            
            'latest_soh': latest_soh
        }
        
        final_output_list.append(df_params)
        
        # Calling Output URL
        skip_params = [
            'tower_name',
            'tower_id', 
            'battery_id',
            'install_datetime',
            'total_cycles',
            'cycle_limit',
            'charge_weight',
            'discharge_weight'
        ]
        
        output_params = dict()
        for key, value in df_params.items():
            if key not in skip_params:
                output_params.update({key + suffix: value})
        
        print('Posting to URL: {}'.format(output_url))
        print('Output Params to the URL: {}'.format(output_params))
        print()
        post_response = requests.post(output_url, headers=cc.get_auth_header(), data=json.dumps(output_params))
        print('POST Response: {}'.format(post_response))
        print()
        print('--------------------------------------------------------------------------')
        print()
        
    print('--- Completed Processing. Writing to final_output.xlsx --')
    xw = pd.ExcelWriter('output/final_output.xlsx')
    pd.DataFrame(final_output_list).to_excel(xw, index=False)
    xw.close()
