"""
This script is designed for dynamic filter function

@author: Xiaotong Guo
"""

import pandas as pd
import numpy as np
import functools
import warnings
import pickle
warnings.filterwarnings("ignore")

# data_path = r'/Users/xiaotong/Documents/GitHub/rove/data/CTA_aggre_metrics_10min_oct2020.p'
# data_dict = pickle.load(open(data_path, 'rb'))
# start_time = (7, 30)
# end_time = (15, 30)
sample_time = ((7, 0), (7, 10))

def dynamic_filter_process(metric_dict, start_time, end_time):
    """
    Function for dynamic filter
    :param metric_dict: aggregated metrics in 10 minute buckets
    :param start_time: start time in (hour, minute) format
    :param end_time: end time in (hour, minute) format
    :return: json format of aggre metrics
    """

    freq_threshold = 5
    interval_list = [i for i in list(metric_dict.keys()) if i[0] >= start_time and i[1] <= end_time]
    aggre_metrics = {}

    high_freq_list = []
    low_freq_list = []

    # Timepoints-segment-level
    for metric_type in ['median', '90']:
        metric_str_list = metric_dict[sample_time][metric_type][3].columns.tolist()
        metric_str_list = list(set(metric_str_list) - set(['route', 'segment']))

        df_list = []
        for ind, time_interval in enumerate(interval_list):
            df = metric_dict[time_interval][metric_type][3].copy()
            keep_same = {'route', 'segment'}
            df.columns = ['{}{}'.format(c, '' if c in keep_same else '_' + str(ind)) for c in df.columns]
            df_list.append(df)
        df_combine = functools.reduce(lambda left, right: pd.merge(left, right,
                                                                   on=['route', 'segment'], how='outer'), df_list)

        df = df_combine[['route', 'segment']]
        for metric_str in metric_str_list:
            col_list = [col for col in df_combine.columns if metric_str in col]
            metric = df_combine[col_list].mean(axis=1)
            df[metric_str] = metric

        index = []
        iter_list = df[['route', 'segment']].values.tolist()
        for i in iter_list:
            str_ = str(i[0]) + "-" + str(i[1][0]) + "-" + str(i[1][1])
            index.append(str_)
        df['index'] = index

        return_json = df.to_json(orient='records')

        if metric_type == 'median':
            aggre_metrics['segment-timepoints-median'] = return_json
        else:
            aggre_metrics['segment-timepoints-90'] = return_json

    # Timepoints-corridor-level
    for metric_type in ['median', '90']:
        metric_str_list = metric_dict[sample_time][metric_type][4].columns.tolist()
        metric_str_list = list(set(metric_str_list) - set(['corridor']))

        df_list = []
        for ind, time_interval in enumerate(interval_list):
            df = metric_dict[time_interval][metric_type][4].copy()
            keep_same = {'corridor'}
            df.columns = ['{}{}'.format(c, '' if c in keep_same else '_' + str(ind)) for c in df.columns]
            df_list.append(df)
        df_combine = functools.reduce(lambda left, right: pd.merge(left, right, on=['corridor'], how='outer'), df_list)

        df = df_combine[['corridor']]
        for metric_str in metric_str_list:
            col_list = [col for col in df_combine.columns if metric_str in col]
            metric = df_combine[col_list].mean(axis=1)
            df[metric_str] = metric

        index = []
        iter_list = df['corridor'].values.tolist()
        for i in iter_list:
            str_ = str(i[0]) + "-" + str(i[1])
            index.append(str_)
        df['index'] = index

        return_json = df.to_json(orient='records')

        if metric_type == 'median':
            aggre_metrics['corridor-timepoints-median'] = return_json
        else:
            aggre_metrics['corridor-timepoints-90'] = return_json


    # Route-level
    for metric_type in ['median', '90']:
        metric_str_list = metric_dict[sample_time][metric_type][2].columns.tolist()
        metric_str_list = list(set(metric_str_list) - set(['route', 'direction']))

        df_list = []
        for ind, time_interval in enumerate(interval_list):
            df = metric_dict[time_interval][metric_type][2].copy()
            keep_same = {'route', 'direction'}
            df.columns = ['{}{}'.format(c, '' if c in keep_same else '_' + str(ind)) for c in df.columns]
            df_list.append(df)
        df_combine = functools.reduce(lambda left, right: pd.merge(left, right, on=['route', 'direction'],
                                                                   how='outer'), df_list)


        df = df_combine[['route', 'direction']]
        for metric_str in metric_str_list:
            if metric_str in ['scheduled_arrival_count', 'observed_arrival_count']:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].sum(axis=1)
                total_hour = end_time[0] - start_time[0] + (end_time[1] - start_time[1]) / 60
                if metric_str == 'scheduled_arrival_count':
                    df['scheduled_frequency'] = metric / total_hour
                else:
                    df['observed_frequency'] = metric / total_hour
            elif metric_str in ['sample_size', 'revenue_hour']:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].sum(axis=1)
                df[metric_str] = metric
            else:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].mean(axis=1)
                df[metric_str] = metric

        if metric_type == 'median':
            route_freq = df[['route', 'direction', 'scheduled_frequency']].groupby(['route'], as_index=False)[
                'scheduled_frequency'].max()
            high_freq_list = route_freq[route_freq['scheduled_frequency'] >= freq_threshold]['route'].values.tolist()
            low_freq_list = route_freq[route_freq['scheduled_frequency'] < freq_threshold]['route'].values.tolist()

        df.loc[df['route'].isin(low_freq_list), ['scheduled_wait_time', 'actual_wait_time',
                                                 'excess_wait_time']] = np.nan

        index = []
        iter_list = df[['route', 'direction']].values.tolist()
        for i in iter_list:
            str_ = str(i[0]) + "-" + str(i[1])
            index.append(str_)
        df['index'] = index

        return_json = df.to_json(orient='records')

        if metric_type == 'median':
            aggre_metrics['route-median'] = return_json
        else:
            aggre_metrics['route-90'] = return_json

    # Corridor-level
    for metric_type in ['median', '90']:
        metric_str_list = metric_dict[sample_time][metric_type][1].columns.tolist()
        metric_str_list = list(set(metric_str_list) - set(['corridor']))

        df_list = []
        for ind, time_interval in enumerate(interval_list):
            df = metric_dict[time_interval][metric_type][1].copy()
            keep_same = {'corridor'}
            df.columns = ['{}{}'.format(c, '' if c in keep_same else '_' + str(ind)) for c in df.columns]
            df_list.append(df)
        df_combine = functools.reduce(lambda left, right: pd.merge(left, right, on=['corridor'], how='outer'), df_list)

        df = df_combine[['corridor']]
        for metric_str in metric_str_list:
            if metric_str in ['scheduled_arrival_count', 'observed_arrival_count']:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].sum(axis=1)
                total_hour = end_time[0] - start_time[0] + (end_time[1] - start_time[1]) / 60
                if metric_str == 'scheduled_arrival_count':
                    df['scheduled_frequency'] = metric / total_hour
                else:
                    df['observed_frequency'] = metric / total_hour
            elif metric_str == 'sample_size':
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].sum(axis=1)
                df[metric_str] = metric
            else:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].mean(axis=1)
                df[metric_str] = metric

        index = []
        iter_list = df['corridor'].values.tolist()
        for i in iter_list:
            str_ = str(i[0]) + "-" + str(i[1])
            index.append(str_)
        df['index'] = index

        return_json = df.to_json(orient='records')

        if metric_type == 'median':
            aggre_metrics['corridor-median'] = return_json
        else:
            aggre_metrics['corridor-90'] = return_json

    # Segment-level
    for metric_type in ['median', '90']:
        metric_str_list = metric_dict[sample_time][metric_type][0].columns.tolist()
        metric_str_list = list(set(metric_str_list) - set(['route', 'segment']))

        df_list = []
        for ind, time_interval in enumerate(interval_list):
            df = metric_dict[time_interval]['median'][0].copy()
            keep_same = {'route', 'segment'}
            df.columns = ['{}{}'.format(c, '' if c in keep_same else '_' + str(ind)) for c in df.columns]
            df_list.append(df)
        df_combine = functools.reduce(lambda left, right: pd.merge(left, right,
                                                                   on=['route', 'segment'], how='outer'), df_list)

        df = df_combine[['route', 'segment']]
        for metric_str in metric_str_list:
            if metric_str in ['scheduled_arrival_count', 'observed_arrival_count']:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].sum(axis=1)
                total_hour = end_time[0] - start_time[0] + (end_time[1] - start_time[1]) / 60
                if metric_str == 'scheduled_arrival_count':
                    df['scheduled_frequency'] = metric / total_hour
                else:
                    df['observed_frequency'] = metric / total_hour
            elif metric_str == 'sample_size':
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].sum(axis=1)
                df[metric_str] = metric
            elif metric_str == 'on_time_performance':
                col_list = [col for col in df_combine.columns if metric_str in col]
                df_combine[col_list].quantile(q=0.5, axis=1)
                df[metric_str] = metric
            else:
                col_list = [col for col in df_combine.columns if metric_str in col]
                metric = df_combine[col_list].mean(axis=1)
                df[metric_str] = metric

        df.loc[df['route'].isin(low_freq_list), ['scheduled_wait_time', 'actual_wait_time', 'excess_wait_time']] = np.nan

        index = []
        iter_list = df[['route', 'segment']].values.tolist()
        for i in iter_list:
            str_ = str(i[0]) + "-" + str(i[1][0]) + "-" + str(i[1][1])
            index.append(str_)
        df['index'] = index

        return_json = df.to_json(orient='records')

        if metric_type == 'median':
            aggre_metrics['segment-median'] = return_json
        else:
            aggre_metrics['segment-90'] = return_json

    return aggre_metrics


# output = dynamic_filter_process(data_dict, start_time, end_time)
