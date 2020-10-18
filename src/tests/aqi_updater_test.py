import pytest
from ..aqi_updater.aqi_updater import AqiUpdater
from ..common.logger import Logger
from ..common.igraph import Edge as E
import common.igraph as ig_utils
import pandas as pd
import json


log = Logger(printing=False)
graph = ig_utils.read_graphml('test_data/kumpula.graphml')
aqi_updater = AqiUpdater(log, graph, aqi_cache = 'test_data/', aqi_updates = 'test_aqi_updates/')
aqi_updater.create_aqi_update_csv('aqi_2020-10-10T08.tif')
aqi_updater.finish_aqi_update()


def read_exported_aqi_update_csv(filepath: str):
    return pd.read_csv(filepath)


def test_create_aqi_update_csv():
    assert aqi_updater.wip_aqi_csv == '' 
    assert aqi_updater.latest_aqi_csv == 'aqi_2020-10-10T08.csv'
    aqi_update_df = pd.read_csv('test_aqi_updates/aqi_2020-10-10T08.csv')
    assert len(aqi_update_df) == 16469


def test_aqi_update_csv_data_ok():
    aqi_update_df = pd.read_csv('test_aqi_updates/aqi_2020-10-10T08.csv')
    assert aqi_update_df[E.id_ig.name].nunique() == 16469
    assert round(aqi_update_df[E.aqi.name].mean(), 3) == 1.684
    assert aqi_update_df[E.aqi.name].median() == 1.67
    assert aqi_update_df[E.aqi.name].min() == 1.63
    assert aqi_update_df[E.aqi.name].max() == 2.04
    assert aqi_update_df[E.id_ig.name].nunique() == 16469
    not_null_aqis = aqi_update_df[aqi_update_df[E.aqi.name].notnull()]
    assert len(not_null_aqis) == 16469


def test_aqi_map_json():
    with open('test_aqi_updates/aqi_map.json') as f:
        aqi_map = json.load(f)
        assert len(aqi_map) == 1
        assert len(aqi_map['data']) == 8162
        for id_aqi_pair in aqi_map['data']:
            assert isinstance(id_aqi_pair[0], int) 
            assert isinstance(id_aqi_pair[1], int)
