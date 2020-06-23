from typing import List, Set, Dict, Tuple, Optional
import sys
sys.path.append('..')
import os
import numpy as np
import rasterio
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import common.igraph as ig_utils
from common.igraph import Edge as E
from common.logger import Logger

class AqiUpdater():

    def __init__(self, log: Logger, graph, aqi_cache: str='aqi_cache/', aqi_updates: str='aqi_updates/'):
        self.log = log
        self.wip_aqi_csv: str = ''
        self.latest_aqi_csv: str = ''
        self.__edge_gdf = self.__get_sampling_point_gdf_from_graph(graph)
        self.__sampling_gdf = self.__edge_gdf.drop_duplicates('lat_lon_id')
        self.__aqi_cache = aqi_cache
        self.__aqi_updates = aqi_updates
        self.__status = ''

    def new_update_available(self, latest_aqi_tif_name: str) -> bool:
        """Returns False if the expected latest aqi file is either already processed or being processed at the moment, 
        else returns True.
        """
        b_available = True
        status = ''
        if (self.latest_aqi_csv == self.__get_aqi_csv_name(latest_aqi_tif_name)):
            status = 'latest AQI update already done'
            b_available = False
        else:
            status = 'new AQI update available: '+ latest_aqi_tif_name
            b_available = True

        if (self.__status != status):
            self.log.info(f'AQI updater status changed to: {status}')
            self.__status = status
        
        return b_available

    def create_aqi_update_csv(self, aqi_tif_name: str) -> None:
        self.wip_aqi_csv = self.__get_aqi_csv_name(aqi_tif_name)
        aqi_tif_file = self.__aqi_cache + aqi_tif_name
        edge_aqi_df = self.__sample_aqi_to_point_gdf(aqi_tif_file)
        # export sampled aqi values to csv
        final_edge_aqi_samples = self.__combine_final_sample_df(edge_aqi_df)
        final_edge_aqi_samples.to_csv(self.__aqi_updates + self.wip_aqi_csv, index=False)
        self.log.info('exported edge_aqi_csv '+ self.wip_aqi_csv)
        self.latest_aqi_csv = self.wip_aqi_csv

    def finish_aqi_update(self) -> None:
        self.wip_aqi_csv = ''
        self.__remove_old_update_files()

    def __get_aqi_csv_name(self, aqi_tif_name: str) -> str:
        return aqi_tif_name.replace('.tif', '.csv')

    def __get_sampling_point_gdf_from_graph(self, graph) -> gpd.GeoDataFrame:
        """Filters out null geometries from edge_gdf and adds columns point_geom & lat_lon_id.
        """
        edge_gdf = ig_utils.get_edge_gdf(graph, attrs=[E.id_ig], geom_attr=E.geom_wgs)
        # filter out edges with null geometry
        edge_gdf = edge_gdf[edge_gdf[E.geom_wgs.name].apply(lambda x: isinstance(x, LineString))]
        edge_gdf['point_geom'] = [geom.interpolate(0.5, normalized=True) for geom in edge_gdf[E.geom_wgs.name]]
        edge_gdf['lat_lon_id'] = [f'{str(round(geom.x, 7))}_{str(round(geom.y, 7))}' for geom in edge_gdf['point_geom']]
        return edge_gdf

    def __sample_aqi_to_point_gdf(self, aqi_tif_file: str) -> str:
        """Joins AQI values from an AQI raster file to edges (edge_gdf) of a graph by spatial sampling. 
        Column 'aqi' will be added to the G.edge_gdf. Center points of the edges are used in the spatial join. 
        Exports a csv file of ege keys and corresponding AQI values to use for updating AQI values to a graph.

        Args:
            G: A GraphHandler object that has edge_gdf and graph as properties.
            aqi_tif_name: The filename of an AQI raster (GeoTiff) file (in aqi_cache directory).
        Todo:
            Implement more precise join for longer edges. 
        Returns:
            The name of the exported csv file (e.g. aqi_2019-11-08T14.csv).
        """
        gdf = self.__sampling_gdf.copy()
        aqi_raster = rasterio.open(aqi_tif_file)
        # get coordinates of edge centers as list of tuples
        coords = [ (x, y) for x, y in zip([point.x for point in gdf['point_geom']], [point.y for point in gdf['point_geom']]) ]
        coords = self.__round_coordinates(coords)
        # extract aqi values at coordinates from raster using sample method from rasterio
        gdf['aqi'] = [round(x.item(), 2) for x in aqi_raster.sample(coords)]

        # validate sampled aqi values
        if (self.__validate_df_aqi(gdf, debug_to_file=False) == False):
            self.log.error('aqi sampling failed')

        return gdf

    def __get_valid_aqi_or_nan(self, aqi: float):
        if (np.isfinite(aqi)):
            if (aqi < 0.95):
                return np.nan
            elif (aqi < 1):
                return 1.0
            else:
                return aqi
        else:
            return np.nan

    def __combine_final_sample_df(self, sampling_gdf) -> gpd.GeoDataFrame:
        edge_gdf_copy = self.__edge_gdf[[E.id_ig.name, 'lat_lon_id']].copy()
        final_sample_df = pd.merge(edge_gdf_copy, sampling_gdf[['lat_lon_id', 'aqi']], on='lat_lon_id', how='left')
        final_sample_df['aqi'] = [self.__get_valid_aqi_or_nan(aqi) for aqi in final_sample_df['aqi']]
        sample_count_all = len(final_sample_df)
        final_sample_df = final_sample_df[final_sample_df['aqi'].notnull()]
        self.log.info(f'found valid AQI samples for {round(100 * len(final_sample_df)/sample_count_all, 2)} % edges')
        return final_sample_df[[E.id_ig.name, 'aqi']]

    def __round_coordinates(self, coords_list: List[tuple], digits=6) -> List[tuple]:
        return [ (round(coords[0], digits), round(coords[1], digits)) for coords in coords_list]

    def __validate_df_aqi(self, edge_gdf: 'pandas DataFrame', debug_to_file: bool=False) -> bool:
        """Validates a dataframe containing AQI values. Checks the validity of the AQI values with several tests.
        Returns True if all AQI values are valid, else returns False. Missing AQI values (AQI=0.0) are ignored (considered valid).
        """
        def validate_aqi_exp(aqi):
            if (not isinstance(aqi, float)):
                return 4
            elif (aqi < 0):
                return 3
            elif (aqi == 0.0): # aqi is just missing
                return 1
            elif (aqi < 1):
                return 2
            else:
                return 0

        edge_gdf_copy = edge_gdf.copy()
        edge_gdf_copy['aqi_validity'] = [validate_aqi_exp(aqi) for aqi in edge_gdf_copy['aqi']]
        row_count = len(edge_gdf_copy.index)
        aqi_ok_count = len(edge_gdf_copy[edge_gdf_copy['aqi_validity'] <= 1].index)
        
        if (debug_to_file == True):
            edge_gdf_copy['geometry'] = list(edge_gdf_copy['center_wgs'])
            edge_gdf_copy.crs = {'init' :'epsg:4326'}
            edge_gdf_copy.drop(columns=['uvkey', 'center_wgs']).to_file('debug/debug.gpkg', layer='edge_centers_wgs', driver="GPKG")
        
        if (row_count == aqi_ok_count):
            return True
        else:
            error_count = row_count - aqi_ok_count
            valid_ratio = round(100 * aqi_ok_count/row_count, 2)
            self.log.warning('row count: '+ str(row_count) +' of which has valid aqi: '+
                str(aqi_ok_count)+ ' = '+ str(valid_ratio) + ' %')
            self.log.warning('invalid aqi count: '+ str(error_count))
            return False

    def __remove_old_update_files(self) -> None:
        """Removes all edge_aqi_csv files older than the latest from from __aqi_updates folder.
        """
        rm_count = 0
        error_count = 0
        for file_n in os.listdir(self.__aqi_updates):
            if (file_n.endswith('.csv') and file_n != self.latest_aqi_csv):
                try:
                    os.remove(self.__aqi_updates + file_n)
                    rm_count += 1
                except Exception:
                    error_count += 1
                    pass
        self.log.info('removed '+ str(rm_count) +' old edge aqi csv files')
        if (error_count > 0):
            self.log.warning(f'could not remove {error_count} old edge aqi csv files')
