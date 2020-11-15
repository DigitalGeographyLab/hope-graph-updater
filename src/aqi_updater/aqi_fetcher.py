import sys
sys.path.append('..')
import os
import zipfile
import rioxarray
import xarray
import boto3
import numpy as np
import pandas as pd
import datetime
import rasterio
from rasterio import fill
from datetime import datetime
from typing import List, Set, Dict, Tuple, Optional
from common.logger import Logger


class AqiFetcher:
    """AqiFetcher can download, extract and adjust air quality index (AQI) data from FMI's Enfuser model. 
    
    Notes:
        The required python environment for using the class can be installed with: conda env create -f conda-env.yml.
        
        Essentially, AQI download workflow is composed of the following steps (executed by fetch_process_current_aqi_data()):
            1)	Create a key for fetching Enfuser data based on current UTC time (e.g. “allPollutants_2019-11-08T11.zip”).
            2)  Fetch a zip archive that contains Enfuser netCDF data from Amazon S3 bucket using the key, 
                aws_access_key_id and aws_secret_access_key. 
            3)  Extract Enfuser netCDF data (e.g. allPollutants_2019-09-11T15.nc) from the downloaded zip archive.
            4)  Extract AQI layer from the allPollutants*.nc file and export it as GeoTiff (WGS84).
            5)  Open the exported raster and fill nodata values with interpolated values. 
                Value 1 is considered nodata in the data. This is an optional step. 

    Attributes:
        log: An instance of Logger class for writing log messages.
        wip_aqi_tif: The name of an aqi tif file that is currently being produced (wip = work in progress).
        latest_aqi_tif: The name of the latest AQI tif file that was processed.
        __aqi_dir: A filepath pointing to a directory where all AQI files will be downloaded to and processed.
        __s3_bucketname: The name of the AWS s3 bucket from where the enfuser data will be fetched from.
        __s3_region: The name of the AWS s3 bucket from where the enfuser data will be fetched from.
        __AWS_ACCESS_KEY_ID: A secret AWS access key id to enfuser s3 bucket.
        __AWS_SECRET_ACCESS_KEY: A secret AWS access key to enfuser s3 bucket.
        __temp_files_to_rm (list): A list where names of created temporary files will be collected during processing.
        __status: The status of the aqi processor - has latest AQI data been processed or not.

    """

    def __init__(self, logger: Logger, aqi_dir: str = 'aqi_cache/'):
        self.log = logger
        self.wip_aqi_tif: str = ''
        self.latest_aqi_tif: str = ''
        self.__aqi_dir = aqi_dir
        self.__s3_bucketname: str = 'enfusernow2'
        self.__s3_region: str = 'eu-central-1'
        self.__AWS_ACCESS_KEY_ID: str = os.getenv('ENFUSER_S3_ACCESS_KEY_ID', None) 
        self.__AWS_SECRET_ACCESS_KEY: str = os.getenv('ENFUSER_S3_SECRET_ACCESS_KEY', None) 
        self.__temp_files_to_rm: list = []
        self.__status: str = ''

    def new_aqi_available(self) -> bool:
        """Returns False if the expected latest aqi file is either already processed or being processed at the moment, 
        else returns True.
        """
        b_available = True
        status = ''
        current_aqi_tif = self.__get_current_aqi_tif_name()
        if (self.latest_aqi_tif == current_aqi_tif):
            status = 'latest AQI data already fetched'
            b_available = False
        else:
            status = 'new AQI data available: '+ current_aqi_tif
            b_available = True

        if (self.__status != status):
            self.log.info(f'AQI processor status changed to: {status}')
            self.__status = status
        return b_available

    def fetch_process_current_aqi_data(self) -> None:
        self.__set_wip_aqi_tif_name(self.__get_current_aqi_tif_name())
        enfuser_data_key, aqi_zip_name = self.__get_current_enfuser_key_filename()
        self.log.info('Created key for current AQI: '+ enfuser_data_key)
        self.log.info('Fetching enfuser data...')
        aqi_zip_name = self.__fetch_enfuser_data(enfuser_data_key, aqi_zip_name)
        self.log.info('Got aqi_zip: '+ aqi_zip_name)
        aqi_nc_name = self.__extract_zipped_aqi(aqi_zip_name)
        self.log.info('Extracted aqi_nc: '+ aqi_nc_name)
        aqi_tif_name = self.__convert_aqi_nc_to_raster(aqi_nc_name)
        self.log.info('Extracted aqi_tif: '+ aqi_tif_name)
        aqi_tif_name = self.__fillna_in_raster(aqi_tif_name, na_val=1.0) 
        self.latest_aqi_tif = aqi_tif_name

    def finish_aqi_fetch(self) -> None:
        self.__remove_temp_files()
        self.__remove_old_aqi_files()
        self.__reset_wip_aqi_tif_name()

    def __get_current_aqi_tif_name(self) -> str:
        """Returns the name of the current expected edge aqi tif file. Note: it might not exist yet.
        """
        curdt = datetime.utcnow().strftime('%Y-%m-%dT%H')
        return 'aqi_'+ curdt +'.tif'

    def __set_wip_aqi_tif_name(self, name: str) -> None:
        """Sets the excpected latest aqi tif filename to attribute wip_aqi_tif.
        """
        self.wip_aqi_tif = name

    def __reset_wip_aqi_tif_name(self) -> None:
        self.wip_aqi_tif = ''

    def __get_current_enfuser_key_filename(self) -> Tuple[str, str]:
        """Returns a key pointing to the expected current enfuser zip file in AWS S3 bucket. 
        Also returns a name for the zip file for exporting the file. The names of the key and the zip file contain
        the current UTC time (e.g. 2019-11-08T11).
        """
        curdt = datetime.utcnow().strftime('%Y-%m-%dT%H')
        enfuser_data_key = 'Finland/pks/allPollutants_' + curdt + '.zip'
        aqi_zip_name = 'allPollutants_' + curdt + '.zip'
        return (enfuser_data_key, aqi_zip_name)

    def __fetch_enfuser_data(self, enfuser_data_key: str, aqi_zip_name: str) -> str:
        """Downloads the current enfuser data as a zip file containing multiple netcdf files to the aqi_cache directory. 
        
        Returns:
            The name of the downloaded zip file (e.g. allPollutants_2019-11-08T14.zip).
        """
        # connect to S3
        s3 = boto3.client('s3',
                        region_name=self.__s3_region,
                        aws_access_key_id=self.__AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=self.__AWS_SECRET_ACCESS_KEY)
                
        # download the netcdf file to a specified location
        file_out = self.__aqi_dir + '/' + aqi_zip_name
        s3.download_file(self.__s3_bucketname, enfuser_data_key, file_out)
        self.__temp_files_to_rm.append(aqi_zip_name)
        return aqi_zip_name

    def __extract_zipped_aqi(self, aqi_zip_name: str) -> str:
        """Extracts the contents of a zip file containing enfuser self files. 

        Args:
            aqi_zip_name: The name of the zip file to be extracted from (in aqi_cache directory).
        Returns:
            The name of the extracted AQI nc file.
        """
        # read zip file in
        archive = zipfile.ZipFile(self.__aqi_dir + aqi_zip_name, 'r')
        
        # loop over files in zip archive
        for file_name in archive.namelist():
            # extract only files with allPollutants string match
            if ('allPollutants' in file_name):
                # extract selected file to aqi_dir directory
                archive.extract(file_name, self.__aqi_dir)
                aqi_nc_name = file_name
        
        self.__temp_files_to_rm.append(aqi_nc_name)
        return aqi_nc_name

    def __convert_aqi_nc_to_raster(self, aqi_nc_name: str) -> str:
        """Converts a netCDF file to a georeferenced raster file. xarray and rioxarray automatically scale and offset 
        each netCDF file opened with proper values from the file itself. No manual scaling or adding offset required.
        CRS of the exported GeoTiff is set to WGS84.

        Args:
            aqi_nc_name: The filename of an nc file to be processed (in aqi_cache directory).
                e.g. allPollutants_2019-09-11T15.nc
        Returns:
            The name of the exported tif file (e.g. aqi_2019-11-08T14.tif).
        """
        # read .nc file containing the AQI layer as a multidimensional array
        data = xarray.open_dataset(self.__aqi_dir + aqi_nc_name)
                
        # retrieve AQI, AQI.data has shape (time, lat, lon)
        # the values are automatically scaled and offset AQI values
        aqi = data['AQI']

        # save AQI to raster (.tif geotiff file recommended)
        aqi = aqi.rio.set_crs('epsg:4326')
        
        # parse date & time from nc filename and export raster
        aqi_date_str = aqi_nc_name[:-3][-13:]
        aqi_tif_name = 'aqi_'+ aqi_date_str +'.tif'
        aqi.rio.to_raster(self.__aqi_dir + aqi_tif_name)
        self.latest_aqi_tif = aqi_tif_name
        return aqi_tif_name

    def __fillna_in_raster(self, aqi_tif_name: str, na_val: float = 1.0) -> str:
        """Fills nodata values in a raster by interpolating values from surrounding cells.
        Value 1.0 is considered as nodata. If no nodata is found with that value, a small offset will be applied,
        as sometimes the nodata value is slightly higher than 1.0 (assumably due to inaccuracy in netcdf to 
        geotiff conversion).
        
        Args:
            aqi_tif_name: The name of a raster file to be processed (in aqi_cache directory).
            na_val: A value that represents nodata in the raster.
        """
        # open AQI band from AQI raster file
        aqi_filepath = self.__aqi_dir + aqi_tif_name
        aqi_raster = rasterio.open(aqi_filepath)
        aqi_band = aqi_raster.read(1)

        # create a nodata mask (map nodata values to 0)
        # nodata value may be slightly higher than 1.0, hence try different offsets
        na_offset = 0
        for offset in [0.0, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.12]:
            na_offset = na_val + offset
            nodata_count = np.sum(aqi_band <= na_offset)
            self.log.info('Nodata offset: '+ str(offset) + ' nodata count: '+ str(nodata_count))
            # check if nodata values can be mapped with the current offset
            if (nodata_count > 180000):
                break
        if (nodata_count < 180000):
            self.log.info('Failed to set nodata values in the aqi tif, nodata count: ', str(nodata_count))

        aqi_nodata_mask = np.where(aqi_band <= na_offset, 0, aqi_band)
        # fill nodata in aqi_band using nodata mask
        aqi_band_fillna = fill.fillnodata(aqi_band, mask=aqi_nodata_mask)

        # validate AQI values after na fill
        invalid_count = np.sum(aqi_band_fillna < 1.0)
        if (invalid_count > 0):
            self.log.warning('AQI band has '+ str(invalid_count) +' below 1 aqi values after na fill')

        # write raster with filled nodata
        aqi_raster_fillna = rasterio.open(
            aqi_filepath,
            'w',
            driver='GTiff',
            height=aqi_raster.shape[0],
            width=aqi_raster.shape[1],
            count=1,
            dtype='float32',
            transform=aqi_raster.transform,
            crs=aqi_raster.crs
        )

        aqi_raster_fillna.write(aqi_band_fillna, 1)
        aqi_raster_fillna.close()
        
        return aqi_tif_name

    def __remove_temp_files(self) -> None:
        """Removes temporary files created during AQI processing to aqi_cache, i.e. files in attribute self.__temp_files_to_rm.
        """
        rm_count = 0
        not_removed = []
        for rm_filename in self.__temp_files_to_rm:
            try:
                os.remove(self.__aqi_dir + rm_filename)
                rm_count += 1
            except Exception:
                not_removed.append(rm_filename)
                pass
        self.log.info('Removed '+ str(rm_count) +' temp files')
        if (len(not_removed) > 0):
            self.log.warning('Could not remove '+ str(len(not_removed)) + ' files')
        self.__temp_files_to_rm = not_removed

    def __remove_old_aqi_files(self) -> None:
        """Removes old aqi tif files from aqi_cache.
        """
        rm_count = 0
        error_count = 0
        for file_n in os.listdir(self.__aqi_dir):
            if (file_n.endswith('.tif') and file_n != self.latest_aqi_tif):
                try:
                    os.remove(self.__aqi_dir + file_n)
                    rm_count += 1
                except Exception:
                    error_count += 1
                    pass
        self.log.info('Removed '+ str(rm_count) +' old edge aqi tif files')
        if (error_count > 0):
            self.log.warning('Could not remove '+ error_count +' old aqi tif files')
