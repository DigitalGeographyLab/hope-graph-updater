import time
import traceback
from datetime import datetime
from aqi_fetcher import AqiFetcher
from aqi_updater import AqiUpdater
from load_env_vars import load_env_vars
from common.logger import Logger
import common.igraph as ig_utils
from common.igraph import Edge as E

log = Logger(printing=True, log_file='aqi_updater_app.log')
load_env_vars(log)
graph = ig_utils.read_graphml('graph/kumpula.graphml')

aqi_fetcher = AqiFetcher(log)
aqi_updater = AqiUpdater(log, graph)

def fetch_process_aqi_data():
    try:
        aqi_fetcher.fetch_process_current_aqi_data()
        log.info('AQI fetch & processing succeeded')
    except Exception:
        traceback.print_exc()
        log.error(f'failed to process AQI data to {aqi_fetcher.wip_aqi_tif}, retrying in 30s')
        time.sleep(30)
    finally:
        aqi_fetcher.finish_aqi_fetch()

def create_aqi_update_csv():
    try:
        aqi_updater.create_aqi_update_csv(aqi_fetcher.latest_aqi_tif)
        log.info('AQI update succeeded')
    except Exception:
        traceback.print_exc()
        log.error(f'failed to update AQI from {aqi_fetcher.latest_aqi_tif}, retrying in 30s')
        time.sleep(30)
    finally:
        aqi_updater.finish_aqi_update()


if (__name__ == '__main__'):
    log.info('starting AQI updater app')

    while True:
        if (aqi_fetcher.new_aqi_available()):
            fetch_process_aqi_data()
        if (aqi_updater.new_update_available(aqi_fetcher.latest_aqi_tif)):
            create_aqi_update_csv()
        time.sleep(10)
