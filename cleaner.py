import configparser
import json
import logging
import os
import subprocess
import time
import psutil

global logger
config = configparser.ConfigParser()
config.read(r'config.ini')
# Scan period
SCAN_SECOND = int(config.get("General", "SCAN_SECOND"))
# Prevent continuously spawn plotting
COOLDOWN_CYCLE = int(config.get("General", "COOLDOWN_CYCLE"))
# If you want to replace old plots when the disk is full
REPLOT_MODE = config.get("General", "REPLOT_MODE")
REPLACE_DDL = int(config.get("Distributing", "REPLACE_DDL"))
FARM_SPARE_GB = int(config.get("Distributing", "FARM_SPARE_GB"))
# Concurrent copy how many plots
MAX_COPY_THREAD = int(config.get("Distributing", "MAX_COPY_THREAD"))
# Destination of HDDs
FARMS = json.loads(config.get("Distributing", "FARMS"))


plot_in_deletion = set([])
exist_plots = {}
last_plot_cycle = COOLDOWN_CYCLE
round = 1
def main():  
    global last_plot_cycle, round
    while True:
        try:
            logger.info(f"starting loop, round number {round}.")
            if REPLOT_MODE.lower() == 'true':
                spare_farms = 0
                for farm in FARMS:
                    farm_free = psutil.disk_usage(farm)[2]
                    if farm_free > FARM_SPARE_GB * 1024 * 1024 * 1024:
                        spare_farms += 1
                if spare_farms >= MAX_COPY_THREAD:
                    # We have enough spare space
                    logger.info(f"Need {MAX_COPY_THREAD}, {spare_farms} farms available.")
                else:
                    clean_farm(MAX_COPY_THREAD)
        except Exception as e:
            logger.exception("Error")
        finally:
            logger.info(f"ended round number {round}. going to sleep for {SCAN_SECOND} seconds")
            round += 1
            time.sleep(SCAN_SECOND)

def load_plot_info(path):
    if path not in exist_plots:
        exist_plots[path] = {"cDate": os.path.getctime(path), "size": os.path.getsize(path)}

def clean_farm(need_farms: int):
    cleaned_farms = 0
    for farm in FARMS:
        # Need to remove old plots
        remove_plots = []
        remove_size = psutil.disk_usage(farm)[2]
        for plot in os.listdir(farm):
            if plot.endswith('.plot'):
                path = f"{farm}/{plot}"
                load_plot_info(path)
                if exist_plots[path]["cDate"] < REPLACE_DDL:
                    remove_plots.append(path)
                    remove_size += exist_plots[path]["size"]
                if remove_size > FARM_SPARE_GB * 1024 * 1024 * 1024:
                    for rm_plot in remove_plots:
                        if rm_plot not in plot_in_deletion:
                            logger.info(f"Removing {rm_plot} for new plot ...")
                            subprocess.Popen([f"mv {rm_plot} {rm_plot}.delete && rm {rm_plot}.delete"], shell=True)
                            plot_in_deletion.add(rm_plot)
                    cleaned_farms += 1
                    break
        if cleaned_farms >= need_farms:
            break
    if cleaned_farms < need_farms:
        logger.warning(f"Cannot clean up {need_farms} farms, all farms will full soon.")

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(' %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    main()
