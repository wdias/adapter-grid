import json
import logging
import os
import requests

logger = logging.getLogger(__name__)

OPTIMIZE_STORAGE: bool = os.getenv('OPTIMIZE_STORAGE', '1') == '1'
logger.info(f"OPTIMIZE_STORAGE: {OPTIMIZE_STORAGE}")
ADAPTER_METADATA = 'http://adapter-metadata.default.svc.cluster.local'


def get_timeseries(timeseries_id: str) -> json:
    r = requests.get(f'{ADAPTER_METADATA}/timeseries/{timeseries_id}')
    assert r.status_code == 200, f'Unable to get timeseries: {timeseries_id}'
    data = r.json()
    return data


def get_regular_grid(location_id: str) -> json:
    r = requests.get(f'{ADAPTER_METADATA}/location/regular-grid/{location_id}')
    assert r.status_code == 200, f'Unable to get location: {location_id}'
    data = r.json()
    print('location: ', data)
    data['rows'] = int(data['rows'])
    data['columns'] = int(data['columns'])
    if 'gridCorners' in data:
        """
        "gridCorners": {
            "upperLeft": {
                "x": 117.35,
                "y": 28.75
            },
            "lowerRight": {
                "x": 117.35,
                "y": 28.75
            }
        }
        """
        grid_corners = data['gridCorners']
        data['xULCorner'] = float(grid_corners['upperLeft']['x'])  # x Upper Left
        data['yULCorner'] = float(grid_corners['upperLeft']['y'])  # y Upper Left
        data['xCellSize'] = abs(float(grid_corners['upperLeft']['x']) - float(grid_corners['lowerRight']['x'])) / data['columns']
        data['yCellSize'] = abs(float(grid_corners['upperLeft']['y']) - float(grid_corners['lowerRight']['y'])) / data['rows']
    elif 'gridFirstCell' in data:
        """
        "gridFirstCell": {
            "firstCellCenter": {
                "x": 117.35,
                "y": 28.75
            },
            "xCellSize": 0.0489,
            "yCellSize": 0.0489
        }
        """
        grid_first_cell = data['gridFirstCell']
        data['xCellSize'] = float(grid_first_cell['xCellSize'])
        data['yCellSize'] = float(grid_first_cell['yCellSize'])
        data['xULCorner'] = float(grid_first_cell['firstCellCenter']['x']) - float(grid_first_cell['xCellSize'])/2
        data['yULCorner'] = float(grid_first_cell['firstCellCenter']['y']) + float(grid_first_cell['yCellSize'])/2
    else:
        assert False, 'Unable to find gridCorners or gridCorners'
    return data


def remove_download_files(flask_app):
    import glob
    from time import time
    now = time()
    # logger.info(f"running {path} - {glob.glob(path + '/download-*.nc')}")
    count = 0
    for i, f in enumerate(glob.glob(f"{flask_app.config['UPLOAD_FOLDER']}/download-*.nc")):
        if os.path.getmtime(f) < now - 60:  # Delete files older than 60 seconds
            if os.path.isfile(f):
                os.remove(f)
                count += 1
    if count:
        logger.info(f'Removed #{count} of download files')


def every(delay, task, flask_app):
    import time, traceback
    next_time = time.time() + delay
    while True:
        time.sleep(max(0, next_time - time.time()))
        try:
            task(flask_app)
        except Exception:
            traceback.print_exc()
        # in production code you might want to have this instead of course:
        # logger.exception("Problem while executing repetitive task.")
        # skip tasks if we are behind schedule:
        next_time += (time.time() - next_time) // delay * delay + delay
