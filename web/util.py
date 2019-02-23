import requests, json

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
