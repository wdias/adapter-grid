import logging
import os
import netCDF4
import numpy as np
from web import util

logger = logging.getLogger(__name__)

NETCDF_FILE_FORMAT = 'NETCDF4_CLASSIC'  # 'NETCDF4', 'NETCDF4_CLASSIC'
ALLOWED_EXTENSIONS = set(['nc'])
DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
TIME_FILTER = pow(10, 10)


def create_parallel_not_exists(file_path, timeseries_id):
    try:
        if not os.path.isfile(file_path):
            logger.info(f'Creating new database for timeseries: {timeseries_id} @ {file_path}')
            nc_file = netCDF4.Dataset(file_path, mode='w', format=NETCDF_FILE_FORMAT, parallel=True)

            timeseries = util.get_timeseries(timeseries_id)
            assert 'locationId' in timeseries, f'locationId not found for Timeseries: {timeseries_id}'
            location = util.get_regular_grid(timeseries.get('locationId'))
            assert location and location.get('rows'), f'location date not found for Timeseries: {timeseries_id}'

            lat_dim = nc_file.createDimension('latitude', location.get('rows'))  # Y axis
            lon_dim = nc_file.createDimension('longitude', location.get('columns'))  # X axis
            time_dim = nc_file.createDimension('timestamp', None)

            nc_file.moduleId = timeseries.get('moduleId')
            nc_file.valueType = timeseries.get('valueType')
            nc_file.parameterId = timeseries.get('parameterId')
            nc_file.locationId = timeseries.get('locationId')
            nc_file.timeseriesType = timeseries.get('timeseriesType')
            nc_file.timeStepId = timeseries.get('timeStepId')

            lat = nc_file.createVariable('latitude', np.float32, ('latitude',))
            lat.units = location.get('geoDatum')
            # logger.info("latitude: %s", lat)
            lon = nc_file.createVariable('longitude', np.float32, ('longitude',))
            lon.units = location.get('geoDatum')
            time = nc_file.createVariable('timestamp', np.float64, ('timestamp',))
            # NOTE: There's an issue with storing larger value with collective mode. In order to reduce the size, decrease the date gap
            # time.units = "days since 1970-01-01 00:00"
            time.units = "days since 2015-01-01 00:00"

            val = nc_file.createVariable('value', np.float32, ('timestamp', 'latitude', 'longitude',))
            val.units = timeseries.get('parameterId')
            # Write lat and lon
            lat[:] = location['yULCorner'] - location['yCellSize'] * np.arange(location['rows'])
            lon[:] = location['xULCorner'] + location['xCellSize'] * np.arange(location['columns'])

            nc_file.sync()
            nc_file.close()
        return True
    except Exception as err:
        logger.error(err)
        return False


def get_non_parallel_netcdf_file(filename, timeseries_id):
    if os.path.isfile(filename):
        nc_file = netCDF4.Dataset(filename, mode='r+', format=NETCDF_FILE_FORMAT)
    else:
        logger.info('Initializing NetCDF file')
        nc_file = netCDF4.Dataset(filename, mode='w', format=NETCDF_FILE_FORMAT)
    # logger.info(ncfile)
    if 'latitude' not in nc_file.dimensions or 'longitude' not in nc_file.dimensions or 'timestamp' not in nc_file.dimensions:
        timeseries = util.get_timeseries(timeseries_id)
        assert 'locationId' in timeseries, f'locationId not found for Timeseries: {timeseries_id}'
        location = util.get_regular_grid(timeseries.get('locationId'))
        assert location and location.get('rows'), f'location date not found for Timeseries: {timeseries_id}'

        lat_dim = nc_file.createDimension('latitude', location.get('rows'))  # Y axis
        lon_dim = nc_file.createDimension('longitude', location.get('columns'))  # X axis
        time_dim = nc_file.createDimension('timestamp', None)
        # logger.info("Dimentions: %s", ncfile.dimensions.items())

        nc_file.moduleId = timeseries.get('moduleId')
        nc_file.valueType = timeseries.get('valueType')
        nc_file.parameterId = timeseries.get('parameterId')
        nc_file.locationId = timeseries.get('locationId')
        nc_file.timeseriesType = timeseries.get('timeseriesType')
        nc_file.timeStepId = timeseries.get('timeStepId')

        lat = nc_file.createVariable('latitude', np.float32, ('latitude',))
        lat.units = "Kandawala"
        # logger.info("latitude: %s", lat)
        lon = nc_file.createVariable('longitude', np.float32, ('longitude',))
        lon.units = "Kandawala"
        time = nc_file.createVariable('timestamp', np.float64, ('timestamp',))
        # NOTE: There's an issue with storing larger value with collective mode. In order to reduce the size, decrease the date gap
        # time.units = "days since 1970-01-01 00:00"
        time.units = "days since 2015-01-01 00:00"
        val = nc_file.createVariable('value', np.float32, ('timestamp', 'latitude', 'longitude',))
        val.units = 'O.Precipitation'
        # Write lat and lon
        lat[:] = location['yULCorner'] - location['yCellSize'] * np.arange(location['rows'])
        lon[:] = location['xULCorner'] + location['xCellSize'] * np.arange(location['columns'])
        nc_file.sync()
    return nc_file
