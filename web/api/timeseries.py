from flask import Blueprint, request, jsonify
import logging
import os
import netCDF4
import numpy as np
from werkzeug.utils import secure_filename
from flask import current_app as app
from web import util

bp = Blueprint('timeseries', __name__)
logger = logging.getLogger(__name__)

NETCDF_FILE_FORMAT = 'NETCDF4_CLASSIC'  # 'NETCDF4', 'NETCDF4_CLASSIC'
ALLOWED_EXTENSIONS = set(['nc'])
DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
TIME_FILTER = pow(10, 10)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def create_not_exists(filepath, timeseries_id):
    try:
        if not os.path.isfile(filepath):
            logger.info(f'Creating new database for timeseries: {timeseries_id}')
            ncfile = netCDF4.Dataset(filepath, mode='w', format=NETCDF_FILE_FORMAT, parallel=True)
            timeseries = util.get_timeseries(timeseries_id)
            assert 'locationId' in timeseries, f'locationId not found for Timeseries: {timeseries_id}'
            location = util.get_regular_grid(timeseries.get('locationId'))

            lat_dim = ncfile.createDimension('latitude', location.get('rows'))  # Y axis
            lon_dim = ncfile.createDimension('longitude', location.get('columns'))  # X axis
            time_dim = ncfile.createDimension('timestamp', None)

            ncfile.moduleId = timeseries.get('moduleId')
            ncfile.valueType = timeseries.get('valueType')
            ncfile.parameterId = timeseries.get('parameterId')
            ncfile.locationId = timeseries.get('locationId')
            ncfile.timeseriesType = timeseries.get('timeseriesType')
            ncfile.timeStepId = timeseries.get('timeStepId')

            lat = ncfile.createVariable('latitude', np.float32, ('latitude',))
            lat.units = location.get('geoDatum')
            # logger.info("latitude: %s", lat)
            lon = ncfile.createVariable('longitude', np.float32, ('longitude',))
            lon.units = location.get('geoDatum')
            time = ncfile.createVariable('timestamp', np.float64, ('timestamp',))
            time.set_collective(True)
            # NOTE: There's an issue with storing larger value with collective mode. In order to reduce the size, decrease the date gap
            # time.units = "days since 1970-01-01 00:00"
            time.units = "days since 2015-01-01 00:00"

            val = ncfile.createVariable('value', np.float32, ('timestamp', 'latitude', 'longitude',))
            val.set_collective(True)
            val.units = timeseries.get('parameterId')
            # Write lat and lon
            lat[:] = location['yULCorner'] - location['yCellSize'] * np.arange(location['rows'])
            lon[:] = location['xULCorner'] + location['xCellSize'] * np.arange(location['columns'])

            ncfile.sync()
            ncfile.close()
        return True
    except Exception as err:
        logger.error(err)
        return False


def merge_netcdf(filename: str, timeseries_id: str):
    merge_nc = netCDF4.Dataset(os.path.join(app.config['UPLOAD_FOLDER'], filename), mode='r', format=NETCDF_FILE_FORMAT)
    assert create_not_exists(f'/tmp/data-{timeseries_id}.nc', timeseries_id), 'Unable to create DB store'
    ncfile = netCDF4.Dataset(f'/tmp/data-{timeseries_id}.nc', mode='r+', format=NETCDF_FILE_FORMAT, parallel=True)
    # ncfile = netCDF4.Dataset(f'/tmp/data-{timeseries_id}.nc', mode='r+', format=NETCDF_FILE_FORMAT)

    merge_time = merge_nc.variables['timestamp']
    merge_val = merge_nc.variables['value']
    time = ncfile.variables['timestamp']
    time.set_collective(True)
    val = ncfile.variables['value']
    val.set_collective(True)

    merge_nc.set_auto_mask(False)
    times = merge_time[merge_time[:] < TIME_FILTER]
    for t in times:
        val[t, :, :] = merge_val[t, :, :]
    time[:] = np.unique(np.append(time, times), axis=0)

    merge_nc.close()
    ncfile.sync()
    ncfile.close()


@bp.route("/timeseries/<string:timeseries_id>", methods=['POST'])
def timeseries_create(timeseries_id):
    assert timeseries_id, 'timeseries_id should be provided.'
    # check if the post request has the file part
    if 'file' not in request.files:
        from werkzeug.datastructures import FileStorage
        from uuid import uuid4
        filename = f'grid_{timeseries_id}-{uuid4()}.nc'
        FileStorage(request.stream).save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        merge_netcdf(filename, timeseries_id)
        return 'Stream OK', 200

    # Solution: https://stackoverflow.com/a/54857411/1461060
    # This'll not work if it checks for `if 'file' not in request.files` above
    #
    # def custom_stream_factory(total_content_length, filename, content_type, content_length=None):
    #     import tempfile
    #     tmpfile = tempfile.NamedTemporaryFile('wb+', prefix='flaskapp', suffix='.nc')
    #     app.logger.info("start receiving file ... filename => " + str(tmpfile.name))
    #     return tmpfile
    #
    # import werkzeug, flask
    # stream, form, files = werkzeug.formparser.parse_form_data(flask.request.environ, stream_factory=custom_stream_factory)
    # for fil in files.values():
    #     app.logger.info(" ".join(["saved form name", fil.name, "submitted as", fil.filename, "to temporary file", fil.stream.name]))
    #     merge_netcdf(fil.stream.name, timeseries_id)
    # return 'OK', 200

    file = request.files['file']
    # if user does not select file, browser also submit an empty part without filename
    if file.filename == '':
        return 'No selected file', 400
    if file and allowed_file(file.filename):
        from uuid import uuid4
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        merge_netcdf(filename, timeseries_id)
        logger.info(filename)
        return 'OK', 200


@bp.route("/timeseries/<string:timeseries_id>/<request_name>", methods=['GET'])
def timeseries_get(timeseries_id: str, request_name):
    from flask import send_from_directory
    filename = f'data-{timeseries_id}.nc'
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True, attachment_filename=request_name, mimetype='application/x-netcdf4')
