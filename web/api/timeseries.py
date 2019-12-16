from flask import Blueprint, request, jsonify
import logging
import os
import netCDF4
import numpy as np
from werkzeug.utils import secure_filename
from flask import current_app as app
from datetime import datetime
from web.api import util_netcdf
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


def merge_netcdf(filename: str, timeseries_id: str):
    merge_nc = netCDF4.Dataset(os.path.join(app.config['UPLOAD_FOLDER'], filename), mode='r', format=NETCDF_FILE_FORMAT)
    filename = os.path.join(app.config['UPLOAD_FOLDER'], f'data-{timeseries_id}.nc')
    assert util_netcdf.create_parallel_not_exists(filename, timeseries_id), 'Unable to create DB store'
    nc_file = netCDF4.Dataset(filename, mode='r+', format=NETCDF_FILE_FORMAT, parallel=True)
    # nc_file = netCDF4.Dataset(filename, mode='r+', format=NETCDF_FILE_FORMAT)

    merge_time = merge_nc.variables['timestamp']
    merge_val = merge_nc.variables['value']
    time = nc_file.variables['timestamp']
    time.set_collective(True)
    val = nc_file.variables['value']
    val.set_collective(True)

    merge_nc.set_auto_mask(False)
    times = merge_time[merge_time[:] < TIME_FILTER]
    for t in times:
        val[t, :, :] = merge_val[t, :, :]
    time[:] = np.unique(np.append(time, times), axis=0)

    merge_nc.close()
    nc_file.sync()
    nc_file.close()


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
        # Remove netCDF file after merge
        if util.OPTIMIZE_STORAGE:
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename))
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


def extract_netcdf(timeseries_id: str, request_id: str, start_time: datetime, end_time: datetime):
    data_filename = os.path.join(app.config['UPLOAD_FOLDER'], f'data-{timeseries_id}.nc')
    assert os.path.isfile(data_filename), f'Timeseries {timeseries_id} data not found'
    nc_all = netCDF4.Dataset(data_filename, mode='r', format=NETCDF_FILE_FORMAT, parallel=True)
    download_filename = os.path.join(app.config['UPLOAD_FOLDER'], f'download-{timeseries_id}-{request_id}.nc')
    nc_file = util_netcdf.get_non_parallel_netcdf_file(download_filename ,timeseries_id)

    all_time = nc_all.variables['timestamp']
    all_val = nc_all.variables['value']
    time = nc_file.variables['timestamp']
    val = nc_file.variables['value']

    nc_all.set_auto_mask(False)
    times = all_time[all_time[:] <= netCDF4.date2num(end_time, all_time.units)]
    times = times[times[:] >= netCDF4.date2num(start_time, all_time.units)]
    for t in times:
        val[t, :, :] = all_val[t, :, :]
    time[:] = np.append(time, times)

    nc_file.sync()
    nc_file.close()
    nc_all.close()


@bp.route("/timeseries/<string:timeseries_id>/<request_name>", methods=['GET'])
def timeseries_get(timeseries_id: str, request_name):
    from flask import send_from_directory
    from secrets import token_urlsafe

    request_id = token_urlsafe(16)
    filename = f'download-{timeseries_id}-{request_id}.nc'
    start = request.args.get('start')
    logger.info(f"extract grid data of {timeseries_id}, starts from {start} to {filename}")
    assert request.args.get('start'), 'start date time should be provide'
    start_time = datetime.strptime(request.args.get('start'), DATE_TIME_FORMAT)
    assert request.args.get('end'), 'end date time should be provide'
    end_time = datetime.strptime(request.args.get('end'), DATE_TIME_FORMAT)
    extract_netcdf(timeseries_id, request_id, start_time, end_time)
    # TODO: Remove netCDF file after download
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True, attachment_filename=request_name, mimetype='application/x-netcdf4')
