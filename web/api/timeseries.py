from flask import Blueprint, request, jsonify
import logging
import sys
import netCDF4
import numpy as np


bp = Blueprint('timeseries', __name__)
logger = logging.getLogger(__name__)

@bp.route("/timeseries", methods=['POST'])
def timeseries_create():
    f = netCDF4.Dataset('data/new.nc')
    logger.info(f)
    return jsonify(f.data_model)
