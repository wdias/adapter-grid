FROM python:3.7-slim

RUN apt-get update && \
    apt-get install -y wget curl libcurl3 && \
    apt-get -yq install gcc \
                        build-essential \
                        tar \
                        bzip2 \
                        m4 \
                        zlib1g-dev \
                        libopenmpi-dev && \
    rm -rf /var/lib/apt/lists/*

# HDF5 C Library
ENV HDF5_MAJOR_REL=hdf5-1.10 \
    HDF5_MINOR_REL=hdf5-1.10.4 \
    HDF5_SRC_URL=http://www.hdfgroup.org/ftp/HDF5/releases
RUN wget -q ${HDF5_SRC_URL}/${HDF5_MAJOR_REL}/${HDF5_MINOR_REL}/src/${HDF5_MINOR_REL}.tar.gz -O ${HDF5_MINOR_REL}.tar.gz
RUN echo "Installing HDF5 $HDF5_MINOR_REL" && \
    tar xf ${HDF5_MINOR_REL}.tar.gz && \
    cd ${HDF5_MINOR_REL} && \
    CC=mpicc ./configure --enable-shared --enable-hl --enable-parallel --prefix=/usr/local/hdf5 && \
    # 2 for number of procs to be used
    make -j 2 && \
    make install && \
    cd .. && \
    rm -rf /hdf5-${HDF5_MINOR_REL} /hdf5-${HDF5_MINOR_REL}.tar.gz

# NetCDF C Library
ENV NETCDF_C_VERSION 4.6.2.1
# ENV NETCDF_C_VERSION `curl https://github.com/Unidata/netcdf-c/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'`
RUN curl -L https://github.com/Unidata/netcdf-c/archive/v${NETCDF_C_VERSION}.tar.gz > netcdf-c-${NETCDF_C_VERSION}.tar.gz
RUN echo "Installing NetCDF $NETCDF_C_VERSION" && \
    tar xf netcdf-c-${NETCDF_C_VERSION}.tar.gz && \
    cd netcdf-c-${NETCDF_C_VERSION} && \
    ./configure --prefix=/usr/local/netcdf --enable-netcdf-4 --enable-shared --disable-dap \
                CC=mpicc \
                LDFLAGS=-L/usr/local/hdf5/lib \
                # CFLAGS=-I/usr/local/hdf5/include \
                CPPFLAGS=-I/usr/local/hdf5/include && \
    # 2 for number of procs to be used
    make -j 2 && \
    make check && \
    make install && \
    cd .. && \
    rm -rf /netcdf-c-${NETCDF_C_VERSION} /netcdf-c-${NETCDF_C_VERSION}.tar.gz


WORKDIR /src
EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--timeout=10", "--workers=2", "web.app:app"]

RUN pip3 install \
  gunicorn \
  flask \
  webargs==4.1.2 \
  netCDF4==1.4.2 \
  numpy==1.16.1 \
  cftime \
  mpi4py

RUN wget https://github.com/Unidata/netcdf4-python/blob/master/examples/data/rtofs_glo_3dz_f006_6hrly_reg3.nc

COPY . /src
RUN cd /src && python3 setup.py develop
