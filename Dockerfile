FROM python:3.7.3-slim

RUN apt-get update && \
    apt-get install -y wget curl libcurl3 sudo openssh-client vim && \
    apt-get -yq install gcc \
                        build-essential \
                        tar \
                        bzip2 \
                        m4 \
                        zlib1g-dev \
                        libopenmpi-dev && \
    rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --gecos '' wdias
RUN adduser wdias sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
WORKDIR /src
RUN chmod 777 /src

USER wdias

# HDF5 C Library
ENV HDF5_MAJOR_REL=hdf5-1.10 \
    HDF5_MINOR_REL=hdf5-1.10.4 \
    HDF5_SRC_URL=http://www.hdfgroup.org/ftp/HDF5/releases
# ENV RUNPARALLEL='mpiexec -n ${NPROCS:\=6}'
ENV RUNPARALLEL='mpiexec -n 2'
RUN wget -q ${HDF5_SRC_URL}/${HDF5_MAJOR_REL}/${HDF5_MINOR_REL}/src/${HDF5_MINOR_REL}.tar.gz -O ${HDF5_MINOR_REL}.tar.gz
RUN echo "Installing HDF5 $HDF5_MINOR_REL" && \
    tar xf ${HDF5_MINOR_REL}.tar.gz && \
    cd ${HDF5_MINOR_REL} && \
    CC=mpicc ./configure --enable-shared --enable-hl --enable-parallel --prefix=/usr/local && \
    # 2 for number of procs to be used
    make -j 2 && \
    sudo make install && \
    cd .. && \
    rm -rf /hdf5-${HDF5_MINOR_REL} /hdf5-${HDF5_MINOR_REL}.tar.gz

# NetCDF C Library
ENV NETCDF_C_VERSION 4.6.3
# ENV NETCDF_C_VERSION `curl https://github.com/Unidata/netcdf-c/releases/latest | grep -o "/v.*\"" | sed 's:^..\(.*\).$:\1:'`
RUN curl -L https://github.com/Unidata/netcdf-c/archive/v${NETCDF_C_VERSION}.tar.gz > netcdf-c-${NETCDF_C_VERSION}.tar.gz
RUN echo "Installing NetCDF $NETCDF_C_VERSION" && \
    tar xf netcdf-c-${NETCDF_C_VERSION}.tar.gz && \
    cd netcdf-c-${NETCDF_C_VERSION} && \
    ./configure --prefix=/usr/local --enable-netcdf-4 --enable-shared --enable-parallel4 --enable-parallel-tests --disable-dap \
                CC=mpicc \
                LDFLAGS=-L/usr/local/lib \
                # CFLAGS=-I/usr/local/include \
                #CPPFLAGS=-I/usr/local/include && \
                CPPFLAGS="-I/usr/local/include -I/usr/include/openmpi" && \
                #LIBS=-lhdf5_hl && \
    # 2 for number of procs to be used
    #make -j 2 check && \
    sudo make install && \
    cd .. && \
    rm -rf /netcdf-c-${NETCDF_C_VERSION} /netcdf-c-${NETCDF_C_VERSION}.tar.gz


#WORKDIR /src
EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--timeout=10", "--workers=2", "web.app:app"]

RUN sudo pip3 install \
  gunicorn \
  flask \
  webargs==4.1.2 \
  numpy==1.16.1 \
  cftime \
  mpi4py \
  requests

#RUN sudo USE_SETUPCFG=0 HDF5_INCDIR=/usr/local/include HDF5_LIBDIR=/usr/local/lib \
#    NETCDF4_INCDIR=/usr/local/include NETCDF4_LIBDIR=/usr/local/lib \
#    mpi_incdir=/usr/include/openmpi pip3 install netCDF4==1.4.3
#RUN sudo pip3 install netCDF4==1.4.3
ENV NETCDF4_PYTHON 1.4.3.1
RUN wget -q https://github.com/Unidata/netcdf4-python/archive/v${NETCDF4_PYTHON}.tar.gz -O netcdf4-python-${NETCDF4_PYTHON}.tar.gz && \
    tar xf  netcdf4-python-${NETCDF4_PYTHON}.tar.gz
RUN cd netcdf4-python-${NETCDF4_PYTHON} && \
    echo "mpi_incdir=/usr/include/openmpi" >> setup.cfg && \
    sudo python setup.py build && sudo python setup.py install && \
    cd .. && rm -rf netcdf4-python-${NETCDF4_PYTHON}.tar.gz

COPY . /src
RUN cd /src && sudo python3 setup.py develop
