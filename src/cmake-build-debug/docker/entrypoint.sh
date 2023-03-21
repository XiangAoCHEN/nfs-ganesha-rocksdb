#!/bin/bash
#
# Entrypoint for the Ganesha docker container.  If "shell" is given as the
# argument, then a shell is run; otherwise, Ganseha is started.

# These are the options for starting Ganesha.  Set them in the environment.
: ${GANESHA_LOGFILE:="/var/log/ganesha.log"}
: ${GANESHA_CONFFILE:="/etc/ganesha/ganesha.conf"}
: ${GANESHA_OPTIONS:="-N NIV_EVENT"}
: ${GANESHA_EPOCH:=""}
: ${GANESHA_LIBPATH:="/usr/lib"}

function rpc_init {
	rpcbind
	rpc.statd -L
	rpc.idmapd
}

if [ "$1" == "shell" ]; then
	/bin/bash
else
	rpc_init
	LD_LIBRARY_PATH="${GANESHA_LIBPATH}" /usr/bin/ganesha.nfsd -F -L ${GANESHA_LOGFILE} -f ${GANESHA_CONFFILE} ${GANESHA_OPTIONS} ${GANESHA_EPOCH}
fi
