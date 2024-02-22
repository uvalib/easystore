#
#
#

DBHOST=rds-postgres15-staging.internal.lib.virginia.edu
DBUSER=easystore
DBNAME=easystore
DBPWD=Iojaiviuhee7toh7Ohni6ho2eoj3iesh
PSQL_TOOL=psql

PGPASSWORD=${DBPWD} ${PSQL_TOOL} -w -q -h ${DBHOST} -U ${DBUSER} -d ${DBNAME}

#
# end of file
#
