#
#
#

DBHOST=rds-postgres15-staging.internal.lib.virginia.edu
DBUSER=easystore
DBNAME=easystore
DBPWD=Iojaiviuhee7toh7Ohni6ho2eoj3iesh
PSQL_TOOL=psql

if [ $# -ne 1 ]; then
   echo "use: $(basename $0) <input file>"
   exit 1
fi

INFILE=$1

PGPASSWORD=${DBPWD} ${PSQL_TOOL} -w -q -h ${DBHOST} -U ${DBUSER} -d ${DBNAME} -f ${INFILE}

#
# end of file
#
