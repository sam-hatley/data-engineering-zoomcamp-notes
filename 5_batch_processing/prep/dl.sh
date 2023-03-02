TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`
    LOCAL_PREFIX="../data/raw/${TAXI_TYPE}/${YEAR}"
    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz
done