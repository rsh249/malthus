source ./av_keys # get Alpha Vantage keys from key file or set environmental variables AV_KEY and AV_STUDENT KEY
mkdir avdata

# get list of NYSE symbols and metadata
wget https://datahub.io/core/nyse-other-listings/r/nyse-listed.csv #may need updating this list is ~2 years old
#wget 'http://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download' #investigate


# collect symbol codes from nyse-listed.csv and wget the API url with an active API key:
cat nyse-listed.csv | sed "1d" | awk -F ',' '{print $1}'  | 
xargs -P 4 -n 1 -I {} wget --output-document "avdata/"{}".csv" "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&apikey=$AV_KEY&datatype=csv&symbol="{}

ls avdata