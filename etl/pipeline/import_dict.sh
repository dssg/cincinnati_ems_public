FILE=$1
OUTDIR=$2
SCHEMA=$3

# Changing xls to csv
# ssconvert will throws some warnings
ssconvert $FILE $OUTDIR/cdf_response_typ.csv
cat $OUTDIR/cdf_response_typ.csv | iconv -t ascii | tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql > $OUTDIR/dict_headers.txt
sed 's/stdin/'"$SCHEMA"'.cdf_response_typ/' $OUTDIR/dict_headers.txt | psql
rm $OUTDIR/dict_headers.txt
cat $OUTDIR/cdf_response_typ.csv | psql -c "\copy $SCHEMA.cdf_response_typ from stdin with csv header;"

