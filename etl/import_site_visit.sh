#!/bin/bash
eval $(cat ../default_profile)
SCHEMA=raw


# Changing xls to csv
# ssconvert will throws some warnings
ssconvert $SRCDIR/../site-visit/June10and11.xls $WORKDIR/june10and11.csv

cat $WORKDIR/june10and11.csv | iconv -t ascii | tr [:upper:] [:lower:] | tr ' ' '_' | csvsql -i postgresql > $WORKDIR/june10and11_headers.txt
sed 's/stdin/'"$SCHEMA"'.june10and11/' $WORKDIR/june10and11_headers.txt | psql
rm $WORKDIR/june10and11_headers.txt
cat $WORKDIR/june10and11.csv | psql -c "\copy $SCHEMA.june10and11 from stdin with csv header;"

