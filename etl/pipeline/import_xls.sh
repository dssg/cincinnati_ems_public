XLSFILE=$1
OUTDIR=$2
SCHEMA=$3

echo $XLSFILE

# Changing xls to csv
# ssconvert will throws some warnings
ssconvert -S "$XLSFILE" $OUTDIR/firehouse_equip_csvs_%n.csv

# The csv 0 file has unnamed columns and csvsql can't deal so manually name
psql -c "CREATE TABLE $SCHEMA.cfd_station_info (
            district VARCHAR(2) NOT NULL,
            engine VARCHAR(8) NOT NULL,
            truck VARCHAR(7),
            medic VARCHAR(5),
            dist VARCHAR(4),
            unnamed1 VARCHAR(6),
            unnamed2 VARCHAR(6),
            unnamed3 VARCHAR(6),
            unnamed4 VARCHAR(6),
            address VARCHAR(25) NOT NULL,
            zip_code INTEGER NOT NULL,
            dept VARCHAR(8) NOT NULL,
            pay_phone VARCHAR(8) NOT NULL
        );"
head -n -2 $OUTDIR/firehouse_equip_csvs_0.csv | psql -c "\copy $SCHEMA.cfd_station_info from stdin with csv header;"

# Import the csv 2 file -- a data dictionary
psql -c "CREATE TABLE $SCHEMA.unit_dictionary (
            agency VARCHAR(2) NOT NULL,
            duty_type VARCHAR(2) NOT NULL,
            description VARCHAR(18) NOT NULL,
            capabilities VARCHAR(6)
        );"
head -n 18 $OUTDIR/firehouse_equip_csvs_2.csv | psql -c "\copy $SCHEMA.unit_dictionary from stdin with csv header;"

# Clean up
rm $OUTDIR/firehouse_equip_csvs_*.csv
