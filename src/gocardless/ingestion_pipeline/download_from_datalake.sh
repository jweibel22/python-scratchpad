#!/usr/bin/env bash

rename_files() {
    for f in $1/*.avro
        do
            window=`echo ${f} | sed 's/^.*\/....-..-..T\(..\)\:\(..\)\:00\.000Z.*$/\1\2/g'`
            pane=`echo ${f} | sed 's/^.*pane-\(.\)-.*$/\1/g'`
            mv $f $1/w${window}-p${pane}.avro
        done
}

convert_to_json() {
    for f in $1/*.avro
    do
        outfile=${f/%.avro/.jsonl}
        avro-tools tojson $f > ${outfile}
    done
}

# gsutil -m cp $1 $2

# avro-tools doesnt like the filenames
# rename_files $2

convert_to_json $2
