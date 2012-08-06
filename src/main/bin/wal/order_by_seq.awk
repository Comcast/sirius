#!/usr/bin/awk -f
BEGIN {
    FS="|";
}
{
    command = $2
    key = $3;
    seq = $4 + 0;
    
    if( !(key in keys) || seq >= keys[key]) {
        keys[key]=seq;
    }
}
END {
    for( item in keys ) {
        print keys[item]"|"item;
    }
}
