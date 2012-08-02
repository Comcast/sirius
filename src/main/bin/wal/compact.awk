#!/usr/bin/awk -f
BEGIN {
    FS="|";
    while ((getline line < FILE) > 0) {
        sequences[line]=1;
    }
    close($2)
}
{
    command = $2;
    key = $3;
    seq = $4;
    seq_key = $4"|"$3

    if(seq_key in sequences) {
        if(command == "PUT") {
            print $0;
        }
    }
}
