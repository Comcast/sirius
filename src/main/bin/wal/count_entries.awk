#!/usr/bin/awk -f
BEGIN {
    FS="|"
    vod=program=mapping = 0;
}
$3 ~ /program\// { program++ }
$3 ~ /vod\// { vod++ }
$3 ~ /mapping\// { mapping++ }
END {
    print "";
    print "================";
    print "  BREAKDOWN  ";
    print "================";
    print "Program: " program;
    print "Vod:     " vod;
    print "Mapping: " mapping;
    print "================";
    print "Total:   " NR;
    print "================";
}
