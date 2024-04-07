#!/bin/bash
expected_pass=50
rm -f output.txt
for i in $( eval echo {1..$expected_pass} );do /usr/bin/time -h go test 2>&1 >> output.txt; done
actual_pass=$(grep -c "PASS" output.txt) 
if [[ $actual_pass != $expected_pass ]]; then
    echo "expected $expected_pass PASS, you got $actual_pass"
    exit 1
else 
    echo "PASS"
fi;