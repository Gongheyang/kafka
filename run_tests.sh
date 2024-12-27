#!/bin/bash

REPORT_DIR="test_reports"

# Number of times to run the test
RUNS=20

# Commands to run the test (replace these with your actual commands)
COMMAND1="tests/docker/ducker-ak up"
COMMAND2="tests/docker/ducker-ak test tests/kafkatest/tests/client/share_consumer_test.py::ShareTest.test_broker_rolling_bounce"

# Directory to store reports

rm -r $REPORT_DIR
mkdir -p $REPORT_DIR

# Loop to run the test multiple times
for ((i=1; i<=RUNS; i++))
do
    echo "Running test iteration $i..."
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    REPORT_FILE="$REPORT_DIR/test_report_$i_$TIMESTAMP.log"

    # Run the first command and save the output
    echo "Running Command 1: $COMMAND1" | tee -a $REPORT_FILE
    $COMMAND1 >> $REPORT_FILE 2>&1

    # Run the second command and save the output
    echo "Running Command 2: $COMMAND2" | tee -a $REPORT_FILE
    $COMMAND2 >> $REPORT_FILE 2>&1

    echo "Test iteration $i completed. Report saved to $REPORT_FILE"
done

echo "All tests completed. Reports are in $REPORT_DIR"