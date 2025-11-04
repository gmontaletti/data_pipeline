#!/bin/bash

echo "=========================================="
echo "Pipeline Progress Monitor"
echo "=========================================="
echo ""

cd ~/Documents/funzioni/data_pipeline

if [ -f "_targets/meta/progress" ]; then
    echo "Current Status:"
    echo "-------------------------------------------"
    tail -20 _targets/meta/progress | column -t -s '|'
    echo ""

    total=$(tail -n +2 _targets/meta/progress | wc -l | tr -d ' ')
    completed=$(tail -n +2 _targets/meta/progress | grep -c "completed")
    running=$(tail -n +2 _targets/meta/progress | grep -c "dispatched")

    echo "Summary:"
    echo "  Total targets: $total"
    echo "  Completed: $completed"
    echo "  Running: $running"
    echo ""

    if [ $completed -gt 0 ]; then
        pct=$((completed * 100 / total))
        echo "  Progress: ${pct}%"
    fi
else
    echo "Pipeline not started yet or progress file not found."
fi

echo ""
echo "=========================================="
echo "To stop the pipeline: kill the R process"
echo "To view full log: tail -f pipeline_run.log"
echo "=========================================="
