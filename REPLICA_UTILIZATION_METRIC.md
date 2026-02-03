# Ray Serve Replica Utilization Metric

## Overview

This implementation adds a new Prometheus metric `ray_serve_replica_utilization` that measures how efficiently a Ray Serve replica is utilizing its configured concurrency capacity.

## Metric Details

### Name
`serve_replica_utilization`

### Type
Gauge (0.0 to 1.0)

### Formula
```
utilization = total_user_code_time / (wall_clock_duration * max_ongoing_requests)
```

Where:
- `total_user_code_time`: Sum of time spent executing user code across all concurrent requests in the measurement window
- `wall_clock_duration`: Elapsed time of the measurement window
- `max_ongoing_requests`: Configured replica concurrency

### Labels
Same as existing replica metrics:
- `deployment`: Name of the deployment
- `replica`: Replica ID
- `application`: Application name

### Example Values
A replica with `max_ongoing_requests=4` over a 10-second window:
- If requests spent a total of 20 seconds in user code → utilization = 20 / (10 * 4) = 0.5 (50%)
- If requests spent 40 seconds total → utilization = 40 / (10 * 4) = 1.0 (100%)

## Implementation Details

### Files Modified

1. **`python/ray/serve/_private/replica.py`**
   - Modified `ReplicaMetricsManager.__init__()` to accept `max_ongoing_requests` parameter
   - Added tracking variables for total user code time and window start time
   - Added `_replica_utilization_gauge` metric definition
   - Modified `record_request_metrics()` to accumulate user code execution time
   - Added `_update_replica_utilization()` method to calculate and update the metric
   - Added `_update_utilization_forever()` async task for periodic updates when cached metrics are disabled
   - Modified `_report_cached_metrics()` to call `_update_replica_utilization()`
   - Updated replica instantiation to pass `max_ongoing_requests` to metrics manager

2. **`python/ray/serve/tests/test_metrics.py`**
   - Added `test_replica_utilization_metric()` test function
   - Updated expected metrics list to include `serve_replica_utilization`

### How It Works

1. **Initialization**: When a replica starts, the metrics manager stores the configured `max_ongoing_requests` value and initializes tracking variables.

2. **Time Tracking**: Every time a request completes, `record_request_metrics()` accumulates the request's execution time (latency) into `_total_user_code_time_s`.

3. **Periodic Calculation**:
   - If cached metrics are enabled: The utilization is calculated every time cached metrics are reported (default interval)
   - If cached metrics are disabled: A separate async task updates utilization every 10 seconds

4. **Calculation**: The `_update_replica_utilization()` method:
   - Calculates the wall clock duration since the last update
   - Computes utilization as `total_user_code_time / (wall_clock_duration * max_ongoing_requests)`
   - Clamps the value to [0.0, 1.0]
   - Resets counters for the next measurement window

5. **Export**: The metric is automatically exported via Ray's Prometheus integration at the standard metrics endpoint (port 9999).

## Use Cases

### Right-sizing Concurrency
Monitor utilization to determine if `max_ongoing_requests` is set appropriately:
- Consistently low utilization (< 0.3) → May be over-provisioned
- Consistently high utilization (> 0.8) → May need more concurrency

### Autoscaling Decisions
Use utilization alongside other metrics to make informed autoscaling decisions:
```python
# Example autoscaling logic
if utilization > 0.8 and queue_length > 10:
    scale_up()
elif utilization < 0.2 and queue_length == 0:
    scale_down()
```

### Performance Analysis
Identify bottlenecks:
- Low utilization + high latency → Slow user code
- High utilization + low latency → Good throughput

### Capacity Planning
Estimate how much additional load a deployment can handle:
```
spare_capacity = (1.0 - utilization) * max_ongoing_requests
```

## Testing

Run the test:
```bash
pytest python/ray/serve/tests/test_metrics.py::test_replica_utilization_metric -v
```

Run the example:
```bash
python example_replica_utilization.py
```

Then check the metrics:
```bash
curl http://localhost:9999/metrics | grep serve_replica_utilization
```

## Configuration

The metric uses the same export interval as other cached metrics:
- Controlled by `RAY_SERVE_METRICS_EXPORT_INTERVAL_MS` environment variable
- Default: 10 seconds (when cached metrics are enabled)
- When cached metrics are disabled (interval = 0), utilization updates every 10 seconds

## Notes

- The metric measures actual user code execution time, not wall clock time with concurrency
- Values can theoretically exceed 1.0 if measurement windows overlap, but this is clamped to 1.0
- The metric resets its measurement window after each calculation to provide fresh data
- Zero-duration or zero-concurrency cases are handled gracefully (returns 0.0)

## Future Enhancements

Potential improvements for future consideration:
1. Add histogram buckets for utilization distribution
2. Make update interval configurable
3. Add per-route utilization breakdown
4. Include queue time in utilization calculation
