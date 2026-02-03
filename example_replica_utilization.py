#!/usr/bin/env python
"""
Example script demonstrating the ray_serve_replica_utilization metric.

This script creates a simple Ray Serve deployment and shows how the utilization
metric tracks how efficiently the replica is using its configured concurrency.
"""

import asyncio
import time

from ray import serve


@serve.deployment(max_ongoing_requests=4)
class WorkerDeployment:
    """A simple deployment that simulates work."""

    async def __call__(self, duration: float = 0.5):
        """Process a request by sleeping for the given duration."""
        await asyncio.sleep(duration)
        return f"Processed request in {duration}s"


def main():
    """Run the example and print instructions."""
    # Start Ray Serve
    serve.start()

    # Deploy the worker
    handle = serve.run(WorkerDeployment.bind(), name="worker_app")

    print("=" * 80)
    print("Ray Serve Replica Utilization Metric Example")
    print("=" * 80)
    print()
    print("Deployment Configuration:")
    print("  - max_ongoing_requests: 4")
    print("  - Request duration: 0.5s")
    print()
    print("The replica utilization metric measures:")
    print(
        "  utilization = total_user_code_time / (wall_clock_duration * max_ongoing_requests)"
    )
    print()
    print("View metrics at: http://localhost:9999/metrics")
    print("Look for: ray_serve_replica_utilization")
    print()
    print("=" * 80)
    print()

    # Make some requests
    print("Sending 4 concurrent requests (should reach ~50% utilization)...")
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(lambda: handle.remote(0.5).result()) for _ in range(4)
        ]
        for future in futures:
            future.result()

    print("Requests completed. Waiting for metrics to update...")
    time.sleep(5)

    print()
    print("Sending 8 concurrent requests (should reach ~100% utilization)...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(lambda: handle.remote(0.5).result()) for _ in range(8)
        ]
        for future in futures:
            future.result()

    print("Requests completed. Waiting for metrics to update...")
    time.sleep(5)

    print()
    print("=" * 80)
    print("Check the Prometheus metrics endpoint to see the utilization values:")
    print("  curl http://localhost:9999/metrics | grep serve_replica_utilization")
    print()
    print("Expected output example:")
    print(
        '  serve_replica_utilization{deployment="WorkerDeployment",replica="...",application="worker_app"} 0.75'
    )
    print()
    print("A value of 1.0 means the replica is fully saturated.")
    print("A value of 0.5 means the replica is using 50% of its capacity.")
    print("=" * 80)


if __name__ == "__main__":
    main()
