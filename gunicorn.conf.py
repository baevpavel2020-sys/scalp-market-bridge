"""Gunicorn runtime settings for long-running Scan+ requests.

Keep a single worker on the small Render instance; the Scan+ orchestrator
already parallelizes provider calls internally.
"""
timeout = 180
graceful_timeout = 30
workers = 1
