"""Postgres side of nmea2s3 — the archive as a queryable wide table.

Its own subpackage because it is the only part that needs psycopg, nmea2000
and pynmea2. They are ordinary dependencies, so one install gives you every
command, but nothing in the capture path imports this package and the logger
process never loads any of it — which is what actually matters for the one
service that has to start unattended and stay up.

The install-time cost is real though, and it lands on the boat: an SBC needs
a wheel for its platform for each of those, or a compiler to build them.
"""
