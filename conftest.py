# conftest.py — pytest configuration for the project root.
#
# WHY this file exists:
# The ETL modules (p1e.py, p1g.py, etc.) live in the project root, not in a
# package directory.  pytest needs to find them when collecting tests/ .
# Placing a conftest.py at the root causes pytest to add the root directory
# to sys.path automatically — no manual sys.path.insert() needed in test files.
#
# This is the standard pytest approach for flat-layout projects (no src/ dir).
# Reference: https://docs.pytest.org/en/stable/explanation/goodpractices.html
