import os
import sys

# Make `cib_rank` and `scripts` importable when pytest runs from the repo root.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
