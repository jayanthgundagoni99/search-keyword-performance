"""Root conftest: adds code/ to sys.path so tests can import the package."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "code"))
