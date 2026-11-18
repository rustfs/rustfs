#!/usr/bin/env python3
"""Exercise the candidate-based replacement for the former release trigger."""
import importlib
import sys
from pathlib import Path
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent / "release"))
module = importlib.import_module("test_pipeline")

if __name__ == "__main__":
    unittest.main(module=module, defaultTest=('HelmWorkflowTests',))
