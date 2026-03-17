"""Shared filesystem paths for data scripts."""

import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

DATA_DIR = os.path.join(BASE_DIR, 'data')

RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
