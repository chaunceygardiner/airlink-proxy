#!/usr/bin/python3

# Copyright (c) 2026 John A Kline
# See the file LICENSE for your full rights.

"""Shared test helpers.  Running a script from tests/ puts this directory on
sys.path, so `import fixtures` then works; this module puts
home/airlinkproxy/bin on the path for everything else.
"""

import os
import sys
import traceback

from typing import Any, Dict, Optional

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airlinkproxy', 'bin'))

from monitor.model import Reading

class bcolors:
    OKGREEN = '\033[92m'
    FAIL    = '\033[91m'
    ENDC    = '\033[0m'

def print_passed() -> None:
    print(bcolors.OKGREEN + 'PASSED' + bcolors.ENDC)

def print_failed(e: Exception) -> None:
    print(bcolors.FAIL + 'FAILED' + bcolors.ENDC)
    print(traceback.format_exc())

def float_eq(v1: Optional[float], v2: Optional[float]) -> bool:
    if v1 is None and v2 is None:
        return True
    elif v1 is None or v2 is None:
        return False
    else:
        return abs(v1 - v2) < 0.0001

def create_test_device_json(ts: int) -> Dict[str, Any]:
    """A well formed type 6 reading, as the AirLink itself would serve it."""
    return {
        'data': {
            'did' : '001D0A100214',
            'name': 'airlink',
            'ts'  : ts,
            'conditions': [{
                'lsid'                     : 347825,
                'data_structure_type'      : 6,
                'temp'                     : 74.1,
                'hum'                      : 62.8,
                'dew_point'                : 60.6,
                'wet_bulb'                 : 64.2,
                'heat_index'               : 74.7,
                'pm_1_last'                : 11,
                'pm_2p5_last'              : 14,
                'pm_10_last'               : 17,
                'pm_1'                     : 11.68,
                'pm_2p5'                   : 15.55,
                'pm_2p5_last_1_hour'       : 19.35,
                'pm_2p5_last_3_hours'      : 20.54,
                'pm_2p5_last_24_hours'     : 18.59,
                'pm_2p5_nowcast'           : 21.25,
                'pm_10'                    : 17.28,
                'pm_10_last_1_hour'        : 23.30,
                'pm_10_last_3_hours'       : 24.64,
                'pm_10_last_24_hours'      : 22.71,
                'pm_10_nowcast'            : 25.60,
                'last_report_time'         : ts,
                'pct_pm_data_last_1_hour'  : 100,
                'pct_pm_data_last_3_hours' : 100,
                'pct_pm_data_nowcast'      : 100,
                'pct_pm_data_last_24_hours': 100,
            }],
        },
        'error': None,
    }

def create_test_reading(ts: int) -> Reading:
    from monitor.model import reading_from_device_json
    return reading_from_device_json(create_test_device_json(ts))
