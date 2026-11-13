# Copyright 2023-2026 Airbus, CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Build a small fake Sentinel-1 IW SLC SAFE product from a real one, for the tests.

- keeps the real XML files (manifest, annotations, calibration, noise, RFI, schemas) of the kept swaths,
- replaces the measurement TIFF files by sparse tiled TIFF files with the same dimensions and data type:
  their tiles are not written (GDAL SPARSE_OK), so they are read as zeros and take a few KB.

It also keeps the real pixels of a 48x48 window at the center of each burst, to check the polarization of the
converted measurements.

The test product of this folder was built from the IW1 swath of
S1A_IW_SLC__1SDV_20260421T234638_20260421T234646_064184_08142A_7054 (Copernicus Data Space Ecosystem) with:

    python make_fake_safe.py <real SAFE> S1A_IW_SLC__1SDV_20260421T234638_20260421T234646_064184_08142A_7054.SAFE iw1

Usage: python make_fake_safe.py <real SAFE> <output SAFE> [<swath>...]
"""

import glob
import os
import re
import shutil
import sys

import rasterio
from lxml import etree
from rasterio.windows import Window

real, fake = sys.argv[1], sys.argv[2]
swaths = [s.lower() for s in sys.argv[3:]] or ["iw1"]
swath_re = re.compile(r"-(iw\d|ew\d)-")

if os.path.exists(fake):
    shutil.rmtree(fake)
for dirpath, _, filenames in os.walk(real):
    rel = os.path.relpath(dirpath, real)
    for filename in filenames:
        match = swath_re.search(filename)
        if match and match[1] not in swaths:
            continue  # other swaths
        if filename.endswith(".pdf") or filename.endswith(".png"):
            continue  # report and quicklooks are not read by EOPF
        src = os.path.join(dirpath, filename)
        dst = os.path.join(fake, rel, filename)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if filename.endswith(".tiff"):
            # Real pixels of a small window at the center of each burst, to test the polarization of the data
            annotation = glob.glob(os.path.join(real, "annotation", filename.replace(".tiff", ".xml")))[0]
            root = etree.parse(annotation).getroot()
            lines_per_burst = int(root.findtext("swathTiming/linesPerBurst"))
            bursts = len(root.findall("swathTiming/burstList/burst"))
            with rasterio.open(src) as tif:
                profile = tif.profile
                tags = tif.tags()
                gcps = tif.gcps
                windows = []
                for burst in range(bursts):
                    window = Window(tif.width // 2 - 16, burst * lines_per_burst + lines_per_burst // 2 - 16, 48, 48)
                    windows.append((window, tif.read(1, window=window)))
            profile.update(tiled=True, blockxsize=512, blockysize=512, compress="deflate", sparse_ok=True)
            with rasterio.open(dst, "w", **profile) as out:
                out.update_tags(**tags)
                if gcps[0]:
                    out.gcps = gcps
                for window, data in windows:
                    out.write(data, 1, window=window)
        else:
            shutil.copy2(src, dst)
print(f"Fake SAFE: {fake}")
