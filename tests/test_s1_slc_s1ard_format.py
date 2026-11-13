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
Tests of the conversion of the Sentinel-1 SLC products in the S1 ARD format, with a real EOPF conversion.

The test product (tests/resources/s1_slc) is the IW1 swath of a real IW SLC SAFE product, with its real XML files,
but sparse measurement TIFF files: only a small window at the center of each burst contains real pixels.
"""

import glob
import os
from pathlib import Path
from typing import Any

import dask
import numpy as np
import pytest
import rasterio
import xarray as xr
import yaml

from rs_dpr_service import s1_slc_s1ard_format
from rs_dpr_service.safe_to_zarr import s1ard_format_hook

RESOURCES = Path(__file__).parent / "resources" / "s1_slc"
SAFE_NAME = "S1A_IW_SLC__1SDV_20260421T234638_20260421T234646_064184_08142A_7054"
SAFE = RESOURCES / f"{SAFE_NAME}.SAFE"
BURST_PREFIX = "S01SIWSLC_20260421T234638_0008_A367_7054_08142A"
BURSTS = [f"{BURST_PREFIX}_{pol}_IW1_{burst_id}" for pol in ("VH", "VV") for burst_id in (132825, 132826)]


@pytest.fixture(autouse=True)
def local_dask_scheduler():
    """Compute the lazy arrays with a local scheduler, whatever the Dask configuration left by the other tests."""
    with dask.config.set(scheduler="threads"):
        yield


@pytest.fixture(name="converted", scope="module")
def converted_fixture(tmp_path_factory) -> xr.DataTree:
    """Convert the test SAFE product with EOPF and the S1 ARD format hook, like safe_to_zarr.py does."""
    # pylint: disable=import-outside-toplevel
    from eopf.config import EOConfiguration
    from eopf.store.convert import convert

    # Don't start a Dask cluster in the tests
    EOConfiguration()["store__convert__use_multithreading"] = False
    zarr_path = str(tmp_path_factory.mktemp("conversion") / f"{SAFE_NAME}.zarr")
    # Use a local scheduler, whatever the Dask configuration left by the other tests
    with dask.config.set(scheduler="threads"), s1ard_format_hook(str(SAFE)) as s1ard_format:
        assert s1ard_format is True
        convert(str(SAFE), zarr_path, target_store_kwargs={"zarr_format": 2})
    return xr.open_datatree(zarr_path, engine="zarr", chunks={})


def check_structure(node, template: dict, path: str = "") -> list[str]:
    """
    Check a DataTree node against a S1 ARD structure template, like s1_ard.computing.structure.check_structure.
    Return the errors.
    """
    errors = []
    for key, value in template.get("attributes", {}).items():
        errors += check_attributes(node.attrs, {key: value}, path)
    for dim, spec in template.get("dimensions", {}).items():
        if dim not in node.dims:
            errors.append(f"Missing dimension: {path}/{dim}")
        elif spec and "size" in spec and node.sizes[dim] != spec["size"]:
            errors.append(f"Incorrect size for dimension {path}/{dim}")
        elif isinstance(node, xr.DataTree) and dim not in node.to_dataset().variables:
            # S1 ARD reads dt[dim]: the dimension must have a coordinate
            errors.append(f"Missing coordinate for dimension: {path}/{dim}")
    for name, child in template.get("coords", {}).items():
        if name not in node.coords:
            errors.append(f"Missing coordinate: {path}/{name}")
        else:
            errors += check_structure(node.coords[name], child, f"{path}/coords/{name}")
    for name, child in template.get("data_variables", {}).items():
        if name not in node.data_vars:
            errors.append(f"Missing data variable: {path}/{name}")
        else:
            errors += check_structure(node.data_vars[name], child, f"{path}/data_variables/{name}")
    for name, child in template.get("groups", {}).items():
        if name not in node.children:
            errors.append(f"Missing group: {path}/{name}")
        else:
            errors += check_structure(node.children[name], child, f"{path}/groups/{name}")
    return errors


def check_attributes(attrs, template: dict, path: str) -> list[str]:
    """Check the attributes, LIST<n> keys are list indexes."""
    errors = []
    for key, value in template.items():
        current = f"{path}/{key}"
        if key not in attrs:
            errors.append(f"Missing key: {current}")
            continue
        for sub_key, sub_value in (value or {}).items():
            if sub_key.startswith("LIST"):
                errors += check_attributes(attrs[key][int(sub_key[4:])], sub_value, current)
            else:
                errors += check_attributes(attrs[key], {sub_key: sub_value}, current)
    return errors


def test_is_s1_topsar_slc_safe():
    """Only the Sentinel-1 IW and EW SLC products are converted in the S1 ARD format."""
    assert s1_slc_s1ard_format.is_s1_topsar_slc_safe(f"s3://bucket/{SAFE_NAME}.SAFE/")
    assert s1_slc_s1ard_format.is_s1_topsar_slc_safe("S1D_EW_SLC__1SSH_20260417T161021_20260417T161037_002388.SAFE")
    assert not s1_slc_s1ard_format.is_s1_topsar_slc_safe("S1A_IW_GRDH_1SDV_20260421T234638_20260421T234646.SAFE")
    assert not s1_slc_s1ard_format.is_s1_topsar_slc_safe("S1A_S1_SLC__1SDV_20260421T234638_20260421T234646.SAFE")
    assert not s1_slc_s1ard_format.is_s1_topsar_slc_safe("S3A_OL_1_EFR____20260421T234638.SEN3")


def test_parse_processing_history():
    """The processing history is read from the manifest, indexed by product level and sorted by time."""
    history = s1_slc_s1ard_format.parse_processing_history(str(SAFE / "manifest.safe"))

    assert list(history) == ["Level-0 Product", "Level-1 Intermediate SLC Product", "Level-1 Product"]
    assert len(history["Level-0 Product"]) == 5
    intermediate = history["Level-1 Intermediate SLC Product"]
    assert len(intermediate) == 1
    assert intermediate[0]["facility"] == "ESA-S1A-PS"
    assert intermediate[0]["processor"] == "Sentinel-1 IPF"
    assert intermediate[0]["version"] == "004.03"
    assert intermediate[0]["time"] == "2026-04-22T09:01:07.000000Z"
    assert len(intermediate[0]["adfs"]) == 4
    assert len(intermediate[0]["inputs"]) == 6
    for entries in history.values():
        assert [entry["time"] for entry in entries] == sorted(entry["time"] for entry in entries)


def test_converted_bursts(converted):
    """There is one burst per polarization, named like with CPM 2.x."""
    assert sorted(converted.children) == sorted(BURSTS)
    for burst in converted.children.values():
        for node in burst.subtree:
            assert "polarization" not in node.dims, node.path


def test_s1ard_input_structure(converted):
    """Each burst has the structure checked by the S1 ARD processor."""
    template = yaml.safe_load((RESOURCES / "S1_L1_SLC.yaml").read_text(encoding="utf-8"))
    for name, burst in converted.children.items():
        assert check_structure(burst, template, name) == []


def test_polarization_of_the_measurements(converted):
    """
    The measurements of each burst are the pixels of its polarization.

    CPM 3.0.0 writes the VH pixels of this product under the VV label: the transformation must fix it.
    """
    for name, burst in converted.children.items():
        pol = name.split("_")[-3]
        slc = burst["measurements"].to_dataset()["slc"]
        lines = burst["measurements"].to_dataset()["line"].values
        rows, cols = slc.sizes["azimuth_time"] // 2, slc.sizes["slant_range_time"] // 2
        window = slc.isel(azimuth_time=slice(rows, rows + 16), slant_range_time=slice(cols, cols + 16)).values
        assert np.count_nonzero(window)
        tiff = glob.glob(str(SAFE / "measurement" / f"s1a-iw1-slc-{pol.lower()}-*.tiff"))[0]
        with rasterio.open(tiff) as tif:
            expected = tif.read(1, window=((lines[rows], lines[rows] + 16), (cols, cols + 16)))
        np.testing.assert_array_equal(window, expected)


def test_noise_luts_of_the_polarization(converted):
    """The noise LUTs are the ones of the polarization of the burst."""
    for name, burst in converted.children.items():
        pol = name.split("_")[-3].lower()
        noise_file = glob.glob(str(SAFE / "annotation" / "calibration" / f"noise-s1a-iw1-slc-{pol}-*.xml"))[0]
        expected_range = s1_slc_s1ard_format._read_noise_range_lut(noise_file)  # pylint: disable=protected-access
        np.testing.assert_array_equal(
            burst["quality/noise_range"].to_dataset()["noise_range_lut"].values,
            expected_range,
        )
        noise_azimuth = burst["quality/noise_azimuth"].to_dataset()
        assert noise_azimuth["noise_azimuth_lut"].dims == ("line",)
        assert noise_azimuth["swath"].item() == "IW1"


def test_line_and_pixel_coordinates(converted):
    """The line and pixel coordinates are the indexes in the measurement TIFF files, like with CPM 2.x."""
    for burst_id, first_line in ((132825, 0), (132826, 1496)):
        measurements = converted[f"{BURST_PREFIX}_VV_IW1_{burst_id}/measurements"].to_dataset()
        np.testing.assert_array_equal(measurements["line"].values, np.arange(first_line, first_line + 1496))
        np.testing.assert_array_equal(measurements["pixel"].values, np.arange(measurements.sizes["slant_range_time"]))
    gcp = converted[f"{BURST_PREFIX}_VV_IW1_132825/conditions/gcp"].to_dataset()
    assert gcp["line"].values.tolist() == [0, 1496, 2991]
    burst_info = converted[f"{BURST_PREFIX}_VV_IW1_132825/conditions/burst_info"].to_dataset()
    assert burst_info["first_valid_sample"].sizes["azimuth_time"] == 1496


def test_attributes(converted):
    """The attributes of each burst are the ones of CPM 2.x, with the format of the product."""
    for name, burst in converted.children.items():
        pol = name.split("_")[-3]
        properties = burst.attrs["stac_discovery"]["properties"]
        assert properties["sar:polarization"] == pol
        for key, value in s1_slc_s1ard_format.S1ARD_FORMAT_PROPERTIES.items():
            assert properties[key] == value
        other_metadata = burst.attrs["other_metadata"]
        assert other_metadata["burst_id"] == name.split("_")[-1]
        assert other_metadata["image_number"] == ("001" if pol == "VH" else "004")
        assert other_metadata["image_annotation"]["image_information"]["slice_number"]
        assert set(other_metadata["processing_information"]) == {
            "azimuth_processing",
            "input_dimensions",
            "processor_scaling_factor",
            "range_processing",
        }
        assert "t_0" in other_metadata["general_annotation"]["azimuth_fm_rate_list"][0]
        assert other_metadata["swath_timing"]["lines_per_burst"] == 1496
        assert burst.attrs["other_metadata_rfi"]["radio_frequency_interference"]["rfi_detection_from_noise_report"]
        history = burst.attrs["processing_history"]
        assert history["Level-1 Intermediate SLC Product"][0]["processor"] == "Sentinel-1 IPF"
        # EOPF convert() adds its entry to the latest level
        assert history["Level-1 Product"][-1]["processor"] == "EOPF-CPM"
    # The format is also written in the properties of the root, used for the catalog item
    root_properties = converted.attrs["stac_discovery"]["properties"]
    assert root_properties["rspy:product_format"] == "s1ard-1.2.0"
    assert root_properties["rspy:product_format_cpm_issue"] == s1_slc_s1ard_format.CPM_ISSUE_URL
    assert "NOT compatible" in root_properties["rspy:product_format_description"]


def test_build_other_metadata_renames_t0():
    """CPM 2.x writes the t_0 key, CPM 3.0.0 writes t0."""
    annotation: dict[str, Any] = {
        "general_annotation": {
            "product_information": {
                "timeliness_category": "NRT",
                "projection": "Slant Range",
                "range_sampling_rate": 1,
            },
            "azimuth_fm_rate_list": [{"t0": 0.5}],
        },
        "image_annotation": {
            "processing_information": {
                "swath_proc_params_list": [{"swath": "IW1", "processor_scaling_factor": 2.0}],
                "input_dimensions_list": [{"swath": "IW1", "number_of_input_lines": 3}],
            },
        },
        "quality_information": {},
        "burst_timing": {"lines_per_burst": 4},
        "swath_merging": {},
    }
    metadata = s1_slc_s1ard_format.build_other_metadata(annotation, {"image_number": "001", "other": 1}, "42")

    assert metadata["general_annotation"]["azimuth_fm_rate_list"] == [{"t_0": 0.5}]
    assert metadata["processing_information"] == {
        "processor_scaling_factor": 2.0,
        "input_dimensions": {"number_of_input_lines": 3},
    }
    assert metadata["swath_timing"] == {"lines_per_burst": 4}
    assert metadata["image_number"] == "001"
    assert "other" not in metadata
    assert metadata["burst_id"] == "42"
    # The input is not modified
    assert annotation["general_annotation"]["azimuth_fm_rate_list"] == [{"t0": 0.5}]


def test_unexpected_burst_name():
    """A burst name that doesn't end with the swath and burst id is rejected."""
    with pytest.raises(ValueError, match="Unexpected Sentinel-1 SLC burst name"):
        # pylint: disable=protected-access
        s1_slc_s1ard_format._split_burst("unexpected", xr.DataTree(), None)  # type: ignore[arg-type]


def test_data_polarizations_unknown_without_pixels(tmp_path):
    """The labels of CPM 3.0.0 are kept when the pixels don't allow to know the polarization of the data."""
    profile = {"driver": "GTiff", "width": 64, "height": 64, "count": 1, "dtype": "complex_int16"}
    tiffs = {}
    for pol in ("VV", "VH"):
        tiffs[("IW1", pol)] = str(tmp_path / f"{pol}.tiff")
        with rasterio.open(tiffs[("IW1", pol)], "w", **profile):
            pass  # zeros
    slc = xr.DataArray(
        np.zeros((2, 64, 64), dtype=np.complex64),
        dims=("polarization", "azimuth_time", "slant_range_time"),
    )
    # pylint: disable=protected-access
    assert s1_slc_s1ard_format._data_polarizations(slc, tiffs, "IW1", 0) is None
    # With different pixels, the polarization of each index is found
    with rasterio.open(tiffs[("IW1", "VH")], "r+") as tif:
        tif.write(np.full((64, 64), 3 + 4j, dtype=np.complex64), 1)
    slc[0] = 3 + 4j
    assert s1_slc_s1ard_format._data_polarizations(slc, tiffs, "IW1", 0) == ["VH", "VV"]


def test_safe_files_are_found():
    """The XML and TIFF files of the test product are indexed by swath and polarization."""
    annotations = s1_slc_s1ard_format.SafeAnnotations(str(SAFE))
    for files in (annotations.annotations, annotations.calibrations, annotations.noises, annotations.rfis):
        assert set(files) == {("IW1", "VV"), ("IW1", "VH")}
    assert set(annotations.measurements) == {("IW1", "VV"), ("IW1", "VH")}
    assert all(os.path.isfile(path) for path in annotations.measurements.values())
