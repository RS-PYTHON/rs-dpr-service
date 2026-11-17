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
Convert the Sentinel-1 TOPSAR SLC products read by CPM 3.0.0 into the format expected by the S1 ARD processor.

The S1 ARD processor (s1-ard 1.2.0) runs with CPM 3.0.0rc4 and validates its SLC inputs against the Zarr
layout written by CPM 2.x: one sub-product per burst AND per polarization, flat ``other_metadata``,
``sar:polarization``, a ``processing_history`` indexed by product level, and ``line``/``pixel`` coordinates.

Since CPM 3.0.0, the SAFE conversion joins both polarizations of each burst along a ``polarization``
dimension (CPM issue #1042: https://gitlab.eopf.copernicus.eu/cpm/eopf-cpm/-/issues/1042), splits
``other_metadata`` by polarization and drops several annotation attributes. Its processing history parser
is also broken (only one "Unknown" entry). So S1 ARD rejects the SLC products converted by CPM 3.0.0.

CPM 3.0.0 also mixes up the polarizations of the data (e.g. the 'VV' label contains the VH pixels and noise LUTs):
the real polarization of the measurements is found from their pixels, and the noise LUTs are read from the XML files.

This module rebuilds the S1 ARD layout from the DataTree read by CPM 3.0.0 (before it is written, lazily,
without copying the measurement data) and from the XML and TIFF files of the SAFE product.
The result is NOT compatible with the CPM 3.0.0 SLC model: this is written in its STAC metadata.

This module runs in the EOPF Dask worker (see safe_to_zarr.py), where eopf, lxml and xarray are installed.
"""

import copy
import datetime
import glob
import logging
import os
import re
from typing import Any

import numpy as np
import rasterio
import xarray as xr
from lxml import (
    etree,  # nosec B410 (only local SAFE files from the conversion input are parsed)
)

logger = logging.getLogger(__name__)

#: CPM issue that changed the Sentinel-1 SLC layout and broke the compatibility with S1 ARD
CPM_ISSUE_URL = "https://gitlab.eopf.copernicus.eu/cpm/eopf-cpm/-/issues/1042"

#: STAC properties added to the root and to each burst of the converted product
S1ARD_FORMAT_PROPERTIES = {
    "rspy:product_format": "s1ard-1.2.0",
    "rspy:product_format_cpm_compatible": "3.0.0rc4",
    "rspy:product_format_cpm_incompatible": "3.0.0",
    "rspy:product_format_cpm_issue": CPM_ISSUE_URL,
    "rspy:product_format_description": (
        "Sentinel-1 SLC layout expected by the S1 ARD 1.2.0 processor, which runs with CPM 3.0.0rc4: one "
        "sub-product per burst and per polarization (CPM 2.x / 3.0.0rc4 layout). It is NOT compatible with "
        "the CPM 3.0.0 SLC model, which joins both polarizations of each burst along a 'polarization' "
        f"dimension since CPM issue #1042 ({CPM_ISSUE_URL})."
    ),
}

#: Legacy Sentinel-1 TOPSAR SLC SAFE product name, e.g. S1A_IW_SLC__1SDV_20260421T234638_..._7054.SAFE
S1_TOPSAR_SLC_SAFE_PATTERN = re.compile(r"^S1[A-D]_(IW|EW)_SLC__1S[A-Z]{2}_.*")

# Swath and polarization in the SAFE XML file names, e.g. s1a-iw1-slc-vh-20260421t234638-...-001.xml
_XML_FILE_PATTERN = re.compile(r"(?:^|-)((?:iw|ew)\d)-slc-(vv|vh|hh|hv)-")

# Burst name written by CPM 3.0.0, e.g. S01SIWSLC_20260421T234638_0008_A367_7054_08142A_IW1_132825
_BURST_NAME_PATTERN = re.compile(r"^(?P<prefix>.+)_(?P<swath>(?:IW|EW)\d)_(?P<burst_id>\d+)$")

# XML namespace of the SAFE manifest (an identifier, not an URL that is called)
_SAFE_NAMESPACE = "http://www.esa.int/safe/sentinel-1.0"  # NOSONAR
_UNKNOWN = "Unknown"
_UNKNOWN_TIME = "1970-01-01T00:00:00.000000Z"
_HISTORY_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
_AUX_MARKER = "AUX"
_LEVEL_ROLE_PATTERN = re.compile(r"^Level-\d.*Product$")
_LEVEL_NAME_PATTERNS = (re.compile(r".*S1[A-D]_[A-Z0-9]{2}_[A-Z0-9_]{4}_(\d).*"), re.compile(r".*[Ll]evel.?(\d).*"))


def is_s1_topsar_slc_safe(safe_uri: str) -> bool:
    """Return True if the SAFE product is a Sentinel-1 IW or EW SLC product."""
    return bool(S1_TOPSAR_SLC_SAFE_PATTERN.match(os.path.basename(str(safe_uri).rstrip("/"))))


#####################
# Processing history #
#####################


def _local_name(node: Any) -> str:
    """Local XML tag name without namespace, or "" for comments and processing instructions."""
    return etree.QName(node).localname if isinstance(node.tag, str) else ""


def _child(node: Any, name: str) -> Any:
    """First child element with the given local name, or None."""
    return next((child for child in node if _local_name(child) == name), None)


def _level_from_names(names: list[str]) -> str | None:
    """Deduce the product level from the name of a product, e.g. S1A_IW_RAW__0SDV_... -> Level-0 Product"""
    for pattern in _LEVEL_NAME_PATTERNS:
        if names and (match := pattern.match(names[0])):
            return f"Level-{match[1]} Product"
    return None


def _history_facility(node: Any, entry: dict[str, Any]) -> None:
    """Read the processor, version and facility of a safe:processing node into its history entry."""
    if (facility := _child(node, "facility")) is None:
        return
    if (software := _child(facility, "software")) is not None:
        entry["processor"] = software.attrib.get("name", _UNKNOWN)
        entry["version"] = software.attrib.get("version", _UNKNOWN)
    if parts := [facility.attrib[key] for key in ("organisation", "name") if key in facility.attrib]:
        entry["facility"] = "-".join(parts)


def _history_time(node: Any) -> str:
    """Processing stop time of a safe:processing node, or the unknown time marker, like CPM."""
    time = node.attrib.get("stop", "")
    time = time if time.endswith("Z") else time + "Z"
    try:
        datetime.datetime.strptime(time, _HISTORY_TIME_FORMAT)
    except ValueError:
        return _UNKNOWN_TIME
    return time


def _history_entry(node: Any) -> dict[str, Any]:
    """Read the processing history entry of a safe:processing node."""
    entry: dict[str, Any] = {
        "processor": _UNKNOWN,
        "version": _UNKNOWN,
        "facility": _UNKNOWN,
        "time": _history_time(node),
        "inputs": [],
        "outputs": [],
    }
    _history_facility(node, entry)
    resources = [child for child in node if _local_name(child) == "resource" and "role" in child.attrib]
    names = [resource.attrib.get("name", "") for resource in resources]
    # All the auxiliary data files have an AUX marker in their name
    if adfs := [name for name in names if _AUX_MARKER in name]:
        entry["adfs"] = adfs
    entry["inputs"] = [name for name in names if _AUX_MARKER not in name]
    parent = node.getparent()
    if parent is not None and _local_name(parent) == "resource" and "name" in parent.attrib:
        entry["outputs"] = [parent.attrib["name"]]
    return entry


def _history_level(node: Any, entry: dict[str, Any], is_product: bool) -> str:
    """Product level of a processing history entry."""
    if is_product:
        # The first processing is the one of the product itself: read its level from the manifest version
        version = node.getroottree().getroot().attrib.get("version", "")
        match = re.match(r".*[Ll]evel-(\d).*", version)
        return f"Level-{match[1]} Product" if match else _UNKNOWN
    parent = node.getparent()
    role = parent.attrib.get("role", "") if parent is not None else ""
    if _LEVEL_ROLE_PATTERN.match(role):
        return role
    if role == "Raw Data":
        return "Level-0 Product"
    return _level_from_names(entry["outputs"]) or _level_from_names(entry["inputs"]) or _UNKNOWN


def parse_processing_history(manifest_path: str) -> dict[str, list[dict[str, Any]]]:
    """
    Read the processing history of a Sentinel-1 SAFE manifest, indexed by product level.

    This implements what the CPM 3.0.0 ToProcessingHistoryS01 formatter is meant to do: the CPM version
    tests XML attributes with `"name" in node` (which looks for child elements in lxml), so it never
    descends into the processing tree and returns a single "Unknown" entry dated 1970.
    S1 ARD reads processing_history["Level-1 Intermediate SLC Product"][0].
    """
    root = etree.parse(manifest_path).getroot()  # nosec B320 (local SAFE manifest of the conversion input)
    nodes = root.xpath(
        "metadataSection/metadataObject[@ID='processing']/metadataWrap/xmlData/safe:processing",
        namespaces={"safe": _SAFE_NAMESPACE},
    )
    history: dict[str, list[dict[str, Any]]] = {}

    def walk(node: Any, is_product: bool):
        if _local_name(node) == "processing":
            entry = _history_entry(node)
            history.setdefault(_history_level(node, entry, is_product), []).append(entry)
            is_product = False
        for child in node:
            # The processing history of the auxiliary data files is not of interest
            if isinstance(child.tag, str) and _AUX_MARKER not in child.attrib.get("name", _AUX_MARKER):
                walk(child, is_product)

    for node in nodes:
        walk(node, True)

    # Like CPM 2.x: entries sorted by time, and levels sorted by their first entry. EOPF convert() then adds the
    # SAFE product name and the CPM entry to the latest entry.
    for entries in history.values():
        entries.sort(key=lambda entry: entry["time"])
    return dict(sorted(history.items(), key=lambda item: item[1][0]["time"]))


#######################
# SAFE XML annotations #
#######################


def _xml_files(safe_path: str, pattern: str) -> dict[tuple[str, str], str]:
    """SAFE XML files indexed by (swath, polarization), e.g. ("IW1", "VH")."""
    files = {}
    for path in sorted(glob.glob(os.path.join(safe_path, pattern))):
        if match := _XML_FILE_PATTERN.search(os.path.basename(path)):
            files[(match[1].upper(), match[2].upper())] = path
    return files


def _ints(text: str | None) -> np.ndarray:
    """Space separated integers of an XML element."""
    return np.array((text or "").split(), dtype=np.int32)


def _read_vectors(path: str, vector_path: str) -> tuple[np.ndarray, np.ndarray]:
    """Line of each vector, and pixels of the first vector, of a calibration or noise range XML file."""
    root = etree.parse(path).getroot()  # nosec B320
    vectors = root.findall(vector_path)
    lines = np.array([int(vector.findtext("line")) for vector in vectors], dtype=np.int32)
    pixels = _ints(vectors[0].findtext("pixel")) if vectors else np.array([], dtype=np.int32)
    return lines, pixels


def _read_gcp_grid(path: str) -> tuple[np.ndarray, np.ndarray]:
    """Lines and pixels of the geolocation grid of an annotation XML file."""
    root = etree.parse(path).getroot()  # nosec B320
    points = root.findall("geolocationGrid/geolocationGridPointList/geolocationGridPoint")
    lines = sorted({int(point.findtext("line")) for point in points})
    pixels = sorted({int(point.findtext("pixel")) for point in points})
    return np.array(lines, dtype=np.int32), np.array(pixels, dtype=np.int32)


def _read_noise_azimuth(path: str) -> xr.Dataset:
    """Azimuth noise vector of a noise XML file, as written by CPM 2.x (one swath per file in TOPSAR SLC)."""
    root = etree.parse(path).getroot()  # nosec B320
    vector = root.find("noiseAzimuthVectorList/noiseAzimuthVector")
    lut = np.array(vector.findtext("noiseAzimuthLut").split(), dtype=np.float32)
    return xr.Dataset(
        data_vars={"noise_azimuth_lut": ("line", lut)},
        coords={
            "line": ("line", _ints(vector.findtext("line"))),
            "swath": vector.findtext("swath"),
            "first_azimuth_line": np.int32(vector.findtext("firstAzimuthLine")),
            "first_range_sample": np.int32(vector.findtext("firstRangeSample")),
            "last_azimuth_line": np.int32(vector.findtext("lastAzimuthLine")),
            "last_range_sample": np.int32(vector.findtext("lastRangeSample")),
        },
    )


def _read_image_number(path: str) -> str | None:
    """Image number of an annotation XML file, e.g. "001"."""
    return etree.parse(path).getroot().findtext("adsHeader/imageNumber")  # nosec B320


def _read_noise_range_lut(path: str) -> np.ndarray | None:
    """Range noise LUT of a noise XML file (azimuth_time, slant_range_time), or None if its vectors differ in size."""
    root = etree.parse(path).getroot()  # nosec B320
    luts = [
        vector.findtext("noiseRangeLut").split() for vector in root.findall("noiseRangeVectorList/noiseRangeVector")
    ]
    if not luts or len({len(lut) for lut in luts}) != 1:
        return None
    return np.array(luts, dtype=np.float32)


def _read_rfi(path: str) -> dict[str, Any]:
    """CPM 2.x other_metadata_rfi attribute, from a RFI XML file."""
    root = etree.parse(path).getroot()  # nosec B320

    def values(report_path: str, field: str, as_float: bool = False) -> list:
        texts = [report.findtext(field) for report in root.findall(report_path)]
        return [float(text) for text in texts] if as_float else texts

    noise_report = "rfiDetectionFromNoiseReportList/rfiDetectionFromNoiseReport"
    burst_report = "rfiBurstReportList/rfiBurstReport"
    return {
        "radio_frequency_interference": {
            "rfi_burst_report": {
                "azimuth_time": values(burst_report, "azimuthTime"),
                "frequency_domain": {},
                "in_band_out_band_power_ratio": values(burst_report, "inBandOutBandPowerRatio", True),
                "time_domain": {},
            },
            "rfi_detection_from_noise_report": {
                "max_fisher_z": values(noise_report, "maxFisherZ", True),
                "max_kl_divergence": values(noise_report, "maxKLDivergence", True),
                "max_rfi_psd": values(noise_report, "maxRfiPsd", True),
                "noise_sensing_time": values(noise_report, "noiseSensingTime"),
                "rfi_detected": values(noise_report, "rfiDetected"),
            },
        },
    }


class SafeAnnotations:
    """XML annotations of a Sentinel-1 TOPSAR SLC SAFE product, read once for all the bursts."""

    def __init__(self, safe_path: str):
        # pylint: disable=import-outside-toplevel
        from eopf.store.product_specific.S01 import parse_annotations

        self.safe_path = safe_path
        self.history = parse_processing_history(os.path.join(safe_path, "manifest.safe"))
        # Annotation metadata, before CPM 3.0.0 deletes its "deprecated" attributes
        self.metadata = {}
        for path, metadata in parse_annotations(safe_path).items():
            if match := _XML_FILE_PATTERN.search(os.path.basename(path)):
                self.metadata[(match[1].upper(), match[2].upper())] = metadata
        self.annotations = _xml_files(safe_path, "annotation/s1*.xml")
        self.calibrations = _xml_files(safe_path, "annotation/calibration/calibration-*.xml")
        self.noises = _xml_files(safe_path, "annotation/calibration/noise-*.xml")
        self.rfis = _xml_files(safe_path, "annotation/rfi/rfi-*.xml")
        self.measurements = _xml_files(safe_path, "measurement/s1*.tiff")


def _rename_keys(struct: Any, renames: dict[str, str]) -> Any:
    """Recursively rename dict keys."""
    if isinstance(struct, dict):
        return {renames.get(key, key): _rename_keys(value, renames) for key, value in struct.items()}
    if isinstance(struct, list):
        return [_rename_keys(value, renames) for value in struct]
    return struct


def build_other_metadata(
    annotation: dict[str, Any],
    cpm_metadata: dict[str, Any],
    burst_id: str,
    image_number: str | None = None,
) -> dict[str, Any]:
    """
    Rebuild the CPM 2.x other_metadata of one burst and polarization.

    Parameters:
        annotation: metadata of the annotation XML file, from eopf parse_annotations()
        cpm_metadata: other_metadata[<polarization>] written by CPM 3.0.0
        burst_id: burst identifier
        image_number: image number of the annotation XML file. CPM 3.0.0 writes the same image number for all the
            polarizations, so it is read from the XML file.
    """
    annotation = _rename_keys(copy.deepcopy(annotation), {"t0": "t_0"})
    product_information = annotation["general_annotation"]["product_information"]
    processing = annotation["image_annotation"]["processing_information"]
    swath_params = {key: value for key, value in processing["swath_proc_params_list"][0].items() if key != "swath"}
    input_dimensions = {key: value for key, value in processing["input_dimensions_list"][0].items() if key != "swath"}
    metadata: dict[str, Any] = {
        key: cpm_metadata[key] for key in ("eopf_category", "title", "image_number") if key in cpm_metadata
    }
    if image_number is not None:
        metadata["image_number"] = image_number
    metadata.update(
        {
            "timeliness_category": product_information["timeliness_category"],
            "projection": product_information["projection"],
            "range_sampling_rate": product_information["range_sampling_rate"],
            "processing_information": {**swath_params, "input_dimensions": input_dimensions},
            "quality_information": annotation["quality_information"],
            "general_annotation": annotation["general_annotation"],
            "image_annotation": annotation["image_annotation"],
            "swath_timing": annotation["burst_timing"],
            "swath_merging": annotation["swath_merging"],
            "burst_id": burst_id,
        },
    )
    return metadata


##################
# DataTree layout #
##################


def _group(node: xr.DataTree, path: str) -> xr.Dataset:
    """Dataset of a group of a DataTree, without the inherited coordinates."""
    group = node[path]
    if not isinstance(group, xr.DataTree):
        raise TypeError(f"{path!r} is not a group of {node.name!r}")
    return group.to_dataset(inherit=False)


def _select_polarization(dataset: xr.Dataset, index: int) -> xr.Dataset:
    """Keep one polarization of a dataset, and remove the polarization dimension and coordinate."""
    if "polarization" in dataset.dims:
        dataset = dataset.isel(polarization=index, drop=True)
    return dataset.drop_vars("polarization", errors="ignore")


def _with_line_pixel(dataset: xr.Dataset, lines: np.ndarray, pixels: np.ndarray) -> xr.Dataset:
    """Add the CPM 2.x line and pixel coordinates along the azimuth_time and slant_range_time dimensions."""
    return dataset.assign_coords(line=("azimuth_time", lines), pixel=("slant_range_time", pixels))


def _burst_info(measurements: xr.Dataset, burst: dict[str, Any]) -> xr.Dataset:
    """CPM 2.x burst_info group: first and last valid sample of each line of the burst."""

    def samples(value: Any) -> np.ndarray:
        # A list decoded from the XML schema, or the space separated values of the XML element
        values = value if isinstance(value, list) else str(value).split()
        return np.array(values, dtype=np.float64).astype(np.int64)

    return xr.Dataset(
        data_vars={key: ("azimuth_time", samples(burst[key])) for key in ("first_valid_sample", "last_valid_sample")},
        coords={"azimuth_time": measurements["azimuth_time"].values},
    )


def _data_polarizations(
    slc: xr.DataArray,
    measurements: dict[tuple[str, str], str],
    swath: str,
    first_line: int,
) -> list[str] | None:
    """
    Real polarization of each index of the polarization dimension of the measurements.

    CPM 3.0.0 can write the measurements of a polarization under the label of the other one (seen with
    S1A_IW_SLC__1SDV_20260421T234638_20260421T234646_064184_08142A_7054: the 'VV' label contains the VH pixels).
    So we compare a small window at the center of the burst with the measurement TIFF file of each polarization.

    Returns:
        The polarization of each index, or None if the window doesn't allow to know it (e.g. same pixels).
    """
    size = 16
    row = slc.sizes["azimuth_time"] // 2
    col = slc.sizes["slant_range_time"] // 2
    windows = {}
    for (tiff_swath, pol), path in measurements.items():
        if tiff_swath == swath:
            with rasterio.open(path) as tiff:
                windows[pol] = tiff.read(1, window=((first_line + row, first_line + row + size), (col, col + size)))
    polarizations = []
    for index in range(slc.sizes["polarization"]):
        window = slc.isel(
            polarization=index,
            azimuth_time=slice(row, row + size),
            slant_range_time=slice(col, col + size),
        ).values
        matches = [pol for pol, tiff_window in windows.items() if np.array_equal(window, tiff_window)]
        if len(matches) != 1:
            return None
        polarizations.append(matches[0])
    return polarizations if len(set(polarizations)) == len(polarizations) else None


def _split_burst(
    name: str,
    burst: xr.DataTree,
    annotations: SafeAnnotations,
) -> dict[str, xr.DataTree]:
    """Split a CPM 3.0.0 burst into one CPM 2.x burst per polarization."""
    if not (match := _BURST_NAME_PATTERN.match(name)):
        raise ValueError(f"Unexpected Sentinel-1 SLC burst name: {name!r}")
    prefix, swath, burst_id = match["prefix"], match["swath"], match["burst_id"]
    labels = [str(pol) for pol in burst["measurements"].ds["polarization"].values]

    # Line of the burst in the measurement TIFF files
    swath_timing = annotations.metadata[(swath, labels[0])]["burst_timing"]
    burst_index = next(
        i for i, item in enumerate(swath_timing["burst_list"]) if str(item.get("burst_id", i)) == burst_id
    )
    first_line = burst_index * int(swath_timing["lines_per_burst"])

    # Index of the data of each polarization: don't trust the CPM 3.0.0 labels
    data_polarizations = _data_polarizations(
        burst["measurements"].ds["slc"],
        annotations.measurements,
        swath,
        first_line,
    )
    if data_polarizations is None:
        data_polarizations = labels
    elif data_polarizations != labels:
        logger.warning(
            f"{name}: CPM 3.0.0 wrote the measurements of the {data_polarizations} polarizations under the "
            f"{labels} labels. They are written with their real polarization.",
        )

    bursts = {}
    for index, pol in enumerate(data_polarizations):
        key = (swath, pol)
        metadata = annotations.metadata[key]
        node = burst.map_over_datasets(lambda dataset, i=index: _select_polarization(dataset, i))

        # Measurements: line index in the whole swath, pixel index in the line
        measurements = _group(node, "measurements")
        node["measurements"] = xr.DataTree(
            _with_line_pixel(
                measurements,
                np.arange(first_line, first_line + measurements.sizes["azimuth_time"], dtype=np.int64),
                np.arange(measurements.sizes["slant_range_time"], dtype=np.int64),
            ),
        )
        node["conditions/burst_info"] = xr.DataTree(
            _burst_info(measurements, metadata["burst_timing"]["burst_list"][burst_index]),
        )
        node["conditions/gcp"] = xr.DataTree(
            _with_line_pixel(
                _group(node, "conditions/gcp"),
                *_read_gcp_grid(annotations.annotations[key]),
            ),
        )
        calibration = _group(node, "quality/calibration")
        node["quality/calibration"] = xr.DataTree(
            _with_line_pixel(
                calibration,
                *_read_vectors(annotations.calibrations[key], "calibrationVectorList/calibrationVector"),
            ),
        )
        # The noise LUTs are read from the XML files, like CPM 2.x: CPM 3.0.0 can swap their polarizations
        noise_range = _group(node, "quality/noise_range")
        lut = _read_noise_range_lut(annotations.noises[key])
        if lut is not None and lut.shape == noise_range["noise_range_lut"].shape:
            noise_range["noise_range_lut"] = noise_range["noise_range_lut"].copy(data=lut)
        node["quality/noise_range"] = xr.DataTree(
            _with_line_pixel(
                noise_range,
                *_read_vectors(annotations.noises[key], "noiseRangeVectorList/noiseRangeVector"),
            ),
        )
        node["quality/noise_azimuth"] = xr.DataTree(_read_noise_azimuth(annotations.noises[key]))

        attrs = copy.deepcopy(burst.attrs)
        attrs["other_metadata"] = build_other_metadata(
            metadata,
            attrs["other_metadata"][pol],
            burst_id,
            _read_image_number(annotations.annotations[key]),
        )
        if key in annotations.rfis:
            attrs["other_metadata_rfi"] = _read_rfi(annotations.rfis[key])
        attrs["stac_discovery"]["properties"]["sar:polarization"] = pol
        attrs["stac_discovery"]["properties"].update(S1ARD_FORMAT_PROPERTIES)
        attrs["processing_history"] = copy.deepcopy(annotations.history)
        node.attrs = attrs
        bursts[f"{prefix}_{pol}_{swath}_{burst_id}"] = node
    return bursts


def to_s1ard_format(product: xr.DataTree, safe_path: str) -> xr.DataTree:
    """
    Convert a Sentinel-1 TOPSAR SLC product read by CPM 3.0.0 into the format expected by S1 ARD 1.2.0.

    Parameters:
        product: product read from the SAFE by eopf.store.open_datatree (CPM 3.0.0)
        safe_path: local path of the SAFE product, to read its XML files

    Returns:
        A new DataTree, with one burst per polarization. The measurement data stays lazy.
    """
    annotations = SafeAnnotations(safe_path)
    result = xr.DataTree(dataset=product.to_dataset(inherit=False), name=product.name)
    result.attrs = copy.deepcopy(product.attrs)
    result.attrs.setdefault("stac_discovery", {}).setdefault("properties", {}).update(S1ARD_FORMAT_PROPERTIES)
    for name, burst in product.children.items():
        for burst_name, node in _split_burst(name, burst, annotations).items():
            result[burst_name] = node
    return result
