# DPR processor integration

This procedure validates a new DPR processor (or a new version of a processor) and integrates it into RS-Python. It
starts with a delivery of the processor by the [EOPF](https://eopf.copernicus.eu/) project and ends with:

- a Prefect flow able to generate the products of the processor from the Copernicus data sources (CADIP, AUXIP,
  PRIP, CDSE...), that calls the generic processing flow `process-generic`,
- the conversion flows of the auxiliary files and digital elevation models needed by the processor, if any,
- all the generated products stored in the object storage and published in the catalog,
- the list of the issues found in the Copernicus data sources, in EOPF CPM, in the processor and in RS-Python.

## Principles

1. **Step by step**: the procedure is executed step by step. When a blocking issue is found, apply a workaround if
   one exists, to find the next issues. Otherwise, the procedure is paused until the issue is solved. Several
   integrations can be run in parallel.
2. **No untracked issue**: raise the issues to the relevant teams (RS-Python, EOPF CPM, processor, data sources...)
   as soon as they are found.
3. **Iterative approach**: when a case is not covered by this procedure, update the procedure and its templates.

Steps 1 and 2 are executed by the development team, steps 3 to 8 by the validation team.

## Steps

### Step 1: identify the baseline

Define the versions of what is integrated, and register the useful links (source code, documentation):

- the processor name and version, its Git repository and commit,
- the EOPF CPM version it is compliant with,
- the task table specification version it is compliant with,
- the Dask cluster settings: Dask and Dask Gateway versions, number of workers, CPU, RAM and threads per worker, node
  flavour,
- the dependencies of the processor.

### Step 2: integrate from a payload file

Run the processor on the cluster with a payload file written manually, without Prefect and without the
operational data sources, with the
[processor integration notebook](https://github.com/RS-PYTHON/rs-demo/blob/develop/notebooks/processor-integration/processor_integration.ipynb)
of rs-demo (the `config_*` folders next to it contain examples of payloads). This step checks that:

- the Docker image of the processor is built (see [Dask images](https://github.com/RS-PYTHON/rs-workflow-env/blob/develop/docs/dask_images.md))
  and the processor runs on the RS-Python Dask clusters and produces its outputs,
- the processor stops properly when it receives a `SIGINT` signal: all its tasks are stopped within a minute and all
  the resources (CPU, RAM, disk) are released.

The difference with the environment of the processor developers is the use of the RS-Python cluster.

### Step 3: validate the task table

Check that the task table of the processor is compliant with the task table specification of the baseline. If it is
not, raise an issue to the processor team and, if possible, fix it on the RS-Python side to go on (the task tables
used by RS-Python are in the [config](https://github.com/RS-PYTHON/rs-dpr-service/tree/develop/config) folder of
rs-dpr-service; see [Task tables and payload file](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/flows/tasktable-payload.md)).

### Step 4: check the auxiliary files

Identify all the auxiliary files (ADFs) needed by the processor, and collect for each of them:

| AUX product type | STAC collection of the AUX items | Target bucket | Input location | Input format | Required format | Transformation needed |
|---|---|---|---|---|---|---|
| e.g. `AUX_ORBSCT` | e.g. `aux_orbsct` | e.g. `<prefix>-s01-aux` | AUXIP, CDSE, LTA, PRIP, or the catalog after a transformation | SAFE, Zarr... | SAFE, Zarr... | Yes or no |

When a transformation is needed (e.g. a conversion to Zarr), implement it as a conversion flow (see the
[conversion flows](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/flows/common-flows.md#conversion-flows));
the transformed files are then read from the catalog. Check that the
[bucket configuration](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/administration/update-rs-server.md#object-storage-buckets-and-data-lifecycle)
of the platform contains the target buckets.

### Step 5: check the digital elevation models (optional)

If the digital elevation model (DEM) is managed like an auxiliary file, collect the same information as in step 4.
If it is read from outside RS-Python (e.g. another bucket, or the [EarthDataHub](https://earthdatahub.destine.eu/)),
collect its location, its format and the credentials needed, and configure them in the
[storage configuration](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/advanced/customise-dpr-flow.md#storage-configuration).

### Step 6: define the flow parameters

Split the [parameters of `process-generic`](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/advanced/customise-dpr-flow.md#parameters-of-process-generic)
into:

- the **default parameters**, that don't change in the nominal case: pipeline, priority, workflow, processor name
  and version, processing mode, owner, Dask cluster, auxiliary and generated products mappings. They are stored in
  the [default settings](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/advanced/customise-dpr-flow.md#default-settings-of-the-processing-flows)
  Prefect variable of the mission,
- the **specific parameters** of each run: the input products defined in the task table.

### Step 7: check the storage configuration

Check the [storage configuration](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/advanced/customise-dpr-flow.md#storage-configuration):

1. all the generated assets are stored in an S3 bucket,
2. the auxiliary files are read from an S3 bucket,
3. the specific credentials are defined (e.g. to read a DEM from another bucket), if needed.

### Step 8: create and run the specific flow

Develop the Prefect flow dedicated to the processor, that computes the specific parameters and calls
`process-generic` (see e.g. the [Sentinel-3 flows](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/flows/sentinel3-flows.md)),
then:

1. run the flow from Prefect,
2. raise the issues found,
3. fix the blocking issues or apply workarounds, and go back to 1,
4. check that all the auxiliary files and DEMs are stored in the buckets defined at steps 4 and 5; otherwise update
   the bucket configuration or the flow parameters and go back to 1,
5. check that all the products are stored in the expected buckets, according to the task table and the bucket
   configuration,
6. check that all the products are published in the expected catalog collections.

## Tips

### Dask configurations

In the nominal configuration, the processor runs on a Dask Gateway cluster made of a scheduler and of one or
several workers, and reads and writes its data directly in the object storage. To create such a cluster from a
notebook, use the notebooks of the `notebooks/init-dask-clusters` folder of
[rs-demo](https://github.com/RS-PYTHON/rs-demo), see [Add and delete Dask clusters](https://github.com/RS-PYTHON/rs-workflow-env/blob/develop/docs/dask-clusters.md).

For the investigations, two experimental configurations are available (see [DPR processors](index.md#experimental-configurations)):

- **LocalCluster**: Dask Gateway creates a single worker, in which EOPF runs a Dask `LocalCluster`. No data is
  exchanged with the scheduler, but the processing cannot be distributed on several workers.
- **Local files**: the inputs are downloaded on the local disk of the worker, the processor reads and writes local
  files, and the outputs are uploaded to the object storage at the end.

### Resources consumed by the processor

In Grafana, open the *Kubernetes / Views / Pods* dashboard, select the `dask-gateway` namespace and look at the
*CPU Usage by container* and *Memory Usage by container* panels. The performance report of Dask only shows the
consumption of the scheduler, not of the workers.

### Report template

For each test, report:

- the processor information: name, version, Git URL and commit, CPM version, task table specification version,
  Dask cluster settings, dependencies, and the differences with the official baseline if a fork is used,
- the test data and the payload file used,
- the output metrics: execution time, number and size of the generated files, maximum CPU and RAM consumption per
  worker,
- the issues found, with the error logs, and the workarounds applied,
- any relevant file: logs, list of generated outputs, `.zmetadata` files...
