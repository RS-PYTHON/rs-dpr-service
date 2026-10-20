# rs-dpr-service

The web service of [RS-Python](https://home.rs-python.eu/), the Copernicus Reference System Python, that runs the
[EOPF](https://eopf.copernicus.eu/) DPR processors (the re-engineered Sentinel processors) on the RS-Python
[Dask](https://www.dask.org/) clusters, with an [OGC API - Processes](https://ogcapi.ogc.org/processes/) interface.

## Overview

- `GET /dpr/processes/{processor}` returns the [task table](https://cpm.pages.eopf.copernicus.eu/eopf-cpm/main/processor-orchestration-guide/tasktables.html)
  of a processor. The task tables are in the [config](https://github.com/RS-PYTHON/rs-dpr-service/tree/develop/config) folder.
- `POST /dpr/processes/{processor}/execution` runs a processor on a Dask cluster, with an EOPF payload file, and
  returns a job identifier.
- `GET /dpr/jobs/{job_id}`, `GET /dpr/jobs/{job_id}/logs` and `DELETE /dpr/jobs/{job_id}` follow, stream the logs of
  and cancel a job.

The processors available are `s1_l0`, `s3_l0`, `s1_ard`, `s3_l1olci`, `s3_l2olci`, plus a `mockup` processor and
the `conv_safe_zarr` conversion of legacy SAFE products to Zarr. The service is called by the processing flows of
the [rs-client-libraries](https://github.com/RS-PYTHON/rs-client-libraries).

## Getting started

- Run the processors from the Prefect flows, see [Customise the DPR processing](https://github.com/RS-PYTHON/rs-client-libraries/blob/develop/docs/doc/advanced/customise-dpr-flow.md).
- Run the service on your computer with the [local mode](https://github.com/RS-PYTHON/rs-demo/blob/develop/docs/local-mode.md)
  of rs-demo.
- Deploy it with the [rs-dpr-service Helm chart](https://github.com/RS-PYTHON/rs-helm/blob/develop/charts/rs-dpr-service/README.md)
  and [rs-server-deployment](https://github.com/RS-PYTHON/rs-server-deployment).
- Develop on it:

    ```shell
    poetry install --with dev
    poetry run pre-commit install
    poetry run pytest -m unit
    ```

    The service must use the same Python and Dask versions as the Dask clusters of the processors, see
    [Dask images](https://github.com/RS-PYTHON/rs-workflow-env/blob/develop/docs/dask_images.md).

## Documentation

- [DPR processors](docs/doc/index.md): role, architecture, deployment
- [Cancel a DPR run](docs/doc/cancel-dpr-run.md)
- [DPR processor integration](docs/doc/processor-integration.md)
- Online documentation of the whole project: <https://home.rs-python.eu/rs-documentation/>

## Related repositories

| Repository | Relationship |
|---|---|
| [rs-client-libraries](https://github.com/RS-PYTHON/rs-client-libraries) | The processing flows that call this service. |
| [rs-workflow-env](https://github.com/RS-PYTHON/rs-workflow-env) | Docker images of the Dask clusters of the processors. |
| [rs-server](https://github.com/RS-PYTHON/rs-server) | The other RS-Python services. |
| [rs-helm](https://github.com/RS-PYTHON/rs-helm), [rs-server-deployment](https://github.com/RS-PYTHON/rs-server-deployment) | Helm chart and deployment. |
| [rs-testmeans](https://github.com/RS-PYTHON/rs-testmeans) | Mockup processor. |
| [rs-documentation](https://github.com/RS-PYTHON/rs-documentation) | Online documentation of the whole project. |

## Contributing

See the [contribution guidelines](https://github.com/RS-PYTHON/rs-documentation/blob/develop/docs/developers/contributing.md).
Report bugs in the [issue tracker](https://github.com/RS-PYTHON/rs-dpr-service/issues). In case of sensitive bugs like
security vulnerabilities, please contact <ops_coprs@airbus.com> directly instead of using the issue tracker.

## Licensing

The code in this project is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

---

![](https://raw.githubusercontent.com/RS-PYTHON/.github/refs/heads/main/profile/banner_logo.jpg)

This project is funded by the EU and ESA.
