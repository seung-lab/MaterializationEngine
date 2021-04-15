[![Actions Status](https://github.com/seung-lab/MaterializationEngine/workflows/Materialization%20Engine/badge.svg)](https://github.com/seung-lab/MaterializationEngine/actions)
# Materialization Engine
#### A product of the CAVE (Connectome Annotation Versioning Engine) infrastructure
This is a microservice for creating materializied versions of an analysis database,  merging together spatially bound annotation and a segmentation data stored in a pychunkedgraph that is frozen in time. The data is stored in a PostgreSQL database where the spatial annotations are leveraging postgis point types. The materialization engine can create new time locked versions periodically on a defined scheudle as well as one-off versions for specific use cases.  

Present funcationality:

* A flask microservice as REST API endpoints for creating and querying the materializied databases.
* The backend is powered by workflows running as [Celery][celery] workers, a task queue implementation used to asynchronously execute work.
## Installation

This service is intended to be deployed to a kubernentes cluster as a series of pods.
Local deployment is currently best done by using docker. A docker-compose file is included that will install all the required packages and create a local postgres database and redis broker that is leveraged by the Celery workers for running tasks.

Docker compose example:
```
    $ docker-compose build
    $ docker-compse up
```
Alternatively one can setup a docker container running PostgreSQL database and a seperate Redis container then create a python virtual env and run the following commands:

Setup a redis instance:
```
    $ docker run -p 6379:6379 redis
```
Setup a Postgres database (with postgis):
```
    $ docker run --name db -v /my/own/datadir:/var/lib/postgresql/data -e POSTGRES_PASSWORD=materialize postgis/postgis
```

Setup the flask microservice:
```
    $ cd materializationengine
    $ python3 -m venv mat_engine
    $ source mat_engine/bin/activate
    (mat_engine) $: python setup.py install
    (mat_engine) $: python run.py
```
Start a celery worker for processing tasks. Open another terminal:
```
    $ source mat_engine/bin/activate
    (mat_engine) $ celery worker --app=run.celery --pool=prefork --hostname=worker.process@%h --queues=processcelery --concurrency=4 --loglevel=INFO -Ofair
```
## Workflow Overview

The materialization engine runs celery workflows that create snapshots of spatial annotation data where each spatial point is linked to a segmentation id that is valid at a specific time point.

There are a few workflows that make up the materialization engine:
* [Bulk Upload][bulk] (Load large spatial and segmentation datasets into a PostgreSQL database)
* [Ingest New Annotations][ingest] (Query and insert underlying segmentation data on spatial points with missing segmentation data)
* [Update Root Ids][update] (Query and update expired root ids from the chunkedgraph between a time delta)
* [Create Frozen Database][create] (Creates a timelocked database for all tables)
* [Complete Workflow][complete] (Combines the Ingest New Annotations, Update Root Id and Create Frozen Worklows in one, run in series)


## Meta

Distributed under the MIT license. See ``LICENSE`` for more information.

## Contributing

1. Fork it (<https://github.com/seung-lab/MaterializationEngine/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

<!-- Markdown link & img dfn's -->

[celery]: https://docs.celeryproject.org/en/stable/getting-started/introduction.html
[bulk]: /materializationengine/workflows/bulk_upload.py
[ingest]: /materializationengine/workflows/ingest_new_annotations.py
[update]: /materializationengine/workflows/update_root_ids.py
[create]: /materializationengine/workflows/create_frozen_database.py
[complete]: /materializationengine/workflows/complete_workflow.py