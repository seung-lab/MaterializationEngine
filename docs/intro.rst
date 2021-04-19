User Guide
==========

Introduction
------------
This is a microservice for creating materialized versions of an analysis database, 
merging together spatially bound annotation and a segmentation data stored in a 
chunkedgraph that is frozen in time. The data is stored in a PostgreSQL database 
where the spatial annotations are leveraging PostGIS point types. 

The materialization engine can create new time locked versions periodically on a 
defined schedule as well as one-off versions for specific use cases.  

The materialization engine combines DynamicAnnotationDB and pychunkgraph that
links aligned volume spatial annotations and segmentation data from a chunkgraph.

The materialization engine is a component of the CAVE (Connectome Annotation Versioning Engine) infrastructure.

Database Architecture
---------------------

The materialization engine uses two database models, 'Live Databases' and 'Versioned Materialized Databases'.


Live Databases
    The live database represents a specific dataset with a specific spatially aligned image volume.
    The data within the live database contains a spatial annotation table that can be linked to a specific
    segmentation source. Multiple segmentation sources can be linked to a single spatial annotation table 
    if different segmentation sources are available. 

    The live database is used for inserting and updating spatial annotations, it should not be used for analysis.

    Spatial Annotation Tables
        The spatial annotation tables represent a specific schema to define spatial points that can represent 
        various biological features, some examples include synapses (with pre and post targets), nucleus locations,
        and cell types tagged by location. 
        
        .. note::
            
            Each spatial point is represented by a PostGIS POINTZ datatype to allow for spatial querying.


        The spatial annotation table supports CRUD operations that allow users to edit the spatial points as they see fit.
        Each row has has additional columns to support updating and deleting rows:

        valid
            The valid column is a boolean that determines if the spatial annotation should be included in querying. If will be 
            set to false if an update or delete is done to the row.

        superseded_id
            If a row is updated a new row will be created with the changed values. The superseded id will then point to the 
            newly inserted row.
            
        
        .. warning::

            Update statements will not remove data but rather append the table with a new row and point the foreign key 
            from the segmentation table to the new row.

            Delete statements will flag the row as not valid, no data is actually removed from the table.

    Linked Segmentation Tables
        The linked segmentation table has a foreign key on the id of the spatial annotation table with supervoxel and 
        root ids from a specific timestamp for each spatial point defined in the annotation table. 
        
        Periodically the segmentation root id data will become stale due to error corrections done manually or automatically in the 
        chunkedgraph. In order to enure the segmentation data is not stale the materialization engine can periodically find the stale
        data and update the rows with the new root ids.

        .. note::
            Updating the root ids is done across all segmentation tables in the live database at a fixed time stamp. This ensures that all the 
            segmentation rows are valid for that specific time stamp. 


Versioned Materialized Databases
    A versioned materialized database is primarily used for analysis and querying data. Each versioned materialized database is time stamped
    to the last time the stale root ids where updated. When materializing a database a specified live database will be copied. The spatial annotation tables
    with their linked segmentation tables with a specified segmentation source will be merged into one table to simplify querying.  

    .. warning::

        Only one segmentation source can be used for a given versioned materialized database. If there are multiple segmentation tables
        for a given annotation table in the live database separate materialized databases must be created.


Prerequisites
-------------
OS: 

Currently the materialization engine has been tested natively on Ubuntu Linux. It is not supported
on Windows unless running in docker.

Credentials:

The CAVE (Connectome Annotation Versioning Engine) infrastructure requires specific credentials
to connect to a chunkgraph instance. To setup credentials please see the documentation from 
`Cloud Volume Credentials <https://github.com/seung-lab/cloud-volume#credentials>`_ .

Setup
-----

The materialization engine is designed to be deployed on Kubernetes but 
it can also be run locally for development and debugging.

To run the locally use the following commands:


.. code-block:: bash

    $ git clone https://github.com/seung-lab/MaterializationEngine
    $ cd MaterializationEngine
    $ docker-compose build
    $ docker-compose up -d

Once the docker images are running the following services will be available:

DB
    A PostgreSQL database with PostGIS extension installed. The 
    data inserted into the database will be persistent between restarts of the 
    docker image.

Adminer
    A admin interface for visualizing the database.

    It can be accessed at http://localhost:8080

Redis 
    Redis is used to both queue celery tasks as well as storing
    the returned results from the celery tasks.

Redis commander
    An admin interface to inspect the redis instance

    It can be accessed at http://localhost:8083

Celery Process [1,2]
    Two celery workers are configured to run in the docker setup.
    
Celery Beat
    Celery beat schedules periodic tasks with cron args.
    See the celery_worker.py and config.py ['BEAT_SCHEUDLE'] files for details.

Materialize
    A flask app the serves the REST API endpoints for both querying the database
    as well as running workflows.

Flower
    An admin interface to inspect the status of the celery workers.

    It can be accessed at http://localhost:5555

Teardown
--------

To stop materialization engine docker images running locally simply run the following:

.. code-block:: bash

    $ docker-compose down
    