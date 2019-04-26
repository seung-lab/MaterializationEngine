# MaterializationEngine

This is a microservice for managing materializing versions of an analysis database,  merging together annotations in a dynamicannotationdb, and a segmentation stored in a pychunkedgraph.  

Present funcationality:

Provide a flask app with database models to store the versions of the materialization and scripts to reflect that.

# Incremental materialization

incremental_materialize_stage3.py is a script that will create a new analysisversion from a base version. It has 4 phases.  
- Phase 1 copies an existing database from the base_version number to a new version number, taking the timestamp from the command line (default to NOW), and copies the analysis_tables that exist in the base version to the new version.  
- Phase 2 looks for delta root_ids between the two versions, and inserts them into the cell segments table. 
- Phase 3 cycles through the tables in the annotation engine, and if it finds new tables that aren't in the analysistables of the new version, it will materialize them using the annotationclient rest api to grab the annotations.  
- Phase 4 will search all the analysistables for root_ids that are expired, and trigger an update of those rows with the root_id appropriate for the supervoxel_id and the timestamp of the current version.  After this is complete for all the tables it will delete the expired root ids from the cellsegment tables.

To run this materialization at a minimum supply the following arguments (assuming you have set the MATERIALIZATION_POSTGRES_URI environment variable) to the database that contains the analysisversion and analysistable tables (/postgres) with a username and password that has administrative rights.  The code will use this as a base to reformat the uri for other databases on the same server. It also assumes that there is a user 'analysis_user' that already exists on the postgres server.

```
    python scripts/incremental_materialize_stage3.py --base_version BASE_VERSION --n_threads N_THREADS
```
for a full set of arguments use the help functionality of the script.

I have a multiplier on the given number of threads for different operations that likely needs some tuning.

scripts/incremental_materialize_stage2.py is a script that doesn't create a new version but was designed to update an existing version, so it can be modified to help fix errors in various stages, or for example add new annotation tables to previous version. 
It takes both a version and a base_version argument.  