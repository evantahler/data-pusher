# ETL (for voom)

I am a very lazy ETL tool.

1. I will copy over all rows from DB `source` to DB `destination.tmp`
  a. If a table has an `updated_at`, I'll load in all data from the last time I ran
  b. If a table doesn't have an `updated_at`, I'll replace the whole table
2. I'll run any SQL statements in the `transformations` directory of this project on `destination.tmp`
3. I'll move `destination.tmp` to `destination`

## Setup
* Copy `.env.example` to `.env` and be sure you have access to those databases
* run me via `npm start`
* test me via `npm test`

## Notes:

* I only speak Postgress
* I only log to STDERR and STDOUT
