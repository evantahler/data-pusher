# ETL (for voom)

I am a very lazy ETL tool.

[![CircleCI](https://circleci.com/gh/evantahler/etl.svg?style=svg&circle-token=fd0f461c1b9af93c35fdb6fbdcf2285133bddd84)](https://circleci.com/gh/evantahler/etl)

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

* I only speak Postgres (v9.5+ required for [upserts](https://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql))
* I only log to STDERR and STDOUT
