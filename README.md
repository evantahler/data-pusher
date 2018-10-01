# Data Pusher

I am a simple ETL tool.

[![CircleCI](https://circleci.com/gh/evantahler/data-pusher.svg?style=svg)](https://circleci.com/gh/evantahler/data-pusher)

* I've got a logger and basic process control
* I've got connection types that I support
  * Postgres
  * CSV
  * ... your own!

## Philosophy
This ETL assumes the following:
 * You want to replicate your data in a /streaming/ manner, ie: you want to always poll "sources" and only add the new/updates to "destinations"
 * The details of the source data's types are not that important.  i.e. if an `int` becomes a `bigint`, that's OK
 * You need a higher-level programming language as part of your ETL.  Perhaps you are decorating your data with information from an API...

### Why Node?
With async/await, node is now the best way to program parallel processes which spend most of its time waiting for data.  An ETL is largely asking one source for data, doing some simple (read: non-cpu bound) transformation on that data, and then sending it off to another destination.  With promise flow control, this again becomes very simple!

## Example
Say you want to move all the tables with data newer than X from one database to another.  We will be demoing this with a Rails-like database, wher an `updated_at` column can be used to check for new or updated records.

```js
const ETL = require('../lib/etl.js')
// in your project, `const ETL = require('data-pusher')`

const connections = {
  source: {
    type: 'pg',
    connectionString: process.env.SOURCE
  },
  destination: {
    type: 'pg',
    connectionString: process.env.DESTINATION
  }
}

const etl = new ETL(connections)
const updateColumns = ['updated_at', 'created_at']

const main = async () => {
  await etl.connect()

  let promises = []
  const tables = await etl.connections.source.listTables()
  for (let i in tables) {
    promises.push(copyTable(tables[i]))
  }

  await Promise.all(promises)
  await etl.end()
}

const copyTable = async (table) => {
  let copyType = 'full'
  let tableUpdateCol
  const destinationTables = await etl.connections.destination.listTables()
  if (destinationTables.includes(table)) {
    const columns = await etl.connections.destination.listColumns(table)
    updateColumns.reverse().forEach((updateCol) => {
      if (columns.includes(updateCol)) {
        copyType = 'update'
        tableUpdateCol = updateCol
      }
    })
  }

  if (copyType === 'full') {
    await etl.connections.source.read(table, async (data) => {
      await etl.connections.destination.write(table, data)
    })
  } else {
    const latest = await etl.connections.destination.max(table, tableUpdateCol)
    await etl.connections.source.read(table, async (data) => {
      await etl.connections.destination.write(table, data)
    }, latest, tableUpdateCol)
  }
}

(async function () { await main() }())
```
This example can be run with `node ./examples/simpleRails.js`

## Creating your own connections

Connections must support the following methods:
* `async connect()`
* `async end ()`
* `async read('id', handler, ...)`
* `async write('id', data, ...)`

And then any other methods you might want

## Notes:

* I require node.js v10+, as there are some helpers for pipes and filters which this project uses
* I only speak Postgres (v9.5+ required for [upserts](https://stackoverflow.com/questions/17267417/how-to-upsert-merge-insert-on-duplicate-update-in-postgresql))
* I only log to STDERR and STDOUT

## Thanks
* Inspired by [Empujar](https://github.com/taskrabbit/empujar) and [Forklift](https://github.com/taskrabbit/forklift)
