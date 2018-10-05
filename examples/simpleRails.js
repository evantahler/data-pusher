const DataPusher = require('../lib/dataPusher.js')
// in your project, `const DataPusher = require('data-pusher')`

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

const etl = new DataPusher(connections)
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
  let copyTypeMode = 'full'
  let tableUpdateCol
  const destinationTables = await etl.connections.destination.listTables()
  if (destinationTables.includes(table)) {
    const columns = await etl.connections.destination.listColumns(table)
    updateColumns.reverse().forEach((updateCol) => {
      if (columns.includes(updateCol)) {
        copyTypeMode = 'update'
        tableUpdateCol = updateCol
      }
    })
  }

  if (copyTypeMode === 'full') {
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
