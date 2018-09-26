const ETL = require('./lib/etl.js')

class VoomETL {
  constructor () {
    this.etl = new ETL()
  }

  async run () {
    this.etl.log('Starting ETL')

    try {
      await this.etl.connect()
      await this.copyAllTables()
    } catch (error) {
      this.etl.logError(error)
    } finally {
      await this.etl.end()
    }
  }

  async copyAllTables () {
    const source = this.etl.connections.source
    const destination = this.etl.connections.destination
    let promises = []

    for (let i in source.tables) {
      let table = source.tables[i]
      let promise = this.etl.copyTable(source, table, destination, table)
      promises.push(promise)
    }

    await Promise.all(promises)
  }
}

// --------------------- //

(async function () {
  const voomETLInstance = new VoomETL()
  await voomETLInstance.run()
}())
