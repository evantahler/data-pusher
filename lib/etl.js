const Logger = require('./logger.js')
const PG = require('./pg.js')

module.exports = class ETL {
  constructor () {
    this.logger = new Logger()
    this.connections = {
      source: new PG('SOURCE', process.env.SOURCE),
      destination: new PG('DESTINATION', process.env.DESTINATION)
    }

    for (let name in this.connections) {
      this.connections[name].on('log', (message, level, data) => {
        this.log(message, level, data)
      })
    }
  }

  log (message, level, data) { this.logger.log(message, level, data) }
  logError (error) { this.logger.logError(error) }

  async connect () {
    for (let name in this.connections) {
      await this.connections[name].connect()
    }
  }

  async end () {
    this.log('disconnecting...')
    for (let name in this.connections) {
      await this.connections[name].end()
    }
    this.log('ETL Complete!')
  }

  async copyTable (table) {
    const since = await this.destination.max(table)
    await this.source.read(table, since, rows => this.destination.write(table, rows))
    // await new Promise((resolve) => {
    //   const reader = await this.source.read(table)
    //   const writer = await this.destination.write(table)
    //   reader.pipe(writer)
    //   reader.start()
    //   writer.on('end', resolve)
    // })
  }
}
