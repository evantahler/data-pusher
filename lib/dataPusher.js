const Logger = require('./logger.js')
const fs = require('fs')

module.exports = class ETL {
  constructor (connections = {}, logLevel) {
    this.logger = new Logger(logLevel)
    this.connections = {}

    for (let name in connections) {
      let type = connections[name].type
      let connectionString = connections[name].connectionString

      if (fs.existsSync(`${__dirname}/connections/${type}.js`)) {
        this.connections[name] = new (require(`${__dirname}/connections/${type}.js`))(name, connectionString)
      } else if (fs.existsSync(`${process.cwd()}/connections/${type}.js`)) {
        this.connections[name] = new (require(`${process.cwd()}/connections/${type}.js`))(name, connectionString)
      } else {
        throw new Error(`Cannot load connection type ${type} in either ${__dirname}/connections/${type}.js or ${process.cwd()}/connections/${type}.js`)
      }
    }

    for (let name in this.connections) {
      this.connections[name].on('log', (message, level, data) => {
        this.log(message, level, data)
      })

      this.connections[name].on('error', (error, data) => {
        this.logError(error, data)
      })
    }
  }

  log (message, level, data) { this.logger.log(message, level, data) }
  logError (error, data) { this.logger.logError(error, data) }

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
}
