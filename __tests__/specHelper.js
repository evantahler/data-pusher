const fs = require('fs')
const path = require('path')
const CSV = require('./../lib/csv')
const PG = require('./../lib/pg')

const fsReaddirAsync = async (p) => {
  return new Promise((resolve, reject) => {
    fs.readdir(p, (error, files) => {
      if (error) { return reject(error) }
      return resolve(files)
    })
  })
}

module.exports = class SpecHelper {
  constructor () {
    this.csv = new CSV()
    this.connections = {
      source: new PG('spechelper-source', process.env.TEST_SOURCE),
      destination: new PG('spechelper-destination', process.env.TEST_DESTINATION)
    }
  }

  async connect () {
    for (let name in this.connections) {
      await this.connections[name].connect()
    }
  }

  async end () {
    for (let name in this.connections) {
      await this.connections[name].end()
    }
  }

  async seed () {
    const seedFiles = await fsReaddirAsync(path.join(__dirname, 'seeds'))
    for (let i in seedFiles) { await this.seedTableFromCsv(seedFiles[i]) }
  }

  async seedTableFromCsv (file) {
    let tableName = file.split('.')[0]
    await this.connections.source.dropTable(tableName)
    const data = this.csv.parse(path.join(__dirname, 'seeds', file))
    await this.connections.source.write(tableName, data)
  }
}
