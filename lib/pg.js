const { Pool } = require('pg')
const { EventEmitter } = require('events')

module.exports = class PG extends EventEmitter {
  constructor (name, connectionString, schema = 'public') {
    super()

    this.name = name
    this.connectionString = connectionString
    this.schema = schema
    this.pool = new Pool({connectionString})
    this.tables = []

    this.pool.on('error', (error, client) => {
      this.log('Unexpected error on idle client', 'error', error)
    })
  }

  // log (message, level) { this.logger.log(`[${this.name}] ${message}`, level) }
  log (message, level, data) {
    this.emit('log', `[${this.name}] ${message}`, level, data)
  }

  async connect () {
    await this.queryTables()
  }

  async end () {
    await this.pool.end()
  }

  async query (q, values) {
    const client = await this.pool.connect()

    try {
      const response = await client.query(q, values)
      return response
    } catch (error) {
      if (!error.toString().match(/does not exist/)) {
        this.logger.error(error)
        throw error
      }
    } finally {
      client.release()
    }
  }

  async queryTables () {
    const {rows} = await this.query('SELECT * FROM pg_catalog.pg_tables')
    const schemaRowData = rows.filter(tableData => tableData.schemaname === this.schema)
    const tableNames = schemaRowData.map((tableData) => { return tableData.tablename })
    this.tables = tableNames.sort()
    this.log(`found ${this.tables.length} tables`)
  }

  async count (table, where, values) {
    const {rows} = await this.query(`SELECT COUNT(*) as __count FROM '${table}' ${where}`, values)
    return rows[0].__count
  }

  async max (table, sortColumn = 'updated_at') {
    try {
      const {rows} = await this.query(`SELECT MAX(${sortColumn}) as __max FROM '${table}'`)
      return rows[0].__max
    } catch (error) {
      return null
    }
  }

  async read (table, since, handler, sortColumn = 'updated_at') {
    if (since) {
      return this.readTableSince(table, since, handler, sortColumn)
    } else {
      return this.readFullTable(table, handler)
    }
  }

  async readTableSince (table, since, handler, sortColumn = 'updated_at') {
    const count = await this.count(table, `WHERE $1 >= $2`, [sortColumn, since])
    this.log(`getting ${count} records from '${table}' newer than ${sortColumn}=${since}`)
  }

  async readFullTable (table, since, handler) {
    const count = await this.count(table)
    this.log(`getting ${count} records from '${table}'`)
  }
}
