const { Pool, types } = require('pg')
const { EventEmitter } = require('events')

// ---
// opt into integers for BIGINT
types.setTypeParser(20, function (val) { return parseInt(val) })
// ---

module.exports = class PG extends EventEmitter {
  constructor (name, connectionString, schema = 'public') {
    super()

    this.name = name
    this.connectionString = connectionString
    this.schema = schema
    this.pool = new Pool({ connectionString })
    this.tables = []
    this.chunkSize = 1000

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
        this.emit('error', error)
        throw error
      }
    } finally {
      client.release()
    }
  }

  async queryTables () {
    const { rows } = await this.query('SELECT * FROM pg_catalog.pg_tables')
    const schemaRowData = rows.filter(tableData => tableData.schemaname === this.schema)
    const tableNames = schemaRowData.map((tableData) => { return tableData.tablename })
    this.tables = tableNames.sort()
    this.log(`found ${this.tables.length} tables`)
    return this.tables
  }

  async count (table, where, values) {
    const { rows } = await this.query(`SELECT COUNT(*) as __count FROM ${table} ${where}`, values)
    return rows[0].__count
  }

  async max (table, sortColumn = 'updated_at') {
    try {
      const { rows } = await this.query(`SELECT MAX(${sortColumn}) as __max FROM ${table}`)
      return rows[0].__max
    } catch (error) {
      return null
    }
  }

  async read (table, since, handler, sortColumn = 'updated_at') {
    // if (since) {
    //   return this.readTableSince(table, since, handler, sortColumn)
    // } else {
    return this.readFullTable(table, handler)
    // }
  }

  // async readTableSince (table, since, handler, sortColumn = 'updated_at') {
  //   const count = await this.count(table, `WHERE $1 >= $2`, [sortColumn, since])
  //   this.log(`getting ${count} records from '${table}' newer than ${sortColumn}=${since}`)
  // }

  async readFullTable (table, handler) {
    if (!handler || typeof handler !== 'function') {
      throw new Error('handler function is required')
    }

    let continueGettingData = true
    let limit = this.chunkSize
    let offset = 0
    let total = 0
    const totalCount = await this.count(table)
    this.log(`getting ${totalCount} records from ${table}...`)

    while (continueGettingData) {
      const { rows } = await this.query(`SELECT * FROM ${table} LIMIT ${limit} OFFSET ${offset}`)
      total = total + rows.length
      if (rows.length > 0) { this.log(`  got ${total}/${totalCount} records from ${table}`) }
      await handler(rows)
      offset = offset + limit
      if (rows.length === 0) { continueGettingData = false }
    }
  }

  async write (table, rows) {
    await this.ensureTable(table)
    let columns = await this.getColumns(table)
    let keys = []
    let values = []

    for (let i in rows) {
      let row = rows[i]
      let cols = Object.keys(row)
      for (let j in cols) {
        let columnName = cols[j]
        if (!columns.includes(columnName)) {
          await this.addColumn(table, columnName, rows.map((r) => { return r[columnName] }))
          columns = await this.getColumns(table)
        }

        if (!keys.includes(columnName)) { keys.push(columnName) }
      }

      let theseValues = []
      keys.forEach((k) => { theseValues.push(row[k]) })
      values.push(theseValues)
    }

    // TODO: Combine this into 1 insert statement
    const baseStatement = `INSERT INTO ${table} (${keys.join(', ')})`
    for (let i in values) {
      let theseValues = values[i]
      let statement = `${baseStatement} VALUES (`
      let z = 1
      while (z <= theseValues.length) {
        statement += `$${z}`
        if (z < theseValues.length) { statement += ', ' }
        z++
      }
      statement += ')'
      await this.query(statement, theseValues)
    }
  }

  async dropTable (table) {
    await this.query(`DROP TABLE IF EXISTS ${table}`)
  }

  async ensureTable (table, primaryKey = 'id') {
    await this.query(`CREATE TABLE IF NOT EXISTS ${table} ()`)
  }

  async getColumns (table) {
    const { rows } = await this.query(`SELECT * FROM information_schema.columns WHERE table_schema = '${this.schema}' AND table_name = '${table}'`)
    return rows.map((row) => { return row.column_name })
  }

  async addColumn (table, columnName, exampleValues) {
    let type = 'TEXT'

    exampleValues.forEach((exampleValue) => {
      if (exampleValue === null) { return }
      if (exampleValue === undefined) { return }
      if (exampleValue === '') { return }

      if (typeof exampleValue === 'boolean') {
        type = 'BOOLEAN'
      } else if (typeof exampleValue === 'number') {
        if (!type) { type = 'BIGINT' }
        if (exampleValue.toString().indexOf('.') > 0) {
          type = 'FLOAT'
        }
      } else if (exampleValue instanceof Date) {
        type = 'TIMESTAMP'
      } else {
        type = 'TEXT'
      }
    })

    await this.query(`ALTER TABLE ${table} ADD COLUMN ${columnName} ${type}`)
  }
}
