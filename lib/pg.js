const { Pool, types } = require('pg')
const { EventEmitter } = require('events')
const format = require('pg-format')

// ---
// opt into integers for BIGINT
types.setTypeParser(20, function (val) { return parseInt(val) })
// ---

// There's a lot to learn about what types of interploations PG allows in queries...
// https://github.com/brianc/node-postgres/issues/539

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
      await client.query('BEGIN')
      const response = await client.query(q, values)
      await client.query('COMMIT')
      return response
    } catch (error) {
      await client.query('ROLLBACK')
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

  async count (table, where, whereValues) {
    if (!where) {
      const { rows } = await this.query(format(`SELECT COUNT(*) as __count FROM %I`, table))
      return rows[0].__count
    } else {
      const { rows } = await this.query(format(`SELECT COUNT(*) as __count FROM %I WHERE ${where}`, table))
      return rows[0].__count
    }
  }

  async max (table, sortColumn = 'updated_at') {
    try {
      const { rows } = await this.query(format(`SELECT MAX(%I) as __max FROM %I`, sortColumn, table))
      return rows[0].__max
    } catch (error) {
      return null
    }
  }

  async read (table, handler, since, sortColumn) {
    const tableColumns = await this.getColumns(table)
    if (since && tableColumns.includes(since)) {
      return this.readTableSince(table, handler, since, sortColumn)
    } else {
      return this.readFullTable(table, handler)
    }
  }

  async readTableSince (table, handler, since, sortColumn = 'updated_at') {
    if (!handler || typeof handler !== 'function') {
      throw new Error('handler function is required')
    }

    let rowsFound = 0
    let limit = this.chunkSize
    let offset = 0
    let continueGettingData = true

    const whereClause = format(`%I >= %L`, sortColumn, since)
    const totalCount = await this.count(table, whereClause)
    this.log(`getting ${totalCount} records from ${table} newer than ${sortColumn}=${since}`)

    while (continueGettingData) {
      const { rows } = await this.query(format(`SELECT * FROM %I WHERE ${whereClause} LIMIT %s OFFSET %s`, table, limit, offset))
      rowsFound = rowsFound + rows.length
      if (rows.length > 0) { this.log(`  got ${rowsFound}/${totalCount} records from ${table}`) }
      await handler(rows)
      offset = offset + limit
      if (rows.length === 0) { continueGettingData = false }
    }
  }

  async readFullTable (table, handler) {
    if (!handler || typeof handler !== 'function') {
      throw new Error('handler function is required')
    }

    let continueGettingData = true
    let limit = this.chunkSize
    let offset = 0
    let rowsFound = 0
    const totalCount = await this.count(table)
    this.log(`getting ${totalCount} records from ${table}...`)

    while (continueGettingData) {
      const { rows } = await this.query(format(`SELECT * FROM %I LIMIT %s OFFSET %s`, table, limit, offset))
      rowsFound = rowsFound + rows.length
      if (rows.length > 0) { this.log(`  got ${rowsFound}/${totalCount} records from ${table}`) }
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
    await this.query(format(`DROP TABLE IF EXISTS %I`, table))
  }

  async ensureTable (table, primaryKey = 'id') {
    await this.query(format(`CREATE TABLE IF NOT EXISTS %I ()`, table))
  }

  async getColumns (table) {
    const { rows } = await this.query(format(`SELECT * FROM information_schema.columns WHERE table_schema = %L AND table_name = %L`, this.schema, table))
    return rows.map((row) => { return row.column_name })
  }

  async addColumn (table, columnName, exampleValues) {
    let type

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

    await this.query(format(`ALTER TABLE %I ADD COLUMN %I %s`, table, columnName, type))
  }
}
