const pg = require('pg')
const fs = require('fs')
const pgRange = require('pg-range')
const { EventEmitter } = require('events')
const format = require('pg-format')

// ---
pg.types.setTypeParser(20, function (val) { return parseInt(val) }) // opt into integers for BIGINT
pgRange.install(pg) // install tzrange type parser
// ---

module.exports = class PG extends EventEmitter {
  constructor (name, connectionString, schema = 'public') {
    super()

    this.statementDelimiter = ';'
    this.name = name
    this.connectionString = connectionString
    this.schema = schema
    this.pool = new pg.Pool({ connectionString })
    this.tables = []
    this.chunkSize = 1000
    this.primaryKeyColumn = 'id'

    this.pool.on('error', (error, client) => {
      this.log('Unexpected error on idle client', 'error', error)
    })
  }

  log (message, level, data) {
    this.emit('log', `[${this.name}] ${message}`, level, data)
  }

  async connect () {
    await this.listTables()
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
        this.emit('error', error, { query: q, values })
        throw error
      }
    } finally {
      client.release()
    }
  }

  async listTables () {
    const { rows } = await this.query('SELECT * FROM pg_catalog.pg_tables')
    const schemaRowData = rows.filter(tableData => tableData.schemaname === this.schema)
    const tableNames = schemaRowData.map((tableData) => { return tableData.tablename })
    this.tables = tableNames.sort()
    this.log(`found ${this.tables.length} tables`, 'debug')
    return this.tables
  }

  async count (table, where, whereValues) {
    if (!where) {
      const { rows } = await this.query(
        format(`SELECT COUNT(*) as __count FROM %I`, table)
      )
      return rows[0].__count
    } else {
      const { rows } = await this.query(
        format(`SELECT COUNT(*) as __count FROM %I WHERE ${where}`, table)
      )
      return rows[0].__count
    }
  }

  async max (table, sortColumn = 'updated_at') {
    // There's a lot to learn about what types of interploations PG allows in queries...
    // https://github.com/brianc/node-postgres/issues/539
    try {
      const { rows } = await this.query(
        format(`SELECT MAX(%I) as __max FROM %I`, sortColumn, table)
      )
      return rows[0].__max
    } catch (error) {
      return null
    }
  }

  async read (table, handler, since, sortColumn = 'updated_at') {
    if (!handler || typeof handler !== 'function') {
      throw new Error('handler function is required')
    }

    const tableColumns = await this.listColumns(table)
    if (since && tableColumns.includes(sortColumn)) {
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

    let order = ''
    const tableColumns = await this.listColumns(table)
    if (tableColumns.includes(this.primaryKeyColumn)) {
      order = `ORDER BY ${this.primaryKeyColumn}`
    }

    const whereClause = format(`%I >= %L`, sortColumn, since)
    const totalCount = await this.count(table, whereClause)
    this.log(`getting ${totalCount} records from ${table} newer than ${sortColumn}=${since}`)

    while (continueGettingData) {
      const { rows } = await this.query(
        format(`SELECT * FROM %I WHERE ${whereClause} ${order} LIMIT %s OFFSET %s`, table, limit, offset)
      )
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

    let order = ''
    const tableColumns = await this.listColumns(table)
    if (tableColumns.includes(this.primaryKeyColumn)) {
      order = `ORDER BY ${this.primaryKeyColumn}`
    }

    while (continueGettingData) {
      const { rows } = await this.query(
        format(`SELECT * FROM %I ${order} LIMIT %s OFFSET %s`, table, limit, offset)
      )
      rowsFound = rowsFound + rows.length
      if (rows.length > 0) { this.log(`  got ${rowsFound}/${totalCount} records from ${table}`) }
      await handler(rows)
      offset = offset + limit
      if (rows.length === 0) { continueGettingData = false }
    }
  }

  async execSqlFile (file) {
    const contents = fs.readFileSync(file).toString()
    const commands = contents.split(this.statementDelimiter)
    for (let i in commands) { await this.query(commands[i]) }
  }

  async write (table, rows) {
    await this.ensureTable(table)
    let columns = await this.listColumns(table)
    let keys = []
    let values = []
    let decimalColumns = []
    let attemptedNewColumns = []

    for (let i in rows) {
      let row = rows[i]
      let cols = Object.keys(row)
      for (let j in cols) {
        let columnName = cols[j]
        let value = row[columnName]
        if (!columns.includes(columnName) && !attemptedNewColumns.includes(columnName)) {
          await this.addColumn(table, columnName, rows.map((r) => { return r[columnName] }))
          columns = await this.listColumns(table)
          attemptedNewColumns.push(columnName)
        }

        if (!keys.includes(columnName) && columns.includes(columnName)) { keys.push(columnName) }

        if (typeof value === 'number' && value.toString().indexOf('.') > 0) {
          if (!decimalColumns.includes(columnName)) {
            decimalColumns.push(columnName)
          }
        }
      }

      let theseValues = []
      keys.forEach((k) => { theseValues.push(row[k]) })
      values.push(theseValues)
    }

    for (let k in decimalColumns) {
      // because in JS 10 === 10.0,
      // we need to actually look for the decinal in the string to determine the numeric type :/
      await this.ensureDecimalToFloat(table, decimalColumns[k])
    }

    // TODO: Combine this into 1 insert statement
    for (let i in values) {
      let theseValues = values[i]
      for (let i in theseValues) {
        if (theseValues[i] && typeof theseValues[i] === 'object' && theseValues[i].begin && theseValues[i].end) {
          theseValues[i] = pgRange.Range(theseValues[i].begin, theseValues[i].end)
        }
      }

      let spannedValues = ''
      let z = 1
      while (z <= theseValues.length) {
        spannedValues += `$${z}`
        if (z < theseValues.length) { spannedValues += ', ' }
        z++
      }

      let statement = `INSERT
        INTO ${table} (${keys.join(', ')})
        VALUES (${spannedValues})
        ON CONFLICT (${this.primaryKeyColumn})
        DO UPDATE SET
          (${keys.join(', ')})
          = (${spannedValues})`

      await this.query(statement, theseValues)
    }

    if (rows.length > 0) {
      this.log(`  wrote ${rows.length} records to ${table}`)
    }
  }

  async dropTable (table) {
    await this.query(format(`DROP TABLE IF EXISTS %I`, table))
  }

  async ensureTable (table, primaryKey = 'id') {
    await this.query(format(`CREATE TABLE IF NOT EXISTS %I ()`, table))
  }

  async listColumns (table) {
    const { rows } = await this.query(
      format(`SELECT * FROM information_schema.columns WHERE table_schema = %L AND table_name = %L`, this.schema, table)
    )
    let columns = rows.map((row) => { return row.column_name })
    columns.sort((a, b) => {
      if (a === this.primaryKeyColumn) { return a }
      if (b === this.primaryKeyColumn) { return b }
      return a > b
    })
    return columns
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
      } else if (typeof exampleValue === 'object' && exampleValue.begin && exampleValue.end) {
        type = 'TSRANGE'
      } else if (typeof exampleValue === 'object') {
        type = 'JSON'
      } else {
        type = 'TEXT'
      }
    })

    if (!type) { return }

    let query = `ALTER TABLE %I ADD COLUMN %I %s`
    if (columnName === this.primaryKeyColumn) {
      query += ' PRIMARY KEY'
    }

    await this.query(format(query, table, columnName, type))
  }

  async ensureDecimalToFloat (table, columnName) {
    let query = `ALTER TABLE %I ALTER COLUMN %I TYPE FLOAT`
    await this.query(format(query, table, columnName))
  }

  async describeTable (table) {
    const { rows } = await this.query(
      format(`SELECT column_name, data_type, character_maximum_length FROM information_schema.columns where table_name = %L`, table)
    )
    return rows
  }
}
