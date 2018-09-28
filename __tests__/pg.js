const SpecHelper = require('./specHelper')
const helper = new SpecHelper()

describe('pg', async () => {
  beforeAll(async () => { await helper.connect() })
  beforeAll(async () => { await helper.seed() })
  afterAll(async () => { await helper.end() })

  test('#queryTables', async () => {
    const tables = await helper.connections.source.queryTables()
    expect(tables).toEqual([
      'cart_products',
      'carts',
      'products',
      'users'
    ])
  })

  describe('#max', async () => {
    test('table with updated_at', async () => {
      const max = await helper.connections.source.max('users')
      expect(max.getTime()).toBeGreaterThanOrEqual(1520046183000)
      expect(max.getTime()).toBeLessThanOrEqual(1520074983000)
    })

    test('table without updated_at', async () => {
      const max = await helper.connections.source.max('carts')
      expect(max).toBeNull()
    })
  })

  test('#count', async () => {
    let usersCount = await helper.connections.source.count('users')
    expect(usersCount).toEqual(3)
  })

  describe('#read', () => {
    beforeAll(() => { helper.connections.source.chunkSize = 2 })
    afterAll(() => { helper.connections.source.chunkSize = 1000 })

    test('can read tables with updated_at', async () => {
      let totalRows = []
      const handler = (rows) => { totalRows = totalRows.concat(rows) }

      await helper.connections.source.read('users', handler)
      expect(totalRows.length).toEqual(3)
    })

    test('can read tables without', async () => {
      let totalRows = []
      const handler = (rows) => { totalRows = totalRows.concat(rows) }

      await helper.connections.source.read('carts', handler)
      expect(totalRows.length).toEqual(3)
    })

    describe('#readTableSince', () => {
      test('reads in batches with a since', async () => {
        let timesHandled = 0
        let totalRows = []
        const handler = (rows) => {
          timesHandled++
          totalRows = totalRows.concat(rows)
        }

        const since = new Date(Date.parse('2018-01-02'))
        await helper.connections.source.readTableSince('users', handler, since, 'updated_at')

        expect(timesHandled).toBe(2)
        expect(totalRows.length).toBe(2)
        expect(totalRows[0].first_name).toBe('Brian')
      })
    })

    describe('#readFullTable', () => {
      test('reads in batches', async () => {
        let timesHandled = 0
        let totalRows = []
        const handler = (rows) => {
          timesHandled++
          totalRows = totalRows.concat(rows)
        }

        await helper.connections.source.readFullTable('users', handler)

        expect(timesHandled).toBe(3)
        expect(totalRows.length).toBe(3)
        expect(totalRows[0].first_name).toBe('Evan')
      })
    })

    describe('#ensureTable', async () => {
      afterEach(async () => { await helper.connections.source.dropTable('fish') })

      test('it will create a missing table', async () => {
        await helper.connections.source.ensureTable('fish')
        let tables = await helper.connections.source.queryTables()
        expect(tables).toContain('fish')
      })

      test('it will not blow up if the table exists already', async () => {
        // do it 2x
        await helper.connections.source.ensureTable('fish')
        await helper.connections.source.ensureTable('fish')
      })
    })

    describe('#dropTable', () => {
      afterEach(async () => { await helper.connections.source.dropTable('fish') })

      test('it will drop a table', async () => {
        await helper.connections.source.ensureTable('fish')
        await helper.connections.source.dropTable('fish')
        let tables = await helper.connections.source.queryTables()
        expect(tables).not.toContain('fish')
      })

      test('it will throw if the table does not exist', async () => {
        await helper.connections.source.dropTable('fish')
      })
    })

    test('#getColumns', async () => {
      let columns = await helper.connections.source.getColumns('users')
      expect(columns).toEqual(['id', 'created_at', 'email', 'first_name', 'last_name', 'updated_at'])
    })

    describe('#write', () => {
      const data = [
        {
          id: 1,
          fish: 'salmon',
          created_at: new Date(),
          updated_at: new Date()
        },
        {
          id: 2,
          fish: 'tuna',
          created_at: new Date(),
          updated_at: new Date()
        },
        {
          id: 3,
          fish: 'cod',
          created_at: new Date(),
          updated_at: new Date()
        }
      ]

      afterEach(async () => { await helper.connections.source.dropTable('fish') })

      test('it will create a new table', async () => {
        await helper.connections.source.write('fish', data)
        let tables = await helper.connections.source.queryTables()
        expect(tables).toContain('fish')
      })

      test('it will add coumns of the proper type', async () => {
        await helper.connections.source.write('fish', data)

        let totalRows = []
        const handler = (rows) => { totalRows = totalRows.concat(rows) }
        await helper.connections.source.read('fish', handler)

        expect(totalRows.length).toEqual(3)
        expect(totalRows[0].id).toEqual(1)
        expect(totalRows[0].fish).toEqual('salmon')
        expect(totalRows[0].created_at.getTime()).toEqual(totalRows[0].updated_at.getTime())
        expect(totalRows[0].created_at.getTime()).toEqual(totalRows[1].created_at.getTime())
      })

      test('it will update items with the same primary key', async () => {
        await helper.connections.source.write('fish', data)
        await helper.connections.source.write('fish', [ { id: 1, fish: 'super-salmon' } ])

        let totalRows = []
        const handler = (rows) => { totalRows = totalRows.concat(rows) }
        await helper.connections.source.read('fish', handler)

        expect(totalRows.length).toEqual(3)
        expect(totalRows[0].id).toEqual(1)
        expect(totalRows[0].fish).toEqual('super-salmon')
      })
    })
  })
})
