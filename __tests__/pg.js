const SpecHelper = require('./specHelper')
const helper = new SpecHelper()

describe('specHelper', async () => {
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
  })
})
