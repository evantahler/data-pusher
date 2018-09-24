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
      expect(max.getTime()).toEqual(1520074983000)
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
})
