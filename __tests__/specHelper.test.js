const SpecHelper = require('./specHelper')
const helper = new SpecHelper()

describe('specHelper', async () => {
  beforeAll(async () => { await helper.connect() })
  beforeAll(async () => { await helper.seed() })
  afterAll(async () => { await helper.end() })

  test('it created database tables from seeds', async () => {
    const tables = await helper.connections.source.listTables()
    expect(tables.length).toEqual(4)
  })

  test('the seed databases have contnet from the csvs', async () => {
    let usersCount = await helper.connections.source.count('users')
    expect(usersCount).toEqual(3)
  })
})
