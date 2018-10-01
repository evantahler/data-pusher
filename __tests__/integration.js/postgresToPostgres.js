const SpecHelper = require('./../specHelper')
const helper = new SpecHelper()

const ETL = require('./../../lib/etl.js')
const connections = {
  source: {
    type: 'pg',
    connectionString: process.env.TEST_SOURCE
  },
  destination: {
    type: 'pg',
    connectionString: process.env.TEST_DESTINATION
  }
}
const logLevel = 'error'
const etl = new ETL(connections, logLevel)

describe('integration', async () => {
  describe('postgres to postgres', async () => {
    beforeAll(async () => { await helper.connect() })
    beforeAll(async () => { await helper.seed() })
    afterAll(async () => { await helper.clearDestinationDatabase() })
    afterAll(async () => { await helper.end() })

    describe('initial run', () => {
      beforeAll(async () => {
        await etl.connect()
        const tables = await etl.connections.source.listTables()
        for (let i in tables) {
          let table = tables[i]
          await etl.connections.source.read(table, async (data) => {
            await etl.connections.destination.write(table, data)
          })
        }
        await etl.end()
      })

      test('created all tables', async () => {
        const destinationTables = await helper.connections.destination.listTables()
        expect(destinationTables).toEqual(['cart_products', 'carts', 'products', 'users'])
      })

      test('tables have primary keys', async () => {
        const usersDesribe = await helper.connections.destination.describeTable('users')
        expect(usersDesribe).toEqual([
          { column_name: 'id',
            data_type: 'bigint',
            character_maximum_length: null },
          { column_name: 'email',
            data_type: 'text',
            character_maximum_length: null },
          { column_name: 'first_name',
            data_type: 'text',
            character_maximum_length: null },
          { column_name: 'last_name',
            data_type: 'text',
            character_maximum_length: null },
          { column_name: 'created_at',
            data_type: 'timestamp without time zone',
            character_maximum_length: null },
          { column_name: 'updated_at',
            data_type: 'timestamp without time zone',
            character_maximum_length: null
          }
        ])

        const cartsDescribe = await helper.connections.destination.describeTable('carts')
        expect(cartsDescribe).toEqual([
          { column_name: 'id',
            data_type: 'bigint',
            character_maximum_length: null },
          { column_name: 'user_id',
            data_type: 'bigint',
            character_maximum_length: null
          }
        ])
      })

      test('tables have the proper number of rows', async () => {
        const cartProductsCount = await helper.connections.destination.count('cart_products')
        const cartsCount = await helper.connections.destination.count('carts')
        const productsCount = await helper.connections.destination.count('products')
        const usersCount = await helper.connections.destination.count('users')

        expect(cartProductsCount).toEqual(6)
        expect(cartsCount).toEqual(3)
        expect(productsCount).toEqual(3)
        expect(usersCount).toEqual(3)
      })

      test('spot check the data', async () => {
        let users = []
        const handler = async (rows) => { users = users.concat(rows) }
        await helper.connections.destination.read('users', handler)

        expect(users[0].first_name).toEqual('Evan')
        expect(users[1].first_name).toEqual('Brian')
        expect(users[2].first_name).toEqual('Molly')
      })
    })
  })
})
