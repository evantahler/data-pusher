const CSV = require('./../lib/csv')
const path = require('path')

describe('csv', async () => {
  const csv = new CSV()

  describe('maChunkSizeBeforePause', () => {
    beforeAll(() => { csv.maChunkSizeBeforePause = 1 })
    afterAll(() => { csv.maChunkSizeBeforePause = 1000 })

    test('can read CSV files in batches', async () => {
      const file = path.join(__dirname, 'seeds', 'users.csv')
      let data = []
      let handlerCalls = 0
      const handler = (chunk) => {
        data = data.concat(chunk)
        handlerCalls++
      }

      await csv.read(file, handler)
      expect(handlerCalls).toBeGreaterThanOrEqual(3)
      expect(data.length).toEqual(3)
    })

    test('can write csv files', async () => {
      const file = '/tmp/test.csv'
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

      await csv.write(file, data)
    })
  })
})
