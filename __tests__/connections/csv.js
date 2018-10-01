const CSV = require('./../../lib/connections/csv')
const path = require('path')
const fs = require('fs')

describe('connection', async () => {
  describe('csv', async () => {
    beforeAll(() => { csv.maChunkSizeBeforePause = 1 })
    afterAll(() => { csv.maChunkSizeBeforePause = 1000 })
    const csv = new CSV()

    test('can read CSV files in batches', async () => {
      const file = path.join(__dirname, '..', 'seeds', 'users.csv')
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

    describe('#write', () => {
      const file = '/tmp/fish.csv'
      let contents
      let date = new Date(Date.parse('2018-01-01 12:30'))
      const data = [
        {
          id: 1,
          fish: 'salmon',
          created_at: date,
          updated_at: date
        },
        {
          id: 2,
          fish: 'tuna',
          created_at: date,
          updated_at: date
        },
        {
          id: 3,
          fish: 'cod',
          created_at: date,
          updated_at: date
        }
      ]

      beforeAll(async () => {
        await csv.write(file, data)
        contents = fs.readFileSync(file).toString().split('\n')
      })

      afterAll(() => { fs.unlinkSync(file) })

      test('wrote the content', () => {
        expect(contents.length).toEqual(5)
      })

      test('is should have headers', () => {
        expect(contents[0]).toEqual('id,fish,created_at,updated_at')
      })

      test('stringified dates', async () => {
        expect(contents[1]).toContain('2018-01-01 12:30:00')
      })
    })
  })
})
