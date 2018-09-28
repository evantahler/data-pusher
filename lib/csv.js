const fs = require('fs')
const csvParse = require('csv-parse')
const csvStringify = require('csv-stringify')

module.exports = class CSV {
  constructor (options) {
    const defaultOptions = {
      columns: true,
      header: true,
      cast: true,
      trim: true,
      cast_date: true
    }

    this.maChunkSizeBeforePause = 1000
    this.options = Object.assign({}, defaultOptions, options)
  }

  async read (file, handler) {
    if (!handler || typeof handler !== 'function') {
      throw new Error('handler function is required')
    }

    return new Promise(async (resolve, reject) => {
      let data = []

      try {
        const readStream = fs.createReadStream(file)
        const csvStream = csvParse(this.options)
        readStream.pipe(csvStream)

        csvStream.on('data', async (chunk) => {
          data.push(chunk)

          if (data.length >= this.maChunkSizeBeforePause) {
            csvStream.pause()
            await handler(data)
            data = []
            csvStream.resume()
          }
        })

        csvStream.on('end', async () => {
          await handler(data)
          return resolve()
        })

        readStream.on('error', reject)
        csvStream.on('error', reject)
      } catch (error) {
        return reject(error)
      }
    })
  }

  async write (file, data) {
    return new Promise(async (resolve, reject) => {
      try {
        let columns = {}
        data.forEach((row) => {
          for (let key in row) {
            if (!columns[key]) { columns[key] = key }
          }
        })

        const writeStream = fs.createWriteStream(file)
        const options = Object.assign(this.options, columns)
        const csvStream = csvStringify(options)
        csvStream.pipe(writeStream)

        writeStream.on('error', reject)
        writeStream.on('close', resolve)
        csvStream.on('error', reject)

        for (let i in data) {
          csvStream.write(data[i])
        }
        // csvStream.write(data)

        writeStream.close()
      } catch (error) {
        return reject(error)
      }
    })
  }
}
