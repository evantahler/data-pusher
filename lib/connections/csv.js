const fs = require('fs')
const { EventEmitter } = require('events')
const csvParse = require('csv-parse')
const csvStringify = require('csv-stringify')
const moment = require('moment')

module.exports = class CSV extends EventEmitter {
  constructor (options) {
    super()

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

  log (message, level, data) {
    this.emit('log', `[${this.name}] ${message}`, level, data)
  }

  async connect () {
    // noop
  }

  async end () {
    // noop
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
            if (data.length > 0) { this.log(`  got ${data.length} records from ${file}`) }
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

        const formatters = {
          date: (value) => {
            return moment(value).format('YYYY-MM-DD HH:mm:ss')
          }
        }

        const writeStream = fs.createWriteStream(file)
        const options = Object.assign(this.options, { columns, formatters })
        const csvStream = csvStringify(options)
        csvStream.pipe(writeStream)

        writeStream.on('error', reject)
        writeStream.on('close', resolve)
        csvStream.on('error', reject)

        for (let i in data) {
          csvStream.write(data[i])
        }
        this.log(`  wrote ${data.length} records to ${file}`)

        csvStream.end()
      } catch (error) {
        return reject(error)
      }
    })
  }
}
