const fs = require('fs')
const csvParse = require('csv-parse/lib/sync')

module.exports = class CSV {
  constructor (options) {
    const defaultOptions = {
      columns: true,
      cast: true,
      trim: true,
      cast_date: true
    }

    this.options = Object.assign({}, defaultOptions, options)
  }

  parse (file) {
    let contents = fs.readFileSync(file)
    return csvParse(contents, this.options)
  }
}
