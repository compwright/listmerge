# listmerge

Combines CSV files by searching for matching values

## Features

* Works with CSV format
* Easy-to-use interactive commandline interface (CLI)
* In-memory BM25 fulltext mapping engine (similar to Lucene/Elastic Search)
* Streams input and output data

## Requirements

* Node.js 8+

## Installation

```bash
$ npm install -g listmerge
```

## Usage

```
$ node index.js --help
index.js [sources] --output target.csv

Options:
  --version  Show version number                                                       [boolean]
  --output   path to write the combined output CSV file to  [required] [default: "combined.csv"]
  --help     Show help                                                                 [boolean]
```

## License

MIT license
