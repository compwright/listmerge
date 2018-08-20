#!/usr/bin/env node

const path = require('path');
const fs = require('fs');
const Stream = require('stream');
const Case = require('case');
const fromCsv = require('csv-parse');
const toCsv = require('csv-stringify');
const { csvParse } = require('d3-dsv');
const firstline = require('firstline');
const inquirer = require('inquirer');
const { zip, zipObject, fill, pick, flatten } = require('lodash');
const engine = require('wink-bm25-text-search')();
const nlpUtils = require('wink-nlp-utils');
const yargs = require('yargs');

const readable = new Stream.Readable({ objectMode: true });

function makeColName(file, name) {
    return Case.pascal(file.name) + '__' + name
}

async function getHeaders(files = []) {
    // resolve to absolute paths
    const paths = files.map(f => path.resolve(f));
    const names = paths.map(f => path.basename(f));

    // validate file paths
    paths.forEach(file => {
        if (!fs.existsSync(file)) {
            throw new Error('File not found: ' + file);
        }
    });

    // parse the header line from each file
    const headers = await Promise.all(
        paths.map(f => 
            firstline(f)
                .then(csvParse)
                .then(h => h.columns)
        )
    );

    return zip(paths, names, headers)
        .map(values => zipObject(['path', 'name', 'headers'], values))
        .map(file => {
            file.fileHeaders = file.headers
                .concat('match_certainty')
                .map(header => makeColName(file, header));
            return file;
        });
}

function loadSearchEngine(file) {
    return new Promise((resolve, reject) => {
        try {
            engine.defineConfig({
                fldWeights: zipObject(file.selected, fill(Array(file.selected.length), 1))
            });
              
            engine.definePrepTasks([
                nlpUtils.string.lowerCase,
                nlpUtils.string.removeExtraSpaces,
                nlpUtils.string.tokenize0
            ]);
        
            let rownum = 0;
            file.index = [];
    
            fs.createReadStream(file.path)
                .on('error', reject)
                .pipe(fromCsv())
                .on('error', reject)
                .on('data', (values) => {
                    if (rownum++ === 0) return; // skip header row
                    const row = zipObject(file.headers, values);
                    file.index.push(row);
                    engine.addDoc(pick(row, file.selected), file.index.length - 1);
                })
                .on('error', reject)
                .on('end', () => {
                    engine.consolidate();
                    file.engine = engine;
                    resolve(file);
                });
        } catch (err) {
            reject(err);
        }
    });
}

function matchFile(a, b) {
    return new Promise((resolve, reject) => {
        try {
            let rownum = 0;
    
            fs.createReadStream(b.path)
                .on('error', reject)
                .pipe(fromCsv())
                .on('error', reject)
                .on('data', (values) => {
                    if (rownum++ === 0) return; // skip header row

                    const query = Object.values(
                        pick(
                            zipObject(b.headers, values),
                            b.selected
                        )
                    ).join(' ');
    
                    const [ a_index, certainty ] = a.engine.search(query, 1)[0];
                    
                    Object.assign(
                        a.index[a_index],
                        zipObject(b.fileHeaders, values.concat(certainty))
                    );
                })
                .on('error', reject)
                .on('end', resolve);
        } catch (err) {
            reject(err);
        }
    });
}

(async () => {
    try {
        const argv = yargs
            .usage('$0 [sources] --output target.csv')
            .option('output', {
                description: 'path to write the combined output CSV file to',
                default: 'combined.csv',
                require: true
            })
            .help()
            .argv;

        const output = path.resolve(argv.output);
        const files = await getHeaders(argv._);

        if (files.length < 2) {
            throw new Error('At least two source CSV files are required');
        }

        // Configure the matching engine
        for (var i = 0; i < files.length; i++) {
            const file = files[i];

            switch (i) {
                case 0: {
                    console.log(`To configure the matching engine, we need to know a few things about ${file.name}.`);
                    const { selected } = await inquirer.prompt([
                        {
                            message: 'Which fields should be searchable?',
                            name: 'selected',
                            type: 'checkbox',
                            choices: file.headers
                        }
                    ]);
                    file.selected = selected;
                }
                break;

                default: {
                    console.log(`Tell us how to match ${file.name}:`);
                    const { selected } = await inquirer.prompt([
                        {
                            message: 'Which fields should be used to search?',
                            name: 'selected',
                            type: 'checkbox',
                            choices: file.headers
                        }
                    ]);
                    file.selected = selected;
                }
            }
        }

        console.log('Loading data...');
        const a = await loadSearchEngine(files[0]);

        // Add column headers that will be needed for matched records,
        // so that we can generate a CSV from a consistent row schema
        const addHeaders = flatten(files.filter((f, i) => i >= 1).map(f => f.fileHeaders));
        const addColumns = zipObject(addHeaders, fill(Array(addHeaders.length), undefined));
        a.index.forEach(row => Object.assign(row, addColumns));

        for (var i = 1; i < files.length; i++) {
            const b = files[i];
            console.log('Merging', b.name, '...');
            await matchFile(a, b);
        }

        console.log('Writing output to', output);

        readable
            .pipe(toCsv({ header: true }))
            .pipe(fs.createWriteStream(output));

        a.index.forEach(row => {
            readable.push(row);
        });
        readable.push(null);

        console.log('Done!');
        
    } catch (e) {
        console.error(e);
        process.exitCode = 1;
    }
})();
