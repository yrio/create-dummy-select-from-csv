const fs = require('fs');
const path = require('path');
const util = require('util');
const parseCsv = util.promisify(require('csv-parse'));
const fn = require('./common/functions.js');

const myfs = {
    readdir: util.promisify(fs.readdir),
    stat: util.promisify(fs.stat),
    readFile: util.promisify(fs.readFile),
    unlink: util.promisify(fs.unlink),
    writeFile: util.promisify(fs.writeFile)
};

main();

function main() {
    if (process.argv.length < 3) {
        console.log('Usage: node index.js <path to input directory>');
        return;
    }

    const inputDirname = process.argv[2];

    process.on('unhandledRejection', (reason, promise) => {
        throw reason;
    });

    myfs.readdir(inputDirname).then(listDir.bind(null, inputDirname));
}

function listDir(dirPath, filenameArr) {
    for (const filename of filenameArr) {
        const filePath = path.join(dirPath, filename);

        myfs.stat(filePath).then(stats => {
            if (stats.isDirectory()) {
                myfs.readdir(filePath).then(listDir.bind(null, filePath));
            }
            else if (stats.isFile() && path.extname(filename) === '.csv') {
                const [outputFilePath, inputFilenameWithoutExtension] = fn.computeOutputFilePathFor(dirPath, filename);
                console.info(`begin processing CSV file ${inputFilenameWithoutExtension}`);

                /* the .then(csvDataArr) line is where the Promise approach differs from callback
                */
                myfs.readFile(filePath).then(rawCsvContent => {
                    return parseCsv(rawCsvContent);
                }).then(csvDataArr => {
                    if (csvDataArr.length <= 1) {
                        myfs.unlink(outputFilePath).then(() => {
                            console.info(`Delete file ${outputFilePath} success`);
                        }, err => {
                            console.info(`Delete file ${outputFilePath} failed (maybe because the file did not exist)`);
                        });

                        return;
                    }
                    
                    // compute metadata from header row
                    const columnMetadataMap = fn.computeMetadataFrom(csvDataArr[0]);

                    // compute select statements string from CSV rows
                    const selectStatements = fn.computeUnionedSelectStatementsFrom(csvDataArr, columnMetadataMap);
                    const sqlObjectType = `rec_${inputFilenameWithoutExtension}`;
                    const sqlObjectConstructorParamString = fn.computeSqlObjectConstructorParamString(columnMetadataMap);
                    const result = `cursor c1 is ${selectStatements}

temp ${sqlObjectType};
begin
for rec in c1
loop
    temp := ${sqlObjectType}(${sqlObjectConstructorParamString});
    pipe row (temp);
end loop;
end;
`;

                    myfs.writeFile(outputFilePath, result).then(() => {
                        console.info(`Write result to ${outputFilePath} success`);
                    });
                });
            }
        });
    }
}