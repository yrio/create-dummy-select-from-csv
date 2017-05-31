const fs = require('fs');
const path = require('path');
const parseCsv = require('csv-parse');
const fn = require('./common/functions.js');

main();

function main() {
    if (process.argv.length < 3) {
        console.log('Usage: node index.js <path to input directory>');
        return;
    }

    const inputDirname = process.argv[2];
    fs.readdir(inputDirname, listDir.bind(null, inputDirname));
}

function listDir(dirPath, err, filenameArr) {
    if (err) {
        throw err;
    }

    for (const filename of filenameArr) {
        const filePath = path.join(dirPath, filename);

        fs.stat(filePath, (err, stats) => {
            if (err) {
                throw err;
            }

            if (stats.isDirectory()) {
                fs.readdir(filePath, listDir.bind(null, filePath));
            }
            else if (stats.isFile() && path.extname(filename) === '.csv') {
                const [outputFilePath, inputFilenameWithoutExtension] = fn.computeOutputFilePathFor(dirPath, filename);
                console.info(`begin processing CSV file ${inputFilenameWithoutExtension}`);

                fs.readFile(filePath, (err, csvContents) => {
                    if (err) {
                        throw err;
                    }

                    parseCsv(csvContents, (err, csvDataArr) => {
                        if (err) {
                            throw err;
                        }

                        if (csvDataArr.length <= 1) {
                            // delete existing output file, if any
                            fs.unlink(outputFilePath, err => {
                                if (err) {
                                    console.info(`Delete file ${outputFilePath} failed (maybe because the file did not exist)`);
                                }
                                else {
                                    console.info(`Delete file ${outputFilePath} success`);
                                }
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

                        // write result to output file
                        fs.writeFile(outputFilePath, result, err => {
                            if (err) {
                                throw err;
                            }

                            console.info(`Write result to ${outputFilePath} success`);
                        });
                    });
                });
            }
        });
    }
}