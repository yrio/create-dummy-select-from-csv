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

async function main() {
    if (process.argv.length < 3) {
        console.log('Usage: node index.js <path to input directory>');
        return;
    }

    const inputDirname = process.argv[2];

    process.on('unhandledRejection', (reason, promise) => {
        throw reason;
    });

    const inputDirnameContents = await myfs.readdir(inputDirname);
    listDir(inputDirname, inputDirnameContents);
}

async function listDir(dirPath, filenameArr) {
    for (const filename of filenameArr) {
        const filePath = path.join(dirPath, filename);

        const stats = await myfs.stat(filePath);
        if (stats.isDirectory()) {
            const filePathContents = await myfs.readdir(filePath);
            listDir(filePath, filePathContents);
        }
        else if (stats.isFile() && path.extname(filename) === '.csv') {
            const [outputFilePath, inputFilenameWithoutExtension] = fn.computeOutputFilePathFor(dirPath, filename);
            console.info(`begin processing CSV file ${filename}`);

            const rawCsvContent = await myfs.readFile(filePath);
            const csvDataArr = await parseCsv(rawCsvContent);

            if (csvDataArr.length <= 1) {
                try {
                    await myfs.unlink(outputFilePath);
                    console.info(`Delete file ${outputFilePath} success`);
                }
                catch (err) {
                    console.info(`Delete file ${outputFilePath} failed (maybe because the file did not exist)`);
                }

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

            await myfs.writeFile(outputFilePath, result);
            console.info(`Write result to ${outputFilePath} success`);
        }
    }
}