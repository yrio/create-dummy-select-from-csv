const fs = require('fs');
const path = require('path');
const util = require('util');
const parseCsv = util.promisify(require('csv-parse'));

const myfs = {
    readdir: util.promisify(fs.readdir),
    stat: util.promisify(fs.stat),
    readFile: util.promisify(fs.readFile),
    unlink: util.promisify(fs.unlink),
    writeFile: util.promisify(fs.writeFile)
};

class ColumnMetadata {
    constructor(columnName, dataType) {
        this.name = columnName;
        this.dataType = dataType;
    }
}

const listDir = async (dirPath, filenameArr) => {
    for (const filename of filenameArr) {
        const filePath = path.join(dirPath, filename);

        const stats = await myfs.stat(filePath);
        if (stats.isDirectory()) {
            const filePathContents = await myfs.readdir(filePath);
            listDir(filePath, filePathContents);
        }
        else if (stats.isFile() && path.extname(filename) === '.csv') {
            const inputFilenameWithoutExtension = path.basename(filename, '.csv');
            const outputFilename = inputFilenameWithoutExtension + '.sql';
            const outputFilePath = path.join(dirPath, outputFilename);
            console.info(`begin processing CSV file ${inputFilenameWithoutExtension}`);

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
            const columnMetadataMap = computeMetadataFrom(csvDataArr[0]);

            // compute select statements string from CSV rows
            const selectStatements = computeUnionedSelectStatementsFrom(csvDataArr, columnMetadataMap);
            const sqlObjectType = `rec_${inputFilenameWithoutExtension}`;
            const sqlObjectConstructorParamString = computeSqlObjectConstructorParamString(columnMetadataMap);
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

function computeMetadataFrom(headerRow) {
    const regex = /(\w+) \((\w+)\)/;
    const metadata = new Map();

    for (let i = 0; i < headerRow.length; i++) {
        const cell = headerRow[i];
        const match = cell.match(regex);
        const columnName = match[1];
        const dataType = match[2];
        metadata[i] = new ColumnMetadata(columnName, dataType);
    }

    return metadata;
}

function computeUnionedSelectStatementsFrom(csvRows, columnMetadataMap) {
    const selectStatementArr = [];

    for (let i = 1; i < csvRows.length; i++) {
        const row = csvRows[i];
        const selectStatement = computeSelectStatementFrom(row, columnMetadataMap);
        selectStatementArr.push(selectStatement);
    }

    const selectStatementString = selectStatementArr.reduce((combinedVal, currVal) => combinedVal + 'union all\n' + currVal);
    return selectStatementString;
}

function computeSelectStatementFrom(row, columnMetadataMap) {
    const columnSelectExprArr = [];

    for (let i = 0; i < row.length; i++) {
        let cell = row[i];
        let columnSelectExpr;

        switch (columnMetadataMap[i].dataType) {
            case 'date':
                columnSelectExpr = `date '${cell}' as ${columnMetadataMap[i].name}`;
                break;
            case 'string':
                columnSelectExpr = `'${cell}' as ${columnMetadataMap[i].name}`;
                break;
            default:
                if (cell === '') {
                    cell = 'null';
                }
                columnSelectExpr = `${cell} as ${columnMetadataMap[i].name}`;
        }

        columnSelectExprArr.push(columnSelectExpr);
    }

    const columnSelectsString = columnSelectExprArr.reduce((combinedVal, currVal) => combinedVal + ', ' + currVal);
    return `select ${columnSelectsString} from dual\n`;
}

function computeSqlObjectConstructorParamString(columnMetadataMap) {
    let result = '';
    // console.log(columnMetadataMap);

    for (let idx in columnMetadataMap) {
        const metadata = columnMetadataMap[idx];

        if (idx > 0) {
            result += ', ';
        }

        result += `rec.${metadata.name}`;
    }

    return result;
}