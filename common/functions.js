const path = require('path');
const ColumnMetadata = require('./ColumnMetadata.js');

function computeOutputFilePathFor(dirPath, inputFilename) {
    const inputFilenameWithoutExtension = path.basename(inputFilename, '.csv');
    const outputFilename = inputFilenameWithoutExtension + '.sql';
    const outputFilePath = path.join(dirPath, outputFilename);
    return [outputFilePath, inputFilenameWithoutExtension];
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

module.exports = {
    computeOutputFilePathFor,
    computeMetadataFrom,
    computeUnionedSelectStatementsFrom,
    computeSelectStatementFrom,
    computeSqlObjectConstructorParamString
};