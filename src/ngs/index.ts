import type { JsonPrimitive } from 'type-fest'
import {
  readFileSync,
  writeFileSync,
} from 'node:fs'
import * as cheerio from 'cheerio'
import {
  createRegExp,
  digit,
  exactly,
  letter,
  maybe,
} from 'magic-regexp/further-magic'
import papaparse from 'papaparse'
import { resolve } from 'pathe'
import {
  get,
  group,
  isEmpty,
  isString,
  range
  ,
} from 'radash'

const codesSource = '/Users/benjaminkoplin/Library/CloudStorage/OneDrive-ReedSmithLLP/Downloads/NGS CPT CODE 2024 DOJ_Gutekunst000107.csv'

const remoteURL = 'https://www.cms.gov/medicare-coverage-database/view/article.aspx?articleId=56199'

type NgRecord = Record<'Code' | 'Description' | 'Related Party' | 'Category' | 'Code, if Crosswalked' | 'Prelim Rate 2019', JsonPrimitive>

const codeObjects = papaparse.parse < NgRecord>(readFileSync(codesSource, 'utf-8'), { header: true })

const codesToSearch: string[] = []

const codeVariableRegexp = createRegExp(digit.times.atLeast(1).at.lineStart(), exactly('X').as('variable'), digit.times.atLeast(1), maybe(letter).at.lineEnd())

const relevantTableRegexp = createRegExp('gdvHcpcsCodes')

const codeReplacer = range(0, 9)

fetchAndParseTables(remoteURL)

const codeFields: Array<keyof NgRecord> = ['Code', 'Code, if Crosswalked']

const g = group(codeObjects.data, o => get(o, 'Category', 'EMPTY'))

for (const field of codeFields) {
  for (const row of codeObjects.data) {
    if (isString(row[field]) && !isEmpty(row[field])) {
      const isVariableCode = codeVariableRegexp.test(row[field])

      if (isVariableCode) {
        for (const replacement of codeReplacer) {
          const newCode = row[field].replaceAll('X', `${replacement}`)

          addCodeToSearch(newCode)
        }
      }
      else {
        addCodeToSearch(row[field])
      }
    }
  }
}
function addCodeToSearch(newCode: string): void {
  if (!codesToSearch.includes(newCode))
    codesToSearch.push(newCode)
}
async function fetchAndParseTables(url: string): Promise<void> {
  const stringRows: string[] = []

  try {
    // const response = await axios.get(url)
    const response = readFileSync(resolve('./src/ngs/Article - Billing and Coding_ Molecular Pathology Procedures (A56199).html'), 'utf-8')

    const $ = cheerio.load(response)

    const tables = $('table').filter((i, el) => relevantTableRegexp.test($(el).attr('id') ?? ''))

    tables.each((index, el) => {
      const table = $(el)

      const descriptionSection = table.siblings('.document-view-section-text').children('p').filter(i => i < 2).map((i, el) => $(el).text().trim()).toArray().join('\t')

      const rows = table.find('tr')

      rows.each((rowIndex, row) => {
        const cells = $(row).find('td, th')

        const rowData = cells.map((_i, cell) => $(cell).text()?.trim() || '').toArray()

        if (rowIndex === 0)
          stringRows.push(`${rowData.join('\t')}\tCode Type\tType Description`)

        else stringRows.push(`${rowData.join('\t')}\t${descriptionSection}`)
      })
    })
    writeFileSync('./src/ngs/tableRows.csv', stringRows.join('\n'), 'utf-8')
  }
  catch (error) {
    console.error('Error fetching or parsing the HTML:', error)
  }
}
