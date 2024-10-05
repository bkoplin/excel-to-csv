import { readFileSync } from 'node:fs'
import { inspect } from 'node:util'
import XLSX from 'xlsx'
import * as cherry from 'cheerio'
import { padStart } from 'lodash-es'

/* obtain HTML string.  This example reads from SheetJSTable.html */
const html_str = readFileSync('/Users/benkoplin/Library/CloudStorage/OneDrive-SharedLibraries-ReedSmithLLP/Illumina - CID 23-1561 - Documents/Facts - Product On-Market Support/Confluence/Confluence-space-export-203141-6 LRM Manager/906863771.html', 'utf8')

/* get first TABLE element */
const doc = cherry.load(html_str)

doc('.columnLayout.three-equal table').each((i, el) => {
  console.log(inspect(el, { colors: true }))
  /* generate workbook */
  const ws = XLSX.utils.table_to_sheet(el)
  const workbook = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(workbook, ws, 'Sheet1')
  XLSX.writeFile(workbook, `./out/906863771_${padStart(`${i}`, 3, '0')}.xlsx`)
})
