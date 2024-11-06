// ==UserScript==
// @name         Download SAP Table Definition
// @namespace    local.CR
// @version      1.0.0
// @description  Generate XLSX  File From Steam Market History Page
// @match        https://leanx.eu/*
// @require      https://cdn.sheetjs.com/xlsx-0.20.3/package/dist/xlsx.full.min.js
// @require      https://cdn.jsdelivr.net/npm/lodash@4.17.21/lodash.min.js
// @require      https://cdn.jsdelivr.net/npm/jquery@3.7.1/dist/jquery.min.js
// @grant        GM_download
// @grant        GM_log
// @grant        GM_notification
// @run-at       document-end
// ==/UserScript==

/// <reference path="../node_modules/@types/tampermonkey/index.d.ts" />
/// <reference path="../node_modules/xlsx/types/index.d.ts" />
/// <reference path="../node_modules/@types/jquery/index.d.ts" />

(function () {
  'use strict'

  /** @type {import("../node_modules/xlsx/types/index.d.ts")} */
  const XLSX = globalThis.XLSX

  /** @type {jQuery} */
  const $ = globalThis.jQuery
  const title = $('title').text()
  const table = $('table.table.table-condensed.table-striped')
  const ws = XLSX.utils.table_to_book(table[0])
  XLSX.writeFile(ws, `${title}.xlsx`))
  // Your code here...
})()
