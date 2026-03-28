#!/usr/bin/env node
/**
 * 483IQ ETL — Supabase REST API loader
 * Loads fda483_sample.csv into Supabase via HTTP (no direct TCP needed)
 * Usage: node etl-rest.js [--file path/to/file.csv] [--dry-run]
 */
'use strict';
const fs = require('fs');
const { execSync } = require('child_process');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;
const DRY_RUN = process.argv.includes('--dry-run');
const FILE = process.argv.includes('--file')
  ? process.argv[process.argv.indexOf('--file') + 1]
  : 'data/fda483_sample.csv';

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
  process.exit(1);
}

const CATEGORIES = [
  ['Data Integrity',        ['data integrit','audit trail','electronic record','21 cfr part 11','record retention','falsif','backdated']],
  ['Process Controls',      ['process control','in-process','batch record','deviation','capa','corrective action']],
  ['Laboratory Controls',   ['laboratory','lab control','out-of-spec','oos','stability','analytical method','testing','specification']],
  ['Equipment',             ['equipment','calibrat','qualif','maintenance','cleaning validation','instrument']],
  ['Facilities & Utilities',['facilit','hvac','water system','cleanroom','environmental monitoring']],
  ['Quality Systems',       ['quality unit','quality system','sop','procedure','investigation','change control','supplier','vendor']],
  ['Materials Management',  ['material','raw material','component','container','quarantine','receiv','storage']],
  ['Sterility & Aseptic',   ['sterili','aseptic','endotoxin','bioburden']],
  ['Computer Systems',      ['computer','software','system validation','access control','backup']],
];

function classify(text) {
  if (!text) return 'Other / General';
  const t = text.toLowerCase();
  for (const [cat, patterns] of CATEGORIES)
    if (patterns.some(p => t.includes(p))) return cat;
  return 'Other / General';
}

function extractCFR(text) {
  if (!text) return null;
  const m = text.match(/21\s*CFR\s*[\d.()a-zA-Z]+/i);
  return m ? m[0].trim() : null;
}

function parseCSV(content) {
  const lines = content.split('\n').filter(l => l.trim());
  const headers = lines[0].split(',').map(h => h.replace(/"/g,'').trim());
  return lines.slice(1).map(line => {
    const fields = []; let cur = '', inQ = false;
    for (const c of line) {
      if (c === '"') inQ = !inQ;
      else if (c === ',' && !inQ) { fields.push(cur.trim()); cur = ''; }
      else cur += c;
    }
    fields.push(cur.trim());
    const row = {};
    headers.forEach((h, i) => row[h] = (fields[i] || '').replace(/^"|"$/g,'').trim());
    return row;
  });
}

function upsert(table, rows) {
  const data = JSON.stringify(rows);
  const result = execSync(
    `curl -s -X POST "${SUPABASE_URL}/rest/v1/${table}" ` +
    `-H "apikey: ${SUPABASE_KEY}" ` +
    `-H "Authorization: Bearer ${SUPABASE_KEY}" ` +
    `-H "Content-Type: application/json" ` +
    `-H "Prefer: resolution=merge-duplicates,return=minimal" ` +
    `-d '${data.replace(/'/g, "'\\''")}'`,
    { encoding: 'utf8' }
  );
  return result;
}

const content = fs.readFileSync(FILE, 'utf8');
const rows = parseCSV(content);
console.log(`Parsed ${rows.length} rows from ${FILE}`);

const inspections = [], observations = [], seen = new Set();
for (const row of rows) {
  const fei = row['FEI Number'];
  const endDate = row['Inspection End Date'] || null;
  const id = `${fei}_${endDate}`.replace(/\s/g,'');
  if (!id || seen.has(id)) continue;
  seen.add(id);
  inspections.push({ inspection_id:id, fei_number:fei||null, facility:row['Legal Name']||null,
    street:row['Street Address']||null, city:row['City']||null, state:row['State/Province']||null,
    zip:row['Zip Code']||null, country:row['Country/Area']||null, district:row['District']||null,
    program:row['Program Area']||null, fiscal_year:row['Fiscal Year']||null,
    inspection_end_date:endDate, classification:row['Classification']||null,
    posting_date:row['Posting Date']||null });
  for (let n=1; n<=3; n++) {
    const text = row[`Observation_${n}`];
    if (text) observations.push({ inspection_id:id, citation_num:n, citation_text:text,
      category:classify(text), cfr_reference:extractCFR(text) });
  }
}

console.log(`→ ${inspections.length} inspections, ${observations.length} observations`);
if (DRY_RUN) { console.log('Dry run — no DB writes'); process.exit(0); }

const BATCH = 100;
let ok1 = 0;
for (let i=0; i<inspections.length; i+=BATCH) {
  upsert('fda_inspections', inspections.slice(i,i+BATCH));
  ok1 += Math.min(BATCH, inspections.length-i);
  process.stdout.write(`\r  Inspections: ${ok1}/${inspections.length}`);
}
console.log(' ✅');

let ok2 = 0;
for (let i=0; i<observations.length; i+=BATCH) {
  upsert('fda_observations', observations.slice(i,i+BATCH));
  ok2 += Math.min(BATCH, observations.length-i);
  process.stdout.write(`\r  Observations: ${ok2}/${observations.length}`);
}
console.log(' ✅');
console.log(`\nETL complete — ${ok1} inspections + ${ok2} observations`);
