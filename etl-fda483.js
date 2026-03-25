#!/usr/bin/env node
/**
 * ============================================================
 * 483IQ — FDA Form 483 ETL Pipeline
 * ============================================================
 * Ingests FDA inspection + Form 483 observation CSV data into
 * PostgreSQL (fda schema: inspections + observations tables).
 *
 * USAGE:
 *   node etl-fda483.js                    # download + full ingest
 *   node etl-fda483.js --file ./data.csv  # use local CSV file
 *   node etl-fda483.js --dry-run          # parse only, no DB writes
 *   node etl-fda483.js --validate         # validate row counts only
 *
 * ENV VARS (or .env file):
 *   DATABASE_URL=postgresql://user:pass@host:5432/dbname
 *   -- OR --
 *   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 * ============================================================
 */

'use strict';

const fs      = require('fs');
const path    = require('path');
const https   = require('https');
const http    = require('http');
const { Transform, pipeline } = require('stream');
const { promisify } = require('util');
const pipelineAsync = promisify(pipeline);

// ─── Load .env if present ─────────────────────────────────────
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
    const [k, ...v] = line.trim().split('=');
    if (k && !k.startsWith('#') && v.length) process.env[k] = v.join('=').replace(/^['"]|['"]$/g, '');
  });
}

// ─── Config ───────────────────────────────────────────────────
const CONFIG = {
  // FDA data sources (tried in order)
  FDA_CSV_URLS: [
    'https://www.accessdata.fda.gov/scripts/oig/oig_inspection_database_listing.cfm?output=csv',
    'http://datadashboard.fda.gov/oii/cd/inspections.csv',
    'https://www.accessdata.fda.gov/scripts/oig/oig_inspections_download.cfm',
  ],
  // Local cache path
  CACHE_DIR:    path.join(__dirname, 'data'),
  CACHE_FILE:   path.join(__dirname, 'data', 'fda483_latest.csv'),
  // PostgreSQL schema
  SCHEMA:       'fda',
  BATCH_SIZE:   500,      // rows per DB batch insert
  LOG_EVERY:    1000,     // log progress every N rows
};

const ARGS = {
  file:    process.argv.includes('--file')     ? process.argv[process.argv.indexOf('--file') + 1]    : null,
  dryRun:  process.argv.includes('--dry-run'),
  validate:process.argv.includes('--validate'),
};

// ─── Logger ───────────────────────────────────────────────────
const log = {
  info:  (...a) => console.log(`[${ts()}] ℹ️  INFO `, ...a),
  ok:    (...a) => console.log(`[${ts()}] ✅ OK   `, ...a),
  warn:  (...a) => console.warn(`[${ts()}] ⚠️  WARN `, ...a),
  error: (...a) => console.error(`[${ts()}] ❌ ERROR`, ...a),
  prog:  (...a) => process.stdout.write(`\r[${ts()}] ⏳ ${a.join(' ')}          `),
};
function ts() { return new Date().toISOString().replace('T',' ').slice(0,19); }

// ─── Category Classification ──────────────────────────────────
/**
 * Tags each 483 observation by regulatory category.
 * Based on FDA's own classification framework + GAMP 5 / 21 CFR 211 mappings.
 */
const CATEGORY_RULES = [
  { category: 'Data Integrity',       patterns: [/data integrit/i, /audit trail/i, /electronic record/i, /21 cfr part 11/i, /record retention/i, /falsif/i, /backdated/i] },
  { category: 'Process Controls',     patterns: [/process control/i, /in-process/i, /yield/i, /batch record/i, /deviation/i, /CAPA/i, /corrective action/i] },
  { category: 'Laboratory Controls',  patterns: [/laboratory/i, /lab control/i, /out.of.spec/i, /OOS/i, /stability/i, /analytical method/i, /testing/i, /specification/i] },
  { category: 'Equipment',            patterns: [/equipment/i, /calibrat/i, /qualif/i, /maintenance/i, /cleaning validation/i, /IQ\/OQ\/PQ/i, /instrument/i] },
  { category: 'Facilities & Utilities',patterns:[/facilit/i, /HVAC/i, /water system/i, /cleanroom/i, /environmental monitoring/i, /utility/i] },
  { category: 'Quality Systems',      patterns: [/quality unit/i, /quality system/i, /SOP/i, /procedure/i, /investigation/i, /change control/i, /supplier/i, /vendor/i] },
  { category: 'Materials Management', patterns: [/material/i, /raw material/i, /component/i, /container/i, /label/i, /quarantine/i, /receiv/i, /storage/i] },
  { category: 'Packaging & Labeling', patterns: [/packag/i, /label/i, /tamper/i, /expir/i] },
  { category: 'Sterility & Aseptic',  patterns: [/sterili/i, /aseptic/i, /endotoxin/i, /bioburden/i, /depyrogenation/i, /fill.finish/i] },
  { category: 'Computer Systems',     patterns: [/computer/i, /software/i, /system validation/i, /access control/i, /backup/i, /disaster recovery/i] },
];

function classifyObservation(text) {
  if (!text) return 'Uncategorized';
  for (const rule of CATEGORY_RULES) {
    if (rule.patterns.some(p => p.test(text))) return rule.category;
  }
  return 'Other / General';
}

function extractCFR(text) {
  if (!text) return null;
  const match = text.match(/21\s*CFR\s*[\d.()a-z]+/i);
  return match ? match[0].replace(/\s+/g,' ').trim() : null;
}

// ─── CSV Parser ───────────────────────────────────────────────
/**
 * Lightweight streaming CSV parser (no external deps needed).
 * Handles quoted fields, embedded commas, newlines in fields.
 */
class CSVParser extends Transform {
  constructor() {
    super({ objectMode: true });
    this._headers = null;
    this._buf = '';
    this._rowNum = 0;
  }

  _parseRow(line) {
    const fields = [];
    let cur = '', inQuote = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') {
        if (inQuote && line[i+1] === '"') { cur += '"'; i++; }
        else inQuote = !inQuote;
      } else if (c === ',' && !inQuote) {
        fields.push(cur.trim()); cur = '';
      } else {
        cur += c;
      }
    }
    fields.push(cur.trim());
    return fields;
  }

  _transform(chunk, enc, cb) {
    this._buf += chunk.toString();
    const lines = this._buf.split('\n');
    this._buf = lines.pop(); // keep incomplete line

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      const fields = this._parseRow(trimmed);

      if (!this._headers) {
        // Normalise header names
        this._headers = fields.map(h =>
          h.toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g,'')
        );
        continue;
      }

      this._rowNum++;
      const row = {};
      this._headers.forEach((h, i) => { row[h] = (fields[i] || '').trim(); });
      this.push(row);
    }
    cb();
  }

  _flush(cb) {
    if (this._buf.trim()) {
      const fields = this._parseRow(this._buf.trim());
      if (this._headers && fields.length > 1) {
        const row = {};
        this._headers.forEach((h, i) => { row[h] = (fields[i] || '').trim(); });
        this.push(row);
      }
    }
    cb();
  }
}

// ─── Row Mapper ───────────────────────────────────────────────
/**
 * Maps raw CSV row → { inspection, observations[] }
 * Handles both FDA inspection classification CSV and 483 observation CSVs.
 * Column names vary by FDA data source — we try multiple aliases.
 */
function mapRow(row) {
  const get = (...keys) => {
    for (const k of keys) {
      const val = row[k] || row[k.replace(/ /g,'_')] || row[k.toLowerCase()] || '';
      if (val) return val;
    }
    return '';
  };

  // Build inspection ID — use FEI + date as composite key if no direct ID
  const feiNumber  = get('fei_number','fei','firm_fei','establishment_id');
  const inspDate   = get('inspection_end_date','end_date','inspection_date','date_completed','date');
  const inspId     = get('inspection_id','id') || `${feiNumber}_${inspDate}`.replace(/\s+/g,'');

  const inspection = {
    inspection_id: inspId || null,
    fei_number:    feiNumber || null,
    facility:      get('legal_name','firm_name','company_name','facility','establishment_name') || null,
    street:        get('street_address','address','street') || null,
    city:          get('city','city_name') || null,
    state:         get('state_province','state','province') || null,
    zip:           get('zip_code','zip','postal_code') || null,
    country:       get('country_area','country') || null,
    district:      get('district','fda_district') || null,
    program:       get('program_area','program','product_type','product_type_description') || null,
    fiscal_year:   get('fiscal_year','fy') || null,
    inspection_end_date: inspDate || null,
    classification:get('classification','inspection_classification') || null,
    posting_date:  get('posting_date') || null,
  };

  // Build observations array from citation columns (if present)
  const observations = [];
  // Some FDA exports have observation_1 ... observation_N columns
  let obsIdx = 1;
  while (true) {
    const text = get(`observation_${obsIdx}`, `citation_${obsIdx}`, `obs_${obsIdx}`);
    if (!text) break;
    observations.push({
      inspection_id: inspId,
      citation_num:  obsIdx,
      citation_text: text,
      category:      classifyObservation(text),
      cfr_reference: extractCFR(text),
    });
    obsIdx++;
  }

  // Some exports have a single 'observations' / 'citation_text' column
  const singleObs = get('observations','citation_text','observation_text','obs_text');
  if (singleObs && observations.length === 0) {
    observations.push({
      inspection_id: inspId,
      citation_num:  1,
      citation_text: singleObs,
      category:      classifyObservation(singleObs),
      cfr_reference: extractCFR(singleObs),
    });
  }

  return { inspection, observations };
}

// ─── PostgreSQL Client ────────────────────────────────────────
/**
 * Lightweight PostgreSQL wrapper using the `pg` npm package.
 * Implements upsert (INSERT ... ON CONFLICT DO UPDATE) for deduplication.
 */
class DB {
  constructor() { this.pool = null; }

  async connect() {
    const { Pool } = require('pg');
    const connStr = process.env.DATABASE_URL;
    this.pool = connStr
      ? new Pool({ connectionString: connStr, ssl: { rejectUnauthorized: false } })
      : new Pool({
          host:     process.env.DB_HOST     || 'localhost',
          port:     parseInt(process.env.DB_PORT || '5432'),
          database: process.env.DB_NAME     || '483iq',
          user:     process.env.DB_USER     || 'postgres',
          password: process.env.DB_PASSWORD || '',
        });
    await this.pool.query('SELECT 1'); // test connection
    log.ok('Database connected');
  }

  async ensureSchema() {
    const schema = CONFIG.SCHEMA;
    await this.pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${schema}.inspections (
        inspection_id       TEXT PRIMARY KEY,
        fei_number          TEXT,
        facility            TEXT,
        street              TEXT,
        city                TEXT,
        state               TEXT,
        zip                 TEXT,
        country             TEXT,
        district            TEXT,
        program             TEXT,
        fiscal_year         TEXT,
        inspection_end_date DATE,
        classification      TEXT,
        posting_date        DATE,
        ingested_at         TIMESTAMPTZ DEFAULT NOW(),
        updated_at          TIMESTAMPTZ DEFAULT NOW()
      )
    `);

    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${schema}.observations (
        id                  SERIAL PRIMARY KEY,
        inspection_id       TEXT REFERENCES ${schema}.inspections(inspection_id) ON DELETE CASCADE,
        citation_num        INTEGER,
        citation_text       TEXT,
        category            TEXT,
        cfr_reference       TEXT,
        ingested_at         TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(inspection_id, citation_num)
      )
    `);

    // Indexes for common query patterns
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_insp_fei       ON ${schema}.inspections(fei_number)`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_insp_district  ON ${schema}.inspections(district)`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_insp_program   ON ${schema}.inspections(program)`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_obs_category   ON ${schema}.observations(category)`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS idx_obs_cfr        ON ${schema}.observations(cfr_reference)`);

    log.ok('Schema and tables verified');
  }

  async upsertInspections(rows) {
    if (!rows.length) return 0;
    const schema = CONFIG.SCHEMA;
    let upserted = 0;

    // Build batch upsert
    const vals = rows.map((r, i) => {
      const base = i * 14;
      return `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8},$${base+9},$${base+10},$${base+11},$${base+12},$${base+13},$${base+14})`;
    }).join(',');

    const params = rows.flatMap(r => [
      r.inspection_id, r.fei_number, r.facility, r.street,
      r.city, r.state, r.zip, r.country, r.district, r.program,
      r.fiscal_year, r.inspection_end_date || null, r.classification, r.posting_date || null,
    ]);

    const result = await this.pool.query(`
      INSERT INTO ${schema}.inspections
        (inspection_id, fei_number, facility, street, city, state, zip,
         country, district, program, fiscal_year, inspection_end_date,
         classification, posting_date)
      VALUES ${vals}
      ON CONFLICT (inspection_id) DO UPDATE SET
        facility            = EXCLUDED.facility,
        district            = EXCLUDED.district,
        program             = EXCLUDED.program,
        classification      = EXCLUDED.classification,
        posting_date        = EXCLUDED.posting_date,
        updated_at          = NOW()
      RETURNING inspection_id
    `, params);

    return result.rowCount;
  }

  async upsertObservations(rows) {
    if (!rows.length) return 0;
    const schema = CONFIG.SCHEMA;
    const vals = rows.map((r, i) => {
      const base = i * 5;
      return `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5})`;
    }).join(',');

    const params = rows.flatMap(r => [
      r.inspection_id, r.citation_num, r.citation_text, r.category, r.cfr_reference
    ]);

    const result = await this.pool.query(`
      INSERT INTO ${schema}.observations
        (inspection_id, citation_num, citation_text, category, cfr_reference)
      VALUES ${vals}
      ON CONFLICT (inspection_id, citation_num) DO UPDATE SET
        citation_text = EXCLUDED.citation_text,
        category      = EXCLUDED.category,
        cfr_reference = EXCLUDED.cfr_reference
      RETURNING id
    `, params);

    return result.rowCount;
  }

  async getRowCounts() {
    const schema = CONFIG.SCHEMA;
    const [i, o] = await Promise.all([
      this.pool.query(`SELECT COUNT(*) AS n FROM ${schema}.inspections`),
      this.pool.query(`SELECT COUNT(*) AS n FROM ${schema}.observations`),
    ]);
    return {
      inspections:  parseInt(i.rows[0].n),
      observations: parseInt(o.rows[0].n),
    };
  }

  async close() {
    if (this.pool) await this.pool.end();
  }
}

// ─── Downloader ───────────────────────────────────────────────
async function downloadFDAData(destPath) {
  fs.mkdirSync(path.dirname(destPath), { recursive: true });

  for (const url of CONFIG.FDA_CSV_URLS) {
    log.info(`Trying FDA source: ${url}`);
    try {
      await new Promise((resolve, reject) => {
        const proto = url.startsWith('https') ? https : http;
        const req = proto.get(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; 483IQ-ETL/1.0)',
            'Accept': 'text/csv,text/plain,*/*',
          },
          timeout: 30000,
        }, (res) => {
          if (res.statusCode === 301 || res.statusCode === 302) {
            // follow redirect once
            const redirectUrl = res.headers.location;
            log.info(`Redirect → ${redirectUrl}`);
            const rProto = redirectUrl.startsWith('https') ? https : http;
            rProto.get(redirectUrl, { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 30000 }, (r2) => {
              if (r2.statusCode !== 200) return reject(new Error(`HTTP ${r2.statusCode} on redirect`));
              const out = fs.createWriteStream(destPath);
              r2.pipe(out);
              out.on('finish', resolve);
              out.on('error', reject);
            }).on('error', reject);
            return;
          }
          if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode}`));
          const out = fs.createWriteStream(destPath);
          res.pipe(out);
          out.on('finish', resolve);
          out.on('error', reject);
        });
        req.on('error', reject);
        req.on('timeout', () => reject(new Error('Request timeout')));
      });

      // Verify it looks like CSV
      const sample = fs.readFileSync(destPath, 'utf8').slice(0, 500);
      if (sample.includes('<html') || sample.includes('<!DOCTYPE')) {
        throw new Error('Response was HTML, not CSV');
      }

      const size = fs.statSync(destPath).size;
      log.ok(`Downloaded ${(size/1024/1024).toFixed(2)} MB from ${url}`);
      return destPath;

    } catch (err) {
      log.warn(`Source failed (${url}): ${err.message}`);
      if (fs.existsSync(destPath)) fs.unlinkSync(destPath);
    }
  }
  throw new Error('All FDA data sources failed. Use --file <path> to supply a local CSV.');
}

// ─── Main ETL ─────────────────────────────────────────────────
async function run() {
  const startTime = Date.now();
  log.info('━'.repeat(60));
  log.info('483IQ FDA Form 483 ETL Pipeline starting');
  log.info(`Mode: ${ARGS.dryRun ? 'DRY RUN' : ARGS.validate ? 'VALIDATE' : 'FULL INGEST'}`);
  log.info('━'.repeat(60));

  // ── Step 1: Get CSV file ──────────────────────────────────
  let csvPath;
  if (ARGS.file) {
    csvPath = path.resolve(ARGS.file);
    if (!fs.existsSync(csvPath)) throw new Error(`File not found: ${csvPath}`);
    log.ok(`Using local file: ${csvPath}`);
  } else {
    log.info('Downloading latest FDA 483 inspection data...');
    csvPath = await downloadFDAData(CONFIG.CACHE_FILE);
  }

  const fileSize = (fs.statSync(csvPath).size / 1024 / 1024).toFixed(2);
  log.info(`Processing: ${csvPath} (${fileSize} MB)`);

  // ── Step 2: Connect to DB (skip in dry-run) ────────────────
  const db = new DB();
  if (!ARGS.dryRun && !ARGS.validate) {
    await db.connect();
    await db.ensureSchema();
  }

  // ── Step 3: Stream & ingest CSV ───────────────────────────
  const stats = { rows: 0, inspections: 0, observations: 0, errors: 0, skipped: 0 };
  const inspBatch = [], obsBatch = [];
  const seenIds = new Set();

  const flushBatch = async () => {
    if (ARGS.dryRun) {
      // Just count, don't write
      stats.inspections += inspBatch.length;
      stats.observations += obsBatch.length;
      inspBatch.length = 0;
      obsBatch.length = 0;
      return;
    }
    if (inspBatch.length) {
      const n = await db.upsertInspections([...inspBatch]);
      stats.inspections += n;
      inspBatch.length = 0;
    }
    if (obsBatch.length) {
      const n = await db.upsertObservations([...obsBatch]);
      stats.observations += n;
      obsBatch.length = 0;
    }
  };

  const csvStream = fs.createReadStream(csvPath, { encoding: 'utf8' });
  const parser = new CSVParser();
  csvStream.pipe(parser);

  for await (const row of parser) {
    stats.rows++;

    if (stats.rows % CONFIG.LOG_EVERY === 0) {
      log.prog(`Processed ${stats.rows.toLocaleString()} rows | inspections: ${stats.inspections} | observations: ${stats.observations}`);
    }

    let mapped;
    try {
      mapped = mapRow(row);
    } catch (err) {
      stats.errors++;
      log.warn(`Row ${stats.rows} map error: ${err.message}`);
      continue;
    }

    const { inspection, observations } = mapped;

    // Deduplication check
    if (!inspection.inspection_id) {
      stats.skipped++;
      continue;
    }
    if (seenIds.has(inspection.inspection_id)) {
      stats.skipped++;
      continue;
    }
    seenIds.add(inspection.inspection_id);

    inspBatch.push(inspection);
    obsBatch.push(...observations);

    if (inspBatch.length >= CONFIG.BATCH_SIZE) {
      await flushBatch();
    }
  }

  // Flush remaining
  await flushBatch();

  // ── Step 4: Validation ────────────────────────────────────
  console.log(''); // newline after progress
  log.info('Running row count validation...');

  let dbCounts = { inspections: 0, observations: 0 };
  if (!ARGS.dryRun && !ARGS.validate) {
    dbCounts = await db.getRowCounts();
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  log.info('━'.repeat(60));
  log.ok(`ETL COMPLETE in ${elapsed}s`);
  log.info(`  CSV rows processed:       ${stats.rows.toLocaleString()}`);
  log.info(`  Rows skipped (no ID/dup): ${stats.skipped}`);
  log.info(`  Parse errors:             ${stats.errors}`);
  log.info(`  Inspections upserted:     ${stats.inspections.toLocaleString()}`);
  log.info(`  Observations upserted:    ${stats.observations.toLocaleString()}`);
  if (!ARGS.dryRun) {
    log.info(`  DB total inspections:     ${dbCounts.inspections.toLocaleString()}`);
    log.info(`  DB total observations:    ${dbCounts.observations.toLocaleString()}`);

    // Validation check
    if (stats.inspections > 0 && dbCounts.inspections === 0) {
      log.warn('⚠️  Validation warning: rows were upserted but DB count is 0');
    } else {
      log.ok('Row count validation passed ✓');
    }
  }
  log.info('━'.repeat(60));

  await db.close();
}

// ─── Entry point ─────────────────────────────────────────────
run().catch(err => {
  log.error(err.message);
  if (process.env.DEBUG) console.error(err);
  process.exit(1);
});
