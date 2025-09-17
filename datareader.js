// save_dataflows_to_sqlite_fix.js
const Database = require('better-sqlite3');

const URL_RAW = 'https://raw.githubusercontent.com/ILinIky/TEST_META/refs/heads/main/datenbank_export%20(22).json';
const DB_PATH = process.argv[2] || './meta.db';

const fetchCompat = typeof fetch === 'function'
  ? fetch
  : (...args) => import('node-fetch').then(({ default: f }) => f(...args));

/* ---------- Parser-Utils (unverändert) ---------- */
function stripJsonComments(input){
  const s=String(input); let out='',inStr=false,q=null,esc=false;
  for(let i=0;i<s.length;i++){ const c=s[i],n=s[i+1];
    if(inStr){ out+=c; if(esc) esc=false; else if(c==='\\') esc=true; else if(c===q){inStr=false;q=null;} continue; }
    if(c=='"'||c=="'"){inStr=true;q=c;out+=c;continue;}
    if(c==='/'&&n==='/'){ while(i<s.length&&s[i]!=='\n') i++; out+='\n'; continue; }
    if(c==='/'&&n==='*'){ i+=2; while(i<s.length&&!(s[i]==='*'&&s[i+1]==='/')) i++; i++; continue; }
    out+=c;
  } return out;
}
function removeTrailingCommas(input){
  const s=String(input); let out='',inStr=false,q=null,esc=false;
  for(let i=0;i<s.length;i++){ const c=s[i];
    if(inStr){ out+=c; if(esc) esc=false; else if(c==='\\') esc=true; else if(c===q){inStr=false;q=null;} continue; }
    if(c=='"'||c=="'"){inStr=true;q=c;out+=c;continue;}
    if(c===','){ let j=i+1; while(j<s.length&&/\s/.test(s[j])) j++; if(s[j]==='}'||s[j]===']') continue; }
    out+=c;
  } return out;
}
function parseLooseJson(maybe){
  if(maybe && typeof maybe==='object') return maybe;
  if(typeof maybe!=='string') return null;
  let s=maybe.trim(); if(!s) return null;
  if((s.startsWith('"')&&s.endsWith('"'))||(s.startsWith("'")&&s.endsWith("'"))){
    const inner=s.slice(1,-1).trim();
    if((inner.startsWith('{')&&inner.endsWith('}'))||(inner.startsWith('[')&&inner.endsWith(']'))) s=inner;
  }
  s = removeTrailingCommas(stripJsonComments(s));
  try{
    const p=JSON.parse(s);
    if(typeof p==='string'){
      const inner=p.trim();
      if((inner.startsWith('{')&&inner.endsWith('}'))||(inner.startsWith('[')&&inner.endsWith(']'))){
        try{ return JSON.parse(removeTrailingCommas(stripJsonComments(inner))); }catch{}
      }
    }
    return p;
  }catch{
    try{ return JSON.parse(s.replace(/\\\//g,'/')); }catch{ return null; }
  }
}
/* ----------------------------------------------- */

const toArray = (v) =>
  Array.isArray(v) ? v
  : (v && typeof v==='object' && Array.isArray(v.items)) ? v.items
  : (v && typeof v==='object' && Array.isArray(v.data)) ? v.data
  : (v && typeof v==='object' && Array.isArray(v.rows)) ? v.rows
  : [];

function collectBlocksDeep(node, path = '$', acc = []){
  if (!node || typeof node !== 'object') return acc;
  if (Array.isArray(node.blocks)){
    for (let i=0;i<node.blocks.length;i++){
      acc.push({ b: node.blocks[i], path: `${path}.mappings[${i}]` });
    }
  }
  if (Array.isArray(node)){
    for (let i=0;i<node.length;i++){
      collectBlocksDeep(node[i], `${path}[${i}]`, acc);
    }
    return acc;
  }
  for (const [k,v] of Object.entries(node)){
    if (v && typeof v === 'object'){
      collectBlocksDeep(v, `${path}.${k}`, acc);
    }
  }
  return acc;
}

(async function main(){
  // ---- 1) Daten laden + extrahieren
  const res = await fetchCompat(URL_RAW);
  if(!res.ok) throw new Error(`Fetch failed: ${res.status} ${res.statusText}`);
  let root = await res.json();

  const all = toArray(root).length ? toArray(root)
            : Array.isArray(root)   ? root
            : Object.values(root);

  const dataflows = all.filter(e => String(e?.category ?? '').toLowerCase() === 'datareader');
  console.log(`Gefundene DataReader: ${dataflows.length}`);
  const rows = [];
  for (let i=0;i<dataflows.length;i++){
    const entry = dataflows[i];
    const ProcedureName = entry.procedureName;
    const actionId = entry.subcategory;
    console.log(`Processing [${i+1}/${dataflows.length}] ${ProcedureName} / ${actionId}`);

    const textObj = parseLooseJson(entry?.text);
    ///console.log('textObj', textObj);
    console.log(textObj.mappings?.length ? `  → ${textObj.mappings.length} Blocks` : '  → No Blocks found');
    if (!textObj?.mappings?.length) continue;
    if (!textObj) continue;

    const ids = [];
for (let i = 0; i < textObj.mappings.length; i++) {
  const id = textObj.mappings[i]?.mdbUniqueId;
  const id2 = textObj.mappings[i]?.sqlFieldUniqueId;
  //if (id) ids.push(id);
const cubeid = 20;
const cubeid2 = 201;
const datareadername = textObj.name;

   // const found = collectBlocksDeep(textObj, `$[${i}].text`);
   

      //const n = b?.cubeIdx ?? b?.cubeidx ?? null;
      //const cubeIdx = Number.isFinite(+n) ? Math.trunc(+n) : null;
      rows.push({
        datareadername,
        cubeid2,
        cubeid,
        cubeid,
        id,
        id,
        id,
        id
      });
    }
  }

  // ---- 2) DB öffnen + Schema prüfen/erstellen
  const db = new Database(DB_PATH);
  db.pragma('foreign_keys = ON');
  db.pragma('journal_mode = WAL');

  // Prüfe, ob cubes Schema passt (muss Spalte idx haben, die UNIQUE/PK ist)
  const cubesInfo = db.prepare(`PRAGMA table_info(cubes)`).all();
  const cubesPkIdx = cubesInfo.find(c => c.name === 'idx');
  const hasCubes = cubesInfo.length > 0;
  if (hasCubes && !cubesPkIdx){
    console.warn('[WARN] Tabelle "cubes" existiert, hat aber keine Spalte "idx". Der FK kann so nicht funktionieren.');
  }
  db.exec(`drop table reader_blocks;`);
  db.exec(`
    CREATE TABLE IF NOT EXISTS cubes (
      idx INTEGER PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS reader_blocks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT,
      entity_idx INTEGER,
      cube_idx INTEGER,
      assignedGroup TEXT,
      connectionName TEXT,
      query TEXT,
      join_ TEXT,
      where_ TEXT,

      FOREIGN KEY (cube_idx)
    REFERENCES cubes(idx)
    ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED,

  FOREIGN KEY (entity_idx)
    REFERENCES entity(idx)
    ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_dataflow_blocks
      ON dataflow_blocks (procedure_name, action_id, cube_idx, translation, letter, block_label, source);
  `);

  

  // ---- 3) Eltern (cubes) zuerst einragen + Blocks speichern
  //const uniqueCubeIdx = [...new Set(rows.map(r => r.cubeIdx).filter(v => Number.isInteger(v)))];
  //const insCube = db.prepare(`INSERT OR IGNORE INTO cubes (idx) VALUES (?);`);
  const insRow  = db.prepare(`
    INSERT OR IGNORE INTO reader_blocks
      (name, entity_idx, cube_idx, assignedGroup, connectionName, query, join_,where_)
    VALUES (?,?,?,?,?,?,?,?);
  `);

  const tx = db.transaction(() => {
    //for (const c of uniqueCubeIdx) insCube.run(c);
    for (const r of rows){
      insRow.run(
        r.datareadername ?? null,
        r.entity_idx ?? null,
        r.cube_idx, // darf NULL sein
        r.assignedGroup ?? null,
        r.connectionName ?? null,
        r.query ?? null,
        r.join_ ?? null,
        r.where_ ?? null
      );
    }
  });

  try {
    tx();
  } catch (e) {
    console.error('Transaktion abgebrochen:', e.message);
    console.error('Hinweis: Existiert "cubes.idx" wirklich als Zeile für jeden verwendeten cube_idx?');
    const existing = new Set(db.prepare(`SELECT idx FROM cubes`).all().map(r => r.idx));
    const missing = [...new Set(rows.map(r => r.cubeIdx).filter(v => Number.isInteger(v) && !existing.has(v)))];
    if (missing.length) {
      console.error('Fehlende cube_idx in cubes:', missing.slice(0,50), missing.length > 50 ? `... (+${missing.length-50} weitere)` : '');
    }
    process.exit(1);
  }

  
  

  console.log(`OK: ${rows.length} Blocks gespeichert. DB: ${DB_PATH}`);
  console.log(`Views: v_action_cube_idx_list, v_procedure_cube_idx_list`);
  console.log(`Snapshot: action_summary (persistente cube_idx_list) aktualisiert.`);
})();
