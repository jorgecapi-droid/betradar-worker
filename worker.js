// BetRadar Cloudflare Worker — Full Analysis Backend v2
// Cron 5x/dia: 7h, 11h, 12h, 16h, 17h UTC
// 11h e 16h = lineup runs (só actualiza lineups)
// Dados: fixtures, odds, forma (20j), H2H+BTTS, stats+golos por período,
//        standings, previsões, lesões, stats avançadas, lineups, transfers, árbitros

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, x-apisports-key, x-api-key, anthropic-version, anthropic-dangerous-direct-browser-access',
};

const LEAGUE_IDS = [
  94, 39, 45, 40, 140, 78, 135, 61, 62, 88,
  144, 203, 179, 2, 3, 848, 71, 128, 262, 253,
  13, 5, 1, 32, 34, 4, 9, 6, 960, 79,
  11, 265, 307, 98, 292, 169, 95, 97, 96, 99,
  141, 137, 48, 218, 197, 119, 113, 207, 106, 283,
  103, 345, 271, 210, 333, 384, 357, 332, 172, 164,
  244, 239, 240, 281, 268, 285, 269,
  188, 193, 195, 192, 196, 19,
];

const BASE = 'https://v3.football.api-sports.io';
const TTL = 60 * 60 * 14;
const BATCH = 3;

// ── BETTING DAY (06:00 Lisboa → 06:00 Lisboa do dia seguinte) ──
// Resolve o problema de jogos sul-americanos (Libertadores, Brasileirão noite, MLS, Argentina)
// que começam após a meia-noite Lisboa mas pertencem ao "matchday" do dia anterior.
const BETTING_DAY_CUTOFF_HOUR = 6;

// Devolve a data Lisboa (YYYY-MM-DD) do início do "betting day" actual.
// Se agora são 03:00 Lisboa, o betting day actual ainda começou ontem.
function getBettingDayDate(date = new Date(), offsetDays = 0) {
  const lisbonStr = date.toLocaleString('en-CA', { timeZone: 'Europe/Lisbon', hour12: false });
  const [datePart, timePart] = lisbonStr.split(', ');
  const [ly, lm, ld] = datePart.split('-').map(Number);
  const [lh] = timePart.split(':').map(Number);
  let baseY = ly, baseM = lm, baseD = ld;
  if (lh < BETTING_DAY_CUTOFF_HOUR) {
    const d = new Date(Date.UTC(ly, lm - 1, ld));
    d.setUTCDate(d.getUTCDate() - 1);
    baseY = d.getUTCFullYear(); baseM = d.getUTCMonth() + 1; baseD = d.getUTCDate();
  }
  if (offsetDays !== 0) {
    const d = new Date(Date.UTC(baseY, baseM - 1, baseD));
    d.setUTCDate(d.getUTCDate() + offsetDays);
    baseY = d.getUTCFullYear(); baseM = d.getUTCMonth() + 1; baseD = d.getUTCDate();
  }
  return `${baseY}-${String(baseM).padStart(2, '0')}-${String(baseD).padStart(2, '0')}`;
}

// Bounds em ms UTC para o betting day a partir de uma data Lisboa YYYY-MM-DD
function getBettingDayBoundsFromDate(lisbonYmd) {
  const [y, m, d] = lisbonYmd.split('-').map(Number);
  // descobrir offset Lisboa nesse dia (probe às 12:00 UTC)
  const probe = new Date(Date.UTC(y, m - 1, d, 12, 0, 0));
  const lisbonProbe = probe.toLocaleString('en-CA', {
    timeZone: 'Europe/Lisbon', hour12: false,
    year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit'
  });
  const lisbonHour = parseInt(lisbonProbe.split(', ')[1].split(':')[0], 10);
  const utcOffsetMin = (lisbonHour - 12) * 60; // +60 verão, 0 inverno
  // 06:00 Lisboa nesse dia em ms UTC
  const startUtc = Date.UTC(y, m - 1, d, BETTING_DAY_CUTOFF_HOUR, 0, 0) - utcOffsetMin * 60000;
  return { startMs: startUtc, endMs: startUtc + 24 * 3600 * 1000 };
}

// Para a data Lisboa do betting day, devolve as datas de calendário API (YYYY-MM-DD) que
// é preciso pedir à API. Pode ser 1 (sempre o próprio dia) ou 2 (também o seguinte, para
// apanhar madrugada). 06:00→06:00 Lisboa cobre sempre 2 dias de calendário.
function getApiDatesForBettingDay(lisbonYmd) {
  const [y, m, d] = lisbonYmd.split('-').map(Number);
  const next = new Date(Date.UTC(y, m - 1, d));
  next.setUTCDate(next.getUTCDate() + 1);
  const nextYmd = `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, '0')}-${String(next.getUTCDate()).padStart(2, '0')}`;
  return [lisbonYmd, nextYmd];
}

async function batchAll(items, fn, size = BATCH, delay = 200) {
  const results = [];
  for (let i = 0; i < items.length; i += size) {
    const batch = items.slice(i, i + size);
    const r = await Promise.all(batch.map(fn));
    results.push(...r);
    if (i + size < items.length) await new Promise(r => setTimeout(r, delay));
  }
  return results;
}

async function fetchFixturesAndOdds(today, season, headers) {
  const allFixtures = [], allOdds = [];
  // `today` é a data Lisboa do início do betting day (ex: 2026-04-30).
  // O betting day cobre 06:00 Lisboa today → 06:00 Lisboa tomorrow,
  // logo precisamos de pedir AMBAS as datas de calendário à API.
  const [d1, d2] = getApiDatesForBettingDay(today);
  const { startMs, endMs } = getBettingDayBoundsFromDate(today);
  const seenFixIds = new Set();
  const seenOddsIds = new Set();
  await batchAll(LEAGUE_IDS, async (lid) => {
    for (const dateStr of [d1, d2]) {
      try {
        const rf = await fetch(`${BASE}/fixtures?league=${lid}&season=${season}&date=${dateStr}&timezone=Europe/Lisbon`, { headers });
        if (!rf.ok) continue;
        const fd = await rf.json();
        const fixtures = fd?.response || [];
        if (!fixtures.length) continue;
        // Filtrar pela janela betting-day e dedup por fixture id
        const inWindow = fixtures.filter(f => {
          const t = f.fixture?.date ? new Date(f.fixture.date).getTime() : 0;
          if (!t || t < startMs || t >= endMs) return false;
          const fid = f.fixture?.id;
          if (fid && seenFixIds.has(fid)) return false;
          if (fid) seenFixIds.add(fid);
          return true;
        });
        if (!inWindow.length) continue;
        allFixtures.push(...inWindow.map(f => ({ ...f, _lid: lid })));
        const ro = await fetch(`${BASE}/odds?league=${lid}&season=${season}&date=${dateStr}&timezone=Europe/Lisbon`, { headers });
        if (ro.ok) {
          const od = await ro.json();
          const oddsArr = od?.response || [];
          // Só guardar odds de fixtures que entraram (e dedup)
          oddsArr.forEach(o => {
            const fid = o.fixture?.id;
            if (!fid || !seenFixIds.has(fid) || seenOddsIds.has(fid)) return;
            seenOddsIds.add(fid);
            allOdds.push(o);
          });
        }
      } catch (e) { console.warn(`Fixtures/odds ${lid} ${dateStr}:`, e.message); }
    }
  }, BATCH, 500);
  return { allFixtures, allOdds };
}

async function fetchTeamForms(fixtures, season, headers, half = null) {
  const formData = {};
  const teams = new Map();
  fixtures.forEach(f => {
    const hId = f.teams?.home?.id, hName = f.teams?.home?.name;
    const aId = f.teams?.away?.id, aName = f.teams?.away?.name;
    const lid = f._lid;
    if (hId && hName) teams.set(hId, { id: hId, name: hName, lid });
    if (aId && aName) teams.set(aId, { id: aId, name: aName, lid });
  });
  let teamList = [...teams.values()];
  // half=1 → primeira metade, half=2 → segunda metade, null → tudo
  if (half === 1) teamList = teamList.slice(0, Math.ceil(teamList.length / 2));
  else if (half === 2) teamList = teamList.slice(Math.ceil(teamList.length / 2));
  console.log(`Form for ${teamList.length} teams (half=${half||'all'})...`);
  let okCount = 0, emptyCount = 0, failCount = 0;
  await batchAll(teamList, async ({ id, name, lid }) => {
    try {
      const r = await fetch(`${BASE}/fixtures?team=${id}&league=${lid}&season=${season}&last=10&status=FT`, { headers });
      if (!r.ok) { failCount++; console.warn(`Form HTTP ${r.status} for ${name} (lid=${lid})`); return; }
      const d = await r.json();
      const fl = d?.response || [];
      if (!fl.length) { emptyCount++; return; }
      okCount++;
      const formAll = [], formHome = [], formAway = [];
      // Estatísticas calculadas a partir dos últimos 5 jogos (mesma lógica do front-end)
      let goalsFor = 0, goalsAgainst = 0, cleanSheets = 0, bttsCount = 0, n = 0;
      let goalsForHome = 0, goalsAgainstHome = 0, nHome = 0;
      let goalsForAway = 0, goalsAgainstAway = 0, nAway = 0;
      const last5 = fl.slice(0, 5);
      fl.forEach(f => {
        const isHome = f.teams.home.id === id;
        const gh = f.goals.home ?? 0, ga = f.goals.away ?? 0;
        const res = gh === ga ? 'D' : isHome ? (gh > ga ? 'W' : 'L') : (ga > gh ? 'W' : 'L');
        formAll.push(res);
        if (isHome) formHome.push(res); else formAway.push(res);
      });
      last5.forEach(f => {
        const isHome = f.teams.home.id === id;
        const gh = f.goals.home ?? 0, ga = f.goals.away ?? 0;
        const gf = isHome ? gh : ga;
        const gAg = isHome ? ga : gh;
        goalsFor += gf; goalsAgainst += gAg;
        if (gAg === 0) cleanSheets++;
        if (gh > 0 && ga > 0) bttsCount++;
        n++;
        if (isHome) { goalsForHome += gf; goalsAgainstHome += gAg; nHome++; }
        else { goalsForAway += gf; goalsAgainstAway += gAg; nAway++; }
      });
      formData[id] = {
        name, tid: id,
        form: formAll.reverse().slice(0, 10),
        formHome: formHome.reverse().slice(0, 10),
        formAway: formAway.reverse().slice(0, 10),
        recentFixtures: fl.slice(0, 3).map(f => f.fixture?.id).filter(Boolean),
        attackAvg: n > 0 ? +(goalsFor / n).toFixed(2) : 0,
        defenseAvg: n > 0 ? +(goalsAgainst / n).toFixed(2) : 0,
        cleanSheets,
        bttsCount,
        gamesAnalyzed: n,
        attackAvgHome: nHome > 0 ? +(goalsForHome / nHome).toFixed(2) : 0,
        defenseAvgHome: nHome > 0 ? +(goalsAgainstHome / nHome).toFixed(2) : 0,
        attackAvgAway: nAway > 0 ? +(goalsForAway / nAway).toFixed(2) : 0,
        defenseAvgAway: nAway > 0 ? +(goalsAgainstAway / nAway).toFixed(2) : 0,
      };
    } catch (e) { failCount++; console.warn(`Form ${id}:`, e.message); }
  }, BATCH, 200);
  console.log(`Form summary: ok=${okCount}, empty=${emptyCount}, fail=${failCount}, total=${teamList.length}`);
  return formData;
}

async function fetchH2HData(fixtures, headers) {
  const h2hData = {};
  const pairs = new Map();
  fixtures.forEach(f => {
    const hId = f.teams?.home?.id, aId = f.teams?.away?.id;
    if (hId && aId) {
      const key = `${Math.min(hId, aId)}_${Math.max(hId, aId)}`;
      pairs.set(key, { key, hId, aId });
    }
  });
  const pairList = [...pairs.values()];
  console.log(`H2H for ${pairList.length} pairs...`);
  await batchAll(pairList, async ({ key, hId, aId }) => {
    try {
      const r = await fetch(`${BASE}/fixtures/headtohead?h2h=${hId}-${aId}&last=10`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const matches = d?.response || [];
      if (!matches.length) return;
      let t1w = 0, t2w = 0, draws = 0, over25 = 0, btts = 0, totalGoals = 0;
      matches.forEach(m => {
        const gh = m.goals.home ?? 0, ga = m.goals.away ?? 0;
        totalGoals += gh + ga;
        if (gh + ga > 2.5) over25++;
        if (gh > 0 && ga > 0) btts++;
        if (gh === ga) draws++;
        else if (m.teams.home.id === hId ? gh > ga : ga > gh) t1w++;
        else t2w++;
      });
      h2hData[key] = {
        total: matches.length,
        team1wins: t1w, team2wins: t2w, draws,
        over25, btts,
        avgGoals: (totalGoals / matches.length).toFixed(1),
      };
    } catch (e) { console.warn(`H2H ${key}:`, e.message); }
  }, BATCH, 200);
  return h2hData;
}

async function fetchTeamStats(fixtures, season, headers) {
  const statsData = {};
  const requests = new Map();
  fixtures.forEach(f => {
    const lid = f._lid;
    if (f.teams?.home?.id) requests.set(`${f.teams.home.id}_${lid}`, { tid: f.teams.home.id, lid });
    if (f.teams?.away?.id) requests.set(`${f.teams.away.id}_${lid}`, { tid: f.teams.away.id, lid });
  });
  const reqList = [...requests.entries()].map(([key, val]) => ({ key, ...val }));
  console.log(`Stats for ${reqList.length} combos...`);
  await batchAll(reqList, async ({ key, tid, lid }) => {
    try {
      const r = await fetch(`${BASE}/teams/statistics?team=${tid}&league=${lid}&season=${season}`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const s = d?.response;
      if (!s) return;
      const gfMin = s.goals?.for?.minute || {};
      const gaMin = s.goals?.against?.minute || {};
      statsData[key] = {
        goalsForAvgHome: parseFloat(s.goals?.for?.average?.home) || 0,
        goalsForAvgAway: parseFloat(s.goals?.for?.average?.away) || 0,
        goalsAgainstAvgHome: parseFloat(s.goals?.against?.average?.home) || 0,
        goalsAgainstAvgAway: parseFloat(s.goals?.against?.average?.away) || 0,
        winsHome: s.fixtures?.wins?.home || 0,
        winsAway: s.fixtures?.wins?.away || 0,
        drawsHome: s.fixtures?.draws?.home || 0,
        cleanSheetsHome: s.clean_sheet?.home || 0,
        cleanSheetsAway: s.clean_sheet?.away || 0,
        playedHome: s.fixtures?.played?.home || 0,
        playedAway: s.fixtures?.played?.away || 0,
        goalsFor1H: (gfMin['0-15']?.total||0)+(gfMin['16-30']?.total||0)+(gfMin['31-45']?.total||0),
        goalsFor2H: (gfMin['46-60']?.total||0)+(gfMin['61-75']?.total||0)+(gfMin['76-90']?.total||0),
        goalsAgainst1H: (gaMin['0-15']?.total||0)+(gaMin['16-30']?.total||0)+(gaMin['31-45']?.total||0),
        goalsAgainst2H: (gaMin['46-60']?.total||0)+(gaMin['61-75']?.total||0)+(gaMin['76-90']?.total||0),
      };
    } catch (e) { console.warn(`Stats ${key}:`, e.message); }
  }, BATCH, 200);
  return statsData;
}

async function fetchStandings(fixtures, season, headers) {
  const standingsData = {};
  const leaguesWithGames = [...new Set(fixtures.map(f => f._lid))];
  console.log(`Standings for ${leaguesWithGames.length} leagues...`);
  await batchAll(leaguesWithGames, async (lid) => {
    try {
      const r = await fetch(`${BASE}/standings?league=${lid}&season=${season}`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const standings = d?.response?.[0]?.league?.standings?.[0] || [];
      if (!standings.length) return;
      const table = {};
      standings.forEach(t => {
        table[t.team.id] = {
          pos: t.rank, pts: t.points,
          played: t.all?.played || 0,
          w: t.all?.win || 0, d: t.all?.draw || 0, l: t.all?.lose || 0,
          gf: t.all?.goals?.for || 0, ga: t.all?.goals?.against || 0,
          home: { played: t.home?.played||0, w: t.home?.win||0, d: t.home?.draw||0, l: t.home?.lose||0 },
          away: { played: t.away?.played||0, w: t.away?.win||0, d: t.away?.draw||0, l: t.away?.lose||0 },
          name: t.team.name, form: t.form || '',
        };
      });
      standingsData[`${lid}_${season}`] = table;
    } catch (e) { console.warn(`Standings ${lid}:`, e.message); }
  }, BATCH, 200);
  return standingsData;
}

async function fetchPredictionsAndInjuries(fixtures, headers) {
  const predData = {}, injData = {};
  const fids = fixtures.map(f => f.fixture?.id).filter(Boolean);
  console.log(`Predictions+injuries for ${fids.length} fixtures...`);
  await batchAll(fids, async (fid) => {
    try {
      const [rp, ri] = await Promise.all([
        fetch(`${BASE}/predictions?fixture=${fid}`, { headers }),
        fetch(`${BASE}/injuries?fixture=${fid}`, { headers }),
      ]);
      if (rp.ok) {
        const dp = await rp.json();
        const p = dp?.response?.[0];
        if (p) predData[fid] = {
          winPct: { home: p.predictions?.percent?.home, away: p.predictions?.percent?.away, draw: p.predictions?.percent?.draw },
          underOver: p.predictions?.under_over, advice: p.predictions?.advice,
        };
      }
      if (ri.ok) {
        const di = await ri.json();
        injData[fid] = (di?.response || []).map(i => ({ player: i.player?.name, team: i.team?.name, type: i.player?.type }));
      }
    } catch (e) { console.warn(`Pred/inj ${fid}:`, e.message); }
  }, 5, 200);
  return { predData, injData };
}

async function fetchAdvancedStats(formData, headers) {
  const advData = {};
  const teamList = Object.entries(formData).filter(([, d]) => d.recentFixtures?.length);
  console.log(`Advanced stats for ${teamList.length} teams...`);
  await batchAll(teamList, async ([tid, data]) => {
    try {
      const fids = data.recentFixtures.slice(0, 3);
      let shotsOn=0, shotsTotal=0, possession=0, corners=0, n=0;
      let cornersHome=0, cornersAway=0, nHome=0, nAway=0;
      await Promise.all(fids.map(async (fid) => {
        try {
          const [rs, rf] = await Promise.all([
            fetch(`${BASE}/fixtures/statistics?fixture=${fid}&team=${tid}`, { headers }),
            fetch(`${BASE}/fixtures?id=${fid}`, { headers }),
          ]);
          if (!rs.ok) return;
          const ds = await rs.json();
          const stats = ds?.response?.[0]?.statistics || [];
          const get = (t) => stats.find(s => s.type === t)?.value;
          const sOn = parseInt(get('Shots on Goal'))||0;
          const sTotal = parseInt(get('Total Shots'))||0;
          const poss = parseInt((get('Ball Possession')||'0%').replace('%',''))||0;
          const corn = parseInt(get('Corner Kicks'))||0;
          shotsOn+=sOn; shotsTotal+=sTotal; possession+=poss; corners+=corn; n++;
          if (rf.ok) {
            const df = await rf.json();
            const isHome = df?.response?.[0]?.teams?.home?.id === parseInt(tid);
            if (isHome){cornersHome+=corn;nHome++;}else{cornersAway+=corn;nAway++;}
          }
        } catch {}
      }));
      if (n>0) advData[tid] = {
        name: data.name,
        shotsOnAvg: parseFloat((shotsOn/n).toFixed(1)),
        shotsTotalAvg: parseFloat((shotsTotal/n).toFixed(1)),
        possessionAvg: parseFloat((possession/n).toFixed(0)),
        cornersAvg: parseFloat((corners/n).toFixed(1)),
        cornersHomeAvg: nHome>0?parseFloat((cornersHome/nHome).toFixed(1)):null,
        cornersAwayAvg: nAway>0?parseFloat((cornersAway/nAway).toFixed(1)):null,
        xgFromShots: parseFloat(((shotsOn/n)*0.33).toFixed(2)),
        games: n,
      };
    } catch (e) { console.warn(`Adv stats ${tid}:`, e.message); }
  }, 4, 300);
  return advData;
}

async function fetchLineups(fixtures, headers) {
  const lineupData = {};
  const fids = fixtures.map(f => f.fixture?.id).filter(Boolean);
  console.log(`Lineups for ${fids.length} fixtures...`);
  await batchAll(fids, async (fid) => {
    try {
      const r = await fetch(`${BASE}/fixtures/lineups?fixture=${fid}`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const lineups = d?.response || [];
      if (!lineups.length) return;
      lineupData[fid] = lineups.map(t => ({
        team: t.team?.name, teamId: t.team?.id, formation: t.formation,
        startXI: (t.startXI||[]).map(p => ({ name: p.player?.name, number: p.player?.number, pos: p.player?.pos })),
        substitutes: (t.substitutes||[]).map(p => ({ name: p.player?.name, pos: p.player?.pos })),
      }));
    } catch (e) { console.warn(`Lineups ${fid}:`, e.message); }
  }, 5, 200);
  return lineupData;
}

async function fetchTransfers(fixtures, headers) {
  const transferData = {};
  const teams = new Map();
  fixtures.forEach(f => {
    if (f.teams?.home?.id) teams.set(f.teams.home.id, f.teams.home.name);
    if (f.teams?.away?.id) teams.set(f.teams.away.id, f.teams.away.name);
  });
  const teamList = [...teams.entries()];
  console.log(`Transfers for ${teamList.length} teams...`);
  const now = new Date();
  const cutoff = new Date(now); cutoff.setMonth(cutoff.getMonth()-3);
  const cutoffStr = cutoff.toISOString().slice(0,10);
  await batchAll(teamList, async ([tid, name]) => {
    try {
      const r = await fetch(`${BASE}/transfers?team=${tid}`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const transfers = (d?.response||[]).filter(t => {
        const date = t.transfers?.[0]?.date;
        return date && date >= cutoffStr;
      }).slice(0,5).map(t => ({
        player: t.player?.name,
        type: t.transfers?.[0]?.type,
        date: t.transfers?.[0]?.date,
        teamIn: t.transfers?.[0]?.teams?.in?.name,
        teamOut: t.transfers?.[0]?.teams?.out?.name,
      }));
      if (transfers.length) transferData[tid] = { name, transfers };
    } catch (e) { console.warn(`Transfers ${tid}:`, e.message); }
  }, BATCH, 200);
  return transferData;
}

function extractReferees(fixtures) {
  const refereeData = {};
  fixtures.forEach(f => {
    if (f.fixture?.id && f.fixture?.referee) refereeData[f.fixture.id] = f.fixture.referee;
  });
  return refereeData;
}

// ── FASE 10: SCORING COMPLETO + TOPS ─────────────────────────────
function normalizeName(n){
  return(n||'').toLowerCase()
    .replace(/[àáâãäå]/g,'a').replace(/[èéêë]/g,'e').replace(/[ìíîï]/g,'i')
    .replace(/[òóôõöø]/g,'o').replace(/[ùúûü]/g,'u').replace(/[ýÿ]/g,'y')
    .replace(/[ñ]/g,'n').replace(/[ç]/g,'c').replace(/[şs]/g,'s').replace(/[ğg]/g,'g')
    .replace(/[ıi]/g,'i').replace(/[žz]/g,'z').replace(/[čc]/g,'c')
    .replace(/\b(fc|sc|ac|cf|afc|bfc|sv|rb|bsc|cd|rc|ss|as|ud|ca|cr|ec|se|sl|if|sk|fk|nk|hk|bk|ik|gd|rcd|kaa|kv|oh|aek|paok|rsc|real|club|atletico|sporting|sport|united|city|town|rovers|wanderers|athletic)\b/gi,'')
    .replace(/[^a-z0-9\s]/g,'').replace(/\s+/g,' ').trim();
}

function getTidFromAnalysis(name, formData){
  for(const [tid, d] of Object.entries(formData||{})){
    if(!d.name)continue;
    if(d.name===name)return parseInt(tid);
    const na=normalizeName(name), nb=normalizeName(d.name);
    if(na&&nb&&na.length>3&&nb.length>3&&(na===nb||na.includes(nb)||nb.includes(na)))return parseInt(tid);
  }
  return null;
}

function betSideWorker(pick){
  const b=pick.bet||'';
  const has=(s,tok)=>new RegExp(`(^|[\\s+])${tok}([\\s+]|$)`).test(s);
  if(has(b,'1X')||b.startsWith('Vitória '+pick.home))return 'home';
  if(has(b,'X2')||b.startsWith('Vitória '+pick.away))return 'away';
  if(has(b,'12'))return 'either';
  return 'none';
}

function calcXGWorker(hs, as_, hName, aName, advData){
  const hAdv=hName?advData[hName]:null;
  const aAdv=aName?advData[aName]:null;
  if(hAdv&&aAdv){
    const hShots=hAdv.shotsOnAvg||0;
    const aShots=aAdv.shotsOnAvg||0;
    const hXG=parseFloat((hShots*0.30*1.1).toFixed(2));
    const aXG=parseFloat((aShots*0.30).toFixed(2));
    return{home:Math.max(0.3,hXG),away:Math.max(0.2,aXG),total:parseFloat((Math.max(0.3,hXG)+Math.max(0.2,aXG)).toFixed(2))};
  }
  if(!hs||!as_)return null;
  const leagueAvg=1.35;
  const hAttack=(hs.goalsForAvgHome||leagueAvg)/leagueAvg;
  const hDef=(hs.goalsAgainstAvgHome||leagueAvg)/leagueAvg;
  const aAttack=(as_.goalsForAvgAway||leagueAvg)/leagueAvg;
  const aDef=(as_.goalsAgainstAvgAway||leagueAvg)/leagueAvg;
  const hXG=parseFloat((hAttack*aDef*leagueAvg*1.25).toFixed(2));
  const aXG=parseFloat((aAttack*hDef*leagueAvg).toFixed(2));
  return{home:hXG,away:aXG,total:parseFloat((hXG+aXG).toFixed(2))};
}

function calcConfidenceWorker(pick, analysis, season){
  const {formData,h2hData,statsData,standingsData,predData,injData,advData} = analysis;
  let score=0;

  // 1. EV
  if(pick.ev>15)score+=3;
  else if(pick.ev>8)score+=2;
  else if(pick.ev>3)score+=1;
  else if(pick.ev<-8)score-=2;
  else if(pick.ev<-3)score-=1;

  // 2. Forma ponderada
  const hTid=getTidFromAnalysis(pick.home,formData);
  const aTid=getTidFromAnalysis(pick.away,formData);
  const hFormData=hTid?formData[hTid]:null;
  const aFormData=aTid?formData[aTid]:null;
  const hf=hFormData?.form||[];
  const af=aFormData?.form||[];
  const hfHome=hFormData?.formHome||[];
  const afAway=aFormData?.formAway||[];
  const weightedScore=(form)=>{
    const weights=[1,2,3,4,5];let ws=0,total=0;
    [...form].slice(-5).forEach((r,i)=>{const w=weights[i]||1;total+=w;if(r==='W')ws+=w;else if(r==='L')ws-=w;});
    return total>0?ws/total:null;
  };
  if(hf.length>0||af.length>0){
    const side=betSideWorker(pick);
    const hfEff=side==='home'&&hfHome.length>=2?hfHome:hf;
    const afEff=side==='away'&&afAway.length>=2?afAway:af;
    const hScore=weightedScore(hfEff);
    const aScore=weightedScore(afEff);
    if(side==='home'&&hScore!==null){
      if(hScore>0.5)score+=4;else if(hScore>0.25)score+=3;else if(hScore>0)score+=2;
      else if(hScore>-0.25)score+=1;else if(hScore<-0.5)score-=3;else score-=1;
      if(aScore!==null){if(aScore<-0.3)score+=1;else if(aScore>0.4)score-=1;}
    }else if(side==='away'&&aScore!==null){
      if(aScore>0.5)score+=4;else if(aScore>0.25)score+=3;else if(aScore>0)score+=2;
      else if(aScore>-0.25)score+=1;else if(aScore<-0.5)score-=3;else score-=1;
      if(hScore!==null){if(hScore<-0.3)score+=1;else if(hScore>0.4)score-=1;}
    }
  }

  // 3. H2H
  if(hTid&&aTid){
    const h2hKey=`${Math.min(hTid,aTid)}_${Math.max(hTid,aTid)}`;
    const h2h=h2hData?.[h2hKey];
    if(h2h&&h2h.total>=3){
      const side=betSideWorker(pick);
      const hRate=h2h.team1wins/h2h.total;
      const aRate=h2h.team2wins/h2h.total;
      if(side==='home'){
        if(hRate>=0.65)score+=3;else if(hRate>=0.5)score+=2;else if(hRate>=0.4)score+=1;
        else if(aRate>=0.65)score-=2;else if(aRate>=0.5)score-=1;
      }else if(side==='away'){
        if(aRate>=0.65)score+=3;else if(aRate>=0.5)score+=2;else if(aRate>=0.4)score+=1;
        else if(hRate>=0.65)score-=2;else if(hRate>=0.5)score-=1;
      }
      if(pick.marketType==='totals'&&h2h.over25!=null){
        const line=parseFloat(pick.bet.match(/[\d.]+/)?.[0]||2.5);
        const h2hOverRate=h2h.over25/h2h.total;
        const lineAdj=line>2.5?-0.1*(line-2.5):0.05*(2.5-line);
        const adjustedRate=Math.min(1,Math.max(0,h2hOverRate+lineAdj));
        if(pick.bet.includes('Over')){
          if(adjustedRate>=0.65)score+=3;else if(adjustedRate>=0.5)score+=2;
          else if(adjustedRate>=0.4)score+=1;else if(adjustedRate<=0.3)score-=2;else score-=1;
        }else{
          if(adjustedRate<=0.35)score+=3;else if(adjustedRate<=0.5)score+=2;
          else if(adjustedRate<=0.6)score+=1;else if(adjustedRate>=0.7)score-=2;else score-=1;
        }
      }
    }
  }

  // 4. Standings
  if(hTid&&aTid&&pick.leagueId){
    const standings=standingsData?.[`${pick.leagueId}_${season}`];
    const hSt=standings?.[hTid];
    const aSt=standings?.[aTid];
    if(hSt&&aSt){
      const side=betSideWorker(pick);
      const gap=(aSt.pos||99)-(hSt.pos||99);
      if(side==='home'){
        if(gap>=8)score+=2;else if(gap>=4)score+=1;else if(gap<=-8)score-=2;else if(gap<=-4)score-=1;
        if(hSt.home?.played>=3){const r=hSt.home.w/hSt.home.played;if(r>=0.6)score+=1;else if(r<=0.25)score-=1;}
      }else if(side==='away'){
        if(gap<=-8)score+=2;else if(gap<=-4)score+=1;else if(gap>=8)score-=2;else if(gap>=4)score-=1;
        if(aSt.away?.played>=3){const r=aSt.away.w/aSt.away.played;if(r>=0.4)score+=1;else if(r<=0.15)score-=1;}
      }
    }
  }

  // 5. Lesões
  if(pick.fixtureId){
    const injuries=injData?.[pick.fixtureId]||[];
    if(injuries.length>0){
      const side=betSideWorker(pick);
      const hInj=injuries.filter(i=>i.team===pick.home).length;
      const aInj=injuries.filter(i=>i.team===pick.away).length;
      if(side==='home'||side==='none'){if(hInj>=4)score-=3;else if(hInj>=2)score-=2;else if(hInj>=1)score-=1;}
      if(side==='away'||side==='none'){if(aInj>=4)score-=3;else if(aInj>=2)score-=2;else if(aInj>=1)score-=1;}
    }
  }

  // 6. xG
  const hStats=hTid&&pick.leagueId?statsData?.[`${hTid}_${pick.leagueId}`]:null;
  const aStats=aTid&&pick.leagueId?statsData?.[`${aTid}_${pick.leagueId}`]:null;
  const advDataByName={};
  Object.entries(advData||{}).forEach(([tid,d])=>{if(d.name)advDataByName[d.name]=d;});
  const xg=calcXGWorker(hStats,aStats,pick.home,pick.away,advDataByName);
  if(xg){
    if(pick.marketType==='totals'){
      const line=parseFloat(pick.bet.match(/[\d.]+/)?.[0]||2.5);
      const diff=xg.total-line;
      if(pick.bet.includes('Over')){
        if(diff>0.7)score+=3;else if(diff>0.3)score+=2;else if(diff>0)score+=1;
        else if(diff<-0.7)score-=3;else if(diff<-0.3)score-=2;else score-=1;
      }else{
        if(diff<-0.7)score+=3;else if(diff<-0.3)score+=2;else if(diff<0)score+=1;
        else if(diff>0.7)score-=3;else if(diff>0.3)score-=2;else score-=1;
      }
    }else if(pick.marketType==='btts'){
      const bothScore=xg.home>=1.0&&xg.away>=1.0;
      const eitherLow=xg.home<0.6||xg.away<0.6;
      if(pick.bet.includes('✓')){if(bothScore)score+=3;else if(!eitherLow)score+=1;else score-=2;}
      else{if(eitherLow)score+=3;else if(!bothScore)score+=1;else score-=2;}
    }
  }

  // 7. Previsão API
  if(pick.fixtureId){
    const pred=predData?.[pick.fixtureId];
    if(pred){
      const pHome=parseInt(pred.winPct?.home)||0;
      const pAway=parseInt(pred.winPct?.away)||0;
      const side=betSideWorker(pick);
      if(side==='home'&&pHome>55)score+=1;else if(side==='away'&&pAway>55)score+=1;
      else if(side==='home'&&pHome<35)score-=1;else if(side==='away'&&pAway<35)score-=1;
    }
  }

  // 8. Penalizações
  if(pick.odd>4.5)score-=2;else if(pick.odd>3.5)score-=1;
  if(pick.marketType==='combo')score-=2;
  if(pick.marketType==='ht')score-=1;

  return Math.min(10,Math.max(1,score));
}

function normalizeBookmakers(bookmakers, home, away){
  const picks=[];
  for(const bk of bookmakers){
    for(const bet of (bk.bets||[])){
      const bn=bet.name||'';const id=bet.id;
      const vs=bet.values||[];
      if(id===1||bn==='Match Winner'){
        const hO=parseFloat(vs.find(v=>v.value==='Home')?.odd)||0;
        const dO=parseFloat(vs.find(v=>v.value==='Draw')?.odd)||0;
        const aO=parseFloat(vs.find(v=>v.value==='Away')?.odd)||0;
        const total=hO>1?1/hO:0+dO>1?1/dO:0+aO>1?1/aO:0;
        if(hO>1)picks.push({marketType:'h2h',bet:`Vitória ${home}`,odd:hO,ev:Math.round((hO-total*hO)*10)/10,prob:Math.round(100/hO)});
        if(dO>1)picks.push({marketType:'h2h',bet:'Empate',odd:dO,ev:Math.round((dO-total*dO)*10)/10,prob:Math.round(100/dO)});
        if(aO>1)picks.push({marketType:'h2h',bet:`Vitória ${away}`,odd:aO,ev:Math.round((aO-total*aO)*10)/10,prob:Math.round(100/aO)});
      }
      if(id===5||bn.includes('Over/Under')){
        const o25=parseFloat(vs.find(v=>v.value==='Over 2.5')?.odd)||0;
        const u25=parseFloat(vs.find(v=>v.value==='Under 2.5')?.odd)||0;
        if(o25>1)picks.push({marketType:'totals',bet:'Over 2.5',odd:o25,ev:Math.round((o25-1/o25*o25)*10)/10,prob:Math.round(100/o25)});
        if(u25>1)picks.push({marketType:'totals',bet:'Under 2.5',odd:u25,ev:Math.round((u25-1/u25*u25)*10)/10,prob:Math.round(100/u25)});
      }
      if(id===8||bn==='Both Teams Score'){
        const bY=parseFloat(vs.find(v=>v.value==='Yes')?.odd)||0;
        const bN=parseFloat(vs.find(v=>v.value==='No')?.odd)||0;
        if(bY>1)picks.push({marketType:'btts',bet:'Ambas Marcam ✓',odd:bY,ev:Math.round((bY-1/bY*bY)*10)/10,prob:Math.round(100/bY)});
        if(bN>1)picks.push({marketType:'btts',bet:'Ambas Marcam ✗',odd:bN,ev:Math.round((bN-1/bN*bN)*10)/10,prob:Math.round(100/bN)});
      }
      if(id===3||bn==='Double Chance'){
        const x1=parseFloat(vs.find(v=>v.value==='Home/Draw')?.odd)||0;
        const x2=parseFloat(vs.find(v=>v.value==='Draw/Away')?.odd)||0;
        const d12=parseFloat(vs.find(v=>v.value==='Home/Away')?.odd)||0;
        if(x1>1)picks.push({marketType:'combo',bet:'1X',odd:x1,ev:Math.round((x1-1/x1*x1)*10)/10,prob:Math.round(100/x1)});
        if(x2>1)picks.push({marketType:'combo',bet:'X2',odd:x2,ev:Math.round((x2-1/x2*x2)*10)/10,prob:Math.round(100/x2)});
        if(d12>1)picks.push({marketType:'combo',bet:'12',odd:d12,ev:Math.round((d12-1/d12*d12)*10)/10,prob:Math.round(100/d12)});
      }
    }
  }
  return picks;
}

function calcTops(allFixtures, allOdds, analysis, season){
  const oddsMap={};
  (allOdds||[]).forEach(item=>{const id=item.fixture?.id;if(id)oddsMap[id]=item.bookmakers||[];});

  const allPicks=[];
  for(const f of allFixtures){
    const fid=f.fixture?.id;
    const home=f.teams?.home?.name;
    const away=f.teams?.away?.name;
    const lid=f._lid;
    if(!home||!away||!fid)continue;
    const bms=oddsMap[fid]||[];
    if(!bms.length)continue;
    const raw=normalizeBookmakers(bms,home,away);
    const league=f.league?.name||'';
    const time=f.fixture?.date?new Date(f.fixture.date).toLocaleTimeString('pt-PT',{hour:'2-digit',minute:'2-digit',timeZone:'Europe/Lisbon'}):null;
    for(const p of raw){
      const pick={...p,home,away,league,time,fixtureId:fid,leagueId:lid,group:'soccer'};
      pick.confidence=calcConfidenceWorker(pick,analysis,season);
      allPicks.push(pick);
    }
  }

  const composite=p=>(p.confidence*10)+(p.ev>0?p.ev*0.5:p.ev*0.3)+(p.odd<2.5?2:0);
  allPicks.sort((a,b)=>composite(b)-composite(a));

  // Dedup por jogo nos tops — 1 pick por jogo
  const topFilter=(picks,marketType,betFilter)=>{
    const seen=new Set();
    return picks.filter(p=>{
      if(marketType&&p.marketType!==marketType)return false;
      if(betFilter&&!betFilter(p))return false;
      const k=`${p.home}|${p.away}`;
      if(seen.has(k))return false;
      seen.add(k);return true;
    });
  };

  return{
    top15:topFilter(allPicks.filter(p=>p.odd>=1.40&&p.odd<=3.00&&p.confidence>=5),'','').slice(0,15),
    h2h:topFilter(allPicks,'h2h','').slice(0,20),
    totals:topFilter(allPicks,'totals','').slice(0,20),
    btts:topFilter(allPicks,'btts','').slice(0,20),
    combo_over:topFilter(allPicks,'combo',p=>p.bet.includes('Over')||p.bet.includes('1/2')).slice(0,10),
    combo_under:topFilter(allPicks,'combo',p=>p.bet.includes('Under')).slice(0,10),
    combo_btts:topFilter(allPicks,'combo',p=>p.bet.includes('Ambas')).slice(0,10),
    combo_bttsno:topFilter(allPicks,'combo',p=>p.bet.includes('Não')).slice(0,10),
    generatedAt:new Date().toISOString(),
  };
}

// ── FASE 11: HISTÓRICO AUTOMÁTICO ────────────────────────────────
function checkResult(pick, fixture){
  const gh = fixture.goals?.home ?? null;
  const ga = fixture.goals?.away ?? null;
  if(gh === null || ga === null) return null;
  const bet = pick.bet || '';
  const home = pick.home;
  const away = pick.away;

  if(bet.startsWith('Vitória '+home)) return gh > ga ? 'green' : 'red';
  if(bet.startsWith('Vitória '+away)) return ga > gh ? 'green' : 'red';
  if(bet === 'Empate') return gh === ga ? 'green' : 'red';
  if(bet.includes('1X')) return gh >= ga ? 'green' : 'red';
  if(bet.includes('X2')) return ga >= gh ? 'green' : 'red';
  if(bet.includes('Over 2.5')) return (gh + ga) > 2.5 ? 'green' : 'red';
  if(bet.includes('Under 2.5')) return (gh + ga) < 2.5 ? 'green' : 'red';
  if(bet.includes('Over 1.5')) return (gh + ga) > 1.5 ? 'green' : 'red';
  if(bet.includes('Under 1.5')) return (gh + ga) < 1.5 ? 'green' : 'red';
  if(bet.includes('Ambas Marcam ✓')) return gh > 0 && ga > 0 ? 'green' : 'red';
  if(bet.includes('Ambas Marcam ✗')) return gh === 0 || ga === 0 ? 'green' : 'red';
  if(bet.includes('1/2 + Over 2.5')) return gh !== ga && (gh+ga) > 2.5 ? 'green' : 'red';
  if(bet.includes('1/2 + Under 2.5')) return gh !== ga && (gh+ga) < 2.5 ? 'green' : 'red';
  if(bet.includes('1/2 + BTTS')) return gh !== ga && gh > 0 && ga > 0 ? 'green' : 'red';
  if(bet.includes('1X + Over')) return gh >= ga && (gh+ga) > 2.5 ? 'green' : 'red';
  if(bet.includes('X2 + Over')) return ga >= gh && (gh+ga) > 2.5 ? 'green' : 'red';
  if(bet.includes('1X + Under')) return gh >= ga && (gh+ga) < 2.5 ? 'green' : 'red';
  if(bet.includes('X2 + Under')) return ga >= gh && (gh+ga) < 2.5 ? 'green' : 'red';
  if(bet.includes('1X + Ambas Marcam')) return gh >= ga && gh > 0 && ga > 0 ? 'green' : 'red';
  if(bet.includes('X2 + Ambas Marcam')) return ga >= gh && gh > 0 && ga > 0 ? 'green' : 'red';
  return null;
}

async function updateHistory(env, today, headers, season){
  try{
    // Betting day de ontem (mesma fronteira 06:00→06:00)
    const yDate = getBettingDayDate(new Date(), -1);
    const yTops = await env.CACHE.get(`tops_${yDate}`);
    if(!yTops){ console.log('No tops for yesterday, skipping history.'); return; }

    const tops = JSON.parse(yTops);
    const picksToVerify = tops.top15 || [];
    if(!picksToVerify.length){ console.log('No picks to verify.'); return; }

    // Buscar resultados das DUAS datas de calendário API que cobrem o betting day de ontem
    // (ex: betting day 2026-04-30 = jogos de 30 Apr 06:00 → 1 May 06:00 Lisboa,
    //  portanto os jogos pós-meia-noite estão na data 2026-05-01 da API).
    const [apiD1, apiD2] = getApiDatesForBettingDay(yDate);
    const { startMs, endMs } = getBettingDayBoundsFromDate(yDate);
    const results = [];
    for (const dateStr of [apiD1, apiD2]) {
      const rf = await fetch(`${BASE}/fixtures?date=${dateStr}&timezone=Europe/Lisbon&status=FT`, {
        headers: { 'x-apisports-key': headers['x-apisports-key'] }
      });
      if(!rf.ok){ console.warn('Failed to fetch results for', dateStr); continue; }
      const fd = await rf.json();
      const arr = fd?.response || [];
      // só fixtures dentro da janela do betting day de ontem
      arr.forEach(f => {
        const t = f.fixture?.date ? new Date(f.fixture.date).getTime() : 0;
        if (t >= startMs && t < endMs) results.push(f);
      });
    }

    // Criar mapa de resultados por equipas
    const resultsMap = {};
    results.forEach(f => {
      const home = f.teams?.home?.name;
      const away = f.teams?.away?.name;
      if(home && away) resultsMap[`${home}|${away}`] = f;
    });

    // Verificar cada pick
    const verifiedPicks = picksToVerify.map(pick => {
      const key = `${pick.home}|${pick.away}`;
      const fixture = resultsMap[key];
      if(!fixture) return { ...pick, result: 'void', date: yDate };
      const result = checkResult(pick, fixture);
      return { ...pick, result: result || 'void', date: yDate,
        score: `${fixture.goals?.home}-${fixture.goals?.away}` };
    }).filter(p => p.result !== 'void');

    if(!verifiedPicks.length){ console.log('No verified picks for', yDate); return; }

    // Carregar histórico existente
    const histRaw = await env.CACHE.get('history');
    const history = histRaw ? JSON.parse(histRaw) : { days: [], stats: {} };

    // Adicionar dia ao histórico
    const greens = verifiedPicks.filter(p => p.result === 'green').length;
    const reds = verifiedPicks.filter(p => p.result === 'red').length;
    const total = greens + reds;
    const roi = total > 0 ? parseFloat(((verifiedPicks.filter(p=>p.result==='green').reduce((s,p)=>s+p.odd,0) - total) / total * 100).toFixed(1)) : 0;

    history.days.unshift({
      date: yDate,
      picks: verifiedPicks,
      greens, reds, total,
      rate: total > 0 ? parseFloat((greens/total*100).toFixed(1)) : 0,
      roi,
    });

    // Manter só 90 dias
    if(history.days.length > 90) history.days = history.days.slice(0, 90);

    // Recalcular stats globais
    const allDays = history.days;
    const totalG = allDays.reduce((s,d)=>s+d.greens,0);
    const totalR = allDays.reduce((s,d)=>s+d.reds,0);
    const totalP = totalG + totalR;
    history.stats = {
      totalPicks: totalP,
      greens: totalG,
      reds: totalR,
      rate: totalP > 0 ? parseFloat((totalG/totalP*100).toFixed(1)) : 0,
      roi: parseFloat((allDays.reduce((s,d)=>s+d.roi,0)/Math.max(allDays.length,1)).toFixed(1)),
      days: allDays.length,
      streak: calcStreak(allDays),
    };

    await env.CACHE.put('history', JSON.stringify(history), { expirationTtl: 60 * 60 * 24 * 91 });
    console.log(`History updated: ${yDate} — ${greens}✅ ${reds}❌ (${total} picks)`);
  }catch(e){
    console.warn('History update failed:', e.message);
  }
}

function calcStreak(days){
  if(!days.length) return { type: 'none', count: 0 };
  let count = 0;
  const lastType = days[0].greens >= days[0].reds ? 'green' : 'red';
  for(const d of days){
    const t = d.greens >= d.reds ? 'green' : 'red';
    if(t === lastType) count++;
    else break;
  }
  return { type: lastType, count };
}

async function runCron(env, isLineupRun = false, phaseGroup = null) {
  const apiKey = env.API_FOOTBALL_KEY;
  if (!apiKey) { console.error('API_FOOTBALL_KEY not set'); return; }

  // Lock para impedir runs sobrepostos. Se o lock está activo (< 5 min), abortar.
  // Isto previne que /data/force?group=X sobreponha um run anterior que ainda não acabou.
  const lockKey = 'cron_lock';
  const existingLock = await env.CACHE.get(lockKey);
  if (existingLock) {
    const lockTime = parseInt(existingLock);
    if (Date.now() - lockTime < 5 * 60 * 1000) {
      console.log(`Cron skipped: another run in progress (started ${Math.round((Date.now()-lockTime)/1000)}s ago)`);
      return { skipped: true, reason: 'lock active' };
    }
  }
  await env.CACHE.put(lockKey, String(Date.now()), { expirationTtl: 600 }); // expira em 10min

  try {
    return await runCronInner(env, isLineupRun, phaseGroup);
  } finally {
    await env.CACHE.delete(lockKey);
  }
}

async function runCronInner(env, isLineupRun = false, phaseGroup = null) {
  const apiKey = env.API_FOOTBALL_KEY;
  if (!apiKey) { console.error('API_FOOTBALL_KEY not set'); return; }
  const now = new Date();
  const season = now.getMonth()<7?now.getFullYear()-1:now.getFullYear();
  const today = getBettingDayDate(now, 0);
  const headers = { 'x-apisports-key': apiKey };
  console.log(`Cron: bettingDay=${today}, season ${season}, lineupRun=${isLineupRun}, phaseGroup=${phaseGroup}`);

  if (isLineupRun) {
    const cached = await env.CACHE.get('data_today');
    if (cached) {
      const data = JSON.parse(cached);
      if (data.fixtures?.length) {
        const lineupData = await fetchLineups(data.fixtures, headers);
        const analysis = JSON.parse(await env.CACHE.get('analysis_today') || '{}');
        analysis.lineupData = lineupData;
        await env.CACHE.put('analysis_today', JSON.stringify(analysis), { expirationTtl: TTL });
        console.log(`Lineup run done: ${Object.keys(lineupData).length} lineups`);
        return;
      }
    }
  }

  // ── PHASE GROUPS — divide o trabalho pelos 5 cron runs/dia para caber no CPU
  // limit dos Workers (10ms free / 30s paid). Cada run faz uma fatia e acumula em
  // analysis_today. Depois do 5º run, analysisReady=true.
  //
  // - 'all'       (default, /data/force): faz tudo de uma vez (versão antiga)
  // - 'fixtures'  (07h UTC): Phase 1 (fixtures+odds)  + Phase 2 (form)
  // - 'h2h-stats' (11h UTC): Phase 3 (H2H)            + Phase 4 (stats)
  // - 'standings' (12h UTC): Phase 5 (standings)      + Phase 6 (pred+inj)
  // - 'advanced'  (16h UTC): Phase 7 (advanced)       + Phase 8 (transfers)
  // - 'finalize'  (17h UTC): Phase 9 (lineups)        + Phase 10 (tops) + 11 (history)
  const group = phaseGroup || 'all';

  // === Carregar fixtures+odds (sempre necessário ler) ===
  let fixturesData = null;
  // Phase 1 só corre para 'fixtures' (full) ou 'all'.
  // Os sub-grupos 'fixtures-1', 'fixtures-2' usam fixtures já no KV (chama fixtures primeiro!).
  if (group === 'fixtures' || group === 'all') {
    console.log('Phase 1: Fixtures + Odds...');
    const { allFixtures, allOdds } = await fetchFixturesAndOdds(today, season, headers);
    console.log(`Phase 1: ${allFixtures.length} fixtures, ${allOdds.length} odds`);
    if (!allFixtures.length) { console.log('No fixtures today.'); return; }
    const refereeData = extractReferees(allFixtures);
    await env.CACHE.put('data_today', JSON.stringify({
      fixtures: allFixtures, odds: allOdds,
      fetchedAt: now.toISOString(), today, season,
      leaguesFetched: [...new Set(allFixtures.map(f=>f._lid))].length,
      analysisReady: false,
    }), { expirationTtl: TTL });
    fixturesData = { allFixtures, allOdds, refereeData };
    // Inicializar analysis_today APENAS se está vazio ou se é 'all'.
    const existing = await env.CACHE.get('analysis_today');
    if (!existing || group === 'all') {
      const analysis = { formData: {}, h2hData: {}, statsData: {}, standingsData: {}, predData: {}, injData: {}, advData: {}, transferData: {}, lineupData: {}, refereeData };
      await env.CACHE.put('analysis_today', JSON.stringify(analysis), { expirationTtl: TTL });
    } else {
      const cur = JSON.parse(existing);
      cur.refereeData = refereeData;
      await env.CACHE.put('analysis_today', JSON.stringify(cur), { expirationTtl: TTL });
    }
  } else {
    // Outros grupos (incluindo fixtures-1/2) — re-usar fixtures já buscados
    const cached = await env.CACHE.get('data_today');
    if (!cached) { console.warn(`Group ${group}: data_today not yet populated — corre /data/force?group=fixtures primeiro`); return; }
    const data = JSON.parse(cached);
    if (!data.fixtures?.length) { console.warn(`Group ${group}: no fixtures, skipping`); return; }
    fixturesData = { allFixtures: data.fixtures, allOdds: data.odds || [], refereeData: extractReferees(data.fixtures) };
  }
  const { allFixtures, allOdds, refereeData } = fixturesData;

  // Helper para ler/escrever incrementalmente em analysis_today.
  // Para formData usa MERGE (não replace) para não apagar a metade anterior.
  const updateAnalysis = async (patch, mergeKeys = []) => {
    const cur = JSON.parse(await env.CACHE.get('analysis_today') || '{}');
    for (const [k, v] of Object.entries(patch)) {
      if (mergeKeys.includes(k) && cur[k] && typeof cur[k] === 'object') {
        cur[k] = { ...cur[k], ...v };
      } else {
        cur[k] = v;
      }
    }
    await env.CACHE.put('analysis_today', JSON.stringify(cur), { expirationTtl: TTL });
  };

  // ── PHASE 2 — FORM (com opção de meio para reduzir CPU)
  if (group === 'fixtures' || group === 'all') {
    console.log('Phase 2: Form (full)...');
    const formData = await fetchTeamForms(allFixtures, season, headers);
    await updateAnalysis({ formData });
    if (group === 'fixtures') { console.log('Group "fixtures" done.'); return; }
  }
  if (group === 'fixtures-1') {
    console.log('Phase 2: Form (1ª metade)...');
    const formData = await fetchTeamForms(allFixtures, season, headers, 1);
    await updateAnalysis({ formData }, ['formData']);
    console.log('Group "fixtures-1" done.'); return;
  }
  if (group === 'fixtures-2') {
    console.log('Phase 2: Form (2ª metade)...');
    const formData = await fetchTeamForms(allFixtures, season, headers, 2);
    await updateAnalysis({ formData }, ['formData']);
    console.log('Group "fixtures-2" done.'); return;
  }

  if (group === 'h2h-stats' || group === 'all') {
    console.log('Phase 3: H2H...');
    const h2hData = await fetchH2HData(allFixtures, headers);
    await updateAnalysis({ h2hData });
    console.log('Phase 4: Stats...');
    const statsData = await fetchTeamStats(allFixtures, season, headers);
    await updateAnalysis({ statsData });
    if (group === 'h2h-stats') { console.log('Group "h2h-stats" done.'); return; }
  }

  if (group === 'standings' || group === 'all') {
    console.log('Phase 5: Standings...');
    const standingsData = await fetchStandings(allFixtures, season, headers);
    await updateAnalysis({ standingsData });
    console.log('Phase 6: Predictions + Injuries...');
    const { predData, injData } = await fetchPredictionsAndInjuries(allFixtures, headers);
    await updateAnalysis({ predData, injData });
    if (group === 'standings') { console.log('Group "standings" done.'); return; }
  }

  if (group === 'advanced' || group === 'all') {
    console.log('Phase 7: Advanced stats...');
    const cur = JSON.parse(await env.CACHE.get('analysis_today') || '{}');
    const advData = await fetchAdvancedStats(cur.formData || {}, headers);
    await updateAnalysis({ advData });
    console.log('Phase 8: Transfers...');
    const transferData = await fetchTransfers(allFixtures, headers);
    await updateAnalysis({ transferData });
    if (group === 'advanced') { console.log('Group "advanced" done.'); return; }
  }

  if (group === 'finalize' || group === 'all') {
    console.log('Phase 9: Lineups...');
    const lineupData = await fetchLineups(allFixtures, headers);
    await updateAnalysis({ lineupData });

    // Phase 10: Calculate tops with full scoring
    console.log('Phase 10: Scoring + Tops...');
    const analysis = JSON.parse(await env.CACHE.get('analysis_today') || '{}');
    const tops = calcTops(allFixtures, allOdds, analysis, season);
    await env.CACHE.put(`tops_${today}`, JSON.stringify({...tops, date: today}), { expirationTtl: 60 * 60 * 24 * 91 });
    await env.CACHE.put('tops_today', JSON.stringify(tops), { expirationTtl: TTL });

    // Phase 11: History
    console.log('Phase 11: History...');
    await updateHistory(env, today, headers, season);

    // Marcar como pronto
    await env.CACHE.put('data_today', JSON.stringify({
      fixtures: allFixtures, odds: allOdds,
      fetchedAt: now.toISOString(), today, season,
      leaguesFetched: [...new Set(allFixtures.map(f=>f._lid))].length,
      analysisReady: true,
    }), { expirationTtl: TTL });

    console.log('Cron complete!');
  }
}

export default {
  async scheduled(event, env, ctx) {
    const hour = new Date(event.scheduledTime).getUTCHours();
    // Mapeamento horas UTC → grupo de fases.
    // Cron actual: 5, 6, 7, 8, 9, 13, 17 UTC
    // Análise completa pronta às 9h UTC = 10h Lisboa (verão) / 9h Lisboa (inverno) —
    // antes dos jogos do final da manhã / início da tarde.
    // Lineup-refresh às 13h e 17h para apanhar mudanças de lineup mais perto da hora dos jogos.
    let phaseGroup = 'all', isLineupRun = false;
    if (hour === 5) phaseGroup = 'fixtures';        // Phase 1+2: fixtures + form
    else if (hour === 6) phaseGroup = 'h2h-stats';  // Phase 3+4: H2H + stats
    else if (hour === 7) phaseGroup = 'standings';  // Phase 5+6: standings + pred+inj
    else if (hour === 8) phaseGroup = 'advanced';   // Phase 7+8: advanced + transfers
    else if (hour === 9) phaseGroup = 'finalize';   // Phase 9+10+11: lineups + tops + history
    else if (hour === 13 || hour === 17) isLineupRun = true; // só refresh de lineups
    ctx.waitUntil(runCron(env, isLineupRun, phaseGroup));
  },

  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS_HEADERS });
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === '/data' || path === '/data/') {
      try {
        const cached = await env.CACHE.get('data_today');
        if (cached) return new Response(cached, {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'X-Cache': 'HIT' }
        });
        ctx.waitUntil(runCron(env, false));
        return new Response(JSON.stringify({ status: 'fetching', message: 'A buscar dados, tenta em 60 segundos' }), {
          status: 202, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
    }

    if (path === '/analysis') {
      try {
        const cached = await env.CACHE.get('analysis_today');
        if (cached) return new Response(cached, {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'X-Cache': 'HIT' }
        });
        return new Response(JSON.stringify({ status: 'not_ready' }), {
          status: 202, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
    }

    if (path === '/tops') {
      try {
        const cached = await env.CACHE.get('tops_today');
        if (cached) return new Response(cached, {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'X-Cache': 'HIT' }
        });
        return new Response(JSON.stringify({ status: 'not_ready' }), {
          status: 202, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
    }

    if (path === '/history') {
      try {
        const cached = await env.CACHE.get('history');
        if (cached) return new Response(cached, {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json', 'X-Cache': 'HIT' }
        });
        return new Response(JSON.stringify({ days: [], stats: { totalPicks: 0, greens: 0, reds: 0, rate: 0, roi: 0, days: 0 } }), {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
    }

    if (path === '/data/force') {
      // Por defeito faz 'all' (modo antigo). Pode forçar fase específica via ?group=fixtures|h2h-stats|standings|advanced|finalize
      const group = url.searchParams.get('group') || 'all';
      // Verificar lock antes de iniciar — devolver feedback imediato ao user
      const lockTime = await env.CACHE.get('cron_lock');
      if (lockTime && (Date.now() - parseInt(lockTime) < 5 * 60 * 1000)) {
        const ageSec = Math.round((Date.now() - parseInt(lockTime)) / 1000);
        return new Response(JSON.stringify({
          status: 'busy',
          message: `Outro run em curso há ${ageSec}s. Espera mais ${Math.max(0, 180-ageSec)}s antes de tentar.`
        }), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
      }
      ctx.waitUntil(runCron(env, false, group));
      return new Response(JSON.stringify({ status: 'started', group }), {
        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
      });
    }

    if (path === '/data/status') {
      const [d, a] = await Promise.all([env.CACHE.get('data_today'), env.CACHE.get('analysis_today')]);
      if (d) {
        const data = JSON.parse(d);
        return new Response(JSON.stringify({
          fetchedAt: data.fetchedAt, today: data.today,
          fixtures: data.fixtures.length, odds: data.odds.length,
          leagues: data.leaguesFetched, analysisReady: data.analysisReady||false,
          analysisKeys: a?Object.keys(JSON.parse(a)).length:0,
        }), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
      }
      return new Response(JSON.stringify({ status: 'empty' }), {
        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
      });
    }

    // Endpoint que corre fetchTeamForms isolado com 3 fixtures de teste, devolve o resultado raw
    if (path === '/diag2') {
      const apiKey = env.API_FOOTBALL_KEY;
      if (!apiKey) return new Response(JSON.stringify({ error: 'no key' }), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
      const headers = { 'x-apisports-key': apiKey };
      const now = new Date();
      const season = now.getMonth()<7?now.getFullYear()-1:now.getFullYear();
      const dataRaw = await env.CACHE.get('data_today');
      if (!dataRaw) return new Response(JSON.stringify({ error: 'no data_today' }), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
      const data = JSON.parse(dataRaw);
      const sampleFixtures = data.fixtures.slice(0, 3);
      const result = await fetchTeamForms(sampleFixtures, season, headers);
      return new Response(JSON.stringify({
        sampleFixtures: sampleFixtures.length,
        teamsAttempted: sampleFixtures.length * 2,
        formDataReturned: Object.keys(result).length,
        formDataKeys: Object.keys(result),
        formDataSample: result,
      }, null, 2), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
    }

    // Endpoint de diagnóstico: faz uma call real à API com uma fixture conhecida e
    // devolve o resultado raw. Permite ver se o problema é credencial, season,
    // ou um corpo de resposta inesperado.
    if (path === '/diag') {
      const apiKey = env.API_FOOTBALL_KEY;
      if (!apiKey) {
        return new Response(JSON.stringify({ error: 'API_FOOTBALL_KEY not set' }), {
          status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
      const headers = { 'x-apisports-key': apiKey };
      const now = new Date();
      const season = now.getMonth()<7?now.getFullYear()-1:now.getFullYear();
      // Buscar 1 fixture do KV para usar como sample
      const dataRaw = await env.CACHE.get('data_today');
      if (!dataRaw) {
        return new Response(JSON.stringify({ error: 'no data_today' }), {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
      const data = JSON.parse(dataRaw);
      const sample = data.fixtures?.[0];
      if (!sample) {
        return new Response(JSON.stringify({ error: 'no sample fixture' }), {
          headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
        });
      }
      const lid = sample._lid;
      const teamId = sample.teams?.home?.id;
      const teamName = sample.teams?.home?.name;
      // Test 1: form query (a que está a falhar?)
      const formUrl = `${BASE}/fixtures?team=${teamId}&league=${lid}&season=${season}&last=20&status=FT`;
      const r1 = await fetch(formUrl, { headers });
      const r1json = await r1.json();
      // Test 2: stats query
      const statsUrl = `${BASE}/teams/statistics?team=${teamId}&league=${lid}&season=${season}`;
      const r2 = await fetch(statsUrl, { headers });
      const r2json = await r2.json();
      // Test 3: predictions
      const predUrl = `${BASE}/predictions?fixture=${sample.fixture?.id}`;
      const r3 = await fetch(predUrl, { headers });
      const r3json = await r3.json();
      return new Response(JSON.stringify({
        sampleFixture: { id: sample.fixture?.id, lid, teamId, teamName, season },
        form: { url: formUrl, status: r1.status, errors: r1json.errors, results: r1json.results, responseLen: (r1json.response||[]).length },
        stats: { url: statsUrl, status: r2.status, errors: r2json.errors, hasResponse: !!r2json.response },
        pred: { url: predUrl, status: r3.status, errors: r3json.errors, results: r3json.results, responseLen: (r3json.response||[]).length },
      }, null, 2), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
    }

    const target = url.searchParams.get('target');
    if (!target) return new Response(JSON.stringify({ error: 'Missing target param' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
    });

    try {
      const headers = {};
      for (const [k, v] of request.headers.entries()) {
        if (!['host','cf-connecting-ip','cf-ray','cf-visitor'].includes(k.toLowerCase())) headers[k]=v;
      }
      const body = request.method==='POST'?await request.text():undefined;
      const upstream = await fetch(target, { method: request.method, headers, body });
      const responseBody = await upstream.arrayBuffer();
      return new Response(responseBody, {
        status: upstream.status,
        headers: { ...CORS_HEADERS, 'Content-Type': upstream.headers.get('Content-Type')||'application/json' }
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
      });
    }
  }
};
