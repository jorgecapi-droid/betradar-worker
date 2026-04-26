// BetRadar Cloudflare Worker — Full Analysis Backend
// Cron 3x/dia: busca fixtures, odds, forma, H2H, stats, standings, previsões
// Cliente lê tudo do KV — zero prefetch, carregamento instantâneo

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
];

const BASE = 'https://v3.football.api-sports.io';
const TTL = 60 * 60 * 14;
const BATCH = 8;

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
  await batchAll(LEAGUE_IDS, async (lid) => {
    try {
      const rf = await fetch(`${BASE}/fixtures?league=${lid}&season=${season}&date=${today}&timezone=Europe/Lisbon`, { headers });
      if (!rf.ok) return;
      const fd = await rf.json();
      const fixtures = fd?.response || [];
      if (!fixtures.length) return;
      allFixtures.push(...fixtures.map(f => ({ ...f, _lid: lid })));
      const ro = await fetch(`${BASE}/odds?league=${lid}&season=${season}&date=${today}&timezone=Europe/Lisbon`, { headers });
      if (ro.ok) {
        const od = await ro.json();
        allOdds.push(...(od?.response || []));
      }
    } catch (e) { console.warn(`Fixtures/odds league ${lid}:`, e.message); }
  }, BATCH, 300);
  return { allFixtures, allOdds };
}

async function fetchTeamForms(fixtures, season, headers) {
  const formData = {};
  const teams = new Map();
  fixtures.forEach(f => {
    const hId = f.teams?.home?.id, hName = f.teams?.home?.name;
    const aId = f.teams?.away?.id, aName = f.teams?.away?.name;
    const lid = f._lid;
    if (hId && hName) teams.set(hId, { id: hId, name: hName, lid });
    if (aId && aName) teams.set(aId, { id: aId, name: aName, lid });
  });
  const teamList = [...teams.values()];
  console.log(`Fetching form for ${teamList.length} teams...`);
  await batchAll(teamList, async ({ id, name, lid }) => {
    try {
      const r = await fetch(`${BASE}/fixtures?team=${id}&league=${lid}&season=${season}&last=10&status=FT`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const fl = d?.response || [];
      if (!fl.length) return;
      const formAll = [], formHome = [], formAway = [];
      fl.forEach(f => {
        const isHome = f.teams.home.id === id;
        const gh = f.goals.home ?? 0, ga = f.goals.away ?? 0;
        const res = gh === ga ? 'D' : isHome ? (gh > ga ? 'W' : 'L') : (ga > gh ? 'W' : 'L');
        formAll.push(res);
        if (isHome) formHome.push(res); else formAway.push(res);
      });
      formData[id] = {
        name, tid: id,
        form: formAll.reverse().slice(0, 5),
        formHome: formHome.reverse().slice(0, 5),
        formAway: formAway.reverse().slice(0, 5),
        recentFixtures: fl.slice(0, 3).map(f => f.fixture?.id).filter(Boolean),
      };
    } catch (e) { console.warn(`Form team ${id}:`, e.message); }
  }, BATCH, 200);
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
  console.log(`Fetching H2H for ${pairList.length} pairs...`);
  await batchAll(pairList, async ({ key, hId, aId }) => {
    try {
      const r = await fetch(`${BASE}/fixtures/headtohead?h2h=${hId}-${aId}&last=10`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const matches = d?.response || [];
      if (!matches.length) return;
      let t1w = 0, t2w = 0, draws = 0, over25 = 0;
      matches.forEach(m => {
        const gh = m.goals.home ?? 0, ga = m.goals.away ?? 0;
        if (gh + ga > 2.5) over25++;
        if (gh === ga) draws++;
        else if (m.teams.home.id === hId ? gh > ga : ga > gh) t1w++;
        else t2w++;
      });
      h2hData[key] = { total: matches.length, team1wins: t1w, team2wins: t2w, draws, over25 };
    } catch (e) { console.warn(`H2H ${key}:`, e.message); }
  }, BATCH, 200);
  return h2hData;
}

async function fetchTeamStats(fixtures, season, headers) {
  const statsData = {};
  const requests = new Map();
  fixtures.forEach(f => {
    const lid = f._lid;
    const hId = f.teams?.home?.id, aId = f.teams?.away?.id;
    if (hId) requests.set(`${hId}_${lid}`, { tid: hId, lid });
    if (aId) requests.set(`${aId}_${lid}`, { tid: aId, lid });
  });
  const reqList = [...requests.entries()].map(([key, val]) => ({ key, ...val }));
  console.log(`Fetching team stats for ${reqList.length} combos...`);
  await batchAll(reqList, async ({ key, tid, lid }) => {
    try {
      const r = await fetch(`${BASE}/teams/statistics?team=${tid}&league=${lid}&season=${season}`, { headers });
      if (!r.ok) return;
      const d = await r.json();
      const s = d?.response;
      if (!s) return;
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
      };
    } catch (e) { console.warn(`Stats ${key}:`, e.message); }
  }, BATCH, 200);
  return statsData;
}

async function fetchStandings(fixtures, season, headers) {
  const standingsData = {};
  const leaguesWithGames = [...new Set(fixtures.map(f => f._lid))];
  console.log(`Fetching standings for ${leaguesWithGames.length} leagues...`);
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
          home: { played: t.home?.played || 0, w: t.home?.win || 0, d: t.home?.draw || 0, l: t.home?.lose || 0 },
          away: { played: t.away?.played || 0, w: t.away?.win || 0, d: t.away?.draw || 0, l: t.away?.lose || 0 },
          name: t.team.name,
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
  console.log(`Fetching predictions+injuries for ${fids.length} fixtures...`);
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
          underOver: p.predictions?.under_over,
          advice: p.predictions?.advice,
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
  console.log(`Fetching advanced stats for ${teamList.length} teams...`);
  await batchAll(teamList, async ([tid, data]) => {
    try {
      const fids = data.recentFixtures.slice(0, 3);
      let shotsOn = 0, shotsTotal = 0, possession = 0, corners = 0, n = 0;
      let cornersHome = 0, cornersAway = 0, nHome = 0, nAway = 0;
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
          const sOn = parseInt(get('Shots on Goal')) || 0;
          const sTotal = parseInt(get('Total Shots')) || 0;
          const poss = parseInt((get('Ball Possession') || '0%').replace('%', '')) || 0;
          const corn = parseInt(get('Corner Kicks')) || 0;
          shotsOn += sOn; shotsTotal += sTotal; possession += poss; corners += corn; n++;
          if (rf.ok) {
            const df = await rf.json();
            const isHome = df?.response?.[0]?.teams?.home?.id === parseInt(tid);
            if (isHome) { cornersHome += corn; nHome++; } else { cornersAway += corn; nAway++; }
          }
        } catch {}
      }));
      if (n > 0) {
        advData[tid] = {
          name: data.name,
          shotsOnAvg: parseFloat((shotsOn / n).toFixed(1)),
          shotsTotalAvg: parseFloat((shotsTotal / n).toFixed(1)),
          possessionAvg: parseFloat((possession / n).toFixed(0)),
          cornersAvg: parseFloat((corners / n).toFixed(1)),
          cornersHomeAvg: nHome > 0 ? parseFloat((cornersHome / nHome).toFixed(1)) : null,
          cornersAwayAvg: nAway > 0 ? parseFloat((cornersAway / nAway).toFixed(1)) : null,
          xgFromShots: parseFloat(((shotsOn / n) * 0.33).toFixed(2)),
          games: n,
        };
      }
    } catch (e) { console.warn(`Advanced stats team ${tid}:`, e.message); }
  }, 4, 300);
  return advData;
}

async function runCron(env) {
  const apiKey = env.API_FOOTBALL_KEY;
  if (!apiKey) { console.error('API_FOOTBALL_KEY not set'); return; }
  const now = new Date();
  const season = now.getMonth() < 7 ? now.getFullYear() - 1 : now.getFullYear();
  const today = now.toLocaleDateString('sv-SE', { timeZone: 'Europe/Lisbon' });
  const headers = { 'x-apisports-key': apiKey };
  console.log(`Cron started: ${today}, season ${season}`);

  console.log('Phase 1: Fixtures + Odds...');
  const { allFixtures, allOdds } = await fetchFixturesAndOdds(today, season, headers);
  console.log(`Phase 1 done: ${allFixtures.length} fixtures, ${allOdds.length} odds`);

  await env.CACHE.put('data_today', JSON.stringify({
    fixtures: allFixtures, odds: allOdds,
    fetchedAt: now.toISOString(), today, season,
    leaguesFetched: [...new Set(allFixtures.map(f => f._lid))].length,
    analysisReady: false,
  }), { expirationTtl: TTL });

  if (!allFixtures.length) { console.log('No fixtures today.'); return; }

  console.log('Phase 2: Team form...');
  const formData = await fetchTeamForms(allFixtures, season, headers);

  console.log('Phase 3: H2H...');
  const h2hData = await fetchH2HData(allFixtures, headers);

  console.log('Phase 4: Team stats...');
  const statsData = await fetchTeamStats(allFixtures, season, headers);

  console.log('Phase 5: Standings...');
  const standingsData = await fetchStandings(allFixtures, season, headers);

  console.log('Phase 6: Predictions + Injuries...');
  const { predData, injData } = await fetchPredictionsAndInjuries(allFixtures, headers);

  console.log('Phase 7: Advanced stats...');
  const advData = await fetchAdvancedStats(formData, headers);

  const analysis = { formData, h2hData, statsData, standingsData, predData, injData, advData };
  await env.CACHE.put('analysis_today', JSON.stringify(analysis), { expirationTtl: TTL });

  await env.CACHE.put('data_today', JSON.stringify({
    fixtures: allFixtures, odds: allOdds,
    fetchedAt: now.toISOString(), today, season,
    leaguesFetched: [...new Set(allFixtures.map(f => f._lid))].length,
    analysisReady: true,
  }), { expirationTtl: TTL });

  console.log('Cron complete!');
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runCron(env));
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
        ctx.waitUntil(runCron(env));
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

    if (path === '/data/force') {
      ctx.waitUntil(runCron(env));
      return new Response(JSON.stringify({ status: 'started' }), {
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
          leagues: data.leaguesFetched, analysisReady: data.analysisReady || false,
          analysisKeys: a ? Object.keys(JSON.parse(a)).length : 0,
        }), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
      }
      return new Response(JSON.stringify({ status: 'empty' }), {
        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
      });
    }

    const target = url.searchParams.get('target');
    if (!target) return new Response(JSON.stringify({ error: 'Missing target param' }), {
      status: 400, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
    });

    try {
      const headers = {};
      for (const [k, v] of request.headers.entries()) {
        if (!['host', 'cf-connecting-ip', 'cf-ray', 'cf-visitor'].includes(k.toLowerCase())) {
          headers[k] = v;
        }
      }
      const body = request.method === 'POST' ? await request.text() : undefined;
      const upstream = await fetch(target, { method: request.method, headers, body });
      const responseBody = await upstream.arrayBuffer();
      return new Response(responseBody, {
        status: upstream.status,
        headers: { ...CORS_HEADERS, 'Content-Type': upstream.headers.get('Content-Type') || 'application/json' }
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), {
        status: 500, headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
      });
    }
  }
};
