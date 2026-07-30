let _fpTf = '1h', _fpData = null;
let _fpView = { cnt: 0, off: 0 };   // off = баров скрыто справа
let _fpDrag = null, _fpMouse = null;
let _fpAnom = true, _fpAnomCache = null;
let _fpMode = localStorage.getItem('fp_mode') || 'cells';   // cells | poc
function fpToggleMode(btn) {
  // 💠 режим отображения: полные ячейки ↔ POC-блоки (как resonance «D»)
  _fpMode = _fpMode === 'cells' ? 'poc' : 'cells';
  try { localStorage.setItem('fp_mode', _fpMode); } catch (e) {}
  if (btn) {
    const on = _fpMode === 'poc';
    btn.style.background = on ? 'rgba(123,92,255,0.20)' : 'var(--dark)';
    btn.style.borderColor = on ? 'rgba(123,92,255,0.6)' : 'var(--border)';
    btn.style.color = on ? '#b28bff' : 'var(--muted)';
  }
  fpRender();
}
let _fpSigs = null, _fpSigPair = null, _fpSigNotes = {};
async function fpLoadSigs(pair) {
  // 🪧 эмодзи сигналов журнала на кластерном графике (поверх, в канавке)
  if (_fpSigPair === pair && _fpSigs) return;
  _fpSigPair = pair; _fpSigs = null;
  try {
    const r = await fetch(`/api/journal/by-symbol?symbol=${pair}&days=90`);
    const j = await r.json();
    const items = j.signals || j.items || (Array.isArray(j) ? j : []);
    _fpSigs = items.map(s => {
      const ts = (s.at_ts || 0) * 1000;
      if (!ts) return null;
      let icon = '';
      try {
        const m = (s.pattern || '').match(/^\s*(\p{Extended_Pictographic}[️‍]*)/u);
        if (m) icon = m[1];
      } catch (e) {}
      if (!icon) icon = s.direction === 'LONG' ? '🔺' : s.direction === 'SHORT' ? '🔻' : '·';
      if (s.ten_plus) icon = '🏅' + icon;   // ДЕСЯТКА — кандидат +10%/сделку
      return { ts, icon, isLong: s.direction === 'LONG',
               txt: `${s.pattern || s.source || ''}${s.direction ? ' · ' + s.direction : ''}` };
    }).filter(Boolean);
    if (_fpSigPair === pair && _fpData) fpRender();
  } catch (e) { _fpSigs = []; }
}
function fpToggleAnom(btn) {
  _fpAnom = !_fpAnom;
  if (btn) {
    btn.style.background = _fpAnom ? 'rgba(255,210,62,0.15)' : 'var(--dark)';
    btn.style.borderColor = _fpAnom ? 'rgba(255,210,62,0.5)' : 'var(--border)';
    btn.style.color = _fpAnom ? '#ffd23e' : 'var(--muted)';
  }
  fpRender();
}
function fpFindAnomalies(bars) {
  // все пороги — относительно ВИДИМОГО окна (robust-перцентили)
  const cellVols = [];
  bars.forEach(b => b.cells.forEach(c => { if (c[1] > 0) cellVols.push(c[1]); }));
  const out = { cells: [], bars: [], notes: {} };
  if (cellVols.length < 30) return out;
  const cs = [...cellVols].sort((x, y) => x - y);
  const q = f => cs[Math.min(cs.length - 1, Math.floor(f * cs.length))];
  const med = q(0.5), p99 = q(0.99), p95 = q(0.95);
  const vols = bars.map(b => b.v).sort((x, y) => x - y);
  const vMed = vols[Math.floor(vols.length / 2)];
  const dAbs = bars.map(b => Math.abs(b.d)).sort((x, y) => x - y);
  const dQ75 = dAbs[Math.floor(dAbs.length * 0.75)];
  const rngs = bars.map(b => (b.h - b.l) / b.c * 100).sort((x, y) => x - y);
  const rMed = rngs[Math.floor(rngs.length / 2)];
  let maxH = -Infinity, minL = Infinity;
  bars.forEach(b => { if (b.h > maxH) maxH = b.h; if (b.l < minL) minL = b.l; });
  const note = (i, t) => { (out.notes[i] = out.notes[i] || []).push(t); };
  bars.forEach((b, i) => {
    b.cells.forEach(c => {
      const [lvl, vol, dd] = c;
      if (vol >= p99 && vol >= 4 * med) {
        out.cells.push({ i, lvl, kind: 'vol' });
        note(i, `⚡ аномальный объём уровня (${fpNum(vol)})`);
      } else if (vol >= 3 * med && Math.abs(dd) / vol >= 0.65) {
        out.cells.push({ i, lvl, kind: dd >= 0 ? 'bi' : 'si' });
        note(i, dd >= 0 ? `🟩 имбаланс покупок на уровне (Δ +${fpNum(dd)})`
                        : `🟥 имбаланс продаж на уровне (Δ ${fpNum(dd)})`);
      }
    });
    const up = b.c >= b.o;
    if (dQ75 > 0 && up && b.d < -dQ75) {
      out.bars.push({ i, icon: '🧲', top: false });
      note(i, '🧲 абсорбция: рост при крупных продажах — лимитный покупатель');
    } else if (dQ75 > 0 && !up && b.d > dQ75) {
      out.bars.push({ i, icon: '🧲', top: true });
      note(i, '🧲 абсорбция: падение при крупных покупках — лимитный продавец');
    }
    const rng = (b.h - b.l) / b.c * 100;
    if (rng > 2.2 * rMed && b.v < vMed) {
      out.bars.push({ i, icon: '💨', top: !up });
      note(i, '💨 ход без объёма — тонкий стакан, склонно к возврату');
    }
    if (b.h >= maxH && b.cells.length) {
      const topCell = b.cells.reduce((acc, c) => c[0] > acc[0] ? c : acc);
      if (topCell[1] >= p95) {
        out.bars.push({ i, icon: '🛑', top: true });
        note(i, '🛑 stopping volume на хае — крупный продавец остановил рост');
      }
    }
    if (b.l <= minL && b.cells.length) {
      const botCell = b.cells.reduce((acc, c) => c[0] < acc[0] ? c : acc);
      if (botCell[1] >= p95) {
        out.bars.push({ i, icon: '🛡', top: false });
        note(i, '🛡 stopping volume на лоу — крупный покупатель держит дно');
      }
    }
  });
  return out;
}
function fpSetTf(btn) {
  _fpTf = btn.dataset.tf;
  document.querySelectorAll('#fpTfs button').forEach(b => {
    const on = b === btn;
    b.style.background = on ? 'rgba(76,201,240,0.15)' : 'var(--dark)';
    b.style.borderColor = on ? 'rgba(76,201,240,0.5)' : 'var(--border)';
    b.style.color = on ? '#4cc9f0' : 'var(--muted)';
  });
  (function () {
  const b = document.getElementById('fpModeBtn');
  if (b && _fpMode === 'poc') {
    b.style.background = 'rgba(123,92,255,0.20)';
    b.style.borderColor = 'rgba(123,92,255,0.6)';
    b.style.color = '#b28bff';
  }
})();
loadFootprint();
}
async function loadFootprint() {
  const st = document.getElementById('fpStatus');
  let pair = (document.getElementById('fpPair').value || 'BTCUSDT').toUpperCase().replace('/', '').trim();
  if (!pair.endsWith('USDT')) pair += 'USDT';
  st.textContent = '⏳ строю…';
  try {
    const bars = document.getElementById('fpBars').value || '60';
    const r = await fetch(`/api/footprint?pair=${pair}&tf=${_fpTf}&bars=${bars}`);
    const d = await r.json();
    if (!d.ok) { st.textContent = '⛔ ' + (d.error || 'ошибка'); return; }
    _fpData = d;
    _fpView = { cnt: Math.min(60, d.bars.length), off: 0 };
    fpLoadSigs(pair);
    fpRender();
    fpStatus();
  } catch (e) { st.textContent = '⛔ ' + e.message; }
}
function fpStatus() {
  const d = _fpData;
  if (!d) return;
  const n = d.bars.length;
  const cnt = Math.min(_fpView.cnt || n, n);
  const dSum = d.bars.reduce((a, b) => a + b.d, 0);
  document.getElementById('fpStatus').textContent =
    `${d.pair} · ${d.tf} · вижу ${cnt} из ${n} · Σ дельта ${dSum >= 0 ? '+' : ''}${fpNum(dSum)}` +
    ` · 🖱 тяни · колесо — зум · 2×клик — всё окно`;
  fpAnalyze();
}
function fpChip(txt, tone) {
  const C = { good: ['rgba(0,229,160,0.10)', 'rgba(0,229,160,0.4)', '#00e5a0'],
              bad: ['rgba(255,77,109,0.10)', 'rgba(255,77,109,0.4)', '#ff5e9c'],
              warn: ['rgba(255,210,62,0.10)', 'rgba(255,210,62,0.45)', '#ffd23e'],
              mute: ['rgba(154,164,181,0.08)', 'rgba(154,164,181,0.3)', '#9aa4b5'] }[tone || 'mute'];
  return `<span style="padding:4px 9px;border-radius:6px;background:${C[0]};border:1px solid ${C[1]};color:${C[2]};white-space:nowrap;">${txt}</span>`;
}
function fpAnalyze() {
  // 🧠 выводы по ВИДИМОМУ окну (пересчёт при зуме/прокрутке)
  const box = document.getElementById('fpAnalysis');
  if (!box || !_fpData) return;
  const { bars, phases } = fpSlice();
  const n = bars.length;
  if (n < 5) { box.style.display = 'none'; return; }
  const last = bars[n - 1];
  const chg = (last.c / bars[0].o - 1) * 100;
  const dSum = bars.reduce((a, b) => a + b.d, 0);
  const dLast5 = bars.slice(-5).reduce((a, b) => a + b.d, 0);
  let vb = bars[0];
  bars.forEach(b => { if (b.v > vb.v) vb = b; });
  const lvl = {};
  bars.forEach(b => b.cells.forEach(c => { lvl[c[0]] = (lvl[c[0]] || 0) + c[1]; }));
  let hk = -1, hv = 0;
  Object.entries(lvl).forEach(([k, v]) => { if (v > hv) { hv = v; hk = +k; } });
  const hvnP = _fpData.p_min + (hk + 0.5) * _fpData.tick;
  const distHvn = (last.c / hvnP - 1) * 100;
  const chips = [];
  chips.push(fpChip(`${chg >= 0 ? '📈' : '📉'} окно ${chg >= 0 ? '+' : ''}${chg.toFixed(1)}%`,
                    chg >= 0 ? 'good' : 'bad'));
  const agree = (chg >= 0) === (dSum >= 0);
  if (agree) {
    chips.push(fpChip(chg >= 0
      ? '✅ рост подтверждён дельтой — платит покупатель'
      : '✅ падение подтверждено дельтой — давит продавец', 'mute'));
  } else {
    chips.push(fpChip(chg >= 0
      ? '⚠️ дивергенция: рост на продажах маркетом — распределение, рост хрупкий'
      : '⚠️ дивергенция: слив выкупают маркетом — абсорбция/набор', 'warn'));
  }
  chips.push(fpChip(`послед. 5 баров: Δ ${dLast5 >= 0 ? '+' : ''}${fpNum(dLast5)} — ` +
                    `${dLast5 >= 0 ? 'покупатель у руля' : 'продавец у руля'}`,
                    dLast5 >= 0 ? 'good' : 'bad'));
  const vbDt = new Date(vb.t).toLocaleString('ru', _fpTf === '1d'
    ? { day: '2-digit', month: '2-digit' } : { day: '2-digit', month: '2-digit', hour: '2-digit' });
  chips.push(fpChip(`🔥 max-объём ${vbDt}: Δ ${vb.d >= 0 ? '+' : ''}${fpNum(vb.d)} ` +
                    `${vb.d >= 0 ? '(агрессивная покупка)' : '(агрессивная продажа)'}`,
                    vb.d >= 0 ? 'good' : 'bad'));
  chips.push(fpChip(`🧲 HVN ${fpPrice(hvnP)} · цена ${distHvn >= 0 ? 'выше' : 'ниже'} на ` +
                    `${Math.abs(distHvn).toFixed(1)}%` +
                    (Math.abs(distHvn) > 7 ? ' — растяжка от магнита объёма' : ' — у магнита объёма'),
                    Math.abs(distHvn) > 7 ? 'warn' : 'mute'));
  const ph = phases[n - 1];
  if (ph) {
    const PE = { LONG: ['🟢 фаза LONG', 'good'], SHORT: ['🔴 фаза SHORT', 'bad'],
                 NEUTRAL: ['⚪ без стороны', 'mute'] }[ph];
    if (PE) chips.push(fpChip(PE[0], PE[1]));
  }
  // 🪧 сигналы платформы в видимом окне (грузятся из журнала по-монетно)
  try {
    if (_fpSigs && _fpSigs.length && bars.length) {
      const dur = ({ '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 }[_fpTf] || 3600) * 1000;
      let nl = 0, ns = 0;
      _fpSigs.forEach(s => {
        const i = Math.floor((s.ts - bars[0].t) / dur);
        if (i >= 0 && i < n) { if (s.isLong) nl++; else ns++; }
      });
      chips.push(fpChip(nl + ns
        ? `🪧 сигналов в окне: ${nl + ns} (▲${nl} ▼${ns}) — эмодзи над/под барами`
        : '🪧 в видимом окне сигналов нет — отдались (2×клик) или листай назад',
        (nl + ns) ? 'mute' : 'warn'));
    }
  } catch (e) {}
  box.innerHTML = chips.join('');
  box.style.display = 'flex';
}
function fpNum(v) {
  const a = Math.abs(v);
  return a >= 1e9 ? (v / 1e9).toFixed(1) + 'B' : a >= 1e6 ? (v / 1e6).toFixed(1) + 'M'
    : a >= 1e3 ? (v / 1e3).toFixed(1) + 'K' : (+v).toFixed(0);
}
function fpPrice(v) {
  return v >= 1000 ? v.toFixed(1) : v >= 10 ? v.toFixed(3) : v >= 0.1 ? v.toFixed(4) : v.toPrecision(4);
}
function fpSlice() {
  const d = _fpData;
  const nAll = d.bars.length;
  const cnt = Math.max(10, Math.min(_fpView.cnt || nAll, nAll));
  const off = Math.max(0, Math.min(_fpView.off, nAll - cnt));
  const i0 = nAll - cnt - off;
  return { bars: d.bars.slice(i0, i0 + cnt), phases: (d.phases || []).slice(i0, i0 + cnt), i0, cnt };
}
const FP_AXIS = 80, FP_DELTA = 64, FP_PHASE = 14, FP_TIME = 18;
function fpGeom(cv) {
  const W = cv.clientWidth || cv.parentElement.clientWidth - 16;
  const H = Math.max(500, Math.min(720, window.innerHeight - 290));
  return { W, H, chartW: W - FP_AXIS, chartH: H - FP_DELTA - FP_PHASE - FP_TIME - 8 };
}
function fpRender() {
  const d = _fpData;
  if (!d) return;
  const cv = document.getElementById('fpCanvas');
  const { W, H, chartW, chartH } = fpGeom(cv);
  const dpr = window.devicePixelRatio || 1;
  cv.width = W * dpr; cv.height = H * dpr;
  cv.style.height = H + 'px';
  const g = cv.getContext('2d');
  g.setTransform(dpr, 0, 0, dpr, 0, 0);
  g.clearRect(0, 0, W, H);
  const { bars, phases, i0 } = fpSlice();
  const n = bars.length;
  const bw = chartW / n;
  const gMin = d.p_min, tick = d.tick;
  // автомасштаб цены по видимому окну
  let pLo = Infinity, pHi = -Infinity;
  bars.forEach(b => { if (b.l < pLo) pLo = b.l; if (b.h > pHi) pHi = b.h; });
  const pad = (pHi - pLo) * 0.03 || pLo * 0.01;
  pLo -= pad; pHi += pad;
  const y = p => chartH - (p - pLo) / (pHi - pLo) * chartH;
  const p_at = yy => pLo + (chartH - yy) / chartH * (pHi - pLo);
  const cellH = Math.max(2, tick / (pHi - pLo) * chartH);
  // сетка
  g.strokeStyle = 'rgba(154,164,181,0.08)';
  g.fillStyle = 'rgba(154,164,181,0.75)';
  g.font = '10px monospace';
  g.textAlign = 'left';
  for (let i = 0; i <= 6; i++) {
    const p = pLo + (pHi - pLo) * i / 6;
    const yy = y(p);
    g.beginPath(); g.moveTo(0, yy); g.lineTo(chartW, yy); g.stroke();
    g.fillText(fpPrice(p), chartW + 6, yy + 3);
  }
  // ячейки: яркость нормируется внутри бара; поверх — контур тела
  bars.forEach((b, i) => {
    const x0 = i * bw + 1.5;
    const cw = Math.max(2, bw - 3);
    const up = b.c >= b.o;
    const xc = i * bw + bw / 2;
    g.strokeStyle = up ? 'rgba(0,229,160,0.7)' : 'rgba(255,77,109,0.7)';
    g.lineWidth = 1;
    g.beginPath(); g.moveTo(xc, y(b.h)); g.lineTo(xc, y(b.l)); g.stroke();
    let vLoc = 0, pocLvl = -1;
    b.cells.forEach(c => { if (c[1] > vLoc) { vLoc = c[1]; pocLvl = c[0]; } });
    if (_fpMode === 'poc') {
      // 💠 POC-режим (resonance «D»): один жирный блок доминирующего
      // кластера, цвет по ДЕЛЬТЕ БАРА; сверху рамка если сам POC-уровень
      // против бара (например бар зелёный, а POC-ячейка продажная)
      if (pocLvl >= 0) {
        const yy = y(gMin + (pocLvl + 1) * tick);
        const bh2 = Math.max(3, cellH * 1.4);
        const pocCell = b.cells.find(c => c[0] === pocLvl);
        g.fillStyle = b.d >= 0 ? 'rgba(0,200,140,0.92)' : 'rgba(255,70,100,0.92)';
        g.fillRect(x0, yy - bh2 / 2 + cellH / 2, cw, bh2);
        if (pocCell && ((pocCell[2] >= 0) !== (b.d >= 0)) && bw > 8) {
          g.strokeStyle = pocCell[2] >= 0 ? '#00e5a0' : '#ff4d6d';
          g.lineWidth = 1.4;
          g.strokeRect(x0 - 0.5, yy - bh2 / 2 + cellH / 2 - 0.5, cw + 1, bh2 + 1);
          g.lineWidth = 1;
        }
      }
    } else {
    b.cells.forEach(c => {
      const [lvl, vol, dd] = c;
      const yy = y(gMin + (lvl + 1) * tick);
      if (yy > chartH + cellH || yy < -cellH) return;
      const inten = 0.12 + 0.72 * Math.pow(vLoc ? vol / vLoc : 0, 0.6);
      g.fillStyle = dd >= 0 ? `rgba(0,200,140,${inten})` : `rgba(255,70,100,${inten})`;
      g.fillRect(x0, yy + 0.5, cw, Math.max(1.5, cellH - 1));
    });
    if (pocLvl >= 0 && bw > 5) {
      const yy = y(gMin + (pocLvl + 1) * tick);
      g.strokeStyle = 'rgba(255,210,62,0.85)';
      g.lineWidth = 1;
      g.strokeRect(x0 + 0.5, yy + 1, cw - 1, Math.max(1.5, cellH - 2));
    }
    }
    const yB1 = y(Math.max(b.o, b.c)), yB2 = y(Math.min(b.o, b.c));
    g.strokeStyle = up ? '#00e5a0' : '#ff4d6d';
    g.lineWidth = 1.6;
    g.strokeRect(x0 + 0.8, yB1, Math.max(1, cw - 1.6), Math.max(2, yB2 - yB1));
    g.lineWidth = 1;
    // числа в ячейках при сильном приближении: объём × дельта
    if (_fpMode === 'cells' && bw >= 90 && cellH >= 12) {
      g.font = '9px monospace';
      g.textAlign = 'center';
      b.cells.forEach(c => {
        const yy = y(gMin + (c[0] + 1) * tick);
        if (yy < 0 || yy > chartH) return;
        g.fillStyle = 'rgba(230,238,248,0.85)';
        g.fillText(`${fpNum(c[1])} | ${c[2] >= 0 ? '+' : ''}${fpNum(c[2])}`,
                   x0 + cw / 2, yy + cellH / 2 + 3);
      });
      g.textAlign = 'left';
      g.font = '10px monospace';
    } else if (_fpMode === 'cells' && bw >= 54 && cellH >= 11) {
      g.font = '9px monospace';
      g.textAlign = 'center';
      g.fillStyle = 'rgba(230,238,248,0.8)';
      b.cells.forEach(c => {
        const yy = y(gMin + (c[0] + 1) * tick);
        if (yy < 0 || yy > chartH) return;
        g.fillText(fpNum(c[2]), x0 + cw / 2, yy + cellH / 2 + 3);
      });
      g.textAlign = 'left';
      g.font = '10px monospace';
    }
  });
  // ⚠ аномалии ордерфлоу (кольца ячеек + значки над барами)
  // usedTop/usedBot — общие стеки значков: сначала аномалии (ближе к бару),
  // затем эмодзи сигналов журнала (дальше) — друг друга не перекрывают
  const usedTop = {}, usedBot = {};
  _fpAnomCache = _fpAnom ? fpFindAnomalies(bars) : null;
  // 💎 дно+покупатель: новый 48ч-лоу + >=65% объёма бара в покупку
  // (90д: лонг WR 52.3%, EV +0.67 — единственная рабочая ячейка имбаланса)
  if (_fpAnomCache) {
    const all = d.bars;
    const volsAll = all.map(b => b.v).sort((x, y) => x - y);
    const vMedAll = volsAll[Math.floor(volsAll.length / 2)];
    bars.forEach((b, i) => {
      const gi = i0 + i;
      if (gi < 48 || !b.v) return;
      let lo = Infinity;
      for (let k = gi - 48; k < gi; k++) if (all[k].l < lo) lo = all[k].l;
      if (b.l <= lo && b.v > 1.5 * vMedAll && b.d / b.v >= 0.65) {
        _fpAnomCache.bars.push({ i, icon: '💎', top: false });
        (_fpAnomCache.notes[i] = _fpAnomCache.notes[i] || []).push(
          '💎 дно+покупатель: ≥65% объёма в покупку на новом 48ч-лоу (90д: лонг WR 52%, EV +0.67)');
      }
    });
  }
  if (_fpAnomCache) {
    const AC = { vol: '#4cc9f0', bi: '#00e5a0', si: '#ff4d6d' };
    if (_fpMode === 'cells') _fpAnomCache.cells.forEach(m => {
      const x0 = m.i * bw + 1.5;
      const cw = Math.max(2, bw - 3);
      const yy = y(gMin + (m.lvl + 1) * tick);
      if (yy > chartH + cellH || yy < -cellH) return;
      g.strokeStyle = AC[m.kind] || '#fff';
      g.lineWidth = 1.6;
      g.strokeRect(x0 - 0.5, yy - 0.5, cw + 1, Math.max(2.5, cellH));
      g.lineWidth = 1;
    });
    if (bw >= 7) {
      g.font = `${Math.min(14, Math.max(10, bw * 0.5))}px sans-serif`;
      g.textAlign = 'center';
      _fpAnomCache.bars.forEach(m => {
        const b = bars[m.i];
        const xc = m.i * bw + bw / 2;
        if (m.top) {
          const off = (usedTop[m.i] = (usedTop[m.i] || 0) + 1);
          g.fillText(m.icon, xc, Math.max(12, y(b.h) - 4 - (off - 1) * 15));
        } else {
          const off = (usedBot[m.i] = (usedBot[m.i] || 0) + 1);
          g.fillText(m.icon, xc, Math.min(chartH - 2, y(b.l) + 12 + (off - 1) * 15));
        }
      });
      g.textAlign = 'left';
      g.font = '10px monospace';
    }
  }
  // 🪧 эмодзи сигналов журнала: SHORT — над баром, LONG — под баром,
  // в продолжение стека аномалий (не закрывают ни ячейки, ни значки)
  _fpSigNotes = {};
  if (_fpSigs && _fpSigs.length && bw >= 4) {
    const dur = ({ '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 }[_fpTf] || 3600) * 1000;
    const gTop = {}, gBot = {};
    _fpSigs.forEach(s => {
      let i = Math.floor((s.ts - bars[0].t) / dur);
      if (i < 0 || i >= n) return;
      if (!(bars[i].t <= s.ts && s.ts < bars[i].t + dur)) {
        i = -1;   // дырка в барах — честный поиск
        for (let k = 0; k < n; k++) {
          if (bars[k].t <= s.ts && s.ts < bars[k].t + dur) { i = k; break; }
        }
        if (i < 0) return;
      }
      const tgt = s.isLong ? gBot : gTop;
      (tgt[i] = tgt[i] || new Set()).add(s.icon);
      (_fpSigNotes[i] = _fpSigNotes[i] || []).push(s.txt);
    });
    g.textAlign = 'center';
    // 29.07 «не видно совсем» → бейджи: тёмная плашка + цветная рамка
    // (🟢 LONG под баром / 🔴 SHORT над баром) + крупный эмодзи
    const EM = Math.max(13, Math.min(17, bw * 0.8));
    g.font = `${EM}px sans-serif`;
    const drawSig = (map, top) => {
      Object.keys(map).forEach(k => {
        const i = +k, b = bars[i];
        const xc = i * bw + bw / 2;
        const txt = [...map[k]].join('');
        const tw = g.measureText(txt).width;
        const bwd = tw + 10, bh = EM + 8;
        const off = top ? (usedTop[i] = (usedTop[i] || 0) + 1)
                        : (usedBot[i] = (usedBot[i] || 0) + 1);
        const yc = top
          ? Math.max(bh / 2 + 2, y(b.h) - 10 - (off - 1) * (bh + 4))
          : Math.min(chartH - bh / 2 - 2, y(b.l) + 10 + (off - 1) * (bh + 4));
        // плашка
        g.fillStyle = 'rgba(8,11,17,0.94)';
        g.strokeStyle = top ? '#ff4d6d' : '#00e5a0';
        g.lineWidth = 1.6;
        const rx = xc - bwd / 2, ry = yc - bh / 2, rr = 5;
        g.beginPath();
        g.moveTo(rx + rr, ry);
        g.arcTo(rx + bwd, ry, rx + bwd, ry + bh, rr);
        g.arcTo(rx + bwd, ry + bh, rx, ry + bh, rr);
        g.arcTo(rx, ry + bh, rx, ry, rr);
        g.arcTo(rx, ry, rx + bwd, ry, rr);
        g.closePath();
        g.fill();
        g.stroke();
        // хвостик к бару
        g.beginPath();
        if (top) { g.moveTo(xc, ry + bh); g.lineTo(xc, ry + bh + 4); }
        else { g.moveTo(xc, ry); g.lineTo(xc, ry - 4); }
        g.stroke();
        g.lineWidth = 1;
        g.fillStyle = '#fff';
        g.fillText(txt, xc, yc + EM * 0.36);
      });
    };
    drawSig(gTop, true);
    drawSig(gBot, false);
    g.textAlign = 'left';
    g.font = '10px monospace';
  }
  // пунктир последней цены
  const lastC = d.bars[d.bars.length - 1].c;
  if (lastC >= pLo && lastC <= pHi) {
    const yl = y(lastC);
    g.strokeStyle = 'rgba(76,201,240,0.7)';
    g.setLineDash([5, 4]);
    g.beginPath(); g.moveTo(0, yl); g.lineTo(chartW, yl); g.stroke();
    g.setLineDash([]);
    g.fillStyle = '#4cc9f0';
    g.fillRect(chartW + 1, yl - 8, FP_AXIS - 2, 16);
    g.fillStyle = '#0a0e14';
    g.font = 'bold 10px monospace';
    g.fillText(fpPrice(lastC), chartW + 5, yl + 3);
    g.font = '10px monospace';
  }
  // delta панель
  const dTop = chartH + 6;
  let dMax = 0;
  bars.forEach(b => { if (Math.abs(b.d) > dMax) dMax = Math.abs(b.d); });
  g.strokeStyle = 'rgba(154,164,181,0.15)';
  g.beginPath(); g.moveTo(0, dTop + FP_DELTA / 2); g.lineTo(chartW, dTop + FP_DELTA / 2); g.stroke();
  bars.forEach((b, i) => {
    const hh = dMax ? Math.abs(b.d) / dMax * (FP_DELTA / 2 - 2) : 0;
    g.fillStyle = b.d >= 0 ? 'rgba(0,229,160,0.75)' : 'rgba(255,77,109,0.75)';
    g.fillRect(i * bw + 1, b.d >= 0 ? dTop + FP_DELTA / 2 - hh : dTop + FP_DELTA / 2,
               Math.max(2, bw - 2), Math.max(1, hh));
  });
  g.fillStyle = 'rgba(154,164,181,0.6)';
  g.fillText('Delta', chartW + 6, dTop + FP_DELTA / 2 + 3);
  // фаза рынка
  const phTop = dTop + FP_DELTA + 2;
  const PC = { LONG: 'rgba(0,229,160,0.65)', SHORT: 'rgba(255,77,109,0.65)', NEUTRAL: 'rgba(154,164,181,0.4)' };
  phases.forEach((ph, i) => {
    g.fillStyle = PC[ph] || 'rgba(60,66,80,0.3)';
    g.fillRect(i * bw + 1, phTop, Math.max(2, bw - 2), FP_PHASE - 3);
  });
  g.fillStyle = 'rgba(154,164,181,0.6)';
  g.fillText('Фаза', chartW + 6, phTop + 9);
  // время
  g.fillStyle = 'rgba(154,164,181,0.65)';
  g.textAlign = 'center';
  const step = Math.max(1, Math.round(n / 8));
  for (let i = 0; i < n; i += step) {
    const dt = new Date(bars[i].t);
    const lbl = _fpTf === '1d'
      ? dt.toLocaleDateString('ru', { day: '2-digit', month: '2-digit' })
      : dt.toLocaleString('ru', { day: '2-digit', month: '2-digit', hour: '2-digit' }).replace(',', '');
    g.fillText(lbl, i * bw + bw / 2, phTop + FP_PHASE + 12);
  }
  g.textAlign = 'left';
  // кроссхейр
  if (_fpMouse && !_fpDrag && _fpMouse.x <= chartW && _fpMouse.y <= chartH) {
    g.strokeStyle = 'rgba(154,164,181,0.35)';
    g.setLineDash([3, 3]);
    g.beginPath(); g.moveTo(_fpMouse.x, 0); g.lineTo(_fpMouse.x, chartH); g.stroke();
    g.beginPath(); g.moveTo(0, _fpMouse.y); g.lineTo(chartW, _fpMouse.y); g.stroke();
    g.setLineDash([]);
    const pc = p_at(_fpMouse.y);
    g.fillStyle = 'rgba(30,38,50,0.95)';
    g.fillRect(chartW + 1, _fpMouse.y - 8, FP_AXIS - 2, 16);
    g.fillStyle = '#e6eef8';
    g.font = 'bold 10px monospace';
    g.fillText(fpPrice(pc), chartW + 5, _fpMouse.y + 3);
    g.font = '10px monospace';
  }
}
(() => {
  const cv = document.getElementById('fpCanvas');
  cv.style.cursor = 'crosshair';
  cv.addEventListener('mousedown', ev => {
    if (!_fpData) return;
    ev.preventDefault();
    _fpDrag = { x: ev.clientX, off: _fpView.off, cnt: fpSlice().cnt };
    _fpMouse = null;
    document.getElementById('fpTip').style.display = 'none';
    cv.style.cursor = 'grabbing';
  });
  window.addEventListener('mouseup', () => {
    _fpDrag = null;
    cv.style.cursor = 'crosshair';
  });
  // драг на уровне окна — не обрывается при выходе курсора за канвас
  window.addEventListener('mousemove', ev => {
    if (!_fpDrag || !_fpData) return;
    const { chartW } = fpGeom(cv);
    const bw = chartW / _fpDrag.cnt;
    const dx = ev.clientX - _fpDrag.x;
    const shift = Math.round(dx / bw);
    const nAll = _fpData.bars.length;
    const off = Math.max(0, Math.min(_fpDrag.off + shift, nAll - _fpDrag.cnt));
    if (off !== _fpView.off) { _fpView.off = off; fpRender(); fpStatus(); }
  });
  cv.addEventListener('mousemove', ev => {
    const d = _fpData;
    const tip = document.getElementById('fpTip');
    if (!d) { if (tip) tip.style.display = 'none'; return; }
    if (_fpDrag) { tip.style.display = 'none'; return; }
    const rect = cv.getBoundingClientRect();
    const x = ev.clientX - rect.left, yPx = ev.clientY - rect.top;
    const { chartW } = fpGeom(cv);
    const sl = fpSlice();
    const bw = chartW / sl.bars.length;
    _fpMouse = { x, y: yPx };
    fpRender();
    const i = Math.floor(x / bw);
    if (i < 0 || i >= sl.bars.length || x > chartW) { tip.style.display = 'none'; return; }
    const b = sl.bars[i];
    const dt = new Date(b.t);
    const E = { LONG: '🟢', SHORT: '🔴', NEUTRAL: '⚪' };
    tip.innerHTML = `<b>${dt.toLocaleString('ru', {day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'})}</b>` +
      ` ${E[sl.phases[i]] || ''}<br>` +
      `O ${fpPrice(b.o)} · H ${fpPrice(b.h)}<br>L ${fpPrice(b.l)} · C ${fpPrice(b.c)}<br>` +
      `Vol ${fpNum(b.v)} · <span style="color:${b.d >= 0 ? 'var(--buy)' : 'var(--sell)'};">Δ ${b.d >= 0 ? '+' : ''}${fpNum(b.d)}</span>`;
    if (_fpAnomCache && _fpAnomCache.notes[i]) {
      tip.innerHTML += '<br>' + _fpAnomCache.notes[i].slice(0, 4)
        .map(t => `<span style="color:#ffd23e;">${t}</span>`).join('<br>');
    }
    if (_fpSigNotes[i]) {
      tip.innerHTML += '<br>' + _fpSigNotes[i].slice(0, 5)
        .map(t => `<span style="color:#9fd0ff;">${t}</span>`).join('<br>');
    }
    tip.style.display = 'block';
    tip.style.left = Math.min(x + 16, cv.clientWidth - 195) + 'px';
    tip.style.top = Math.max(4, yPx - 64) + 'px';
  });
  cv.addEventListener('mouseleave', () => {
    document.getElementById('fpTip').style.display = 'none';
    _fpMouse = null;
    if (_fpData) fpRender();
  });
  // зум с якорем под курсором — как на TradingLite/TradingView
  cv.addEventListener('wheel', ev => {
    if (!_fpData) return;
    ev.preventDefault();
    const rect = cv.getBoundingClientRect();
    const x = ev.clientX - rect.left;
    const { chartW } = fpGeom(cv);
    const nAll = _fpData.bars.length;
    const sl = fpSlice();
    const bw = chartW / sl.cnt;
    const anchorIdx = sl.i0 + Math.floor(Math.min(x, chartW - 1) / bw);
    const frac = Math.min(x, chartW - 1) / chartW;
    const next = Math.max(10, Math.min(Math.round(sl.cnt * (ev.deltaY > 0 ? 1.25 : 0.8)), nAll));
    let i0new = Math.round(anchorIdx - frac * next);
    i0new = Math.max(0, Math.min(i0new, nAll - next));
    _fpView.cnt = next;
    _fpView.off = nAll - next - i0new;
    fpRender(); fpStatus();
  }, { passive: false });
  cv.addEventListener('dblclick', () => {
    if (!_fpData) return;
    _fpView = { cnt: _fpData.bars.length, off: 0 };
    fpRender(); fpStatus();
  });
})();
window.addEventListener('resize', () => { if (_fpData) fpRender(); });

// init из URL (?pair=&tf=&bars=) — для /footprint-view (iframe в модалке)
(function () {
  try {
    const q = new URLSearchParams(location.search);
    const p = q.get('pair');
    if (p) { const el = document.getElementById('fpPair'); if (el) el.value = p; }
    const t = (q.get('tf') || '').toLowerCase();
    if (['15m', '1h', '4h', '1d'].includes(t)) _fpTf = t;
    const bb = q.get('bars');
    if (bb) { const el = document.getElementById('fpBars'); if (el && [...el.options].some(o => o.value === bb)) el.value = bb; }
    document.querySelectorAll('#fpTfs button').forEach(btn => {
      const on = btn.dataset.tf === _fpTf;
      btn.style.background = on ? 'rgba(76,201,240,0.15)' : 'var(--dark)';
      btn.style.borderColor = on ? 'rgba(76,201,240,0.5)' : 'var(--border)';
      btn.style.color = on ? '#4cc9f0' : 'var(--muted)';
    });
  } catch (e) {}
})();
(function () {
  const b = document.getElementById('fpModeBtn');
  if (b && _fpMode === 'poc') {
    b.style.background = 'rgba(123,92,255,0.20)';
    b.style.borderColor = 'rgba(123,92,255,0.6)';
    b.style.color = '#b28bff';
  }
})();
loadFootprint();
