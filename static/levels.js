// 📏 уровни-зоны v3 — ЕДИНЫЙ билдер для KChart (base.html) и
// кластерного графика (footprint.js). Вынесен из renderKChart 06.08,
// чтобы iframe /footprint-view имел тот же источник истины.
// Алгоритм: пивоты (фрактал крыло 6) кластеризуются по цене; полоса =
// диапазон кластера от экстремума в пустоту от цены. Мёртвые уровни
// (>2 закрытий сквозь полосу ПОСЛЕ последнего касания) отбраковываются.
// 2 живых на сторону в 8% (фолбэк до 25%, на 12h/1d до 45%). Сила =
// калиброванная P(отскок) (бэктест 05.08.26: год, 308 пар, 117k
// событий 1h + ресемпл 4h/12h/1d). Достройка бедной стороны:
// микро-свинги (крыло 3) + role reversal (пробитые вершины), до 60%.
window._kcBuildZones = function(bars) {
  const out = [];
  if (!bars || bars.length < 30) return out;
  const tail = bars.slice(-120);
  const avg = tail.reduce((s, b) => s + (b.h - b.l), 0) / Math.max(1, tail.length);
  const px = bars[bars.length - 1].c;
  if (!(avg > 0) || !(px > 0)) return out;
  const LW = 6;
  const ups = [], dns = [];
  for (let i = LW; i < bars.length - LW; i++) {
    const b = bars[i];
    let isHi = true, isLo = true;
    for (let j = i - LW; j <= i + LW; j++) {
      if (bars[j].h > b.h) isHi = false;
      if (bars[j].l < b.l) isLo = false;
      if (!isHi && !isLo) break;
    }
    if (isHi) ups.push({ p: b.h, i: i });
    if (isLo) dns.push({ p: b.l, i: i });
  }
  const tol = Math.min(avg * 0.7, px * 0.008);
  const minW = avg * 0.5;
  const maxW = Math.min(avg * 1.5, px * 0.02);
  const cluster = (arr) => {
    const sorted = arr.slice().sort((a, b) => a.p - b.p);
    const cls = [];
    let cur = null;
    sorted.forEach(s => {
      // размах кластера не шире полосы — два разных уровня не клеим
      if (cur && s.p - cur.hiP <= tol && s.p - cur.loP <= maxW) {
        cur.hiP = s.p; cur.members.push(s);
      }
      else { cur = { loP: s.p, hiP: s.p, members: [s] }; cls.push(cur); }
    });
    cls.forEach(c => {
      c.touches = c.members.length;
      c.lastI = c.members.reduce((m, s) => Math.max(m, s.i), 0);
      c.firstT = bars[c.members.reduce((m, s) => Math.min(m, s.i), 1e9)].t;
    });
    return cls;
  };
  // уровень мёртв, если после последнего касания цена >2 раз ЗАКРЫЛАСЬ
  // за полосой (фитильные проколы не считаются)
  const alive = (c, isRes) => {
    let n = 0;
    for (let j = c.lastI + LW; j < bars.length; j++) {
      if (isRes ? bars[j].c > c.hiP : bars[j].c < c.loP) { if (++n > 2) return false; }
    }
    return true;
  };
  const mkBand = (c, isRes) => {
    // полоса лежит НАД фитилями (сопротивление) / ПОД фитилями
    // (поддержка): край касается свингов, тянется в пустоту от цены
    if (isRes ? c.loP <= px * 1.002 : c.hiP >= px * 0.998) return null;
    let lo, hi;
    if (isRes) {
      lo = c.loP;
      hi = Math.min(Math.max(c.hiP, lo + minW), lo + maxW);
    } else {
      hi = c.hiP;
      lo = Math.max(Math.min(c.loP, hi - minW), hi - maxW);
    }
    // порог схлопнутости не выше половины капа ширины (1d-вола >13%)
    if (hi - lo < Math.min(avg * 0.15, maxW * 0.5)) return null;
    // сила = калиброванная P(отскок); базы зависят от ТФ (на старших
    // поддержки держат хуже: 1d подд 32%, 1d сопр 53%)
    const barIv = bars.length > 1 ? bars[1].t - bars[0].t : 3600000;
    let p;
    if (barIv >= 86400000)      p = isRes ? 53 : 32;
    else if (barIv >= 43200000) p = isRes ? 60 : 41;
    else if (barIv >= 14400000) p = isRes ? 65 : 55.5;
    else                        p = isRes ? 66.5 : 61.5;
    if (c.touches >= 5) p += 9; else if (c.touches >= 3) p += 3;
    const wrr = (hi - lo) / avg;
    if (wrr >= 1.1) p += 13; else if (wrr >= 0.7) p += 7;
    if (bars.length - 1 - c.lastI > 72) p += 3;
    const strength = Math.max(20, Math.min(92, Math.round(p)));
    // полоса рисуется от НАЧАЛА ПОСЛЕДНЕГО свинга — длинный хвост через
    // весь график «ездил» по свечам истории и сбивал (кейс ZEC 06.08)
    return { lo, hi, t: bars[Math.max(0, c.lastI - LW)].t, res: isRes,
             touches: c.touches, strength: strength };
  };
  const build = (arr, isRes, soft) => {
    const dist = c => isRes ? (c.loP / px - 1) : (1 - c.hiP / px);
    const cls = cluster(arr)
      .filter(c => isRes ? c.loP > px : c.hiP < px)
      .sort((a, b) => dist(a) - dist(b));
    const acc = [];
    const tryAdd = (c) => {
      if (acc.length >= 2) return;
      const z = mkBand(c, isRes);
      if (!z) return;
      const gap = avg * 0.25;
      if (acc.some(o => z.lo < o.hi + gap && o.lo < z.hi + gap)) return;
      acc.push(z);
    };
    // soft-режим (достройка): alive не требуем — у флип-уровня
    // (пробитая вершина) закрытия «сквозь» были ДО пробоя, это норма
    cls.filter(c => dist(c) <= 0.08 && (soft || alive(c, isRes))).forEach(tryAdd);
    // фолбэк: живых в 8% нет — одна ближайшая до 25% (12h/1d — 45%,
    // soft-достройка на экстремальной параболе — 60%)
    if (!acc.length) {
      const barIv = bars.length > 1 ? bars[1].t - bars[0].t : 0;
      const farLim = barIv >= 43200000 ? 0.45 : (soft ? 0.60 : 0.25);
      const far = cls.filter(c => dist(c) <= farLim);
      const fa = far.filter(c => alive(c, isRes));
      for (const c of (fa.length ? fa : far)) {
        tryAdd(c);
        if (acc.length) break;
      }
    }
    return acc;
  };
  build(ups, true).forEach(z => out.push(z));
  build(dns, false).forEach(z => out.push(z));
  // 🚀 параболика (TAKE/PORTAL/HFT 06.08): сторона бедна, если зон нет
  // ВООБЩЕ или меньше двух и ближайшая дальше 4% — достраиваем
  // микро-свингами (крыло 3) и флипами; сила −8 (не бэктест-класс)
  const distZ = z => z.res ? (z.lo / px - 1) : (1 - z.hi / px);
  const needSide = r => {
    const zs = out.filter(z => z.res === r);
    return !zs.length
      || (zs.length < 2 && Math.min.apply(null, zs.map(distZ)) > 0.04);
  };
  const needRes = needSide(true);
  const needSup = needSide(false);
  if (needRes || needSup) {
    const LW3 = 3;
    const ups3 = [], dns3 = [];
    for (let i = LW3; i < bars.length - LW3; i++) {
      const b = bars[i];
      let isHi = true, isLo = true;
      for (let j = i - LW3; j <= i + LW3; j++) {
        if (bars[j].h > b.h) isHi = false;
        if (bars[j].l < b.l) isLo = false;
        if (!isHi && !isLo) break;
      }
      if (isHi) ups3.push({ p: b.h, i: i });
      if (isLo) dns3.push({ p: b.l, i: i });
    }
    const soften = r => z => {
      // кап 2 на сторону суммарно + не дублируем существующие
      if (out.filter(o => o.res === r).length >= 2) return;
      const g2 = avg * 0.25;
      if (out.some(o => o.res === r && z.lo < o.hi + g2 && o.lo < z.hi + g2)) return;
      z.strength = Math.max(20, (z.strength || 60) - 8);
      out.push(z);
    };
    // + role reversal: пустую сторону поддержек заполняют ПРОБИТЫЕ ХАИ
    // ниже цены (ретест пробитой вершины), сопротивлений — пробитые лои
    if (needRes) {
      build(ups3.concat(dns.concat(dns3).filter(s => s.p > px * 1.002)),
            true, true).forEach(soften(true));
    }
    if (needSup) {
      build(dns3.concat(ups.concat(ups3).filter(s => s.p < px * 0.998)),
            false, true).forEach(soften(false));
    }
  }
  return out;
};

// 📈 ST(10,3)-флипы для меток «тренд сменился» на графике (тумблер
// kc_trend_flips). Возвращает [{t, dir, price}] по закрытым барам.
window._stFlipMarks = function(bars, period, mult) {
  period = period || 10; mult = mult || 3.0;
  const out = [];
  const n = bars.length;
  if (n < period + 4) return out;
  const atr = new Array(n).fill(0);
  let s = 0;
  for (let i = 1; i <= period; i++) {
    s += Math.max(bars[i].h - bars[i].l,
      Math.abs(bars[i].h - bars[i - 1].c), Math.abs(bars[i].l - bars[i - 1].c));
  }
  atr[period] = s / period;
  for (let i = period + 1; i < n; i++) {
    const tr = Math.max(bars[i - 1].h - bars[i - 1].l,
      Math.abs(bars[i - 1].h - bars[i - 2].c),
      Math.abs(bars[i - 1].l - bars[i - 2].c));
    atr[i] = (atr[i - 1] * (period - 1) + tr) / period;
  }
  let fu = 0, fl = 0, dir = 1, prev = 1;
  for (let i = period + 1; i < n; i++) {
    const hl2 = (bars[i].h + bars[i].l) / 2;
    const ub = hl2 + mult * atr[i];
    const lb = hl2 - mult * atr[i];
    fu = (i === period + 1 || ub < fu || bars[i - 1].c > fu) ? ub : fu;
    fl = (i === period + 1 || lb > fl || bars[i - 1].c < fl) ? lb : fl;
    prev = dir;
    if (bars[i].c < fl) dir = -1;
    else if (bars[i].c > fu) dir = 1;
    if (i > period + 2 && dir !== prev) {
      out.push({ t: bars[i].t, dir: dir,
                 price: dir > 0 ? bars[i].l : bars[i].h });
    }
  }
  return out;
};
