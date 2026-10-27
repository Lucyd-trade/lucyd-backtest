(function () {
  "use strict";
  var D = JSON.parse(document.getElementById("data").textContent);
  function addr(i) { return i === null || !D.markets[i] ? "" : D.markets[i].market; }
  D.fills = (D.fills || []).map(function (f) { return [f[0], addr(f[1]), f[2], f[3], f[4], f[5], f[6], f[7]]; });
  D.orders = (D.orders || []).map(function (a) {
    return { id: a[0], market: addr(a[1]), outcome: a[2], side: a[3], tif: a[4], price: a[5], quantity: a[6], filled: a[7],
      avg_price: a[8], status: a[9], reason: a[10], submitted_ms: a[11], slippage: a[12] };
  });
  var NS = "http://www.w3.org/2000/svg";
  var LIMIT = 500;
  var tip = document.getElementById("tip");

  // ---------- helpers ----------
  function el(tag, attrs, kids) {
    var e = tag.indexOf("svg:") === 0 ? document.createElementNS(NS, tag.slice(4)) : document.createElement(tag);
    for (var k in attrs || {}) {
      if (k === "text") e.textContent = attrs[k];
      else if (attrs[k] !== null && attrs[k] !== undefined) e.setAttribute(k, attrs[k]);
    }
    (kids || []).forEach(function (c) { if (c) e.appendChild(c); });
    return e;
  }
  function s(tag, attrs) { return el("svg:" + tag, attrs); }
  function pad(n) { return (n < 10 ? "0" : "") + n; }
  function fmtTime(ms, span) {
    var d = new Date(ms);
    var hm = pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes());
    var md = pad(d.getUTCMonth() + 1) + "-" + pad(d.getUTCDate());
    if (span === undefined) return d.getUTCFullYear() + "-" + md + " " + hm + ":" + pad(d.getUTCSeconds());
    if (span < 15 * 60e3) return hm + ":" + pad(d.getUTCSeconds());
    if (span < 2 * 86400e3) return hm;
    return md + " " + hm;
  }
  function num(v, d) { return v === null || v === undefined ? "–" : Number(v).toFixed(d === undefined ? 2 : d); }
  function signed(v) { return v === null || v === undefined ? "–" : (v >= 0 ? "+" : "") + Number(v).toFixed(2); }
  function short(a) { return a && a.length > 14 ? a.slice(0, 8) + "…" + a.slice(-4) : a; }
  function showTip(text, x, y) {
    tip.textContent = text;
    tip.hidden = false;
    var w = tip.offsetWidth, h = tip.offsetHeight;
    var left = x + 14, top = y + 14;
    if (left + w > window.innerWidth - 8) left = x - w - 14;
    if (left < 8) left = 8;
    if (top + h > window.innerHeight - 8) top = y - h - 14;
    tip.style.left = left + "px";
    tip.style.top = top + "px";
  }
  function hideTip() { tip.hidden = true; }
  function niceTicks(lo, hi, n) {
    if (!(hi > lo)) { hi = lo + 1; lo = lo - 1; }
    var raw = (hi - lo) / n, mag = Math.pow(10, Math.floor(Math.log10(raw)));
    var step = [1, 2, 2.5, 5, 10].map(function (m) { return m * mag; }).find(function (v) { return v >= raw; });
    var out = [];
    for (var v = Math.ceil(lo / step) * step; v <= hi + step * 1e-9; v += step) out.push(+v.toFixed(10));
    return { ticks: out, step: step };
  }
  function bisect(xs, x) {
    var lo = 0, hi = xs.length - 1;
    while (hi - lo > 1) { var m = (lo + hi) >> 1; if (xs[m] <= x) lo = m; else hi = m; }
    return Math.abs(xs[hi] - x) < Math.abs(xs[lo] - x) ? hi : lo;
  }
  // keep the shape of a long series: min and max of every pixel bucket
  function thin(xs, ys, width) {
    if (xs.length <= width * 2) return [xs, ys];
    var ox = [], oy = [], x0 = xs[0], span = xs[xs.length - 1] - x0 || 1, b = -1, lo, hi;
    for (var i = 0; i < xs.length; i++) {
      var k = Math.floor((xs[i] - x0) / span * width);
      if (k !== b) {
        if (b >= 0) { ox.push(xs[lo], xs[hi]); oy.push(ys[lo], ys[hi]); }
        b = k; lo = hi = i;
      } else {
        if (ys[i] < ys[lo]) lo = i;
        if (ys[i] > ys[hi]) hi = i;
      }
    }
    ox.push(xs[lo], xs[hi]); oy.push(ys[lo], ys[hi]);
    var idx = ox.map(function (_, i) { return i; }).sort(function (a, b) { return ox[a] - ox[b]; });
    return [idx.map(function (i) { return ox[i]; }), idx.map(function (i) { return oy[i]; })];
  }

  // ---------- line chart ----------
  // o: {xs, ys, yfmt, split, area, step, lineCls, markers: [{x, y, cls, up, tip}], xmin, xmax, ymin, ymax, label}
  function lineChart(box, o) {
    box.textContent = "";
    if (!o.xs.length) { box.appendChild(el("div", { class: "empty", text: o.empty || "No data" })); return; }
    var W = Math.max(280, box.clientWidth - 16), H = W < 500 ? 200 : 260;
    var m = { l: 56, r: 12, t: 12, b: 26 };
    var xmin = o.xmin !== undefined ? o.xmin : o.xs[0], xmax = o.xmax !== undefined ? o.xmax : o.xs[o.xs.length - 1];
    if (xmax <= xmin) xmax = xmin + 1;
    var ylo = Math.min.apply(null, o.ys), yhi = Math.max.apply(null, o.ys);
    (o.markers || []).forEach(function (k) { ylo = Math.min(ylo, k.y); yhi = Math.max(yhi, k.y); });
    if (o.ymin !== undefined) ylo = Math.min(ylo, o.ymin);
    if (o.ymax !== undefined) yhi = Math.max(yhi, o.ymax);
    if (yhi === ylo) { yhi += 1; ylo -= 1; }
    var yt = niceTicks(ylo, yhi, 4);
    ylo = Math.min(ylo, yt.ticks[0]); yhi = Math.max(yhi, yt.ticks[yt.ticks.length - 1]);
    var X = function (x) { return m.l + (x - xmin) / (xmax - xmin) * (W - m.l - m.r); };
    var Y = function (y) { return m.t + (yhi - y) / (yhi - ylo) * (H - m.t - m.b); };
    var svg = s("svg", { viewBox: "0 0 " + W + " " + H, role: "img", "aria-label": o.label || "" });
    yt.ticks.forEach(function (v) {
      svg.appendChild(s("line", { class: "grid", x1: m.l, x2: W - m.r, y1: Y(v), y2: Y(v) }));
      var t = s("text", { x: m.l - 6, y: Y(v) + 4, "text-anchor": "end" });
      t.textContent = (o.yfmt || num)(v);
      svg.appendChild(t);
    });
    var span = xmax - xmin, nx = Math.max(2, Math.floor((W - m.l) / 110));
    for (var i = 0; i <= nx; i++) {
      var xv = xmin + span * i / nx, t2 = s("text", { x: X(xv), y: H - 8, "text-anchor": i === 0 ? "start" : i === nx ? "end" : "middle" });
      t2.textContent = fmtTime(xv, span);
      svg.appendChild(t2);
    }
    svg.appendChild(s("line", { class: "axis", x1: m.l, x2: W - m.r, y1: H - m.b, y2: H - m.b }));
    var th = thin(o.xs, o.ys, W), d = "";
    for (var j = 0; j < th[0].length; j++) {
      var px = X(th[0][j]).toFixed(1), py = Y(th[1][j]).toFixed(1);
      if (j === 0) d = "M" + px + " " + py;
      else if (o.step) d += "H" + px + "V" + py;
      else d += "L" + px + " " + py;
    }
    if (o.step && th[0].length && th[0][th[0].length - 1] < xmax) d += "H" + X(xmax).toFixed(1);
    if (o.area) {
      var base = Y(0).toFixed(1);
      svg.appendChild(s("path", { class: "dd", d: d + "V" + base + "H" + X(th[0][0]).toFixed(1) + "Z" }));
    } else {
      svg.appendChild(s("path", { class: o.lineCls || "line", d: d }));
    }
    if (o.split !== null && o.split !== undefined && o.split > xmin && o.split < xmax) {
      svg.appendChild(s("line", { class: "split-line", x1: X(o.split), x2: X(o.split), y1: m.t, y2: H - m.b }));
      var lab = s("text", { class: "split-label", x: X(o.split) + 4, y: m.t + 10 });
      lab.textContent = "validation →";
      svg.appendChild(lab);
    }
    var mk = (o.markers || []).map(function (k) {
      var cx = X(k.x), cy = Y(k.y), r = 5;
      var pts = k.up ? [cx, cy - r, cx - r, cy + r * 0.8, cx + r, cy + r * 0.8] : [cx, cy + r, cx - r, cy - r * 0.8, cx + r, cy - r * 0.8];
      svg.appendChild(s("polygon", { class: k.cls, points: pts.map(function (v) { return v.toFixed(1); }).join(" ") }));
      return { cx: cx, cy: cy, tip: k.tip };
    });
    var cross = s("line", { class: "cross", y1: m.t, y2: H - m.b, visibility: "hidden" });
    var dot = s("circle", { class: "dot", r: 4, visibility: "hidden" });
    svg.appendChild(cross); svg.appendChild(dot);
    function move(ev) {
      var r = svg.getBoundingClientRect(), sx = (ev.clientX - r.left) * W / r.width, sy = (ev.clientY - r.top) * H / r.height;
      if (sx < m.l || sx > W - m.r) { leave(); return; }
      var best = null, bd = 144;
      mk.forEach(function (k) { var dd = (k.cx - sx) * (k.cx - sx) + (k.cy - sy) * (k.cy - sy); if (dd < bd) { bd = dd; best = k; } });
      if (best) {
        cross.setAttribute("visibility", "hidden"); dot.setAttribute("visibility", "hidden");
        showTip(best.tip, ev.clientX, ev.clientY);
        return;
      }
      var x = xmin + (sx - m.l) / (W - m.l - m.r) * span, i = bisect(o.xs, x);
      if (o.step && o.xs[i] > x && i > 0) i--;
      var cx = X(o.xs[i]), cy = Y(o.ys[i]);
      cross.setAttribute("x1", cx); cross.setAttribute("x2", cx); cross.setAttribute("visibility", "visible");
      dot.setAttribute("cx", cx); dot.setAttribute("cy", cy); dot.setAttribute("visibility", "visible");
      showTip(fmtTime(o.xs[i]) + " UTC\n" + (o.tipLabel || "") + (o.yfmt || num)(o.ys[i]), ev.clientX, ev.clientY);
    }
    function leave() { cross.setAttribute("visibility", "hidden"); dot.setAttribute("visibility", "hidden"); hideTip(); }
    svg.addEventListener("pointermove", move);
    svg.addEventListener("pointerdown", move);
    svg.addEventListener("pointerleave", leave);
    box.appendChild(svg);
    if (o.legend) box.appendChild(o.legend);
  }

  // ---------- equity and drawdown ----------
  var split = D.periods ? D.periods.split_ms : null;
  function drawEquity() {
    var xs = D.equity.map(function (p) { return p.ts_ms; }), ys = D.equity.map(function (p) { return p.equity; });
    lineChart(document.getElementById("equity-chart"), {
      xs: xs, ys: ys, split: split, label: "Equity over time", tipLabel: "equity ", xmin: D.from_ms, xmax: Math.max(D.to_ms, xs[xs.length - 1] || 0)
    });
    var peak = -Infinity, dd = ys.map(function (v) { peak = Math.max(peak, v); return v - peak; });
    lineChart(document.getElementById("drawdown-chart"), {
      xs: xs, ys: dd, split: split, area: true, ymax: 0, label: "Drawdown over time", tipLabel: "drawdown ", xmin: D.from_ms, xmax: Math.max(D.to_ms, xs[xs.length - 1] || 0)
    });
  }

  // ---------- P&L by market ----------
  var selected = null;
  function traded() { return D.markets.filter(function (m) { return m.fills > 0; }); }
  function drawPnl() {
    var box = document.getElementById("pnl-chart");
    box.textContent = "";
    var ms = traded().slice();
    if (!ms.length) { box.appendChild(el("div", { class: "empty", text: "No traded markets" })); return; }
    if (document.getElementById("pnl-sort").value === "pnl") ms.sort(function (a, b) { return b.pnl - a.pnl; });
    else ms.sort(function (a, b) { return a.end < b.end ? -1 : a.end > b.end ? 1 : 0; });
    var W = Math.max(280, box.clientWidth - 16), H = W < 500 ? 200 : 240, m = { l: 56, r: 12, t: 12, b: 12 };
    var lo = Math.min(0, Math.min.apply(null, ms.map(function (x) { return x.pnl; })));
    var hi = Math.max(0, Math.max.apply(null, ms.map(function (x) { return x.pnl; })));
    var yt = niceTicks(lo, hi, 4);
    lo = Math.min(lo, yt.ticks[0]); hi = Math.max(hi, yt.ticks[yt.ticks.length - 1]);
    var Y = function (y) { return m.t + (hi - y) / (hi - lo) * (H - m.t - m.b); };
    var svg = s("svg", { viewBox: "0 0 " + W + " " + H, role: "img", "aria-label": "P&L by market" });
    yt.ticks.forEach(function (v) {
      svg.appendChild(s("line", { class: "grid", x1: m.l, x2: W - m.r, y1: Y(v), y2: Y(v) }));
      var t = s("text", { x: m.l - 6, y: Y(v) + 4, "text-anchor": "end" }); t.textContent = num(v); svg.appendChild(t);
    });
    var bw = (W - m.l - m.r) / ms.length, gap = bw > 6 ? 2 : 0, r = bw > 8 ? 3 : 0;
    ms.forEach(function (x, i) {
      var y0 = Y(0), y1 = Y(x.pnl), top = Math.min(y0, y1), h = Math.max(1, Math.abs(y1 - y0));
      var cls = (x.pnl >= 0 ? "bar-pos" : "bar-neg") + (x.market === selected ? " bar-sel" : "");
      svg.appendChild(s("rect", { class: cls, x: (m.l + i * bw + gap / 2).toFixed(2), y: top.toFixed(1), width: Math.max(0.6, bw - gap).toFixed(2), height: h.toFixed(1), rx: Math.min(r, h / 2) }));
    });
    svg.appendChild(s("line", { class: "axis", x1: m.l, x2: W - m.r, y1: Y(0), y2: Y(0) }));
    function at(ev) {
      var rc = svg.getBoundingClientRect(), sx = (ev.clientX - rc.left) * W / rc.width, i = Math.floor((sx - m.l) / bw);
      return i >= 0 && i < ms.length ? ms[i] : null;
    }
    svg.addEventListener("pointermove", function (ev) {
      var x = at(ev);
      if (!x) { hideTip(); return; }
      showTip(short(x.market) + " · " + x.period + "\nends " + x.end.replace("T", " ").slice(0, 16) + " UTC\nP&L " + signed(x.pnl) + " · " + x.result + " · " + x.sample, ev.clientX, ev.clientY);
    });
    svg.addEventListener("pointerleave", hideTip);
    svg.addEventListener("click", function (ev) { var x = at(ev); if (x) select(x.market, true); });
    box.appendChild(svg);
    box.appendChild(el("div", { class: "legend" }, [
      el("span", {}, [el("i", { style: "background:var(--pos)" }), document.createTextNode("profit")]),
      el("span", {}, [el("i", { style: "background:var(--neg)" }), document.createTextNode("loss")])
    ]));
  }

  // ---------- market detail ----------
  var outcome = 0;
  var fillsBy = {};
  (D.fills || []).forEach(function (f) { (fillsBy[f[1]] = fillsBy[f[1]] || []).push(f); });
  var withOrders = D.markets.filter(function (m) { return m.orders > 0; });
  var detailSel = document.getElementById("detail-market");
  withOrders.forEach(function (m) {
    detailSel.appendChild(el("option", { value: m.market, text: m.end.replace("T", " ").slice(0, 16) + " · " + m.period + " · " + short(m.market) + " · " + signed(m.pnl) }));
  });
  function select(addr, scroll) {
    selected = addr;
    detailSel.value = addr;
    drawDetail();
    drawPnl();
    if (scroll) document.getElementById("market-detail").scrollIntoView({ behavior: "smooth", block: "start" });
  }
  function drawDetail() {
    var box = document.getElementById("detail-chart"), note = document.getElementById("detail-note");
    var m = D.markets.find(function (x) { return x.market === selected; });
    if (!m) { box.textContent = ""; box.appendChild(el("div", { class: "empty", text: "No market with own orders" })); note.textContent = ""; return; }
    var marks = m.marks || [];
    var xs = marks.map(function (p) { return p[0]; }), ys = marks.map(function (p) { return outcome === 0 ? p[1] : 1 - p[1]; });
    var fl = (fillsBy[m.market] || []).filter(function (f) { return f[2] === outcome; });
    var other = (fillsBy[m.market] || []).length - fl.length;
    var name = outcome === 0 ? "up" : "down";
    note.textContent = name + " price (book mid) and own fills on " + name + ": " + fl.length + (other ? " (" + other + " on the other outcome)" : "") +
      ". Result " + m.result + ", P&L " + signed(m.pnl) + " USDC, " + m.orders + " order(s), sample " + m.sample + ".";
    var legend = el("div", { class: "legend" }, [
      el("span", {}, [el("i", { style: "background:var(--mark)" }), document.createTextNode(name + " price")]),
      el("span", {}, [el("i", { style: "background:var(--buy)" }), document.createTextNode("▲ own buy")]),
      el("span", {}, [el("i", { style: "background:var(--sell)" }), document.createTextNode("▼ own sell")])
    ]);
    lineChart(box, {
      xs: xs, ys: ys, step: true, lineCls: "mark-line", label: name + " price", tipLabel: name + " ", ymin: 0, ymax: 1,
      xmin: Date.parse(m.start), xmax: Date.parse(m.end), yfmt: function (v) { return num(v, 3); },
      empty: "No price recorded for this market (version 1 report or no book)", legend: legend,
      markers: fl.map(function (f) {
        var buy = f[3] === "buy";
        return { x: f[0], y: f[4], up: buy, cls: buy ? "buy" : "sell", tip: fmtTime(f[0]) + " UTC\n" + f[3] + " " + num(f[5], 0) + " " + name + " @ " + num(f[4], 4) + "\n" + (f[6] || "") + " · order " + f[7] };
      })
    });
    if (!xs.length && fl.length) {
      // no marks: still show fills
      lineChart(box, {
        xs: fl.map(function (f) { return f[0]; }), ys: fl.map(function (f) { return f[4]; }), step: true, lineCls: "mark-line", ymin: 0, ymax: 1,
        xmin: Date.parse(m.start), xmax: Date.parse(m.end), legend: legend, yfmt: function (v) { return num(v, 3); },
        markers: fl.map(function (f) { var buy = f[3] === "buy"; return { x: f[0], y: f[4], up: buy, cls: buy ? "buy" : "sell", tip: f[3] + " " + num(f[5], 0) + " @ " + num(f[4], 4) }; })
      });
    }
  }
  detailSel.addEventListener("change", function () { select(detailSel.value, false); });
  Array.prototype.forEach.call(document.querySelectorAll(".seg button"), function (b) {
    b.addEventListener("click", function () {
      outcome = +b.getAttribute("data-outcome");
      Array.prototype.forEach.call(document.querySelectorAll(".seg button"), function (x) { x.setAttribute("aria-pressed", x === b ? "true" : "false"); });
      drawDetail();
    });
  });
  Array.prototype.forEach.call(document.querySelectorAll("a[data-market]"), function (a) {
    a.addEventListener("click", function (ev) { ev.preventDefault(); select(a.getAttribute("data-market"), true); });
  });

  // ---------- tables ----------
  function table(box, cols, rows, state, onRow) {
    box.textContent = "";
    if (state.sort !== undefined) {
      var c = cols[state.sort], dir = state.dir;
      rows = rows.slice().sort(function (a, b) {
        var x = c.key(a), y = c.key(b);
        if (x === y) return 0;
        if (x === null || x === undefined) return 1;
        if (y === null || y === undefined) return -1;
        return (x < y ? -1 : 1) * dir;
      });
    }
    var thead = el("tr", {}, cols.map(function (c, i) {
      var th = el("th", { class: c.num ? "num" : null, scope: "col", text: c.title + (state.sort === i ? (state.dir > 0 ? " ▲" : " ▼") : "") });
      th.addEventListener("click", function () {
        state.dir = state.sort === i ? -state.dir : (c.num ? -1 : 1);
        state.sort = i;
        table(box, cols, rows, state, onRow);
      });
      return th;
    }));
    var body = el("tbody");
    rows.slice(0, LIMIT).forEach(function (r) {
      var tr = el("tr", { class: onRow ? "clickable" : null }, cols.map(function (c) {
        var v = c.show ? c.show(r) : c.key(r);
        return el("td", { class: (c.num ? "num" : "") + (c.cls ? " " + c.cls(r) : ""), text: v === null || v === undefined ? "–" : String(v) });
      }));
      if (onRow) tr.addEventListener("click", function () { onRow(r); });
      body.appendChild(tr);
    });
    box.appendChild(el("table", {}, [el("thead", {}, [thead]), body]));
    box.appendChild(el("div", { class: "more", text: rows.length > LIMIT ? "Showing " + LIMIT + " of " + rows.length + " rows; narrow the filter to see the rest." : rows.length + " row(s)" }));
  }
  var pnlCls = function (r) { return r.pnl < 0 ? "neg" : "pos"; };
  var marketCols = [
    { title: "End (UTC)", key: function (r) { return r.end; }, show: function (r) { return r.end.replace("T", " ").slice(0, 16); } },
    { title: "Period", key: function (r) { return r.period; } },
    { title: "Market", key: function (r) { return r.market; }, show: function (r) { return short(r.market); } },
    { title: "Sample", key: function (r) { return r.sample; } },
    { title: "Result", key: function (r) { return r.result; } },
    { title: "P&L", num: true, key: function (r) { return r.pnl; }, show: function (r) { return signed(r.pnl); }, cls: pnlCls },
    { title: "Fills", num: true, key: function (r) { return r.fills; } },
    { title: "Orders", num: true, key: function (r) { return r.orders; } },
    { title: "Turnover", num: true, key: function (r) { return r.turnover; }, show: function (r) { return num(r.turnover); } },
    { title: "Fees", num: true, key: function (r) { return r.fees; }, show: function (r) { return num(r.fees); } },
    { title: "Payout", num: true, key: function (r) { return r.payout; }, show: function (r) { return num(r.payout); } },
    { title: "Recorded fills", num: true, key: function (r) { return r.recorded_fills; } }
  ];
  var mState = {};
  function drawMarkets() {
    var q = document.getElementById("markets-filter").value.trim().toLowerCase();
    var only = document.getElementById("markets-traded").checked;
    var rows = D.markets.filter(function (m) {
      if (only && !m.orders) return false;
      return !q || [m.market, m.result, m.sample, m.period, m.end].join(" ").toLowerCase().indexOf(q) >= 0;
    });
    table(document.getElementById("markets-table"), marketCols, rows, mState, function (r) { if (r.orders) select(r.market, true); });
  }
  var orderCols = [
    { title: "#", num: true, key: function (r) { return r.id; } },
    { title: "Placed (UTC)", key: function (r) { return r.submitted_ms; }, show: function (r) { return fmtTime(r.submitted_ms); } },
    { title: "Market", key: function (r) { return r.market; }, show: function (r) { return short(r.market); } },
    { title: "Outcome", key: function (r) { return r.outcome === 0 ? "up" : "down"; } },
    { title: "Side", key: function (r) { return r.side; } },
    { title: "Type", key: function (r) { return r.tif; } },
    { title: "Price", num: true, key: function (r) { return r.price; }, show: function (r) { return num(r.price, 4); } },
    { title: "Qty", num: true, key: function (r) { return r.quantity; }, show: function (r) { return num(r.quantity, 0); } },
    { title: "Filled", num: true, key: function (r) { return r.filled; }, show: function (r) { return num(r.filled, 0); } },
    { title: "Avg price", num: true, key: function (r) { return r.avg_price; }, show: function (r) { return num(r.avg_price, 4); } },
    { title: "Slippage", num: true, key: function (r) { return r.slippage; }, show: function (r) { return num(r.slippage, 4); } },
    { title: "Status", key: function (r) { return r.status; } },
    { title: "Reason", key: function (r) { return r.reason; } }
  ];
  var oState = {};
  function drawOrders() {
    var q = document.getElementById("orders-filter").value.trim().toLowerCase();
    var st = document.getElementById("orders-status").value;
    var rows = D.orders.filter(function (o) {
      if (st && o.status !== st) return false;
      return !q || [o.market, o.reason || "", o.side, o.tif, o.outcome === 0 ? "up" : "down", o.id].join(" ").toLowerCase().indexOf(q) >= 0;
    });
    table(document.getElementById("orders-table"), orderCols, rows, oState, function (r) { select(r.market, true); });
  }
  ["markets-filter", "markets-traded"].forEach(function (id) { document.getElementById(id).addEventListener("input", drawMarkets); });
  ["orders-filter", "orders-status"].forEach(function (id) { document.getElementById(id).addEventListener("input", drawOrders); });
  document.getElementById("pnl-sort").addEventListener("change", drawPnl);

  // ---------- theme ----------
  var root = document.documentElement;
  try { var saved = localStorage.getItem("lucyd-report-theme"); if (saved) root.setAttribute("data-theme", saved); } catch (e) { /* storage off */ }
  document.getElementById("theme").addEventListener("click", function () {
    var dark = root.getAttribute("data-theme") ? root.getAttribute("data-theme") === "dark" : window.matchMedia("(prefers-color-scheme: dark)").matches;
    var next = dark ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try { localStorage.setItem("lucyd-report-theme", next); } catch (e) { /* storage off */ }
  });

  // ---------- go ----------
  if (withOrders.length) {
    var worst = D.summary.worst_market, best = D.summary.best_market;
    selected = (worst || best || {}).market || withOrders[0].market;
    detailSel.value = selected;
  }
  function drawAll() { drawEquity(); drawPnl(); drawDetail(); }
  drawAll();
  drawMarkets();
  drawOrders();
  var t = null;
  window.addEventListener("resize", function () { clearTimeout(t); t = setTimeout(drawAll, 150); });
})();
