/**
 * MakoletChain – Shared chart utilities
 */

'use strict';

const PALETTE = {
    profit:  '#22c55e',
    loss:    '#ef4444',
    accent:  '#6366f1',
    surface: '#1e293b',
    border:  '#334155',
    gridLine:'#1e293b',
    tickText:'#94a3b8',
};

function formatMoney(amount) {
    return '\u20AA\u202F' + Number(amount).toLocaleString('he-IL', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    });
}

function hexToRgba(hex, alpha) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r},${g},${b},${alpha})`;
}

function buildProfitBarChart(canvasId, labels, values) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    const colors = values.map(v =>
        v >= 0 ? hexToRgba(PALETTE.profit, 0.85) : hexToRgba(PALETTE.loss, 0.85)
    );
    const borderColors = values.map(v =>
        v >= 0 ? PALETTE.profit : PALETTE.loss
    );
    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'רווח משוער (₪)',
                data: values,
                backgroundColor: colors,
                borderColor: borderColors,
                borderWidth: 2,
                borderRadius: 6,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: {
                        label: ctx => ' ' + formatMoney(ctx.parsed.y),
                    },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: PALETTE.tickText, font: { size: 12 } },
                    border: { color: PALETTE.border },
                },
                y: {
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText,
                        font: { size: 11 },
                        callback: v => '₪ ' + Number(v).toLocaleString('he-IL'),
                    },
                },
            },
        },
    });
}

function initDailyActivityChart(canvasId, payload) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return null;
    const ctx = canvas.getContext('2d');
    const datasets = (payload.users || []).map(u => ({
        label: u.name,
        data: u.data,
        borderColor: u.color,
        backgroundColor: hexToRgba(u.color, 0.85),
        borderWidth: 1,
        borderRadius: 4,
    }));
    return new Chart(ctx, {
        type: 'bar',
        data: { labels: payload.labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            barPercentage: 0.8,
            categoryPercentage: 0.8,
            plugins: {
                legend: {
                    position: 'bottom',
                    rtl: true,
                    labels: {
                        color: PALETTE.tickText,
                        font: { size: 12 },
                        usePointStyle: true,
                        boxWidth: 8,
                    },
                },
                tooltip: {
                    rtl: true,
                    backgroundColor: 'rgba(15,23,42,0.97)',
                    titleColor: '#f1f5f9',
                    bodyColor: '#cbd5e1',
                    padding: 10,
                    callbacks: {
                        footer: items => {
                            const total = items.reduce((s, it) => s + (it.parsed.y || 0), 0);
                            return 'סה"כ: ' + total;
                        },
                    },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: PALETTE.tickText, font: { size: 11 } },
                    border: { color: PALETTE.border },
                },
                y: {
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText,
                        font: { size: 11 },
                        precision: 0,
                        stepSize: 1,
                    },
                },
            },
        },
    });
}

/* ── /sales charts ───────────────────────────────────────────
 * Red/blue decision lives 100% in the backend helpers; the
 * frontend only maps the "red"/"blue" string to a hex.
 */
const SALES_COLOR = { red: '#D85A30', blue: '#378ADD' };

function salesShekel(v) {
    return '₪ ' + Number(v).toLocaleString('he-IL', {
        maximumFractionDigits: 0,
    });
}

function salesAxisK(v) {
    v = Number(v);
    return Math.abs(v) >= 1000
        ? '₪' + Math.round(v / 1000) + 'K'
        : '₪' + v;
}

function initSalesDailyChart(canvasId, payload) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !payload || !payload.length) return null;
    const labels = payload.map(p =>
        p.label_secondary ? [p.date, p.label_secondary] : p.date
    );
    const values = payload.map(p => p.value);
    const bg = payload.map(p => hexToRgba(SALES_COLOR[p.color] || SALES_COLOR.blue, 0.85));
    const border = payload.map(p => SALES_COLOR[p.color] || SALES_COLOR.blue);
    return new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: { labels, datasets: [{
            data: values, backgroundColor: bg, borderColor: border,
            borderWidth: 1, borderRadius: 4,
        }] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: { label: c => ' ' + salesShekel(c.parsed.y) },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: PALETTE.tickText, font: { size: 11 } },
                    border: { color: PALETTE.border },
                },
                y: {
                    position: 'right',
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        precision: 0, callback: salesAxisK,
                    },
                },
            },
        },
    });
}

function initSalesDowChart(canvasId, payload) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !payload || !payload.length) return null;
    const labels = payload.map(p => p.label);
    const values = payload.map(p => p.value);
    const bg = payload.map(p => hexToRgba(SALES_COLOR[p.color] || SALES_COLOR.blue, 0.85));
    const border = payload.map(p => SALES_COLOR[p.color] || SALES_COLOR.blue);
    return new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: { labels, datasets: [{
            data: values, backgroundColor: bg, borderColor: border,
            borderWidth: 1, borderRadius: 4,
        }] },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: {
                        label: c => ' ' + salesShekel(c.parsed.x) + ' ממוצע',
                    },
                },
            },
            scales: {
                x: {
                    position: 'top',
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        precision: 0, callback: salesAxisK,
                    },
                },
                y: {
                    position: 'right',
                    grid: { display: false },
                    border: { color: PALETTE.border },
                    ticks: { color: PALETTE.tickText, font: { size: 12 } },
                },
            },
        },
    });
}

function initSalesCumulativeChart(canvasId, payload) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !payload || !payload.length) return null;
    const labels = payload.map(p => p.date);
    const values = payload.map(p => p.value);
    return new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: { labels, datasets: [{
            data: values,
            borderColor: '#378ADD',
            backgroundColor: 'rgba(55,138,221,0.12)',
            borderWidth: 2,
            fill: true,
            tension: 0.3,
            pointRadius: 2,
            pointHoverRadius: 4,
        }] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: {
                        label: c => ' ' + salesShekel(c.parsed.y) + ' מצטבר',
                    },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: PALETTE.tickText, font: { size: 11 } },
                    border: { color: PALETTE.border },
                },
                y: {
                    position: 'right',
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        precision: 0, callback: salesAxisK,
                    },
                },
            },
        },
    });
}

/* ── Network overview (CEO view) ─────────────────────────────
 * All five Chart.js charts on /home_network.html. Each function
 * is called once per page load by templates/home_network.html.
 */

function _branchColor(branchId, branches) {
    if (!branches) return PALETTE.accent;
    const b = branches.find(x => x.id === branchId);
    return (b && b.color) || PALETTE.accent;
}

function initMonthlyRevenueChart(canvasId, rows, branches) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !rows) return null;
    const labels = rows.map(r => r.branch_name);
    const values = rows.map(r => r.value);
    const bg = rows.map(r => hexToRgba(_branchColor(r.branch_id, branches), 0.85));
    const border = rows.map(r => _branchColor(r.branch_id, branches));
    return new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: { labels, datasets: [{
            data: values, backgroundColor: bg, borderColor: border,
            borderWidth: 1, borderRadius: 6,
            // Constrain bar width — without this, 2 categories produce
            // ~450px-wide bars that crowd the card edges.
            categoryPercentage: 0.5,
            barPercentage: 0.7,
        }] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: { label: c => ' ' + salesShekel(c.parsed.y) },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: PALETTE.tickText, font: { size: 11 } },
                    border: { color: PALETTE.border },
                },
                y: {
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        callback: salesAxisK,
                    },
                },
            },
        },
    });
}

function initTrend6mChart(canvasId, payload) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !payload) return null;
    const labels = payload.months || [];
    const datasets = (payload.series || []).map(s => ({
        label: s.branch_name,
        data: s.data,
        borderColor: s.color,
        backgroundColor: hexToRgba(s.color, 0.15),
        borderWidth: 2,
        tension: 0.3,
        pointRadius: 3,
        pointHoverRadius: 5,
    }));
    return new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: PALETTE.tickText, font: { size: 12 }, usePointStyle: true, boxWidth: 8 },
                },
                tooltip: {
                    rtl: true,
                    callbacks: { label: c => ' ' + c.dataset.label + ': ' + salesShekel(c.parsed.y) },
                },
            },
            scales: {
                x: {
                    grid: { display: false },
                    ticks: { color: PALETTE.tickText, font: { size: 11 } },
                    border: { color: PALETTE.border },
                },
                y: {
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        callback: salesAxisK,
                    },
                },
            },
        },
    });
}

function initProfitabilityChart(canvasId, rows) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !rows || !rows.length) return null;
    const labels = rows.map(r => r.branch_name);
    const goodsData = rows.map(r => r.goods);
    const salaryData = rows.map(r => r.salary);
    const fixedData = rows.map(r => r.fixed);
    const elecData = rows.map(r => r.electricity);
    const profitData = rows.map(r => Math.max(0, r.profit));

    return new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels,
            datasets: [
                { label: 'סחורה', data: goodsData, backgroundColor: hexToRgba('#D85A30', 0.85), borderColor: '#D85A30', borderWidth: 1 },
                { label: 'שכר', data: salaryData, backgroundColor: hexToRgba('#7F77DD', 0.85), borderColor: '#7F77DD', borderWidth: 1 },
                { label: 'הוצאות קבועות', data: fixedData, backgroundColor: hexToRgba('#888780', 0.85), borderColor: '#888780', borderWidth: 1 },
                { label: 'חשמל', data: elecData, backgroundColor: hexToRgba('#E0A82E', 0.85), borderColor: '#E0A82E', borderWidth: 1 },
                { label: 'רווח', data: profitData, backgroundColor: hexToRgba(PALETTE.profit, 0.85), borderColor: PALETTE.profit, borderWidth: 1 },
            ],
        },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            // RTL needs breathing room between the y-tick labels (rendered
            // on visual-right) and the bar area, plus a side margin so the
            // bar end never butts the card edge.
            layout: { padding: { left: 12, right: 12 } },
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: PALETTE.tickText, font: { size: 12 }, usePointStyle: true, boxWidth: 10 },
                },
                tooltip: {
                    rtl: true,
                    callbacks: {
                        label: c => ' ' + c.dataset.label + ': ' + salesShekel(c.parsed.x),
                        afterBody: items => {
                            if (!items.length) return '';
                            const row = rows[items[0].dataIndex];
                            return ['', 'הכנסות: ' + salesShekel(row.revenue),
                                    'רווח נטו: ' + salesShekel(row.profit) + ' (' + row.profit_pct + '%)'];
                        },
                    },
                },
            },
            scales: {
                x: {
                    stacked: true,
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: { color: PALETTE.tickText, font: { size: 11 }, callback: salesAxisK },
                },
                y: {
                    stacked: true,
                    grid: { display: false },
                    border: { color: PALETTE.border },
                    ticks: { color: PALETTE.tickText, font: { size: 12 }, padding: 14 },
                },
            },
        },
    });
}

function initAvgBasketChart(canvasId, rows, branches) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !rows) return null;
    const labels = rows.map(r => r.branch_name);
    const values = rows.map(r => r.value);
    const bg = rows.map(r => hexToRgba(_branchColor(r.branch_id, branches), 0.85));
    const border = rows.map(r => _branchColor(r.branch_id, branches));
    return new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: { labels, datasets: [{
            data: values, backgroundColor: bg, borderColor: border,
            borderWidth: 1, borderRadius: 4,
        }] },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: false,
            // RTL: same label-vs-bar gap fix as initProfitabilityChart.
            layout: { padding: { left: 12, right: 12 } },
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: { label: c => ' ₪' + Number(c.parsed.x).toLocaleString('he-IL', {maximumFractionDigits: 2}) },
                },
            },
            scales: {
                x: {
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        callback: v => '₪' + Number(v).toLocaleString('he-IL'),
                    },
                },
                y: {
                    grid: { display: false },
                    border: { color: PALETTE.border },
                    ticks: { color: PALETTE.tickText, font: { size: 12 }, padding: 14 },
                },
            },
        },
    });
}

function initExpenseBreakdownDonut(canvasId, payload) {
    const canvas = document.getElementById(canvasId);
    if (!canvas || !payload) return null;
    const labels = ['סחורה', 'שכר', 'חשמל', 'הוצאות קבועות אחרות'];
    const values = [payload.goods || 0, payload.salary || 0, payload.electricity || 0, payload.fixed_other || 0];
    const colors = ['#D85A30', '#7F77DD', '#E0A82E', '#888780'];
    return new Chart(canvas.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: colors.map(c => hexToRgba(c, 0.85)),
                borderColor: colors,
                borderWidth: 1,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: PALETTE.tickText, font: { size: 12 }, usePointStyle: true, boxWidth: 10 },
                },
                tooltip: {
                    rtl: true,
                    textDirection: 'rtl',
                    callbacks: {
                        label: c => {
                            const total = c.dataset.data.reduce((s, v) => s + v, 0);
                            const pct = total > 0 ? ((c.parsed / total) * 100).toFixed(1) : 0;
                            return ' ' + c.label + ': ' + salesShekel(c.parsed) + ' (' + pct + '%)';
                        },
                    },
                },
            },
        },
    });
}


function loadLiveSales() {
    if (window.IS_MULTI_BRANCH) {
        loadLiveSalesNetwork();
        return;
    }
    fetch('/api/live-sales')
        .then(r => r.json())
        .then(d => {
            const el = document.getElementById('live-amount');
            if (!el) return;
            const subEl = document.getElementById('live-sub');
            const basketEl = document.getElementById('live-basket');
            const updatedEl = document.getElementById('live-updated');
            if (d.is_closed) {
                el.textContent = 'החנות סגורה';
                el.className = 'kpi-value';
                if (subEl) subEl.textContent = 'המכירות יתעדכנו עם פתיחת החנות';
                if (basketEl) { basketEl.textContent = ''; basketEl.style.display = 'none'; }
                if (updatedEl) updatedEl.textContent = d.last_date ? 'עדכון אחרון: ' + d.last_date : '';
            } else if (d.amount !== null && d.amount !== undefined) {
                el.textContent = '₪ ' + d.amount.toLocaleString('he-IL', {minimumFractionDigits: 0});
                el.className = 'kpi-value profit';
                if (subEl) subEl.textContent = (d.transactions || 0) + ' עסקאות';
                if (basketEl) {
                    if (d.transactions && d.transactions > 0) {
                        basketEl.textContent = 'סל ממוצע: ₪' + (d.amount / d.transactions).toFixed(2);
                        basketEl.style.display = '';
                    } else {
                        basketEl.textContent = '';
                        basketEl.style.display = 'none';
                    }
                }
                if (updatedEl) {
                    const timeOnly = d.last_updated ? d.last_updated.split(' ')[0] : '';
                    updatedEl.textContent = timeOnly ? 'עודכן: ' + timeOnly : '';
                }
            } else {
                el.textContent = 'אין נתונים';
                el.className = 'kpi-value';
                if (subEl) subEl.textContent = '';
                if (basketEl) { basketEl.textContent = ''; basketEl.style.display = 'none'; }
                if (updatedEl) updatedEl.textContent = '';
            }
            // Also refresh summary tiles (income includes live data)
            if (typeof loadSummary === 'function') loadSummary();
        })
        .catch(() => {
            const el = document.getElementById('live-amount');
            if (el) el.textContent = 'שגיאה';
        });
}

setInterval(loadLiveSales, 300000);


function loadLiveSalesNetwork() {
    fetch('/api/live-sales/network')
        .then(r => r.json())
        .then(d => {
            const el = document.getElementById('live-amount');
            const subEl = document.getElementById('live-sub');
            const basketEl = document.getElementById('live-basket');
            const updatedEl = document.getElementById('live-updated');
            if (el) {
                if (d.chain_total > 0) {
                    el.textContent = '₪ ' + d.chain_total.toLocaleString('he-IL', {minimumFractionDigits: 0});
                    el.className = 'kpi-value profit';
                } else {
                    el.textContent = 'אין נתונים';
                    el.className = 'kpi-value';
                }
            }
            if (subEl) subEl.textContent = (d.active_count || 0) + ' סניפים פעילים מתוך ' + (d.total_count || 0);
            if (basketEl) { basketEl.textContent = ''; basketEl.style.display = 'none'; }
            if (updatedEl) updatedEl.textContent = '';

            const grid = document.getElementById('live-network-grid');
            if (!grid) return;
            grid.innerHTML = (d.branches || []).map(b => renderBranchLiveTile(b)).join('');

            if (typeof loadSummary === 'function') loadSummary();
        })
        .catch(() => {
            const el = document.getElementById('live-amount');
            if (el) el.textContent = 'שגיאה';
        });
}


function renderBranchLiveTile(b) {
    const name = (b.branch_name || '').replace(/</g, '&lt;');
    if (b.is_closed) {
        return (
            '<div style="background:rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:0.7rem; opacity:0.55;">' +
              '<div style="font-size:0.85rem; font-weight:700; color:#cbd5e1; margin-bottom:4px;">' + name + '</div>' +
              '<div style="display:inline-block; background:rgba(100,116,139,0.2); color:#94a3b8; font-size:0.68rem; font-weight:600; padding:2px 8px; border-radius:999px; margin-bottom:6px;">סגור</div>' +
              '<div style="font-size:0.78rem; color:#64748b;">החנות סגורה</div>' +
              (b.last_date ? '<div style="font-size:0.7rem; color:#475569; margin-top:4px;">עדכון אחרון: ' + b.last_date + '</div>' : '') +
            '</div>'
        );
    }
    if (b.amount === null || b.amount === undefined) {
        return (
            '<div style="background:rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:0.7rem;">' +
              '<div style="font-size:0.85rem; font-weight:700; color:#f1f5f9; margin-bottom:4px;">' + name + '</div>' +
              '<div style="font-size:0.78rem; color:#94a3b8;">אין נתונים</div>' +
            '</div>'
        );
    }
    const amt = '₪ ' + Number(b.amount).toLocaleString('he-IL', {minimumFractionDigits: 0});
    const txn = (b.transactions || 0) + ' עסקאות';
    const timeOnly = b.last_updated ? b.last_updated.split(' ')[0] : '';
    return (
        '<div style="background:rgba(34,197,94,0.06); border:1px solid rgba(34,197,94,0.25); border-radius:10px; padding:0.7rem;">' +
          '<div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">' +
            '<div style="font-size:0.85rem; font-weight:700; color:#f1f5f9;">' + name + '</div>' +
            '<span style="background:rgba(34,197,94,0.2); color:#86efac; font-size:0.65rem; font-weight:600; padding:2px 7px; border-radius:999px;">פתוח</span>' +
          '</div>' +
          '<div style="font-size:1.05rem; font-weight:700; color:#22c55e; direction:ltr; text-align:right;">' + amt + '</div>' +
          '<div style="font-size:0.74rem; color:#94a3b8; margin-top:2px;">' + txn + '</div>' +
          (timeOnly ? '<div style="font-size:0.7rem; color:#64748b; margin-top:2px;">עודכן: ' + timeOnly + '</div>' : '') +
        '</div>'
    );
}


function toggleLiveNetwork() {
    const sec = document.getElementById('live-network-section');
    const caret = document.getElementById('live-expand-caret');
    if (!sec) return;
    const open = sec.style.display !== 'none' && sec.style.display !== '';
    if (open) {
        sec.style.display = 'none';
        if (caret) caret.textContent = '▼';
    } else {
        sec.style.display = 'block';
        if (caret) caret.textContent = '▲';
        sec.scrollIntoView({behavior: 'smooth', block: 'nearest'});
    }
}
