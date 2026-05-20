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
                    reverse: true,
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
        data: { labels: payload.months || [], datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    position: 'bottom',
                    rtl: true,
                    labels: { color: PALETTE.tickText, font: { size: 12 }, usePointStyle: true, boxWidth: 8 },
                },
                tooltip: {
                    rtl: true,
                    callbacks: { label: c => ' ' + c.dataset.label + ': ' + salesShekel(c.parsed.y) },
                },
            },
            scales: {
                x: {
                    reverse: true,
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
            plugins: {
                legend: {
                    position: 'bottom',
                    rtl: true,
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
                    reverse: true,
                    position: 'top',
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: { color: PALETTE.tickText, font: { size: 11 }, callback: salesAxisK },
                },
                y: {
                    stacked: true,
                    position: 'right',
                    grid: { display: false },
                    border: { color: PALETTE.border },
                    ticks: { color: PALETTE.tickText, font: { size: 12 } },
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
            plugins: {
                legend: { display: false },
                tooltip: {
                    rtl: true,
                    callbacks: { label: c => ' ₪' + Number(c.parsed.x).toLocaleString('he-IL', {maximumFractionDigits: 2}) },
                },
            },
            scales: {
                x: {
                    reverse: true,
                    position: 'top',
                    beginAtZero: true,
                    grid: { color: PALETTE.gridLine },
                    border: { color: PALETTE.border },
                    ticks: {
                        color: PALETTE.tickText, font: { size: 11 },
                        callback: v => '₪' + Number(v).toLocaleString('he-IL'),
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
                    rtl: true,
                    textDirection: 'rtl',
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
    fetch('/api/live-sales')
        .then(r => r.json())
        .then(d => {
            const el = document.getElementById('live-amount');
            if (!el) return;
            if (d.amount !== null && d.amount !== undefined) {
                el.textContent = '₪ ' + d.amount.toLocaleString('he-IL', {minimumFractionDigits: 0});
                // Stale → no green "profit" highlight (dimmed default text).
                el.className = d.is_stale ? 'kpi-value' : 'kpi-value profit';
                document.getElementById('live-sub').textContent = (d.transactions || 0) + ' עסקאות';
                const basketEl = document.getElementById('live-basket');
                if (basketEl) {
                    if (d.transactions && d.transactions > 0) {
                        basketEl.textContent = 'סל ממוצע: ₪' + (d.amount / d.transactions).toFixed(2);
                        basketEl.style.display = '';
                    } else {
                        basketEl.textContent = '';
                        basketEl.style.display = 'none';
                    }
                }
                const updatedEl = document.getElementById('live-updated');
                if (d.is_stale) {
                    updatedEl.textContent = '';
                    const pill = document.createElement('span');
                    pill.className = 'kpi-badge stale';
                    pill.textContent = d.stale_date
                        ? ('מתעדכן בבוקר · עדכון אחרון: ' + d.stale_date)
                        : 'מתעדכן בבוקר';
                    updatedEl.appendChild(pill);
                } else {
                    const timeOnly = d.last_updated ? d.last_updated.split(' ')[0] : '';
                    updatedEl.textContent = timeOnly ? 'עודכן: ' + timeOnly : '';
                }
            } else {
                el.textContent = 'אין נתונים';
                el.className = 'kpi-value';
                document.getElementById('live-sub').textContent = '';
                const basketElNone = document.getElementById('live-basket');
                if (basketElNone) { basketElNone.textContent = ''; basketElNone.style.display = 'none'; }
                document.getElementById('live-updated').textContent = '';
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
