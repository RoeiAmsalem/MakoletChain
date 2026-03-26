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

function loadLiveSales() {
    fetch('/api/live-sales')
        .then(r => r.json())
        .then(d => {
            const el = document.getElementById('live-amount');
            if (!el) return;
            if (d.amount !== null && d.amount !== undefined) {
                el.textContent = '₪ ' + d.amount.toLocaleString('he-IL', {minimumFractionDigits: 0});
                el.className = 'kpi-value profit';
                document.getElementById('live-sub').textContent = (d.transactions || 0) + ' עסקאות';
                const timeOnly = d.last_updated ? d.last_updated.split(' ')[0] : '';
                document.getElementById('live-updated').textContent = timeOnly ? 'עודכן: ' + timeOnly : '';
            } else {
                el.textContent = 'אין נתונים';
                el.className = 'kpi-value';
                document.getElementById('live-sub').textContent = '';
                document.getElementById('live-updated').textContent = '';
            }
        })
        .catch(() => {
            const el = document.getElementById('live-amount');
            if (el) el.textContent = 'שגיאה';
        });
}

setInterval(loadLiveSales, 300000);
