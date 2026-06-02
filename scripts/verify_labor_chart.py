#!/usr/bin/env python3
"""Verification harness for the labor-ratio chart value-label clipping fix.

Pulls REAL labor-cost-ratio data from staging via the Flask test client (admin
session, ?branch_id= switching), then renders the chart with the OLD ("before")
and NEW ("after") Chart.js config side by side and screenshots a 2x2 grid:

    rows    = [low-% branch, normal-% branch]
    columns = [BEFORE (clipped), AFTER (fixed)]

Output: writes /tmp/labor_chart_verify.png and prints it base64 to stdout
(between BEGIN_PNG_B64 / END_PNG_B64) so it can be pulled back without scp.
"""
import os, sys, json, base64, tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app as appmod

flask_app = appmod.app
flask_app.config['TESTING'] = True


def fetch_all_ratios():
    """Return {branch_id: {'name':..., 'rows':[...]}} for active branches."""
    out = {}
    with flask_app.test_client() as c:
        with c.session_transaction() as sess:
            sess['user_id'] = 1
            sess['user_role'] = 'admin'
        # branch list
        with flask_app.app_context():
            db = appmod.get_db()
            branches = db.execute(
                'SELECT id, name FROM branches WHERE active = 1 ORDER BY id'
            ).fetchall()
            branches = [(r['id'], r['name']) for r in branches]
        for bid, name in branches:
            r = c.get(f'/api/labor-cost-ratio?branch_id={bid}')
            if r.status_code != 200:
                continue
            rows = r.get_json()
            rows = [x for x in rows if x.get('income', 0) > 0]
            if rows:
                out[bid] = {'name': name, 'rows': rows}
    return out


def max_ratio(rows):
    return max(x['ratio'] for x in rows)


def pick_branches(data):
    """Pick a low-% (3-6% ideal) and a normal-% (>=8%) branch."""
    items = [(bid, d, max_ratio(d['rows'])) for bid, d in data.items()]
    # low: prefer max ratio in [3,6], else smallest positive max
    low_pref = [it for it in items if 3 <= it[2] <= 6]
    low = (sorted(low_pref, key=lambda x: x[2])[0] if low_pref
           else sorted(items, key=lambda x: x[2])[0])
    # normal: highest max ratio that isn't the low branch
    rest = [it for it in items if it[0] != low[0]]
    normal = sorted(rest, key=lambda x: -x[2])[0] if rest else low
    return low, normal


CHART_JS = """
function labels(rows){return rows.map(function(d){var p=d.month.split('-');return parseInt(p[1])+'/'+p[0];});}

function benchmarkPlugin(){return {id:'benchmarkLine',beforeDraw:function(chart){
  var yScale=chart.scales.y;var yPos=yScale.getPixelForValue(10);
  if(yPos<yScale.top||yPos>yScale.bottom)return;var c=chart.ctx;c.save();
  c.setLineDash([6,4]);c.strokeStyle='rgba(239,68,68,0.5)';c.lineWidth=1.5;c.beginPath();
  c.moveTo(chart.chartArea.left,yPos);c.lineTo(chart.chartArea.right,yPos);c.stroke();
  c.fillStyle='rgba(239,68,68,0.7)';c.font='10px sans-serif';c.textAlign='left';
  c.fillText('\\u05e9\\u05d9\\u05e2\\u05d5\\u05e8 \\u05de\\u05d5\\u05de\\u05dc\\u05e5 10%',chart.chartArea.left+4,yPos-6);c.restore();}};}

function valueLabelsPlugin(rows){return {id:'valueLabels',afterDatasetsDraw:function(chart){
  var c=chart.ctx;var meta=chart.getDatasetMeta(0);
  meta.data.forEach(function(point,i){var val=rows[i].ratio;c.save();
  c.fillStyle='#f59e0b';c.font='bold 11px sans-serif';c.textAlign='center';c.textBaseline='bottom';
  c.fillText(val+'%',point.x,point.y-10);c.restore();});}};}

function baseDataset(rows){return {data:rows.map(function(d){return d.ratio;}),
  borderColor:'#f59e0b',backgroundColor:'rgba(245,158,11,0.12)',fill:true,tension:0.35,
  pointRadius:6,pointBackgroundColor:'#f59e0b',pointBorderColor:'#fff',pointBorderWidth:2,borderWidth:2.5};}

function yScale(extra){var y={title:{display:true,text:'% \\u05e9\\u05db\\u05e8/\\u05d4\\u05db\\u05e0\\u05e1\\u05d5\\u05ea',color:'#64748b',font:{size:11}},
  grid:{color:'rgba(148,163,184,0.08)'},ticks:{color:'#94a3b8',font:{size:11},callback:function(v){return v+'%';}},
  border:{color:'#334155'},beginAtZero:true};for(var k in extra){y[k]=extra[k];}return y;}

function renderChart(canvasId,rows,mode){
  var maxRatio=Math.max.apply(null,rows.map(function(d){return d.ratio;}));
  var options={responsive:false,maintainAspectRatio:false,
    plugins:{legend:{display:false},tooltip:{enabled:false}},
    scales:{x:{grid:{display:false},ticks:{color:'#94a3b8',font:{size:12}},border:{color:'#334155'}},
            y: mode==='after' ? yScale({suggestedMax:Math.max(12,maxRatio*1.25)}) : yScale({})}};
  if(mode==='after'){options.layout={padding:{top:24}};}
  new Chart(document.getElementById(canvasId),{type:'line',
    data:{labels:labels(rows),datasets:[baseDataset(rows)]},
    options:options,plugins:[benchmarkPlugin(),valueLabelsPlugin(rows)]});
}
"""


def build_html(low, normal):
    low_rows = json.dumps(low[1]['rows'])
    normal_rows = json.dumps(normal[1]['rows'])
    low_label = f"{low[1]['name']} (max {low[2]}%)"
    normal_label = f"{normal[1]['name']} (max {normal[2]}%)"
    return f"""<!doctype html><html dir="rtl"><head><meta charset="utf-8">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
  body{{background:#0d1526;color:#e2e8f0;font-family:sans-serif;margin:0;padding:20px;}}
  .grid{{display:grid;grid-template-columns:480px 480px;gap:18px;}}
  .cell{{background:#111c30;border:1px solid #1e293b;border-radius:12px;padding:12px;}}
  .cap{{font-size:13px;margin-bottom:8px;color:#94a3b8;}}
  .badge{{display:inline-block;font-size:11px;padding:2px 8px;border-radius:6px;margin-left:6px;}}
  .before{{background:#7f1d1d;color:#fecaca;}} .after{{background:#14532d;color:#bbf7d0;}}
  h2{{font-size:15px;margin:18px 0 8px;}}
</style></head><body>
<h2>{low_label}</h2>
<div class="grid">
  <div class="cell"><div class="cap"><span class="badge before">BEFORE</span> clipped</div>
    <canvas id="c_low_before" width="456" height="300"></canvas></div>
  <div class="cell"><div class="cap"><span class="badge after">AFTER</span> fixed</div>
    <canvas id="c_low_after" width="456" height="300"></canvas></div>
</div>
<h2>{normal_label}</h2>
<div class="grid">
  <div class="cell"><div class="cap"><span class="badge before">BEFORE</span> clipped</div>
    <canvas id="c_norm_before" width="456" height="300"></canvas></div>
  <div class="cell"><div class="cap"><span class="badge after">AFTER</span> fixed</div>
    <canvas id="c_norm_after" width="456" height="300"></canvas></div>
</div>
<script>
{CHART_JS}
var lowRows={low_rows};var normRows={normal_rows};
renderChart('c_low_before',lowRows,'before');
renderChart('c_low_after',lowRows,'after');
renderChart('c_norm_before',normRows,'before');
renderChart('c_norm_after',normRows,'after');
window.__done=true;
</script></body></html>"""


def main():
    data = fetch_all_ratios()
    if not data:
        print('NO_DATA', file=sys.stderr)
        sys.exit(1)
    low, normal = pick_branches(data)
    print(f"LOW   : id={low[0]} {low[1]['name']!r} maxRatio={low[2]} rows={low[1]['rows']}")
    print(f"NORMAL: id={normal[0]} {normal[1]['name']!r} maxRatio={normal[2]} rows={normal[1]['rows']}")

    html = build_html(low, normal)
    with tempfile.NamedTemporaryFile('w', suffix='.html', delete=False) as f:
        f.write(html)
        html_path = f.name

    from playwright.sync_api import sync_playwright
    out_png = '/tmp/labor_chart_verify.png'
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={'width': 1040, 'height': 820}, device_scale_factor=2)
        pg.goto('file://' + html_path)
        pg.wait_for_function('window.__done === true', timeout=15000)
        pg.wait_for_timeout(800)
        pg.screenshot(path=out_png, full_page=True)
        b.close()

    with open(out_png, 'rb') as fh:
        enc = base64.b64encode(fh.read()).decode()
    print('BEGIN_PNG_B64')
    print(enc)
    print('END_PNG_B64')


if __name__ == '__main__':
    main()
