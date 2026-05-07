package com.kafka.active.web;

import com.kafka.active.metrics.MirrorCompareService;
import java.util.concurrent.ExecutionException;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@Profile("!test")
public class MirrorCompareController {

	private static final int LIMIT_DEFAULT = 30;
	private static final int LIMIT_MAX = 200;

	private final MirrorCompareService mirrorCompareService;

	public MirrorCompareController(MirrorCompareService mirrorCompareService) {
		this.mirrorCompareService = mirrorCompareService;
	}

	@GetMapping(value = "/api/mirror/compare", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public ResponseEntity<?> compareApi(@RequestParam(defaultValue = "30") int limit) {
		int lim = Math.min(LIMIT_MAX, Math.max(1, limit == 0 ? LIMIT_DEFAULT : limit));
		try {
			return ResponseEntity.ok(mirrorCompareService.compare(lim));
		} catch (InterruptedException e) {
			// restore interrupt status
			Thread.currentThread().interrupt();
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("interrupted");
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			String msg = cause != null ? cause.getMessage() : e.getMessage();
			return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
					.body(msg);
		}
	}

	@GetMapping(value = "/mirror/compare", produces = MediaType.TEXT_HTML_VALUE)
	@ResponseBody
	public String comparePage() {
		return HTML;
	}

	private static final String HTML =
			"""
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Kafka 미러 소스 vs 미러 메시지</title>
<style>
  :root { --bg:#0f1419; --panel:#1a2332; --text:#e7ecf3; --muted:#8b9bb4; --accent:#3d9cf0; --ok:#3ecf8e; --warn:#e0a84a; --bad:#f07178; --row:#121a25; }
  * { box-sizing: border-box; }
  body { font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 1rem 1.25rem; }
  h1 { font-size: 1.15rem; font-weight: 650; margin: 0 0 .35rem; letter-spacing: .1px; }
  .sub { color: var(--muted); font-size: .85rem; margin: 0 0 .85rem; line-height: 1.35; }
  .lag { display: flex; flex-wrap: wrap; gap: 1rem; margin-bottom: 1rem; }
  .lag span { background: var(--panel); padding: .45rem .7rem; border-radius: 6px; font-size: .85rem; }
  .lag b { color: var(--accent); }
  .toolbar { display:flex; flex-wrap: wrap; gap:.6rem; align-items:center; margin: .75rem 0 1rem; }
  .toolbar .chip { background: var(--panel); border: 1px solid #2a3545; padding: .45rem .6rem; border-radius: 999px; font-size: .82rem; color: var(--muted); }
  .toolbar input { width: min(520px, 100%); background: var(--panel); border: 1px solid #2a3545; color: var(--text); padding: .5rem .65rem; border-radius: 8px; font-size: .88rem; outline: none; }
  .toolbar input:focus { border-color: #3d9cf0aa; box-shadow: 0 0 0 3px #3d9cf022; }
  .toolbar button { background: #2a3545; border: 1px solid #2a3545; color: var(--text); padding: .48rem .65rem; border-radius: 8px; font-size: .88rem; cursor:pointer; }
  .toolbar button:hover { background: #334156; }
  .toolbar .right { margin-left: auto; display:flex; gap:.6rem; flex-wrap: wrap; align-items:center; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  .col { background: var(--panel); border-radius: 8px; padding: .75rem; min-height: 120px; }
  .col h2 { font-size: .95rem; margin: 0 0 .6rem; color: var(--ok); }
  .col.mirror h2 { color: var(--warn); }
  table { width: 100%; border-collapse: collapse; font-size: .78rem; }
  th, td { text-align: left; padding: .35rem .3rem; border-bottom: 1px solid #2a3545; vertical-align: top; word-break: break-all; }
  th { color: var(--muted); font-weight: 500; }
  tr:hover td { background: #0e1520; }
  .match td { background: #15364a; }
  .match:hover td { background: #17445d; }
  .empty { color: var(--muted); font-size: .85rem; padding: .25rem 0; }
  .err { color: var(--bad); margin-top: .75rem; }
  .mono { font-family: ui-monospace, monospace; }
</style>
</head>
<body>
<h1>MM2 소스 ↔ 미러 메시지 비교</h1>
<p class="sub">
  자동 갱신 <span class="mono">8s</span> · API: <span class="mono">/api/mirror/compare?limit=30</span>
  <br/>팁: 동일한 value(앞 120자 기준)가 소스/미러에 같이 보이면 행이 하이라이트 됩니다.
</p>
<div id="lag" class="lag"></div>
<div class="toolbar">
  <input id="q" placeholder="검색(부분 문자열): value에 포함되면 표시" />
  <button id="btn-refresh" type="button">새로고침</button>
  <div class="right">
    <span id="stats" class="chip">-</span>
    <span id="updated" class="chip">updated: -</span>
  </div>
</div>
<div class="grid">
  <div class="col"><h2 id="h-src">소스</h2><div id="tbl-src"></div></div>
  <div class="col mirror"><h2 id="h-mir">미러</h2><div id="tbl-mir"></div></div>
</div>
<div id="err" class="err"></div>
<script>
const LIMIT = 60;
function esc(s) {
  if (s == null) return '';
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}
function keyOf(r) {
  const v = (r && r.value) ? r.value : '';
  return v.length > 120 ? v.slice(0, 120) : v;
}
function toSet(rows) {
  const s = new Set();
  for (const r of rows) s.add(keyOf(r));
  return s;
}
function filterRows(rows, q) {
  if (!q) return rows;
  const qq = q.toLowerCase();
  return rows.filter(r => (r.value || '').toLowerCase().includes(qq));
}
function table(rows, otherSet) {
  if (!rows.length) return '<div class="empty">레코드 없음</div>';
  let h = '<table><thead><tr><th>p</th><th>offset</th><th>ts</th><th>value</th></tr></thead><tbody>';
  for (const r of rows) {
    const ts = new Date(r.timestampMs).toISOString();
    const matched = otherSet && otherSet.has(keyOf(r));
    h += '<tr class="' + (matched ? 'match' : '') + '"><td class="mono">' + r.partition + '</td><td class="mono">' + r.offset + '</td><td class="mono">' + ts + '</td><td class="mono">' + esc(r.value) + '</td></tr>';
  }
  return h + '</tbody></table>';
}
async function load() {
  const err = document.getElementById('err');
  err.textContent = '';
  try {
    const res = await fetch('/api/mirror/compare?limit=' + LIMIT);
    if (!res.ok) throw new Error(await res.text());
    const d = await res.json();
    const L = d.lag;
    const q = (document.getElementById('q').value || '').trim();
    const srcAll = d.sourceMessages || [];
    const mirAll = d.mirrorMessages || [];
    const src = filterRows(srcAll, q);
    const mir = filterRows(mirAll, q);
    const srcSet = toSet(src);
    const mirSet = toSet(mir);
    document.getElementById('lag').innerHTML =
      '<span>lag(근사) <b>' + L.lagMessages + '</b> msg</span>' +
      '<span>소스 HWM 합 <b>' + L.sourceHighWatermark + '</b></span>' +
      '<span>미러 HWM 합 <b>' + L.mirrorHighWatermark + '</b></span>' +
      '<span class="mono">' + L.sourceCluster + ':' + L.sourceTopic + '</span>' +
      '<span class="mono">' + L.mirrorCluster + ':' + L.mirrorTopic + '</span>';
    document.getElementById('h-src').textContent = '소스 ' + L.sourceCluster + ' · ' + L.sourceTopic;
    document.getElementById('h-mir').textContent = '미러 ' + L.mirrorCluster + ' · ' + L.mirrorTopic;
    document.getElementById('tbl-src').innerHTML = table(src, mirSet);
    document.getElementById('tbl-mir').innerHTML = table(mir, srcSet);
    const matchCount = (() => {
      let n = 0;
      for (const k of srcSet) if (mirSet.has(k)) n++;
      return n;
    })();
    document.getElementById('stats').textContent =
      'src ' + src.length + '/' + srcAll.length + ' · mir ' + mir.length + '/' + mirAll.length + ' · matched ' + matchCount;
    document.getElementById('updated').textContent = 'updated: ' + new Date().toLocaleTimeString();
  } catch (e) {
    err.textContent = '로드 실패: ' + e;
  }
}
load();
setInterval(load, 8000);
document.getElementById('btn-refresh').addEventListener('click', load);
document.getElementById('q').addEventListener('input', () => { window.clearTimeout(window.__t); window.__t = window.setTimeout(load, 250); });
</script>
</body>
</html>
""";
}
