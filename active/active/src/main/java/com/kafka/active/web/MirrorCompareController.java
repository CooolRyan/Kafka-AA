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
  :root { --bg:#0b0f14; --panel:#141c28; --text:#e8eef7; --muted:#8b9bb4; --accent:#4db0ff; --ok:#45d483; --warn:#f0b429; --bad:#ff6b7a; --border:#243044; }
  * { box-sizing: border-box; }
  body { font-family: ui-sans-serif, system-ui, sans-serif; background: var(--bg); color: var(--text); margin: 0; padding: 1rem 1.1rem 2rem; }
  h1 { font-size: 1.2rem; font-weight: 700; margin: 0 0 .4rem; }
  .sub { color: var(--muted); font-size: .84rem; margin: 0 0 1rem; line-height: 1.45; max-width: 900px; }
  .lag { display: flex; flex-wrap: wrap; gap: .65rem; margin-bottom: .85rem; }
  .lag span { background: var(--panel); padding: .4rem .65rem; border-radius: 8px; font-size: .82rem; border: 1px solid var(--border); }
  .lag b { color: var(--accent); }
  .toolbar { display:flex; flex-wrap: wrap; gap:.55rem; align-items:center; margin: 0 0 1rem; }
  .toolbar .chip { background: var(--panel); border: 1px solid var(--border); padding: .4rem .55rem; border-radius: 999px; font-size: .78rem; color: var(--muted); }
  .toolbar input { width: min(480px, 100%); background: var(--panel); border: 1px solid var(--border); color: var(--text); padding: .5rem .6rem; border-radius: 8px; font-size: .86rem; outline: none; }
  .toolbar input:focus { border-color: var(--accent); box-shadow: 0 0 0 2px #4db0ff33; }
  .toolbar button { background: #243044; border: 1px solid var(--border); color: var(--text); padding: .45rem .7rem; border-radius: 8px; font-size: .86rem; cursor:pointer; }
  .toolbar button:hover { background: #2d3d56; }
  .toolbar .right { margin-left: auto; display:flex; gap:.5rem; flex-wrap: wrap; align-items:center; }
  .mono { font-family: ui-monospace, SFMono-Regular, monospace; font-size: .78rem; word-break: break-all; }
  .pairs { display: flex; flex-direction: column; gap: .85rem; }
  .pair-card { background: var(--panel); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  .pair-head { display: flex; flex-wrap: wrap; align-items: center; gap: .75rem 1rem; padding: .55rem .75rem; background: #0e1520; border-bottom: 1px solid var(--border); }
  .pair-head .id { flex: 1 1 200px; color: var(--accent); font-weight: 600; }
  .pair-head .sent { color: var(--muted); }
  .pair-head .sent b { color: var(--text); }
  .delta-pill { margin-left: auto; padding: .35rem .75rem; border-radius: 8px; font-weight: 700; font-size: .95rem; border: 1px solid var(--border); }
  .delta-pill.ok { background: #153d28; color: var(--ok); border-color: #1f5c3a; }
  .delta-pill.mid { background: #3d3515; color: var(--warn); border-color: #5c4a1f; }
  .delta-pill.slow { background: #3d181c; color: var(--bad); border-color: #5c2528; }
  .delta-pill.na { background: #1a2230; color: var(--muted); }
  .pair-split { display: grid; grid-template-columns: 1fr 1fr; min-height: 0; }
  @media (max-width: 960px) { .pair-split { grid-template-columns: 1fr; } }
  .side { padding: .65rem .75rem; border-right: 1px solid var(--border); min-width: 0; }
  .side.mir { border-right: none; }
  @media (max-width: 960px) { .side { border-right: none; border-bottom: 1px solid var(--border); } .side.mir { border-bottom: none; } }
  .side h3 { margin: 0 0 .45rem; font-size: .82rem; font-weight: 650; }
  .side.src h3 { color: var(--ok); }
  .side.mir h3 { color: var(--warn); }
  .kv { display: grid; grid-template-columns: auto 1fr; gap: .2rem .6rem; font-size: .76rem; margin-bottom: .35rem; color: var(--muted); }
  .kv b, .kv .v { color: var(--text); font-weight: 500; }
  .payload { margin-top: .45rem; padding: .45rem .5rem; background: #0a1018; border-radius: 6px; border: 1px solid #1c2736; max-height: 140px; overflow: auto; white-space: pre-wrap; }
  .empty-side { color: var(--bad); font-size: .85rem; padding: 1rem .25rem; }
  .empty { color: var(--muted); font-size: .86rem; padding: .75rem; }
  details.raw { margin-top: 1.25rem; }
  details.raw summary { cursor: pointer; color: var(--muted); font-size: .84rem; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: .75rem; margin-top: .5rem; }
  @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  .col { background: var(--panel); border-radius: 8px; padding: .65rem; border: 1px solid var(--border); }
  .col h2 { font-size: .88rem; margin: 0 0 .5rem; color: var(--ok); }
  .col.mirror h2 { color: var(--warn); }
  table { width: 100%; border-collapse: collapse; font-size: .74rem; }
  th, td { text-align: left; padding: .3rem .25rem; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; }
  tr:hover td { background: #0e1520; }
  .match td { background: #15364a; }
  .err { color: var(--bad); margin-top: .75rem; }
</style>
</head>
<body>
<h1>MM2 동일 메시지 반반 비교</h1>
<p class="sub">
  JSON 페이로드의 <span class="mono">id</span>로 소스/미러 tail을 매칭합니다.
  <span class="mono">Δ Kafka LogAppendTime</span> = 미러 레코드 타임스탬프 − 소스 레코드 타임스탬프 (브로커 LogAppendTime 기준 ms).
  자동 갱신 <span class="mono">5s</span> · <span class="mono">POST /api/produce</span> 로 트래픽을 넣으면 페어가 쌓입니다.
</p>
<div id="lag" class="lag"></div>
<div class="toolbar">
  <input id="q" placeholder="id / value 부분 검색 (페어·원시 테이블 동시 필터)" />
  <button id="btn-refresh" type="button">새로고침</button>
  <div class="right">
    <span id="stats" class="chip">-</span>
    <span id="updated" class="chip">updated: -</span>
  </div>
</div>
<div id="pairs"></div>
<details class="raw"><summary>원시 tail 테이블 (고급)</summary>
<div class="grid">
  <div class="col"><h2 id="h-src">소스</h2><div id="tbl-src"></div></div>
  <div class="col mirror"><h2 id="h-mir">미러</h2><div id="tbl-mir"></div></div>
</div>
</details>
<div id="err" class="err"></div>
<script>
const LIMIT = 80;
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
function filterPairs(rows, q) {
  if (!q) return rows || [];
  const qq = q.toLowerCase();
  return (rows || []).filter(pr => {
    const id = (pr.messageId || '').toLowerCase();
    const sv = (pr.source && pr.source.value) ? pr.source.value : '';
    const mv = (pr.mirror && pr.mirror.value) ? pr.mirror.value : '';
    return id.includes(qq) || sv.toLowerCase().includes(qq) || mv.toLowerCase().includes(qq);
  });
}
function fmtMs(ms) {
  if (ms == null || ms === '') return '—';
  const n = Number(ms);
  return (n >= 0 ? '+' : '') + n + ' ms';
}
function deltaClass(d) {
  if (d == null) return 'na';
  if (d <= 80) return 'ok';
  if (d <= 800) return 'mid';
  return 'slow';
}
function sideBlock(r, label, sideCls) {
  if (!r) return '<div class="side ' + sideCls + '"><h3>' + label + '</h3><div class="empty-side">이 클러스터에 해당 id 없음</div></div>';
  const iso = new Date(r.timestampMs).toISOString();
  return '<div class="side ' + sideCls + '"><h3>' + label + '</h3>' +
    '<div class="kv"><span>p</span><span class="v mono">' + r.partition + '</span>' +
    '<span>offset</span><span class="v mono">' + r.offset + '</span>' +
    '<span>Kafka ts</span><span class="v mono">' + r.timestampMs + ' ms</span>' +
    '<span>ISO</span><span class="v mono">' + iso + '</span></div>' +
    '<div class="payload mono">' + esc(r.value) + '</div></div>';
}
function renderPairs(rows) {
  if (!rows.length) return '<div class="empty">매칭된 페어 없음 · /api/produce 로 JSON(id,sentAtMs) 메시지를 넣고 MM2가 미러까지 복제될 때까지 잠시 기다리세요.</div>';
  let h = '<div class="pairs">';
  for (const pr of rows) {
    const d = pr.deltaKafkaTimestampMs;
    const pill = '<span class="delta-pill ' + deltaClass(d) + '">Δ Kafka ' + fmtMs(d) + '</span>';
    h += '<div class="pair-card"><div class="pair-head">' +
      '<span class="mono id">' + esc(pr.messageId) + '</span>' +
      '<span class="sent">sentAtMs <b class="mono">' + pr.sentAtMs + '</b></span>' + pill + '</div>' +
      '<div class="pair-split">' +
      sideBlock(pr.source, '소스 클러스터', 'src') +
      sideBlock(pr.mirror, '미러 클러스터', 'mir') +
      '</div></div>';
  }
  return h + '</div>';
}
function table(rows, otherSet) {
  if (!rows.length) return '<div class="empty">레코드 없음</div>';
  let h = '<table><thead><tr><th>p</th><th>offset</th><th>Kafka ts (ms)</th><th>value</th></tr></thead><tbody>';
  for (const r of rows) {
    const matched = otherSet && otherSet.has(keyOf(r));
    h += '<tr class="' + (matched ? 'match' : '') + '"><td class="mono">' + r.partition + '</td><td class="mono">' + r.offset + '</td><td class="mono">' + r.timestampMs + '</td><td class="mono">' + esc(r.value) + '</td></tr>';
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
    const pairs = filterPairs(d.pairedRows || [], q);
    document.getElementById('lag').innerHTML =
      '<span>lag(근사) <b>' + L.lagMessages + '</b> msg</span>' +
      '<span>소스 HWM 합 <b>' + L.sourceHighWatermark + '</b></span>' +
      '<span>미러 HWM 합 <b>' + L.mirrorHighWatermark + '</b></span>' +
      '<span class="mono">' + L.sourceCluster + ':' + L.sourceTopic + '</span>' +
      '<span class="mono">' + L.mirrorCluster + ':' + L.mirrorTopic + '</span>';
    document.getElementById('h-src').textContent = '소스 ' + L.sourceCluster + ' · ' + L.sourceTopic;
    document.getElementById('h-mir').textContent = '미러 ' + L.mirrorCluster + ' · ' + L.mirrorTopic;
    document.getElementById('pairs').innerHTML = renderPairs(pairs);
    document.getElementById('tbl-src').innerHTML = table(src, mirSet);
    document.getElementById('tbl-mir').innerHTML = table(mir, srcSet);
    const both = pairs.filter(p => p.source && p.mirror).length;
    document.getElementById('stats').textContent =
      'pairs ' + pairs.length + ' (양쪽 ' + both + ') · tail src ' + src.length + ' / mir ' + mir.length;
    document.getElementById('updated').textContent = 'updated: ' + new Date().toLocaleTimeString();
  } catch (e) {
    err.textContent = '로드 실패: ' + e;
  }
}
load();
setInterval(load, 5000);
document.getElementById('btn-refresh').addEventListener('click', load);
document.getElementById('q').addEventListener('input', () => { window.clearTimeout(window.__t); window.__t = window.setTimeout(load, 200); });
</script>
</body>
</html>
""";
}
