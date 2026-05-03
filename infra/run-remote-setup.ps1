#Requires -Version 5.1
<#
  - NCP Compute(Rocky 등): PEM(-i) + 예시 root. Kafka 설치/토픽 생성.
  - 로컬 Ubuntu(예: 192.168.160.147): PEM 아님. ubuntu/ubuntu 비밀번호로 대화형 접속.
    ClickHouse 블록은 -i 없이 scp/ssh 하므로 비밀번호 입력이 뜬다.

  예 (NCP Kafka만):
    .\infra\run-remote-setup.ps1 -PemPath ".\ncp-kafka.pem" -KafkaHosts @("211.188.50.31","223.130.154.172") -KafkaUser root -KafkaNodes @(1,2)

  예 (로컬 Ubuntu ClickHouse만 — PEM 불필요, scp/ssh 시 비밀번호 입력):
    .\infra\run-remote-setup.ps1 -KafkaHosts @() -ClickHouseHost "192.168.160.147"
#>
param(
  [string]$PemPath = "",
  [string[]]$KafkaHosts = @("211.188.50.31", "223.130.154.172"),
  [string]$KafkaUser = "root",
  [int[]]$KafkaNodes = @(1, 2),
  [string]$ClickHouseHost = "",
  [string]$ClickHouseUser = "ubuntu"
)

$ErrorActionPreference = "Stop"
# PSScriptRoot = ...\kafka-aa\infra
$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path

if ($KafkaHosts.Count -gt 0) {
  if (-not $PemPath) { throw "NCP Kafka 설치 시 -PemPath 가 필요합니다." }
  $pem = Resolve-Path $PemPath
  icacls $pem /inheritance:r | Out-Null
  icacls $pem /grant:r "$env:USERNAME`:R" | Out-Null
}

$kafkaSrc = Join-Path $repoRoot "infra\kafka"
if (-not (Test-Path $kafkaSrc)) { throw "not found: $kafkaSrc" }

for ($i = 0; $i -lt $KafkaHosts.Count; $i++) {
  $h = $KafkaHosts[$i]
  $node = $KafkaNodes[$i]
  Write-Host "=== Kafka NODE $node -> $KafkaUser@$h ===" -ForegroundColor Cyan
  $remoteDir = "/tmp/kafka-infra-$node"
  scp -i $pem -o StrictHostKeyChecking=accept-new -r "$kafkaSrc" "${KafkaUser}@${h}:$remoteDir"
  ssh -i $pem -o StrictHostKeyChecking=accept-new "${KafkaUser}@${h}" "sudo bash $remoteDir/install-kafka4-kraft-rocky9.sh $node"
  ssh -i $pem -o StrictHostKeyChecking=accept-new "${KafkaUser}@${h}" "sudo /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic aa-demo-events --partitions 3 --replication-factor 1" 2>$null
  Write-Host "done $h" -ForegroundColor Green
}

if ($ClickHouseHost) {
  Write-Host "=== ClickHouse (비밀번호 인증, PEM 미사용) -> $ClickHouseUser@$ClickHouseHost ===" -ForegroundColor Cyan
  $chSrc = Join-Path $repoRoot "infra\clickhouse"
  if (-not (Test-Path $chSrc)) { throw "not found: $chSrc" }
  scp -o StrictHostKeyChecking=accept-new -r "$chSrc" "${ClickHouseUser}@${ClickHouseHost}:/tmp/clickhouse-infra"
  $remote = "chmod +x /tmp/clickhouse-infra/install-clickhouse-ubuntu.sh /tmp/clickhouse-infra/bootstrap-after-docker.sh 2>/dev/null; sudo bash /tmp/clickhouse-infra/install-clickhouse-ubuntu.sh"
  ssh -o StrictHostKeyChecking=accept-new "${ClickHouseUser}@${ClickHouseHost}" $remote
  Write-Host "ClickHouse done (native APT). Docker 쓰면 서버에서: cd /tmp/clickhouse-infra && sudo docker compose up -d && sudo bash bootstrap-after-docker.sh" -ForegroundColor Yellow
}

Write-Host "All finished." -ForegroundColor Green
