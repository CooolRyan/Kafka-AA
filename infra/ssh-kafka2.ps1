#Requires -Version 5.1
<#
  kafka-active002 전용 OpenSSH 개인키 파일로 접속 테스트.
  키는 PNG/캡처에서 OCR로 복사하면 깨지므로, 서버에 이미 있는 파일을 그대로 내려받거나
  에디터에 붙여넣어 저장소 루트에 `kafka2-ssh` 로 저장한 뒤 실행한다.

  예:
    # 저장소 kafka-aa 루트에 kafka2-ssh 두고
    .\infra\ssh-kafka2.ps1
    .\infra\ssh-kafka2.ps1 -SshHost 223.130.154.172 -KeyPath ".\kafka2-ssh" -Command "hostname"
#>
param(
  [string]$SshHost = "223.130.154.172",
  [string]$User = "root",
  [string]$KeyPath = "",
  [string]$Command = "hostname && uname -a"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
if (-not $KeyPath) {
  $KeyPath = Join-Path $repoRoot "kafka2-ssh"
}
if (-not (Test-Path $KeyPath)) {
  Write-Host "키 파일 없음: $KeyPath" -ForegroundColor Red
  Write-Host "서버(kafka-active002)에서: cat /root/.ssh/kafka2-ssh 출력을 위 경로에 그대로 저장하거나,"
  Write-Host "다른 PC에서 NCP PEM으로 접속한 뒤: scp root@${SshHost}:/root/.ssh/kafka2-ssh ."
  exit 2
}

$key = (Resolve-Path $KeyPath).Path
icacls $key /inheritance:r | Out-Null
icacls $key /grant:r "$env:USERNAME`:R" | Out-Null

ssh -i $key -o StrictHostKeyChecking=accept-new "${User}@${SshHost}" $Command
