# Windows에서 PEM 키로 NCP Linux에 SSH 붙기 (OpenSSH) — 권한 오류부터 정리까지

클라우드에 올라간 **NCP(Naver Cloud Platform) Compute** 같은 Linux VM에는 보통 배포 시 받은 **`.pem` 개인키**로 접속한다. 반면 집·사내에 깔아 둔 **로컬 Ubuntu 서버**(예: `192.168.160.147`)는 키가 아니라 **계정/비밀번호(`ubuntu` / `ubuntu`)** 로 접속하는 경우가 많다. 이 글에서는 **Windows + OpenSSH 클라이언트** 기준으로, PEM 권한 때문에 막히는 경우를 포함해 NCP 쪽 접속 과정을 블로그용으로 정리한다.

---

## 1. 어떤 서버에 무엇을 쓰는지 먼저 구분하기

| 구분 | 예시 | 인증 방식 |
|------|------|-----------|
| **NCP Compute** (Rocky 등 클라우드 VM) | 공인 IP + 콘솔에서 받은 PEM | **SSH 개인키(PEM)** + (보통) `root` 또는 콘솔에 안내된 사용자 |
| **로컬/사설망 Ubuntu** | `192.168.160.147` | **ID/비밀번호** (`ubuntu` / `ubuntu` 등). **PEM은 사용하지 않는다.** |

PEM은 **NCP에 등록해 둔 공개키와 짝인 그 VM**에만 쓴다. 로컬 192 대역 서버용 키가 아니라면, 당연히 그 서버에는 PEM으로 붙지 않는다. 로컬은 비밀번호나, 따로 등록한 **별도의 공개키**를 쓰면 된다.

---

## 2. Windows OpenSSH가 PEM을 “거부”하는 이유

터미널에서 다음과 비슷한 메시지가 나온다면:

```text
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@         WARNING: UNPROTECTED PRIVATE KEY FILE!          @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Permissions for '...\\ncp-kafka.pem' are too open.
It is required that your private key files are NOT accessible by others.
This private key will be ignored.
```

OpenSSH는 **개인키 파일이 “나 말고 다른 주체(Users, Everyone, Administrators 등)에게도 읽히면”** 보안상 로드하지 않는다. Windows에서는 탐색기로 복사한 파일이 **상속 ACL** 때문에 `Users` 그룹에 읽기 권한이 남아 있는 경우가 많고, 그게 곧 “too open”이다.

---

## 3. 해결: `icacls`로 상속 제거 + 본인만 읽기

**관리자 권한이 꼭 필요한 것은 아니다.** 일반 PowerShell에서 PEM이 있는 경로로 가서 실행한다.

```powershell
$pem = "C:\path\to\ncp-kafka.pem"

# 상속 제거 (다른 보안 주체에서 퍼진 권한 정리)
icacls $pem /inheritance:r

# 현재 Windows 로그온 사용자에게만 읽기(R) 부여
icacls $pem /grant:r "$env:USERNAME`:R"
```

확인:

```powershell
icacls $pem
```

출력에 **`DESKTOP-…\yourname:(R)`** 처럼 **본인만** 나오면 된다. `Users`, `Everyone`이 보이면 아직 넓다.

> **OneDrive·한글 경로**: 경로에 한글이 있어도 OpenSSH는 동작하는 경우가 많다. 다만 이슈가 있으면 PEM을 `C:\keys\ncp.pem` 같이 **짧은 ASCII 경로**로 복사해 같은 `icacls`를 적용해 보면 된다.

---

## 4. NCP Linux에 SSH 접속하기

PEM 권한을 맞춘 뒤:

```powershell
ssh -i "C:\path\to\ncp-kafka.pem" -o StrictHostKeyChecking=accept-new root@<NCP_공인IP>
```

- **사용자**: 콘솔/가이드에 나온 계정(`root`가 아닐 수 있음).
- **최초 접속**: 호스트 키 질문이 나오면 `accept-new` 옵션으로 한 번에 넘길 수 있다(신뢰할 서버일 때만).

여기까지 되면 **“PEM 권한 + NCP 키 페어”** 구간은 통과한 것이다.

그 다음에도 `Permission denied (publickey)` 가 나오면, 그건 권한 문제가 아니라 **서버에 등록된 공개키와 PEM이 짝이 안 맞거나**, **해당 사용자에 키가 안 박혀 있는 경우**다. NCP 콘솔에서 “로그인 키 / ACG / 실제 접속 계정”을 다시 확인하면 된다.

---

## 5. 로컬 Ubuntu(`192.168.160.147`)는 PEM이 아니라 비밀번호

이 프로젝트에서 말하는 **로컬 서버**는 예를 들어 다음처럼 붙는다:

```powershell
ssh ubuntu@192.168.160.147
# 비밀번호: ubuntu (초기 이미지 등에서 흔한 조합 — 운영 전 반드시 변경 권장)
```

`scp`로 스크립트를 올릴 때도 **`-i` 없이** 접속해 비밀번호를 입력하면 된다. 자동화하려면 WSL의 `sshpass` 같은 도구를 쓰거나, **로컬 서버에 전용 SSH 공개키**를 등록해 PEM과 분리하는 편이 안전하다.

---

## 6. 정리

1. **NCP Compute** → 콘솔에서 받은 **PEM** + OpenSSH `ssh -i …`.
2. Windows에서 PEM이 무시되면 → **`icacls /inheritance:r` + `/grant:r 본인:R`**.
3. **로컬 Ubuntu(192.168.x 등)** → 이 레포의 PEM과 **무관**할 수 있음. **`ubuntu` / `ubuntu` 같은 비밀번호** 또는 별도 키.
4. 운영 환경에서는 기본 비밀번호·넓은 ACL·공유 PEM을 피하고, **키 회전·최소 권한·ACG**를 맞추는 것이 좋다.

이 문서는 동일 레포의 `infra/run-remote-setup.ps1` 등에서 **NCP는 PEM, 로컬은 비밀번호**로 나누어 쓰는 전제와 맞춰 두었다.

---

## 7. 부록: `kafka-active002` 전용 키 `kafka2-ssh` (NCP 첫 PEM과 별개)

VM마다 **콘솔에서 받은 기본 PEM**과 별도로, 서버 안 `/root/.ssh/kafka2-ssh` 같은 **추가 OpenSSH 개인키**를 쓰는 경우가 있다. 스크린샷·이미지로 키를 공유하면 **OCR 한 글자 오류**만 있어도 `Load key: invalid format` 또는 `Permission denied`가 나므로, **키 전문은 이미지가 아니라 파일로** 옮기는 것이 안전하다.

**권장 절차**

1. 이미 `kafka-active002`에 쉘이 있다면, 그 세션에서 로컬로 내려받을 수 있는 경로로 복사하거나, **다른 PC에서 NCP 기본 PEM으로 002에 접속**한 뒤:
   ```bash
   scp root@<002_공인IP>:/root/.ssh/kafka2-ssh ./kafka2-ssh
   ```
2. Windows에서 저장소 루트(예: `kafka-aa/kafka2-ssh`)에 두고, 본문 앞 절의 **`icacls`** 로 권한을 본인 전용으로 맞춘다.
3. 접속:
   ```powershell
   ssh -i .\kafka2-ssh -o StrictHostKeyChecking=accept-new root@223.130.154.172
   ```
   공인 IP는 콘솔 기준으로 바꾼다.

레포에는 **`infra/ssh-kafka2.ps1`** 를 두어, 기본으로 `kafka2-ssh` 경로·`icacls`·`ssh` 한 번에 시험할 수 있다. 이 파일은 **`.gitignore`에 `kafka2-ssh`가 있으므로 키가 커밋되지 않게** 한다.
