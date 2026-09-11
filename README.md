# Impala Profile Parser

Python 3.9 기반으로 CDP 7.1.7의 인코딩된 impalad profile 로그를 Parquet으로 변환하는 로컬 CLI입니다.

## 설치 및 실행

Linux/macOS에서 Python 3.9 이상의 가상환경을 사용합니다. 실행할 컴퓨터에는 대상 OS와
CPU에 맞는 PyArrow 패키지가 필요합니다. Impala 서버에 직접 설치할 필요는 없습니다.

```bash
python3.9 -m venv .venv
. .venv/bin/activate
python -m pip install .

impala-profile-parser \
  --input ./local/profiles \
  --output ./local/output \
  --timezone Asia/Seoul
```

결과는 `local/output/result.parquet`입니다. **입력 profile 레코드 한 건당 한 행**이며,
동일 query ID의 여러 스냅샷과 중복 행도 보존합니다. 재실행하면 입력 전체로 결과를 교체합니다.

운영 로그와 변환 결과는 Git에서 제외되는 `local/` 아래에 보관합니다.
저장소에 포함된 `tests/fixtures/impala-3.4-synthetic.log`는 검증용 합성 데이터입니다.

`--timezone`은 프로파일의 timezone 없는 `Start Time`/`End Time` 문자열에 적용하는
IANA timezone입니다. 기본은 UTC이며 운영 Impala 서버의 시간대에 맞춰 지정합니다.
로그 헤더의 epoch 시각에는 영향을 주지 않습니다. 출력 시각은 UTC 밀리초입니다.

## 입력

일관된 사본의 인코딩 로그 파일 하나 또는 로그만 담긴 디렉터리를 지정합니다.
디렉터리는 하위 경로까지 이름 순서로 읽고 숨김 항목·심볼릭 링크는 제외합니다.
입력으로 직접 지정한 심볼릭 링크는 거부합니다. 입력·출력 경로는 포함 관계이면 안 됩니다.

각 행은 아래 형식입니다. 빈 행은 무시합니다.

```text
<epoch milliseconds> <query-id> <base64(zlib(compact-thrift-profile))>
```

텍스트/JSON export, 독립 Base64 문자열, 비압축 Thrift, gzip 파일은 지원하지 않습니다.
파일이 읽는 동안 변경되면 실패하므로 회전이 끝난 로그의 사본을 사용합니다.

## 출력 지표와 지원 범위

쿼리 ID·기록 시각·원본 위치, 사용자·SQL·상태·시작/종료 시각 등의 요약 정보를 기록합니다.
SQL은 Impala가 기록한 문자열(마스킹되었을 수 있음)을 공백·줄바꿈과 함께 보존합니다.

`resultRows`는 `ImpalaServer/NumRowsFetched`이며 `resultRowsKind=CLIENT_FETCHED`로
표시합니다. client가 가져간 행 수이고, SELECT의 전체 생성 행 수나 DML의 커밋 행 수를
보장하지 않습니다. 중간 operator의 `RowsReturned`를 합산하지 않습니다.

`cpuMilliseconds`는 query 범위의 `TotalCpuTime`이 TIME_NS 단위로 존재할 때만 채웁니다.
출력은 구현된 지표만 포함한 22개 컬럼이며 스키마 버전은 `2`입니다.
원본에 없는 선택 지표와 실제 0을 구분합니다. 상세한 필드·출처는 [출력 스키마](docs/reference/schema.md)에 있습니다.

대상은 CDP Private Cloud Base 7.1.7이며 공식 구성표상 Impala는
`3.4.0.7.1.7.0-551`입니다. 공개 Apache 3.4 계열 구조와 생성 fixture로 검증합니다.
**실제 CDP 운영 로그와 SP/CHF별 변경은 아직 검증하지 않았습니다.**
Thrift reader에는 기존 필드를 유지하는 Apache 4.6.0 생성 코드를 포함했으며,
이는 대상 Impala 버전이 4.6이라는 뜻이 아닙니다. 실행 시 shell/컴파일러 설치는 필요 없습니다.

## 실패 처리

임시 Parquet에 기록하고 스키마·footer·행 수를 검증한 다음 원자적으로 교체합니다.
게시 전 실패하면 기존 `result.parquet`은 보존합니다. 동일 출력 디렉터리의 동시 실행은
OS 잠금으로 거부합니다. 정상 빈 입력은 스키마가 있는 0행 파일을 만듭니다.

| 종료 코드 | 의미 |
| --- | --- |
| 0 | 정상 종료. 선택 필드 경고/null이 있을 수 있음 |
| 1 | 파일·디코딩·구조·출력 오류, 기존 결과 보존 |
| 2 | 인자·입출력 경로·timezone 오류 |
| 3 | 출력 디렉터리 잠금 충돌 |
| 130 | 사용자 중단 |

선택 필드 오류는 해당 값을 null로 두고 `parseStatus=WARNING`, `warningCount`에 반영합니다.
필수 식별자·payload 손상은 실행 전체 실패입니다. 진행 결과는 stdout, 경고·오류는 stderr로
기록합니다. 진단 로그에는 SQL이나 인코딩 payload를 포함하지 않습니다.

| 옵션 | 기본값 | 의미 |
| --- | --- | --- |
| `--batch-size` | 1024 | 배치당 최대 행 수. 추정 16 MiB 도달 시에도 기록 |
| `--max-line-bytes` | 67108864 | 개행을 포함한 로그 행 최대 바이트 수 |
| `--max-decoded-bytes` | 67108864 | 압축 해제 Thrift 최대 바이트 수 |
| `--timezone` | UTC | Start/End Time 문자열의 시간대 |
| `--help`, `--version` | | 도움말과 버전 |

Thrift 내부에는 별도 한계가 있습니다: 개별 문자열 16 MiB, 컨테이너 항목과
프로파일 노드 각각 100,000개, 구조 중첩 64단계입니다.

## 개발·검증

```bash
uv sync --python 3.9 --locked
bash scripts/verify-worktree.sh
uv build
```

런타임 의존성은 `thrift==0.16.0`, `pyarrow==21.0.0`, `tzdata==2025.2`로 고정합니다.
PyArrow 22는 Python 3.10 이상을 요구하므로 Python 3.9에서는 21을 사용합니다.
테스트는 pytest 8.4.2이고 전체 해결 결과는 `uv.lock`에 기록합니다.

## CI 다운로드와 릴리스

[GitHub Actions](https://github.com/jonggeun2001/impala-profile-parser/actions/workflows/test.yml)에서
성공한 실행을 열고 **Artifacts → `impala-profile-parser-<버전>`**을 다운로드합니다.
ZIP에는 설치용 `.whl`, 소스 배포본 `.tar.gz`, 검증용 `SHA256SUMS`가 들어 있으며
보관 기간은 30일입니다. 저장소 접근 권한이 있는 GitHub 계정으로 로그인해야 합니다.
`Run workflow`로 원하는 브랜치의 빌드를 다시 실행할 수도 있습니다.

압축 해제 후 Python 3.9 가상환경에서 설치합니다.

```bash
python -m pip install ./impala_profile_parser-*.whl
impala-profile-parser --help
```

CI는 테스트 후 패키지를 빌드하고 별도 가상환경에 wheel을 설치해 합성 로그 변환까지
검증한 뒤 업로드합니다. 실제 운영 로그와 Parquet 결과는 배포 패키지에 포함하지 않습니다.

개발 브랜치는 `main`, 릴리스 브랜치는 `release`입니다. 릴리스는 패키지 버전을 명시적으로
갱신하고 `main → release` PR의 CI가 통과하면 merge commit에 `0.0.1` 형식의
annotated tag를 달아 푸시합니다. 태그 CI는 태그와 패키지 버전이 일치하는지도 검사합니다.

[검증 기록](docs/development/verification.md) ·
[오픈소스 출처](THIRD-PARTY-NOTICES.md)
