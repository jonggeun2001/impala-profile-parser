# 검증 기록

대상 계약은 Python 3.9 / CDP Private Cloud Base 7.1.7이다.
로컬 실행 환경은 macOS arm64, CPython 3.9.6, PyArrow 21.0.0, Thrift 0.16.0이다.

## 최종 결과 (2026-09-11)

- Python 3.9.6: 전체 **102개 테스트 통과** (decoder 60, metrics 20, CLI/output 22).
- Ruff, `git diff --check`, `uv lock --check` 통과.
- wheel/sdist 빌드 및 별도 Python 3.9 환경 설치 통과.
- 소스 밖 CLI 실행 후 Parquet 1행/22컬럼(스키마 버전 2)과 기대값 일치 확인.
- 출력 처리와 전체 코드 독립 리뷰 완료; 발견된 중요 사항 수정 후 재검토 통과.

## 재현 명령

```bash
uv sync --python 3.9 --locked
bash scripts/verify-worktree.sh
uv build --quiet
uv venv --python 3.9 local/wheel-env
uv pip install --python local/wheel-env/bin/python dist/impala_profile_parser-0.0.1-py3-none-any.whl
```

빌드된 wheel을 별도 Python 3.9 환경에 설치한 뒤 소스 디렉터리 밖에서 아래 CLI를
실행하고 PyArrow로 파일을 다시 읽었다. 프로젝트 경로는 실제 절대 경로로 바꾼다.

```bash
PROJECT_ROOT=/absolute/path/to/impala-profile-parser
cd /tmp
"$PROJECT_ROOT/local/wheel-env/bin/impala-profile-parser" \
  --input "$PROJECT_ROOT/tests/fixtures/impala-3.4-synthetic.log" \
  --output "$PROJECT_ROOT/local/wheel-smoke" \
  --timezone UTC
```

기대 결과는 22개 컬럼, 1행, queryId `0000000000000001:0000000000000002`,
durationMilliseconds 1250, resultRows 1이다.

## 검증 범위

- 디코딩: 3.4 독립 wire fixture와 생성 객체 fixture, version 1/2, required 필드,
  Base64/zlib/Thrift 손상·잘림·잔여 데이터·크기/깊이/정수 한계, 집계 배열 정합성.
- 지표: Summary/ImpalaServer/Execution Profile 범위, 평균 카운터 제외, SQL 보존,
  누락과 0, 충돌, UTC/IANA/DST/나노초→밀리초, query ID 불일치.
- 출력: 다중 배치, 빈 입력, Parquet 재읽기, footer 손상, 기존 파일 보존,
  원자적 교체 실패, 프로세스 간 잠금.
- 입력: 전체 상대경로 순서, 숨김·링크 제외, 디렉터리 교체 race,
  입력 파일 변경 감지, 경로/인자 오류, payload 없는 진단 로그.
- 패키징: wheel/sdist 생성, 별도 환경 설치, 소스 밖 CLI 실행, license/provenance 포함.

독립 코드 리뷰에서 발견된 상대경로 정렬·디렉터리 링크 교체·진단 출처 누락은
회귀 테스트와 수정 후 재검토했다. 평균 scalar 카운터의 오해석과 숫자 디코딩의
무제한 정수 생성도 회귀 테스트 대상으로 반영했다.

## 확인하지 않은 범위

실제 CDP 운영 로그, SP/CHF별 프로파일 차이, 운영 데이터 규모에서의 처리량은
확인하지 않았다. 위 검증 결과는 로컬 Python 3.9 실행과 합성 fixture 기준이다.

Linux/Python 3.9 검증은 push와 pull request마다 자동 실행하며 수동 실행도 지원한다.
테스트 후 wheel/sdist를 빌드하고, 소스 밖 별도 가상환경의 wheel 설치·변환 검증이
통과하면 패키지와 SHA256 체크섬을 다운로드 가능한 artifact로 30일간 보관한다.
태그 실행은 패키지 버전과 태그 이름이 일치해야 통과한다.
실행 결과는 [GitHub Actions](https://github.com/jonggeun2001/impala-profile-parser/actions/workflows/test.yml)에서 확인한다.
