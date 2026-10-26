# -*- coding: utf-8 -*-
"""FlowerGarden friend-cheer server PR #1 test-only allowlist patcher.

입력: GitHub 'friend-cheer-test' 브랜치의 flowergarden_chat_server_render.py
출력: 같은 폴더의 flowergarden_chat_server_render_cheer_test.py (원본은 변경하지 않음)

가오픈 테스트 서버 Render 환경변수:
  FRIEND_CHEER_TEST_ENABLED=1
  FRIEND_CHEER_TEST_GARDEN_NUMBERS=<실제 테스트 정원번호1>,<실제 테스트 정원번호2>

반드시 테스트 서버에서 먼저 검증하세요. 이 파일을 실행하는 것만으로
GitHub PR이 병합되거나 서버에 배포되지는 않습니다.
"""

import ast
import sys
from pathlib import Path


def replace_exact(code: str, old: str, new: str, label: str) -> str:
    count = code.count(old)
    if count != 1:
        raise ValueError(f"{label}: 예상 위치 1곳인데 {count}곳 발견. 최신 PR 서버 파일인지 확인하세요.")
    return code.replace(old, new, 1)


def patch(source: str) -> str:
    if "FRIEND_CHEER_TEST_GARDEN_NUMBERS" in source:
        raise ValueError("이미 가오픈 설정이 있는 서버 파일입니다. 중복 적용을 중단합니다.")
    # 이미 import os가 있으면 그대로 사용, 없을 때만 추가합니다.
    if "import os\n" not in source:
        source = "import os\n" + source

    source = replace_exact(
        source,
        'FRIEND_CHEER_RELEASE_AT = datetime(2026, 11, 9, 5, 0, tzinfo=KST)\nFRIEND_CHEER_EGG_REWARD = 5',
        '''FRIEND_CHEER_RELEASE_AT = datetime(2026, 11, 9, 5, 0, tzinfo=KST)
FRIEND_CHEER_EGG_REWARD = 5
# 가오픈은 실제 DB 정원번호로 두 계정 이상 명시 지정한 경우에만 허용합니다.
FRIEND_CHEER_TEST_ENABLED = os.getenv("FRIEND_CHEER_TEST_ENABLED", "0") == "1"
FRIEND_CHEER_TEST_GARDEN_NUMBERS = frozenset(
    number.strip()
    for number in os.getenv("FRIEND_CHEER_TEST_GARDEN_NUMBERS", "").split(",")
    if number.strip()
)''',
        "테스트 환경변수",
    )
    source = replace_exact(
        source,
        '''def friend_cheer_live() -> bool:
    return datetime.now(KST) >= FRIEND_CHEER_RELEASE_AT
''',
        '''def friend_cheer_live() -> bool:
    return datetime.now(KST) >= FRIEND_CHEER_RELEASE_AT


def is_friend_cheer_test_account(user_id: str) -> bool:
    # 클라이언트 입력의 시험 모드 플래그를 신뢰하지 않습니다.
    if not FRIEND_CHEER_TEST_ENABLED or len(FRIEND_CHEER_TEST_GARDEN_NUMBERS) < 2:
        return False
    if not user_id:
        return False
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT garden_number FROM gardener_accounts WHERE user_id = %s",
                    (user_id,),
                )
                row = cur.fetchone()
                return bool(row) and str(row[0]).strip() in FRIEND_CHEER_TEST_GARDEN_NUMBERS
    except Exception as exc:
        print(f"[응원 테스트 허용목록 확인 오류] {type(exc).__name__}: {exc}")
        return False
''',
        "테스트 계정 검사",
    )
    source = replace_exact(
        source,
        '''def friend_cheer_sent_today(user_id: str):
    if not friend_cheer_live():
        return []''',
        '''def friend_cheer_sent_today(user_id: str):
    if not (friend_cheer_live() or is_friend_cheer_test_account(user_id)):
        return []''',
        "응원 목록 날짜 잠금",
    )
    source = replace_exact(
        source,
        '''def create_friend_cheer(sender_user_id: str, receiver_garden_number: str):
    if not friend_cheer_live():
        return False, "not_released", "친구 응원은 11월 9일 오전 5시부터 시작해요."''',
        '''def create_friend_cheer(sender_user_id: str, receiver_garden_number: str):
    if not (friend_cheer_live() or is_friend_cheer_test_account(sender_user_id)):
        return False, "not_released", "친구 응원은 11월 9일 오전 5시부터 시작해요."''',
        "응원 발신 잠금",
    )
    source = replace_exact(
        source,
        '''    receiver_user_id = str(receiver["user_id"])
    if sender_user_id == receiver_user_id:''',
        '''    receiver_user_id = str(receiver["user_id"])
    if not (friend_cheer_live() or is_friend_cheer_test_account(receiver_user_id)):
        return False, "test_accounts_only", "가오픈 기간에는 등록된 테스트 친구끼리만 응원할 수 있어요."
    if sender_user_id == receiver_user_id:''',
        "응원 수신 잠금",
    )
    source = replace_exact(
        source,
        '''def create_friend_cheer_thanks(user_id: str, cheer_entry_id: int):
    if not friend_cheer_live():''',
        '''def create_friend_cheer_thanks(user_id: str, cheer_entry_id: int):
    if not (friend_cheer_live() or is_friend_cheer_test_account(user_id)):''',
        "감사 인사 발신 잠금",
    )
    source = replace_exact(
        source,
        '''                supporter_id = str(found[1])
                if get_friend_status(user_id, supporter_id) != "friends":''',
        '''                supporter_id = str(found[1])
                if not (friend_cheer_live() or is_friend_cheer_test_account(supporter_id)):
                    return False, "test_accounts_only", "가오픈 기간에는 등록된 테스트 친구끼리만 감사 인사할 수 있어요."
                if get_friend_status(user_id, supporter_id) != "friends":''',
        "감사 인사 수신 잠금",
    )
    source = replace_exact(
        source,
        '''                            and not bool(row[7]) and friend_cheer_live()''',
        '''                            and not bool(row[7]) and (friend_cheer_live() or is_friend_cheer_test_account(user_id))''',
        "방명록 감사 버튼 잠금",
    )
    # DB 테이블 삭제/초기화, 기존 계정·보상 수치 변경 없음.
    ast.parse(source)
    return source


def main() -> int:
    if len(sys.argv) != 2:
        print("사용법: python enable_friend_cheer_test_allowlist.py flowergarden_chat_server_render.py")
        return 2
    path = Path(sys.argv[1])
    if not path.is_file():
        print(f"파일이 없습니다: {path}")
        return 2
    original = path.read_text(encoding="utf-8")
    try:
        updated = patch(original)
    except (ValueError, SyntaxError) as exc:
        print(f"중단: {exc}")
        return 1
    output = path.with_name("flowergarden_chat_server_render_cheer_test.py")
    if output.exists():
        print(f"덮어쓰기 방지: 출력 파일이 이미 있습니다: {output}")
        return 1
    output.write_text(updated, encoding="utf-8")
    print(f"성공: {output} (원본 보존, 테스트 서버에 별도로 배포해야 함)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
