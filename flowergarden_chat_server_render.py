import asyncio
import json
import os
import socket
import time
from collections import deque
from datetime import datetime, timedelta, timezone

from websockets.asyncio.server import serve

import psycopg
from psycopg import errors as psycopg_errors

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8765"))

MIN_CHAT_LEVEL = 5
MAX_HISTORY = 50
MAX_MESSAGE_LENGTH = 100

# ==================================================
# 🌱 계정 시스템 영구저장 - 외부 PostgreSQL
# ==================================================
# Render Free Web Service의 로컬 파일은 영구 저장용으로 사용하지 않습니다.
# DATABASE_URL 환경변수에 Neon/Supabase 등 외부 PostgreSQL 연결주소를 넣으면
# 정원번호/닉네임/레벨이 서버 재시작·재배포 후에도 유지됩니다.
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
ACCOUNT_LOOKUP_COOLDOWN_SECONDS = 1.0

# 신고 정책
REPORT_WINDOW_SECONDS = 10 * 60
REPORT_THRESHOLD = 3
REPORT_AUTO_MUTE_SECONDS = 10 * 60

# 기본 금칙어 필터
BLOCKED_WORDS = [
    "씨발",
    "시발",
    "ㅅㅂ",
    "병신",
    "개새끼",
]

KST = timezone(timedelta(hours=9))

clients = {}                  # websocket -> state
history = deque(maxlen=MAX_HISTORY)
message_sequence = 0

# user_id -> monotonic 만료시각
muted_until_by_user = {}
banned_until_by_user = {}

# target_user_id -> {reporter_user_id: monotonic_report_time}
report_votes = {}

def now_kst_iso() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def normalize_for_filter(text: str) -> str:
    return "".join(text.lower().split())


def contains_blocked_word(text: str) -> bool:
    normalized = normalize_for_filter(text)
    return any(word in normalized for word in BLOCKED_WORDS)


def is_valid_garden_number(value: str) -> bool:
    return len(value) == 6 and value.isdigit()


def get_account_db_connection():
    if not DATABASE_URL:
        return None
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=10,
    )


def ensure_account_db() -> bool:
    if not DATABASE_URL:
        print("[계정 DB] DATABASE_URL 환경변수가 아직 없습니다.")
        return False

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_accounts (
                        user_id VARCHAR(80) PRIMARY KEY,
                        garden_number CHAR(6) UNIQUE NOT NULL,
                        nickname VARCHAR(12) NOT NULL,
                        level INTEGER NOT NULL DEFAULT 0,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            conn.commit()
        return True
    except Exception as exc:
        print(
            "[계정 DB 준비 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return False


def count_account_records() -> int:
    if not DATABASE_URL:
        return 0

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM gardener_accounts")
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception:
        return 0


def lookup_account_record(garden_number: str):
    if not DATABASE_URL:
        return None, "account_db_unavailable"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, nickname, garden_number, level
                    FROM gardener_accounts
                    WHERE garden_number = %s
                    """,
                    (garden_number,),
                )
                row = cur.fetchone()

        if not row:
            return None, ""

        return {
            "user_id": str(row[0]),
            "nickname": str(row[1]),
            "garden_number": str(row[2]).strip(),
            "level": int(row[3]),
        }, ""

    except Exception as exc:
        print(
            "[계정 DB 검색 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return None, "account_db_error"


def register_account_record(
    nickname: str,
    user_id: str,
    garden_number: str,
    level: int,
):
    nickname = str(nickname).strip()
    user_id = str(user_id).strip()
    garden_number = str(garden_number).strip()

    if not user_id or len(user_id) > 80:
        return False, "invalid_user_id", "사용자 정보를 확인할 수 없어요."

    if (
        "\n" in nickname
        or "\r" in nickname
        or not (2 <= len(nickname) <= 12)
    ):
        return False, "invalid_nickname", "닉네임은 2~12글자로 입력해주세요."

    if contains_blocked_word(nickname):
        return False, "blocked_nickname", "사용할 수 없는 닉네임이에요."

    if not is_valid_garden_number(garden_number):
        return False, "invalid_garden_number", "정원번호 6자리를 확인해주세요."

    if not DATABASE_URL:
        return (
            False,
            "account_db_unavailable",
            "계정 영구저장 서버가 아직 연결되지 않았어요.",
        )

    try:
        level = max(0, int(level))
    except (TypeError, ValueError):
        level = 0

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT garden_number
                    FROM gardener_accounts
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                own_row = cur.fetchone()

                if own_row and str(own_row[0]).strip() != garden_number:
                    return (
                        False,
                        "garden_number_locked",
                        "이 정원사의 정원번호는 이미 다른 번호로 등록되어 있어요.",
                    )

                cur.execute(
                    """
                    SELECT user_id
                    FROM gardener_accounts
                    WHERE garden_number = %s
                    """,
                    (garden_number,),
                )
                number_row = cur.fetchone()

                if number_row and str(number_row[0]).strip() != user_id:
                    return (
                        False,
                        "garden_number_conflict",
                        "이 정원번호가 다른 정원사와 겹쳤어요. 운영자에게 알려주세요.",
                    )

                if own_row:
                    cur.execute(
                        """
                        UPDATE gardener_accounts
                        SET nickname = %s,
                            level = %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                        """,
                        (nickname, level, user_id),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO gardener_accounts (
                            user_id,
                            nickname,
                            garden_number,
                            level,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, NOW())
                        """,
                        (user_id, nickname, garden_number, level),
                    )

            conn.commit()

        return True, "ok", "정원사 정보가 영구 저장되었어요."

    except psycopg_errors.UniqueViolation:
        return (
            False,
            "garden_number_conflict",
            "이 정원번호가 다른 정원사와 겹쳤어요. 운영자에게 알려주세요.",
        )
    except Exception as exc:
        print(
            "[계정 DB 저장 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return (
            False,
            "account_db_error",
            "계정 저장 서버에 잠시 문제가 있어요.",
        )


def local_ipv4_candidates():
    found = set()

    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127."):
                found.add(ip)
    except OSError:
        pass

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        found.add(s.getsockname()[0])
        s.close()
    except OSError:
        pass

    return sorted(found)


def remaining_seconds(table: dict, user_id: str) -> int:
    until = table.get(user_id, 0.0)
    now = time.monotonic()

    if until <= now:
        table.pop(user_id, None)
        return 0

    return max(1, int(until - now + 0.999))


def format_remaining(seconds: int) -> str:
    if seconds <= 0:
        return "0분"

    minutes = max(1, (seconds + 59) // 60)
    return f"{minutes}분"


async def send_json(ws, payload: dict):
    await ws.send(json.dumps(payload, ensure_ascii=False))


async def broadcast(payload: dict):
    joined = [
        ws
        for ws, state in list(clients.items())
        if state.get("joined")
    ]

    if not joined:
        return

    text = json.dumps(payload, ensure_ascii=False)

    await asyncio.gather(
        *(ws.send(text) for ws in joined),
        return_exceptions=True,
    )


async def broadcast_presence():
    count = sum(
        1
        for state in clients.values()
        if state.get("joined")
    )

    await broadcast({
        "type": "presence",
        "count": count,
    })


def validate_join(payload: dict):
    nickname = str(payload.get("nickname", "")).strip()
    user_id = str(payload.get("user_id", "")).strip()
    garden_number = str(payload.get("garden_number", "")).strip()

    try:
        level = int(payload.get("level", 0))
    except (TypeError, ValueError):
        level = 0

    if level < MIN_CHAT_LEVEL:
        return (
            False,
            "level_required",
            f"정원사 광장은 Lv.{MIN_CHAT_LEVEL}부터 이용할 수 있어요.",
        )

    if not user_id or len(user_id) > 80:
        return (
            False,
            "invalid_user_id",
            "사용자 정보를 확인할 수 없어요.",
        )

    if (
        "\n" in nickname
        or "\r" in nickname
        or not (2 <= len(nickname) <= 12)
    ):
        return (
            False,
            "invalid_nickname",
            "닉네임은 2~12글자로 입력해주세요.",
        )

    if contains_blocked_word(nickname):
        return (
            False,
            "blocked_nickname",
            "사용할 수 없는 닉네임이에요.",
        )

    banned_remaining = remaining_seconds(
        banned_until_by_user,
        user_id,
    )

    if banned_remaining > 0:
        return (
            False,
            "temporarily_banned",
            "광장 이용이 "
            + format_remaining(banned_remaining)
            + " 동안 제한되어 있어요.",
        )

    return (
        True,
        {
            "nickname": nickname,
            "user_id": user_id,
            "level": level,
            "garden_number": garden_number,
        },
        "",
    )


async def handle_join(ws, payload: dict):
    state = clients[ws]

    if state.get("joined"):
        await send_json(
            ws,
            {
                "type": "error",
                "code": "already_joined",
                "message": "이미 광장에 입장했어요.",
            },
        )
        return

    ok, data, message = validate_join(payload)

    if not ok:
        await send_json(
            ws,
            {
                "type": "join_denied",
                "code": data,
                "message": message,
            },
        )
        return

    state.update(data)
    state["joined"] = True

    if is_valid_garden_number(state.get("garden_number", "")):
        account_ok, account_code, account_message = register_account_record(
            state["nickname"],
            state["user_id"],
            state["garden_number"],
            state["level"],
        )
        if not account_ok:
            print(
                "[광장 계정등록 경고] "
                f"{account_code}: {account_message}"
            )

    mute_remaining = remaining_seconds(
        muted_until_by_user,
        state["user_id"],
    )

    await send_json(
        ws,
        {
            "type": "joined",
            "nickname": state["nickname"],
            "user_id": state["user_id"],
            "level": state["level"],
            "history": list(history),
            "mute_remaining_seconds": mute_remaining,
            "server_time": now_kst_iso(),
        },
    )

    await broadcast_presence()

    print(
        f"[입장] {state['nickname']} / "
        f"Lv.{state['level']} / {state['user_id']}"
    )


async def handle_account_register(ws, payload: dict):
    nickname = str(payload.get("nickname", "")).strip()
    user_id = str(payload.get("user_id", "")).strip()
    garden_number = str(payload.get("garden_number", "")).strip()

    try:
        level = int(payload.get("level", 0))
    except (TypeError, ValueError):
        level = 0

    ok, code, message = register_account_record(
        nickname,
        user_id,
        garden_number,
        level,
    )

    await send_json(
        ws,
        {
            "type": "account_register_result",
            "ok": ok,
            "code": code,
            "message": message,
            "garden_number": garden_number,
        },
    )

    if ok:
        print(
            f"[계정등록] {nickname} / "
            f"정원번호 {garden_number} / {user_id}"
        )


async def handle_account_lookup(ws, payload: dict):
    state = clients[ws]
    now = time.monotonic()
    last_lookup = float(state.get("last_account_lookup", 0.0))

    if now - last_lookup < ACCOUNT_LOOKUP_COOLDOWN_SECONDS:
        await send_json(
            ws,
            {
                "type": "error",
                "code": "lookup_too_fast",
                "message": "정원사 찾기는 잠시 후 다시 이용해주세요.",
            },
        )
        return

    state["last_account_lookup"] = now
    garden_number = str(payload.get("garden_number", "")).strip()

    if not is_valid_garden_number(garden_number):
        await send_json(
            ws,
            {
                "type": "account_lookup_result",
                "found": False,
                "code": "invalid_garden_number",
                "message": "정원번호 6자리를 확인해주세요.",
            },
        )
        return

    record, lookup_error = lookup_account_record(garden_number)

    if lookup_error:
        await send_json(
            ws,
            {
                "type": "error",
                "code": lookup_error,
                "message": (
                    "정원사 계정 저장소에 연결하지 못했어요. "
                    "잠시 후 다시 시도해주세요."
                ),
            },
        )
        return

    if not record:
        await send_json(
            ws,
            {
                "type": "account_lookup_result",
                "found": False,
                "garden_number": garden_number,
            },
        )
        return

    await send_json(
        ws,
        {
            "type": "account_lookup_result",
            "found": True,
            "nickname": record.get("nickname", "정원사"),
            "garden_number": garden_number,
            "level": int(record.get("level", 0)),
        },
    )


async def handle_chat_message(ws, payload: dict):
    global message_sequence

    state = clients[ws]

    if not state.get("joined"):
        await send_json(
            ws,
            {
                "type": "error",
                "code": "not_joined",
                "message": "먼저 광장에 입장해주세요.",
            },
        )
        return

    if state.get("level", 0) < MIN_CHAT_LEVEL:
        await send_json(
            ws,
            {
                "type": "error",
                "code": "level_required",
                "message": f"Lv.{MIN_CHAT_LEVEL}부터 채팅할 수 있어요.",
            },
        )
        return

    mute_remaining = remaining_seconds(
        muted_until_by_user,
        state["user_id"],
    )

    if mute_remaining > 0:
        await send_json(
            ws,
            {
                "type": "moderation",
                "action": "mute",
                "remaining_seconds": mute_remaining,
                "message": (
                    "광장 채팅이 "
                    + format_remaining(mute_remaining)
                    + " 동안 제한되어 있어요."
                ),
            },
        )
        return

    message = str(payload.get("message", "")).strip()

    if not message:
        return

    if len(message) > MAX_MESSAGE_LENGTH:
        await send_json(
            ws,
            {
                "type": "error",
                "code": "message_too_long",
                "message": (
                    f"메시지는 {MAX_MESSAGE_LENGTH}자까지 "
                    "입력할 수 있어요."
                ),
            },
        )
        return

    if contains_blocked_word(message):
        await send_json(
            ws,
            {
                "type": "error",
                "code": "blocked_word",
                "message": "보낼 수 없는 표현이 포함되어 있어요.",
            },
        )
        return

    # 일반 사용자에게는 전송 쿨타임을 두지 않습니다.
    message_sequence += 1

    outgoing = {
        "type": "message",
        "message_id": message_sequence,
        "user_id": state["user_id"],
        "nickname": state["nickname"],
        "message": message,
        "time": now_kst_iso(),
    }

    history.append(outgoing)

    await broadcast(outgoing)

    print(
        f"[{state['nickname']}] "
        f"#{message_sequence} {message}"
    )


def find_history_message(message_id: int):
    for item in reversed(history):
        if int(item.get("message_id", -1)) == message_id:
            return item
    return None


def prune_report_votes(target_user_id: str):
    votes = report_votes.get(target_user_id)

    if not votes:
        return {}

    now = time.monotonic()

    stale = [
        reporter_id
        for reporter_id, report_time in votes.items()
        if now - report_time > REPORT_WINDOW_SECONDS
    ]

    for reporter_id in stale:
        votes.pop(reporter_id, None)

    if not votes:
        report_votes.pop(target_user_id, None)
        return {}

    return votes


async def notify_user_id(user_id: str, payload: dict):
    for ws, state in list(clients.items()):
        if (
            state.get("joined")
            and state.get("user_id") == user_id
        ):
            try:
                await send_json(ws, payload)
            except Exception:
                pass


async def handle_report(ws, payload: dict):
    state = clients[ws]

    if not state.get("joined"):
        await send_json(
            ws,
            {
                "type": "error",
                "code": "not_joined",
                "message": "먼저 광장에 입장해주세요.",
            },
        )
        return

    try:
        message_id = int(payload.get("message_id", -1))
    except (TypeError, ValueError):
        message_id = -1

    target_user_id = str(
        payload.get("target_user_id", "")
    ).strip()

    message_item = find_history_message(message_id)

    if (
        message_item is None
        or not target_user_id
        or message_item.get("user_id") != target_user_id
    ):
        await send_json(
            ws,
            {
                "type": "report_result",
                "ok": False,
                "message": "신고할 메시지를 확인할 수 없어요.",
            },
        )
        return

    if target_user_id == state["user_id"]:
        await send_json(
            ws,
            {
                "type": "report_result",
                "ok": False,
                "message": "내 메시지는 신고할 수 없어요.",
            },
        )
        return

    votes = prune_report_votes(target_user_id)

    if state["user_id"] in votes:
        await send_json(
            ws,
            {
                "type": "report_result",
                "ok": False,
                "message": (
                    "같은 정원사는 일정 시간 동안 "
                    "한 번만 신고할 수 있어요."
                ),
            },
        )
        return

    votes = report_votes.setdefault(target_user_id, {})
    votes[state["user_id"]] = time.monotonic()

    count = len(votes)

    print(
        f"[신고] {state['nickname']} -> "
        f"{message_item.get('nickname', target_user_id)} "
        f"(현재 {count}/{REPORT_THRESHOLD})"
    )

    await send_json(
        ws,
        {
            "type": "report_result",
            "ok": True,
            "count": count,
            "threshold": REPORT_THRESHOLD,
            "message": "신고가 접수되었습니다.",
        },
    )

    if count < REPORT_THRESHOLD:
        return

    muted_until_by_user[target_user_id] = (
        time.monotonic() + REPORT_AUTO_MUTE_SECONDS
    )

    report_votes.pop(target_user_id, None)

    await notify_user_id(
        target_user_id,
        {
            "type": "moderation",
            "action": "mute",
            "remaining_seconds": REPORT_AUTO_MUTE_SECONDS,
            "message": (
                "신고 누적으로 광장 채팅이 "
                + format_remaining(REPORT_AUTO_MUTE_SECONDS)
                + " 동안 제한되었습니다."
            ),
        },
    )

    print(
        "[자동 채팅금지] "
        f"{message_item.get('nickname', target_user_id)} "
        f"/ {format_remaining(REPORT_AUTO_MUTE_SECONDS)}"
    )


def matching_clients(target: str):
    exact_id = [
        (ws, state)
        for ws, state in clients.items()
        if state.get("joined")
        and state.get("user_id") == target
    ]

    if exact_id:
        return exact_id

    return [
        (ws, state)
        for ws, state in clients.items()
        if state.get("joined")
        and state.get("nickname") == target
    ]


async def operator_list():
    joined = [
        state
        for state in clients.values()
        if state.get("joined")
    ]

    if not joined:
        print("[운영] 현재 접속자가 없습니다.")
        return

    print("[운영] 현재 접속자")

    for state in joined:
        mute_seconds = remaining_seconds(
            muted_until_by_user,
            state["user_id"],
        )

        mute_text = (
            f" / 채팅금지 {format_remaining(mute_seconds)}"
            if mute_seconds > 0
            else ""
        )

        print(
            " - "
            f"{state['nickname']} / "
            f"Lv.{state['level']} / "
            f"{state['user_id']}"
            f"{mute_text}"
        )


async def operator_mute(target: str, minutes: int):
    matches = matching_clients(target)

    if not matches:
        print("[운영] 해당 접속자를 찾을 수 없습니다.")
        return

    if len(matches) > 1:
        print(
            "[운영] 같은 닉네임이 여러 명입니다. "
            "/list에서 user_id를 확인해주세요."
        )
        return

    _ws, state = matches[0]

    seconds = max(1, minutes) * 60

    muted_until_by_user[state["user_id"]] = (
        time.monotonic() + seconds
    )

    await notify_user_id(
        state["user_id"],
        {
            "type": "moderation",
            "action": "mute",
            "remaining_seconds": seconds,
            "message": (
                "운영자에 의해 광장 채팅이 "
                + format_remaining(seconds)
                + " 동안 제한되었습니다."
            ),
        },
    )

    print(
        f"[운영 채팅금지] {state['nickname']} / "
        f"{minutes}분"
    )


async def operator_unmute(target: str):
    matches = matching_clients(target)

    if not matches:
        # 접속 중이 아니더라도 user_id를 직접 입력할 수 있게 함
        if target in muted_until_by_user:
            muted_until_by_user.pop(target, None)
            print("[운영] 채팅금지를 해제했습니다.")
            return

        print("[운영] 해당 접속자를 찾을 수 없습니다.")
        return

    if len(matches) > 1:
        print(
            "[운영] 같은 닉네임이 여러 명입니다. "
            "/list에서 user_id를 확인해주세요."
        )
        return

    _ws, state = matches[0]

    muted_until_by_user.pop(state["user_id"], None)

    await notify_user_id(
        state["user_id"],
        {
            "type": "moderation",
            "action": "unmute",
            "remaining_seconds": 0,
            "message": "광장 채팅 제한이 해제되었습니다.",
        },
    )

    print(f"[운영 채팅금지 해제] {state['nickname']}")


async def operator_kick(
    target: str,
    ban_minutes: int,
):
    matches = matching_clients(target)

    if not matches:
        print("[운영] 해당 접속자를 찾을 수 없습니다.")
        return

    if len(matches) > 1:
        print(
            "[운영] 같은 닉네임이 여러 명입니다. "
            "/list에서 user_id를 확인해주세요."
        )
        return

    ws, state = matches[0]

    seconds = max(1, ban_minutes) * 60

    banned_until_by_user[state["user_id"]] = (
        time.monotonic() + seconds
    )

    try:
        await send_json(
            ws,
            {
                "type": "moderation",
                "action": "kick",
                "remaining_seconds": seconds,
                "message": (
                    "운영자에 의해 광장에서 퇴장되었습니다. "
                    + format_remaining(seconds)
                    + " 동안 다시 입장할 수 없습니다."
                ),
            },
        )
    except Exception:
        pass

    try:
        await ws.close(
            code=4003,
            reason="operator kick",
        )
    except Exception:
        pass

    print(
        f"[운영 강퇴] {state['nickname']} / "
        f"재입장 제한 {ban_minutes}분"
    )


def print_operator_help():
    print("")
    print("[운영자 명령어]")
    print("  /list")
    print("  /mute 닉네임 10")
    print("  /unmute 닉네임")
    print("  /kick 닉네임 30")
    print("  /help")
    print("")


async def admin_console_loop():
    print_operator_help()

    while True:
        try:
            command = await asyncio.to_thread(input, "")
        except (EOFError, KeyboardInterrupt):
            return

        command = command.strip()

        if not command:
            continue

        parts = command.split()
        action = parts[0].lower()

        try:
            if action == "/list":
                await operator_list()

            elif action == "/mute" and len(parts) >= 2:
                minutes = (
                    int(parts[2])
                    if len(parts) >= 3
                    else 10
                )
                await operator_mute(
                    parts[1],
                    max(1, minutes),
                )

            elif action == "/unmute" and len(parts) >= 2:
                await operator_unmute(parts[1])

            elif action == "/kick" and len(parts) >= 2:
                minutes = (
                    int(parts[2])
                    if len(parts) >= 3
                    else 30
                )
                await operator_kick(
                    parts[1],
                    max(1, minutes),
                )

            elif action == "/help":
                print_operator_help()

            else:
                print(
                    "[운영] 명령어를 확인해주세요. "
                    "/help 를 입력하면 목록이 나옵니다."
                )

        except ValueError:
            print(
                "[운영] 시간은 숫자(분)로 입력해주세요."
            )
        except Exception as exc:
            print(
                "[운영 오류] "
                f"{type(exc).__name__}: {exc}"
            )


async def handle_client(ws):
    clients[ws] = {
        "joined": False,
        "nickname": "",
        "user_id": "",
        "level": 0,
        "garden_number": "",
        "last_account_lookup": 0.0,
    }

    try:
        async for raw in ws:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                await send_json(
                    ws,
                    {
                        "type": "error",
                        "code": "bad_json",
                        "message": "잘못된 요청이에요.",
                    },
                )
                continue

            if not isinstance(payload, dict):
                await send_json(
                    ws,
                    {
                        "type": "error",
                        "code": "bad_request",
                        "message": "잘못된 요청이에요.",
                    },
                )
                continue

            msg_type = payload.get("type")

            if msg_type == "join":
                await handle_join(ws, payload)

            elif msg_type == "account_register":
                await handle_account_register(ws, payload)

            elif msg_type == "account_lookup":
                await handle_account_lookup(ws, payload)

            elif msg_type == "message":
                await handle_chat_message(ws, payload)

            elif msg_type == "report":
                await handle_report(ws, payload)

            elif msg_type == "ping":
                await send_json(
                    ws,
                    {
                        "type": "pong",
                        "time": now_kst_iso(),
                    },
                )

            else:
                await send_json(
                    ws,
                    {
                        "type": "error",
                        "code": "unknown_type",
                        "message": "알 수 없는 요청이에요.",
                    },
                )

    except Exception as exc:
        print(
            f"[연결 종료] "
            f"{type(exc).__name__}: {exc}"
        )

    finally:
        state = clients.pop(ws, None)

        if state and state.get("joined"):
            print(f"[퇴장] {state['nickname']}")
            await broadcast_presence()


async def main():
    account_db_ready = ensure_account_db()

    print("=" * 60)
    print(" FlowerGarden 정원사 광장 + 영구계정 Render 서버 v4")
    print("=" * 60)
    print(f"Render 서버 포트: {PORT}")
    if account_db_ready:
        print(f"영구 계정 DB 연결: 정상 / 등록 {count_account_records()}명")
    else:
        print("영구 계정 DB 연결: 미설정 또는 연결 실패")

    ips = local_ipv4_candidates()

    if ips:
        print("같은 Wi-Fi 휴대폰 주소 후보:")
        for ip in ips:
            print(f"  ws://{ip}:{PORT}")
    else:
        print(
            "로컬 IP를 찾지 못했습니다. "
            "ipconfig로 IPv4 주소를 확인해주세요."
        )

    print(f"채팅 가능 레벨: Lv.{MIN_CHAT_LEVEL} 이상")
    print("일반 채팅 전송 쿨타임: 없음")
    print(
        "신고 자동제재: "
        f"{REPORT_THRESHOLD}명 신고 -> "
        f"{REPORT_AUTO_MUTE_SECONDS // 60}분 채팅금지"
    )
    print("서버 종료: Ctrl + C")
    print("=" * 60)

    async with serve(
        handle_client,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=8 * 1024,
    ):
        console_task = asyncio.create_task(
            admin_console_loop()
        )

        try:
            await asyncio.Future()
        finally:
            console_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n서버를 종료했습니다.")
