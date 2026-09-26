# 2026-09-26 수확물장터 서버 1.0: 상품등록/조회/검색/취소/비밀번호/동시구매방지/판매기록10건/안전수령함 추가
# 2026-09-23 친구목록 칭호연동 복구: 9/22 메인채팅 서버에 9/19 실제 사용칭호 동기화(title_name/title_synced) 재병합 / 기존 메인채팅·귓속말·친구·방명록·함께하는정원·쿠폰 유지
# 2026-09-22 FlowerGarden 메인 채팅: 최근 50개 DB 유지 / 메인 최신 공개채팅용 history / @닉네임 내용 귓속말(송신자+수신자만 전달·저장) / 기존 신고·제재·친구·방명록·함께하는정원·쿠폰 유지
# 2026-09-18 쿠폰코드 시스템 1.0: 운영자센터에서 쿠폰 생성/기간설정/중지 + 게임에서 1계정 1회 사용 + 기존 운영자 선물함으로 안전 지급
# 2026-09-18 운영자 보상 시스템 1.1: 랜덤/지정 씨앗쿠폰 보상 필드 추가 + 웹 운영자센터 연동 준비
# 2026-09-18 운영자 보상 시스템 1단계: 전체/특정 유저 발송 DB + 유저검색 + 발송기록 + 게임 미수령/수령확인 API
# 2026-09-18 운영자 요청: 테스트 중 중복 생성된 닉네임 '헤라' 계정을 현재 DB에서 전부 1회 안전 삭제
# 2026-09-16 긴급수정: 함께하는 정원 100,000송이 + 기존 10%/25% 미지급 보상 1회 복구 + 이후 공동보상 정상지급
# 2026-09-16 운영자 요청: 기존 개발자(PC) Lv.50 / 정원번호 806956 계정을 1회 안전 삭제
# FlowerGarden Render server - legacy friend garden auto-migration for existing users 2026-09-16
# - old snapshots without bloom/timing metadata are upgraded server-side
# - offline growth/steal works without requiring every existing user to open a new APK first
# - old-client resync preserves server-generated legacy bloom/timing metadata when possible
# FlowerGarden Render server - friend garden offline growth/steal fix 2026-09-16
# FlowerGarden Render server - together garden server integration 2026-09-15
# FlowerGarden Render server v8 - account/friends/guestbook/garden steal/flower gifts
# FlowerGarden Render server v7 - 친구 꽃밭 보기/서리 + 기존 계정/친구/방명록/광장 유지
import asyncio
import hashlib
import json
import os
import re
import secrets
import hashlib
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

# ==================================================
# 🎁 운영자 보상 시스템
# ==================================================
# 웹 운영자센터 비밀번호는 코드에 적지 않고 Render Environment의
# ADMIN_PASSWORD 환경변수에만 저장합니다.
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "").strip()

# 게임 안 운영자 계정은 닉네임이 아니라 정원번호로 확인합니다.
ADMIN_GARDEN_NUMBER = "871511"

# 사용자가 확정한 기본 운영자 특별보상
ADMIN_DEFAULT_REWARD = {
    "king_water_drops": 50,
    "water_drops": 100,
    "gold": 10000,
    "lottery_tickets": 10,
    "wait_passes": 10,
    # 씨앗쿠폰은 기본 꾸러미와 별도 보상입니다.
    "random_seed_coupons": 0,
    "choice_seed_coupons": 0,
}

ADMIN_REWARD_MAX_GOLD = 100000000
ADMIN_REWARD_MAX_ITEM_COUNT = 1000000

# ==================================================
# 🌼 함께하는 정원 - 서버 공동 이벤트
# ==================================================
TOGETHER_GARDEN_EVENT_ID = "first_tree_20260915"
TOGETHER_GARDEN_TARGET_FLOWERS = 100000
TOGETHER_GARDEN_REWARD_MILESTONES = (10, 25, 50, 75, 100)
TOGETHER_GARDEN_MAX_HARVEST_PER_ACTION = 100
TRANSFER_CODE_LENGTH = 8
TRANSFER_BACKUP_RETENTION_DAYS = 45
TRANSFER_SAVE_MAX_BYTES = 1500 * 1024

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
                cur.execute(
                    """
                    ALTER TABLE gardener_accounts
                    ADD COLUMN IF NOT EXISTS title_name VARCHAR(40)
                    NOT NULL DEFAULT '초보 정원사'
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE gardener_accounts
                    ADD COLUMN IF NOT EXISTS title_synced BOOLEAN
                    NOT NULL DEFAULT FALSE
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_friend_requests (
                        requester_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        target_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (requester_user_id, target_user_id),
                        CHECK (requester_user_id <> target_user_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_friendships (
                        user_id_a VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        user_id_b VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (user_id_a, user_id_b),
                        CHECK (user_id_a < user_id_b)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_guestbook_entries (
                        entry_id BIGSERIAL PRIMARY KEY,
                        owner_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        author_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        message VARCHAR(60) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_guestbook_owner_created
                    ON gardener_guestbook_entries (owner_user_id, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_garden_snapshots (
                        owner_user_id VARCHAR(80) PRIMARY KEY
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        plots JSONB NOT NULL DEFAULT '[]'::jsonb,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_garden_steals (
                        requester_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        owner_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        area_id VARCHAR(12) NOT NULL,
                        bloom_id VARCHAR(140) NOT NULL,
                        flower_id VARCHAR(100) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (
                            requester_user_id,
                            owner_user_id,
                            area_id,
                            bloom_id
                        ),
                        CHECK (requester_user_id <> owner_user_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_garden_steals_owner_bloom
                    ON gardener_garden_steals (
                        owner_user_id,
                        area_id,
                        bloom_id
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_flower_gifts (
                        gift_id BIGSERIAL PRIMARY KEY,
                        sender_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        receiver_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        flower_id VARCHAR(100) NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        claimed_at TIMESTAMPTZ NULL,
                        CHECK (sender_user_id <> receiver_user_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_flower_gifts_receiver_pending
                    ON gardener_flower_gifts (
                        receiver_user_id,
                        claimed_at,
                        created_at DESC
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS admin_reward_sends (
                        send_id BIGSERIAL PRIMARY KEY,
                        target_kind VARCHAR(12) NOT NULL,
                        target_user_id VARCHAR(80) NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE SET NULL,
                        target_garden_number CHAR(6) NULL,
                        target_nickname VARCHAR(12) NULL,
                        gold INTEGER NOT NULL DEFAULT 0,
                        water_drops INTEGER NOT NULL DEFAULT 0,
                        king_water_drops INTEGER NOT NULL DEFAULT 0,
                        lottery_tickets INTEGER NOT NULL DEFAULT 0,
                        wait_passes INTEGER NOT NULL DEFAULT 0,
                        random_seed_coupons INTEGER NOT NULL DEFAULT 0,
                        choice_seed_coupons INTEGER NOT NULL DEFAULT 0,
                        note VARCHAR(120) NOT NULL DEFAULT '',
                        recipient_count INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CHECK (target_kind IN ('all', 'user')),
                        CHECK (gold >= 0),
                        CHECK (water_drops >= 0),
                        CHECK (king_water_drops >= 0),
                        CHECK (lottery_tickets >= 0),
                        CHECK (wait_passes >= 0),
                        CHECK (random_seed_coupons >= 0),
                        CHECK (choice_seed_coupons >= 0),
                        CHECK (recipient_count >= 0)
                    )
                    """
                )
                # 이미 생성된 운영자 보상 테이블에도 씨앗쿠폰 컬럼을 안전하게 추가합니다.
                cur.execute(
                    """
                    ALTER TABLE admin_reward_sends
                    ADD COLUMN IF NOT EXISTS random_seed_coupons INTEGER NOT NULL DEFAULT 0
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE admin_reward_sends
                    ADD COLUMN IF NOT EXISTS choice_seed_coupons INTEGER NOT NULL DEFAULT 0
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS admin_reward_deliveries (
                        delivery_id BIGSERIAL PRIMARY KEY,
                        send_id BIGINT NOT NULL
                            REFERENCES admin_reward_sends(send_id) ON DELETE CASCADE,
                        receiver_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        delivered_at TIMESTAMPTZ NULL,
                        UNIQUE (send_id, receiver_user_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_admin_reward_delivery_pending
                    ON admin_reward_deliveries (
                        receiver_user_id, delivered_at, created_at
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_admin_reward_sends_created
                    ON admin_reward_sends (created_at DESC, send_id DESC)
                    """
                )

                # ==================================================
                # 🎟 쿠폰 코드
                # ==================================================
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gift_coupons (
                        coupon_id BIGSERIAL PRIMARY KEY,
                        code VARCHAR(32) UNIQUE NOT NULL,
                        title VARCHAR(60) NOT NULL DEFAULT '',
                        gold INTEGER NOT NULL DEFAULT 0,
                        water_drops INTEGER NOT NULL DEFAULT 0,
                        king_water_drops INTEGER NOT NULL DEFAULT 0,
                        lottery_tickets INTEGER NOT NULL DEFAULT 0,
                        wait_passes INTEGER NOT NULL DEFAULT 0,
                        random_seed_coupons INTEGER NOT NULL DEFAULT 0,
                        choice_seed_coupons INTEGER NOT NULL DEFAULT 0,
                        note VARCHAR(120) NOT NULL DEFAULT '',
                        starts_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        ends_at TIMESTAMPTZ NULL,
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CHECK (gold >= 0),
                        CHECK (water_drops >= 0),
                        CHECK (king_water_drops >= 0),
                        CHECK (lottery_tickets >= 0),
                        CHECK (wait_passes >= 0),
                        CHECK (random_seed_coupons >= 0),
                        CHECK (choice_seed_coupons >= 0)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gift_coupon_claims (
                        coupon_id BIGINT NOT NULL
                            REFERENCES gift_coupons(coupon_id) ON DELETE CASCADE,
                        user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        delivery_id BIGINT NULL
                            REFERENCES admin_reward_deliveries(delivery_id) ON DELETE SET NULL,
                        claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (coupon_id, user_id)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_gift_coupons_created
                    ON gift_coupons (created_at DESC, coupon_id DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_gift_coupon_claims_user
                    ON gift_coupon_claims (user_id, claimed_at DESC)
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_transfer_backups (
                        transfer_code CHAR(8) PRIMARY KEY,
                        owner_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        garden_number CHAR(6) NOT NULL,
                        save_data JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        expires_at TIMESTAMPTZ NOT NULL,
                        used_at TIMESTAMPTZ NULL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_transfer_backups_owner_created
                    ON gardener_transfer_backups (owner_user_id, created_at DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gardener_chat_messages (
                        message_id BIGSERIAL PRIMARY KEY,
                        sender_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        sender_nickname VARCHAR(12) NOT NULL,
                        message VARCHAR(100) NOT NULL,
                        whisper_target_user_id VARCHAR(80) NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        whisper_target_nickname VARCHAR(12) NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_chat_messages_created
                    ON gardener_chat_messages (message_id DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_chat_messages_whisper_target
                    ON gardener_chat_messages (
                        whisper_target_user_id, message_id DESC
                    )
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS together_garden_events (
                        event_id VARCHAR(80) PRIMARY KEY,
                        total_flowers INTEGER NOT NULL DEFAULT 0,
                        target_flowers INTEGER NOT NULL DEFAULT 1000,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CHECK (total_flowers >= 0),
                        CHECK (target_flowers > 0)
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT INTO together_garden_events (
                        event_id, total_flowers, target_flowers, updated_at
                    ) VALUES (%s, 0, %s, NOW())
                    ON CONFLICT (event_id) DO UPDATE
                    SET target_flowers = EXCLUDED.target_flowers
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, TOGETHER_GARDEN_TARGET_FLOWERS),
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS together_garden_contributions (
                        event_id VARCHAR(80) NOT NULL
                            REFERENCES together_garden_events(event_id) ON DELETE CASCADE,
                        user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        flower_count INTEGER NOT NULL DEFAULT 0,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (event_id, user_id),
                        CHECK (flower_count >= 0)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS together_garden_harvest_actions (
                        event_id VARCHAR(80) NOT NULL
                            REFERENCES together_garden_events(event_id) ON DELETE CASCADE,
                        user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        action_id VARCHAR(180) NOT NULL,
                        requested_count INTEGER NOT NULL,
                        accepted_count INTEGER NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (event_id, user_id, action_id),
                        CHECK (requested_count > 0),
                        CHECK (accepted_count >= 0)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_together_garden_harvest_user_created
                    ON together_garden_harvest_actions (
                        event_id, user_id, created_at DESC
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS together_garden_reward_delivery (
                        event_id VARCHAR(80) NOT NULL
                            REFERENCES together_garden_events(event_id) ON DELETE CASCADE,
                        user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        milestone INTEGER NOT NULL,
                        status VARCHAR(16) NOT NULL DEFAULT 'pending',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        delivered_at TIMESTAMPTZ NULL,
                        PRIMARY KEY (event_id, user_id, milestone),
                        CHECK (milestone IN (10, 25, 50, 75, 100)),
                        CHECK (status IN ('pending', 'delivered'))
                    )
                    """
                )
                # ==================================================
                # 🌷 수확물장터 1.0 - 서버 영구저장
                # ==================================================
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS harvest_market_listings (
                        listing_id BIGSERIAL PRIMARY KEY,
                        seller_user_id VARCHAR(80) NOT NULL
                            REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        seller_nickname VARCHAR(12) NOT NULL,
                        slot_no INTEGER NOT NULL,
                        harvest_id VARCHAR(100) NOT NULL,
                        harvest_name VARCHAR(80) NOT NULL,
                        rarity VARCHAR(20) NOT NULL DEFAULT '일반',
                        quantity INTEGER NOT NULL,
                        unit_price INTEGER NOT NULL,
                        warehouse_price INTEGER NOT NULL DEFAULT 0,
                        password_hash VARCHAR(64) NULL,
                        status VARCHAR(16) NOT NULL DEFAULT 'active',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        sold_at TIMESTAMPTZ NULL,
                        CHECK (slot_no BETWEEN 1 AND 12),
                        CHECK (quantity BETWEEN 1 AND 30),
                        CHECK (unit_price > 0),
                        CHECK (warehouse_price >= 0),
                        CHECK (status IN ('active', 'sold', 'cancelled'))
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_market_active_seller_slot
                    ON harvest_market_listings (seller_user_id, slot_no)
                    WHERE status = 'active'
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_market_active_created
                    ON harvest_market_listings (status, created_at DESC, listing_id DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS harvest_market_sales (
                        sale_id BIGSERIAL PRIMARY KEY,
                        listing_id BIGINT NOT NULL REFERENCES harvest_market_listings(listing_id),
                        seller_user_id VARCHAR(80) NOT NULL REFERENCES gardener_accounts(user_id),
                        seller_nickname VARCHAR(12) NOT NULL,
                        buyer_user_id VARCHAR(80) NOT NULL REFERENCES gardener_accounts(user_id),
                        buyer_nickname VARCHAR(12) NOT NULL,
                        harvest_id VARCHAR(100) NOT NULL,
                        harvest_name VARCHAR(80) NOT NULL,
                        quantity INTEGER NOT NULL,
                        unit_price INTEGER NOT NULL,
                        total_price INTEGER NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        CHECK (seller_user_id <> buyer_user_id),
                        CHECK (quantity > 0),
                        CHECK (unit_price > 0),
                        CHECK (total_price > 0)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_market_sales_seller_created
                    ON harvest_market_sales (seller_user_id, created_at DESC, sale_id DESC)
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS harvest_market_deliveries (
                        delivery_id BIGSERIAL PRIMARY KEY,
                        sale_id BIGINT NOT NULL REFERENCES harvest_market_sales(sale_id) ON DELETE CASCADE,
                        receiver_user_id VARCHAR(80) NOT NULL REFERENCES gardener_accounts(user_id) ON DELETE CASCADE,
                        delivery_kind VARCHAR(16) NOT NULL,
                        harvest_id VARCHAR(100) NULL,
                        quantity INTEGER NOT NULL DEFAULT 0,
                        gold INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        claimed_at TIMESTAMPTZ NULL,
                        UNIQUE (sale_id, receiver_user_id, delivery_kind),
                        CHECK (delivery_kind IN ('harvest', 'gold')),
                        CHECK (quantity >= 0),
                        CHECK (gold >= 0)
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_market_delivery_pending
                    ON harvest_market_deliveries (receiver_user_id, claimed_at, delivery_id)
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


def run_one_time_account_cleanup() -> None:
    """운영자 요청으로 특정 기존 테스트 계정을 정확히 1회만 삭제합니다.

    안전장치:
    - 정원번호 806956
    - 닉네임 개발자(PC)
    - Lv.50
    세 조건이 모두 맞을 때만 삭제합니다.
    삭제 완료 사실을 migration 테이블에 기록하므로 같은 번호가 미래에 재사용돼도
    다시 삭제되지 않습니다.
    """
    if not DATABASE_URL:
        return

    migration_id = "delete_legacy_dev_pc_806956_20260916"
    target_garden_number = "806956"
    target_nickname = "개발자(PC)"
    target_level = 50

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS server_admin_migrations (
                        migration_id VARCHAR(120) PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    "SELECT 1 FROM server_admin_migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                if cur.fetchone() is not None:
                    return

                cur.execute(
                    """
                    SELECT user_id, nickname, level
                    FROM gardener_accounts
                    WHERE garden_number = %s
                    FOR UPDATE
                    """,
                    (target_garden_number,),
                )
                row = cur.fetchone()
                if row is None:
                    print("[1회 계정정리] 정원번호 806956 계정을 찾지 못했습니다.")
                    return

                actual_user_id = str(row[0])
                actual_nickname = str(row[1])
                actual_level = int(row[2])
                if actual_nickname != target_nickname or actual_level != target_level:
                    print(
                        "[1회 계정정리] 안전조건 불일치로 삭제하지 않았습니다: "
                        f"nickname={actual_nickname!r}, level={actual_level}"
                    )
                    return

                # gardener_accounts를 참조하는 친구관계/친구요청/꽃밭/선물/이전백업/
                # 함께하는정원 사용자별 기록은 FK ON DELETE CASCADE로 함께 정리됩니다.
                cur.execute(
                    "DELETE FROM gardener_accounts WHERE user_id = %s",
                    (actual_user_id,),
                )
                cur.execute(
                    "INSERT INTO server_admin_migrations (migration_id) VALUES (%s)",
                    (migration_id,),
                )
            conn.commit()
        print(
            "[1회 계정정리] 기존 계정 삭제 완료: "
            "개발자(PC) / Lv.50 / 정원번호 806956"
        )
    except Exception as exc:
        print(
            "[1회 계정정리 오류] "
            f"{type(exc).__name__}: {exc}"
        )




def run_one_time_hera_account_cleanup() -> None:
    """현재 DB에 남아 있는 테스트 닉네임 '헤라' 계정을 전부 정확히 1회만 삭제합니다.

    - 닉네임이 정확히 '헤라'인 현재 계정만 대상입니다.
    - 연결된 친구/요청/방명록/꽃밭/선물/이전백업/함께하는정원 기록은
      기존 FK ON DELETE CASCADE 규칙에 따라 함께 정리됩니다.
    - 완료 사실을 server_admin_migrations에 기록하므로,
      이후 새로 만들어지는 '헤라' 계정은 다시 자동 삭제되지 않습니다.
    """
    if not DATABASE_URL:
        return

    migration_id = "delete_all_test_hera_accounts_20260918"
    target_nickname = "헤라"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS server_admin_migrations (
                        migration_id VARCHAR(120) PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    "SELECT 1 FROM server_admin_migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                if cur.fetchone() is not None:
                    return

                cur.execute(
                    """
                    DELETE FROM gardener_accounts
                    WHERE nickname = %s
                    RETURNING user_id, garden_number, level
                    """,
                    (target_nickname,),
                )
                deleted_rows = cur.fetchall()

                # 삭제 대상이 0명이어도 이번 1회 정리 작업은 완료 처리합니다.
                # 따라서 이후 새로 생성되는 정상 '헤라' 계정은 삭제되지 않습니다.
                cur.execute(
                    "INSERT INTO server_admin_migrations (migration_id) VALUES (%s)",
                    (migration_id,),
                )

            conn.commit()

        if deleted_rows:
            print(
                f"[1회 헤라 계정정리] 삭제 완료: {len(deleted_rows)}명"
            )
            for _user_id, garden_number, level in deleted_rows:
                print(
                    "[1회 헤라 계정정리] "
                    f"헤라 / Lv.{int(level)} / 정원번호 {str(garden_number).strip()}"
                )
        else:
            print("[1회 헤라 계정정리] 삭제할 '헤라' 계정이 없습니다.")

    except Exception as exc:
        print(
            "[1회 헤라 계정정리 오류] "
            f"{type(exc).__name__}: {exc}"
        )

def run_one_time_together_garden_reward_recovery() -> None:
    """기존 1,000송이 목표에서 이미 통과한 10%/25% 미지급 보상을 1회 복구합니다."""
    if not DATABASE_URL:
        return

    migration_id = "together_garden_reward_recovery_10_25_20260916"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS server_admin_migrations (
                        migration_id VARCHAR(120) PRIMARY KEY,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    "SELECT 1 FROM server_admin_migrations WHERE migration_id = %s",
                    (migration_id,),
                )
                if cur.fetchone() is not None:
                    return

                for milestone in (10, 25):
                    cur.execute(
                        """
                        INSERT INTO together_garden_reward_delivery (
                            event_id, user_id, milestone, status, created_at
                        )
                        SELECT %s, a.user_id, %s, 'pending', NOW()
                        FROM gardener_accounts a
                        ON CONFLICT (event_id, user_id, milestone) DO NOTHING
                        """,
                        (TOGETHER_GARDEN_EVENT_ID, milestone),
                    )

                cur.execute(
                    "INSERT INTO server_admin_migrations (migration_id) VALUES (%s)",
                    (migration_id,),
                )
            conn.commit()

        print("[함께하는 정원] 기존 10%/25% 미지급 보상 복구 완료")
    except Exception as exc:
        print(
            "[함께하는 정원 보상복구 오류] "
            f"{type(exc).__name__}: {exc}"
        )


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
    title_name=None,
):
    nickname = str(nickname).strip()
    user_id = str(user_id).strip()
    garden_number = str(garden_number).strip()
    if title_name is not None:
        title_name = str(title_name).strip()
        if not title_name:
            title_name = None
        else:
            title_name = title_name[:40]

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
                    if title_name is None:
                        # 구버전/채팅 join처럼 칭호를 보내지 않는 등록은
                        # 이미 저장된 실제 칭호를 절대 덮어쓰지 않습니다.
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
                            UPDATE gardener_accounts
                            SET nickname = %s,
                                level = %s,
                                title_name = %s,
                                title_synced = TRUE,
                                updated_at = NOW()
                            WHERE user_id = %s
                            """,
                            (nickname, level, title_name, user_id),
                        )
                else:
                    cur.execute(
                        """
                        INSERT INTO gardener_accounts (
                            user_id,
                            nickname,
                            garden_number,
                            level,
                            title_name,
                            title_synced,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            user_id,
                            nickname,
                            garden_number,
                            level,
                            title_name or "초보 정원사",
                            title_name is not None,
                        ),
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


def _ensure_together_garden_reward_rows(cur, user_id: str, total_flowers: int, my_contribution: int):
    # 공동 달성 보상은 개인 기여량과 관계없이 등록 정원사에게 지급합니다.
    _ = my_contribution

    for milestone in TOGETHER_GARDEN_REWARD_MILESTONES:
        threshold = (TOGETHER_GARDEN_TARGET_FLOWERS * milestone + 99) // 100
        if total_flowers < threshold:
            continue
        cur.execute(
            """
            INSERT INTO together_garden_reward_delivery (
                event_id, user_id, milestone, status, created_at
            ) VALUES (%s, %s, %s, 'pending', NOW())
            ON CONFLICT (event_id, user_id, milestone) DO NOTHING
            """,
            (TOGETHER_GARDEN_EVENT_ID, user_id, milestone),
        )


def get_together_garden_state(user_id: str):
    if not DATABASE_URL:
        return None, "account_db_unavailable"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT total_flowers, target_flowers
                    FROM together_garden_events
                    WHERE event_id = %s
                    """,
                    (TOGETHER_GARDEN_EVENT_ID,),
                )
                row = cur.fetchone()
                if not row:
                    return None, "event_not_found"

                total_flowers = max(0, int(row[0]))
                target_flowers = max(1, int(row[1]))

                cur.execute(
                    """
                    SELECT flower_count
                    FROM together_garden_contributions
                    WHERE event_id = %s AND user_id = %s
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, user_id),
                )
                contribution_row = cur.fetchone()
                my_contribution = (
                    max(0, int(contribution_row[0]))
                    if contribution_row
                    else 0
                )

                _ensure_together_garden_reward_rows(
                    cur,
                    user_id,
                    total_flowers,
                    my_contribution,
                )

                cur.execute(
                    """
                    SELECT milestone
                    FROM together_garden_reward_delivery
                    WHERE event_id = %s
                      AND user_id = %s
                      AND status = 'pending'
                    ORDER BY milestone ASC
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, user_id),
                )
                pending_rewards = [int(item[0]) for item in cur.fetchall()]
            conn.commit()

        return {
            "event_id": TOGETHER_GARDEN_EVENT_ID,
            "total_flowers": min(total_flowers, target_flowers),
            "target_flowers": target_flowers,
            "my_contribution": my_contribution,
            "pending_rewards": pending_rewards,
            "complete": total_flowers >= target_flowers,
        }, ""
    except Exception as exc:
        print(
            "[함께하는 정원 조회 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return None, "together_garden_db_error"


def add_together_garden_harvest(user_id: str, action_id: str, flower_count: int):
    action_id = str(action_id).strip()
    if not action_id or len(action_id) > 180:
        return None, "invalid_action_id"

    try:
        flower_count = int(flower_count)
    except (TypeError, ValueError):
        return None, "invalid_flower_count"

    if flower_count <= 0 or flower_count > TOGETHER_GARDEN_MAX_HARVEST_PER_ACTION:
        return None, "invalid_flower_count"

    if not DATABASE_URL:
        return None, "account_db_unavailable"

    try:
        duplicate = False
        accepted_count = 0

        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT accepted_count
                    FROM together_garden_harvest_actions
                    WHERE event_id = %s
                      AND user_id = %s
                      AND action_id = %s
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, user_id, action_id),
                )
                existing = cur.fetchone()

                if existing:
                    duplicate = True
                    accepted_count = max(0, int(existing[0]))
                else:
                    cur.execute(
                        """
                        SELECT total_flowers, target_flowers
                        FROM together_garden_events
                        WHERE event_id = %s
                        FOR UPDATE
                        """,
                        (TOGETHER_GARDEN_EVENT_ID,),
                    )
                    event_row = cur.fetchone()
                    if not event_row:
                        return None, "event_not_found"

                    current_total = max(0, int(event_row[0]))
                    target_flowers = max(1, int(event_row[1]))
                    remaining = max(0, target_flowers - current_total)
                    accepted_count = min(flower_count, remaining)

                    cur.execute(
                        """
                        INSERT INTO together_garden_harvest_actions (
                            event_id,
                            user_id,
                            action_id,
                            requested_count,
                            accepted_count,
                            created_at
                        ) VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            TOGETHER_GARDEN_EVENT_ID,
                            user_id,
                            action_id,
                            flower_count,
                            accepted_count,
                        ),
                    )

                    if accepted_count > 0:
                        cur.execute(
                            """
                            UPDATE together_garden_events
                            SET total_flowers = LEAST(target_flowers, total_flowers + %s),
                                updated_at = NOW()
                            WHERE event_id = %s
                            """,
                            (accepted_count, TOGETHER_GARDEN_EVENT_ID),
                        )
                        cur.execute(
                            """
                            INSERT INTO together_garden_contributions (
                                event_id, user_id, flower_count, updated_at
                            ) VALUES (%s, %s, %s, NOW())
                            ON CONFLICT (event_id, user_id) DO UPDATE
                            SET flower_count = together_garden_contributions.flower_count + EXCLUDED.flower_count,
                                updated_at = NOW()
                            """,
                            (
                                TOGETHER_GARDEN_EVENT_ID,
                                user_id,
                                accepted_count,
                            ),
                        )

                cur.execute(
                    """
                    SELECT total_flowers, target_flowers
                    FROM together_garden_events
                    WHERE event_id = %s
                    """,
                    (TOGETHER_GARDEN_EVENT_ID,),
                )
                total_row = cur.fetchone()
                total_flowers = max(0, int(total_row[0]))
                target_flowers = max(1, int(total_row[1]))

                cur.execute(
                    """
                    SELECT flower_count
                    FROM together_garden_contributions
                    WHERE event_id = %s AND user_id = %s
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, user_id),
                )
                contribution_row = cur.fetchone()
                my_contribution = (
                    max(0, int(contribution_row[0]))
                    if contribution_row
                    else 0
                )

                _ensure_together_garden_reward_rows(
                    cur,
                    user_id,
                    total_flowers,
                    my_contribution,
                )

                cur.execute(
                    """
                    SELECT milestone
                    FROM together_garden_reward_delivery
                    WHERE event_id = %s
                      AND user_id = %s
                      AND status = 'pending'
                    ORDER BY milestone ASC
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, user_id),
                )
                pending_rewards = [int(item[0]) for item in cur.fetchall()]
            conn.commit()

        return {
            "event_id": TOGETHER_GARDEN_EVENT_ID,
            "action_id": action_id,
            "duplicate": duplicate,
            "accepted_count": accepted_count,
            "total_flowers": min(total_flowers, target_flowers),
            "target_flowers": target_flowers,
            "my_contribution": my_contribution,
            "pending_rewards": pending_rewards,
            "complete": total_flowers >= target_flowers,
        }, ""
    except Exception as exc:
        print(
            "[함께하는 정원 수확 저장 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return None, "together_garden_db_error"


def acknowledge_together_garden_reward(user_id: str, milestone: int):
    try:
        milestone = int(milestone)
    except (TypeError, ValueError):
        return False, "invalid_milestone"

    if milestone not in TOGETHER_GARDEN_REWARD_MILESTONES:
        return False, "invalid_milestone"
    if not DATABASE_URL:
        return False, "account_db_unavailable"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE together_garden_reward_delivery
                    SET status = 'delivered',
                        delivered_at = COALESCE(delivered_at, NOW())
                    WHERE event_id = %s
                      AND user_id = %s
                      AND milestone = %s
                      AND status = 'pending'
                    """,
                    (TOGETHER_GARDEN_EVENT_ID, user_id, milestone),
                )
                updated = cur.rowcount > 0

                if not updated:
                    cur.execute(
                        """
                        SELECT status
                        FROM together_garden_reward_delivery
                        WHERE event_id = %s
                          AND user_id = %s
                          AND milestone = %s
                        """,
                        (TOGETHER_GARDEN_EVENT_ID, user_id, milestone),
                    )
                    row = cur.fetchone()
                    if row and str(row[0]) == "delivered":
                        conn.commit()
                        return True, "already_delivered"
            conn.commit()
        return updated, "ok" if updated else "reward_not_pending"
    except Exception as exc:
        print(
            "[함께하는 정원 보상 확인 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return False, "together_garden_db_error"


def get_friend_status(user_id: str, target_user_id: str) -> str:
    if not user_id or not target_user_id:
        return "none"
    if user_id == target_user_id:
        return "self"
    if not DATABASE_URL:
        return "none"

    user_a, user_b = sorted([user_id, target_user_id])
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM gardener_friendships
                    WHERE user_id_a = %s AND user_id_b = %s
                    """,
                    (user_a, user_b),
                )
                if cur.fetchone():
                    return "friends"

                cur.execute(
                    """
                    SELECT requester_user_id, target_user_id
                    FROM gardener_friend_requests
                    WHERE (requester_user_id = %s AND target_user_id = %s)
                       OR (requester_user_id = %s AND target_user_id = %s)
                    LIMIT 1
                    """,
                    (user_id, target_user_id, target_user_id, user_id),
                )
                row = cur.fetchone()
                if not row:
                    return "none"
                if str(row[0]) == user_id:
                    return "outgoing"
                return "incoming"
    except Exception as exc:
        print(f"[친구 상태 오류] {type(exc).__name__}: {exc}")
        return "none"


def create_friend_request(user_id: str, target_garden_number: str):
    target, lookup_error = lookup_account_record(target_garden_number)
    if lookup_error:
        return False, "account_db_error", "계정 저장소에 잠시 문제가 있어요.", "none"
    if not target:
        return False, "target_not_found", "해당 정원사를 찾을 수 없어요.", "none"

    target_user_id = str(target.get("user_id", ""))
    if target_user_id == user_id:
        return False, "self_request", "내 정원에는 친구 요청을 보낼 수 없어요.", "self"

    status = get_friend_status(user_id, target_user_id)
    if status == "friends":
        return True, "already_friends", "이미 친구인 정원사예요.", "friends"
    if status == "outgoing":
        return True, "already_requested", "이미 친구 요청을 보냈어요.", "outgoing"
    if status == "incoming":
        return False, "incoming_request", "상대 정원사가 이미 친구 요청을 보냈어요. 받은 요청에서 확인해주세요.", "incoming"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gardener_friend_requests (
                        requester_user_id,
                        target_user_id,
                        created_at
                    ) VALUES (%s, %s, NOW())
                    ON CONFLICT (requester_user_id, target_user_id) DO NOTHING
                    """,
                    (user_id, target_user_id),
                )
            conn.commit()
        return True, "requested", "친구 요청을 보냈어요.", "outgoing"
    except Exception as exc:
        print(f"[친구 요청 저장 오류] {type(exc).__name__}: {exc}")
        return False, "friend_db_error", "친구 요청을 저장하지 못했어요.", "none"


def get_friend_lists(user_id: str):
    friends = []
    incoming = []
    outgoing = []
    if not DATABASE_URL:
        return friends, incoming, outgoing, "account_db_unavailable"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        a.nickname,
                        a.garden_number,
                        a.level,
                        a.title_name,
                        a.title_synced
                    FROM gardener_friendships f
                    JOIN gardener_accounts a
                      ON a.user_id = CASE
                          WHEN f.user_id_a = %s THEN f.user_id_b
                          ELSE f.user_id_a
                      END
                    WHERE f.user_id_a = %s OR f.user_id_b = %s
                    ORDER BY LOWER(a.nickname), a.garden_number
                    """,
                    (user_id, user_id, user_id),
                )
                for row in cur.fetchall():
                    title_synced = bool(row[4])
                    friends.append({
                        "nickname": str(row[0]),
                        "garden_number": str(row[1]).strip(),
                        "level": int(row[2]),
                        "title_name": (
                            str(row[3] or "")
                            if title_synced
                            else ""
                        ),
                        "title_synced": title_synced,
                    })

                cur.execute(
                    """
                    SELECT a.nickname, a.garden_number, a.level
                    FROM gardener_friend_requests r
                    JOIN gardener_accounts a
                      ON a.user_id = r.requester_user_id
                    WHERE r.target_user_id = %s
                    ORDER BY r.created_at DESC
                    """,
                    (user_id,),
                )
                for row in cur.fetchall():
                    incoming.append({
                        "nickname": str(row[0]),
                        "garden_number": str(row[1]).strip(),
                        "level": int(row[2]),
                    })

                # 내가 보냈고 아직 상대가 수락/거절하지 않은 친구 요청입니다.
                cur.execute(
                    """
                    SELECT a.nickname, a.garden_number, a.level
                    FROM gardener_friend_requests r
                    JOIN gardener_accounts a
                      ON a.user_id = r.target_user_id
                    WHERE r.requester_user_id = %s
                    ORDER BY r.created_at DESC
                    """,
                    (user_id,),
                )
                for row in cur.fetchall():
                    outgoing.append({
                        "nickname": str(row[0]),
                        "garden_number": str(row[1]).strip(),
                        "level": int(row[2]),
                    })
        return friends, incoming, outgoing, ""
    except Exception as exc:
        print(f"[친구 목록 오류] {type(exc).__name__}: {exc}")
        return [], [], [], "friend_db_error"


def remove_friend(user_id: str, target_garden_number: str):
    target, lookup_error = lookup_account_record(target_garden_number)
    if lookup_error:
        return False, "account_db_error", "계정 저장소에 잠시 문제가 있어요."
    if not target:
        return False, "target_not_found", "친구 정보를 찾을 수 없어요."

    target_user_id = str(target.get("user_id", ""))
    if not target_user_id or target_user_id == user_id:
        return False, "invalid_target", "삭제할 친구를 확인할 수 없어요."

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM gardener_friendships
                    WHERE (user_id_a = %s AND user_id_b = %s)
                       OR (user_id_a = %s AND user_id_b = %s)
                    """,
                    (user_id, target_user_id, target_user_id, user_id),
                )
                if not cur.fetchone():
                    return False, "not_friends", "이미 친구목록에 없는 정원사예요."

                cur.execute(
                    """
                    DELETE FROM gardener_friendships
                    WHERE (user_id_a = %s AND user_id_b = %s)
                       OR (user_id_a = %s AND user_id_b = %s)
                    """,
                    (user_id, target_user_id, target_user_id, user_id),
                )

                # 삭제 후 서로 다시 친구신청할 수 있도록 혹시 남아 있는 대기 요청도 정리합니다.
                cur.execute(
                    """
                    DELETE FROM gardener_friend_requests
                    WHERE (requester_user_id = %s AND target_user_id = %s)
                       OR (requester_user_id = %s AND target_user_id = %s)
                    """,
                    (user_id, target_user_id, target_user_id, user_id),
                )
            conn.commit()
        return True, "removed", "친구를 삭제했어요."
    except Exception as exc:
        print(f"[친구 삭제 오류] {type(exc).__name__}: {exc}")
        return False, "friend_db_error", "친구를 삭제하지 못했어요. 잠시 후 다시 시도해주세요."

def get_friend_recommendations(user_id: str, limit: int = 8):
    if not DATABASE_URL:
        return [], "account_db_unavailable"

    try:
        limit = max(1, min(20, int(limit)))
    except (TypeError, ValueError):
        limit = 8

    try:
        recommendations = []
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT a.nickname,
                           a.garden_number,
                           a.level
                    FROM gardener_accounts a
                    WHERE a.user_id <> %s
                      AND NOT EXISTS (
                          SELECT 1
                          FROM gardener_friendships f
                          WHERE (f.user_id_a = %s AND f.user_id_b = a.user_id)
                             OR (f.user_id_b = %s AND f.user_id_a = a.user_id)
                      )
                      AND NOT EXISTS (
                          SELECT 1
                          FROM gardener_friend_requests r
                          WHERE (r.requester_user_id = %s AND r.target_user_id = a.user_id)
                             OR (r.requester_user_id = a.user_id AND r.target_user_id = %s)
                      )
                    ORDER BY RANDOM()
                    LIMIT %s
                    """,
                    (user_id, user_id, user_id, user_id, user_id, limit),
                )
                for row in cur.fetchall():
                    recommendations.append({
                        "nickname": str(row[0]),
                        "garden_number": str(row[1]).strip(),
                        "level": int(row[2]),
                    })
        return recommendations, ""
    except Exception as exc:
        print(f"[추천 정원사 오류] {type(exc).__name__}: {exc}")
        return [], "friend_db_error"


def respond_friend_request(user_id: str, requester_garden_number: str, accept: bool):
    requester, lookup_error = lookup_account_record(requester_garden_number)
    if lookup_error:
        return False, "account_db_error", "계정 저장소에 잠시 문제가 있어요."
    if not requester:
        return False, "requester_not_found", "친구 요청을 보낸 정원사를 찾을 수 없어요."

    requester_user_id = str(requester.get("user_id", ""))
    if requester_user_id == user_id:
        return False, "invalid_request", "잘못된 친구 요청이에요."

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM gardener_friend_requests
                    WHERE requester_user_id = %s AND target_user_id = %s
                    """,
                    (requester_user_id, user_id),
                )
                if not cur.fetchone():
                    return False, "request_not_found", "이미 처리되었거나 없는 친구 요청이에요."

                cur.execute(
                    """
                    DELETE FROM gardener_friend_requests
                    WHERE requester_user_id = %s AND target_user_id = %s
                    """,
                    (requester_user_id, user_id),
                )

                if accept:
                    user_a, user_b = sorted([user_id, requester_user_id])
                    cur.execute(
                        """
                        INSERT INTO gardener_friendships (
                            user_id_a, user_id_b, created_at
                        ) VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id_a, user_id_b) DO NOTHING
                        """,
                        (user_a, user_b),
                    )
                    # 양방향으로 남은 중복 요청이 있다면 정리합니다.
                    cur.execute(
                        """
                        DELETE FROM gardener_friend_requests
                        WHERE (requester_user_id = %s AND target_user_id = %s)
                           OR (requester_user_id = %s AND target_user_id = %s)
                        """,
                        (user_id, requester_user_id, requester_user_id, user_id),
                    )
            conn.commit()

        if accept:
            return True, "accepted", "친구가 되었어요!"
        return True, "declined", "친구 요청을 거절했어요."
    except Exception as exc:
        print(f"[친구 요청 처리 오류] {type(exc).__name__}: {exc}")
        return False, "friend_db_error", "친구 요청을 처리하지 못했어요."


# ==================================================
# 📖 방명록 1차 - Neon 영구저장
# 읽기는 모든 등록 정원사, 작성은 친구만 가능합니다.
# 작성자는 자기 글을 삭제할 수 있고, 방명록 주인은 자기 방명록의 모든 글을 삭제할 수 있습니다.
# ==================================================

GARDEN_AREA_IDS = ("center", "right", "left")

# 구버전 꽃밭 스냅샷에는 성장 완료시각/단계당 시간/bloom_id가 없을 수 있습니다.
# 현재 게임 밸런스의 총 성장시간 상한(60분)을 기준으로 보수적으로 복원합니다.
# 예: stage1 스냅샷은 마지막 서버 저장시각 + 최대 60분,
#     stage2는 +40분, stage3는 +20분 후 만개로 처리합니다.
LEGACY_GARDEN_MAX_TOTAL_GROWTH_SECONDS = 60.0 * 60.0
LEGACY_GARDEN_STAGE_SECONDS = LEGACY_GARDEN_MAX_TOTAL_GROWTH_SECONDS / 3.0


def _db_timestamp_to_unix(value, fallback: float | None = None) -> float:
    if fallback is None:
        fallback = time.time()
    if value is None:
        return float(fallback)
    if isinstance(value, datetime):
        try:
            return float(value.timestamp())
        except Exception:
            return float(fallback)
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return float(fallback)


def _make_legacy_bloom_id(
    owner_user_id: str,
    area_id: str,
    flower_id: str,
    snapshot_updated_at,
) -> str:
    """
    구버전 스냅샷용 bloom_id를 결정적으로 생성합니다.
    같은 DB 스냅샷을 여러 번 읽어도 같은 ID가 나오므로 서리 중복 판정이 안정적입니다.
    """
    base_unix = int(_db_timestamp_to_unix(snapshot_updated_at))
    raw = f"{owner_user_id}|{area_id}|{flower_id}|{base_unix}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()[:28]
    return f"legacy_{digest}"


def sanitize_garden_plots(raw_plots):
    if not isinstance(raw_plots, list):
        return None

    by_area = {}
    for raw in raw_plots[:6]:
        if not isinstance(raw, dict):
            continue

        area_id = str(raw.get("area_id", "")).strip()
        if area_id not in GARDEN_AREA_IDS:
            continue

        unlocked = bool(raw.get("unlocked", False))
        flower_id = str(raw.get("flower_id", "")).strip()[:100]
        # bloom_id는 성장 중에도 유지합니다.
        # 심어진 꽃 1회의 고유 ID이므로 주인이 오프라인인 사이 만개해도 같은 꽃으로 서리 판정합니다.
        bloom_id = str(raw.get("bloom_id", "")).strip()[:140]

        try:
            stage = max(0, min(4, int(raw.get("stage", 0))))
        except (TypeError, ValueError):
            stage = 0

        try:
            growth_finish_unix = max(0.0, float(raw.get("growth_finish_unix", 0.0)))
        except (TypeError, ValueError):
            growth_finish_unix = 0.0

        try:
            stage_duration_seconds = max(0.0, float(raw.get("stage_duration_seconds", 0.0)))
        except (TypeError, ValueError):
            stage_duration_seconds = 0.0

        fully_grown = bool(raw.get("fully_grown", False)) or stage >= 4

        if not unlocked:
            flower_id = ""
            bloom_id = ""
            stage = 0
            fully_grown = False
            growth_finish_unix = 0.0
            stage_duration_seconds = 0.0
        elif not flower_id or stage <= 0:
            flower_id = ""
            bloom_id = ""
            stage = 0
            fully_grown = False
            growth_finish_unix = 0.0
            stage_duration_seconds = 0.0
        elif not fully_grown:
            stage = max(1, min(3, stage))
        else:
            stage = 4
            fully_grown = True

        by_area[area_id] = {
            "area_id": area_id,
            "unlocked": unlocked,
            "flower_id": flower_id,
            "stage": stage,
            "fully_grown": fully_grown,
            "bloom_id": bloom_id,
            "growth_finish_unix": growth_finish_unix,
            "stage_duration_seconds": stage_duration_seconds,
        }

    result = []
    for area_id in GARDEN_AREA_IDS:
        if area_id in by_area:
            result.append(by_area[area_id])
        else:
            result.append({
                "area_id": area_id,
                "unlocked": area_id == "center",
                "flower_id": "",
                "stage": 0,
                "fully_grown": False,
                "bloom_id": "",
                "growth_finish_unix": 0.0,
                "stage_duration_seconds": 0.0,
            })
    return result


def upgrade_legacy_garden_plots(
    raw_plots,
    owner_user_id: str,
    snapshot_updated_at,
):
    """
    기존 사용자(구버전 APK)의 꽃밭 데이터를 서버에서 자동 보강합니다.

    - bloom_id가 없으면 서버가 안정적인 legacy ID 생성
    - 성장 완료시각이 없으면 마지막 스냅샷 저장시각 + 최대 잔여 성장시간으로 복원
    - stage_duration_seconds가 없으면 현재 전체 성장 상한 기준으로 복원

    이 함수 덕분에 기존 사용자가 새 APK를 한 번 실행하지 않아도,
    오래된 꽃은 시간이 충분히 지났다면 친구 꽃밭에서 만개/서리 가능 상태가 됩니다.
    """
    plots = sanitize_garden_plots(raw_plots)
    if plots is None:
        return None

    base_unix = _db_timestamp_to_unix(snapshot_updated_at)
    upgraded = []

    for raw in plots:
        item = dict(raw)
        if (
            bool(item.get("unlocked", False))
            and str(item.get("flower_id", ""))
            and int(item.get("stage", 0)) > 0
        ):
            area_id = str(item.get("area_id", ""))
            flower_id = str(item.get("flower_id", ""))
            stage = max(1, min(4, int(item.get("stage", 1))))
            fully_grown = bool(item.get("fully_grown", False)) or stage >= 4
            finish_unix = float(item.get("growth_finish_unix", 0.0) or 0.0)
            stage_seconds = float(item.get("stage_duration_seconds", 0.0) or 0.0)

            if not str(item.get("bloom_id", "")):
                item["bloom_id"] = _make_legacy_bloom_id(
                    owner_user_id,
                    area_id,
                    flower_id,
                    snapshot_updated_at,
                )

            if stage_seconds <= 0.0:
                item["stage_duration_seconds"] = LEGACY_GARDEN_STAGE_SECONDS
                stage_seconds = LEGACY_GARDEN_STAGE_SECONDS

            if fully_grown:
                item["stage"] = 4
                item["fully_grown"] = True
                if finish_unix <= 0.0:
                    item["growth_finish_unix"] = base_unix
            elif finish_unix <= 0.0:
                # stage1→만개 최대 3단계, stage2→2단계, stage3→1단계가 남아 있습니다.
                remaining_stages = max(1, 4 - stage)
                item["growth_finish_unix"] = (
                    base_unix
                    + LEGACY_GARDEN_STAGE_SECONDS * float(remaining_stages)
                )

        upgraded.append(item)

    return upgraded


def resolve_garden_growth(raw_plots):
    """
    저장된 완료 Unix 시각을 기준으로 친구 꽃밭의 현재 성장단계를 서버에서 계산합니다.
    주인이 게임을 꺼둔 상태여도 친구가 열어보는 순간 stage1~4/만개 여부가 최신 상태가 됩니다.
    """
    plots = sanitize_garden_plots(raw_plots) or []
    now_unix = time.time()
    resolved = []

    for raw in plots:
        item = dict(raw)
        unlocked = bool(item.get("unlocked", False))
        flower_id = str(item.get("flower_id", ""))
        fully_grown = bool(item.get("fully_grown", False))
        stage = int(item.get("stage", 0))
        finish_unix = float(item.get("growth_finish_unix", 0.0) or 0.0)
        stage_seconds = float(item.get("stage_duration_seconds", 0.0) or 0.0)

        if unlocked and flower_id and stage > 0 and not fully_grown and finish_unix > 0.0:
            remaining = finish_unix - now_unix
            if remaining <= 0.0:
                fully_grown = True
                stage = 4
            elif stage_seconds > 0.0:
                if remaining > stage_seconds * 2.0:
                    stage = 1
                elif remaining > stage_seconds:
                    stage = 2
                else:
                    stage = 3

        if fully_grown:
            stage = 4

        item["stage"] = max(0, min(4, stage))
        item["fully_grown"] = fully_grown
        resolved.append(item)

    return resolved


def _plots_by_area(plots):
    return {
        str(item.get("area_id", "")): item
        for item in (plots or [])
        if isinstance(item, dict)
    }


def merge_garden_snapshot_for_save(
    incoming_plots,
    existing_plots,
    owner_user_id: str,
    existing_updated_at,
):
    """
    구버전 클라이언트가 bloom/timing 필드 없이 다시 동기화해도,
    서버가 이미 보강한 필드를 같은 꽃/같은 단계에는 유지합니다.

    새 버전 클라이언트가 완전한 필드를 보내는 경우에는 그대로 신뢰합니다.
    """
    incoming = sanitize_garden_plots(incoming_plots)
    if incoming is None:
        return None

    existing = upgrade_legacy_garden_plots(
        existing_plots,
        owner_user_id,
        existing_updated_at,
    ) if existing_plots is not None else sanitize_garden_plots([])
    existing_map = _plots_by_area(existing)

    merged = []
    now_dt = datetime.now(timezone.utc)

    for raw in incoming:
        item = dict(raw)
        area_id = str(item.get("area_id", ""))
        prev = existing_map.get(area_id)

        is_planted = (
            bool(item.get("unlocked", False))
            and bool(item.get("flower_id", ""))
            and int(item.get("stage", 0)) > 0
        )

        # 빈 화단/잠금 화단은 기존 꽃 메타데이터를 이어받지 않습니다.
        if not is_planted:
            merged.append(item)
            continue

        incoming_complete = (
            bool(item.get("bloom_id", ""))
            and float(item.get("growth_finish_unix", 0.0) or 0.0) > 0.0
            and float(item.get("stage_duration_seconds", 0.0) or 0.0) > 0.0
        )

        if incoming_complete:
            merged.append(item)
            continue

        same_legacy_plant = False
        if prev:
            same_legacy_plant = (
                str(prev.get("flower_id", "")) == str(item.get("flower_id", ""))
                and int(prev.get("stage", 0)) == int(item.get("stage", 0))
                and bool(prev.get("fully_grown", False)) == bool(item.get("fully_grown", False))
            )

        if same_legacy_plant:
            if not str(item.get("bloom_id", "")):
                item["bloom_id"] = str(prev.get("bloom_id", ""))
            if float(item.get("growth_finish_unix", 0.0) or 0.0) <= 0.0:
                item["growth_finish_unix"] = float(prev.get("growth_finish_unix", 0.0) or 0.0)
            if float(item.get("stage_duration_seconds", 0.0) or 0.0) <= 0.0:
                item["stage_duration_seconds"] = float(prev.get("stage_duration_seconds", 0.0) or 0.0)

        # 여전히 비어 있는 legacy 필드는 루프가 끝난 뒤 3개 area 전체를 한 번에 보강합니다.
        merged.append(item)

    upgraded_merged = upgrade_legacy_garden_plots(
        merged,
        owner_user_id,
        now_dt,
    )
    return sanitize_garden_plots(upgraded_merged)


def save_garden_snapshot(user_id: str, raw_plots):
    if not DATABASE_URL:
        return False, "account_db_unavailable", "꽃밭 저장소가 아직 연결되지 않았어요."

    incoming = sanitize_garden_plots(raw_plots)
    if incoming is None:
        return False, "invalid_garden_snapshot", "꽃밭 정보를 확인할 수 없어요."

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT plots, updated_at
                    FROM gardener_garden_snapshots
                    WHERE owner_user_id = %s
                    FOR UPDATE
                    """,
                    (user_id,),
                )
                row = cur.fetchone()

                existing_plots = None
                existing_updated_at = datetime.now(timezone.utc)
                if row:
                    existing_plots = row[0]
                    if isinstance(existing_plots, str):
                        existing_plots = json.loads(existing_plots)
                    existing_updated_at = row[1]

                plots = merge_garden_snapshot_for_save(
                    incoming,
                    existing_plots,
                    user_id,
                    existing_updated_at,
                )
                if plots is None:
                    return False, "invalid_garden_snapshot", "꽃밭 정보를 확인할 수 없어요."

                plots_json = json.dumps(plots, ensure_ascii=False)
                cur.execute(
                    """
                    INSERT INTO gardener_garden_snapshots (
                        owner_user_id,
                        plots,
                        updated_at
                    )
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (owner_user_id)
                    DO UPDATE SET
                        plots = EXCLUDED.plots,
                        updated_at = NOW()
                    """,
                    (user_id, plots_json),
                )
            conn.commit()
        return True, "ok", "꽃밭 정보가 저장되었어요."
    except Exception as exc:
        print(f"[꽃밭 저장 오류] {type(exc).__name__}: {exc}")
        return False, "garden_db_error", "꽃밭 정보를 저장하지 못했어요."


def load_garden_snapshot(requester_user_id: str, owner_garden_number: str):
    owner_record, lookup_error = lookup_account_record(owner_garden_number)
    if lookup_error:
        return None, lookup_error
    if not owner_record:
        return None, "owner_not_found"

    owner_user_id = str(owner_record.get("user_id", ""))
    is_owner = requester_user_id == owner_user_id
    if not is_owner and get_friend_status(requester_user_id, owner_user_id) != "friends":
        return None, "friends_only"

    try:
        plots = []
        has_snapshot = False
        stolen_keys = set()

        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                # 기존 스냅샷의 legacy 필드를 이 요청에서 바로 보강하고 DB에 저장합니다.
                cur.execute(
                    """
                    SELECT plots, updated_at
                    FROM gardener_garden_snapshots
                    WHERE owner_user_id = %s
                    FOR UPDATE
                    """,
                    (owner_user_id,),
                )
                row = cur.fetchone()
                if row:
                    has_snapshot = True
                    raw_plots = row[0]
                    snapshot_updated_at = row[1]
                    if isinstance(raw_plots, str):
                        raw_plots = json.loads(raw_plots)

                    sanitized = sanitize_garden_plots(raw_plots)
                    upgraded = upgrade_legacy_garden_plots(
                        raw_plots,
                        owner_user_id,
                        snapshot_updated_at,
                    )
                    if upgraded is not None:
                        # bloom_id/완료시각만 보강해 영구 저장합니다.
                        # updated_at 자체는 바꾸지 않아 기존 꽃의 시간 기준점이 유지됩니다.
                        if sanitized != upgraded:
                            cur.execute(
                                """
                                UPDATE gardener_garden_snapshots
                                SET plots = %s::jsonb
                                WHERE owner_user_id = %s
                                """,
                                (json.dumps(upgraded, ensure_ascii=False), owner_user_id),
                            )
                        plots = resolve_garden_growth(upgraded)

                if not is_owner:
                    cur.execute(
                        """
                        SELECT area_id, bloom_id
                        FROM gardener_garden_steals
                        WHERE requester_user_id = %s
                          AND owner_user_id = %s
                        """,
                        (requester_user_id, owner_user_id),
                    )
                    stolen_keys = {
                        (str(r[0]), str(r[1]))
                        for r in cur.fetchall()
                    }
            conn.commit()

        if not plots:
            plots = sanitize_garden_plots([])

        result_plots = []
        for plot in plots:
            item = dict(plot)
            key = (
                str(item.get("area_id", "")),
                str(item.get("bloom_id", "")),
            )
            bloom_ready = (
                bool(item.get("unlocked", False))
                and bool(item.get("fully_grown", False))
                and int(item.get("stage", 0)) >= 4
                and bool(item.get("flower_id", ""))
                and bool(item.get("bloom_id", ""))
            )
            already_stolen = (not is_owner) and key in stolen_keys
            item["already_stolen"] = already_stolen
            item["can_steal"] = (
                not is_owner
                and bloom_ready
                and not already_stolen
            )
            result_plots.append(item)

        return {
            "owner_nickname": owner_record.get("nickname", "정원사"),
            "owner_garden_number": owner_garden_number,
            "owner_level": int(owner_record.get("level", 0)),
            "is_owner": is_owner,
            "has_snapshot": has_snapshot,
            "plots": result_plots,
        }, ""

    except Exception as exc:
        print(f"[꽃밭 불러오기 오류] {type(exc).__name__}: {exc}")
        return None, "garden_db_error"


def steal_garden_flower(
    requester_user_id: str,
    owner_garden_number: str,
    area_id: str,
    bloom_id: str,
):
    area_id = str(area_id).strip()
    bloom_id = str(bloom_id).strip()

    if area_id not in GARDEN_AREA_IDS or not bloom_id:
        return False, "invalid_steal_target", "서리할 꽃을 확인할 수 없어요.", ""

    owner_record, lookup_error = lookup_account_record(owner_garden_number)
    if lookup_error:
        return False, lookup_error, "꽃밭 저장소에 연결하지 못했어요.", ""
    if not owner_record:
        return False, "owner_not_found", "해당 정원사를 찾을 수 없어요.", ""

    owner_user_id = str(owner_record.get("user_id", ""))
    if requester_user_id == owner_user_id:
        return False, "cannot_steal_self", "내 꽃밭에서는 서리할 수 없어요.", ""

    if get_friend_status(requester_user_id, owner_user_id) != "friends":
        return False, "friends_only", "친구의 꽃밭에서만 서리할 수 있어요.", ""

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT plots, updated_at
                    FROM gardener_garden_snapshots
                    WHERE owner_user_id = %s
                    FOR UPDATE
                    """,
                    (owner_user_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False, "garden_not_synced", "친구 꽃밭 정보가 아직 없어요.", ""

                raw_plots = row[0]
                snapshot_updated_at = row[1]
                if isinstance(raw_plots, str):
                    raw_plots = json.loads(raw_plots)

                # 구버전 꽃밭도 여기서 bloom/timer를 보강한 뒤 실제 시각 기준으로 판정합니다.
                sanitized = sanitize_garden_plots(raw_plots)
                upgraded = upgrade_legacy_garden_plots(
                    raw_plots,
                    owner_user_id,
                    snapshot_updated_at,
                )
                if upgraded is None:
                    return False, "invalid_garden_snapshot", "친구 꽃밭 정보를 확인할 수 없어요.", ""
                if sanitized != upgraded:
                    cur.execute(
                        """
                        UPDATE gardener_garden_snapshots
                        SET plots = %s::jsonb
                        WHERE owner_user_id = %s
                        """,
                        (json.dumps(upgraded, ensure_ascii=False), owner_user_id),
                    )

                plots = resolve_garden_growth(upgraded)

                target = None
                for plot in plots:
                    if str(plot.get("area_id", "")) == area_id:
                        target = plot
                        break

                if not target:
                    return False, "plot_not_found", "해당 꽃밭을 찾을 수 없어요.", ""

                if (
                    not bool(target.get("unlocked", False))
                    or not bool(target.get("fully_grown", False))
                    or int(target.get("stage", 0)) < 4
                    or not str(target.get("flower_id", ""))
                ):
                    return False, "not_bloomed", "지금은 서리할 만개 꽃이 없어요.", ""

                current_bloom_id = str(target.get("bloom_id", ""))
                if not current_bloom_id or current_bloom_id != bloom_id:
                    return False, "bloom_changed", "꽃밭 상태가 바뀌었어요. 다시 확인해주세요.", ""

                flower_id = str(target.get("flower_id", ""))

                try:
                    cur.execute(
                        """
                        INSERT INTO gardener_garden_steals (
                            requester_user_id,
                            owner_user_id,
                            area_id,
                            bloom_id,
                            flower_id,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            requester_user_id,
                            owner_user_id,
                            area_id,
                            bloom_id,
                            flower_id,
                        ),
                    )
                except psycopg_errors.UniqueViolation:
                    conn.rollback()
                    return False, "already_stolen", "이 만개 꽃은 이미 서리했어요.", ""

            conn.commit()

        return True, "ok", "서리 성공! 창고에 꽃 1개가 추가돼요.", flower_id

    except Exception as exc:
        print(f"[꽃밭 서리 오류] {type(exc).__name__}: {exc}")
        return False, "garden_db_error", "서리하지 못했어요. 잠시 후 다시 시도해주세요.", ""


def is_valid_gift_flower_id(flower_id: str) -> bool:
    flower_id = str(flower_id).strip()
    if not flower_id or len(flower_id) > 100:
        return False
    return re.fullmatch(r"[A-Za-z0-9_-]+", flower_id) is not None


def send_flower_gift(
    sender_user_id: str,
    receiver_garden_number: str,
    flower_id: str,
):
    flower_id = str(flower_id).strip()
    if not is_valid_gift_flower_id(flower_id):
        return False, "invalid_flower", "선물할 꽃을 확인할 수 없어요.", 0, ""

    receiver, lookup_error = lookup_account_record(receiver_garden_number)
    if lookup_error:
        return False, lookup_error, "계정 저장소에 연결하지 못했어요.", 0, ""
    if not receiver:
        return False, "receiver_not_found", "해당 정원사를 찾을 수 없어요.", 0, ""

    receiver_user_id = str(receiver.get("user_id", ""))
    if sender_user_id == receiver_user_id:
        return False, "cannot_gift_self", "나에게는 꽃을 선물할 수 없어요.", 0, ""

    if get_friend_status(sender_user_id, receiver_user_id) != "friends":
        return False, "friends_only", "친구에게만 꽃을 선물할 수 있어요.", 0, ""

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gardener_flower_gifts (
                        sender_user_id,
                        receiver_user_id,
                        flower_id,
                        created_at
                    )
                    VALUES (%s, %s, %s, NOW())
                    RETURNING gift_id
                    """,
                    (sender_user_id, receiver_user_id, flower_id),
                )
                row = cur.fetchone()
                gift_id = int(row[0]) if row else 0
            conn.commit()

        return (
            True,
            "ok",
            "꽃 선물을 보냈어요.",
            gift_id,
            str(receiver.get("nickname", "정원사")),
        )
    except Exception as exc:
        print(f"[꽃 선물 보내기 오류] {type(exc).__name__}: {exc}")
        return False, "gift_db_error", "선물을 보내지 못했어요. 잠시 후 다시 시도해주세요.", 0, ""


def load_flower_gift_inbox(receiver_user_id: str):
    gifts = []
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT g.gift_id,
                           g.flower_id,
                           a.nickname,
                           a.garden_number,
                           g.created_at
                    FROM gardener_flower_gifts g
                    JOIN gardener_accounts a
                      ON a.user_id = g.sender_user_id
                    WHERE g.receiver_user_id = %s
                      AND g.claimed_at IS NULL
                    ORDER BY g.created_at DESC, g.gift_id DESC
                    LIMIT 100
                    """,
                    (receiver_user_id,),
                )
                for row in cur.fetchall():
                    gifts.append({
                        "gift_id": int(row[0]),
                        "flower_id": str(row[1]),
                        "sender_nickname": str(row[2]),
                        "sender_garden_number": str(row[3]).strip(),
                        "time_text": format_guestbook_time(row[4]),
                    })
        return gifts, ""
    except Exception as exc:
        print(f"[받은 꽃 선물 불러오기 오류] {type(exc).__name__}: {exc}")
        return [], "gift_db_error"


def load_flower_gift_history(receiver_user_id: str):
    gifts = []
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT g.gift_id,
                           g.flower_id,
                           a.nickname,
                           a.garden_number,
                           g.claimed_at
                    FROM gardener_flower_gifts g
                    JOIN gardener_accounts a
                      ON a.user_id = g.sender_user_id
                    WHERE g.receiver_user_id = %s
                      AND g.claimed_at IS NOT NULL
                    ORDER BY g.claimed_at DESC, g.gift_id DESC
                    LIMIT 100
                    """,
                    (receiver_user_id,),
                )
                for row in cur.fetchall():
                    gifts.append({
                        "gift_id": int(row[0]),
                        "flower_id": str(row[1]),
                        "sender_nickname": str(row[2]),
                        "sender_garden_number": str(row[3]).strip(),
                        "time_text": format_guestbook_time(row[4]),
                    })
        return gifts, ""
    except Exception as exc:
        print(f"[받은 꽃 선물 기록 불러오기 오류] {type(exc).__name__}: {exc}")
        return [], "gift_db_error"


def claim_flower_gift(receiver_user_id: str, gift_id: int):
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT g.receiver_user_id,
                           g.flower_id,
                           g.claimed_at,
                           a.nickname
                    FROM gardener_flower_gifts g
                    JOIN gardener_accounts a
                      ON a.user_id = g.sender_user_id
                    WHERE g.gift_id = %s
                    FOR UPDATE
                    """,
                    (gift_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False, "gift_not_found", "해당 선물을 찾을 수 없어요.", "", ""

                gift_receiver_user_id = str(row[0])
                flower_id = str(row[1])
                claimed_at = row[2]
                sender_nickname = str(row[3])

                if gift_receiver_user_id != receiver_user_id:
                    return False, "not_receiver", "내 선물만 받을 수 있어요.", "", ""

                if claimed_at is not None:
                    return False, "already_claimed", "이미 받은 선물이에요.", "", ""

                cur.execute(
                    """
                    UPDATE gardener_flower_gifts
                    SET claimed_at = NOW()
                    WHERE gift_id = %s
                    """,
                    (gift_id,),
                )
            conn.commit()

        return True, "ok", "선물을 받았어요.", flower_id, sender_nickname
    except Exception as exc:
        print(f"[꽃 선물 받기 오류] {type(exc).__name__}: {exc}")
        return False, "gift_db_error", "선물을 받지 못했어요. 잠시 후 다시 시도해주세요.", "", ""


def is_valid_transfer_code(value: str) -> bool:
    value = str(value).strip()
    return len(value) == TRANSFER_CODE_LENGTH and value.isdigit()


def create_transfer_backup(
    owner_user_id: str,
    garden_number: str,
    save_data,
):
    owner_user_id = str(owner_user_id).strip()
    garden_number = str(garden_number).strip()

    if not owner_user_id or not is_valid_garden_number(garden_number):
        return False, "invalid_account", "정원사 정보를 확인할 수 없어요.", ""

    if not isinstance(save_data, dict):
        return False, "invalid_save_data", "게임 저장정보를 확인할 수 없어요.", ""

    saved_user_id = str(save_data.get("gardener_plaza_user_id", "")).strip()
    saved_garden_number = str(
        save_data.get("gardener_account_garden_number", "")
    ).strip()
    if saved_user_id != owner_user_id or saved_garden_number != garden_number:
        return False, "save_owner_mismatch", "현재 정원사와 저장정보가 일치하지 않아요.", ""

    try:
        save_json = json.dumps(save_data, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return False, "invalid_save_data", "게임 저장정보를 읽지 못했어요.", ""

    if len(save_json.encode("utf-8")) > TRANSFER_SAVE_MAX_BYTES:
        return False, "save_too_large", "게임 저장정보가 너무 커서 이전코드를 만들지 못했어요.", ""

    if not DATABASE_URL:
        return False, "account_db_unavailable", "데이터 이전 서버가 연결되지 않았어요.", ""

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM gardener_transfer_backups
                    WHERE expires_at <= NOW()
                       OR used_at IS NOT NULL
                    """
                )

                transfer_code = ""
                for _ in range(20):
                    candidate = str(
                        secrets.randbelow(10 ** TRANSFER_CODE_LENGTH)
                    ).zfill(TRANSFER_CODE_LENGTH)
                    cur.execute(
                        """
                        SELECT 1
                        FROM gardener_transfer_backups
                        WHERE transfer_code = %s
                        """,
                        (candidate,),
                    )
                    if cur.fetchone() is None:
                        transfer_code = candidate
                        break

                if not transfer_code:
                    return False, "code_generation_failed", "이전코드를 만들지 못했어요. 다시 시도해주세요.", ""

                cur.execute(
                    """
                    INSERT INTO gardener_transfer_backups (
                        transfer_code,
                        owner_user_id,
                        garden_number,
                        save_data,
                        created_at,
                        expires_at,
                        used_at
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s::jsonb,
                        NOW(),
                        NOW() + (%s * INTERVAL '1 day'),
                        NULL
                    )
                    """,
                    (
                        transfer_code,
                        owner_user_id,
                        garden_number,
                        save_json,
                        TRANSFER_BACKUP_RETENTION_DAYS,
                    ),
                )
            conn.commit()

        return True, "ok", "정식판 이전코드가 만들어졌어요.", transfer_code
    except Exception as exc:
        print(f"[데이터 이전 백업 오류] {type(exc).__name__}: {exc}")
        return False, "transfer_db_error", "이전코드를 만들지 못했어요. 잠시 후 다시 시도해주세요.", ""


def load_transfer_backup(transfer_code: str):
    transfer_code = str(transfer_code).strip()
    if not is_valid_transfer_code(transfer_code):
        return None, "invalid_transfer_code"
    if not DATABASE_URL:
        return None, "account_db_unavailable"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT b.owner_user_id,
                           b.garden_number,
                           b.save_data,
                           a.nickname
                    FROM gardener_transfer_backups b
                    JOIN gardener_accounts a
                      ON a.user_id = b.owner_user_id
                    WHERE b.transfer_code = %s
                      AND b.used_at IS NULL
                      AND b.expires_at > NOW()
                    """,
                    (transfer_code,),
                )
                row = cur.fetchone()

        if not row:
            return None, "transfer_not_found"

        save_data = row[2]
        if isinstance(save_data, str):
            save_data = json.loads(save_data)
        if not isinstance(save_data, dict):
            return None, "invalid_save_data"

        return {
            "owner_user_id": str(row[0]),
            "garden_number": str(row[1]).strip(),
            "save_data": save_data,
            "nickname": str(row[3]),
        }, ""
    except Exception as exc:
        print(f"[데이터 이전 불러오기 오류] {type(exc).__name__}: {exc}")
        return None, "transfer_db_error"


def complete_transfer_backup(transfer_code: str):
    transfer_code = str(transfer_code).strip()
    if not is_valid_transfer_code(transfer_code):
        return False

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gardener_transfer_backups
                    SET used_at = NOW()
                    WHERE transfer_code = %s
                      AND used_at IS NULL
                      AND expires_at > NOW()
                    """,
                    (transfer_code,),
                )
                changed = cur.rowcount > 0
            conn.commit()
        return changed
    except Exception as exc:
        print(f"[데이터 이전 완료처리 오류] {type(exc).__name__}: {exc}")
        return False


def format_guestbook_time(value) -> str:
    try:
        dt = value.astimezone(KST)
        period = "오전" if dt.hour < 12 else "오후"
        hour = dt.hour % 12
        if hour == 0:
            hour = 12
        return f"{dt.month}월 {dt.day}일 {period} {hour}:{dt.minute:02d}"
    except Exception:
        return ""


def load_guestbook(user_id: str, owner_garden_number: str):
    owner, lookup_error = lookup_account_record(owner_garden_number)
    if lookup_error:
        return None, "account_db_error"
    if not owner:
        return None, "owner_not_found"

    owner_user_id = str(owner.get("user_id", ""))
    can_write = get_friend_status(user_id, owner_user_id) == "friends"
    is_owner = user_id == owner_user_id
    entries = []

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT g.entry_id,
                           g.author_user_id,
                           a.nickname,
                           a.garden_number,
                           g.message,
                           g.created_at
                    FROM gardener_guestbook_entries g
                    JOIN gardener_accounts a
                      ON a.user_id = g.author_user_id
                    WHERE g.owner_user_id = %s
                    ORDER BY g.created_at DESC, g.entry_id DESC
                    LIMIT 50
                    """,
                    (owner_user_id,),
                )
                for row in cur.fetchall():
                    author_user_id = str(row[1])
                    entries.append({
                        "entry_id": int(row[0]),
                        "author_nickname": str(row[2]),
                        "author_garden_number": str(row[3]).strip(),
                        "message": str(row[4]),
                        "time_text": format_guestbook_time(row[5]),
                        "can_delete": (
                            user_id == author_user_id
                            or user_id == owner_user_id
                        ),
                    })
    except Exception as exc:
        print(f"[방명록 불러오기 오류] {type(exc).__name__}: {exc}")
        return None, "guestbook_db_error"

    return {
        "owner_nickname": str(owner.get("nickname", "정원사")),
        "owner_garden_number": owner_garden_number,
        "can_write": can_write,
        "is_owner": is_owner,
        "entries": entries,
    }, ""


def post_guestbook_entry(user_id: str, owner_garden_number: str, message: str):
    owner, lookup_error = lookup_account_record(owner_garden_number)
    if lookup_error:
        return False, "account_db_error", "계정 저장소에 잠시 문제가 있어요."
    if not owner:
        return False, "owner_not_found", "해당 정원사를 찾을 수 없어요."

    owner_user_id = str(owner.get("user_id", ""))
    if owner_user_id == user_id:
        return False, "self_write", "내 방명록에는 직접 글을 남길 수 없어요."

    if get_friend_status(user_id, owner_user_id) != "friends":
        return False, "friends_only", "친구에게만 방명록을 남길 수 있어요."

    clean_message = str(message).strip()
    if not clean_message:
        return False, "empty_message", "방명록 내용을 입력해주세요."
    if len(clean_message) > 60:
        return False, "message_too_long", "방명록은 60자까지 남길 수 있어요."
    if "\n" in clean_message or "\r" in clean_message:
        clean_message = " ".join(clean_message.splitlines()).strip()
    if contains_blocked_word(clean_message):
        return False, "blocked_word", "남길 수 없는 표현이 포함되어 있어요."

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gardener_guestbook_entries (
                        owner_user_id,
                        author_user_id,
                        message,
                        created_at
                    ) VALUES (%s, %s, %s, NOW())
                    """,
                    (owner_user_id, user_id, clean_message),
                )
            conn.commit()
        return True, "posted", "방명록을 남겼어요."
    except Exception as exc:
        print(f"[방명록 저장 오류] {type(exc).__name__}: {exc}")
        return False, "guestbook_db_error", "방명록을 저장하지 못했어요."


def delete_guestbook_entry(user_id: str, entry_id: int):
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT owner_user_id, author_user_id
                    FROM gardener_guestbook_entries
                    WHERE entry_id = %s
                    """,
                    (entry_id,),
                )
                row = cur.fetchone()
                if not row:
                    return False, "entry_not_found", "이미 삭제되었거나 없는 글이에요."

                owner_user_id = str(row[0])
                author_user_id = str(row[1])
                if user_id not in (owner_user_id, author_user_id):
                    return False, "delete_not_allowed", "이 글을 삭제할 수 없어요."

                cur.execute(
                    "DELETE FROM gardener_guestbook_entries WHERE entry_id = %s",
                    (entry_id,),
                )
            conn.commit()
        return True, "deleted", "방명록 글을 삭제했어요."
    except Exception as exc:
        print(f"[방명록 삭제 오류] {type(exc).__name__}: {exc}")
        return False, "guestbook_db_error", "방명록 글을 삭제하지 못했어요."


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



def normalize_admin_reward_payload(payload: dict):
    """운영자 발송 수량을 정수/허용범위로 안전하게 정리합니다."""
    def read_int(key: str, maximum: int) -> int:
        try:
            value = int(payload.get(key, 0))
        except (TypeError, ValueError):
            value = 0
        return max(0, min(maximum, value))

    rewards = {
        "king_water_drops": read_int(
            "king_water_drops", ADMIN_REWARD_MAX_ITEM_COUNT
        ),
        "water_drops": read_int(
            "water_drops", ADMIN_REWARD_MAX_ITEM_COUNT
        ),
        "gold": read_int("gold", ADMIN_REWARD_MAX_GOLD),
        "lottery_tickets": read_int(
            "lottery_tickets", ADMIN_REWARD_MAX_ITEM_COUNT
        ),
        "wait_passes": read_int(
            "wait_passes", ADMIN_REWARD_MAX_ITEM_COUNT
        ),
        "random_seed_coupons": read_int(
            "random_seed_coupons", ADMIN_REWARD_MAX_ITEM_COUNT
        ),
        "choice_seed_coupons": read_int(
            "choice_seed_coupons", ADMIN_REWARD_MAX_ITEM_COUNT
        ),
    }
    return rewards


def admin_reward_has_value(rewards: dict) -> bool:
    return any(int(value) > 0 for value in rewards.values())


def search_admin_accounts(query: str, limit: int = 100):
    query = str(query).strip()
    limit = max(1, min(100, int(limit)))

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                if query:
                    like_query = "%" + query + "%"
                    cur.execute(
                        """
                        SELECT user_id, nickname, garden_number, level, updated_at
                        FROM gardener_accounts
                        WHERE nickname ILIKE %s
                           OR garden_number LIKE %s
                        ORDER BY
                            CASE WHEN garden_number = %s THEN 0 ELSE 1 END,
                            LOWER(nickname),
                            garden_number
                        LIMIT %s
                        """,
                        (like_query, like_query, query, limit),
                    )
                else:
                    cur.execute(
                        """
                        SELECT user_id, nickname, garden_number, level, updated_at
                        FROM gardener_accounts
                        ORDER BY updated_at DESC, garden_number
                        LIMIT %s
                        """,
                        (limit,),
                    )

                users = []
                for row in cur.fetchall():
                    users.append({
                        "user_id": str(row[0]),
                        "nickname": str(row[1]),
                        "garden_number": str(row[2]).strip(),
                        "level": int(row[3]),
                        "is_operator": (
                            str(row[2]).strip() == ADMIN_GARDEN_NUMBER
                        ),
                    })
        return users, ""
    except Exception as exc:
        print(
            "[운영자 유저검색 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return [], "admin_user_search_db_error"


def create_admin_reward_send(
    target_kind: str,
    target_user_id: str,
    rewards: dict,
    note: str,
):
    target_kind = str(target_kind).strip().lower()
    target_user_id = str(target_user_id).strip()
    note = str(note).strip()[:120]

    if target_kind not in ("all", "user"):
        return False, "invalid_target_kind", "발송 대상을 확인해주세요.", 0, 0, []

    if not admin_reward_has_value(rewards):
        return False, "empty_reward", "보상이 하나도 설정되지 않았어요.", 0, 0, []

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                target_garden_number = None
                target_nickname = None
                receiver_ids = []

                if target_kind == "user":
                    cur.execute(
                        """
                        SELECT user_id, nickname, garden_number
                        FROM gardener_accounts
                        WHERE user_id = %s
                        FOR UPDATE
                        """,
                        (target_user_id,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        return (
                            False,
                            "target_not_found",
                            "선택한 정원사 계정을 찾을 수 없어요.",
                            0,
                            0,
                            [],
                        )
                    target_user_id = str(row[0])
                    target_nickname = str(row[1])
                    target_garden_number = str(row[2]).strip()
                    receiver_ids = [target_user_id]
                else:
                    cur.execute(
                        """
                        SELECT user_id
                        FROM gardener_accounts
                        ORDER BY user_id
                        """
                    )
                    receiver_ids = [str(row[0]) for row in cur.fetchall()]

                if not receiver_ids:
                    return (
                        False,
                        "no_recipients",
                        "보상을 받을 등록 계정이 아직 없어요.",
                        0,
                        0,
                        [],
                    )

                cur.execute(
                    """
                    INSERT INTO admin_reward_sends (
                        target_kind,
                        target_user_id,
                        target_garden_number,
                        target_nickname,
                        gold,
                        water_drops,
                        king_water_drops,
                        lottery_tickets,
                        wait_passes,
                        random_seed_coupons,
                        choice_seed_coupons,
                        note,
                        recipient_count,
                        created_at
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s,
                        %s, %s, NOW()
                    )
                    RETURNING send_id
                    """,
                    (
                        target_kind,
                        target_user_id if target_kind == "user" else None,
                        target_garden_number,
                        target_nickname,
                        int(rewards["gold"]),
                        int(rewards["water_drops"]),
                        int(rewards["king_water_drops"]),
                        int(rewards["lottery_tickets"]),
                        int(rewards["wait_passes"]),
                        int(rewards["random_seed_coupons"]),
                        int(rewards["choice_seed_coupons"]),
                        note,
                        len(receiver_ids),
                    ),
                )
                send_row = cur.fetchone()
                send_id = int(send_row[0]) if send_row else 0

                cur.executemany(
                    """
                    INSERT INTO admin_reward_deliveries (
                        send_id,
                        receiver_user_id,
                        created_at
                    )
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (send_id, receiver_user_id) DO NOTHING
                    """,
                    [(send_id, user_id) for user_id in receiver_ids],
                )
            conn.commit()

        print(
            "[운영자 보상 발송] "
            f"send_id={send_id} / 대상={target_kind} / "
            f"수신 {len(receiver_ids)}명 / {rewards}"
        )
        return (
            True,
            "ok",
            "운영자 보상을 발송했습니다.",
            send_id,
            len(receiver_ids),
            receiver_ids,
        )
    except Exception as exc:
        print(
            "[운영자 보상 발송 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return (
            False,
            "admin_reward_db_error",
            "보상을 발송하지 못했어요. 잠시 후 다시 시도해주세요.",
            0,
            0,
            [],
        )


def load_admin_reward_history(limit: int = 100):
    limit = max(1, min(100, int(limit)))
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        s.send_id,
                        s.target_kind,
                        s.target_garden_number,
                        s.target_nickname,
                        s.gold,
                        s.water_drops,
                        s.king_water_drops,
                        s.lottery_tickets,
                        s.wait_passes,
                        s.random_seed_coupons,
                        s.choice_seed_coupons,
                        s.note,
                        s.recipient_count,
                        s.created_at,
                        COUNT(d.delivery_id) FILTER (
                            WHERE d.delivered_at IS NOT NULL
                        ) AS delivered_count
                    FROM admin_reward_sends s
                    LEFT JOIN admin_reward_deliveries d
                      ON d.send_id = s.send_id
                    GROUP BY s.send_id
                    ORDER BY s.created_at DESC, s.send_id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                items = []
                for row in cur.fetchall():
                    created_at = row[13]
                    items.append({
                        "send_id": int(row[0]),
                        "target_kind": str(row[1]),
                        "target_garden_number": (
                            str(row[2]).strip() if row[2] is not None else ""
                        ),
                        "target_nickname": (
                            str(row[3]) if row[3] is not None else ""
                        ),
                        "gold": int(row[4]),
                        "water_drops": int(row[5]),
                        "king_water_drops": int(row[6]),
                        "lottery_tickets": int(row[7]),
                        "wait_passes": int(row[8]),
                        "random_seed_coupons": int(row[9]),
                        "choice_seed_coupons": int(row[10]),
                        "note": str(row[11]),
                        "recipient_count": int(row[12]),
                        "time_text": format_guestbook_time(created_at),
                        "delivered_count": int(row[14] or 0),
                    })
        return items, ""
    except Exception as exc:
        print(
            "[운영자 발송기록 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return [], "admin_reward_history_db_error"


def load_operator_reward_inbox(receiver_user_id: str):
    rewards = []
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        d.delivery_id,
                        s.send_id,
                        s.gold,
                        s.water_drops,
                        s.king_water_drops,
                        s.lottery_tickets,
                        s.wait_passes,
                        s.random_seed_coupons,
                        s.choice_seed_coupons,
                        s.note,
                        s.created_at
                    FROM admin_reward_deliveries d
                    JOIN admin_reward_sends s
                      ON s.send_id = d.send_id
                    WHERE d.receiver_user_id = %s
                      AND d.delivered_at IS NULL
                    ORDER BY s.created_at ASC, d.delivery_id ASC
                    LIMIT 100
                    """,
                    (receiver_user_id,),
                )
                for row in cur.fetchall():
                    rewards.append({
                        "delivery_id": int(row[0]),
                        "send_id": int(row[1]),
                        "gold": int(row[2]),
                        "water_drops": int(row[3]),
                        "king_water_drops": int(row[4]),
                        "lottery_tickets": int(row[5]),
                        "wait_passes": int(row[6]),
                        "random_seed_coupons": int(row[7]),
                        "choice_seed_coupons": int(row[8]),
                        "note": str(row[9]),
                        "time_text": format_guestbook_time(row[10]),
                    })
        return rewards, ""
    except Exception as exc:
        print(
            "[운영자 보상함 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return [], "operator_reward_inbox_db_error"


def ack_operator_reward_delivery(receiver_user_id: str, delivery_id: int):
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE admin_reward_deliveries
                    SET delivered_at = COALESCE(delivered_at, NOW())
                    WHERE delivery_id = %s
                      AND receiver_user_id = %s
                    RETURNING send_id, delivered_at
                    """,
                    (delivery_id, receiver_user_id),
                )
                row = cur.fetchone()
            conn.commit()

        if row is None:
            return False, "delivery_not_found"
        return True, "ok"
    except Exception as exc:
        print(
            "[운영자 보상 수령확인 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return False, "operator_reward_ack_db_error"


# ==================================================
# 🎟 쿠폰 코드
# ==================================================
def normalize_coupon_code(value: str) -> str:
    code = str(value or "").strip().upper()
    code = re.sub(r"\s+", "", code)
    return code


def is_valid_coupon_code(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9-]{4,32}", value))


def parse_admin_datetime(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # 운영자센터의 timezone 정보가 빠진 경우 KST로 해석합니다.
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(timezone.utc)


def format_coupon_kst(dt_value) -> str:
    if dt_value is None:
        return "제한 없음"
    return dt_value.astimezone(KST).strftime("%Y-%m-%d %H:%M")


def create_admin_coupon(
    code: str,
    title: str,
    rewards: dict,
    note: str,
    starts_at,
    ends_at,
):
    code = normalize_coupon_code(code)
    title = str(title or "").strip()[:60]
    note = str(note or "").strip()[:120]

    if not is_valid_coupon_code(code):
        return False, "invalid_coupon_code", (
            "쿠폰 코드는 영문 대문자/숫자/하이픈으로 4~32자까지 사용할 수 있어요."
        ), None

    if not admin_reward_has_value(rewards):
        return False, "empty_reward", "쿠폰 보상을 하나 이상 입력해주세요.", None

    start_dt = parse_admin_datetime(starts_at)
    end_dt = parse_admin_datetime(ends_at)
    if start_dt is None:
        start_dt = datetime.now(timezone.utc)
    if end_dt is not None and end_dt <= start_dt:
        return False, "invalid_coupon_period", (
            "쿠폰 종료시간은 시작시간보다 뒤여야 합니다."
        ), None

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gift_coupons (
                        code,
                        title,
                        gold,
                        water_drops,
                        king_water_drops,
                        lottery_tickets,
                        wait_passes,
                        random_seed_coupons,
                        choice_seed_coupons,
                        note,
                        starts_at,
                        ends_at,
                        enabled,
                        created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, TRUE, NOW()
                    )
                    RETURNING coupon_id
                    """,
                    (
                        code,
                        title,
                        int(rewards.get("gold", 0)),
                        int(rewards.get("water_drops", 0)),
                        int(rewards.get("king_water_drops", 0)),
                        int(rewards.get("lottery_tickets", 0)),
                        int(rewards.get("wait_passes", 0)),
                        int(rewards.get("random_seed_coupons", 0)),
                        int(rewards.get("choice_seed_coupons", 0)),
                        note,
                        start_dt,
                        end_dt,
                    ),
                )
                coupon_id = int(cur.fetchone()[0])
            conn.commit()
        return True, "ok", "쿠폰을 만들었습니다.", coupon_id
    except psycopg_errors.UniqueViolation:
        return False, "coupon_code_exists", "이미 사용 중인 쿠폰 코드예요.", None
    except Exception as exc:
        print(f"[쿠폰 생성 오류] {type(exc).__name__}: {exc}")
        return False, "coupon_create_db_error", "쿠폰 생성 중 오류가 발생했어요.", None


def list_admin_coupons(limit: int = 100):
    limit = max(1, min(100, int(limit)))
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        c.coupon_id,
                        c.code,
                        c.title,
                        c.gold,
                        c.water_drops,
                        c.king_water_drops,
                        c.lottery_tickets,
                        c.wait_passes,
                        c.random_seed_coupons,
                        c.choice_seed_coupons,
                        c.note,
                        c.starts_at,
                        c.ends_at,
                        c.enabled,
                        c.created_at,
                        COUNT(cl.user_id) AS claim_count
                    FROM gift_coupons c
                    LEFT JOIN gift_coupon_claims cl
                      ON cl.coupon_id = c.coupon_id
                    GROUP BY c.coupon_id
                    ORDER BY c.created_at DESC, c.coupon_id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()

        now_utc = datetime.now(timezone.utc)
        items = []
        for row in rows:
            start_dt = row[11]
            end_dt = row[12]
            enabled = bool(row[13])
            if not enabled:
                status = "stopped"
            elif start_dt is not None and now_utc < start_dt:
                status = "scheduled"
            elif end_dt is not None and now_utc >= end_dt:
                status = "expired"
            else:
                status = "active"

            items.append({
                "coupon_id": int(row[0]),
                "code": str(row[1]),
                "title": str(row[2]),
                "gold": int(row[3]),
                "water_drops": int(row[4]),
                "king_water_drops": int(row[5]),
                "lottery_tickets": int(row[6]),
                "wait_passes": int(row[7]),
                "random_seed_coupons": int(row[8]),
                "choice_seed_coupons": int(row[9]),
                "note": str(row[10]),
                "starts_at": row[11].isoformat() if row[11] else "",
                "ends_at": row[12].isoformat() if row[12] else "",
                "starts_at_text": format_coupon_kst(row[11]),
                "ends_at_text": format_coupon_kst(row[12]),
                "enabled": enabled,
                "status": status,
                "created_at": row[14].isoformat() if row[14] else "",
                "created_at_text": format_coupon_kst(row[14]),
                "claim_count": int(row[15]),
            })
        return items, ""
    except Exception as exc:
        print(f"[쿠폰 목록 오류] {type(exc).__name__}: {exc}")
        return [], "coupon_list_db_error"


def disable_admin_coupon(coupon_id: int):
    if coupon_id <= 0:
        return False, "invalid_coupon_id", "잘못된 쿠폰 번호예요."
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gift_coupons
                    SET enabled = FALSE
                    WHERE coupon_id = %s
                    RETURNING code
                    """,
                    (coupon_id,),
                )
                row = cur.fetchone()
            conn.commit()
        if row is None:
            return False, "coupon_not_found", "쿠폰을 찾지 못했어요."
        return True, "ok", f"{str(row[0])} 쿠폰을 중지했습니다."
    except Exception as exc:
        print(f"[쿠폰 중지 오류] {type(exc).__name__}: {exc}")
        return False, "coupon_disable_db_error", "쿠폰 중지 중 오류가 발생했어요."


def redeem_gift_coupon(receiver_user_id: str, raw_code: str):
    code = normalize_coupon_code(raw_code)
    if not is_valid_coupon_code(code):
        return False, "invalid_coupon", "쿠폰 코드를 다시 확인해주세요.", None

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                # 같은 쿠폰에 대한 동시 요청도 한 줄씩 처리합니다.
                cur.execute(
                    """
                    SELECT
                        coupon_id,
                        code,
                        title,
                        gold,
                        water_drops,
                        king_water_drops,
                        lottery_tickets,
                        wait_passes,
                        random_seed_coupons,
                        choice_seed_coupons,
                        note,
                        starts_at,
                        ends_at,
                        enabled
                    FROM gift_coupons
                    WHERE code = %s
                    FOR UPDATE
                    """,
                    (code,),
                )
                coupon = cur.fetchone()

                if coupon is None:
                    return False, "coupon_not_found", "존재하지 않는 쿠폰 코드예요.", None

                coupon_id = int(coupon[0])
                now_utc = datetime.now(timezone.utc)
                starts_at = coupon[11]
                ends_at = coupon[12]
                enabled = bool(coupon[13])

                if not enabled:
                    return False, "coupon_stopped", "사용이 종료된 쿠폰이에요.", None
                if starts_at is not None and now_utc < starts_at:
                    return False, "coupon_not_started", (
                        "아직 사용할 수 없는 쿠폰이에요. "
                        + format_coupon_kst(starts_at)
                        + "부터 사용할 수 있어요."
                    ), None
                if ends_at is not None and now_utc >= ends_at:
                    return False, "coupon_expired", "사용기간이 끝난 쿠폰이에요.", None

                cur.execute(
                    """
                    SELECT 1
                    FROM gift_coupon_claims
                    WHERE coupon_id = %s
                      AND user_id = %s
                    """,
                    (coupon_id, receiver_user_id),
                )
                if cur.fetchone() is not None:
                    return False, "coupon_already_used", "이미 사용한 쿠폰이에요.", None

                cur.execute(
                    """
                    SELECT nickname, garden_number
                    FROM gardener_accounts
                    WHERE user_id = %s
                    """,
                    (receiver_user_id,),
                )
                receiver = cur.fetchone()
                if receiver is None:
                    return False, "account_not_found", (
                        "정원사 계정 정보를 찾지 못했어요."
                    ), None

                rewards = {
                    "gold": int(coupon[3]),
                    "water_drops": int(coupon[4]),
                    "king_water_drops": int(coupon[5]),
                    "lottery_tickets": int(coupon[6]),
                    "wait_passes": int(coupon[7]),
                    "random_seed_coupons": int(coupon[8]),
                    "choice_seed_coupons": int(coupon[9]),
                }

                # 쿠폰 보상도 기존 운영자 선물함 delivery로 만들어
                # 게임의 저장→ACK→중복방지 흐름을 그대로 사용합니다.
                cur.execute(
                    """
                    INSERT INTO admin_reward_sends (
                        target_kind,
                        target_user_id,
                        target_garden_number,
                        target_nickname,
                        gold,
                        water_drops,
                        king_water_drops,
                        lottery_tickets,
                        wait_passes,
                        random_seed_coupons,
                        choice_seed_coupons,
                        note,
                        recipient_count,
                        created_at
                    ) VALUES (
                        'user', %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, 1, NOW()
                    )
                    RETURNING send_id
                    """,
                    (
                        receiver_user_id,
                        str(receiver[1]).strip(),
                        str(receiver[0]),
                        rewards["gold"],
                        rewards["water_drops"],
                        rewards["king_water_drops"],
                        rewards["lottery_tickets"],
                        rewards["wait_passes"],
                        rewards["random_seed_coupons"],
                        rewards["choice_seed_coupons"],
                        str(coupon[10] or "")[:120],
                    ),
                )
                send_id = int(cur.fetchone()[0])

                cur.execute(
                    """
                    INSERT INTO admin_reward_deliveries (
                        send_id,
                        receiver_user_id,
                        created_at,
                        delivered_at
                    ) VALUES (%s, %s, NOW(), NULL)
                    RETURNING delivery_id
                    """,
                    (send_id, receiver_user_id),
                )
                delivery_id = int(cur.fetchone()[0])

                cur.execute(
                    """
                    INSERT INTO gift_coupon_claims (
                        coupon_id,
                        user_id,
                        delivery_id,
                        claimed_at
                    ) VALUES (%s, %s, %s, NOW())
                    """,
                    (coupon_id, receiver_user_id, delivery_id),
                )
            conn.commit()

        return True, "ok", "쿠폰이 등록됐어요! 선물을 확인해주세요.", {
            "coupon_id": coupon_id,
            "code": code,
            "title": str(coupon[2] or ""),
            "delivery_id": delivery_id,
        }
    except psycopg_errors.UniqueViolation:
        return False, "coupon_already_used", "이미 사용한 쿠폰이에요.", None
    except Exception as exc:
        print(f"[쿠폰 사용 오류] {type(exc).__name__}: {exc}")
        return False, "coupon_redeem_db_error", (
            "쿠폰 확인 중 오류가 발생했어요. 잠시 후 다시 시도해주세요."
        ), None


def resolve_whisper_target(nickname: str):
    """@닉네임 귓속말 대상 계정을 정확히 찾습니다.

    닉네임은 현재 DB에서 유일 제약이 없으므로 동일 닉네임이 여러 계정이면
    잘못된 사람에게 보내지 않도록 전송을 거부합니다.
    """
    target = str(nickname).strip()
    if not target:
        return None, "whisper_target_missing"

    if not DATABASE_URL:
        # DB가 없을 때는 현재 접속자 중 정확히 한 명인 경우만 허용합니다.
        matches = {}
        for state in clients.values():
            if not state.get("joined"):
                continue
            if str(state.get("nickname", "")).strip() != target:
                continue
            user_id = str(state.get("user_id", "")).strip()
            if user_id:
                matches[user_id] = str(state.get("nickname", target))
        if len(matches) == 1:
            user_id, actual_nickname = next(iter(matches.items()))
            return {
                "user_id": user_id,
                "nickname": actual_nickname,
            }, ""
        if len(matches) > 1:
            return None, "whisper_target_ambiguous"
        return None, "whisper_target_not_found"

    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, nickname
                    FROM gardener_accounts
                    WHERE nickname = %s
                    ORDER BY updated_at DESC
                    LIMIT 2
                    """,
                    (target,),
                )
                rows = cur.fetchall()

        if not rows:
            return None, "whisper_target_not_found"
        if len(rows) > 1:
            return None, "whisper_target_ambiguous"

        return {
            "user_id": str(rows[0][0]),
            "nickname": str(rows[0][1]),
        }, ""
    except Exception as exc:
        print(
            "[귓속말 대상 조회 오류] "
            f"{type(exc).__name__}: {exc}"
        )
        return None, "whisper_target_db_error"


def persist_chat_message(
    sender_user_id: str,
    sender_nickname: str,
    message: str,
    whisper_target_user_id: str = "",
    whisper_target_nickname: str = "",
):
    """채팅 한 건을 DB에 저장하고 (message_id, ISO time)을 반환합니다."""
    global message_sequence

    if DATABASE_URL:
        try:
            target_user_id = (
                str(whisper_target_user_id).strip() or None
            )
            target_nickname = (
                str(whisper_target_nickname).strip() or None
            )
            with get_account_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO gardener_chat_messages (
                            sender_user_id,
                            sender_nickname,
                            message,
                            whisper_target_user_id,
                            whisper_target_nickname,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        RETURNING message_id, created_at
                        """,
                        (
                            str(sender_user_id),
                            str(sender_nickname),
                            str(message),
                            target_user_id,
                            target_nickname,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()

            if row:
                message_id = int(row[0])
                created_at = row[1]
                if created_at is not None:
                    return message_id, created_at.astimezone(KST).isoformat(
                        timespec="seconds"
                    )

        except Exception as exc:
            print(
                "[채팅 DB 저장 오류] "
                f"{type(exc).__name__}: {exc}"
            )
            return None, ""

    # DB가 없는 로컬 테스트용 fallback
    message_sequence += 1
    return message_sequence, now_kst_iso()


def get_chat_history_for_user(user_id: str):
    """공개채팅 + 본인 관련 귓속말만 최근 MAX_HISTORY개 반환합니다."""
    safe_user_id = str(user_id).strip()

    if DATABASE_URL and safe_user_id:
        try:
            with get_account_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            message_id,
                            sender_user_id,
                            sender_nickname,
                            message,
                            whisper_target_user_id,
                            whisper_target_nickname,
                            created_at
                        FROM gardener_chat_messages
                        WHERE
                            whisper_target_user_id IS NULL
                            OR sender_user_id = %s
                            OR whisper_target_user_id = %s
                        ORDER BY message_id DESC
                        LIMIT %s
                        """,
                        (safe_user_id, safe_user_id, MAX_HISTORY),
                    )
                    rows = cur.fetchall()

            result = []
            for row in reversed(rows):
                target_user_id = (
                    "" if row[4] is None else str(row[4])
                )
                target_nickname = (
                    "" if row[5] is None else str(row[5])
                )
                outgoing = {
                    "type": "message",
                    "message_id": int(row[0]),
                    "user_id": str(row[1]),
                    "nickname": str(row[2]),
                    "message": str(row[3]),
                    "time": row[6].astimezone(KST).isoformat(
                        timespec="seconds"
                    ),
                }
                if target_user_id:
                    outgoing["whisper"] = True
                    outgoing["target_user_id"] = target_user_id
                    outgoing["target_nickname"] = target_nickname
                    # 저장된 message는 @닉네임 본문 형식입니다.
                    prefix = "@" + target_nickname
                    raw_message = str(row[3])
                    body = raw_message
                    if raw_message.startswith(prefix):
                        body = raw_message[len(prefix):].lstrip()
                    outgoing["whisper_body"] = body
                result.append(outgoing)
            return result

        except Exception as exc:
            print(
                "[채팅 DB 불러오기 오류] "
                f"{type(exc).__name__}: {exc}"
            )

    # 로컬 테스트/DB 장애 시 메모리 history를 개인정보 범위에 맞게 필터링합니다.
    visible = []
    for item in list(history):
        if not bool(item.get("whisper", False)):
            visible.append(dict(item))
            continue
        if (
            str(item.get("user_id", "")) == safe_user_id
            or str(item.get("target_user_id", "")) == safe_user_id
        ):
            visible.append(dict(item))
    return visible[-MAX_HISTORY:]


def get_latest_public_chat_message():
    """메인 1줄용 최신 공개채팅 1건."""
    if DATABASE_URL:
        try:
            with get_account_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            message_id,
                            sender_user_id,
                            sender_nickname,
                            message,
                            created_at
                        FROM gardener_chat_messages
                        WHERE whisper_target_user_id IS NULL
                        ORDER BY message_id DESC
                        LIMIT 1
                        """
                    )
                    row = cur.fetchone()
            if row:
                return {
                    "type": "message",
                    "message_id": int(row[0]),
                    "user_id": str(row[1]),
                    "nickname": str(row[2]),
                    "message": str(row[3]),
                    "time": row[4].astimezone(KST).isoformat(
                        timespec="seconds"
                    ),
                }
        except Exception as exc:
            print(
                "[최신 공개채팅 조회 오류] "
                f"{type(exc).__name__}: {exc}"
            )

    for item in reversed(history):
        if not bool(item.get("whisper", False)):
            return dict(item)
    return None


def find_persisted_chat_message(message_id: int):
    if DATABASE_URL and int(message_id) >= 0:
        try:
            with get_account_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            message_id,
                            sender_user_id,
                            sender_nickname,
                            message,
                            whisper_target_user_id,
                            whisper_target_nickname,
                            created_at
                        FROM gardener_chat_messages
                        WHERE message_id = %s
                        """,
                        (int(message_id),),
                    )
                    row = cur.fetchone()
            if row:
                target_user_id = (
                    "" if row[4] is None else str(row[4])
                )
                return {
                    "type": "message",
                    "message_id": int(row[0]),
                    "user_id": str(row[1]),
                    "nickname": str(row[2]),
                    "message": str(row[3]),
                    "whisper": bool(target_user_id),
                    "target_user_id": target_user_id,
                    "target_nickname": (
                        "" if row[5] is None else str(row[5])
                    ),
                    "time": row[6].astimezone(KST).isoformat(
                        timespec="seconds"
                    ),
                }
        except Exception as exc:
            print(
                "[채팅 메시지 조회 오류] "
                f"{type(exc).__name__}: {exc}"
            )
    return None


async def send_to_user_ids(user_ids, payload: dict):
    targets = {
        str(value).strip()
        for value in user_ids
        if str(value).strip()
    }
    if not targets:
        return

    sockets = [
        ws
        for ws, state in list(clients.items())
        if state.get("joined")
        and str(state.get("user_id", "")).strip() in targets
    ]
    if not sockets:
        return

    encoded = json.dumps(payload, ensure_ascii=False)
    await asyncio.gather(
        *(ws.send(encoded) for ws in sockets),
        return_exceptions=True,
    )

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
            f"FlowerGarden 채팅은 Lv.{MIN_CHAT_LEVEL}부터 이용할 수 있어요.",
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
            "채팅 이용이 "
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
                "message": "이미 채팅에 연결되어 있어요.",
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
            "history": get_chat_history_for_user(state["user_id"]),
            "latest_public": get_latest_public_chat_message(),
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

    # 최신 클라이언트만 title_name을 보냅니다.
    # 구버전이 접속했을 때 기존 실제 칭호를 '초보 정원사'로 덮어쓰지 않습니다.
    raw_title_name = payload.get("title_name", None)
    title_name = None
    if raw_title_name is not None:
        parsed_title_name = str(raw_title_name).strip()
        if parsed_title_name:
            title_name = parsed_title_name

    try:
        level = int(payload.get("level", 0))
    except (TypeError, ValueError):
        level = 0

    ok, code, message = register_account_record(
        nickname,
        user_id,
        garden_number,
        level,
        title_name,
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
        state = clients[ws]
        state["account_registered"] = True
        state["account_user_id"] = user_id
        state["garden_number"] = garden_number
        state["nickname"] = nickname
        state["level"] = level
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

    requester_user_id = str(state.get("account_user_id", ""))
    target_user_id = str(record.get("user_id", ""))
    friend_status = (
        get_friend_status(requester_user_id, target_user_id)
        if requester_user_id
        else "none"
    )

    await send_json(
        ws,
        {
            "type": "account_lookup_result",
            "found": True,
            "nickname": record.get("nickname", "정원사"),
            "garden_number": garden_number,
            "level": int(record.get("level", 0)),
            "friend_status": friend_status,
        },
    )


async def require_registered_account(ws):
    state = clients[ws]
    if state.get("account_registered") and state.get("account_user_id"):
        return True
    await send_json(
        ws,
        {
            "type": "error",
            "code": "account_registration_required",
            "message": "먼저 정원사 계정 정보를 서버에 등록해주세요.",
        },
    )
    return False



async def require_admin_auth(ws):
    state = clients[ws]
    if state.get("admin_authenticated"):
        return True
    await send_json(
        ws,
        {
            "type": "admin_auth_required",
            "ok": False,
            "message": "운영자 로그인이 필요합니다.",
        },
    )
    return False


async def handle_admin_auth(ws, payload: dict):
    import hmac

    password = str(payload.get("password", ""))
    if not ADMIN_PASSWORD:
        await send_json(
            ws,
            {
                "type": "admin_auth_result",
                "ok": False,
                "code": "admin_password_not_configured",
                "message": (
                    "Render Environment에 ADMIN_PASSWORD가 아직 설정되지 않았습니다."
                ),
            },
        )
        return

    ok = hmac.compare_digest(password, ADMIN_PASSWORD)
    clients[ws]["admin_authenticated"] = ok

    await send_json(
        ws,
        {
            "type": "admin_auth_result",
            "ok": ok,
            "code": "ok" if ok else "wrong_password",
            "message": (
                "운영자 로그인 완료"
                if ok
                else "운영자 비밀번호가 맞지 않습니다."
            ),
            "default_reward": ADMIN_DEFAULT_REWARD if ok else {},
        },
    )


async def handle_admin_user_search(ws, payload: dict):
    if not await require_admin_auth(ws):
        return

    query = str(payload.get("query", "")).strip()
    users, error_code = search_admin_accounts(query)
    await send_json(
        ws,
        {
            "type": "admin_user_search_result",
            "ok": not bool(error_code),
            "code": error_code or "ok",
            "users": users,
            "query": query,
        },
    )


async def handle_admin_reward_history(ws, _payload: dict):
    if not await require_admin_auth(ws):
        return

    items, error_code = load_admin_reward_history()
    await send_json(
        ws,
        {
            "type": "admin_reward_history_result",
            "ok": not bool(error_code),
            "code": error_code or "ok",
            "items": items,
        },
    )


async def handle_admin_reward_send(ws, payload: dict):
    if not await require_admin_auth(ws):
        return

    target_kind = str(payload.get("target_kind", "")).strip().lower()
    target_user_id = str(payload.get("target_user_id", "")).strip()
    note = str(payload.get("note", "")).strip()
    rewards_value = payload.get("rewards", {})
    rewards = (
        normalize_admin_reward_payload(rewards_value)
        if isinstance(rewards_value, dict)
        else dict(ADMIN_DEFAULT_REWARD)
    )

    ok, code, message, send_id, recipient_count, receiver_ids = (
        create_admin_reward_send(
            target_kind,
            target_user_id,
            rewards,
            note,
        )
    )

    await send_json(
        ws,
        {
            "type": "admin_reward_send_result",
            "ok": ok,
            "code": code,
            "message": message,
            "send_id": send_id,
            "recipient_count": recipient_count,
            "rewards": rewards,
        },
    )

    if not ok:
        return

    notice = {
        "type": "operator_reward_notice",
        "send_id": send_id,
        "message": "🎁 운영자 선물이 도착했어요!",
    }
    await asyncio.gather(
        *(
            notify_account_user_id(user_id, notice)
            for user_id in receiver_ids
        ),
        return_exceptions=True,
    )


async def handle_operator_reward_inbox(ws, _payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    rewards, error_code = load_operator_reward_inbox(
        str(state.get("account_user_id", ""))
    )
    await send_json(
        ws,
        {
            "type": "operator_reward_inbox_result",
            "ok": not bool(error_code),
            "code": error_code or "ok",
            "rewards": rewards,
        },
    )


async def handle_operator_reward_ack(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    try:
        delivery_id = int(payload.get("delivery_id", 0))
    except (TypeError, ValueError):
        delivery_id = 0

    if delivery_id <= 0:
        await send_json(
            ws,
            {
                "type": "operator_reward_ack_result",
                "ok": False,
                "code": "invalid_delivery_id",
                "delivery_id": delivery_id,
            },
        )
        return

    state = clients[ws]
    ok, code = ack_operator_reward_delivery(
        str(state.get("account_user_id", "")),
        delivery_id,
    )
    await send_json(
        ws,
        {
            "type": "operator_reward_ack_result",
            "ok": ok,
            "code": code,
            "delivery_id": delivery_id,
        },
    )


async def handle_admin_coupon_create(ws, payload: dict):
    if not await require_admin_auth(ws):
        return

    rewards_value = payload.get("rewards", {})
    rewards = (
        normalize_admin_reward_payload(rewards_value)
        if isinstance(rewards_value, dict)
        else {}
    )
    ok, code, message, coupon_id = create_admin_coupon(
        str(payload.get("code", "")),
        str(payload.get("title", "")),
        rewards,
        str(payload.get("note", "")),
        payload.get("starts_at"),
        payload.get("ends_at"),
    )
    await send_json(
        ws,
        {
            "type": "admin_coupon_create_result",
            "ok": ok,
            "code": code,
            "message": message,
            "coupon_id": coupon_id,
            "coupon_code": normalize_coupon_code(payload.get("code", "")),
        },
    )


async def handle_admin_coupon_list(ws, _payload: dict):
    if not await require_admin_auth(ws):
        return
    items, error_code = list_admin_coupons()
    await send_json(
        ws,
        {
            "type": "admin_coupon_list_result",
            "ok": not bool(error_code),
            "code": error_code or "ok",
            "items": items,
        },
    )


async def handle_admin_coupon_disable(ws, payload: dict):
    if not await require_admin_auth(ws):
        return
    try:
        coupon_id = int(payload.get("coupon_id", 0))
    except (TypeError, ValueError):
        coupon_id = 0
    ok, code, message = disable_admin_coupon(coupon_id)
    await send_json(
        ws,
        {
            "type": "admin_coupon_disable_result",
            "ok": ok,
            "code": code,
            "message": message,
            "coupon_id": coupon_id,
        },
    )


async def handle_coupon_redeem(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    ok, code, message, coupon_info = redeem_gift_coupon(
        str(state.get("account_user_id", "")),
        str(payload.get("code", "")),
    )
    await send_json(
        ws,
        {
            "type": "coupon_redeem_result",
            "ok": ok,
            "code": code,
            "message": message,
            "coupon": coupon_info or {},
        },
    )


async def handle_operator_status(ws, _payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    is_operator = (
        str(state.get("garden_number", "")).strip()
        == ADMIN_GARDEN_NUMBER
    )
    await send_json(
        ws,
        {
            "type": "operator_status_result",
            "ok": True,
            "is_operator": is_operator,
            "garden_number": str(state.get("garden_number", "")).strip(),
        },
    )


async def handle_friend_request(ws, payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    garden_number = str(payload.get("garden_number", "")).strip()
    ok, code, message, status = create_friend_request(
        str(state.get("account_user_id", "")),
        garden_number,
    )
    await send_json(
        ws,
        {
            "type": "friend_request_result",
            "ok": ok,
            "code": code,
            "message": message,
            "garden_number": garden_number,
            "friend_status": status,
        },
    )


async def handle_friend_list(ws, _payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    friends, incoming, outgoing, error_code = get_friend_lists(
        str(state.get("account_user_id", ""))
    )
    if error_code:
        await send_json(
            ws,
            {
                "type": "error",
                "code": error_code,
                "message": "친구 목록을 불러오지 못했어요. 잠시 후 다시 시도해주세요.",
            },
        )
        return
    await send_json(
        ws,
        {
            "type": "friend_list_result",
            "friends": friends,
            "incoming_requests": incoming,
            "outgoing_requests": outgoing,
        },
    )



async def handle_friend_remove(ws, payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    garden_number = str(payload.get("garden_number", "")).strip()
    ok, code, message = remove_friend(
        str(state.get("account_user_id", "")),
        garden_number,
    )
    await send_json(
        ws,
        {
            "type": "friend_remove_result",
            "ok": ok,
            "code": code,
            "message": message,
            "garden_number": garden_number,
        },
    )

async def handle_friend_recommendations(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    try:
        limit = int(payload.get("limit", 8))
    except (TypeError, ValueError):
        limit = 8

    recommendations, error_code = get_friend_recommendations(
        str(state.get("account_user_id", "")),
        limit,
    )

    if error_code:
        await send_json(ws, {
            "type": "friend_recommendations_result",
            "ok": False,
            "code": error_code,
            "message": "추천 정원사를 불러오지 못했어요. 잠시 후 다시 시도해주세요.",
            "recommendations": [],
        })
        return

    await send_json(ws, {
        "type": "friend_recommendations_result",
        "ok": True,
        "code": "ok",
        "message": "",
        "recommendations": recommendations,
    })


async def handle_friend_response(ws, payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    requester_number = str(payload.get("garden_number", "")).strip()
    accept = bool(payload.get("accept", False))
    ok, code, message = respond_friend_request(
        str(state.get("account_user_id", "")),
        requester_number,
        accept,
    )
    await send_json(
        ws,
        {
            "type": "friend_response_result",
            "ok": ok,
            "code": code,
            "message": message,
            "garden_number": requester_number,
            "accepted": accept and ok,
        },
    )


async def handle_guestbook_load(ws, payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    owner_number = str(payload.get("garden_number", "")).strip()
    data, error_code = load_guestbook(
        str(state.get("account_user_id", "")),
        owner_number,
    )
    if error_code:
        message = (
            "해당 정원사를 찾을 수 없어요."
            if error_code == "owner_not_found"
            else "방명록을 불러오지 못했어요. 잠시 후 다시 시도해주세요."
        )
        await send_json(ws, {
            "type": "guestbook_load_result",
            "ok": False,
            "code": error_code,
            "message": message,
        })
        return

    await send_json(ws, {
        "type": "guestbook_load_result",
        "ok": True,
        **data,
    })


async def handle_guestbook_post(ws, payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    owner_number = str(payload.get("garden_number", "")).strip()
    message = str(payload.get("message", ""))
    ok, code, result_message = post_guestbook_entry(
        str(state.get("account_user_id", "")),
        owner_number,
        message,
    )
    await send_json(ws, {
        "type": "guestbook_post_result",
        "ok": ok,
        "code": code,
        "message": result_message,
        "garden_number": owner_number,
    })


async def handle_guestbook_delete(ws, payload: dict):
    if not await require_registered_account(ws):
        return
    state = clients[ws]
    try:
        entry_id = int(payload.get("entry_id", 0))
    except (TypeError, ValueError):
        entry_id = 0
    if entry_id <= 0:
        await send_json(ws, {
            "type": "guestbook_delete_result",
            "ok": False,
            "code": "invalid_entry_id",
            "message": "삭제할 방명록 글을 확인할 수 없어요.",
        })
        return

    ok, code, result_message = delete_guestbook_entry(
        str(state.get("account_user_id", "")),
        entry_id,
    )
    await send_json(ws, {
        "type": "guestbook_delete_result",
        "ok": ok,
        "code": code,
        "message": result_message,
        "entry_id": entry_id,
    })


async def handle_garden_sync(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    ok, code, message = save_garden_snapshot(
        str(state.get("account_user_id", "")),
        payload.get("plots", []),
    )
    await send_json(ws, {
        "type": "garden_sync_result",
        "ok": ok,
        "code": code,
        "message": message,
    })


async def handle_garden_load(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    owner_number = str(payload.get("garden_number", "")).strip()
    data, error_code = load_garden_snapshot(
        str(state.get("account_user_id", "")),
        owner_number,
    )

    if error_code:
        if error_code == "friends_only":
            message = "친구의 꽃밭만 볼 수 있어요."
        elif error_code == "owner_not_found":
            message = "해당 정원사를 찾을 수 없어요."
        else:
            message = "친구 꽃밭을 불러오지 못했어요. 잠시 후 다시 시도해주세요."
        await send_json(ws, {
            "type": "garden_load_result",
            "ok": False,
            "code": error_code,
            "message": message,
        })
        return

    await send_json(ws, {
        "type": "garden_load_result",
        "ok": True,
        **data,
    })


async def handle_garden_steal(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    owner_number = str(payload.get("garden_number", "")).strip()
    area_id = str(payload.get("area_id", "")).strip()
    bloom_id = str(payload.get("bloom_id", "")).strip()

    ok, code, message, flower_id = steal_garden_flower(
        str(state.get("account_user_id", "")),
        owner_number,
        area_id,
        bloom_id,
    )
    await send_json(ws, {
        "type": "garden_steal_result",
        "ok": ok,
        "code": code,
        "message": message,
        "garden_number": owner_number,
        "area_id": area_id,
        "bloom_id": bloom_id,
        "flower_id": flower_id,
    })



async def handle_together_garden_sync(ws, _payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    data, error_code = get_together_garden_state(
        str(state.get("account_user_id", ""))
    )
    if error_code or not data:
        await send_json(ws, {
            "type": "together_garden_sync_result",
            "ok": False,
            "code": error_code or "together_garden_db_error",
            "message": "함께하는 정원 정보를 불러오지 못했어요.",
        })
        return

    await send_json(ws, {
        "type": "together_garden_sync_result",
        "ok": True,
        **data,
    })


async def handle_together_garden_harvest(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    data, error_code = add_together_garden_harvest(
        str(state.get("account_user_id", "")),
        str(payload.get("action_id", "")),
        payload.get("flower_count", 0),
    )
    if error_code or not data:
        await send_json(ws, {
            "type": "together_garden_harvest_result",
            "ok": False,
            "code": error_code or "together_garden_db_error",
            "message": "수확 기여도를 서버에 반영하지 못했어요.",
            "action_id": str(payload.get("action_id", "")),
        })
        return

    await send_json(ws, {
        "type": "together_garden_harvest_result",
        "ok": True,
        **data,
    })


async def handle_together_garden_reward_ack(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    milestone = payload.get("milestone", 0)
    ok, code = acknowledge_together_garden_reward(
        str(state.get("account_user_id", "")),
        milestone,
    )
    await send_json(ws, {
        "type": "together_garden_reward_ack_result",
        "ok": ok,
        "code": code,
        "milestone": milestone,
    })


async def handle_transfer_backup(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    ok, code, message, transfer_code = create_transfer_backup(
        str(state.get("account_user_id", "")),
        str(state.get("garden_number", "")),
        payload.get("save_data"),
    )
    await send_json(ws, {
        "type": "transfer_backup_result",
        "ok": ok,
        "code": code,
        "message": message,
        "transfer_code": transfer_code,
        "expires_days": TRANSFER_BACKUP_RETENTION_DAYS if ok else 0,
    })


async def handle_transfer_restore(ws, payload: dict):
    state = clients[ws]
    now = time.monotonic()
    last_attempt = float(state.get("last_transfer_restore_attempt", 0.0))
    if now - last_attempt < 1.0:
        await send_json(ws, {
            "type": "transfer_restore_result",
            "ok": False,
            "code": "too_fast",
            "message": "잠시 후 다시 시도해주세요.",
        })
        return
    state["last_transfer_restore_attempt"] = now

    transfer_code = str(payload.get("transfer_code", "")).strip()
    data, error_code = load_transfer_backup(transfer_code)
    if error_code:
        message = (
            "이전코드 8자리를 확인해주세요."
            if error_code == "invalid_transfer_code"
            else "사용할 수 없거나 만료된 이전코드예요."
            if error_code == "transfer_not_found"
            else "이전 데이터를 불러오지 못했어요. 잠시 후 다시 시도해주세요."
        )
        await send_json(ws, {
            "type": "transfer_restore_result",
            "ok": False,
            "code": error_code,
            "message": message,
        })
        return

    state["loaded_transfer_code"] = transfer_code
    await send_json(ws, {
        "type": "transfer_restore_result",
        "ok": True,
        "code": "ok",
        "message": "베타판 데이터를 불러왔어요.",
        "transfer_code": transfer_code,
        "garden_number": data["garden_number"],
        "nickname": data["nickname"],
        "save_data": data["save_data"],
    })


async def handle_transfer_complete(ws, payload: dict):
    state = clients[ws]
    transfer_code = str(payload.get("transfer_code", "")).strip()
    loaded_code = str(state.get("loaded_transfer_code", "")).strip()

    if not transfer_code or transfer_code != loaded_code:
        await send_json(ws, {
            "type": "transfer_complete_result",
            "ok": False,
            "code": "transfer_not_loaded",
            "message": "먼저 이전 데이터를 불러와주세요.",
        })
        return

    ok = complete_transfer_backup(transfer_code)
    if ok:
        state["loaded_transfer_code"] = ""
    await send_json(ws, {
        "type": "transfer_complete_result",
        "ok": ok,
        "code": "ok" if ok else "transfer_complete_failed",
        "message": (
            "데이터 이전이 완료됐어요."
            if ok
            else "이전 완료처리를 하지 못했어요. 다시 시도해주세요."
        ),
    })


async def handle_gift_send(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    receiver_number = str(payload.get("garden_number", "")).strip()
    flower_id = str(payload.get("flower_id", "")).strip()

    ok, code, message, gift_id, receiver_nickname = send_flower_gift(
        str(state.get("account_user_id", "")),
        receiver_number,
        flower_id,
    )
    await send_json(ws, {
        "type": "gift_send_result",
        "ok": ok,
        "code": code,
        "message": message,
        "gift_id": gift_id,
        "garden_number": receiver_number,
        "receiver_nickname": receiver_nickname,
        "flower_id": flower_id,
    })


async def handle_gift_inbox(ws, _payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    gifts, error_code = load_flower_gift_inbox(
        str(state.get("account_user_id", ""))
    )
    if error_code:
        await send_json(ws, {
            "type": "gift_inbox_result",
            "ok": False,
            "code": error_code,
            "message": "받은 선물을 불러오지 못했어요. 잠시 후 다시 시도해주세요.",
            "gifts": [],
        })
        return

    await send_json(ws, {
        "type": "gift_inbox_result",
        "ok": True,
        "code": "ok",
        "message": "",
        "gifts": gifts,
    })


async def handle_gift_history(ws, _payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    gifts, error_code = load_flower_gift_history(
        str(state.get("account_user_id", ""))
    )

    if error_code:
        await send_json(ws, {
            "type": "gift_history_result",
            "ok": False,
            "code": error_code,
            "message": "받은 기록을 불러오지 못했어요. 잠시 후 다시 시도해주세요.",
            "gifts": [],
        })
        return

    await send_json(ws, {
        "type": "gift_history_result",
        "ok": True,
        "code": "ok",
        "message": "",
        "gifts": gifts,
    })


async def handle_gift_claim(ws, payload: dict):
    if not await require_registered_account(ws):
        return

    state = clients[ws]
    try:
        gift_id = int(payload.get("gift_id", 0))
    except (TypeError, ValueError):
        gift_id = 0

    if gift_id <= 0:
        await send_json(ws, {
            "type": "gift_claim_result",
            "ok": False,
            "code": "invalid_gift_id",
            "message": "받을 선물을 확인할 수 없어요.",
            "gift_id": gift_id,
            "flower_id": "",
        })
        return

    ok, code, message, flower_id, sender_nickname = claim_flower_gift(
        str(state.get("account_user_id", "")),
        gift_id,
    )
    await send_json(ws, {
        "type": "gift_claim_result",
        "ok": ok,
        "code": code,
        "message": message,
        "gift_id": gift_id,
        "flower_id": flower_id,
        "sender_nickname": sender_nickname,
    })



async def handle_chat_message(ws, payload: dict):
    state = clients[ws]

    if not state.get("joined"):
        await send_json(
            ws,
            {
                "type": "error",
                "code": "not_joined",
                "message": "먼저 플라워가든 채팅에 연결해주세요.",
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
                    "채팅이 "
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

    # @닉네임 + 공백 + 내용 형식은 별도 1:1방 없이 같은 채팅창에서 귓속말로 처리합니다.
    whisper_match = re.match(r"^@([^\s@]+)\s+(.+)$", message, flags=re.DOTALL)
    if whisper_match:
        target_name = whisper_match.group(1).strip()
        whisper_body = whisper_match.group(2).strip()

        if not whisper_body:
            await send_json(
                ws,
                {
                    "type": "error",
                    "code": "whisper_empty",
                    "message": "귓속말 내용을 입력해주세요.",
                },
            )
            return

        target, target_error = resolve_whisper_target(target_name)
        if target_error:
            message_by_code = {
                "whisper_target_not_found": (
                    "해당 닉네임의 정원사를 찾을 수 없어요."
                ),
                "whisper_target_ambiguous": (
                    "같은 닉네임이 여러 명 있어 귓속말 대상을 정할 수 없어요."
                ),
                "whisper_target_db_error": (
                    "귓속말 대상을 확인하지 못했어요. 잠시 후 다시 시도해주세요."
                ),
            }
            await send_json(
                ws,
                {
                    "type": "error",
                    "code": target_error,
                    "message": message_by_code.get(
                        target_error,
                        "귓속말 대상을 확인할 수 없어요.",
                    ),
                },
            )
            return

        target_user_id = str(target["user_id"])
        target_nickname = str(target["nickname"])

        if target_user_id == str(state["user_id"]):
            await send_json(
                ws,
                {
                    "type": "error",
                    "code": "whisper_self",
                    "message": "자기 자신에게는 귓속말을 보낼 수 없어요.",
                },
            )
            return

        message_id, sent_time = persist_chat_message(
            state["user_id"],
            state["nickname"],
            message,
            target_user_id,
            target_nickname,
        )
        if message_id is None:
            await send_json(
                ws,
                {
                    "type": "error",
                    "code": "chat_save_failed",
                    "message": "메시지를 저장하지 못했어요. 잠시 후 다시 시도해주세요.",
                },
            )
            return

        outgoing = {
            "type": "message",
            "message_id": int(message_id),
            "user_id": state["user_id"],
            "nickname": state["nickname"],
            "message": message,
            "time": sent_time or now_kst_iso(),
            "whisper": True,
            "target_user_id": target_user_id,
            "target_nickname": target_nickname,
            "whisper_body": whisper_body,
        }

        history.append(outgoing)
        await send_to_user_ids(
            {str(state["user_id"]), target_user_id},
            outgoing,
        )

        print(
            f"[귓속말] {state['nickname']} -> "
            f"{target_nickname} #{message_id} {whisper_body}"
        )
        return

    # 일반 전체채팅
    message_id, sent_time = persist_chat_message(
        state["user_id"],
        state["nickname"],
        message,
    )
    if message_id is None:
        await send_json(
            ws,
            {
                "type": "error",
                "code": "chat_save_failed",
                "message": "메시지를 저장하지 못했어요. 잠시 후 다시 시도해주세요.",
            },
        )
        return

    outgoing = {
        "type": "message",
        "message_id": int(message_id),
        "user_id": state["user_id"],
        "nickname": state["nickname"],
        "message": message,
        "time": sent_time or now_kst_iso(),
        "whisper": False,
    }

    history.append(outgoing)
    await broadcast(outgoing)

    print(
        f"[{state['nickname']}] "
        f"#{message_id} {message}"
    )


def find_history_message(message_id: int):
    for item in reversed(history):
        if int(item.get("message_id", -1)) == message_id:
            return item

    return find_persisted_chat_message(message_id)

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


# ==================================================
# 🌷 수확물장터 1.0
# - 서버가 상품의 단 한 번 판매를 DB 트랜잭션 + FOR UPDATE로 보장합니다.
# - 게임 골드/창고는 현재 클라이언트 저장 구조이므로 구매 결과를 delivery로 안전 전달합니다.
# - delivery는 클라이언트가 실제 반영한 뒤 ack 해야 사라집니다.
# ==================================================
MARKET_MAX_SLOT = 12
MARKET_MAX_QUANTITY = 30
MARKET_HISTORY_LIMIT = 10
MARKET_LIST_LIMIT = 200
MARKET_ALLOWED_RARITIES = {"일반", "고급", "에픽", "프리미엄"}


def market_password_hash(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def market_listing_to_dict(row, viewer_user_id: str = "") -> dict:
    return {
        "listing_id": int(row[0]),
        "seller_user_id": str(row[1]),
        "seller_nickname": str(row[2]),
        "slot_no": int(row[3]),
        "harvest_id": str(row[4]),
        "harvest_name": str(row[5]),
        "rarity": str(row[6]),
        "quantity": int(row[7]),
        "unit_price": int(row[8]),
        "warehouse_price": int(row[9] or 0),
        "has_password": bool(row[10]),
        "is_mine": str(row[1]) == str(viewer_user_id),
        "created_at": row[11].isoformat() if row[11] else "",
    }


def load_market_listings(viewer_user_id: str, query: str = ""):
    if not DATABASE_URL:
        return [], "db_not_configured"
    q = str(query or "").strip()[:80]
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                params = []
                where = "WHERE m.status = 'active'"
                if q:
                    where += " AND (m.harvest_name ILIKE %s OR m.seller_nickname ILIKE %s)"
                    like = f"%{q}%"
                    params.extend([like, like])
                params.append(MARKET_LIST_LIMIT)
                cur.execute(
                    f"""
                    SELECT m.listing_id, m.seller_user_id, m.seller_nickname,
                           m.slot_no, m.harvest_id, m.harvest_name, m.rarity,
                           m.quantity, m.unit_price, m.warehouse_price,
                           (m.password_hash IS NOT NULL), m.created_at
                    FROM harvest_market_listings m
                    {where}
                    ORDER BY m.created_at DESC, m.listing_id DESC
                    LIMIT %s
                    """,
                    tuple(params),
                )
                return [market_listing_to_dict(r, viewer_user_id) for r in cur.fetchall()], ""
    except Exception as exc:
        print(f"[장터 목록 오류] {type(exc).__name__}: {exc}")
        return [], "market_db_error"


def load_my_market_listings(user_id: str):
    if not DATABASE_URL:
        return [], "db_not_configured"
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT listing_id, seller_user_id, seller_nickname,
                           slot_no, harvest_id, harvest_name, rarity,
                           quantity, unit_price, warehouse_price,
                           (password_hash IS NOT NULL), created_at
                    FROM harvest_market_listings
                    WHERE seller_user_id = %s AND status = 'active'
                    ORDER BY slot_no ASC
                    """,
                    (user_id,),
                )
                return [market_listing_to_dict(r, user_id) for r in cur.fetchall()], ""
    except Exception as exc:
        print(f"[내 장터 오류] {type(exc).__name__}: {exc}")
        return [], "market_db_error"


def create_market_listing(user_id: str, nickname: str, payload: dict):
    try:
        slot_no = int(payload.get("slot_no", 0))
        quantity = int(payload.get("quantity", 0))
        unit_price = int(payload.get("unit_price", 0))
        warehouse_price = int(payload.get("warehouse_price", 0))
    except (TypeError, ValueError):
        return False, "invalid_number", "수량 또는 가격을 확인해주세요.", 0
    harvest_id = str(payload.get("harvest_id", "")).strip()[:100]
    harvest_name = str(payload.get("harvest_name", "")).strip()[:80]
    rarity = str(payload.get("rarity", "일반")).strip()
    password = str(payload.get("password", "")).strip()
    if not (1 <= slot_no <= MARKET_MAX_SLOT):
        return False, "invalid_slot", "장터칸을 확인해주세요.", 0
    if not harvest_id or not harvest_name:
        return False, "invalid_harvest", "등록할 수확물을 확인해주세요.", 0
    if not (1 <= quantity <= MARKET_MAX_QUANTITY):
        return False, "invalid_quantity", "한 칸에는 최대 30개까지 등록할 수 있어요.", 0
    if unit_price <= 0 or unit_price > 2_000_000_000:
        return False, "invalid_price", "판매가격을 확인해주세요.", 0
    if warehouse_price < 0:
        warehouse_price = 0
    if rarity not in MARKET_ALLOWED_RARITIES:
        rarity = "일반"
    if len(password) > 20:
        return False, "password_too_long", "비밀번호는 20자 이하로 설정해주세요.", 0
    password_hash = market_password_hash(password) if password else None
    if not DATABASE_URL:
        return False, "db_not_configured", "장터 DB가 준비되지 않았어요.", 0
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO harvest_market_listings (
                        seller_user_id, seller_nickname, slot_no,
                        harvest_id, harvest_name, rarity, quantity,
                        unit_price, warehouse_price, password_hash
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING listing_id
                    """,
                    (user_id, nickname[:12], slot_no, harvest_id, harvest_name,
                     rarity, quantity, unit_price, warehouse_price, password_hash),
                )
                listing_id = int(cur.fetchone()[0])
            conn.commit()
        return True, "ok", "수확물이 장터에 등록되었습니다.", listing_id
    except psycopg_errors.UniqueViolation:
        return False, "slot_in_use", "이미 상품이 등록된 장터칸이에요.", 0
    except Exception as exc:
        print(f"[장터 등록 오류] {type(exc).__name__}: {exc}")
        return False, "market_db_error", "상품을 등록하지 못했어요.", 0


def cancel_market_listing(user_id: str, listing_id: int):
    if not DATABASE_URL:
        return False, "db_not_configured", "장터 DB가 준비되지 않았어요.", {}
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT harvest_id, harvest_name, quantity
                    FROM harvest_market_listings
                    WHERE listing_id=%s AND seller_user_id=%s AND status='active'
                    FOR UPDATE
                    """,
                    (listing_id, user_id),
                )
                row = cur.fetchone()
                if row is None:
                    return False, "listing_not_found", "판매 중인 상품을 찾을 수 없어요.", {}
                cur.execute(
                    """UPDATE harvest_market_listings
                       SET status='cancelled' WHERE listing_id=%s""",
                    (listing_id,),
                )
            conn.commit()
        return True, "ok", "판매를 취소했습니다.", {
            "harvest_id": str(row[0]), "harvest_name": str(row[1]), "quantity": int(row[2])
        }
    except Exception as exc:
        print(f"[장터 취소 오류] {type(exc).__name__}: {exc}")
        return False, "market_db_error", "판매를 취소하지 못했어요.", {}


def purchase_market_listing(buyer_user_id: str, buyer_nickname: str, listing_id: int, password: str):
    """동시구매 방지 핵심. 한 listing 행을 FOR UPDATE로 잠근 뒤 sold 처리합니다."""
    if not DATABASE_URL:
        return False, "db_not_configured", "장터 DB가 준비되지 않았어요.", {}
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT listing_id, seller_user_id, seller_nickname,
                           harvest_id, harvest_name, quantity, unit_price,
                           password_hash, status
                    FROM harvest_market_listings
                    WHERE listing_id=%s
                    FOR UPDATE
                    """,
                    (listing_id,),
                )
                row = cur.fetchone()
                if row is None or str(row[8]) != "active":
                    return False, "already_sold", "이미 판매되었거나 내려간 상품이에요.", {}
                seller_user_id = str(row[1])
                if seller_user_id == buyer_user_id:
                    return False, "cannot_buy_own", "내가 등록한 상품은 구매할 수 없어요.", {}
                stored_hash = row[7]
                if stored_hash:
                    supplied_hash = market_password_hash(str(password or ""))
                    if not secrets.compare_digest(str(stored_hash), supplied_hash):
                        return False, "wrong_password", "비밀번호가 맞지 않아요.", {}
                quantity = int(row[5]); unit_price = int(row[6]); total_price = quantity * unit_price
                cur.execute(
                    """
                    UPDATE harvest_market_listings
                    SET status='sold', sold_at=NOW()
                    WHERE listing_id=%s AND status='active'
                    """,
                    (listing_id,),
                )
                if cur.rowcount != 1:
                    return False, "already_sold", "다른 정원사가 먼저 구매했어요.", {}
                cur.execute(
                    """
                    INSERT INTO harvest_market_sales (
                        listing_id, seller_user_id, seller_nickname,
                        buyer_user_id, buyer_nickname, harvest_id, harvest_name,
                        quantity, unit_price, total_price
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING sale_id
                    """,
                    (listing_id, seller_user_id, str(row[2]), buyer_user_id,
                     buyer_nickname[:12], str(row[3]), str(row[4]), quantity,
                     unit_price, total_price),
                )
                sale_id = int(cur.fetchone()[0])
                # 구매자 수확물 / 판매자 골드를 각각 미수령 delivery로 기록합니다.
                cur.execute(
                    """
                    INSERT INTO harvest_market_deliveries
                    (sale_id, receiver_user_id, delivery_kind, harvest_id, quantity, gold)
                    VALUES (%s,%s,'harvest',%s,%s,0)
                    RETURNING delivery_id
                    """,
                    (sale_id, buyer_user_id, str(row[3]), quantity),
                )
                buyer_delivery_id = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO harvest_market_deliveries
                    (sale_id, receiver_user_id, delivery_kind, harvest_id, quantity, gold)
                    VALUES (%s,%s,'gold',NULL,0,%s)
                    RETURNING delivery_id
                    """,
                    (sale_id, seller_user_id, total_price),
                )
                seller_delivery_id = int(cur.fetchone()[0])
            conn.commit()
        return True, "ok", "구매가 완료되었습니다.", {
            "sale_id": sale_id, "listing_id": listing_id,
            "seller_user_id": seller_user_id, "seller_nickname": str(row[2]),
            "buyer_user_id": buyer_user_id, "buyer_nickname": buyer_nickname[:12],
            "harvest_id": str(row[3]), "harvest_name": str(row[4]),
            "quantity": quantity, "unit_price": unit_price, "total_price": total_price,
            "buyer_delivery_id": buyer_delivery_id, "seller_delivery_id": seller_delivery_id,
        }
    except Exception as exc:
        print(f"[장터 구매 오류] {type(exc).__name__}: {exc}")
        return False, "market_db_error", "구매 처리 중 오류가 발생했어요.", {}


def load_market_sales_history(seller_user_id: str):
    if not DATABASE_URL:
        return [], "db_not_configured"
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT sale_id, buyer_nickname, harvest_id, harvest_name,
                           quantity, unit_price, total_price, created_at
                    FROM harvest_market_sales
                    WHERE seller_user_id=%s
                    ORDER BY created_at DESC, sale_id DESC
                    LIMIT %s
                    """,
                    (seller_user_id, MARKET_HISTORY_LIMIT),
                )
                items=[]
                for r in cur.fetchall():
                    items.append({
                        "sale_id": int(r[0]), "buyer_nickname": str(r[1]),
                        "harvest_id": str(r[2]), "harvest_name": str(r[3]),
                        "quantity": int(r[4]), "unit_price": int(r[5]),
                        "total_price": int(r[6]),
                        "created_at": r[7].isoformat() if r[7] else "",
                    })
                return items, ""
    except Exception as exc:
        print(f"[장터 기록 오류] {type(exc).__name__}: {exc}")
        return [], "market_db_error"


def load_market_pending_deliveries(user_id: str):
    if not DATABASE_URL:
        return [], "db_not_configured"
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT delivery_id, sale_id, delivery_kind,
                           COALESCE(harvest_id,''), quantity, gold, created_at
                    FROM harvest_market_deliveries
                    WHERE receiver_user_id=%s AND claimed_at IS NULL
                    ORDER BY delivery_id ASC
                    """,
                    (user_id,),
                )
                items=[]
                for r in cur.fetchall():
                    items.append({
                        "delivery_id": int(r[0]), "sale_id": int(r[1]),
                        "kind": str(r[2]), "harvest_id": str(r[3]),
                        "quantity": int(r[4]), "gold": int(r[5]),
                        "created_at": r[6].isoformat() if r[6] else "",
                    })
                return items, ""
    except Exception as exc:
        print(f"[장터 수령함 오류] {type(exc).__name__}: {exc}")
        return [], "market_db_error"


def ack_market_delivery(user_id: str, delivery_id: int):
    if not DATABASE_URL:
        return False, "db_not_configured"
    try:
        with get_account_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE harvest_market_deliveries
                    SET claimed_at=NOW()
                    WHERE delivery_id=%s AND receiver_user_id=%s AND claimed_at IS NULL
                    """,
                    (delivery_id, user_id),
                )
                changed = cur.rowcount == 1
            conn.commit()
        return (True, "ok") if changed else (False, "delivery_not_found")
    except Exception as exc:
        print(f"[장터 수령확인 오류] {type(exc).__name__}: {exc}")
        return False, "market_db_error"


async def handle_market_list(ws, payload: dict):
    if not await require_registered_account(ws): return
    state=clients[ws]; uid=str(state.get("account_user_id", ""))
    items, code = load_market_listings(uid, str(payload.get("query", "")))
    await send_json(ws, {"type":"market_list_result","ok":not bool(code),"code":code or "ok","items":items})


async def handle_market_my_list(ws, _payload: dict):
    if not await require_registered_account(ws): return
    uid=str(clients[ws].get("account_user_id", ""))
    items, code=load_my_market_listings(uid)
    await send_json(ws,{"type":"market_my_list_result","ok":not bool(code),"code":code or "ok","items":items})


async def handle_market_register(ws, payload: dict):
    if not await require_registered_account(ws): return
    state=clients[ws]; uid=str(state.get("account_user_id", "")); nick=str(state.get("nickname", "정원사"))
    ok,code,msg,lid=create_market_listing(uid,nick,payload)
    await send_json(ws,{"type":"market_register_result","ok":ok,"code":code,"message":msg,"listing_id":lid,"client_token":str(payload.get("client_token", ""))})


async def handle_market_cancel(ws, payload: dict):
    if not await require_registered_account(ws): return
    try: lid=int(payload.get("listing_id",0))
    except (TypeError,ValueError): lid=0
    uid=str(clients[ws].get("account_user_id", ""))
    ok,code,msg,item=cancel_market_listing(uid,lid)
    await send_json(ws,{"type":"market_cancel_result","ok":ok,"code":code,"message":msg,"listing_id":lid,"return_item":item})


async def handle_market_purchase(ws, payload: dict):
    if not await require_registered_account(ws): return
    try: lid=int(payload.get("listing_id",0))
    except (TypeError,ValueError): lid=0
    state=clients[ws]; uid=str(state.get("account_user_id", "")); nick=str(state.get("nickname", "정원사"))
    ok,code,msg,sale=purchase_market_listing(uid,nick,lid,str(payload.get("password", "")))
    await send_json(ws,{"type":"market_purchase_result","ok":ok,"code":code,"message":msg,"sale":sale})
    if ok:
        seller_uid=str(sale.get("seller_user_id", ""))
        await notify_account_user_id(seller_uid,{"type":"market_sold_notice","sale":sale,"message":f"{nick}님이 {sale.get('harvest_name','수확물')} {sale.get('quantity',0)}개를 구매했어요."})


async def handle_market_history(ws, _payload: dict):
    if not await require_registered_account(ws): return
    uid=str(clients[ws].get("account_user_id", ""))
    items,code=load_market_sales_history(uid)
    await send_json(ws,{"type":"market_history_result","ok":not bool(code),"code":code or "ok","items":items})


async def handle_market_deliveries(ws, _payload: dict):
    if not await require_registered_account(ws): return
    uid=str(clients[ws].get("account_user_id", ""))
    items,code=load_market_pending_deliveries(uid)
    await send_json(ws,{"type":"market_deliveries_result","ok":not bool(code),"code":code or "ok","items":items})


async def handle_market_delivery_ack(ws, payload: dict):
    if not await require_registered_account(ws): return
    try: did=int(payload.get("delivery_id",0))
    except (TypeError,ValueError): did=0
    uid=str(clients[ws].get("account_user_id", ""))
    ok,code=ack_market_delivery(uid,did)
    await send_json(ws,{"type":"market_delivery_ack_result","ok":ok,"code":code,"delivery_id":did})


async def notify_account_user_id(user_id: str, payload: dict):
    """광장 입장 여부와 무관하게 계정등록된 현재 연결에 알립니다."""
    targets = []
    for ws, state in list(clients.items()):
        account_match = (
            state.get("account_registered")
            and state.get("account_user_id") == user_id
        )
        plaza_match = (
            state.get("joined")
            and state.get("user_id") == user_id
        )
        if account_match or plaza_match:
            targets.append(ws)

    if not targets:
        return

    await asyncio.gather(
        *(send_json(ws, payload) for ws in targets),
        return_exceptions=True,
    )


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
                "message": "먼저 플라워가든 채팅에 연결해주세요.",
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

    if bool(message_item.get("whisper", False)):
        whisper_sender = str(message_item.get("user_id", ""))
        whisper_target = str(message_item.get("target_user_id", ""))
        reporter_user_id = str(state.get("user_id", ""))
        if reporter_user_id not in {whisper_sender, whisper_target}:
            await send_json(
                ws,
                {
                    "type": "report_result",
                    "ok": False,
                    "message": "신고할 수 없는 메시지예요.",
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
                "신고 누적으로 채팅이 "
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
                "운영자에 의해 채팅이 "
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
        "account_registered": False,
        "account_user_id": "",
        "last_account_lookup": 0.0,
        "last_transfer_restore_attempt": 0.0,
        "loaded_transfer_code": "",
        "admin_authenticated": False,
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

            if msg_type == "admin_auth":
                await handle_admin_auth(ws, payload)

            elif msg_type == "admin_user_search":
                await handle_admin_user_search(ws, payload)

            elif msg_type == "admin_reward_send":
                await handle_admin_reward_send(ws, payload)

            elif msg_type == "admin_reward_history":
                await handle_admin_reward_history(ws, payload)

            elif msg_type == "admin_coupon_create":
                await handle_admin_coupon_create(ws, payload)

            elif msg_type == "admin_coupon_list":
                await handle_admin_coupon_list(ws, payload)

            elif msg_type == "admin_coupon_disable":
                await handle_admin_coupon_disable(ws, payload)

            elif msg_type == "coupon_redeem":
                await handle_coupon_redeem(ws, payload)

            elif msg_type == "operator_reward_inbox":
                await handle_operator_reward_inbox(ws, payload)

            elif msg_type == "operator_reward_ack":
                await handle_operator_reward_ack(ws, payload)

            elif msg_type == "operator_status":
                await handle_operator_status(ws, payload)

            elif msg_type == "join":
                await handle_join(ws, payload)

            elif msg_type == "account_register":
                await handle_account_register(ws, payload)

            elif msg_type == "account_lookup":
                await handle_account_lookup(ws, payload)

            elif msg_type == "friend_request":
                await handle_friend_request(ws, payload)

            elif msg_type == "friend_list":
                await handle_friend_list(ws, payload)

            elif msg_type == "friend_remove":
                await handle_friend_remove(ws, payload)

            elif msg_type == "friend_recommendations":
                await handle_friend_recommendations(ws, payload)

            elif msg_type == "friend_response":
                await handle_friend_response(ws, payload)

            elif msg_type == "guestbook_load":
                await handle_guestbook_load(ws, payload)

            elif msg_type == "guestbook_post":
                await handle_guestbook_post(ws, payload)

            elif msg_type == "guestbook_delete":
                await handle_guestbook_delete(ws, payload)

            elif msg_type == "garden_sync":
                await handle_garden_sync(ws, payload)

            elif msg_type == "garden_load":
                await handle_garden_load(ws, payload)

            elif msg_type == "garden_steal":
                await handle_garden_steal(ws, payload)

            elif msg_type == "together_garden_sync":
                await handle_together_garden_sync(ws, payload)

            elif msg_type == "together_garden_harvest":
                await handle_together_garden_harvest(ws, payload)

            elif msg_type == "together_garden_reward_ack":
                await handle_together_garden_reward_ack(ws, payload)

            elif msg_type == "transfer_backup":
                await handle_transfer_backup(ws, payload)

            elif msg_type == "transfer_restore":
                await handle_transfer_restore(ws, payload)

            elif msg_type == "transfer_complete":
                await handle_transfer_complete(ws, payload)

            elif msg_type == "gift_send":
                await handle_gift_send(ws, payload)

            elif msg_type == "gift_inbox":
                await handle_gift_inbox(ws, payload)

            elif msg_type == "gift_history":
                await handle_gift_history(ws, payload)

            elif msg_type == "gift_claim":
                await handle_gift_claim(ws, payload)

            elif msg_type == "market_list":
                await handle_market_list(ws, payload)

            elif msg_type == "market_my_list":
                await handle_market_my_list(ws, payload)

            elif msg_type == "market_register":
                await handle_market_register(ws, payload)

            elif msg_type == "market_cancel":
                await handle_market_cancel(ws, payload)

            elif msg_type == "market_purchase":
                await handle_market_purchase(ws, payload)

            elif msg_type == "market_history":
                await handle_market_history(ws, payload)

            elif msg_type == "market_deliveries":
                await handle_market_deliveries(ws, payload)

            elif msg_type == "market_delivery_ack":
                await handle_market_delivery_ack(ws, payload)

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
    if account_db_ready:
        run_one_time_account_cleanup()
        run_one_time_hera_account_cleanup()
        run_one_time_together_garden_reward_recovery()

    print("=" * 60)
    print(" FlowerGarden 서버 2026-09-22 / 메인 전체채팅 + @귓속말 / 함께하는 정원 유지")
    print("[MAIN_CHAT_20260922] 메인 전체채팅 + @귓속말 + 최근 50개 저장 적용")
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
    print(
        "운영자 보상 시스템: 준비"
        if ADMIN_PASSWORD
        else "운영자 보상 시스템: ADMIN_PASSWORD 설정 대기"
    )
    print("=" * 60)

    async with serve(
        handle_client,
        HOST,
        PORT,
        ping_interval=20,
        ping_timeout=20,
        max_size=2 * 1024 * 1024,
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
