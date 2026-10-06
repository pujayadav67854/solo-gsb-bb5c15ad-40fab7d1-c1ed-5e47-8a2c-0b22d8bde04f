"""授权间转拨功能与并发安全测试（真实 PostgreSQL + TestClient）。

覆盖需求：
- 同一素材、地区、渠道的两条不同授权间转拨未占用发行次数（时段可不同）；
  仅调整两端 max_count，既有申请与预留的归属不变。
- 受理条件：两端均启用且通道未冻结；先在通道锁内结算到期预留并按原
  顺序处理候补，再以 max_count-used_count-reserved_count 判定可转出量。
- 整笔拒绝（两端额度均不变）：可转出量不足、转出后源额度非正、
  授权不存在（404）、维度不符、未启用、通道冻结、两端相同。
- 成功后原子更新两端 max_count，同事务重算候补：新增余量优先供仍有效
  的队首，过期或失配者依现有规则出队。
- 幂等：相同操作号+相同参数重放返回首次转拨结果（不受随后冻结/停用/
  后续转拨影响）；相同操作号+不同参数 409；并发不超额、不重复转拨。

运行：
    DATABASE_URL=postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439 \
        python3 -m tests.test_transfer
"""
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import func
from sqlalchemy import update as sa_update

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439",
)

from app.database import Base, SessionLocal, engine, init_db  # noqa: E402
from app.main import app  # noqa: E402
from app import models  # noqa: E402

CST = timezone(timedelta(hours=8))
OCT_START = "2026-10-01T00:00:00+08:00"
OCT_END = "2026-11-01T00:00:00+08:00"
OCCUR = "2026-10-15T12:00:00+08:00"

passed = 0
failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}  {detail}")


# ------------------------------------------------------------- 构造辅助
def make_material(c, code):
    return c.post(
        "/api/v1/materials", json={"code": code, "name": code}
    ).json()["id"]


def make_auth(c, mid, count, region="CN", channel="web",
              starts=OCT_START, ends=OCT_END):
    return c.post(
        "/api/v1/authorizations",
        json={
            "material_id": mid,
            "region": region,
            "channel": channel,
            "starts_at": starts,
            "ends_at": ends,
            "max_count": count,
        },
    ).json()["id"]


def apply(c, mids, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/distributions",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
        },
    )


def reserve(c, mids, ttl=15, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/reservations",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
            "ttl_minutes": ttl,
        },
    )


def waitlist(c, mids, ttl=120, region="CN", channel="web", occur=OCCUR):
    return c.post(
        "/api/v1/waitlists",
        json={
            "material_ids": mids,
            "region": region,
            "channel": channel,
            "occur_at": occur,
            "ttl_minutes": ttl,
        },
    )


def transfer(c, op, source, target, count):
    return c.post(
        "/api/v1/authorizations/transfer",
        json={
            "operation_no": op,
            "source_authorization_id": source,
            "target_authorization_id": target,
            "count": count,
        },
    )


def freeze(c, region, channel, reason="合规检查临时冻结"):
    return c.put(
        "/api/v1/channel-freezes",
        json={"region": region, "channel": channel,
              "action": "freeze", "reason": reason},
    )


def resume(c, region, channel):
    return c.put(
        "/api/v1/channel-freezes",
        json={"region": region, "channel": channel, "action": "resume"},
    )


def auth_view(c, aid):
    return c.get(f"/api/v1/authorizations/{aid}").json()


def set_status(c, aid, status):
    return c.patch(
        f"/api/v1/authorizations/{aid}", json={"status": status}
    )


def force_expire_reservation(reservation_id):
    db = SessionLocal()
    try:
        db.execute(
            sa_update(models.Reservation)
            .where(models.Reservation.id == reservation_id)
            .values(expires_at=func.now())
        )
        db.commit()
    finally:
        db.close()


def force_expire_waitlist(waitlist_id):
    db = SessionLocal()
    try:
        db.execute(
            sa_update(models.Waitlist)
            .where(models.Waitlist.id == waitlist_id)
            .values(expires_at=func.now())
        )
        db.commit()
    finally:
        db.close()


# ------------------------------------------------------------- 场景
def scenario_basic_and_snapshot():
    print("== 1. 基本转拨：两端 max_count 原子更新，归属不变，返回快照 ==")
    c = TestClient(app)
    m = make_material(c, "t1-m")
    # 两条授权时段不同（转拨不要求时段一致）
    a = make_auth(c, m, 5, channel="t1", ends="2026-11-01T00:00:00+08:00")
    b = make_auth(c, m, 3, channel="t1", ends="2026-12-01T00:00:00+08:00")

    # 源授权形成正式占用与未到期预留（归属在转拨后必须保持不变）
    d = apply(c, [m], channel="t1").json()
    rv = reserve(c, [m], channel="t1").json()
    assert d["items"][0]["authorization"]["id"] == a
    assert rv["items"][0]["authorization"]["id"] == a

    r = transfer(c, "OP-T1-1", a, b, 2)
    check("transfer 200", r.status_code == 200, r.text)
    body = r.json()
    check("operation_no echoed", body["operation_no"] == "OP-T1-1", body)
    check("count echoed", body["count"] == 2, body)
    check("created_at present", bool(body.get("created_at")), body)

    src, tgt = body["source_authorization"], body["target_authorization"]
    check("source snapshot id", src["id"] == a, src)
    check("source max 5-2=3", src["max_count"] == 3, src)
    check("source used unchanged", src["used_count"] == 1, src)
    check("source reserved unchanged", src["reserved_count"] == 1, src)
    check("source remaining", src["remaining"] == 2, src)
    check("source reservable", src["reservable_remaining"] == 1, src)
    check("target snapshot id", tgt["id"] == b, tgt)
    check("target max 3+2=5", tgt["max_count"] == 5, tgt)
    check("target used unchanged", tgt["used_count"] == 0, tgt)

    # 库内状态与快照一致；既有申请/预留归属不变
    av = auth_view(c, a)
    check("db source max", av["max_count"] == 3, av)
    check("db source used/reserved",
          av["used_count"] == 1 and av["reserved_count"] == 1, av)
    bv = auth_view(c, b)
    check("db target max", bv["max_count"] == 5, bv)
    d2 = c.get(f"/api/v1/distributions/{d['id']}").json()
    check("distribution still on source",
          d2["items"][0]["authorization"]["id"] == a, d2)
    rv2 = c.get(f"/api/v1/reservations/{rv['id']}").json()
    check("reservation still on source",
          rv2["items"][0]["authorization"]["id"] == a, rv2)


def scenario_idempotent_replay():
    print("== 2. 幂等：相同操作号+参数重放返回首次结果，不重复转拨 ==")
    c = TestClient(app)
    m = make_material(c, "t2-m")
    a = make_auth(c, m, 5, channel="t2")
    b = make_auth(c, m, 2, channel="t2")

    r1 = transfer(c, "OP-T2-1", a, b, 2)
    check("first 200", r1.status_code == 200, r1.text)
    first = r1.json()

    for i in range(2):
        r = transfer(c, "OP-T2-1", a, b, 2)
        check(f"replay {i+1} 200", r.status_code == 200, r.text)
        check(f"replay {i+1} identical", r.json() == first,
              f"{r.json()} vs {first}")

    av = auth_view(c, a)
    bv = auth_view(c, b)
    check("source max moved exactly once",
          av["max_count"] == 3, av)
    check("target max moved exactly once",
          bv["max_count"] == 4, bv)

    # 后续状态变化（再次转拨/冻结/停用）不影响重放：仍返回首次快照
    r2 = transfer(c, "OP-T2-2", a, b, 1)
    check("second transfer 200", r2.status_code == 200, r2.text)
    freeze(c, "CN", "t2")
    set_status(c, a, "inactive")
    set_status(c, b, "inactive")
    r = transfer(c, "OP-T2-1", a, b, 2)
    check("replay after freeze+inactive 200",
          r.status_code == 200, r.text)
    check("replay returns first snapshot",
          r.json() == first, f"{r.json()} vs {first}")
    resume(c, "CN", "t2")


def scenario_param_conflict():
    print("== 3. 相同操作号+不同参数：409 冲突 ==")
    c = TestClient(app)
    m = make_material(c, "t3-m")
    a = make_auth(c, m, 9, channel="t3")
    b = make_auth(c, m, 2, channel="t3")
    d = make_auth(c, m, 2, channel="t3")

    r = transfer(c, "OP-T3-1", a, b, 1)
    check("first 200", r.status_code == 200, r.text)

    r = transfer(c, "OP-T3-1", a, b, 2)
    check("different count 409", r.status_code == 409, r.text)
    check("conflict code",
          r.json()["error"]["code"] == "transfer_operation_conflict",
          r.text)
    r = transfer(c, "OP-T3-1", a, d, 1)
    check("different target 409", r.status_code == 409, r.text)
    r = transfer(c, "OP-T3-1", b, a, 1)
    check("swapped source/target 409", r.status_code == 409, r.text)

    # 冲突不影响已落库结果：原参数重放仍返回首次结果
    r = transfer(c, "OP-T3-1", a, b, 1)
    check("original replay still 200", r.status_code == 200, r.text)
    check("source max moved once", auth_view(c, a)["max_count"] == 8)


def scenario_rejections():
    print("== 4. 整笔拒绝：不足/非正/不存在/维度/相同/停用/冻结 ==")
    c = TestClient(app)
    m = make_material(c, "t4-m")
    m2 = make_material(c, "t4-m2")
    a = make_auth(c, m, 3, channel="t4")
    b = make_auth(c, m, 2, channel="t4")

    # 形成占用与预留：available = 3 - 1 - 1 = 1
    apply(c, [m], channel="t4")
    rv = reserve(c, [m], channel="t4").json()

    r = transfer(c, "OP-T4-1", a, b, 2)
    check("insufficient 422", r.status_code == 422, r.text)
    check("insufficient code",
          r.json()["error"]["code"] == "transfer_rejected", r.text)
    check("insufficient reason",
          r.json()["error"]["details"][0]["reason"] == "insufficient_quota",
          r.text)
    check("insufficient leaves quota",
          auth_view(c, a)["max_count"] == 3
          and auth_view(c, b)["max_count"] == 2)

    # 可转出量充足（used=reserved=0）但转出后源额度非正：count == max
    a2 = make_auth(c, m, 2, channel="t4")
    b2 = make_auth(c, m, 2, channel="t4")
    r = transfer(c, "OP-T4-2", a2, b2, 2)
    check("nonpositive 422", r.status_code == 422, r.text)
    check("nonpositive reason",
          r.json()["error"]["details"][0]["reason"]
          == "source_quota_nonpositive", r.text)
    check("nonpositive leaves quota",
          auth_view(c, a2)["max_count"] == 2
          and auth_view(c, b2)["max_count"] == 2)
    # 边界：max-count=1 仍可转拨成功
    r = transfer(c, "OP-T4-2b", a2, b2, 1)
    check("max-count=1 allowed", r.status_code == 200
          and r.json()["source_authorization"]["max_count"] == 1, r.text)

    # 授权不存在
    r = transfer(c, "OP-T4-3", 999999, b, 1)
    check("missing source 404", r.status_code == 404, r.text)
    check("missing source code",
          r.json()["error"]["code"] == "not_found", r.text)
    r = transfer(c, "OP-T4-4", a, 999999, 1)
    check("missing target 404", r.status_code == 404, r.text)

    # 维度不符：素材 / 地区 / 渠道
    other_material = make_auth(c, m2, 2, channel="t4")
    other_region = make_auth(c, m, 2, region="US", channel="t4")
    other_channel = make_auth(c, m, 2, channel="t4x")
    for name, tgt in (("material", other_material),
                      ("region", other_region),
                      ("channel", other_channel)):
        r = transfer(c, f"OP-T4-{name}", a, tgt, 1)
        check(f"{name} mismatch 422", r.status_code == 422, r.text)
        check(f"{name} mismatch reason",
              r.json()["error"]["details"][0]["reason"]
              == "dimension_mismatch", r.text)

    # 两端相同
    r = transfer(c, "OP-T4-same", a, a, 1)
    check("same auth 422", r.status_code == 422, r.text)
    check("same auth reason",
          r.json()["error"]["details"][0]["reason"] == "same_authorization",
          r.text)

    # 未启用：源停用 / 目标停用
    set_status(c, a, "inactive")
    r = transfer(c, "OP-T4-5", a, b, 1)
    check("source inactive 422", r.status_code == 422, r.text)
    check("source inactive reason",
          r.json()["error"]["details"][0]["reason"]
          == "authorization_inactive", r.text)
    set_status(c, a, "active")
    set_status(c, b, "inactive")
    r = transfer(c, "OP-T4-6", a, b, 1)
    check("target inactive 422", r.status_code == 422, r.text)
    set_status(c, b, "active")

    # 通道冻结
    freeze(c, "CN", "t4")
    r = transfer(c, "OP-T4-7", a, b, 1)
    check("frozen 422", r.status_code == 422, r.text)
    check("frozen code",
          r.json()["error"]["code"] == "channel_frozen", r.text)
    resume(c, "CN", "t4")

    # 全部拒绝均未改变两端额度
    check("all rejections leave quota",
          auth_view(c, a)["max_count"] == 3
          and auth_view(c, b)["max_count"] == 2)


def scenario_settle_before_judge():
    print("== 5. 先在通道锁内结算到期预留，再判定可转出量 ==")
    c = TestClient(app)
    m = make_material(c, "t5-m")
    a = make_auth(c, m, 2, channel="t5")
    b = make_auth(c, m, 1, channel="t5")
    apply(c, [m], channel="t5")                    # a: used=1
    rv = reserve(c, [m], ttl=1, channel="t5").json()  # a: reserved=1
    force_expire_reservation(rv["id"])             # 已到期未结算

    # 可转出量须先结算到期预留：2 - 1 - 0 = 1，转拨 1 成功
    r = transfer(c, "OP-T5-1", a, b, 1)
    check("transfer after settle 200", r.status_code == 200, r.text)
    rv2 = c.get(f"/api/v1/reservations/{rv['id']}").json()
    check("reservation settled expired",
          rv2["status"] == "expired", rv2)
    av = auth_view(c, a)
    check("source max 2-1=1, reserved released",
          av["max_count"] == 1 and av["reserved_count"] == 0
          and av["used_count"] == 1, av)


def scenario_waitlist_served_by_new_quota():
    print("== 6. 转拨后同事务重算候补：新增余量优先供仍有效队首 ==")
    c = TestClient(app)
    m = make_material(c, "t6-m")
    # 源授权只覆盖 10 月上旬；目标授权只覆盖 10 月下旬（时段可不同）
    a = make_auth(c, m, 2, channel="t6",
                  starts="2026-10-01T00:00:00+08:00",
                  ends="2026-10-10T00:00:00+08:00")
    b = make_auth(c, m, 1, channel="t6",
                  starts="2026-10-20T00:00:00+08:00",
                  ends="2026-10-31T00:00:00+08:00")
    r = apply(c, [m], channel="t6", occur="2026-10-25T12:00:00+08:00")
    check("target filled", r.status_code == 201, r.text)   # b: used=1 满

    # 候补只可能落在目标授权上（源授权不覆盖其发行时刻），额度不足入队
    w = waitlist(c, [m], channel="t6", occur="2026-10-25T12:00:00+08:00")
    check("waitlist pending", w.status_code == 201
          and w.json()["status"] == "pending", w.text)
    wid = w.json()["id"]

    # 转拨 1 次：目标 max 1→2，同事务重算候补 -> 队首用新增余量成交
    r = transfer(c, "OP-T6-1", a, b, 1)
    check("transfer 200", r.status_code == 200, r.text)
    check("target max 1+1=2",
          r.json()["target_authorization"]["max_count"] == 2, r.text)
    w2 = c.get(f"/api/v1/waitlists/{wid}").json()
    check("waitlist fulfilled by new quota",
          w2["status"] == "fulfilled", w2)
    dist = w2["distribution"]
    check("fulfilled on target auth",
          dist["items"][0]["authorization"]["id"] == b, dist)
    bv = auth_view(c, b)
    check("target used 2/2 after fulfill",
          bv["used_count"] == 2 and bv["max_count"] == 2, bv)


def scenario_waitlist_expire_and_fail():
    print("== 7. 重算候补：过期队首出队、失配候补失败，后续按序成交 ==")
    c = TestClient(app)
    m = make_material(c, "t7-m")
    # 源授权（10 月上旬）与目标授权（10 月下旬）时段不同
    a = make_auth(c, m, 3, channel="t7",
                  starts="2026-10-01T00:00:00+08:00",
                  ends="2026-10-10T00:00:00+08:00")
    b = make_auth(c, m, 1, channel="t7",
                  starts="2026-10-20T00:00:00+08:00",
                  ends="2026-10-31T00:00:00+08:00")
    apply(c, [m], channel="t7", occur="2026-10-25T12:00:00+08:00")  # b 满

    # 队首 W1 将被强制过期；W2 仍有效，等待目标授权新增余量
    w1 = waitlist(c, [m], ttl=5, channel="t7",
                  occur="2026-10-25T12:00:00+08:00").json()
    w2 = waitlist(c, [m], ttl=120, channel="t7",
                  occur="2026-10-25T12:00:00+08:00").json()
    check("w1 pending", w1["status"] == "pending", w1)
    check("w2 pending", w2["status"] == "pending", w2)
    force_expire_waitlist(w1["id"])

    r = transfer(c, "OP-T7-1", a, b, 1)
    check("transfer 200", r.status_code == 200, r.text)
    w1v = c.get(f"/api/v1/waitlists/{w1['id']}").json()
    check("expired head dequeued",
          w1v["status"] == "expired"
          and w1v["expiry_reason"] == "ttl_expired", w1v)
    w2v = c.get(f"/api/v1/waitlists/{w2['id']}").json()
    check("next valid fulfilled",
          w2v["status"] == "fulfilled", w2v)

    # 失配出队：另一通道，目标授权被停用后候补不再匹配 -> failed
    m2 = make_material(c, "t7-m2")
    g = make_auth(c, m2, 2, channel="t8",
                  starts="2026-10-01T00:00:00+08:00",
                  ends="2026-10-10T00:00:00+08:00")
    h = make_auth(c, m2, 1, channel="t8",
                  starts="2026-10-20T00:00:00+08:00",
                  ends="2026-10-31T00:00:00+08:00")
    k = make_auth(c, m2, 1, channel="t8",
                  starts="2026-10-01T00:00:00+08:00",
                  ends="2026-10-10T00:00:00+08:00")
    apply(c, [m2], channel="t8", occur="2026-10-25T12:00:00+08:00")  # h 满
    w3 = waitlist(c, [m2], channel="t8",
                  occur="2026-10-25T12:00:00+08:00").json()
    check("w3 pending", w3["status"] == "pending", w3)
    set_status(c, h, "inactive")  # 停用后 w3 不再匹配（不重算，留待转拨）

    r = transfer(c, "OP-T8-1", g, k, 1)
    check("transfer 200 (t8)", r.status_code == 200, r.text)
    w3v = c.get(f"/api/v1/waitlists/{w3['id']}").json()
    check("mismatched waitlist failed",
          w3v["status"] == "failed"
          and w3v["failure_reason"] == "authorization_inactive", w3v)


def scenario_validation():
    print("== 8. 请求体校验：次数/操作号/编号非法一律 422 ==")
    c = TestClient(app)
    m = make_material(c, "t9-m")
    a = make_auth(c, m, 3, channel="t9")
    b = make_auth(c, m, 2, channel="t9")

    bad_bodies = [
        ("count zero", {"operation_no": "V1", "source_authorization_id": a,
                        "target_authorization_id": b, "count": 0}),
        ("count negative", {"operation_no": "V2", "source_authorization_id": a,
                            "target_authorization_id": b, "count": -2}),
        ("count missing", {"operation_no": "V3",
                           "source_authorization_id": a,
                           "target_authorization_id": b}),
        ("count non-integer", {"operation_no": "V4",
                               "source_authorization_id": a,
                               "target_authorization_id": b, "count": 1.5}),
        ("operation missing", {"source_authorization_id": a,
                               "target_authorization_id": b, "count": 1}),
        ("operation blank", {"operation_no": "   ",
                             "source_authorization_id": a,
                             "target_authorization_id": b, "count": 1}),
        ("source nonpositive", {"operation_no": "V7",
                                "source_authorization_id": 0,
                                "target_authorization_id": b, "count": 1}),
        ("target nonpositive", {"operation_no": "V8",
                                "source_authorization_id": a,
                                "target_authorization_id": -1, "count": 1}),
    ]
    for name, body in bad_bodies:
        r = c.post("/api/v1/authorizations/transfer", json=body)
        check(f"{name} 422", r.status_code == 422, r.text)

    # 全部校验失败均不产生转拨：额度不变
    check("validation leaves quota",
          auth_view(c, a)["max_count"] == 3
          and auth_view(c, b)["max_count"] == 2)


def scenario_concurrent_same_operation():
    print("== 9. 并发：相同操作号+参数并发，恰转拨一次且全部返回首次结果 ==")
    c = TestClient(app)
    m = make_material(c, "t10-m")
    a = make_auth(c, m, 10, channel="t10")
    b = make_auth(c, m, 1, channel="t10")

    def one(_):
        cc = TestClient(app)
        return transfer(cc, "OP-T10-SAME", a, b, 1)

    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(one, range(8)))

    codes = [r.status_code for r in results]
    check("all 200", all(s == 200 for s in codes), codes)
    bodies = [r.json() for r in results]
    check("all identical first result",
          all(x == bodies[0] for x in bodies), bodies)
    av = auth_view(c, a)
    bv = auth_view(c, b)
    check("source moved exactly once",
          av["max_count"] == 9, av)
    check("target moved exactly once",
          bv["max_count"] == 2, bv)


def scenario_concurrent_distinct_operations():
    print("== 10. 并发：不同操作号并发转拨，绝不超额、源额度恒为正 ==")
    c = TestClient(app)
    m = make_material(c, "t11-m")
    a = make_auth(c, m, 5, channel="t11")   # 可转出上限 4（max 须保持 >= 1）
    b = make_auth(c, m, 1, channel="t11")

    def one(i):
        cc = TestClient(app)
        return transfer(cc, f"OP-T11-{i}", a, b, 1)

    with ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(one, range(10)))

    oks = [r for r in results if r.status_code == 200]
    rejects = [r for r in results if r.status_code == 422]
    check("exactly 4 succeed", len(oks) == 4,
          [r.status_code for r in results])
    check("rest rejected", len(rejects) == 6,
          [r.status_code for r in results])
    check("rejects are transfer_rejected",
          all(r.json()["error"]["code"] == "transfer_rejected"
              for r in rejects),
          [r.text for r in rejects])
    av = auth_view(c, a)
    bv = auth_view(c, b)
    check("source max 5-4=1 (still positive)", av["max_count"] == 1, av)
    check("target max 1+4=5", bv["max_count"] == 5, bv)
    check("invariant used+reserved<=max",
          av["used_count"] + av["reserved_count"] <= av["max_count"], av)


def scenario_concurrent_conflicting_params():
    print("== 11. 并发：相同操作号+不同参数，恰一笔成功、其余 409 ==")
    c = TestClient(app)
    m = make_material(c, "t12-m")
    a = make_auth(c, m, 30, channel="t12")
    b = make_auth(c, m, 1, channel="t12")

    def one(i):
        cc = TestClient(app)
        # 同一操作号、各不相同的转拨次数
        return transfer(cc, "OP-T12-CONFLICT", a, b, i + 1)

    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(one, range(8)))

    oks = [r for r in results if r.status_code == 200]
    conflicts = [r for r in results if r.status_code == 409]
    check("exactly one succeeds", len(oks) == 1,
          [r.status_code for r in results])
    check("others 409 conflict", len(conflicts) == 7,
          [r.status_code for r in results])
    check("conflict code",
          all(r.json()["error"]["code"] == "transfer_operation_conflict"
              for r in conflicts),
          [r.text for r in conflicts])
    won = oks[0].json()["count"]
    av = auth_view(c, a)
    check("only winner applied",
          av["max_count"] == 30 - won, f"{av} won={won}")


def scenario_consistency():
    print("== 12. 账实相符：全库授权不变量校验 ==")
    db = SessionLocal()
    try:
        auths = db.query(models.Authorization).all()
        ok = True
        for a in auths:
            used = db.query(models.DistributionItem).join(
                models.Distribution,
                models.DistributionItem.distribution_id
                == models.Distribution.id,
            ).filter(
                models.DistributionItem.authorization_id == a.id,
                models.Distribution.status == "approved",
            ).count()
            reserved = db.query(models.ReservationItem).join(
                models.Reservation,
                models.ReservationItem.reservation_id
                == models.Reservation.id,
            ).filter(
                models.ReservationItem.authorization_id == a.id,
                models.Reservation.status == "pending",
            ).count()
            if not (a.used_count == used and a.reserved_count == reserved
                    and a.used_count >= 0 and a.reserved_count >= 0
                    and a.used_count + a.reserved_count <= a.max_count
                    and a.max_count >= 1):
                ok = False
                print(f"    不一致：auth {a.id} max={a.max_count} "
                      f"used={a.used_count}/{used} "
                      f"reserved={a.reserved_count}/{reserved}")
        check("all authorizations consistent", ok)

        # 转拨记录快照结构自洽：两端授权对象齐全、源额度为正
        transfers = db.query(models.QuotaTransfer).all()
        snap_ok = all(
            t.result["source_authorization"]["max_count"] >= 1
            and t.result["source_authorization"]["id"]
            == t.source_authorization_id
            and t.result["target_authorization"]["id"]
            == t.target_authorization_id
            for t in transfers
        )
        check("transfer records present", len(transfers) > 0 and snap_ok)
    finally:
        db.close()


def main() -> int:
    Base.metadata.drop_all(bind=engine)
    init_db()

    scenario_basic_and_snapshot()
    scenario_idempotent_replay()
    scenario_param_conflict()
    scenario_rejections()
    scenario_settle_before_judge()
    scenario_waitlist_served_by_new_quota()
    scenario_waitlist_expire_and_fail()
    scenario_validation()
    scenario_concurrent_same_operation()
    scenario_concurrent_distinct_operations()
    scenario_concurrent_conflicting_params()
    scenario_consistency()

    print(f"\n结果：{passed} 通过，{failed} 失败")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
