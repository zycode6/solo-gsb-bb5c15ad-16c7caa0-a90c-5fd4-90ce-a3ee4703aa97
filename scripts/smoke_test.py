#!/usr/bin/env python3
"""端到端冒烟测试（仅依赖 Python 标准库）。

先启动服务（docker compose up --build），然后执行：
    python3 scripts/smoke_test.py [base_url]

覆盖场景：获取 / 冲突 / 校验 / 续租 / 释放 / 防栅序号单调递增 /
旧持有者在超时后无法影响新租约，以及批量申请的全成全败、
冲突时不写不递增、过期接管与凭据兼容，以及批量续租的全成全败、
逐项失败原因、失败时不改变任何 TTL、审计逐任务落账，以及租约交接：
换新凭据且防栅序号递增、保留原过期时刻不续期、旧凭据立即失效 /
新凭据立即可用、连续交接、三类 409 不改变租约与防栅序号；
以及租约审计流：
四类操作（含被拒 / 忽略）按任务键落账、事件字段正确、
cursor 分页不重不漏、非法 cursor/limit 拒绝、批量冲突仅冲突方落账、
按保留条数裁剪最旧事件；以及任务结果提交：
仅有效租约凭据可写 / 相同内容重放 / 同序号不同内容 409 且不改结果 /
旧持有者过期或被接管后无法覆盖 / 新租约可提交新结果 /
结果不随租约到期删除 / 并发同内容恰好提交一次 / 提交尝试逐次落审计；
以及全局任务结果变更流：
仅首次写入新结果版本（普通提交 / commit-checked）追加一条变更 /
失败（凭据 / result_conflict / stale_dependencies）与相同内容重放不追加 /
变更含 task_key、防栅序号、提交时间与当次 JSON 结果 / 跨任务键按写入全局
定序 / cursor + next_cursor 分页不重不漏 / 早于最早保留条目的游标 410 /
非法 cursor 400、limit 越界 422 / 路由优先于 {task_key} 捕获，以及游标裁剪
竞态：恰满一页（末页 next_cursor=null、满页可继续翻页、按写入顺序衔接不
重复）、小保留窗口下并发 XADD MAXLEN 持续裁剪时任何 200 页面与读取前快照
逐条衔接且失效游标仍返回 410（不静默漏读）、空流（含带游标）空页 200；
以及滑动窗口成本限流的
剩余额度 / 成本累计 / 超限 429 / 窗口滑动恢复 / 并发不超额 /
参数校验 / 过期数据自动清理，以及 request_id 幂等：
重试回放不重复扣减 / 并发相同 request_id 只扣一次 / 429 结果回放 /
request_id 跨窗口可复用 / 非法 request_id 拒绝；
以及加权拒绝的重试等待：limit=5 窗口内先后通过成本 1 与 4 后成本 3
被拒，retry_after_ms 按记录到期顺序累计释放、首次足以容纳本次 cost
（等到成本 4 滑出而非仅最早的成本 1），reset_at_ms 仍为最老记录滑出、
按旧等待时长重试仍 429 / 等到位后放行，同毫秒到期记录合并判断
（直连 Redis 构造同分记录验证）；
以及同一 client_key 混用不同 window_ms：长窗口放入的记录不被随后的
短窗口检查删除，切回长窗口后仍被计入并 429（窗口内累计超限时
remaining 按 0 返回、reset_at_ms 仍跟踪最老记录），最老记录滑出
长窗口后恢复放行；
以及任务依赖门禁：
无定义即可执行 / 保存去重且限 16 个 / 自身依赖 422 /
环检测只遍历现有定义且拒绝时保留原定义 / 空列表清除 /
ready 依据持久任务结果判定全部直接前置并返回未完成键 /
环检测单次访问节点上限 1024（超限拒绝且保留原定义，恰好 1024 放行）；
以及前置结果版本保护：
snapshot 单脚本读定义与各前置结果版本（pending 不出快照 / 无定义空快照 /
版本与 GET results 同源 / 按定义顺序）、
commit-checked 与普通提交相同的凭据 / 防栅 / 幂等语义、
前置版本漂移或定义变更 / 乱序 -> 409 stale_dependencies 且不改结果、
重新拉快照后成功、空快照（含缺省字段）等价普通提交、
快照参数校验 422、并发同内容恰好一次提交、stale_dependencies 落审计；
以及按资源键分配并发容量：
未配置即申请 409 not_configured / 设置容量 / 加权申请剩余递减 /
容量不足 429 不占用且带 Retry-After / 权重超容量 409 /
容量下调低于当前有效占用 409 below_active 且保留原配置 /
续期仅认凭据（错误 / 过期 409 not_found，不能复活旧占用）/
释放仅认凭据且幂等、旧凭据不影响新占用 / 短 TTL 过期自动回收且续期可延长 /
参数校验 / 并发（含加权）绝不超配；
以及已占用权重汇总：
升级前遗留的"有占用集合、缺汇总"数据在首次操作时于脚本内一次性重建、
申请 / 释放同步增减、到期清理逐笔扣减、集合自然过期 / 清空后汇总同步失效
且后续申请不被旧计数阻挡（直连 Redis 构造升级前数据布局验证，
连接地址可用 SMOKE_REDIS_HOST / SMOKE_REDIS_PORT 覆盖，键前缀用
SMOKE_KEY_PREFIX 覆盖，默认 lease）。
"""
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
FAILED = []


def call(method, path, body=None):
    status_code, headers, data = call_ex(method, path, body)
    return status_code, data


def call_ex(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.headers, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, e.headers, json.loads(e.read())


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f"  -> {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append(name)


def main():
    key = f"smoke:{int(time.time() * 1000)}"

    code, body = call("GET", "/healthz")
    check("healthz", code == 200 and body.get("status") == "ok", f"{code} {body}")

    # 1. 申请租约
    code, a = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire #1", code == 200 and a["acquired"] and a["token"], f"{code} {a}")
    token_a, fence_a = a["token"], a["fencing_token"]

    # 2. 占用期间再次申请 -> 409
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 1500})
    check("acquire conflict -> 409", code == 409 and not b["acquired"], f"{code} {b}")

    # 3. 校验当前持有者
    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("validate holder", code == 200 and v["valid"] and v["remaining_ms"] > 0,
          f"{code} {v}")

    # 4. 错误令牌续租 -> 409
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": "wrong", "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew with wrong token -> 409", code == 409 and not r["renewed"],
          f"{code} {r}")

    # 5. 正确续租
    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_a, "fencing_token": fence_a, "ttl_ms": 1500},
    )
    check("renew ok", code == 200 and r["renewed"], f"{code} {r}")

    # 6. 错误防栅序号释放 -> 幂等忽略
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a + 99},
    )
    check("release with wrong fencing ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    # 7. 正确释放
    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_a, "fencing_token": fence_a},
    )
    check("release ok", code == 200 and rel["released"], f"{code} {rel}")

    # 8. 重新申请，防栅序号必须单调递增
    code, b = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 800})
    check("re-acquire after release", code == 200 and b["acquired"], f"{code} {b}")
    check("fencing monotonic", b["fencing_token"] > fence_a,
          f"{b['fencing_token']} <= {fence_a}")
    token_b, fence_b = b["token"], b["fencing_token"]

    # 9. 等租约过期，新持有者可接管；旧持有者此后无法干扰
    time.sleep(1.0)
    code, c = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 5000})
    check("acquire after expiry", code == 200 and c["acquired"], f"{code} {c}")
    token_c, fence_c = c["token"], c["fencing_token"]

    code, r = call(
        "POST",
        "/v1/leases/renew",
        {"task_key": key, "token": token_b, "fencing_token": fence_b, "ttl_ms": 5000},
    )
    check("stale holder renew rejected", code == 409 and not r["renewed"],
          f"{code} {r}")

    code, rel = call(
        "POST",
        "/v1/leases/release",
        {"task_key": key, "token": token_b, "fencing_token": fence_b},
    )
    check("stale holder release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")

    code, v = call(
        "POST",
        "/v1/leases/validate",
        {"task_key": key, "token": token_c, "fencing_token": fence_c},
    )
    check("new lease unaffected", code == 200 and v["valid"], f"{code} {v}")

    # 10. 状态查询
    code, st = call("GET", f"/v1/leases/{key}")
    check("inspect", code == 200 and st["held"] and st["fencing_token"] == fence_c,
          f"{code} {st}")

    batch_checks()
    renew_batch_checks()
    transfer_checks()
    result_checks()
    result_changes_checks()
    audit_checks()
    limit_checks()
    dependency_checks()
    checked_commit_checks()
    capacity_checks()
    capacity_summary_checks()

    print()
    if FAILED:
        print(f"{len(FAILED)} check(s) FAILED: {', '.join(FAILED)}")
        sys.exit(1)
    print("All checks passed.")


def batch_checks():
    """acquire-batch：参数校验 / 全成 / 全败不写不递增 / 过期接管 / 凭据兼容。"""
    base = f"smoke-batch:{int(time.time() * 1000)}"
    k1, k2, k3, k4, k5 = [f"{base}:{i}" for i in range(5)]

    # 参数校验：少于 2 个 / 多于 32 个 / 有重复 -> 422
    code, _ = call("POST", "/v1/leases/acquire-batch", {"task_keys": [k1]})
    check("batch size < 2 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [f"{base}:x{i}" for i in range(33)]})
    check("batch size > 32 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k1], "ttl_ms": 5000})
    check("batch duplicate keys -> 422", code == 422, f"code={code}")

    # 全成：顺序一致、token 独立、fencing 独立递增、expires_at 同一基准
    code, b = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k2, k3], "ttl_ms": 5000})
    check("batch acquire ok", code == 200 and b["acquired"], f"{code} {b}")
    leases = b.get("leases", [])
    check("batch leases order/count",
          [l["task_key"] for l in leases] == [k1, k2, k3], f"{leases}")
    check("batch distinct tokens",
          len({l["token"] for l in leases}) == 3, f"{leases}")
    check("batch same expires baseline",
          len({l["expires_at_ms"] for l in leases}) == 1, f"{leases}")

    # 凭据兼容现有 validate 接口
    for l in leases:
        code, v = call("POST", "/v1/leases/validate",
                       {"task_key": l["task_key"], "token": l["token"],
                        "fencing_token": l["fencing_token"]})
        check(f"batch credential validate {l['task_key']}",
              code == 200 and v["valid"], f"{code} {v}")

    # 冲突：k3 仍被占用，k5 从未申请；应 409、仅报 k3，且 k5 不得被创建、计数器不得递增
    code, c = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k3, k5], "ttl_ms": 5000})
    check("batch conflict -> 409", code == 409 and not c.get("acquired"),
          f"{code} {c}")
    check("batch conflict keys",
          c.get("conflicting_task_keys") == [k3], f"{c}")
    code, st5 = call("GET", f"/v1/leases/{k5}")
    check("batch conflict created nothing",
          code == 200 and not st5["held"], f"{code} {st5}")
    # k5 从未申请成功；其第一次成功申请的 fencing 必须为 1，证明批量失败未 INCR
    code, a5 = call("POST", "/v1/leases/acquire", {"task_key": k5, "ttl_ms": 5000})
    check("batch conflict no fencing increment",
          code == 200 and a5["fencing_token"] == 1, f"{code} {a5}")

    # 全败时冲突方自己也不受影响：k3 的旧凭据仍可续租
    l3 = next(l for l in leases if l["task_key"] == k3)
    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": k3, "token": l3["token"],
                    "fencing_token": l3["fencing_token"], "ttl_ms": 5000})
    check("batch conflict holder untouched", code == 200 and r["renewed"],
          f"{code} {r}")

    # 释放整批后，另起一批；过期租约可被接管，全新键与过期键混在一起
    for l in leases:
        code, rel = call("POST", "/v1/leases/release",
                         {"task_key": l["task_key"], "token": l["token"],
                          "fencing_token": l["fencing_token"]})
        check(f"batch release {l['task_key']}",
              code == 200 and rel["released"], f"{code} {rel}")
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": k5, "token": a5["token"],
                      "fencing_token": a5["fencing_token"]})
    check("batch setup release k5", code == 200 and rel["released"], f"{code} {rel}")

    # k1 用短 TTL 单独占用，等待过期后与全新的 k4 一起批量接管
    code, old = call("POST", "/v1/leases/acquire", {"task_key": k1, "ttl_ms": 300})
    check("batch takeover setup", code == 200 and old["acquired"], f"{code} {old}")
    time.sleep(0.7)
    code, t = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [k1, k4], "ttl_ms": 5000})
    check("batch acquire after expiry", code == 200 and t["acquired"], f"{code} {t}")
    taken = {l["task_key"]: l for l in t.get("leases", [])}
    check("batch takeover fencing advanced",
          k1 in taken and taken[k1]["fencing_token"] > old["fencing_token"],
          f"{t.get('leases')}")
    check("batch free key fencing starts 1",
          taken.get(k4, {}).get("fencing_token") == 1, f"{t.get('leases')}")

    # 旧持有者释放不得影响批量接管后的新租约
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": k1, "token": old["token"],
                      "fencing_token": old["fencing_token"]})
    check("batch stale release ignored", code == 200 and not rel["released"],
          f"{code} {rel}")
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": k1, "token": taken[k1]["token"],
                    "fencing_token": taken[k1]["fencing_token"]})
    check("batch new lease valid", code == 200 and v["valid"], f"{code} {v}")


def renew_batch_checks():
    """renew-batch：参数校验 / 全成 / 全败逐项原因 / 失败不动任何 TTL / 审计落账 / 过期。"""
    base = f"smoke-renew-batch:{int(time.time() * 1000)}"
    path = "/v1/leases/renew-batch"
    k1, k2, k3, k4, k5 = [f"{base}:{i}" for i in range(5)]

    def item(key, token, fence):
        return {"task_key": key, "token": token, "fencing_token": fence}

    # 参数校验：少于 2 项 / 多于 32 项 / task_key 重复 -> 422
    code, _ = call("POST", path, {"items": [item(k1, "t", 1)]})
    check("renew-batch size < 2 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path, {
        "items": [item(f"{base}:x{i}", "t", 1) for i in range(33)]})
    check("renew-batch size > 32 -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path, {"items": [item(k1, "t", 1), item(k1, "t", 1)]})
    check("renew-batch duplicate keys -> 422", code == 422, f"code={code}")
    # ttl_ms 越界 -> 400（与单项续租一致，由服务端 resolve_ttl 判定）
    code, _ = call("POST", path, {"items": [item(k1, "t", 1), item(k2, "t", 1)],
                                  "ttl_ms": 999_999})
    check("renew-batch ttl out of range -> 400", code == 400, f"code={code}")

    # 准备 3 个有效租约（k1/k2/k3），k4 从不申请，k5 短 TTL 稍后过期
    creds = {}
    for k in (k1, k2, k3):
        code, a = call("POST", "/v1/leases/acquire", {"task_key": k, "ttl_ms": 5000})
        check("renew-batch setup acquire", code == 200 and a["acquired"], f"{code} {a}")
        creds[k] = (a["token"], a["fencing_token"])
    code, a5 = call("POST", "/v1/leases/acquire", {"task_key": k5, "ttl_ms": 300})
    check("renew-batch setup short lease", code == 200 and a5["acquired"], f"{code} {a5}")
    creds[k5] = (a5["token"], a5["fencing_token"])

    # 全成：200、顺序与输入一致、同一 expires 基准、租约确实被续到新时刻
    req_items = [item(k3, *creds[k3]), item(k1, *creds[k1]), item(k2, *creds[k2])]
    code, b = call("POST", path, {"items": req_items, "ttl_ms": 8000})
    check("renew-batch ok", code == 200 and b["renewed"], f"{code} {b}")
    leases = b.get("leases", [])
    check("renew-batch order/count",
          [l["task_key"] for l in leases] == [k3, k1, k2], f"{leases}")
    check("renew-batch same expires baseline",
          len({l["expires_at_ms"] for l in leases}) == 1, f"{leases}")
    new_expires = leases[0]["expires_at_ms"]
    code, st1 = call("GET", f"/v1/leases/{k1}")
    check("renew-batch persisted",
          code == 200 and st1["expires_at_ms"] == new_expires, f"{code} {st1}")
    # 凭据不变，单项 validate 仍然有效
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": k1, "token": creds[k1][0],
                    "fencing_token": creds[k1][1]})
    check("renew-batch credential still valid", code == 200 and v["valid"],
          f"{code} {v}")

    # 全败：k1 凭据错误(conflict)、k4 不存在(not_found)、k5 过期(expired/not_found)
    time.sleep(0.7)
    bad_items = [
        item(k1, "wrong-token", creds[k1][1]),
        item(k4, "ghost", 1),
        item(k5, *creds[k5]),
    ]
    code, f = call("POST", path, {"items": bad_items, "ttl_ms": 8000})
    check("renew-batch partial invalid -> 409",
          code == 409 and not f["renewed"], f"{code} {f}")
    failures = f.get("failures", [])
    check("renew-batch failure order/count",
          [x["index"] for x in failures] == [0, 1, 2]
          and [x["task_key"] for x in failures] == [k1, k4, k5],
          f"{failures}")
    by_key = {x["task_key"]: x["reason"] for x in failures}
    check("renew-batch reasons conflict/not_found",
          by_key[k1] == "conflict" and by_key[k4] == "not_found", f"{failures}")
    check("renew-batch expired reason",
          by_key[k5] in ("expired", "not_found"), f"{failures}")
    check("renew-batch no leases on failure", f.get("leases", []) == [], f"{f}")

    # 全败时不改变任何 TTL：k1（成功项）的 expires_at 必须仍是上一次成功续租的值
    code, st1 = call("GET", f"/v1/leases/{k1}")
    check("renew-batch failure changes no ttl",
          code == 200 and st1["expires_at_ms"] == new_expires, f"{code} {st1}")

    # 审计（此时只发生过一次成功批与一次失败批）：
    # 成功批次每项各落一条 renewed（batch_size=3）；失败批次仅失败项再各落一条
    # 原因事件，成功项 k2/k3 在失败批次中不落任何事件（k1 是失败项，应有两条）
    for k in (k1, k2, k3):
        code, page = fetch_events(k)
        renewed = [e for e in page["events"]
                   if e["op"] == "renew_batch" and e["result"] == "renewed"]
        check(f"renew-batch audit single renewed {k}",
              len(renewed) == 1 and renewed[0]["batch_size"] == 3
              and renewed[0]["fencing_token"] == creds[k][1], f"{page}")
    code, page = fetch_events(k1)
    check("renew-batch audit conflict event appended",
          [e["result"] for e in page["events"]
           if e["op"] == "renew_batch" and e["batch_size"] == 3]
          == ["renewed", "conflict"], f"{page}")
    check("renew-batch audit conflict fencing",
          next(e for e in page["events"] if e["result"] == "conflict")
          ["fencing_token"] == creds[k1][1], f"{page}")
    code, page2k = fetch_events(k2)
    check("renew-batch valid item no failure event",
          [e["result"] for e in page2k["events"] if e["op"] == "renew_batch"]
          == ["renewed"], f"{page2k}")
    code, page4 = fetch_events(k4)
    ev4 = [e for e in page4["events"] if e["op"] == "renew_batch"]
    check("renew-batch audit not_found event on new key",
          len(ev4) == 1 and ev4[0]["result"] == "not_found"
          and ev4[0]["fencing_token"] is None and ev4[0]["batch_size"] == 3,
          f"{page4}")

    # 失败批次后，用正确凭据重新整批续租仍可成功（k2 再落一条 size=2 的 renewed）
    code, b2 = call("POST", path,
                    {"items": [item(k1, *creds[k1]), item(k2, *creds[k2])],
                     "ttl_ms": 5000})
    check("renew-batch retry ok", code == 200 and b2["renewed"], f"{code} {b2}")
    code, page2 = fetch_events(k2)
    rb2 = [(e["result"], e["batch_size"])
           for e in page2["events"] if e["op"] == "renew_batch"]
    check("renew-batch success item untouched on failure batch",
          rb2 == [("renewed", 3), ("renewed", 2)], f"{page2}")

    # 防栅序号错误同样判为 conflict，且整批失败（k2 不续租）
    token1, fence1 = creds[k1]
    expires_k2_before = b2["leases"][1]["expires_at_ms"]
    code, f = call("POST", path,
                   {"items": [item(k1, token1, fence1 + 5), item(k2, *creds[k2])],
                    "ttl_ms": 5000})
    check("renew-batch wrong fencing -> 409 conflict",
          code == 409 and len(f.get("failures", [])) == 1
          and f["failures"][0]["task_key"] == k1
          and f["failures"][0]["reason"] == "conflict", f"{code} {f}")
    code, st2 = call("GET", f"/v1/leases/{k2}")
    check("renew-batch wrong fencing touches no ttl",
          code == 200 and st2["expires_at_ms"] == expires_k2_before, f"{code} {st2}")


def transfer_checks():
    """/v1/leases/transfer：换凭据 / 防栅递增 / 不续期 / 旧凭据失效 / 三类 409 / 审计。"""
    base = f"smoke-transfer:{int(time.time() * 1000)}"
    path = "/v1/leases/transfer"
    key = f"{base}:main"

    def tr(k, token, fence):
        return call("POST", path,
                    {"task_key": k, "token": token, "fencing_token": fence})

    # 参数校验：缺 token / 负 fencing -> 422
    code, _ = call("POST", path, {"task_key": key, "fencing_token": 1})
    check("transfer missing token -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path,
                   {"task_key": key, "token": "t", "fencing_token": -1})
    check("transfer negative fencing -> 422", code == 422, f"code={code}")

    # not_found：租约不存在 -> 409，不创建租约、不递增防栅计数器
    code, f = tr(key, "ghost", 1)
    check("transfer not_found -> 409",
          code == 409 and not f["transferred"] and f["reason"] == "not_found"
          and f["token"] is None and f["fencing_token"] is None, f"{code} {f}")
    code, st = call("GET", f"/v1/leases/{key}")
    check("transfer not_found created nothing",
          code == 200 and not st["held"], f"{code} {st}")

    # 准备一个 10 秒租约用于交接
    code, a = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 10_000})
    check("transfer setup acquire", code == 200 and a["acquired"], f"{code} {a}")
    token1, fence1, expires1 = a["token"], a["fencing_token"], a["expires_at_ms"]

    # conflict：错误 token / 错误 fencing 各一次，均不改租约、不递增防栅序号
    code, f = tr(key, "wrong-token", fence1)
    check("transfer wrong token -> 409 conflict",
          code == 409 and f["reason"] == "conflict", f"{code} {f}")
    code, f = tr(key, token1, fence1 + 1)
    check("transfer wrong fencing -> 409 conflict",
          code == 409 and f["reason"] == "conflict", f"{code} {f}")
    code, st = call("GET", f"/v1/leases/{key}")
    check("transfer conflict touches nothing",
          code == 200 and st["held"] and st["fencing_token"] == fence1
          and st["expires_at_ms"] == expires1, f"{code} {st}")
    # 冲突后原凭据仍可正常续租
    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": key, "token": token1,
                    "fencing_token": fence1, "ttl_ms": 10_000})
    check("transfer conflict holder still renews",
          code == 200 and r["renewed"], f"{code} {r}")
    expires_after_renew = r["expires_at_ms"]

    # 成功交接：新 token 服务端生成、防栅序号 +1、expires_at 原样保留（交接不续期）
    code, t1 = tr(key, token1, fence1)
    check("transfer ok",
          code == 200 and t1["transferred"] and t1["token"]
          and t1["token"] != token1 and t1["fencing_token"] == fence1 + 1
          and t1["expires_at_ms"] == expires_after_renew and t1["reason"] is None,
          f"{code} {t1}")
    token2, fence2 = t1["token"], t1["fencing_token"]

    # 旧凭据立即在续租 / 释放 / 校验接口失效
    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": key, "token": token1,
                    "fencing_token": fence1, "ttl_ms": 10_000})
    check("old credential renew rejected after transfer",
          code == 409 and not r["renewed"] and r["reason"] == "conflict",
          f"{code} {r}")
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": key, "token": token1, "fencing_token": fence1})
    check("old credential release ignored after transfer",
          code == 200 and not rel["released"], f"{code} {rel}")
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": key, "token": token1, "fencing_token": fence1})
    check("old credential invalid after transfer",
          code == 200 and not v["valid"], f"{code} {v}")
    # 用旧凭据再次交接同样 409 conflict，且不得再递增防栅序号
    code, f = tr(key, token1, fence1)
    check("old credential transfer rejected",
          code == 409 and f["reason"] == "conflict"
          and f["fencing_token"] == fence2, f"{code} {f}")

    # 新凭据立即可用：校验通过、剩余时间与保留的 expires_at 一致
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": key, "token": token2, "fencing_token": fence2})
    check("new credential valid after transfer",
          code == 200 and v["valid"] and v["remaining_ms"] > 0, f"{code} {v}")
    code, st = call("GET", f"/v1/leases/{key}")
    check("transfer preserved expiry",
          code == 200 and st["fencing_token"] == fence2
          and st["expires_at_ms"] == expires_after_renew, f"{code} {st}")

    # 连续交接：第二次成功，序号继续递增、过期时刻仍不变
    code, t2 = tr(key, token2, fence2)
    check("second transfer ok",
          code == 200 and t2["transferred"] and t2["token"] != token2
          and t2["fencing_token"] == fence2 + 1
          and t2["expires_at_ms"] == expires_after_renew, f"{code} {t2}")
    token3, fence3 = t2["token"], t2["fencing_token"]
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": key, "token": token3, "fencing_token": fence3})
    check("chained new credential valid", code == 200 and v["valid"], f"{code} {v}")
    # 前一代凭据也已失效
    code, v = call("POST", "/v1/leases/validate",
                   {"task_key": key, "token": token2, "fencing_token": fence2})
    check("previous credential invalid after chained transfer",
          code == 200 and not v["valid"], f"{code} {v}")
    # 新凭据可正常续租
    code, r = call("POST", "/v1/leases/renew",
                   {"task_key": key, "token": token3,
                    "fencing_token": fence3, "ttl_ms": 10_000})
    check("new credential renew ok", code == 200 and r["renewed"], f"{code} {r}")
    # 新凭据可释放
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": key, "token": token3, "fencing_token": fence3})
    check("new credential release ok", code == 200 and rel["released"], f"{code} {rel}")
    # 释放后交接 -> not_found
    code, f = tr(key, token3, fence3)
    check("transfer after release -> 409 not_found",
          code == 409 and f["reason"] == "not_found", f"{code} {f}")

    # expired：短 TTL 租约过期后交接 -> 409（expired/not_found），且不递增防栅序号；
    # 随后正常接管只递增一次
    ekey = f"{base}:expired"
    code, ae = call("POST", "/v1/leases/acquire", {"task_key": ekey, "ttl_ms": 300})
    check("transfer expired setup", code == 200 and ae["acquired"], f"{code} {ae}")
    efence = ae["fencing_token"]
    time.sleep(0.7)
    code, f = tr(ekey, ae["token"], efence)
    check("transfer expired -> 409",
          code == 409 and not f["transferred"]
          and f["reason"] in ("expired", "not_found"), f"{code} {f}")
    code, anew = call("POST", "/v1/leases/acquire",
                      {"task_key": ekey, "ttl_ms": 5000})
    check("transfer expired no fencing increment",
          code == 200 and anew["acquired"]
          and anew["fencing_token"] == efence + 1, f"{code} {anew}")

    # 审计：主任务键的 transfer 事件按时间顺序为
    # 首次 not_found、两次 conflict(fence1)、transferred(fence2)、
    # 旧凭据 conflict(fence2)、transferred(fence3)、释放后 not_found
    code, page = fetch_events(key)
    t_events = [e for e in page["events"] if e["op"] == "transfer"]
    check("transfer audit event results",
          [(e["result"], e["fencing_token"]) for e in t_events] == [
              ("not_found", None),
              ("conflict", fence1),
              ("conflict", fence1),
              ("transferred", fence2),
              ("conflict", fence2),
              ("transferred", fence3),
              ("not_found", None),
          ], f"{t_events}")
    check("transfer audit time ascending",
          all(t_events[i]["time_ms"] <= t_events[i + 1]["time_ms"]
              for i in range(len(t_events) - 1)), f"{[e['time_ms'] for e in t_events]}")

    # 不存在键 / 过期键的失败交接也各落一条审计事件
    gkey = f"{base}:ghost-only"
    code, _ = tr(gkey, "x", 1)
    code, page = fetch_events(gkey)
    g_events = [e for e in page["events"] if e["op"] == "transfer"]
    check("transfer audit not_found on fresh key",
          len(g_events) == 1 and g_events[0]["result"] == "not_found"
          and g_events[0]["fencing_token"] is None, f"{page}")
    code, page = fetch_events(ekey)
    e_events = [e for e in page["events"] if e["op"] == "transfer"]
    check("transfer audit expired event",
          len(e_events) == 1
          and e_events[0]["result"] in ("expired", "not_found")
          and e_events[0]["fencing_token"] in (efence, None), f"{page}")


def result_checks():
    """/v1/results：凭据校验 / 提交 / 重放 / 内容冲突 / 过期与接管 / 持久化 / 并发 / 审计。"""
    base = f"smoke-result:{int(time.time() * 1000)}"
    path = "/v1/results/commit"
    key = f"{base}:main"

    def commit(k, token, fence, result):
        return call("POST", path,
                    {"task_key": k, "token": token,
                     "fencing_token": fence, "result": result})

    def commit_ex(k, token, fence, result):
        return call_ex("POST", path,
                       {"task_key": k, "token": token,
                        "fencing_token": fence, "result": result})

    # 从未提交：GET 返回 committed=false
    code, g = call("GET", f"/v1/results/{key}")
    check("result get never committed",
          code == 200 and g["committed"] is False
          and g["fencing_token"] is None and g["result"] is None
          and g["committed_at_ms"] is None, f"{code} {g}")

    # 无租约提交 -> 409 not_found，不产生结果
    code, c = commit(key, "ghost", 1, {"v": 1})
    check("result commit no lease -> 409 not_found",
          code == 409 and not c["committed"] and c["reason"] == "not_found",
          f"{code} {c}")
    code, g = call("GET", f"/v1/results/{key}")
    check("result not_found wrote nothing",
          code == 200 and not g["committed"], f"{code} {g}")

    # 参数校验：缺 token / 负 fencing / 缺 result -> 422
    code, _ = call("POST", path,
                   {"task_key": key, "fencing_token": 1, "result": {}})
    check("result missing token -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path,
                   {"task_key": key, "token": "t", "fencing_token": -1,
                    "result": {}})
    check("result negative fencing -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path,
                   {"task_key": key, "token": "t", "fencing_token": 1})
    check("result missing result -> 422", code == 422, f"code={code}")

    # 申请租约（10 秒）；错误凭据提交 -> 409 conflict，不产生结果
    code, a = call("POST", "/v1/leases/acquire",
                   {"task_key": key, "ttl_ms": 10_000})
    check("result setup acquire", code == 200 and a["acquired"], f"{code} {a}")
    token1, fence1 = a["token"], a["fencing_token"]

    code, c = commit(key, "wrong-token", fence1, {"v": 1})
    check("result commit wrong token -> 409 conflict",
          code == 409 and c["reason"] == "conflict", f"{code} {c}")
    code, c = commit(key, token1, fence1 + 5, {"v": 1})
    check("result commit wrong fencing -> 409 conflict",
          code == 409 and c["reason"] == "conflict", f"{code} {c}")
    code, g = call("GET", f"/v1/results/{key}")
    check("result conflict wrote nothing",
          code == 200 and not g["committed"], f"{code} {g}")

    # 首次提交 -> 200 committed=true replayed=false
    payload = {"status": "done", "rows": 42, "nested": {"x": [1, 2, True, None]}}
    code, c = commit(key, token1, fence1, payload)
    check("result first commit",
          code == 200 and c["committed"] and c["replayed"] is False
          and c["fencing_token"] == fence1 and c["result"] == payload
          and isinstance(c["committed_at_ms"], int), f"{code} {c}")
    committed_at = c["committed_at_ms"]

    # GET 与首次一致
    code, g = call("GET", f"/v1/results/{key}")
    check("result get after commit",
          code == 200 and g["committed"] and g["fencing_token"] == fence1
          and g["result"] == payload and g["committed_at_ms"] == committed_at,
          f"{code} {g}")

    # 相同内容重试（键顺序打乱、带空白）-> 200 replayed=true，结果 / 时间戳不变
    code, headers, c2 = commit_ex(
        key, token1, fence1,
        {"nested": {"x": [1, 2, True, None]}, "rows": 42, "status": "done"})
    check("result same content replay",
          code == 200 and c2["committed"] and c2["replayed"] is True
          and c2["fencing_token"] == fence1 and c2["result"] == payload
          and c2["committed_at_ms"] == committed_at, f"{code} {c2}")
    check("result replay header",
          headers.get("Result-Replay") == "true",
          f"Result-Replay={headers.get('Result-Replay')}")

    # 标量 / 数组 / 字符串等任意 JSON 也支持（用另一把租约简单验证一次）
    skey = f"{base}:scalar"
    code, sa = call("POST", "/v1/leases/acquire",
                    {"task_key": skey, "ttl_ms": 10_000})
    code, sc = commit(skey, sa["token"], sa["fencing_token"], [1, "two", False])
    check("result array payload ok",
          code == 200 and sc["committed"] and sc["result"] == [1, "two", False],
          f"{code} {sc}")
    code, sg = call("GET", f"/v1/results/{skey}")
    check("result array get", code == 200 and sg["result"] == [1, "two", False],
          f"{code} {sg}")

    # 同一防栅序号不同内容 -> 409 result_conflict，已存结果不变
    code, c3 = commit(key, token1, fence1, {"status": "done", "rows": 43})
    check("result diff content -> 409 result_conflict",
          code == 409 and not c3["committed"]
          and c3["reason"] == "result_conflict", f"{code} {c3}")
    code, g = call("GET", f"/v1/results/{key}")
    check("result conflict keeps stored",
          code == 200 and g["result"] == payload
          and g["committed_at_ms"] == committed_at, f"{code} {g}")
    # 数字与字符串类型差异也算不同内容
    code, c3b = commit(key, token1, fence1,
                       {"status": "done", "rows": "42",
                        "nested": {"x": [1, 2, True, None]}})
    check("result type-diff -> 409 result_conflict",
          code == 409 and c3b["reason"] == "result_conflict", f"{code} {c3b}")

    # 并发：同序号同内容 20 个并发提交，恰好 1 个首次、19 个重放，时间戳一致
    cckey = f"{base}:concurrent"
    code, ca = call("POST", "/v1/leases/acquire",
                    {"task_key": cckey, "ttl_ms": 10_000})
    check("result concurrent setup", code == 200 and ca["acquired"], f"{code} {ca}")
    cbody = {"task_key": cckey, "token": ca["token"],
             "fencing_token": ca["fencing_token"], "result": {"v": 7}}

    def one_commit(_):
        return call_ex("POST", path, cbody)

    with ThreadPoolExecutor(max_workers=20) as pool:
        cresults = list(pool.map(one_commit, range(20)))
    firsts = [d for _, h, d in cresults if d.get("replayed") is False]
    replays = [d for _, h, d in cresults if d.get("replayed") is True]
    check("result concurrent single commit",
          all(code_ == 200 for code_, _, _ in cresults)
          and len(firsts) == 1 and len(replays) == 19
          and all(d["committed_at_ms"] == firsts[0]["committed_at_ms"]
                  for d in replays),
          f"firsts={len(firsts)} replays={len(replays)}")
    check("result concurrent replay headers",
          sum(1 for _, h, _ in cresults if h.get("Result-Replay") == "true") == 19,
          f"{[h.get('Result-Replay') for _, h, _ in cresults]}")

    # 旧租约过期后被接管：旧凭据提交（无论新旧内容）-> 409，无法覆盖；
    # 新租约（更大 fencing）可提交新结果并覆盖；旧结果在此之前仍可读
    ekey = f"{base}:expiry"
    code, ea = call("POST", "/v1/leases/acquire",
                    {"task_key": ekey, "ttl_ms": 300})
    check("result expiry setup", code == 200 and ea["acquired"], f"{code} {ea}")
    etoken, efence = ea["token"], ea["fencing_token"]
    code, ec = commit(ekey, etoken, efence, {"gen": 1})
    check("result expiry first commit", code == 200 and ec["committed"],
          f"{code} {ec}")
    first_at = ec["committed_at_ms"]
    time.sleep(0.7)
    # 旧凭据重试：租约已过期（Redis PEXPIRE 通常已删除 Hash）-> expired/not_found
    code, old = commit(ekey, etoken, efence, {"gen": 1})
    check("result stale holder rejected",
          code == 409 and old["reason"] in ("expired", "not_found"),
          f"{code} {old}")
    # 旧结果在被覆盖前仍可读（结果不随租约到期删除）
    code, g = call("GET", f"/v1/results/{ekey}")
    check("result survives lease expiry",
          code == 200 and g["committed"] and g["fencing_token"] == efence
          and g["result"] == {"gen": 1} and g["committed_at_ms"] == first_at,
          f"{code} {g}")
    # 新持有者接管并提交新结果 -> 覆盖
    code, na = call("POST", "/v1/leases/acquire",
                    {"task_key": ekey, "ttl_ms": 10_000})
    check("result takeover acquire",
          code == 200 and na["acquired"]
          and na["fencing_token"] == efence + 1, f"{code} {na}")
    code, nc = commit(ekey, na["token"], na["fencing_token"], {"gen": 2})
    check("result new lease commits new result",
          code == 200 and nc["committed"] and nc["replayed"] is False
          and nc["result"] == {"gen": 2}
          and nc["committed_at_ms"] >= first_at, f"{code} {nc}")
    code, g = call("GET", f"/v1/results/{ekey}")
    check("result get latest after takeover",
          code == 200 and g["fencing_token"] == na["fencing_token"]
          and g["result"] == {"gen": 2}, f"{code} {g}")
    # 旧凭据再用同序号提交旧内容也不能重放覆盖（凭据已不属当前租约）
    code, old2 = commit(ekey, etoken, efence, {"gen": 1})
    check("result old credential after takeover conflict",
          code == 409 and old2["reason"] in ("conflict", "not_found"),
          f"{code} {old2}")

    # 释放不删结果
    code, rel = call("POST", "/v1/leases/release",
                     {"task_key": key, "token": token1, "fencing_token": fence1})
    check("result setup release", code == 200 and rel["released"], f"{code} {rel}")
    code, g = call("GET", f"/v1/results/{key}")
    check("result survives release",
          code == 200 and g["committed"] and g["result"] == payload,
          f"{code} {g}")

    # 审计：主任务键的 result_commit 事件序列（并发键单独只查数量）
    code, page = fetch_events(key)
    rc = [(e["result"], e["fencing_token"])
          for e in page["events"] if e["op"] == "result_commit"]
    check("result audit sequence",
          rc == [
              ("not_found", None),       # 无租约提交
              ("conflict", fence1),      # 错误 token
              ("conflict", fence1),      # 错误 fencing
              ("committed", fence1),     # 首次
              ("replayed", fence1),      # 相同内容重放
              ("result_conflict", fence1),  # 不同内容
              ("result_conflict", fence1),  # 类型差异
          ], f"{rc}")
    code, page = fetch_events(ekey)
    erc = [(e["result"], e["fencing_token"])
           for e in page["events"] if e["op"] == "result_commit"]
    check("result audit expiry sequence",
          erc[0] == ("committed", efence)
          and erc[1][0] in ("expired", "not_found")
          and erc[2] == ("committed", efence + 1)
          and erc[3][0] in ("conflict", "not_found"), f"{erc}")
    code, page = fetch_events(cckey)
    crc = [e for e in page["events"] if e["op"] == "result_commit"]
    check("result audit concurrent one committed rest replayed",
          len(crc) == 20
          and sum(1 for e in crc if e["result"] == "committed") == 1
          and sum(1 for e in crc if e["result"] == "replayed") == 19
          and all(e["fencing_token"] == ca["fencing_token"] for e in crc),
          f"{[(e['result'], e['fencing_token']) for e in crc]}")


def fetch_changes(cursor=None, limit=None):
    """翻一页全局结果变更，返回 (http_code, body)。"""
    qs = []
    if cursor is not None:
        qs.append(f"cursor={cursor}")
    if limit is not None:
        qs.append(f"limit={limit}")
    suffix = ("?" + "&".join(qs)) if qs else ""
    return call("GET", f"/v1/results/changes{suffix}")


def result_changes_checks():
    """/v1/results/changes：仅首次写入新结果版本追加 / 失败与重放不追加 /
    跨任务键全局顺序 / 游标分页 / 410 过期游标 / 参数校验 / 路由优先于
    {task_key} 捕获路由（GET changes 不被当成任务键 "changes"）。"""
    base = f"smoke-changes:{int(time.time() * 1000)}"
    k1, k2, k3 = f"{base}:k1", f"{base}:k2", f"{base}:k3"

    def commit(k, token, fence, result):
        return call("POST", "/v1/results/commit",
                    {"task_key": k, "token": token,
                     "fencing_token": fence, "result": result})

    def commit_checked(k, token, fence, result, snapshot):
        return call("POST", "/v1/results/commit-checked",
                    {"task_key": k, "token": token,
                     "fencing_token": fence, "result": result,
                     "snapshot": snapshot})

    # 记录测试开始前全局流的最早条目（已存在其他冒烟提交），作为后续基线游标
    code, first_page = fetch_changes(limit=200)
    check("changes route resolves (not captured by {task_key})",
          code == 200 and "changes" in first_page and "next_cursor" in first_page,
          f"{code} {first_page}")
    baseline = first_page["changes"][0]["id"] if first_page["changes"] else None

    # 取租约
    creds = {}
    for k in (k1, k2, k3):
        code, a = call("POST", "/v1/leases/acquire",
                       {"task_key": k, "ttl_ms": 60_000})
        check("changes setup acquire", code == 200 and a["acquired"], f"{code} {a}")
        creds[k] = (a["token"], a["fencing_token"])

    # k1 首次提交 -> 一条变更
    code, c1 = commit(k1, *creds[k1], {"v": 1})
    check("changes k1 first commit", code == 200 and c1["committed"]
          and not c1["replayed"], f"{code} {c1}")
    # 相同内容重放（键序打乱）-> 不追加
    code, c1r = commit(k1, *creds[k1], {"v": 1})
    check("changes k1 replay ok", code == 200 and c1r["replayed"], f"{code} {c1r}")
    # 同序号不同内容 -> 409，不追加
    code, c1c = commit(k1, *creds[k1], {"v": 2})
    check("changes k1 conflict -> 409",
          code == 409 and c1c["reason"] == "result_conflict", f"{code} {c1c}")
    # 凭据错误 -> 409，不追加
    code, c1b = commit(k1, "bad-token", creds[k1][1], {"v": 3})
    check("changes k1 bad creds -> 409",
          code == 409 and c1b["reason"] == "conflict", f"{code} {c1b}")

    # k2 首次提交 -> 一条变更（排在 k1 变更之后，验证跨任务键全局定序）
    code, c2 = commit(k2, *creds[k2], {"hello": ["world", 1, True, None]})
    check("changes k2 first commit", code == 200 and c2["committed"], f"{code} {c2}")

    # k3 经 commit-checked（空快照）首次提交 -> 一条变更；重放 / stale 不追加
    code, c3 = commit_checked(k3, *creds[k3], {"ok": True}, [])
    check("changes k3 checked commit", code == 200 and c3["committed"]
          and not c3["replayed"], f"{code} {c3}")
    code, c3r = commit_checked(k3, *creds[k3], {"ok": True}, [])
    check("changes k3 checked replay", code == 200 and c3r["replayed"], f"{code} {c3r}")
    # stale_dependencies：给 k3 加一个未完成前置后用空快照提交 -> 409，不追加
    code, _ = call("POST", f"/v1/dependencies/{k3}", {"dependencies": [k1]})
    code, c3s = commit_checked(k3, *creds[k3], {"ok": True}, [])
    check("changes k3 stale -> 409",
          code == 409 and c3s["reason"] == "stale_dependencies", f"{code} {c3s}")
    # 前置 k1 早已完成：重新拉快照（k1 版本 1）。同 fencing 提交不同内容会
    # result_conflict，故先重取租约拿到 fencing 2，再提交新内容 -> 追加一条
    code, snap = call("GET", f"/v1/dependencies/{k3}/snapshot")
    check("changes k3 snapshot ready",
          code == 200 and snap["status"] == "snapshot", f"{code} {snap}")
    code, _ = call("POST", "/v1/leases/release",
                   {"task_key": k3, "token": creds[k3][0],
                    "fencing_token": creds[k3][1]})
    code, a3b = call("POST", "/v1/leases/acquire",
                     {"task_key": k3, "ttl_ms": 60_000})
    check("changes k3 re-acquire", code == 200 and a3b["acquired"], f"{code} {a3b}")
    creds[k3] = (a3b["token"], a3b["fencing_token"])
    code, c3v2 = commit_checked(k3, *creds[k3], {"ok": "v2"}, snap["dependencies"])
    check("changes k3 checked new version",
          code == 200 and c3v2["committed"] and not c3v2["replayed"], f"{code} {c3v2}")
    call("POST", f"/v1/dependencies/{k3}", {"dependencies": []})

    # 拉取基线之后的全部变更并校验
    tail = []
    cursor = baseline
    while True:
        code, page = fetch_changes(cursor, 1)
        check("changes page ok", code == 200, f"{code} {page}")
        tail.extend(page["changes"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
        if len(tail) > 5000:
            check("changes pagination terminates", False, "too many pages")
            break

    # k3 先以 fencing 1 空快照提交、再经重取租约以 fencing 2 + 快照提交新版本
    k3_f1 = c3["fencing_token"]
    k3_f2 = c3v2["fencing_token"]
    assert k3_f2 > k3_f1, (k3_f1, k3_f2)
    expected = [
        (k1, creds[k1][1], {"v": 1}, c1["committed_at_ms"]),
        (k2, creds[k2][1], {"hello": ["world", 1, True, None]}, c2["committed_at_ms"]),
        (k3, k3_f1, {"ok": True}, c3["committed_at_ms"]),
        (k3, k3_f2, {"ok": "v2"}, c3v2["committed_at_ms"]),
    ]
    mine = [ch for ch in tail if ch["task_key"] in (k1, k2, k3)]
    check("changes only first-write appended (4 changes)",
          len(mine) == 4, f"{[(c['task_key'], c['result']) for c in mine]}")
    check("changes content & order",
          [(c["task_key"], c["fencing_token"], c["result"], c["committed_at_ms"])
           for c in mine] == expected, f"{mine}")
    # 字段形态与 ID 全局升序
    check("changes fields",
          all(isinstance(c["id"], str)
              and tuple(p.isdigit() for p in c["id"].split("-")) == (True, True)
              and isinstance(c["fencing_token"], int)
              and isinstance(c["committed_at_ms"], int)
              for c in mine), f"{mine}")
    ids = [tuple(int(p) for p in c["id"].split("-")) for c in tail]
    check("changes globally ascending",
          all(ids[i] < ids[i + 1] for i in range(len(ids) - 1)), "ids not sorted")
    # 变更的提交时间 / 结果与 GET /v1/results/{task_key} 同源
    code, g1 = call("GET", f"/v1/results/{k1}")
    check("changes matches GET results k1",
          g1["fencing_token"] == mine[0]["fencing_token"]
          and g1["result"] == mine[0]["result"]
          and g1["committed_at_ms"] == mine[0]["committed_at_ms"], f"{g1} {mine[0]}")

    # 分页：limit=2 恰好满页时给 next_cursor；游标为"上次最后一条"，不重不漏
    code, p1 = fetch_changes(baseline, 2)
    check("changes page size & cursor",
          code == 200 and len(p1["changes"]) == 2
          and p1["next_cursor"] == p1["changes"][-1]["id"], f"{p1}")
    code, p2 = fetch_changes(p1["next_cursor"], 2)
    check("changes page 2 no overlap",
          code == 200 and p2["changes"]
          and p2["changes"][0]["id"] != p1["changes"][-1]["id"]
          and _id_tuple(p2["changes"][0]["id"]) > _id_tuple(p1["changes"][-1]["id"]),
          f"{p1} {p2}")

    # 410：游标早于当前保留的最早条目（流非空，最早条目即基线）
    oldest = first_page["changes"][0]["id"] if first_page["changes"] else tail[0]["id"]
    oms, oseq = oldest.split("-")
    stale_cursor = f"{int(oms) - 1}-0"
    code, body = fetch_changes(stale_cursor, 10)
    check("changes stale cursor -> 410",
          code == 410 and isinstance(body.get("detail"), str), f"{code} {body}")

    # 合法但指向未来的游标 -> 200 空页、next_cursor=null（不算过期）
    lms, lseq = tail[-1]["id"].split("-")
    future_cursor = f"{lms}-{int(lseq) + 1000}"
    code, page = fetch_changes(future_cursor, 10)
    check("changes future cursor -> empty 200",
          code == 200 and page["changes"] == []
          and page["next_cursor"] is None, f"{code} {page}")

    # 不传游标：从当前保留的最早条目开始
    code, page = fetch_changes(limit=1)
    check("changes no cursor starts at oldest retained",
          code == 200 and len(page["changes"]) == 1
          and page["changes"][0]["id"] == oldest, f"{code} {page}")

    # 参数校验：非法 cursor 400；limit 越界 422
    code, _ = fetch_changes("not-a-cursor")
    check("changes bad cursor -> 400", code == 400, f"code={code}")
    code, _ = fetch_changes(limit=0)
    check("changes limit 0 -> 422", code == 422, f"code={code}")
    code, _ = fetch_changes(limit=201)
    check("changes limit >200 -> 422", code == 422, f"code={code}")

    result_changes_race_checks()


def result_changes_race_checks():
    """/v1/results/changes 游标裁剪竞态回归（单 Lua 原子读取）：

    - 恰满一页：满页且后面还有条目 -> next_cursor 指向本页末条；
      满页恰好是最后一页 -> next_cursor 为 null；按写入顺序衔接且不重复。
    - 并发裁剪：小保留窗口下并发提交持续 XADD MAXLEN 裁剪旧条目，
      任何 200 页面都必须与读取前一刻的流快照逐条衔接（不得静默漏读），
      游标早于该快照最早条目时返回 410。
    - 空流语义：流为空时（含带游标）返回 200 空页，沿用既有语义。
    """
    base = f"smoke-changes-race:{int(time.time() * 1000)}"
    prefix = os.environ.get("SMOKE_KEY_PREFIX", "lease")
    host = os.environ.get("SMOKE_REDIS_HOST", "localhost")
    port = int(os.environ.get("SMOKE_REDIS_PORT", "6379"))
    stream = f"{prefix}:result-changes"

    def commit(k, result):
        code, a = call("POST", "/v1/leases/acquire",
                       {"task_key": k, "ttl_ms": 60_000})
        if code != 200 or not a.get("acquired"):
            return code, a
        return call("POST", "/v1/results/commit",
                    {"task_key": k, "token": a["token"],
                     "fencing_token": a["fencing_token"], "result": result})

    # ---- 恰满一页（确定性，本进程内此刻无并发提交）----
    keys = [f"{base}:full:{i}" for i in range(4)]
    for i, k in enumerate(keys):
        code, c = commit(k, {"seq": i})
        check("race full-page setup commit",
              code == 200 and c["committed"], f"{code} {c}")
    code, snap = fetch_changes(limit=200)
    all_entries = snap["changes"]
    mine = [ch for ch in all_entries if ch["task_key"] in keys]
    check("race full-page setup visible", len(mine) == 4, f"{mine}")
    full_ids = [ch["id"] for ch in mine]
    # cursor 为"上次最后一条"（排他）：取本块首条的前一条作为锚点，
    # 本块恰为流首时不带游标
    start_idx = next(i for i, ch in enumerate(all_entries)
                     if ch["id"] == full_ids[0])
    anchor = all_entries[start_idx - 1]["id"] if start_idx > 0 else None

    # limit=4 恰好满页且后面没有更多条目 -> next_cursor 为 null
    code, p = fetch_changes(anchor, 4)
    check("race exact full page is last page",
          code == 200 and [ch["id"] for ch in p["changes"]] == full_ids
          and p["next_cursor"] is None, f"{p}")

    # limit=3 满页且后面还有 1 条 -> next_cursor 指向本页末条，下一页恰好衔接
    code, p = fetch_changes(anchor, 3)
    check("race full page with more gives cursor",
          code == 200 and [ch["id"] for ch in p["changes"]] == full_ids[:3]
          and p["next_cursor"] == full_ids[2], f"{p}")
    code, p2 = fetch_changes(p["next_cursor"], 3)
    check("race page after full page continues in order",
          code == 200 and [ch["id"] for ch in p2["changes"]] == full_ids[3:]
          and p2["next_cursor"] is None, f"{p2}")

    # limit=2 两页衔接：不重不漏、ID 严格递增
    code, q1 = fetch_changes(anchor, 2)
    check("race first of two full pages",
          code == 200 and [ch["id"] for ch in q1["changes"]] == full_ids[:2]
          and q1["next_cursor"] == full_ids[1], f"{q1}")
    code, q2 = fetch_changes(q1["next_cursor"], 2)
    joined = [ch["id"] for ch in q1["changes"]] + [
        ch["id"] for ch in q2["changes"]]
    check("race full pages join without dup/gap",
          code == 200 and joined == full_ids and q2["next_cursor"] is None,
          f"{q1} {q2}")

    # ---- 并发裁剪：小保留窗口（MAXLEN=10）下持续提交裁剪 ----
    try:
        r = _RespClient(host, port)
    except OSError as exc:
        check("race redis reachable", False, f"{host}:{port} {exc}")
        return
    try:
        keep = 10
        page_limit = 5
        stop = threading.Event()
        pause = threading.Event()

        def trimmer(idx):
            client = _RespClient(host, port)
            n = 0
            try:
                while not stop.is_set():
                    if pause.is_set():
                        time.sleep(0.005)
                        continue
                    n += 1
                    client.execute(
                        "XADD", stream, "MAXLEN", keep, "*",
                        "task_key", f"{base}:trimmer:{idx}:{n}",
                        "fencing", str(n),
                        "committed_at_ms", "0",
                        "result", "null")
                    # 限速到约 2k 条 / 秒：让裁剪既有概率落在 HTTP 请求之前
                    # （410 分支），也有概率落在旧实现"最早条目校验 -> 页面
                    # 读取"的往返之间（旧实现 200 静默漏读分支）
                    time.sleep(0.008)
            except (OSError, RuntimeError):
                pass
            finally:
                client.close()

        threads = [threading.Thread(target=trimmer, args=(i,), daemon=True)
                   for i in range(16)]
        for t in threads:
            t.start()
        try:
            # 等流进入"满窗"稳态：达到 keep 条后每次 XADD 都裁掉最旧条目
            deadline = time.time() + 10
            while time.time() < deadline:
                if r.execute("XLEN", stream) >= keep:
                    break
                time.sleep(0.01)
            xlen = r.execute("XLEN", stream)
            check("race stream reached steady window", xlen >= keep,
                  f"XLEN={xlen}")

            # 强制 410：暂停裁剪 -> 快照 -> 再写一条裁掉快照游标 -> 读取
            # 旧实现（先查最早、再分两次读取）可能在裁剪窗口内得到 200 静默漏读
            pause.set()
            time.sleep(0.05)
            snap0 = r.execute("XRANGE", stream, "-", "+")
            stale_cursor = snap0[0][0]
            r.execute("XADD", stream, "MAXLEN", keep, "*",
                      "task_key", f"{base}:force-trim", "fencing", "1",
                      "committed_at_ms", "0", "result", "null")
            code, body = fetch_changes(stale_cursor, page_limit)
            check("race cursor trimmed between -> 410",
                  code == 410 and isinstance(body.get("detail"), str),
                  f"{code} {body}")

            # 强制 200：暂停裁剪后从快照最早条目起读，第一页满页且后面还有
            # 条目 -> next_cursor 非空；第二页承接剩余条目且为最后一页
            snap1 = r.execute("XRANGE", stream, "-", "+")
            code, p = fetch_changes(snap1[0][0], page_limit)
            check("race fresh cursor first page joins snapshot",
                  code == 200 and [ch["id"] for ch in p["changes"]]
                  == [e[0] for e in snap1[1:1 + page_limit]]
                  and p["next_cursor"] == snap1[page_limit][0], f"{code} {p}")
            code, p = fetch_changes(p["next_cursor"], page_limit)
            check("race fresh cursor second page joins in order",
                  code == 200 and [ch["id"] for ch in p["changes"]]
                  == [e[0] for e in snap1[page_limit + 1:]]
                  and p["next_cursor"] is None, f"{code} {p}")

            # 压力轮次：每轮先直连快照再发起读取。410 合法（快照后游标已被裁）；
            # 200 时页面必须与快照逐条一致——旧实现的静默漏读必在此暴露
            pause.clear()
            saw_410 = False
            saw_200 = False
            consistent = True
            rounds = 400
            for _ in range(rounds):
                snap_i = r.execute("XRANGE", stream, "-", "+")
                code, p = fetch_changes(snap_i[0][0], page_limit)
                expected = [e[0] for e in snap_i[1:1 + page_limit]]
                if code == 410:
                    saw_410 = True
                elif not (code == 200 and len(p["changes"]) == page_limit
                          and [ch["id"] for ch in p["changes"]] == expected
                          and p["next_cursor"] == snap_i[page_limit][0]):
                    consistent = False
                    break
                else:
                    saw_200 = True
            check("race hammer page consistent with snapshot",
                  consistent,
                  "a 200 page missed entries trimmed between checks"
                  if not consistent else "")
            check("race hammer exercised 410 under concurrent trims",
                  saw_410)
            check("race hammer exercised 200 under concurrent trims",
                  saw_200)
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=5)
    finally:
        r.close()

    # ---- 空流语义：清空后无游标 / 带游标均为 200 空页（不判 410）----
    try:
        r2 = _RespClient(host, port)
    except OSError as exc:
        check("race redis reachable (empty)", False, f"{host}:{port} {exc}")
        return
    try:
        r2.execute("DEL", stream)
        code, p = fetch_changes(limit=5)
        check("race empty stream no cursor -> empty page",
              code == 200 and p["changes"] == [] and p["next_cursor"] is None,
              f"{code} {p}")
        code, p = fetch_changes("1-0", 5)
        check("race empty stream with cursor -> empty page",
              code == 200 and p["changes"] == [] and p["next_cursor"] is None,
              f"{code} {p}")
    finally:
        r2.close()


def _id_tuple(entry_id):
    ms, seq = entry_id.split("-", 1)
    return int(ms), int(seq)


def fetch_events(key, cursor=None, limit=None):
    """翻一页审计事件，返回 (events, next_cursor)。"""
    qs = []
    if cursor is not None:
        qs.append(f"cursor={cursor}")
    if limit is not None:
        qs.append(f"limit={limit}")
    suffix = ("?" + "&".join(qs)) if qs else ""
    code, body = call("GET", f"/v1/leases/{key}/events{suffix}")
    return code, body


def fetch_all_events(key, page_size=10):
    """从最旧事件开始翻完全部页，校验分页不重不漏，返回事件列表。"""
    events, cursor, seen = [], None, set()
    pages = 0
    while True:
        code, body = fetch_events(key, cursor, page_size)
        check(f"events page {pages} ({key})", code == 200, f"{code} {body}")
        page = body.get("events", [])
        for e in page:
            check("event id unique across pages", e["id"] not in seen, e["id"])
            seen.add(e["id"])
        events.extend(page)
        pages += 1
        cursor = body.get("next_cursor")
        if cursor is None:
            break
        check("events page not over limit", len(page) <= page_size, f"{len(page)}")
        if pages > 5000:
            check("pagination terminates", False, "too many pages")
            break
    return events


def audit_checks():
    """审计流：落账字段 / 四类操作 / 分页 / 参数校验 / 批量落账 / 保留裁剪。"""
    base = f"smoke-audit:{int(time.time() * 1000)}"
    key = f"{base}:single"

    # 从未发生过操作的任务键：空流，next_cursor 为 null
    code, page = fetch_events(f"{base}:never")
    check("events empty stream", code == 200 and page["events"] == []
          and page["next_cursor"] is None, f"{code} {page}")

    # 1) acquire 成功
    code, a = call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 5000})
    token, fence = a["token"], a["fencing_token"]
    # 2) acquire 冲突
    call("POST", "/v1/leases/acquire", {"task_key": key, "ttl_ms": 5000})
    # 3) renew 令牌错误 -> conflict
    call("POST", "/v1/leases/renew",
         {"task_key": key, "token": "wrong", "fencing_token": fence, "ttl_ms": 5000})
    # 4) renew 成功
    call("POST", "/v1/leases/renew",
         {"task_key": key, "token": token, "fencing_token": fence, "ttl_ms": 5000})
    # 5) release 防栅序号错误 -> ignored
    call("POST", "/v1/leases/release",
         {"task_key": key, "token": token, "fencing_token": fence + 1})
    # 6) release 成功
    call("POST", "/v1/leases/release",
         {"task_key": key, "token": token, "fencing_token": fence})
    # 7) release 租约已不存在 -> not_found 语义同样落 ignored
    call("POST", "/v1/leases/release",
         {"task_key": key, "token": token, "fencing_token": fence})

    events = fetch_all_events(key, page_size=3)
    results = [(e["op"], e["result"]) for e in events]
    check("audit event count & order",
          results == [
              ("acquire", "acquired"),
              ("acquire", "conflict"),
              ("renew", "conflict"),
              ("renew", "renewed"),
              ("release", "ignored"),
              ("release", "released"),
              ("release", "ignored"),
          ], f"{results}")

    # 字段：fencing 关联 / time_ms 为 Redis 毫秒 / 单键事件无 batch_size
    now_ms = int(time.time() * 1000)
    # 最后一次 release 发生在租约删除之后，无关联序号 -> null；其余均关联 fence
    for i, e in enumerate(events):
        expected_fence = None if i == len(events) - 1 else fence
        check("audit event fields",
              e["fencing_token"] == expected_fence and e["batch_size"] is None
              and abs(e["time_ms"] - now_ms) < 60_000
              and isinstance(e["id"], str)
              and tuple(p.isdigit() for p in e["id"].split("-")) == (True, True),
              f"{e}")
    check("audit time_ms ascending",
          all(events[i]["time_ms"] <= events[i + 1]["time_ms"]
              for i in range(len(events) - 1)), f"{[e['time_ms'] for e in events]}")

    # 小页分页：limit=2，首页 next_cursor 指向第二条，且能继续翻页
    code, p1 = fetch_events(key, limit=2)
    check("audit page size limit", code == 200 and len(p1["events"]) == 2
          and p1["next_cursor"] == p1["events"][-1]["id"], f"{p1}")
    code, p2 = fetch_events(key, p1["next_cursor"], 2)
    check("audit page 2 no overlap",
          code == 200 and p2["events"]
          and p2["events"][0]["id"] == events[2]["id"], f"{p2}")
    check("audit last page no cursor",
          p2["next_cursor"] is not None or len(p2["events"]) < 2, f"{p2}")

    # 参数校验
    code, _ = fetch_events(key, cursor="not-a-cursor")
    check("audit bad cursor -> 400", code == 400, f"code={code}")
    code, _ = fetch_events(key, limit=0)
    check("audit limit 0 -> 422", code == 422, f"code={code}")
    code, _ = fetch_events(key, limit=201)
    check("audit limit > 200 -> 422", code == 422, f"code={code}")

    # 批量申请：成功时逐任务落 acquired（带 batch_size=2）；
    # 冲突批次仅被占用任务落 conflict，未涉及任务不落账
    bk1, bk2, bk3 = f"{base}:b1", f"{base}:b2", f"{base}:b3"
    call("POST", "/v1/leases/acquire-batch",
         {"task_keys": [bk1, bk2], "ttl_ms": 5000})
    code, _ = call("POST", "/v1/leases/acquire-batch",
                   {"task_keys": [bk1, bk3], "ttl_ms": 5000})
    check("audit batch conflict call -> 409", code == 409, f"code={code}")

    ev1 = fetch_all_events(bk1, 10)
    check("audit batch success+conflict on busy key",
          [(e["op"], e["result"]) for e in ev1] == [
              ("acquire_batch", "acquired"), ("acquire_batch", "conflict")]
          and all(e["batch_size"] == 2 for e in ev1)
          and all(e["fencing_token"] == 1 for e in ev1), f"{ev1}")
    code, p3 = fetch_events(bk3)
    check("audit batch non-conflict key untouched",
          code == 200 and p3["events"] == [], f"{p3}")

    # 保留条数裁剪（默认 LEASE_EVENT_RETENTION=1000）：
    # 对被占用任务连续 1050 次冲突申请，每次落 1 条，流应精确保留最新 1000 条，
    # 最旧事件被丢弃
    tkey = f"{base}:trim"
    call("POST", "/v1/leases/acquire", {"task_key": tkey, "ttl_ms": 60_000})

    def one_conflict(_):
        return call("POST", "/v1/leases/acquire",
                    {"task_key": tkey, "ttl_ms": 60_000})

    with ThreadPoolExecutor(max_workers=16) as pool:
        codes = list(pool.map(one_conflict, range(1050)))
    check("audit trim setup all conflict",
          all(c == 409 for c, _ in codes), f"{set(c for c, _ in codes)}")

    events = fetch_all_events(tkey, page_size=200)
    check("audit retention trims oldest (MAXLEN=1000)",
          len(events) == 1000, f"len={len(events)}")
    check("audit trimmed results all conflict",
          all(e["op"] == "acquire" and e["result"] == "conflict" for e in events),
          f"{events[:1]}")
    ids = [tuple(int(p) for p in e["id"].split("-")) for e in events]
    check("audit trimmed stream still ascending",
          all(ids[i] < ids[i + 1] for i in range(len(ids) - 1)), "ids not sorted")


def limit_checks():
    """/v1/limits/check：剩余额度 / 成本累计 / 429 / 窗口滑动 / 并发不超额 / 自动清理。"""
    key = f"smoke-limit:{int(time.time() * 1000)}"
    path = "/v1/limits/check"

    # 参数校验：limit=0 / cost=0 / cost>limit / 缺 window_ms / window 超界
    code, b = call("POST", path, {"client_key": key, "limit": 0,
                                  "window_ms": 1000, "cost": 1})
    check("limit zero -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 10,
                                  "window_ms": 1000, "cost": 0})
    check("cost zero -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 5,
                                  "window_ms": 1000, "cost": 6})
    check("cost greater than limit -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 10, "cost": 1})
    check("missing window_ms -> 422", code == 422, f"code={code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 10,
                                  "window_ms": 90_000_000, "cost": 1})
    check("window_ms out of range -> 400", code == 400, f"code={code} {b}")

    # 逐笔扣减，remaining 递减到 0；空窗口的 reset_at_ms ≈ now + window
    now_ms = int(time.time() * 1000)
    code, b = call("POST", path, {"client_key": key, "limit": 3,
                                  "window_ms": 10_000, "cost": 1})
    check("limit allow #1", code == 200 and b["allowed"]
          and b["remaining"] == 2, f"{code} {b}")
    check("limit fresh reset_at near now+window",
          abs(b["reset_at_ms"] - (now_ms + 10_000)) < 2000, f"{b}")
    code, b = call("POST", path, {"client_key": key, "limit": 3,
                                  "window_ms": 10_000, "cost": 1})
    check("limit allow #2 remaining 1",
          code == 200 and b["allowed"] and b["remaining"] == 1, f"{code} {b}")
    code, b = call("POST", path, {"client_key": key, "limit": 3,
                                  "window_ms": 10_000, "cost": 1})
    check("limit allow #3 remaining 0",
          code == 200 and b["allowed"] and b["remaining"] == 0, f"{code} {b}")

    # 超限：429、不扣减、retry_after_ms 在 (0, window] 内，带 Retry-After 头
    code, headers, b = call_ex("POST", path, {"client_key": key, "limit": 3,
                                              "window_ms": 10_000, "cost": 1})
    check("limit exceed -> 429",
          code == 429 and not b["allowed"] and b["remaining"] == 0,
          f"{code} {b}")
    check("limit exceed retry_after_ms",
          0 < b["retry_after_ms"] <= 10_000, f"{b}")
    retry_after_hdr = headers.get("Retry-After")
    check("limit exceed Retry-After header",
          retry_after_hdr is not None and int(retry_after_hdr) >= 1,
          f"Retry-After={retry_after_hdr}")
    # 超限请求不扣减：再来一次结果完全一致
    code, b2 = call("POST", path, {"client_key": key, "limit": 3,
                                   "window_ms": 10_000, "cost": 1})
    check("limit denied not charged",
          code == 429 and b2["remaining"] == b["remaining"]
          and b2["reset_at_ms"] == b["reset_at_ms"], f"{b} vs {b2}")

    # 成本加权：独立键，limit=10：cost 6 放行剩 4，cost 5 拒绝，cost 4 放行剩 0
    wkey = f"{key}:weighted"
    code, b = call("POST", path, {"client_key": wkey, "limit": 10,
                                  "window_ms": 10_000, "cost": 6})
    check("weighted allow cost 6",
          code == 200 and b["allowed"] and b["remaining"] == 4, f"{code} {b}")
    code, b = call("POST", path, {"client_key": wkey, "limit": 10,
                                  "window_ms": 10_000, "cost": 5})
    check("weighted deny cost 5", code == 429 and not b["allowed"],
          f"{code} {b}")
    code, b = call("POST", path, {"client_key": wkey, "limit": 10,
                                  "window_ms": 10_000, "cost": 4})
    check("weighted allow cost 4 remaining 0",
          code == 200 and b["allowed"] and b["remaining"] == 0, f"{code} {b}")

    # 滑动恢复 + 旧数据自动清理：窗口 800ms 打满后等待滑出，应恢复全部额度
    skey = f"{key}:sliding"
    for _ in range(3):
        code, b = call("POST", path, {"client_key": skey, "limit": 3,
                                      "window_ms": 800, "cost": 1})
        check("sliding fill", code == 200 and b["allowed"], f"{code} {b}")
    code, b = call("POST", path, {"client_key": skey, "limit": 3,
                                  "window_ms": 800, "cost": 1})
    check("sliding full -> 429", code == 429, f"{code} {b}")
    time.sleep(1.0)
    now_ms = int(time.time() * 1000)
    code, b = call("POST", path, {"client_key": skey, "limit": 3,
                                  "window_ms": 800, "cost": 1})
    check("sliding recovered full quota",
          code == 200 and b["allowed"] and b["remaining"] == 2, f"{code} {b}")
    # 旧记录已被清理：新窗口第一笔的 reset_at 应基于当前时刻而非最老旧记录
    check("sliding old data cleaned (reset renewed)",
          abs(b["reset_at_ms"] - (now_ms + 800)) < 1500, f"{b}")

    # 并发不超额：20 个同键并发成本 1，limit=10，恰好 10 个放行
    ckey = f"{key}:concurrent"

    def one_hit(_):
        return call("POST", path, {"client_key": ckey, "limit": 10,
                                   "window_ms": 10_000, "cost": 1})

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(one_hit, range(20)))
    allowed = [r for code, r in results if code == 200 and r["allowed"]]
    denied = [r for code, r in results if code == 429 and not r["allowed"]]
    check("concurrent no over-admit",
          len(allowed) == 10 and len(denied) == 10,
          f"allowed={len(allowed)} denied={len(denied)}")
    if allowed:
        remainings = sorted(r["remaining"] for r in allowed)
        check("concurrent remaining consistent",
              remainings == list(range(0, 10)), f"{remainings}")
        check("concurrent denied retry_after present",
              all(r["retry_after_ms"] > 0 for r in denied), f"{denied[:2]}")

    idempotency_checks()
    limit_retry_after_checks()
    limit_mixed_window_checks()


def idempotency_checks():
    """request_id 幂等：回放不重复扣减 / 并发只扣一次 / 429 回放 / 跨窗口复用 / 校验。"""
    base = f"smoke-idem:{int(time.time() * 1000)}"
    path = "/v1/limits/check"

    # 参数校验：空串 / 过长 / 含非法字符 -> 422
    code, _ = call("POST", path, {"client_key": base, "limit": 10,
                                  "window_ms": 10_000, "cost": 1,
                                  "request_id": ""})
    check("idem empty request_id -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path, {"client_key": base, "limit": 10,
                                  "window_ms": 10_000, "cost": 1,
                                  "request_id": "x" * 129})
    check("idem long request_id -> 422", code == 422, f"code={code}")
    code, _ = call("POST", path, {"client_key": base, "limit": 10,
                                  "window_ms": 10_000, "cost": 1,
                                  "request_id": "bad id/中"})
    check("idem illegal chars request_id -> 422", code == 422, f"code={code}")

    # 首次请求放行并回显 request_id；非回放不带 Idempotent-Replay
    key = f"{base}:basic"
    rid = "req-0001"
    code, headers, b = call_ex("POST", path,
                               {"client_key": key, "limit": 3, "window_ms": 10_000,
                                "cost": 1, "request_id": rid})
    check("idem first allow", code == 200 and b["allowed"]
          and b["remaining"] == 2 and b["request_id"] == rid
          and b["idempotent_replay"] is False, f"{code} {b}")
    check("idem first no replay header",
          headers.get("Idempotent-Replay") is None,
          f"Idempotent-Replay={headers.get('Idempotent-Replay')}")

    # 窗口内其他请求把额度打满（与 rid 无关，验证回放不受后续状态影响）
    call("POST", path, {"client_key": key, "limit": 3,
                        "window_ms": 10_000, "cost": 2})
    # 此时真实额度已满；重试相同 request_id 必须回放首次结果（remaining=2）且不扣减
    code, headers, b2 = call_ex("POST", path,
                                {"client_key": key, "limit": 3, "window_ms": 10_000,
                                 "cost": 1, "request_id": rid})
    check("idem replay returns first result",
          code == 200 and b2["allowed"] and b2["remaining"] == 2
          and b2["reset_at_ms"] == b["reset_at_ms"]
          and b2["request_id"] == rid and b2["idempotent_replay"] is True,
          f"{code} first={b} replay={b2}")
    check("idem replay header",
          headers.get("Idempotent-Replay") == "true",
          f"Idempotent-Replay={headers.get('Idempotent-Replay')}")

    # 回放期间的重试参数不重新判定（limit/window 仍以首次记录为准）
    code, b3 = call("POST", path,
                    {"client_key": key, "limit": 99, "window_ms": 10_000,
                     "cost": 1, "request_id": rid})
    check("idem replay echoes first limit/window",
          code == 200 and b3["idempotent_replay"] and b3["limit"] == 3
          and b3["window_ms"] == 10_000, f"{code} {b3}")

    # 不带 request_id 的普通请求不受影响：额度已满 -> 429
    code, b4 = call("POST", path, {"client_key": key, "limit": 3,
                                   "window_ms": 10_000, "cost": 1})
    check("idem non-idem path still charged",
          code == 429 and not b4["allowed"] and b4["request_id"] is None,
          f"{code} {b4}")

    # 429 结果同样被记录：先在新键打满窗口，再携带 request_id 请求被拒
    dkey = f"{base}:denied"
    call("POST", path, {"client_key": dkey, "limit": 2,
                        "window_ms": 10_000, "cost": 2})
    drid = "req-deny-1"
    code, headers1, d1 = call_ex("POST", path,
                                 {"client_key": dkey, "limit": 2, "window_ms": 10_000,
                                  "cost": 1, "request_id": drid})
    check("idem first deny -> 429",
          code == 429 and not d1["allowed"] and d1["retry_after_ms"] > 0
          and d1["idempotent_replay"] is False, f"{code} {d1}")
    check("idem first deny Retry-After",
          headers1.get("Retry-After") is not None,
          f"Retry-After={headers1.get('Retry-After')}")
    # 窗口滑出部分后重试：仍回放首次的 429（同一 reset_at_ms / retry_after_ms 快照）
    code, headers2, d2 = call_ex("POST", path,
                                 {"client_key": dkey, "limit": 2, "window_ms": 10_000,
                                  "cost": 1, "request_id": drid})
    check("idem denied replay",
          code == 429 and d2["idempotent_replay"] is True
          and d2["allowed"] is False
          and d2["reset_at_ms"] == d1["reset_at_ms"]
          and d2["retry_after_ms"] == d1["retry_after_ms"],
          f"{code} first={d1} replay={d2}")
    check("idem denied replay headers",
          headers2.get("Idempotent-Replay") == "true"
          and headers2.get("Retry-After") == headers1.get("Retry-After"),
          f"h1={dict(headers1)} h2={dict(headers2)}")

    # 并发相同 request_id：20 个并发只有 1 个完成扣减，其余全部回放首次结果
    ckey = f"{base}:concurrent"
    crid = "req-conc-1"
    body = {"client_key": ckey, "limit": 10, "window_ms": 10_000,
            "cost": 1, "request_id": crid}

    def one_idem_hit(_):
        return call_ex("POST", path, body)

    with ThreadPoolExecutor(max_workers=20) as pool:
        cresults = list(pool.map(one_idem_hit, range(20)))
    firsts = [d for _, h, d in cresults if d.get("idempotent_replay") is False]
    replays = [d for _, h, d in cresults if d.get("idempotent_replay") is True]
    check("idem concurrent single deduction",
          len(firsts) == 1 and len(replays) == 19
          and all(d["allowed"] and d["remaining"] == 9
                  for _, _, d in cresults)
          and all(d["reset_at_ms"] == firsts[0]["reset_at_ms"]
                  for d in replays),
          f"firsts={len(firsts)} replays={len(replays)}")
    check("idem concurrent replay headers",
          sum(1 for _, h, _ in cresults if h.get("Idempotent-Replay") == "true") == 19,
          f"{[h.get('Idempotent-Replay') for _, h, _ in cresults]}")
    # 实际只扣了一笔：不带 request_id 再请求一笔应放行且 remaining=8
    code, after = call("POST", path, {"client_key": ckey, "limit": 10,
                                      "window_ms": 10_000, "cost": 1})
    check("idem concurrent total charged once",
          code == 200 and after["allowed"] and after["remaining"] == 8,
          f"{code} {after}")

    # request_id 仅在对应窗口存活：800ms 窗口结束后同一 request_id 可再次使用，
    # 且按新请求重新判定扣减
    skey = f"{base}:sliding"
    srid = "req-window-1"
    code, s1 = call("POST", path,
                    {"client_key": skey, "limit": 3, "window_ms": 800,
                     "cost": 1, "request_id": srid})
    check("idem short window first", code == 200 and s1["allowed"], f"{code} {s1}")
    code, s2 = call("POST", path,
                    {"client_key": skey, "limit": 3, "window_ms": 800,
                     "cost": 1, "request_id": srid})
    check("idem short window replay",
          code == 200 and s2["idempotent_replay"] is True, f"{code} {s2}")
    time.sleep(1.1)
    code, s3 = call("POST", path,
                    {"client_key": skey, "limit": 3, "window_ms": 800,
                     "cost": 1, "request_id": srid})
    check("idem request_id reusable across window",
          code == 200 and s3["allowed"] and s3["idempotent_replay"] is False
          and s3["remaining"] == 2
          and s3["reset_at_ms"] != s1["reset_at_ms"], f"{code} {s3}")

    # request_id 作用域为 client_key：不同 client_key 使用同一 request_id 互不影响
    xkey, ykey = f"{base}:scope-a", f"{base}:scope-b"
    shared_rid = "req-shared"
    code, x1 = call("POST", path,
                    {"client_key": xkey, "limit": 1, "window_ms": 10_000,
                     "cost": 1, "request_id": shared_rid})
    code, y1 = call("POST", path,
                    {"client_key": ykey, "limit": 1, "window_ms": 10_000,
                     "cost": 1, "request_id": shared_rid})
    check("idem request_id scoped per client_key",
          x1["allowed"] and y1["allowed"]
          and x1["idempotent_replay"] is False
          and y1["idempotent_replay"] is False, f"{x1} {y1}")


def limit_retry_after_checks():
    """/v1/limits/check 加权拒绝的重试等待回归：

    按记录到期顺序累计释放的成本，返回首次足以容纳本次 cost 的等待毫秒数
    （同毫秒到期记录合并判断）；reset_at_ms 保持"最老一笔滑出"语义不变。"""
    base = f"smoke-limit-retry:{int(time.time() * 1000)}"
    path = "/v1/limits/check"

    # 任务场景：limit=5，同一窗口内先后通过成本 1 与 4（已用 5），随后成本 3
    # 被拒。旧行为 retry_after_ms 只等最早的成本 1 滑出——届时已用 4 仍放不下
    # 3，会再次 429；修复后必须等到成本 4 那笔也滑出（累计释放 1+4=5 >= 3）。
    key = f"{base}:weighted"
    code, b1 = call("POST", path, {"client_key": key, "limit": 5,
                                   "window_ms": 4_000, "cost": 1})
    check("retry allow cost 1",
          code == 200 and b1["allowed"] and b1["remaining"] == 4, f"{code} {b1}")
    first_reset = b1["reset_at_ms"]  # 最早一笔（成本 1）滑出时刻
    time.sleep(1.0)  # 拉开两笔记录的到期时刻，便于区分两种等待时长
    code, b2 = call("POST", path, {"client_key": key, "limit": 5,
                                   "window_ms": 4_000, "cost": 4})
    check("retry allow cost 4",
          code == 200 and b2["allowed"] and b2["remaining"] == 0, f"{code} {b2}")
    check("retry reset_at still oldest after cost 4",
          b2["reset_at_ms"] == first_reset, f"{b2} vs {first_reset}")

    code, headers, d = call_ex("POST", path, {"client_key": key, "limit": 5,
                                              "window_ms": 4_000, "cost": 3})
    check("retry deny cost 3 -> 429",
          code == 429 and not d["allowed"] and d["remaining"] == 0, f"{code} {d}")
    # reset_at_ms 语义不变：仍是最早记录（成本 1）滑出时刻
    check("retry deny reset_at unchanged",
          d["reset_at_ms"] == first_reset, f"{d}")
    now_ms = int(time.time() * 1000)
    oldest_wait = d["reset_at_ms"] - now_ms
    # retry_after_ms 必须指向第二笔（成本 4）滑出：比最早记录滑出晚约 1 秒
    check("retry waits until enough released",
          oldest_wait + 500 < d["retry_after_ms"] <= oldest_wait + 2000,
          f"{d} oldest_wait={oldest_wait}")
    check("retry Retry-After header matches body",
          headers.get("Retry-After") is not None
          and int(headers["Retry-After"]) == -(-d["retry_after_ms"] // 1000),
          f"Retry-After={headers.get('Retry-After')} body={d}")

    # 端到端回归：按旧语义只等最早记录滑出后重试，仍应 429（复现修复前提）；
    # 再等第二笔也滑出后才放行
    time.sleep(max(0.0, oldest_wait / 1000) + 0.3)
    code, d2 = call("POST", path, {"client_key": key, "limit": 5,
                                   "window_ms": 4_000, "cost": 3})
    check("retry still 429 after only oldest slid out",
          code == 429 and not d2["allowed"], f"{code} {d2}")
    time.sleep(1.5)
    code, a3 = call("POST", path, {"client_key": key, "limit": 5,
                                   "window_ms": 4_000, "cost": 3})
    check("retry allowed after enough slid out",
          code == 200 and a3["allowed"] and a3["remaining"] == 2, f"{code} {a3}")

    # 同毫秒到期记录合并判断：直连 Redis 写入两笔相同 score 的记录（公开 API
    # 无法控制同毫秒落点）。窗口内已用 5（1 + 2 + 2），请求成本 2 被拒：
    # 最老一笔（成本 1）滑出只释放 1 < 2，必须等到同毫秒的两笔（成本 2+2）
    # 一起滑出（累计释放 5 >= 2）。
    limit_prefix = os.environ.get("SMOKE_LIMIT_KEY_PREFIX", "ratelimit")
    host = os.environ.get("SMOKE_REDIS_HOST", "localhost")
    port = int(os.environ.get("SMOKE_REDIS_PORT", "6379"))
    mkey = f"{base}:same-ms"
    zkey = f"{limit_prefix}:{mkey}"
    try:
        r = _RespClient(host, port)
    except OSError as exc:
        check("retry same-ms redis reachable", False, f"{host}:{port} {exc}")
        return
    try:
        sec, usec = r.execute("TIME")
        now_server = int(sec) * 1000 + int(usec) // 1000
        r.execute("ZADD", zkey, now_server - 1000, "a" * 32 + ":1")
        r.execute("ZADD", zkey, now_server, "b" * 32 + ":2")
        r.execute("ZADD", zkey, now_server, "c" * 32 + ":2")
        r.execute("PEXPIRE", zkey, 10_000)
        code, headers, dm = call_ex("POST", path,
                                    {"client_key": mkey, "limit": 5,
                                     "window_ms": 10_000, "cost": 2})
        check("retry same-ms deny -> 429",
              code == 429 and not dm["allowed"], f"{code} {dm}")
        # reset_at_ms 仍是最老一笔（now-1000，成本 1）滑出时刻
        check("retry same-ms reset_at is oldest",
              dm["reset_at_ms"] == now_server - 1000 + 10_000,
              f"{dm} expect {now_server - 1000 + 10_000}")
        # retry_after 必须等到同毫秒两笔（now）一起滑出：≈10s，而非 ≈9s
        now_ms = int(time.time() * 1000)
        check("retry same-ms waits for whole group",
              dm["retry_after_ms"] > dm["reset_at_ms"] - now_ms + 500,
              f"{dm} oldest_wait={dm['reset_at_ms'] - now_ms}")
        check("retry same-ms Retry-After header matches body",
              headers.get("Retry-After") is not None
              and int(headers["Retry-After"]) == -(-dm["retry_after_ms"] // 1000),
              f"Retry-After={headers.get('Retry-After')} body={dm}")
    finally:
        r.close()


def limit_mixed_window_checks():
    """/v1/limits/check 同一 client_key 混用不同 window_ms 的回归：

    长窗口放入的记录不得被随后的短窗口检查删除（旧实现按本次窗口
    ZREMRANGEBYSCORE 清旧并缩短键 TTL，会丢掉仍属于长窗口的记录，
    切回长窗口后错误放行）；记录按服务端最大允许窗口保留，
    每次请求只按本次 window_ms 筛选判定。"""
    base = f"smoke-limit-mixed:{int(time.time() * 1000)}"
    path = "/v1/limits/check"
    key = f"{base}:k"

    # 长窗口（2 秒）先放行一笔 cost=2（limit=2，长窗口已打满）
    code, b1 = call("POST", path, {"client_key": key, "limit": 2,
                                   "window_ms": 2_000, "cost": 2})
    check("mixed long-window allow cost 2",
          code == 200 and b1["allowed"] and b1["remaining"] == 0, f"{code} {b1}")

    time.sleep(1.0)  # 首笔记录滑出 300ms 短窗口，但仍在 2 秒长窗口内

    # 短窗口（300ms）检查：首笔记录不在短窗口内 -> 放行；
    # 关键：本次不得删除仍属于长窗口的首笔记录
    code, b2 = call("POST", path, {"client_key": key, "limit": 2,
                                   "window_ms": 300, "cost": 1})
    check("mixed short-window allow (record outside short window)",
          code == 200 and b2["allowed"] and b2["remaining"] == 1, f"{code} {b2}")

    # 切回长窗口：两笔记录都在窗内（累计 3 > 2）-> 必须 429；
    # 旧实现中短窗口检查已删除首笔记录，此处会错误放行
    code, headers, b3 = call_ex("POST", path, {"client_key": key, "limit": 2,
                                               "window_ms": 2_000, "cost": 1})
    check("mixed back to long window -> 429",
          code == 429 and not b3["allowed"], f"{code} {b3}")
    # 窗口内累计（3）已超过 limit（2）：remaining 不得为负，按 0 返回
    check("mixed remaining clamped at 0", b3["remaining"] == 0, f"{b3}")
    # reset_at_ms 仍是首笔（最老）记录滑出长窗口的时刻，与首次响应一致
    check("mixed reset_at tracks oldest record",
          b3["reset_at_ms"] == b1["reset_at_ms"], f"{b1} {b3}")
    check("mixed retry_after positive", b3["retry_after_ms"] > 0, f"{b3}")
    check("mixed Retry-After header",
          headers.get("Retry-After") is not None
          and int(headers["Retry-After"]) >= 1,
          f"Retry-After={headers.get('Retry-After')}")

    # 首笔记录滑出长窗口后（第二笔仍在窗内，used=1）：长窗口放行且 remaining=0
    time.sleep(1.4)
    code, b4 = call("POST", path, {"client_key": key, "limit": 2,
                                   "window_ms": 2_000, "cost": 1})
    check("mixed long-window recovers after oldest slides out",
          code == 200 and b4["allowed"] and b4["remaining"] == 0, f"{code} {b4}")


def set_dependencies(k, deps):
    return call("POST", f"/v1/dependencies/{k}", {"dependencies": deps})


def dependencies_ready(k):
    return call("GET", f"/v1/dependencies/{k}/ready")


def commit_task_result(k, result):
    """申请短租约并提交持久结果，返回 (http_code, commit_body)。"""
    code, a = call("POST", "/v1/leases/acquire", {"task_key": k, "ttl_ms": 60_000})
    if code != 200 or not a.get("acquired"):
        return code, a
    return call(
        "POST",
        "/v1/results/commit",
        {"task_key": k, "token": a["token"],
         "fencing_token": a["fencing_token"], "result": result},
    )


def build_fan_subtree(tag, l3_parent_count):
    """构造一棵 16 叉扇出依赖树（不设置根定义），返回根应依赖的 16 个 l1 键。

    结构：16 个 l1，每个 l1 依赖 16 个 l2（共 256 个 l2），
    前 l3_parent_count 个 l2 各依赖 16 个 l3。
    从 16 个 l1 出发去重遍历的节点数 = 16 + 256 + 16*l3_parent_count：
      l3_parent_count=47 -> 1024（恰好放行）；=48 -> 1040（超过 1024 拒绝）。
    """
    l1 = [f"{tag}:l1:{i}" for i in range(16)]
    l2_flat = [f"{tag}:l2:{i}:{j}" for i in range(16) for j in range(16)]
    l2_by_i = [l2_flat[i * 16:(i + 1) * 16] for i in range(16)]

    def put(mapping):
        key, deps = mapping
        return set_dependencies(key, deps)

    # 每个 l1 -> 16 个 l2（16 个请求并发）
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(put, zip(l1, l2_by_i)))
    # 前 l3_parent_count 个 l2 -> 16 个 l3（并发）
    packs = [
        (l2_flat[p], [f"{l2_flat[p]}:l3:{j}" for j in range(16)])
        for p in range(l3_parent_count)
    ]
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(put, packs))
    return l1


def dependency_checks():
    """/v1/dependencies：保存去重 / 自身依赖 / 环检测 / 1024 上限 / ready 判定。"""
    base = f"smoke-deps:{int(time.time() * 1000)}"
    A, B, C, D, E = [f"{base}:{x}" for x in "ABCDE"]

    # 没有定义的任务视为可执行
    code, b = dependencies_ready(A)
    check("deps undefined task ready",
          code == 200 and b["ready"] is True and b["pending"] == []
          and b["task_key"] == A, f"{code} {b}")

    # 保存 A -> [B, C]：200、回显顺序
    code, b = set_dependencies(A, [B, C])
    check("deps save ok",
          code == 200 and b["saved"] is True
          and b["dependencies"] == [B, C] and b["reason"] is None, f"{code} {b}")

    # 前置均未提交结果 -> ready=false，pending 按定义顺序
    code, b = dependencies_ready(A)
    check("deps not ready pending all",
          code == 200 and b["ready"] is False and b["pending"] == [B, C],
          f"{code} {b}")

    # 保存时按首次出现顺序去重（重复不报错，去重后落库）
    code, b = set_dependencies(A, [B, C, B, C, B])
    check("deps dedup on save",
          code == 200 and b["saved"] and b["dependencies"] == [B, C],
          f"{code} {b}")

    # 自身依赖 -> 422（不触达 Redis；单独自身 / 混在列表中都拒绝）
    code, _ = set_dependencies(A, [A])
    check("deps self only -> 422", code == 422, f"code={code}")
    code, _ = set_dependencies(A, [B, A])
    check("deps self mixed -> 422", code == 422, f"code={code}")

    # 去重后超过 16 个唯一键 -> 422；原始 17 个但去重后 16 -> 200
    code, _ = set_dependencies(A, [f"{base}:u{i}" for i in range(17)])
    check("deps 17 unique -> 422", code == 422, f"code={code}")
    code, b = set_dependencies(A, [B, B] + [f"{base}:v{i}" for i in range(15)])
    check("deps 17 raw dedupe-to-16 -> 200",
          code == 200 and len(b["dependencies"]) == 16, f"{code} {b}")
    set_dependencies(A, [B, C])

    # 提交 B 的持久结果 -> pending 只剩 C；再提交 C -> ready=true
    code, cb = commit_task_result(B, {"v": 1})
    check("deps setup commit B", code == 200 and cb["committed"], f"{code} {cb}")
    code, b = dependencies_ready(A)
    check("deps one committed pending C",
          code == 200 and b["ready"] is False and b["pending"] == [C],
          f"{code} {b}")
    code, cc = commit_task_result(C, {"v": 2})
    check("deps setup commit C", code == 200 and cc["committed"], f"{code} {cc}")
    code, b = dependencies_ready(A)
    check("deps all committed ready",
          code == 200 and b["ready"] is True and b["pending"] == [], f"{code} {b}")
    # 与 GET /v1/results 同源：结果持久保留即视为已完成
    code, g = call("GET", f"/v1/results/{C}")
    check("deps ready uses persistent result",
          code == 200 and g["committed"] and g["result"] == {"v": 2}, f"{code} {g}")

    # 环检测只遍历现有定义，且拒绝时保留原定义
    # B 当前无定义：B -> A 会形成 A->B->A（A 现有 [B,C]）-> 409 cycle，B 仍无定义
    code, b = set_dependencies(B, [A])
    check("deps cycle B->A rejected",
          code == 409 and b["saved"] is False and b["reason"] == "cycle"
          and b["dependencies"] == [], f"{code} {b}")
    code, b = dependencies_ready(B)
    check("deps rejected B stays executable",
          code == 200 and b["ready"] is True, f"{code} {b}")

    # B -> D 合法；随后 D -> A：现有 A->B->D，dep=A 能到 target=D -> 409，D 仍无定义
    code, b = set_dependencies(B, [D])
    check("deps B->D ok", code == 200 and b["saved"], f"{code} {b}")
    code, b = set_dependencies(D, [A])
    check("deps D->A cycle rejected",
          code == 409 and b["reason"] == "cycle" and b["dependencies"] == [],
          f"{code} {b}")
    code, b = dependencies_ready(D)
    check("deps D stays ready after cycle reject",
          code == 200 and b["ready"] is True, f"{code} {b}")

    # E -> B 合法（B 现有走到 D 为死路，回不到 E）；
    # 再把 B 改成 [D, E]：dep=E 沿现有 E->B 回到 target=B -> 409，B 保留旧定义 [D]
    code, b = set_dependencies(E, [B])
    check("deps E->B ok", code == 200 and b["saved"], f"{code} {b}")
    code, b = set_dependencies(B, [D, E])
    check("deps B->E->B cycle rejected keeps old",
          code == 409 and b["reason"] == "cycle"
          and b["dependencies"] == [D], f"{code} {b}")
    # 再来一次仍 409 且回读旧定义
    code, b = set_dependencies(B, [E])
    check("deps repeat cycle reject keeps [D]",
          code == 409 and b["dependencies"] == [D], f"{code} {b}")

    # 合法整体替换生效；空列表清除定义，清除后视为可执行且键被删除
    code, b = set_dependencies(B, [C])
    check("deps replace ok", code == 200 and b["dependencies"] == [C], f"{code} {b}")
    code, b = set_dependencies(B, [])
    check("deps clear ok",
          code == 200 and b["saved"] and b["dependencies"] == [], f"{code} {b}")
    code, b = dependencies_ready(B)
    check("deps cleared task ready",
          code == 200 and b["ready"] is True and b["pending"] == [], f"{code} {b}")

    # pending 顺序与定义一致：A -> [C, B, D]，B/C 已完成，仅 D 未完成
    code, _ = set_dependencies(A, [C, B, D])
    code, b = dependencies_ready(A)
    check("deps pending order",
          code == 200 and b["ready"] is False and b["pending"] == [D],
          f"{code} {b}")
    code, _ = commit_task_result(D, {"v": 3})
    code, b = dependencies_ready(A)
    check("deps last prereq completes ready",
          code == 200 and b["ready"] is True and b["pending"] == [], f"{code} {b}")

    # 环检测单次访问节点上限 1024（纯 API 扇出构造，不直连 Redis）
    # 放行边界：恰好访问 1024 个现有节点 -> 200
    tag_ok = f"{base}:fan-ok"
    l1_ok = build_fan_subtree(tag_ok, 47)
    root_ok = f"{tag_ok}:root"
    code, b = set_dependencies(root_ok, l1_ok)
    check("deps cycle-scan exactly 1024 nodes accepted",
          code == 200 and b["saved"] and b["dependencies"] == l1_ok, f"{code} {b}")

    # 超限：访问 1040 个节点 -> 409 too_many_nodes，且保留原定义 [sentinel]
    tag_bad = f"{base}:fan-bad"
    l1_bad = build_fan_subtree(tag_bad, 48)
    root_bad = f"{tag_bad}:root"
    sentinel = f"{tag_bad}:sentinel"
    code, _ = set_dependencies(root_bad, [sentinel])
    check("deps overflow pre-set old def", code == 200, f"code={code}")
    code, b = set_dependencies(root_bad, l1_bad)
    check("deps cycle-scan 1040 nodes -> 409 too_many_nodes",
          code == 409 and b["saved"] is False
          and b["reason"] == "too_many_nodes" and b["dependencies"] == [sentinel],
          f"{code} {b}")
    # 原定义仍生效：ready 只看 sentinel（未提交）-> pending [sentinel]
    code, b = dependencies_ready(root_bad)
    check("deps overflow old definition still active",
          code == 200 and b["ready"] is False and b["pending"] == [sentinel],
          f"{code} {b}")


def commit_prereq_result(k, result):
    """申请长租约、提交持久结果，返回 (commit_code, commit_body, acquire_body)。"""
    code, a = call("POST", "/v1/leases/acquire", {"task_key": k, "ttl_ms": 60_000})
    if code != 200 or not a.get("acquired"):
        return code, a, None
    code, c = call("POST", "/v1/results/commit",
                   {"task_key": k, "token": a["token"],
                    "fencing_token": a["fencing_token"], "result": result})
    return code, c, a


def checked_commit_ex(k, token, fence, result, snapshot):
    body = {"task_key": k, "token": token, "fencing_token": fence,
            "result": result, "snapshot": snapshot}
    return call_ex("POST", "/v1/results/commit-checked", body)


def checked_commit(k, token, fence, result, snapshot):
    code, _, body = checked_commit_ex(k, token, fence, result, snapshot)
    return code, body


def checked_commit_checks():
    """/v1/dependencies/.../snapshot + /v1/results/commit-checked 全链路。"""
    base = f"smoke-checked:{int(time.time() * 1000)}"
    T, A, B, C = [f"{base}:{x}" for x in "TABC"]

    def snapshot(k):
        return call("GET", f"/v1/dependencies/{k}/snapshot")

    # 无定义任务 -> status=snapshot、空快照
    code, b = snapshot(T)
    check("checked snapshot undefined -> empty",
          code == 200 and b["task_key"] == T and b["status"] == "snapshot"
          and b["dependencies"] == [] and b["pending"] == [], f"{code} {b}")

    # 定义 T -> [A, B]，前置均未完成 -> pending（不出可提交快照）
    code, b = call("POST", f"/v1/dependencies/{T}", {"dependencies": [A, B]})
    check("checked setup deps", code == 200 and b["saved"], f"{code} {b}")
    code, b = snapshot(T)
    check("checked snapshot pending",
          code == 200 and b["status"] == "pending"
          and b["pending"] == [A, B] and b["dependencies"] == [], f"{code} {b}")

    # 提交 A 后 pending 只剩 B（仍按定义顺序）
    code, ca, ca_cred = commit_prereq_result(A, {"v": 1})
    check("checked setup commit A", code == 200 and ca["committed"], f"{code} {ca}")
    code, b = snapshot(T)
    check("checked snapshot one pending",
          b["status"] == "pending" and b["pending"] == [B]
          and b["dependencies"] == [], f"{b}")

    # B 完成 -> 可提交快照；版本与 GET /v1/results 同源、顺序与定义一致
    code, cb, cb_cred = commit_prereq_result(B, {"v": 1})
    check("checked setup commit B", code == 200 and cb["committed"], f"{code} {cb}")
    code, snap = snapshot(T)
    check("checked snapshot ready",
          code == 200 and snap["status"] == "snapshot" and snap["pending"] == []
          and [(d["task_key"], d["fencing_token"]) for d in snap["dependencies"]]
          == [(A, 1), (B, 1)], f"{code} {snap}")
    code, ga = call("GET", f"/v1/results/{A}")
    code2, gb = call("GET", f"/v1/results/{B}")
    check("checked snapshot versions match results",
          code == 200 and code2 == 200
          and ga["fencing_token"] == snap["dependencies"][0]["fencing_token"]
          and gb["fencing_token"] == snap["dependencies"][1]["fencing_token"],
          f"{ga} {gb}")

    # 无租约 checked 提交 -> 409 not_found（凭据校验先于前置核对，不写结果）
    code, b = checked_commit(T, "ghost", 1, {"ok": True}, snap["dependencies"])
    check("checked no lease -> not_found",
          code == 409 and not b["committed"] and b["reason"] == "not_found",
          f"{code} {b}")
    code, g = call("GET", f"/v1/results/{T}")
    check("checked not_found wrote nothing", not g["committed"], f"{g}")

    # 持有有效租约 + 快照一致 -> committed
    code, a = call("POST", "/v1/leases/acquire", {"task_key": T, "ttl_ms": 60_000})
    check("checked setup acquire T", code == 200 and a["acquired"], f"{code} {a}")
    ttoken, tfence = a["token"], a["fencing_token"]
    payload = {"ok": True, "rows": 3}
    code, headers, c = checked_commit_ex(
        T, ttoken, tfence, payload, snap["dependencies"])
    check("checked commit ok",
          code == 200 and c["committed"] and c["replayed"] is False
          and c["fencing_token"] == tfence and c["result"] == payload
          and headers.get("Result-Replay") is None, f"{code} {c}")
    committed_at = c["committed_at_ms"]

    # 相同内容重试（快照不变）-> 重放：200 / replayed / 头 / 时间戳不刷新
    code, headers, c2 = checked_commit_ex(
        T, ttoken, tfence, {"rows": 3, "ok": True}, snap["dependencies"])
    check("checked replay",
          code == 200 and c2["replayed"] is True
          and c2["committed_at_ms"] == committed_at
          and headers.get("Result-Replay") == "true", f"{code} {c2}")

    # 同防栅序号不同内容 -> 409 result_conflict，已存结果不变
    code, c3 = checked_commit(
        T, ttoken, tfence, {"ok": False}, snap["dependencies"])
    check("checked result_conflict",
          code == 409 and c3["reason"] == "result_conflict", f"{code} {c3}")
    code, g = call("GET", f"/v1/results/{T}")
    check("checked conflict keeps stored",
          g["result"] == payload and g["committed_at_ms"] == committed_at, f"{g}")

    # 错误凭据 -> 409 conflict（先于前置核对）
    code, b = checked_commit(T, "wrong", tfence, payload, snap["dependencies"])
    check("checked wrong token -> conflict",
          code == 409 and b["reason"] == "conflict", f"{code} {b}")
    code, b = checked_commit(T, ttoken, tfence + 9, payload, snap["dependencies"])
    check("checked wrong fencing -> conflict",
          code == 409 and b["reason"] == "conflict", f"{code} {b}")

    # 前置 A 被新租约提交新版本 -> 旧快照 stale_dependencies，T 结果不变
    code, _ = call("POST", "/v1/leases/release",
                   {"task_key": A, "token": ca_cred["token"],
                    "fencing_token": ca_cred["fencing_token"]})
    check("checked release A", code == 200, f"code={code}")
    code, a2 = call("POST", "/v1/leases/acquire", {"task_key": A, "ttl_ms": 60_000})
    code, ca2 = call("POST", "/v1/results/commit",
                     {"task_key": A, "token": a2["token"],
                      "fencing_token": a2["fencing_token"], "result": {"v": 2}})
    check("checked A new version",
          code == 200 and ca2["fencing_token"] == 2, f"{code} {ca2}")
    code, b = checked_commit(T, ttoken, tfence, payload, snap["dependencies"])
    check("checked stale dep version -> 409",
          code == 409 and not b["committed"]
          and b["reason"] == "stale_dependencies", f"{code} {b}")
    code, g = call("GET", f"/v1/results/{T}")
    check("checked stale version changed nothing",
          g["result"] == payload and g["committed_at_ms"] == committed_at, f"{g}")

    # 重新拉快照后，用新租约（fence 2）提交新内容 -> 成功覆盖
    code, snap2 = snapshot(T)
    check("checked fresh snapshot versions",
          [(d["task_key"], d["fencing_token"]) for d in snap2["dependencies"]]
          == [(A, 2), (B, 1)], f"{code} {snap2}")
    code, _ = call("POST", "/v1/leases/release",
                   {"task_key": T, "token": ttoken, "fencing_token": tfence})
    code, at2 = call("POST", "/v1/leases/acquire", {"task_key": T, "ttl_ms": 60_000})
    check("checked T re-acquire fence 2",
          code == 200 and at2["fencing_token"] == tfence + 1, f"{code} {at2}")
    code, c = checked_commit(
        T, at2["token"], at2["fencing_token"], {"ok": "v2"}, snap2["dependencies"])
    check("checked commit with fresh snapshot",
          code == 200 and c["committed"] and c["replayed"] is False
          and c["result"] == {"ok": "v2"}, f"{code} {c}")

    # 定义变更：T -> [A, B, C]（C 未完成）：旧快照 stale；snapshot 又变 pending
    code, _ = call("POST", f"/v1/dependencies/{T}", {"dependencies": [A, B, C]})
    code, b = checked_commit(
        T, at2["token"], at2["fencing_token"], {"x": 1}, snap2["dependencies"])
    check("checked definition added -> stale",
          code == 409 and b["reason"] == "stale_dependencies", f"{code} {b}")
    code, b = snapshot(T)
    check("checked snapshot pending after change",
          b["status"] == "pending" and b["pending"] == [C], f"{b}")

    # 快照乱序（[B, A]）-> stale；伪造版本号 -> stale
    code, b = checked_commit(
        T, at2["token"], at2["fencing_token"], {"x": 1},
        [{"task_key": B, "fencing_token": 1},
         {"task_key": A, "fencing_token": 2}])
    check("checked reordered snapshot -> stale",
          code == 409 and b["reason"] == "stale_dependencies", f"{code} {b}")
    code, _ = call("POST", f"/v1/dependencies/{T}", {"dependencies": [A, B]})
    code, b = checked_commit(
        T, at2["token"], at2["fencing_token"], {"x": 1},
        [{"task_key": A, "fencing_token": 1},
         {"task_key": B, "fencing_token": 1}])
    check("checked forged old version -> stale",
          code == 409 and b["reason"] == "stale_dependencies", f"{code} {b}")

    # 空快照：清除定义后 [] 与缺省字段都应通过（先重放再用新租约写新内容）
    code, _ = call("POST", f"/v1/dependencies/{T}", {"dependencies": []})
    code, b = checked_commit(
        T, at2["token"], at2["fencing_token"], {"ok": "v2"}, [])
    check("checked empty snapshot replay",
          code == 200 and b["replayed"] is True, f"{code} {b}")
    code, _ = call("POST", "/v1/leases/release",
                   {"task_key": T, "token": at2["token"],
                    "fencing_token": at2["fencing_token"]})
    code, at3 = call("POST", "/v1/leases/acquire", {"task_key": T, "ttl_ms": 60_000})
    code, b = checked_commit(
        T, at3["token"], at3["fencing_token"], {"final": 1}, [])
    check("checked empty snapshot commit",
          code == 200 and b["committed"] and not b["replayed"], f"{code} {b}")
    # 缺省 snapshot 字段等价空快照 -> 重放
    code, _, body = call_ex("POST", "/v1/results/commit-checked",
                            {"task_key": T, "token": at3["token"],
                             "fencing_token": at3["fencing_token"],
                             "result": {"final": 1}})
    check("checked missing snapshot defaults empty",
          code == 200 and body["replayed"] is True, f"{code} {body}")

    # 参数校验：snapshot 重复键 / 超过 16 项 / fencing<1 -> 422
    code, _ = checked_commit(
        T, at3["token"], at3["fencing_token"], {},
        [{"task_key": A, "fencing_token": 1},
         {"task_key": A, "fencing_token": 1}])
    check("checked duplicate snapshot -> 422", code == 422, f"code={code}")
    code, _ = checked_commit(
        T, at3["token"], at3["fencing_token"], {},
        [{"task_key": f"{base}:k{i}", "fencing_token": 1} for i in range(17)])
    check("checked snapshot >16 -> 422", code == 422, f"code={code}")
    code, _ = checked_commit(
        T, at3["token"], at3["fencing_token"], {},
        [{"task_key": A, "fencing_token": 0}])
    check("checked snapshot fencing <1 -> 422", code == 422, f"code={code}")

    # 过期租约：checked 提交同样按 expired/not_found 拒绝，且不写结果
    ekey = f"{base}:expiry"
    code, ea = call("POST", "/v1/leases/acquire",
                    {"task_key": ekey, "ttl_ms": 300})
    code, ec = checked_commit(ekey, ea["token"], ea["fencing_token"], {"g": 1}, [])
    check("checked short lease commit ok",
          code == 200 and ec["committed"], f"{code} {ec}")
    time.sleep(0.7)
    code, old = checked_commit(
        ekey, ea["token"], ea["fencing_token"], {"g": 2}, [])
    check("checked expired lease rejected",
          code == 409 and old["reason"] in ("expired", "not_found"), f"{code} {old}")
    code, g = call("GET", f"/v1/results/{ekey}")
    check("checked expired changed nothing",
          g["result"] == {"g": 1}, f"{g}")

    # 并发：有效租约 + 一致快照，20 个同内容并发恰好 1 首次 / 19 重放
    cckey = f"{base}:concurrent"
    code, ca_cc = call("POST", "/v1/leases/acquire",
                       {"task_key": cckey, "ttl_ms": 60_000})
    cbody = {"task_key": cckey, "token": ca_cc["token"],
             "fencing_token": ca_cc["fencing_token"],
             "result": {"v": 7}, "snapshot": []}

    def one_checked(_):
        return call_ex("POST", "/v1/results/commit-checked", cbody)

    with ThreadPoolExecutor(max_workers=20) as pool:
        cresults = list(pool.map(one_checked, range(20)))
    firsts = [d for _, _, d in cresults if d.get("replayed") is False]
    replays = [d for _, _, d in cresults if d.get("replayed") is True]
    check("checked concurrent single commit",
          all(s == 200 for s, _, _ in cresults)
          and len(firsts) == 1 and len(replays) == 19
          and all(d["committed_at_ms"] == firsts[0]["committed_at_ms"]
                  for d in replays),
          f"firsts={len(firsts)} replays={len(replays)}")

    # 审计：stale_dependencies 与 committed/replayed 都按 result_commit 落账。
    # 首次版本漂移发生在 T 的 fence 1，定义 / 乱序 / 伪造发生在 fence 2
    events = fetch_all_events(T, page_size=50)
    results = [e["result"] for e in events if e["op"] == "result_commit"]
    check("checked audit stale + committed + replayed",
          "stale_dependencies" in results and "committed" in results
          and "replayed" in results, f"{results}")
    stale_events = [e for e in events
                    if e["op"] == "result_commit"
                    and e["result"] == "stale_dependencies"]
    check("checked audit stale event fencing",
          len(stale_events) >= 4
          and {e["fencing_token"] for e in stale_events} == {tfence, at2["fencing_token"]},
          f"{[(e['result'], e['fencing_token']) for e in stale_events]}")


def capacity_checks():
    """按资源键分配并发容量：
    未配置拒绝申请 / 设置容量 / 加权申请剩余递减 / 超限 429 不占用且带
    Retry-After / 权重超容量 409 / 容量下调低于有效占用 409 保留原配置 /
    续期仅认凭据（错误 / 过期 409 not_found）/ 释放仅认凭据且幂等 /
    过期与旧凭据不影响新占用 / 短 TTL 槽位回收 / 参数校验 /
    并发绝不超配（40 并发恰好放行容量笔）。"""
    base = f"smoke-cap:{int(time.time() * 1000)}"
    acq_path = "/v1/capacity/slots/acquire"
    renew_path = "/v1/capacity/slots/renew"
    release_path = "/v1/capacity/slots/release"

    def cap_path(rk):
        return f"/v1/capacity/{rk}"

    def set_cap(rk, capacity):
        return call("PUT", cap_path(rk), {"capacity": capacity})

    def acquire(rk, weight=1, ttl=None):
        body = {"resource_key": rk, "weight": weight}
        if ttl is not None:
            body["ttl_ms"] = ttl
        return call("POST", acq_path, body)

    def acquire_h(rk, weight=1, ttl=None):
        body = {"resource_key": rk, "weight": weight}
        if ttl is not None:
            body["ttl_ms"] = ttl
        return call_ex("POST", acq_path, body)

    def renew(rk, cred, ttl=None):
        body = {"resource_key": rk, "credential": cred}
        if ttl is not None:
            body["ttl_ms"] = ttl
        return call("POST", renew_path, body)

    def release(rk, cred):
        return call("POST", release_path, {"resource_key": rk, "credential": cred})

    key = f"{base}:gpu"

    # 未配置容量即申请 -> 409 not_configured
    code, b = acquire(key)
    check("cap acquire unconfigured -> 409",
          code == 409 and b["acquired"] is False and b["reason"] == "not_configured",
          f"{code} {b}")

    # 参数校验：容量 0 / 负、权重 0 / 负、空资源键、缺凭据 -> 422
    code, _ = set_cap(key, 0)
    check("cap capacity zero -> 422", code == 422, f"code={code}")
    code, _ = set_cap(key, -3)
    check("cap capacity negative -> 422", code == 422, f"code={code}")
    code, _ = acquire(key, weight=0)
    check("cap weight zero -> 422", code == 422, f"code={code}")
    code, _ = call("POST", acq_path, {"resource_key": "", "weight": 1})
    check("cap empty resource_key -> 422", code == 422, f"code={code}")
    code, _ = call("POST", renew_path, {"resource_key": key})
    check("cap renew missing credential -> 422", code == 422, f"code={code}")
    # ttl 越界 -> 400（与租约 / 限流一致，由 resolve_ttl 判定）
    code, _ = acquire(key, ttl=999_999)
    check("cap ttl out of range -> 400", code == 400, f"code={code}")

    # 设置容量 3：回显容量与有效占用 0
    code, b = set_cap(key, 3)
    check("cap set ok",
          code == 200 and b["configured"] is True and b["capacity"] == 3
          and b["active_weight"] == 0 and b["reason"] is None, f"{code} {b}")

    # 权重 1：剩余 2
    code, a1 = acquire(key)
    check("cap acquire w1",
          code == 200 and a1["acquired"] and a1["weight"] == 1
          and a1["remaining"] == 2 and a1["active_weight"] == 1
          and a1["capacity"] == 3 and a1["credential"].endswith(":1")
          and isinstance(a1["expires_at_ms"], int), f"{code} {a1}")
    cred1 = a1["credential"]

    # 权重 2：剩余 0
    code, a2 = acquire(key, weight=2)
    check("cap acquire w2 full",
          code == 200 and a2["remaining"] == 0 and a2["active_weight"] == 3
          and a2["credential"].endswith(":2"), f"{code} {a2}")
    cred2 = a2["credential"]
    check("cap credentials distinct", cred1 != cred2, f"{cred1} {cred2}")

    # 容量已满：再申请权重 1 -> 429，不占用、带 Retry-After，剩余仍 0
    code, headers, e = acquire_h(key)
    check("cap exhausted -> 429",
          code == 429 and e["acquired"] is False
          and e["reason"] == "capacity_exhausted" and e["remaining"] == 0
          and e["active_weight"] == 3 and e["retry_after_ms"] > 0,
          f"{code} {e}")
    check("cap exhausted Retry-After header",
          headers.get("Retry-After") is not None
          and int(headers["Retry-After"]) >= 1,
          f"Retry-After={headers.get('Retry-After')}")

    # 下调容量低于当前有效占用（3）-> 409 below_active，原配置 3 保留
    code, b = set_cap(key, 2)
    check("cap shrink below active -> 409",
          code == 409 and b["configured"] is False and b["reason"] == "below_active"
          and b["capacity"] == 3 and b["active_weight"] == 3, f"{code} {b}")
    # 原配置仍为 3：依然满，申请还是 429（不是因为容量变成 2）
    code, _ = acquire(key)
    check("cap rejected shrink retains config", code == 429, f"code={code}")
    # 下调到恰好等于有效占用 3 -> 允许
    code, b = set_cap(key, 3)
    check("cap resize equal active ok",
          code == 200 and b["configured"] and b["capacity"] == 3
          and b["active_weight"] == 3, f"{code} {b}")

    # 权重本身超过容量（独立键 cap5 申请 9）-> 409 weight_exceeds_capacity（不可重试）
    big_key = f"{base}:big"
    code, _ = set_cap(big_key, 5)
    code, b = acquire(big_key, weight=9)
    check("cap weight exceeds capacity -> 409",
          code == 409 and b["reason"] == "weight_exceeds_capacity"
          and b["capacity"] == 5 and b["remaining"] == 5, f"{code} {b}")
    # 被拒后容量未被占用：权重 5 可申请
    code, b = acquire(big_key, weight=5)
    check("cap after oversized denial usable",
          code == 200 and b["remaining"] == 0, f"{code} {b}")
    code, _ = release(big_key, b["credential"])

    # 续期：错误凭据 -> 409 not_found，不动任何占用（容量仍满）
    code, b = renew(key, "0" * 32 + ":1")
    check("cap renew wrong credential -> 409",
          code == 409 and b["renewed"] is False and b["reason"] == "not_found"
          and b["credential"].endswith(":1"), f"{code} {b}")
    # 正确凭据续期 -> 200，剩余仍为 0（占用权重不变）
    code, b = renew(key, cred1)
    check("cap renew ok",
          code == 200 and b["renewed"] and b["expires_at_ms"]
          and b["credential"] == cred1 and b["remaining"] == 0, f"{code} {b}")

    # 释放权重 2 -> 剩余 2；伪造凭据释放无效且不影响真实占用
    code, b = release(key, "0" * 32 + ":2")
    check("cap release bogus ignored",
          code == 200 and b["released"] is False and b["weight"] == 0
          and b["remaining"] == 0, f"{code} {b}")
    code, b = release(key, cred2)
    check("cap release weight2",
          code == 200 and b["released"] is True and b["weight"] == 2
          and b["remaining"] == 2, f"{code} {b}")
    # 此时再用伪造凭据 -> 剩余 2（cred1 权重 1 仍占用），released=false
    code, b = release(key, "0" * 32 + ":1")
    check("cap release bogus after free ignored",
          code == 200 and b["released"] is False and b["weight"] == 0
          and b["remaining"] == 2, f"{code} {b}")
    # 释放权重 1 -> 剩余 3
    code, b = release(key, cred1)
    check("cap release weight1",
          code == 200 and b["released"] is True and b["weight"] == 1
          and b["remaining"] == 3, f"{code} {b}")
    # 重复释放同一凭据 -> 幂等 false、归还 0
    code, b = release(key, cred1)
    check("cap release idempotent",
          code == 200 and b["released"] is False and b["weight"] == 0
          and b["remaining"] == 3, f"{code} {b}")

    # 全部释放后允许把容量调小（当前有效占用为 0）
    code, b = set_cap(key, 1)
    check("cap shrink with zero active ok",
          code == 200 and b["capacity"] == 1 and b["active_weight"] == 0,
          f"{code} {b}")
    # 新容量 1 下申请权重 2 -> 409
    code, b = acquire(key, weight=2)
    check("cap weight over resized capacity -> 409",
          code == 409 and b["reason"] == "weight_exceeds_capacity"
          and b["capacity"] == 1, f"{code} {b}")

    # 短 TTL 过期：旧凭据续期 / 释放都不得影响过期后新申请的新占用
    ttl_key = f"{base}:ttl"
    code, _ = set_cap(ttl_key, 1)
    code, s = acquire(ttl_key, ttl=300)
    check("cap short ttl setup", code == 200 and s["acquired"], f"{code} {s}")
    old_cred = s["credential"]
    time.sleep(0.7)
    # 旧凭据续期 -> not_found（不能复活旧占用）
    code, b = renew(ttl_key, old_cred, ttl=5000)
    check("cap renew expired credential -> 409",
          code == 409 and b["reason"] == "not_found", f"{code} {b}")
    # 过期槽位自动回收：可立即重新申请
    code, s2 = acquire(ttl_key)
    check("cap reacquire after expiry",
          code == 200 and s2["acquired"] and s2["credential"] != old_cred
          and s2["remaining"] == 0, f"{code} {s2}")
    new_cred = s2["credential"]
    # 旧凭据释放不得删掉新占用（released=false、剩余仍 0）
    code, b = release(ttl_key, old_cred)
    check("cap stale release cannot touch new",
          code == 200 and b["released"] is False and b["weight"] == 0
          and b["remaining"] == 0, f"{code} {b}")
    # 新占用仍在：再申请 -> 429
    code, _ = acquire(ttl_key)
    check("cap new occupancy still held", code == 429, f"code={code}")
    # 旧凭据续期也不影响新占用；新凭据自身可续期、可释放
    code, _ = renew(ttl_key, old_cred)
    check("cap stale renew cannot touch new", code == 409, f"code={code}")
    code, b = renew(ttl_key, new_cred)
    check("cap new credential renew ok", code == 200 and b["renewed"], f"{code} {b}")
    code, b = release(ttl_key, new_cred)
    check("cap new credential release ok",
          code == 200 and b["released"] is True and b["weight"] == 1
          and b["remaining"] == 1, f"{code} {b}")

    # 续期延长有效期：短 TTL 占用在到期前续长，等待原 TTL 过后仍然有效
    ext_key = f"{base}:extend"
    code, _ = set_cap(ext_key, 1)
    code, s = acquire(ext_key, ttl=300)
    check("cap extend setup", code == 200, f"{code} {s}")
    ext_cred = s["credential"]
    time.sleep(0.2)
    code, b = renew(ext_key, ext_cred, ttl=5000)
    check("cap extend renew", code == 200 and b["renewed"], f"{code} {b}")
    time.sleep(0.4)  # 已超过原始 300ms，但续到 5s：占用应仍在
    code, _ = acquire(ext_key)
    check("cap renewed occupancy survives original ttl", code == 429, f"code={code}")
    code, _ = release(ext_key, ext_cred)

    # 并发绝不超配：容量 10、权重 1，40 个并发申请恰好 10 个成功
    conc_key = f"{base}:concurrent"
    code, _ = set_cap(conc_key, 10)

    def one_acquire(_):
        return call("POST", acq_path, {"resource_key": conc_key, "weight": 1})

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(one_acquire, range(40)))
    ok = [r for c, r in results if c == 200 and r["acquired"]]
    no = [r for c, r in results if c == 429 and not r["acquired"]]
    check("cap concurrent no over-admit",
          len(ok) == 10 and len(no) == 30,
          f"admitted={len(ok)} rejected={len(no)}")
    check("cap concurrent distinct credentials",
          len({r["credential"] for r in ok}) == 10, "credentials collided")
    check("cap concurrent remaining consistent",
          sorted(r["remaining"] for r in ok) == list(range(0, 10)),
          f"{sorted(r['remaining'] for r in ok)}")
    check("cap concurrent exhausted fields",
          all(r["reason"] == "capacity_exhausted" and r["remaining"] == 0
              for r in no), f"{no[:1]}")

    # 加权并发不超配：容量 10，20 个权重 3 的并发，成功数 *3 <= 10 且再放不下一笔
    w_key = f"{base}:weighted-conc"
    code, _ = set_cap(w_key, 10)

    def one_weighted(_):
        return call("POST", acq_path, {"resource_key": w_key, "weight": 3})

    with ThreadPoolExecutor(max_workers=20) as pool:
        wresults = list(pool.map(one_weighted, range(20)))
    wok = [r for c, r in wresults if c == 200]
    wno = [r for c, r in wresults if c == 429]
    used_total = len(wok) * 3
    check("cap weighted concurrent no over-admit",
          len(wok) == 3 and used_total == 9 and len(wno) == 17,
          f"admitted={len(wok)} used={used_total} rejected={len(wno)}")
    # 已用 9：权重 2 放不下（429），权重 1 恰好放下（200，剩余 0）
    code, b = acquire(w_key, weight=2)
    check("cap weighted leftover denies 2",
          code == 429 and b["remaining"] == 1, f"{code} {b}")
    code, b = acquire(w_key, weight=1)
    check("cap weighted leftover fits 1",
          code == 200 and b["remaining"] == 0, f"{code} {b}")


class _RespClient:
    """极简 Redis RESP 客户端（仅冒烟测试使用，仅依赖标准库）。

    用于构造"升级前"的数据布局（占用 ZSet 存在但缺少已占用权重汇总键），
    验证脚本内的一次性汇总重建与汇总失效行为——该布局无法通过公开 API 产生。
    """

    def __init__(self, host, port, timeout=5.0):
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._buf = b""

    def close(self):
        try:
            self._sock.close()
        except OSError:
            pass

    def execute(self, *args):
        parts = [f"*{len(args)}\r\n".encode()]
        for arg in args:
            data = str(arg).encode()
            parts.append(b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n")
        self._sock.sendall(b"".join(parts))
        return self._read_reply()

    def _read_line(self):
        while b"\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("redis connection closed")
            self._buf += chunk
        line, self._buf = self._buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self._buf) < n:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("redis connection closed")
            self._buf += chunk
        data, self._buf = self._buf[:n], self._buf[n:]
        return data

    def _read_reply(self):
        line = self._read_line()
        prefix, rest = line[:1], line[1:]
        if prefix == b"+":
            return rest.decode()
        if prefix == b"-":
            raise RuntimeError(rest.decode())
        if prefix == b":":
            return int(rest)
        if prefix == b"$":
            length = int(rest)
            if length == -1:
                return None
            return self._read_exact(length + 2)[:-2].decode()
        if prefix == b"*":
            count = int(rest)
            if count == -1:
                return None
            return [self._read_reply() for _ in range(count)]
        raise RuntimeError(f"unknown RESP reply: {line!r}")


def capacity_summary_checks():
    """已占用权重汇总：升级前数据一次性重建 / 申请释放同步增减 /
    到期清理逐笔扣减 / 集合自然过期或清空后汇总失效且不被旧计数阻挡。"""
    base = f"smoke-capsum:{int(time.time() * 1000)}"
    prefix = os.environ.get("SMOKE_KEY_PREFIX", "lease")
    host = os.environ.get("SMOKE_REDIS_HOST", "localhost")
    port = int(os.environ.get("SMOKE_REDIS_PORT", "6379"))

    def slots(rk):
        return f"{prefix}:slots:{rk}"

    def used_key(rk):
        return f"{prefix}:slots-used:{rk}"

    def redis_get(key):
        return r.execute("GET", key)

    try:
        r = _RespClient(host, port)
    except OSError as exc:
        check("capsum redis reachable", False, f"{host}:{port} {exc}")
        return

    try:
        # 1) 升级兼容：占用 ZSet 存在但缺少汇总（升级前数据布局），
        #    首次操作在脚本内一次性重建汇总，核算结果不受影响
        rk = f"{base}:legacy"
        sec, usec = r.execute("TIME")
        now_ms = int(sec) * 1000 + int(usec) // 1000
        cred_a, cred_b = "a" * 32 + ":2", "b" * 32 + ":3"
        r.execute("ZADD", slots(rk), now_ms + 60_000, cred_a)
        r.execute("ZADD", slots(rk), now_ms + 60_000, cred_b)
        r.execute("DEL", used_key(rk))
        code, b = call("PUT", f"/v1/capacity/{rk}", {"capacity": 10})
        check("capsum legacy rebuild on set",
              code == 200 and b["configured"] and b["active_weight"] == 5,
              f"{code} {b}")
        check("capsum summary rebuilt in redis",
              redis_get(used_key(rk)) == "5", f"GET={redis_get(used_key(rk))}")
        # 重建后回归增量路径：申请 +3 -> 汇总 8，剩余 2
        code, a = call("POST", "/v1/capacity/slots/acquire",
                       {"resource_key": rk, "weight": 3, "ttl_ms": 60_000})
        check("capsum acquire after rebuild",
              code == 200 and a["remaining"] == 2 and a["active_weight"] == 8,
              f"{code} {a}")
        check("capsum summary incremented",
              redis_get(used_key(rk)) == "8", f"GET={redis_get(used_key(rk))}")
        # 释放升级前遗留凭据：按该笔权重扣减汇总
        code, rel = call("POST", "/v1/capacity/slots/release",
                         {"resource_key": rk, "credential": cred_a})
        check("capsum release legacy credential",
              code == 200 and rel["released"] and rel["weight"] == 2
              and rel["remaining"] == 4, f"{code} {rel}")
        check("capsum summary decremented",
              redis_get(used_key(rk)) == "6", f"GET={redis_get(used_key(rk))}")
        # 续期升级前遗留凭据同样只认 member（权重不变，剩余不变）
        code, rn = call("POST", "/v1/capacity/slots/renew",
                        {"resource_key": rk, "credential": cred_b,
                         "ttl_ms": 60_000})
        check("capsum renew legacy credential",
              code == 200 and rn["renewed"] and rn["remaining"] == 4,
              f"{code} {rn}")
        # 全部释放后集合清空：汇总键一并失效
        call("POST", "/v1/capacity/slots/release",
             {"resource_key": rk, "credential": cred_b})
        code, rel = call("POST", "/v1/capacity/slots/release",
                         {"resource_key": rk, "credential": a["credential"]})
        check("capsum release last slot",
              code == 200 and rel["released"] and rel["remaining"] == 10,
              f"{code} {rel}")
        check("capsum summary gone when set emptied",
              r.execute("EXISTS", slots(rk)) == 0
              and r.execute("EXISTS", used_key(rk)) == 0,
              f"slots={r.execute('EXISTS', slots(rk))} "
              f"used={r.execute('EXISTS', used_key(rk))}")

        # 2) 集合自然过期后汇总同步失效：后续申请不被旧计数阻挡
        rk2 = f"{base}:expire"
        code, _ = call("PUT", f"/v1/capacity/{rk2}", {"capacity": 2})
        code, s = call("POST", "/v1/capacity/slots/acquire",
                       {"resource_key": rk2, "weight": 2, "ttl_ms": 300})
        check("capsum expire setup", code == 200 and s["acquired"], f"{code} {s}")
        check("capsum summary tracked",
              redis_get(used_key(rk2)) == "2", f"GET={redis_get(used_key(rk2))}")
        # 超过占用 TTL 与兜底 PEXPIRE：集合键与汇总键都应自然过期
        time.sleep(0.8)
        check("capsum keys expired naturally",
              r.execute("EXISTS", slots(rk2)) == 0
              and r.execute("EXISTS", used_key(rk2)) == 0,
              f"slots={r.execute('EXISTS', slots(rk2))} "
              f"used={r.execute('EXISTS', used_key(rk2))}")
        code, s2 = call("POST", "/v1/capacity/slots/acquire",
                        {"resource_key": rk2, "weight": 2, "ttl_ms": 60_000})
        check("capsum no stale count blocking",
              code == 200 and s2["acquired"] and s2["remaining"] == 0,
              f"{code} {s2}")

        # 3) 清旧逐笔扣减：一笔短 TTL 到期后，汇总只扣该笔权重
        rk3 = f"{base}:deduct"
        code, _ = call("PUT", f"/v1/capacity/{rk3}", {"capacity": 2})
        code, d1 = call("POST", "/v1/capacity/slots/acquire",
                        {"resource_key": rk3, "weight": 1, "ttl_ms": 300})
        code, d2 = call("POST", "/v1/capacity/slots/acquire",
                        {"resource_key": rk3, "weight": 1, "ttl_ms": 60_000})
        check("capsum deduct setup",
              d1.get("acquired") and d2.get("acquired"), f"{d1} {d2}")
        time.sleep(0.7)  # 第一笔到期（第二笔仍在，键不会整体过期）
        code, d3 = call("POST", "/v1/capacity/slots/acquire",
                        {"resource_key": rk3, "weight": 1, "ttl_ms": 60_000})
        check("capsum expired weight deducted",
              code == 200 and d3["acquired"] and d3["remaining"] == 0,
              f"{code} {d3}")
        check("capsum summary after deduction",
              redis_get(used_key(rk3)) == "2", f"GET={redis_get(used_key(rk3))}")
    finally:
        r.close()


if __name__ == "__main__":
    main()