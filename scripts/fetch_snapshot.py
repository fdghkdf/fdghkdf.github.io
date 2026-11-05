#!/usr/bin/env python3
"""定时抓取 DEX 监控看板数据，产出同站 data.json（仅标准库，无密钥）。

为什么需要这份快照：
- 网页版在浏览器里直连 6 家接口。部分用户网络（或 loris.tools 的跨域
  限制）会把 6 家全部拦住，页面就全红、看不到任何行情。
- GitHub Pages 是纯静态站，没有自己的服务器。解决办法是让 GitHub
  Actions（在能直连这些接口的跑手上）每 15 分钟抓一次，写成仓库里
  的 data.json；页面直连失败时改读自己站内的 data.json，就不用再
  直连那 6 家。快照不是实时行情，页面必须明标抓取时间与延迟。

资金费率口径（与网页版、App 版完全一致，绝不回退已结算口径）：
- Lighter：WebSocket market_stats 的 current_funding_rate（%/h，实时），
  不是 /api/v1/fundings 的已结算费率。
- Arcus：/v1/markets 的 nextFundingRate（下期预测，与前端 1h Funding 一致）。
- Nado：24h 小数 / 24。
- Variational：年化小数 / 8760。
- Ondo Perps：loris.tools 公开页 8h bps，%/h = bps / 800（第三方聚合，只读）。

用法：python3 scripts/fetch_snapshot.py [输出路径，默认仓库根目录 data.json]
任一家失败只影响该家，不会让整份快照作废；全部失败则不覆盖旧快照。
"""
import base64
import json
import os
import re
import socket
import ssl
import struct
import sys
import time
import urllib.parse
import urllib.request

SYMBOLS = ["BTC", "ETH", "SOL", "NVDA", "AAPL", "MSFT", "GOOGL", "AMZN",
           "META", "TSLA", "QQQ", "SPY"]
CMDTY = ["XAU", "XAUT", "XAG", "SLV"]
PAIRS = SYMBOLS + ["USO", "SLV"]
ALL = SYMBOLS + CMDTY + ["USO", "WTI"]

CG_IDS = {
    "NVDA": "nvidia-ondo-tokenized-stock",
    "AAPL": "apple-ondo-tokenized-stock",
    "TSLA": "tesla-ondo-tokenized-stock",
    "MSFT": "microsoft-ondo-tokenized-stock",
    "GOOGL": "alphabet-class-a-ondo-tokenized-stock",
    "AMZN": "amazon-ondo-tokenized-stock",
    "META": "meta-platforms-ondo-tokenized-stock",
    "SPY": "spdr-s-p-500-etf-ondo-tokenized-etf",
    "QQQ": "invesco-qqq-etf-ondo-tokenized-etf",
}


try:
    import requests as _requests
except Exception:
    _requests = None


def _fetch(url, timeout, headers):
    # 优先用 requests（代理/CONNECT 处理更稳，本地实测 urllib 在 Lighter
    # 上会 SSL 握手失败而 requests 正常）；跑手上没 requests 时回退 urllib。
    last = None
    for attempt in range(3):
        try:
            if _requests is not None:
                r = _requests.get(url, timeout=timeout, headers=headers)
                r.raise_for_status()
                return r.text
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "ignore")
        except Exception as e:
            last = e
            time.sleep(1 + attempt)
    raise last


def get_json(url, timeout=20):
    return json.loads(_fetch(url, timeout, {"User-Agent": "dex-snapshot/1.0"}))


def get_text(url, timeout=25):
    return _fetch(url, timeout,
                  {"User-Agent": "Mozilla/5.0 (compatible; dex-snapshot/1.0)"})


def lighter():
    """返回 (prices, funding_pct_h, taker_pct, ask, bid, ids)。"""
    d = get_json("https://api.rh.lighter.xyz/api/v1/orderBookDetails")
    prices, taker, ids = {}, {}, {}
    for ob in d.get("order_book_details", []):
        sym = ob.get("symbol")
        if sym not in ALL or ob.get("status") != "active":
            continue
        try:
            prices[sym] = float(ob["mark_price"])
        except (KeyError, TypeError, ValueError):
            continue
        try:
            taker[sym] = float(ob["taker_fee"]) * 100
        except (KeyError, TypeError, ValueError):
            pass
        try:
            ids[sym] = int(ob["market_id"])
        except (KeyError, TypeError, ValueError):
            pass
    ask, bid = {}, {}
    for sym in PAIRS:
        if sym not in ids:
            continue
        try:
            ob = get_json(
                "https://api.rh.lighter.xyz/api/v1/orderBookOrders"
                f"?market_id={ids[sym]}&limit=1", timeout=15)
            if ob.get("asks"):
                ask[sym] = float(ob["asks"][0]["price"])
            if ob.get("bids"):
                bid[sym] = float(ob["bids"][0]["price"])
        except Exception as e:
            print(f"[lighter] book {sym} failed: {e}", file=sys.stderr)
    funding = lighter_ws_funding(ids)
    return prices, funding, taker, ask, bid


def _tcp_via_proxy(host, port, timeout):
    """有 https_proxy 时先走 HTTP CONNECT 隧道，否则直连。
    （本机裸 socket 不走代理会被出口拦截，跑手上无代理则直连。）"""
    proxy = urllib.request.getproxies().get("https") or \
        urllib.request.getproxies().get("http")
    if not proxy:
        return socket.create_connection((host, port), timeout=timeout)
    p = urllib.parse.urlparse(proxy if "//" in proxy else "//" + proxy)
    raw = socket.create_connection((p.hostname, p.port or 8080), timeout=timeout)
    lines = [f"CONNECT {host}:{port} HTTP/1.1", f"Host: {host}:{port}"]
    if p.username:
        cred = base64.b64encode(
            f"{urllib.parse.unquote(p.username)}:"
            f"{urllib.parse.unquote(p.password or '')}".encode()).decode()
        lines.append(f"Proxy-Authorization: Basic {cred}")
    raw.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
    resp = bytearray()
    while b"\r\n\r\n" not in resp:
        chunk = raw.recv(1)
        if not chunk:
            raise ConnectionError("proxy CONNECT closed")
        resp.extend(chunk)
    if " 200" not in bytes(resp).split(b"\r\n")[0].decode("latin1"):
        raise ConnectionError("proxy CONNECT failed")
    return raw


def lighter_ws_funding(ids, timeout=15):
    """Lighter WS market_stats current_funding_rate，返回 {sym: %/h}。"""
    host = "api.rh.lighter.xyz"
    key = base64.b64encode(os.urandom(16)).decode()
    ctx = ssl.create_default_context()
    raw = _tcp_via_proxy(host, 443, timeout)
    sock = ctx.wrap_socket(raw, server_hostname=host)
    buf = bytearray()

    def read_more():
        chunk = sock.recv(65536)
        if not chunk:
            raise ConnectionError("Lighter WS closed")
        buf.extend(chunk)

    def read_exact(n):
        while len(buf) < n:
            read_more()
        out = bytes(buf[:n])
        del buf[:n]
        return out

    def send_frame(opcode, payload=b""):
        mask = os.urandom(4)
        header = bytearray([0x80 | opcode])
        ln = len(payload)
        if ln < 126:
            header.append(0x80 | ln)
        elif ln < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", ln)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", ln)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        sock.sendall(bytes(header) + mask + masked)

    try:
        sock.sendall(
            ("GET /stream?readonly=true HTTP/1.1\r\n"
             f"Host: {host}\r\n"
             "Upgrade: websocket\r\n"
             "Connection: Upgrade\r\n"
             f"Sec-WebSocket-Key: {key}\r\n"
             "Sec-WebSocket-Version: 13\r\n\r\n").encode())
        while b"\r\n\r\n" not in buf:
            read_more()
        head, _, rest = bytes(buf).partition(b"\r\n\r\n")
        if "101" not in head.split(b"\r\n")[0].decode("latin1"):
            raise ConnectionError("Lighter WS handshake failed")
        buf.clear()
        buf.extend(rest)
        for mid in ids.values():
            send_frame(1, json.dumps(
                {"type": "subscribe", "channel": f"market_stats/{mid}"}).encode())
        id_to_sym = {mid: sym for sym, mid in ids.items()}
        out = {}
        deadline = time.time() + timeout
        sock.settimeout(3)
        while len(out) < len(ids) and time.time() < deadline:
            try:
                b1, b2 = read_exact(2)
            except (socket.timeout, TimeoutError):
                continue
            opcode = b1 & 0x0F
            ln = b2 & 0x7F
            if ln == 126:
                ln = struct.unpack(">H", read_exact(2))[0]
            elif ln == 127:
                ln = struct.unpack(">Q", read_exact(8))[0]
            payload = read_exact(ln) if ln else b""
            if opcode == 9:
                send_frame(10, payload)
                continue
            if opcode not in (1, 2):
                continue
            try:
                msg = json.loads(payload)
            except Exception:
                continue
            ms = msg.get("market_stats") or {}
            sym = id_to_sym.get(ms.get("market_id"))
            cfr = ms.get("current_funding_rate")
            if sym and cfr is not None:
                out[sym] = float(cfr)  # 本身就是 %/h
        return out
    finally:
        try:
            sock.close()
        except Exception:
            pass


def arcus():
    d = get_json("https://api.arcus.xyz/v1/markets")
    prices, funding, taker, next_at = {}, {}, {}, 0
    for m in d.get("markets", []):
        sym = str(m.get("marketDisplayName") or "").split("-")[0]
        if sym not in ALL or m.get("status") != "ONLINE":
            continue
        try:
            prices[sym] = float(m["markPrice"])
        except (KeyError, TypeError, ValueError):
            continue
        fr = m.get("nextFundingRate")
        if fr is None:
            fr = m.get("fundingRate")
        try:
            funding[sym] = float(fr) * 100
        except (TypeError, ValueError):
            pass
        ppm = m.get("effectiveTakerFeePpm")
        if ppm is None:
            ppm = m.get("baseTakerFeePpm")
        try:
            taker[sym] = float(ppm) / 10000
        except (TypeError, ValueError):
            pass
        try:
            na = float(m.get("nextFundingAt") or 0)
            if na > 0 and not next_at:
                next_at = int(na * 1000)
        except (TypeError, ValueError):
            pass
    return prices, funding, taker, next_at


def nado():
    d = get_json("https://api.prod.nado.xyz/archive/v2/contracts")
    prices, funding = {}, {}
    for ticker, c in d.items():
        if not ticker.endswith("-PERP_USDT0"):
            continue
        sym = ticker[:-len("-PERP_USDT0")]
        if sym not in ALL:
            continue
        try:
            prices[sym] = float(c["mark_price"])
        except (KeyError, TypeError, ValueError):
            continue
        try:
            funding[sym] = float(c["funding_rate"]) / 24 * 100
        except (KeyError, TypeError, ValueError):
            pass
    return prices, funding


def variational():
    d = get_json("https://omni-client-api.prod.ap-northeast-1.variational.io"
                 "/metadata/v2/stats")
    prices, funding = {}, {}
    for m in d.get("listings", []):
        t = str(m.get("ticker") or "")
        if not t.endswith("-PERP"):
            continue
        sym = t[:-5]
        if sym not in ALL:
            continue
        try:
            prices[sym] = float(m["mark_price"])
        except (KeyError, TypeError, ValueError):
            continue
        try:
            funding[sym] = float(m["funding"]["last"]["annualized_rate"]) / 8760 * 100
        except (KeyError, TypeError, ValueError):
            pass
    return prices, funding


def ondo_spot():
    ids = ",".join(CG_IDS.values())
    d = get_json("https://api.coingecko.com/api/v3/simple/price"
                 f"?ids={ids}&vs_currencies=usd&precision=4")
    out = {}
    for sym, cg_id in CG_IDS.items():
        e = d.get(cg_id) or {}
        try:
            out[sym] = float(e["usd"])
        except (KeyError, TypeError, ValueError):
            pass
    return out


def ondo_perps():
    html = get_text("https://loris.tools/funding/exchange/ondo")
    out = {}
    for m in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S):
        tr = m.group(1)
        sm = re.search(r'uppercase[^"]*">\s*([A-Za-z0-9._-]+)\s*<', tr)
        if not sm:
            continue
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
        if len(cells) < 3:
            continue
        txt = re.sub(r"<[^>]+>", " ", cells[2])
        try:
            bps = float(txt.strip())
        except ValueError:
            continue
        out[sm.group(1).upper()] = bps / 800  # bps/8h -> %/h
    if not out:
        raise RuntimeError("loris: no rows parsed")
    return out


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else "data.json"
    fetched_ms = int(time.time() * 1000)
    snapshot = {
        "fetched_at_ms": fetched_ms,
        "fetched_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(fetched_ms / 1000)),
        "prices": {}, "funding_pct_h": {}, "taker_pct": {},
        "lighter_ask": {}, "lighter_bid": {}, "arcus_next_at_ms": 0,
        "health": {},
    }
    jobs = [
        ("lighter", lighter), ("arcus", arcus), ("nado", nado),
        ("variational", variational), ("ondo", ondo_spot),
        ("ondo_perps", ondo_perps),
    ]
    for name, fn in jobs:
        try:
            r = fn()
            if name == "lighter":
                prices, funding, taker, ask, bid = r
                snapshot["prices"]["lighter"] = prices
                snapshot["funding_pct_h"]["lighter"] = funding
                snapshot["taker_pct"]["lighter"] = taker
                snapshot["lighter_ask"], snapshot["lighter_bid"] = ask, bid
                snapshot["health"]["lighter"] = bool(prices)
            elif name == "arcus":
                prices, funding, taker, next_at = r
                snapshot["prices"]["arcus"] = prices
                snapshot["funding_pct_h"]["arcus"] = funding
                snapshot["taker_pct"]["arcus"] = taker
                snapshot["arcus_next_at_ms"] = next_at
                snapshot["health"]["arcus"] = bool(prices)
            elif name in ("nado", "variational"):
                prices, funding = r
                snapshot["prices"][name] = prices
                snapshot["funding_pct_h"][name] = funding
                snapshot["health"][name] = bool(prices)
            elif name == "ondo":
                snapshot["prices"]["ondo"] = r
                snapshot["health"]["ondo"] = bool(r)
            elif name == "ondo_perps":
                snapshot["funding_pct_h"]["ondo_perps"] = r
                snapshot["health"]["ondo_perps"] = bool(r)
            print(f"[{name}] ok")
        except Exception as e:
            snapshot["health"][name] = False
            print(f"[{name}] FAILED: {e}", file=sys.stderr)
    if not any(snapshot["health"].values()):
        print("all venues failed, not overwriting existing snapshot",
              file=sys.stderr)
        return 2
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=1, sort_keys=True)
    print(f"wrote {out_path}: " +
          ", ".join(f"{k}={'ok' if v else 'fail'}"
                    for k, v in snapshot["health"].items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
