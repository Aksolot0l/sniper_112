import json, csv
from datetime import datetime, timezone
OC = {
 "2KEGFqfodScm7sBQB4Nwr811WfwrZ4H3SVrDHe24pump": ("基犀牛","10:29:42","CFm9TSJULoUYTJTb5MUU2LFikPjqn3xzrCM9DPTeit6k",9),
 "43TQJqAeWw81x17UTgwadttseEv2xgMv7RK9pBq7pump": ("CI","10:24:13","2SssDAZBovyY7bnTYH2wFLDL2tpAvQBpAcEywUfbE8GN",4467),
 "4Fzuv6MDq5y3ECc2WmUCgGY4DjnmGYCt2kerrCobpump": ("QBUNKER","10:30:08","8kTDB1eKpGQjikVqbqVGZDJd1nzRrgDppHzz5hX2zdKe",2),
 "5nPTzBwfgHrVH19BfuwBXQGv6tN57Tvj2cufRhDLpump": ("DJT","10:35:40","F6Y6hKGXPivgoaWdsfbiAJA81MyDbkq4sLngfWkUm27h",484),
 "5xxorc9CAnHzBHTjMpDEK6wddqTN5M523FmKNwLFpump": ("SEXSI","10:25:55","5tRHiVkuqcaaYKYpvEaeE9ykxF8zs7ky8sUyeWJCmz7t",6),
 "62QkaGw9p7Uq5FR1jQfEWwRYFVTggYsupZ1L9dr1pump": ("LAUGHINGCOW","10:44:13","9rDMVCH7mQ9N2PkyHw8KT8wraMhF8tyMz9R631yyL1df",4084),
 "6ybcrVjDJBdp6kFuQkaDdDrSKFTQoov4iSFpmmQjpump": ("黑天鹅","10:28:08","CFm9TSJULoUYTJTb5MUU2LFikPjqn3xzrCM9DPTeit6k",10),
 "75w2FTG3Lh1eyxEqevf6iHzZ7R3CGrzxfvvAohafpump": ("MONEKO","10:23:12","CjNHcTyWYE1q1zX5GgpoPw53Lw5D1f4yUUmqNzd4sVxG",1069),
 "7BYPTmkKoqkh9qPPGSxsMgP7KXxXm14FYpAtPJifpump": ("lysis","10:27:58","4Q7UKRfCkzRtgAKjxAidM76arvg9ZYkY7WVe69rSRBm1",1),
 "7mEnEUX52CfbsYkJZvYjJWUyexGtWbBWaZeQkuz5pump": ("犀牛","10:28:08","46qTwecWoxYdNotxKtZ71wbRVJnZCEaJk4JQtU8iwc2P",910),
 "8CxQj1xdtu7BQ88TJcYbSgcxJS1aH6RqjqDgZ5z7pump": ("$PONK","10:32:39","H4qxjBBDUi7RiBjcwq531Uxnu2pNo7fSGo2mxLeLznSJ",1),
 "8US7rBZPAZATL1qN4n5iSRtN54DRSyZua3j8b8izpump": ("灰犀牛","10:30:36","3fDJoYeTDn4JKjjr2yYkYNF4RGGVpAHR3R5gkgws3td4",111),
 "94wZeHvs2tfunewUxFUMETUhZow24t1aLjK6vBRupump": ("JayG","10:41:51","5h132ftz4pWjqbTfpBDHRkEJV4rJMt9SFQRsk2YNmtt5",3),
 "994mKg2XgkitAdDxyxXwRJ2bMTuVrqtCvcntR22Eorcx": ("ALPK","10:25:01","xxrr85qsW5No1cvn9vk4kzTiz2y1t7jMN8pf3Xo47f7",449),
 "9HMokL8Vc5hYVkzFJFPeHwc7Pp4xmVHMxXpcPzcfndac": ("Carbon-X","10:32:19","6um7zjnT2xfgMuF5R11aC6L4bt5U8CMoM5YV5hgj29zD",41),
 "9JRUtQvuuKkkV6X6qigG6iXDnxedF6cSk2sT7G7ypump": ("CHONKS","10:30:24","2SssDAZBovyY7bnTYH2wFLDL2tpAvQBpAcEywUfbE8GN",4467),
 "9njhbu48dTP66zM85Q9P7sEKgNGVf2pnAyKr6EHxpump": ("周四","10:30:24","2sRsJHLjLVMSBtfTU4wCkewTzBfLx5bLgcU6XJQRwdR8",135),
 "B1riMsawhhHWxaA48Rr53D65cjpJa6znhtmSuifXpump": ("IIB","10:24:36","3bJC1TBFbKoUBGQbhq6ETHvtzR9HUiBy3y1B9tvSQFe6",9),
 "BWin2CLyDT6MeShxz8FimMbAjWkeFw48Bp3BLXCrpump": ("NoahAi","10:47:06","Fyi4Ay1XLTuJH4UsKPhhb62DinDtRGBKzLVmMJUD19cX",1),
 "BcZHZ2XLwY84EcAqL7XM2GA479eo5HUDPLY4yGXDpump": ("Larp","10:43:42","JC7BYVeBGU8iYwN6eQcuDr85BmtwgHUQPzZkP4r7gDH9",2260),
 "C2ZbmmihRvVUz1Frp4i5z1AuFb8B4rWWv3CwwGHqpump": ("Mimi Coin","10:21:45","DMT3DnnW4apdgYBZN9mfPhRL7nzFwHfC3bnmZMDK1JxQ",1869),
 "CJD4xuw8QdHXBZMk5HX5W3Aja9DCf8KFhW2tonq1pump": ("PHPUPS","10:42:15","6Ur4kkWQJEvXpbKDjYM98UZu1sgzvxaQPD35UQtFqZWz",383),
 "CfQPKqFkKwFwJWiWkxGA914LKrSBqhcLNsksnunwpump": ("PHP","10:44:40","6Ur4kkWQJEvXpbKDjYM98UZu1sgzvxaQPD35Q",383),
 "DgvjeTBvXtsdVXVksaZtiX7YgocSu6MzrxwQ3Fndpump": ("Scotch","10:38:49","CFm9TSJULoUYTJTb5MUU2LFikPjqn3xzrCM9DPTeit6k",10),
 "FFN1MzCpyiRcUz3c6ZdgjDmgWnzRx4AA9x7bSyJgpump": ("Моисей","10:47:46","6wjvrViRddVdcUXAJLG3EFzmZSYkgzQ9nB7jj9eCTt49",77),
 "FQqVKrarwR9i3eG3zKknk2XksukWyDYwR3KrHM7apump": ("BERRY","10:40:49","4o4PumZMX5nQgAfPmhmFrDkFnvAtQRmPUTTy5ME9s5cN",1),
 "G7yvKEG1MgxztB1HtdqwQPxpbNMFSvNgGSqjz37npump": ("灰犀牛(Grey)","10:28:40","CFm9TSJULoUYTJTb5MUU2LFikPjqn3xzrCM9DPTeit6k",9),
 "GfMP71nSVPu6AZ6B2hkxMPPmcaSVr5B3NWyUy7rVpump": ("POKEGRAIL","10:38:25","DjYs2TY7MAVStVbmzbiBtnoDoU9mjf7iXVwk8wdRjV7D",1),
}
trades=json.load(open('/tmp/trades.json'))
DAY="2026-10-22T"
def ep(s): return datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()
print(f"{'arm':3} {'sym':12} {'mint_utc':9} {'first_seen':9} {'open':9} {'MINT→seen':>9} {'seen→open':>9} {'OPEN→CLOSE':>10} {'MINT→OPEN':>9} {'pnl':>8}")
rows=[]
for t in trades:
    oc=OC.get(t['mint'])
    if not oc: print('MISSING',t['mint']); continue
    sym,hhmmss,dev,dm=oc
    mint_t=ep(DAY+hhmmss+"+00:00")
    fs=t['first_seen']; op=t['open_']; cl=t['close']
    rows.append(dict(arm=t['arm'],sym=sym,dev=dev,devMints=dm,mint=mint_t,fs=fs,op=op,cl=cl,pnl=t['pnl'],pct=t['pct'],reason=t['reason'],held=t['held']))
    print(f"{t['arm']:3} {sym[:12]:12} {hhmmss:9} {datetime.fromtimestamp(fs,timezone.utc).strftime('%H:%M:%S'):9} {datetime.fromtimestamp(op,timezone.utc).strftime('%H:%M:%S'):9} {fs-mint_t:9.1f} {op-fs:9.2f} {cl-op:10.2f} {op-mint_t:9.1f} {t['pnl']:8.2f}")
json.dump(rows,open('/tmp/onchain_rows.json','w'))
import statistics
for arm in 'AB':
    rr=[r for r in rows if r['arm']==arm]
    print(f"\n{arm}: n={len(rr)} MINT→OPEN med={statistics.median([r['op']-r['mint'] for r in rr]):.1f}s min={min(r['op']-r['mint'] for r in rr):.1f} max={max(r['op']-r['mint'] for r in rr):.1f}")
    print(f"   MINT→first_seen med={statistics.median([r['fs']-r['mint'] for r in rr]):.1f}s  seen→open med={statistics.median([r['op']-r['fs'] for r in rr]):.2f}s")
