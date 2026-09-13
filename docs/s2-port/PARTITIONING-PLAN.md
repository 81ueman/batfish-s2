# S2 ネットワーク分割（ノード割当・prefix sharding）設計メモ & 評価計画

- Status: draft（実装前）→ **2026-09-13 更新: P1 相当は opt-in で実装済み（後述 0.1）**
- Date: 2026-09-13（更新）
- 対象リポジトリ: `batfish-s2`（branch `master`）
- 関連: `docs/s2-port/M5-SCALE.md`, `docs/s2-port/REMAINING.md`, `nv-papers/papers/s2-2025.pdf`, `XJTU-NetVerify/s2`（参考実装）

## 0. 目的・スコープ

S2 のネットワーク分割を次の2軸で設計する。

1. **(A) ノード→worker 割当**: どの router をどの worker に置くか（graph partitioning）。
2. **(B) prefix sharding**: 経路計算を prefix 単位の shard にどう分けるか（dependency-aware partitioning）。

**主目的**: peak memory 削減 **と** scale/throughput を同等に扱う。
**対象網**: DCN (FatTree/Clos) と WAN/ISP (route reflector・border) の両方。
**METIS**: 外部バイナリ (`gpmetis` / KaHyPar) 利用可。
**本タブの成果物**: 本設計メモと評価計画。実装は別タブ（隣のメモリ実験）と競合しないよう**新規パッケージに隔離**する前提で設計する。

### 0.1 現在地（2026-09-13 更新）

本メモの「P1: shadow lazy 化」に相当する作業は、隣タブ（メモリ実験）で **opt-in で実装済み**。
前提が変わる点を先に記す。

- **config 配布**: controller が parsed config を全 worker へ配布し、worker は再 parse しない
  （`-Ds2.noShipConfigs` で従来動作を再現）。`C`（config）は依然として全 worker 複製。
- **owned-only dataplane** (`-Ds2.ownedDataplane`): worker は **owned ノードのみ** full RIB/FIB を
  構築・保持し、remote（shadow）は config 由来の **stub FIB**（connected/kernel/local/unconditional
  static）だけを持つ。`ShadowMainRibSync` の全 RIB pull と remote の full FIB 構築は行わない。
  最終 `IncrementalDataPlane` も owned ノードに限定（`IncrementalBdpEngine.dataPlaneNodes` フック）。
- 帰結: **`R_w` は partition-aware になった**（owned ノード数に比例）。よって **H1 は条件付き**:
  - owned off: node 割当は peak memory を動かさない（従来の H1）。
  - owned on: node 割当は `R_w` を動かし、**メモリのレバーになる**。
- `C` は分割では削れない（全 config 複製のまま）→ **config 記述子（別タスク）**が必要。
- 測定（`M5-SCALE.md`）: `s2-mega` 436.4 → 301.1 MiB、`s2-giga` 2226.1 → 1938.1 MiB（owned）。
  `s2-giga` owned フェーズは `building nodes` 413（= `C` 主体）、`EGP iter1` 1595、`nextDataplane`
  1740。**FIB 支配ではなく `C` と `T_w` が残る**。
- owned モードの未対応: `TrackReachability` / VXLAN / IPsec / tunnel / BGP session の dataplane
  reachability（→ REMAINING の「owned-mode hardening」）。partition 評価の testbed
  （line/ring/FatTree、aggregate）では問題にならない。
- **`T1`(config 記述子) / `T6`(factory owned スコープ) は本メモの (A)/(B) と直交**。
  詳細は `REMAINING.md`。

---

## 1. 現状インベントリ（コード確認済み）

| 対象 | 現状 | ファイル |
| --- | --- | --- |
| ノード割当 | seed 固定ハッシュで決定的シャッフル → `i % W`。トポロジ・重み・境界コスト非考慮。各 worker が独立に再計算 | `projects/s2/.../NetworkPartitioner.java`; 呼び出し `S2Main.java:307-308` |
| prefix shard 抽出 | `interface` の concrete address + `BGP originationSpace` の prefix のみ | `projects/s2/.../PrefixSharder.java#queryPrefixes` |
| prefix→shard | `Prefix.toString()` ソート → **ラウンドロビン** `i % groups` | `PrefixSharder.java#prefixSpaces` |
| shard 実行 | 全 config から各 worker が同じ shard 列を生成し、**全 worker が全 shard を逐次**実行。`drainV4Routes`/externalize | `S2BdpEngine.java:133-166`, `BgpRoutingProcess.drainV4Routes/setAppointedPrefixSpace/appointed` |
| config 配布 | **実装済み**: controller が全 parsed config を全 worker へ Java シリアライズ送信（`-Ds2.noShipConfigs` で従来動作を再現） | `S2Main.java`, `S2ControlMessages.Start` |
| RIB 複製 | 既定は **全 shadow の main RIB を全 worker に pull** して完全な forwarding analysis を作る。`-Ds2.ownedDataplane` では pull せず remote は stub FIB | `ShadowMainRibSync.java`, `S2BdpEngine`, `VirtualRouter.initStubFib` |
| 局所 forwarding | symbolic 生成は owned source のみ (`OwnedForwardingAnalysis`)、境界 edge は pull | `OwnedForwardingAnalysis.java`, `M5-SCALE.md` §Slice 5 |

### S2 論文 / 参照実装の対応物

- 参照 partitioner 5 方式: `RANDOM` / `EMPIRICAL`（名前ソート均等）/ `METIS`（`gpmetis` をプロセス起動）/ `LOAD_IMBALANCED`（3/4 を1台）/ `COMMUNICATION_HEAVY`（彩色）。既定 `METIS`。
- 参照 `PrefixPartitioner.getIpCluster`: `originationSpace` + `getAggregates()` を coverage でまとめ、**weighted LPT greedy** で shard へ。
- 論文 §4.1: 目的は (1) 負荷分散（計算・メモリ）(2) inter-worker 通信最小化。**メモリが主ボトルネックなので負荷分散を優先**。NP-hard → METIS。ノード重みは経路数の推定（FatTree k: core/agg/edge ≈ `k³/2, k³/2, k³/4`、非標準は一様）。
- 論文 §5.6: random / expert / metis の差は**わずか**。支配要因は**負荷分散**。`LOAD_IMBALANCED` は大幅悪化、通信重視 partition は random よりわずかに悪いだけ（= **通信コストは peak に効きにくい**）。
- 論文 §4.5: prefix sharding は **DPDG**（有向 prefix 依存グラフ）と **WCC + LPT greedy** で正しさと均衡を担保。全 prefix を覆う aggregate があると分割不能。

---

## 2. コストモデルと問題分解

各 worker の peak を分解する。

```
peak_w ≈ C   (config/トポロジ。現状は全 worker に複製)
       + R_w (retained RIB/FIB: real + pull した shadow)
       + T_w (control-plane transient。shard 実行中に live な RIB 量)
       + B_w (owned source の symbolic transition/BDD 表)
       + G   (GC headroom = GC タイミング由来の上振れ)
```

現状の含意:

- `C` は **worker 数に依存しない**（全 config 複製）。`R_w` も既定では worker 非依存（全 shadow RIB pull）だが、**`-Ds2.ownedDataplane` では owned ノード数に比例する**。したがって**ノード割当は owned off では `B_w` と計算量だけを変え、owned on では `R_w`（＝peak memory）も動かす**。
- **ノード割当をメモリのレバーにするには `R_w` を partition-aware にする必要がある**（`ShadowMainRibSync` の全 RIB pull を廃し、remote を boundary-only / stub 化する）。これは隣タブのメモリ実験で **opt-in 実装済み**（owned-only dataplane + remote stub FIB, `-Ds2.ownedDataplane`）。本設計の前提として明示する。
- prefix sharding は `T_w`（externalize 時は `R_w` の一部）を bound する。全 worker が同じ shard を回すため、round の peak は `max_s` で決まる。

2つの分割は、owned on では「ある prefix の伝播がどの worker の real node を通過するか」でのみ結合する。したがって**基本は分離して最適化**し、結合項は評価で確認する。

---

## 3. (A) ノード→worker 割当

### 3.1 通信グラフの構築

`G = (V, E)`:

- `V` = router（`Configuration` の hostname）。
- `E` = **union**:
  - L3 adjacency: `TopologyContext.getLayer3Topology()`
  - BGP session: `BgpTopology`（`S2Snapshot.bgpTopology`）の edge
  - IGP adjacency: `TopologyContext` の OSPF topology
- 辺重み = 推定交換量（下記）。WAN では **BGP session グラフが本質**（RR/loopback 上の multi-hop iBGP があるため、L3 link だけでは通信グラフを誤る）。DCN では L3 link と BGP が概ね一致する。

### 3.2 ノード重み（負荷推定）

simulation 前は経路数が未知。以下を段階的に:

1. **静的推定（v1, 必須）**: config から特徴量を集計。
   - interface 数（concrete address 数）
   - BGP peer 数 / address-family / origination prefix 数 / aggregate 数
   - ACL・policy の行数（forwarding/BDD コストの代理）
   - static route 数、redistribution 有無、VRF 数
   - 重み = `α·interfaces + β·(peers·origination_prefixes) + γ·acl_lines + …`（係数は P0 で単一 worker 実測から当てる）
2. **トポロジ補正（v2, 推奨）**: BGP session グラフ上で「フルテーブルを受ける」ノード（RR/border）の重みを、伝播閉包サイズで増幅。FatTree の解析式（論文 §4.1）も利用。
3. **2-pass profiling（optional）**: 1-worker（または小 worker 数）の制御プレーンだけ先に回し、実 RIB サイズを測ってから partition。精度は最高だが、最悪ケースで単一 worker に収まる必要があるため補助扱い。

### 3.3 アルゴリズム

| scheme | 実装 | 品質 | 決定性 | 位置づけ |
| --- | --- | --- | --- | --- |
| `RANDOM` | 済 | 負荷均等・境界最悪 | ◎ | baseline |
| `NAME_ORDERED` (expert) | 低 | DCN で強い | ◎ | DCN 既定候補 |
| `WEIGHTED_LPT_FM` | 中 | balance 主・cut 副に直結 | ◎ | **汎用既定候補** |
| `GREEDY_REGION` (seed k-center + 隣接最小負荷) | 中 | 局所性◎ | ◎ | WAN 候補 |
| `METIS` | 低(外部) | 高 | ◎（seed 固定） | 品質比較・本命候補 |
| `COMMUNITY` (label propagation 等) | 中 | 自然なクラスタ | ○(要 seed) | community→LPT |

- **`WEIGHTED_LPT_FM`**: 重み降順 LPT で初期割当（balance 重視）→ load cap 制約付きで FM/KL スワップし cut を削減。論文の目的関数（balance 主・cut 副）に直接対応。
- **`METIS`**: 参照実装同様に `metis.input` を書き出し `gpmetis -seed=<fixed>` を起動。ローカルに未インストール（`gpmetis not found`）なので、評価時に `brew install metis` 等で導入。KaHyPar も同枠。
- **決定性**: 乱択 scheme は controller 側で seed 固定で1回だけ計算し、**assignment を worker へ配布**する（現状の「各 worker 再計算」をやめる）。`-Ds2.partition=<scheme>` / protocol の `partition-scheme` で切替。

### 3.4 scheme 自動選択（DCN / WAN 両対応）

- DCN 判定（階層・名前規則・規則的次数）→ `NAME_ORDERED`（expert）または `METIS`。
- それ以外 / WAN → BGP session 重み付きグラフで `METIS`、外部 METIS 不可なら `WEIGHTED_LPT_FM` / `GREEDY_REGION`。
- 選択は controller が行い、worker には結果のみ配布。

---

## 4. (B) prefix sharding

### 4.1 prefix universe の closure（正しさの前提）

protocol ごとの対象 prefix を閉じる:

- BGP: `BgpProcess.getOriginationSpace()`（`network` + **redistribution**）+ `getAggregates()` の `network` + **external BGP announcements**（`S2Snapshot` は `loadExternalBgpAnnouncements` を使う）。
- redistribution closure: `A → B` があるとき A の prefix 集合を B に足す（論文 §4.5）。
- 現状の `PrefixSharder.queryPrefixes` は **aggregates / external ads / redistribution closure を落とす**。

### 4.2 DPDG（directed prefix dependency graph）

- ノード = prefix。
- edge `p1 → p2` iff **p1 の計算が p2 に依存**:
  - p1 が p2 を覆う aggregate（aggregate は寄与 more-specific の存在で activate）
  - p1 の announce が p2 の有無に依存（`summary-only`、conditional advertisement 等）
- **同じ弱連結成分（WCC）は同じ shard に必須**。

### 4.3 WCC → shard 割当

1. DPDG の WCC を列挙。
2. WCC を**重み降順**にソート。
3. `m` 個の空 shard へ、**現在最小の shard** に greedy に割当（= LPT / list scheduling）。
4. 同サイズの WCC は shuffle（特定 worker 由来 prefix による偏りを防ぐ。論文 §4.5）。
5. **退化ケース検出**: WCC が1個で全 prefix を含む（0/0 aggregate 等）→ sharding 不能。警告し、heap cap / config shipping へフォールバック。

### 4.4 shard の重み

「prefix 数」ではなく**伝播後の経路数（メモリ寄与）**で見積もる:

- 対象 prefix を受信・保持する node 数（BGP session グラフ上の到達集合）
- FIB エントリ数、symbolic predicate 数の代理
- aggregate は配下 specific を包含するので WCC 内でまとめて1単位

### 4.5 現状コードのギャップ（バグ候補）

`BgpRoutingProcess` は `appointed()` で advertise/merge をフィルタする（`BgpRoutingProcess.java:1091, 2199, 2234`）。現状の `PrefixSharder` が aggregate prefix を universe に入れないため、**aggregate を使う config では当該経路がどの round でも merge されず欠落しうる**。`networks/example/*` に `aggregate-address` 付き config が存在する。既存テスト（line/ring、aggregate 無し）では露見していない。

---

## 5. (A) × (B) の結合

- owned off（既定）: `R_w` が worker 非依存なので、node 割当は throughput と `B_w` のレバー、prefix shard は `T_w` のレバー。独立に評価可能。
- owned on (`-Ds2.ownedDataplane`): ある shard の live 経路は、その経路が通過する real node を持つ worker に載る。node 割当が per-shard per-worker 負荷を左右する。この段階で `(A)×(B)` の結合最適化（例: shard の伝播閉包が特定 worker に偏らないよう node 割当を補正）を検討。

---

## 6. 評価計画

### 6.1 指標

- **max per-worker peak heap**（主）
- wall time（control plane / data plane / 合計）
- boundary RPC 数・bytes（通信コスト）
- per-round live BGP RIB 量
- partition 品質: imbalance `max/mean`（ノード重み）、weighted cut 比
- 正しさ: ribs / reachability / symbolic / answer の `MATCH`（既存 `scripts/compare-answers.sh`）

### 6.2 testbed

- **FatTree/Clos 生成器**（k 可変、参照実装の `generateFatTreeTopology` を Java 移植）+ 非対称網（border/RR がフルテーブル保持）
- 既存 `networks/s2-line`, `s2-ospf`, `s2-ospf-bgp`, `s2-redist`, `s2-big-bgp`, `s2-big2`, `s2-huge`, `s2-mega`, `s2-giga`（**`s2-ring` は現状未作成** → 生成器または追加で用意）
- **aggregate / redistribution / external announcements 入り** snapshot（新規。DPDG の検証用）
- worker 数: 1, 3, 6, 8, 16

### 6.3 仮説

- **H1（更新）**: owned off では node 割当は peak memory を動かさない。**owned on (`-Ds2.ownedDataplane`) では `R_w` が owned ノード数に比例するため、node 割当が peak memory を動かす**。→ 評価は owned on/off 両方で行う。
- **H2**: WCC-LPT は round-robin より max per-worker peak を下げる（aggregate/依存がある場合）。
- **H3**: 通信 cut は peak に効きにくい（論文 §5.6 の再確認）。ただし throughput には効きうる。
- **H4**: `LOAD_IMBALANCED` は大幅悪化、`NAME_ORDERED` は DCN で `METIS` に近い（論文の再現）。
- **H5**: DCN では `NAME_ORDERED`/`METIS`、WAN では BGP session 重み付き `METIS` が最良。

### 6.4 手順

1. P0 で指標ダンプと生成器を用意し、現状（random, seed 0）を baseline 化。
2. section 6.2 の各 testbed × scheme を sweep。`RANDOM`/`NAME_ORDERED`/`WEIGHTED_LPT_FM`/`METIS` を比較。
3. prefix sharding は `1 / 8 / 20` shard、`queryPrefixes`（現状）と DPDG-LPT（新）を比較。
4. 正しさは常に `compare-answers.sh` で `MATCH` を確認。aggregate snapshot では現状実装の不一致（欠落）を再現→修正で解消を確認。
5. P0 完了後、owned on/off 両方で 1–4 を測定し H1 を検証（P1 相当は実装済み）。

### 6.5 判断基準（暫定）

- 正しさは絶対条件（`MATCH`）。
- peak memory: 最良 scheme が random 比で有意（testbed 間で一貫）に低いこと。
- throughput: balance 改善が wall time に反映されること（通信 cut 単独では効かない想定）。
- DCN/WAN で自動選択が破綻しないこと。

---

## 7. マイルストーン

- **P0 計測基盤**: トポロジ/config 生成器、指標ダンプ、現状 baseline 取得。
- **P1 shadow lazy 化・boundary-only 化**: **概ね実装済み（opt-in）** = `-Ds2.ownedDataplane`（config 由来 stub FIB、`IncrementalBdpEngine.dataPlaneNodes` で最終 dataplane を owned 限定）。残 caveat は REMAINING の owned-mode hardening（Track/VXLAN/tunnel/BGP reachability）。
- **P2 partitioner プラグイン化**: 新パッケージ（例 `.../ibdp/partition/`）に schemes を実装。controller が assignment を算出・配布。`S2Main` 変更は呼び出し1箇所 + scheme plumbing に限定。
- **P3 `PrefixDependencyGraph`**: closure + DPDG + weighted WCC-LPT。aggregate/redistribution/external-ads テスト追加。
- **P4 評価 → 既定 scheme 決定 → `M5-SCALE.md` / `README.md` 更新**。

---

## 8. リスクと隣タブとの調整

- **競合**: `S2Main.java` / `S2BdpEngine.java` / `PrefixSharder.java` は隣タブのメモリ実験と重なる。partition 系は新パッケージ（`.../ibdp/partition/`）に隔離し、既存ファイルの変更を最小化する。P1 相当は実装済みなので、`S2Main` の変更は「assignment を controller で算出して配布」の1箇所 + scheme plumbing に限定し、決定性変更で `S2ControlMessages` に触れる。
- **決定性**: 乱択 scheme は controller 計算 + seed 固定で配布。worker 再計算をやめる。
- **METIS 依存**: `gpmetis` 未インストール。評価環境への導入が必要。無い場合は純 Java scheme にフォールバック。
- **BGP 多重固定点**: cyclic equal-cost 網の tie-break 非決定（`M5-SCALE.md` Known residual）と partition の影響を混同しない。partition 評価は tie が安定な testbed で行う。
- **推定精度**: ノード/prefix 重みの推定が外れると scheme 比較が無意味化。P0 で係数を実測に合わせる。
- **owned モードの caveat**: owned は opt-in で、Track/VXLAN/IPsec/tunnel/BGP reachability 未対応。partition 評価は対応済み網で行い、既定化は caveat 解消後（REMAINING の owned-mode hardening）。
- **prefix closure（正しさ）**: `PrefixSharder.queryPrefixes` が aggregate / redistribution / external ads を落とすため、aggregate 入り網では (B) が経路欠落しうる。DPDG 着手前に修正 + aggregate snapshot で `MATCH` を確認する（section 4.5）。

---

## 9. 未決事項

1. P1 相当は実装済み。残るのは owned-mode hardening の caveat（Track/VXLAN/tunnel/BGP reachability）を誰がいつ埋めるか。
2. ノード重み推定の係数をどの testbed で校准するか。
3. METIS を評価環境に常設するか（Docker image に入れるか）。
4. prefix shard 数を実行時にどう決めるか（メモリ予算から自動決定 or 固定 sweep）。
5. DCN 判定ヒューリスティクスの設計（名前規則に依存しすぎないか）。
