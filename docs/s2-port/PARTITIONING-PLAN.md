# S2 ネットワーク分割（ノード割当・prefix sharding）設計メモ & 評価計画

- Status: draft（実装前）→ **2026-09-13 更新: P1 相当は opt-in で実装済み（後述 0.1）。P2 は完了し、`gpmetis` 導入後の実測を §6.7 に記録（METIS = 品質参照、既定は RANDOM）。O6 ノード重み校准（§6.8）と v2 トポロジ補正（§6.9, opt-in）を実装・測定。§3.4 の `AUTO`（DCN/WAN 自動選択）を実装し runner 既定を `auto` に変更（コード既定は RANDOM のまま）。O6 residual の role-level peer scaling は opt-in で正の結果（§6.10）だが、k=6 FatTree と WAN star を加えた追試で FatTree k=6 に退行があり **gate 維持**（§6.12）**
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
2. **トポロジ補正（v2, 実装済み・opt-in）**: BGP session グラフ上で各ノードの**伝播閉包**を求める。
   具体的には、同一 BGP 連結成分で originate される prefix 数 `fullTableRoutes(v)`（= そのノードが
   受信・保持するフルテーブルの推定経路数）を計算し、`weight(v) = base(v) +
   V2_FULL_TABLE_WEIGHT · fullTableRoutes(v)` とする。FatTree の全ノードは同一 BGP 成分なので補正項は
   ネットワーク共通の定数になり、config 特徴だけの core:edge 比（peers 項により k=4 で 1.5）を実測比
   （~1.10）側へ圧縮する（K=2 で 1.071、K=2.4 でちょうど 1.10）。閉包がノードごとに異なる場合
   （多重 BGP 成分、部分テーブルしか受けないノード）は閉包の大きいノードを比例的に重くする。
   `-Ds2.nodeWeightsV2=true` で有効（既定 off、デモ不変）。係数は `NodeWeights.V2_FULL_TABLE_WEIGHT`。
   **注意**: この補正は重みの*比*を直すが、現 testbed では閉包が成分定数なので重みの*差*を変えず、
   `WEIGHTED_LPT_FM` の assignment とコスト考慮 imbalance は不変だった。実測と限界は §6.9。
   論文 §4.1 の FatTree 解析式も同節で検討する。
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
- **`METIS`**: 参照実装同様に `metis.input` を書き出し `gpmetis -seed=<fixed> -ptype=rb -ufactor=1` を起動。**2026-09-13: `gpmetis` (METIS 5.1.0) を評価環境に導入**し実測（§6.7）。未導入環境では `WEIGHTED_LPT_FM` にフォールバックする。KaHyPar も同枠。
- **決定性**: 乱択 scheme は controller 側で seed 固定で1回だけ計算し、**assignment を worker へ配布**する（現状の「各 worker 再計算」をやめる）。`-Ds2.partition=<scheme>` / protocol の `partition-scheme` で切替。

### 3.4 scheme 自動選択（DCN / WAN 両対応）

**実装済み**: `-Ds2.partition=auto`（別名 `PartitionScheme.AUTO`、`-Ds2.partition=AUTO`）。選択は
controller が union 通信グラフを構築した後、`AutoSchemeSelector.select(requested, graph)` で
**1 回だけ**行い、結果の concrete scheme で assignment を計算して worker へ配布する
（worker は再選択しない）。選択は決定的で、選択理由を controller ログに出す。

**分類ヒューリスティクス**（`AutoSchemeSelector.classify`, グラフのみ使用）:

1. **階層名**: hostname の 50% 以上が DCN tier token（`core` / `agg` / `aggr` / `spine` /
   `leaf` / `tor` / `pod` / `fabric` / `tier` / `edge`）を小文字単語として含む → DCN
   （例: `monitor` は `tor` に一致しない）。
2. **BGP overlay**: union 辺の 25% 以上が **BGP のみ**（下に L3/OSPF 隣接がない BGP session）→
   WAN/ISP。multi-hop iBGP / route reflector で session グラフが IGP より密な場合（§3.1）。
3. **規則的次数**: 次数分布が 3 種以下・平均次数 2 以上・次数 ≥3 のノードが 25% 以上（かつ 3
   ノード以上）・次数が 2 種以上 → DCN。FatTree k=4 は degree-4 が 60%。line/ring/小木は
   スパースなので WAN 側に落ちる。
4. それ以外 → WAN。

**scheme 選択**: `gpmetis` があれば**常に `METIS`**（不在なら `MetisPartitioner` が
`WEIGHTED_LPT_FM` にフォールバック）。`gpmetis` がない場合のみ分類が効き、DCN →
`NAME_ORDERED`、WAN → `WEIGHTED_LPT_FM`。

**既定**: コード既定は `RANDOM` のまま（stock 不変）。S2 **runner** が `auto` を既定にする:
`scripts/local-demo.sh` が `JAVA_TOOL_OPTIONS` の先頭に `-Ds2.partition=auto` を付け、k8s の
worker/controller manifest も `JAVA_TOOL_OPTIONS` に同 `-D` を持つ。ユーザの
`-Ds2.partition=<scheme>` を後ろに置けば上書きできる（§2 の
`s2.prefixSpacePositiveCacheOnly` と同じパターン）。

**限界**（クラス Javadoc にも記載）: 分類は粗く、正しさには無関係（どの scheme でも assignment は
valid）。tier 名のない不規則 DCN、規則的な密 WAN、tier token の偶発一致には誤分類しうる。
bisection 推定や clustering による改良は将来課題。選択した shape と理由はログに出るので自己記述的。

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

**修正済み（C-PFX, 2026-09-13）**: `PrefixSharder` が aggregate ネットワーク・unconditional network statement を universe に加え、**aggregate と被覆 more-specific を同一 shard に束ねる**（union-find + グループ単位 LPT）。

- 再現: `networks/s2-agg`（受信経路を aggregate する 3 ノード eBGP）+ testrig `s2-agg`。
- 修正前: `S2_PREFIX_SHARDS=2` で aggregate `2.128.0.0/16` が消え `ribs=DIFF symbolic=DIFF`（`=3,5` も DIFF）。
- 修正後: `S2_PREFIX_SHARDS=2/3/5/8` すべて `MATCH`。単体テスト `testPrefixShardingWithAggregateMatchesVanilla` で固定。

**external BGP announcements** も対応済み: runner が `external_bgp_announcements.json` を読み込み（`S2Snapshot.load`）、controller が worker へ配布（`Start.externalAdverts`）、sharding universe に取り込み、**各 shard round で再注入**する（`BgpRoutingProcess.restageExternalAdvertisements` / `VirtualRouter.initForEgpPrefixRound`。external adverts は最初の round でのみ注入されるため）。

**redistribution closure** は static/kernel ルートを universe に追加済み（`networks/s2-static` + `testPrefixShardingWithRedistributedStaticMatchesVanilla`。static 行を外すと同テストだけが落ちることを確認）。検証: `networks/s2-external` + `testPrefixShardingWithExternalAnnouncementMatchesVanilla`（`shards=1..4` すべて `MATCH`）。

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

### 6.6 P2 評価結果（2026-09-13, `gpmetis` 未導入）

`S2Main partition`（Java の node weight）→ `scripts/partition-metrics.py --assignment ...
--weights ... --edge-weights estimated`（cut は script 側の L3+BGP union グラフ）。imbalance は
`max/mean`、cut は推定重み。`METIS` はフォールバックのため `WEIGHTED_LPT_FM` と同値。

| network | W | RANDOM imbalance / cut | NAME_ORDERED | WEIGHTED_LPT_FM | GREEDY_REGION | METIS(=fallback) |
| --- | --- | --- | --- | --- | --- | --- |
| s2-triangle | 2 | 1.333 / 6 | 1.333 / 6 | 1.333 / 6 | 1.333 / 6 | 1.333 / 6 |
| s2-line | 3 | 1.077 / 15 | 1.077 / 15 | 1.077 / **6** | 1.500 / **6** | 1.077 / 6 |
| s2-ospf-bgp | 2 | 1.222 / 6 | 1.222 / 6 | 1.222 / 6 | 1.389 / **3** | 1.222 / 6 |
| s2-fat4 (20) | 3 | 1.050 / 60 | 1.050 / 69 | 1.050 / **36** | 1.050 / **36** | 1.050 / 36 |
| s2-big2 (10) | 3 | 1.195 / 27 | 1.199 / 27 | 1.195 / **21** | 1.199 / **6** | 1.195 / 21 |

知見: balance は全 scheme でほぼ同等（weight-aware scheme はごく僅かに改善）。cut は
`WEIGHTED_LPT_FM` が RANDOM を一貫して下回る（`s2-line` 15→6、`s2-fat4` 60→36、`s2-big2`
27→21）。`GREEDY_REGION` は BGP session グラフが密な `s2-big2` で最小 cut だが balance を
やや犠牲にする（locality 優先の設計どおり）。`NAME_ORDERED` は `s2-fat4` で RANDOM より
悪化し、名前規則だけでは DCN で十分でないことを示す。3-node 網 × 3 worker は 1 node/worker
で scheme 差が出ない（想定どおり）。

### 6.7 P2 評価結果（2026-09-13, `gpmetis` 導入後・METIS 実測）

`gpmetis`（METIS 5.1.0）を評価環境に導入して METIS を実測した。既定呼び出しには **graph file
形式のバグ**があり、METIS が辺を読めていなかった。修正内容:

- **根本原因**: `MetisPartitioner.writeGraph` が頂点行を `vwgt ewgt nbr` の順で書いていた。
  METIS の正しい順序は `vwgt nbr ewgt`（頂点重み → 隣接頂点 → 辺重み）。誤順序では辺重み `1`
  が隣接頂点として解釈され、辺が自己ループ化して実グラフが消え、METIS は**重みを無視して台数
  だけ**を均そうとする（例: `s2-fat4` が 14/3/3 = imbalance 2.10）。
- **修正**: フィールド順を `vwgt nbr ewgt` に修正。あわせて `gpmetis` 呼び出しに
  `-ptype=rb -ufactor=1` を明示（既定も `rb` / `ufactor=1.001` だが固定。`-ufactor=0` は
  METIS 5.1 でクラッシュするため不可）。
- **最小再現**（6 頂点・一様重み 20 の line、nparts=3）:
  - 修正前 `.part.3 = 0 2 1 0 0 2` → 負荷 60/20/40 = 3/1/2、imbalance 1.50。
  - 修正後 `.part.3 = 0 0 2 2 1 1` → 2/2/2、imbalance 1.00。
- **testbed before → after**（Java `S2Main partition`, W=3, `imbalance / weighted-cut`）:
  `s2-line` 2.154/4 → **1.077/4**、`s2-fat4` 2.100/36 → **1.050/24**、
  `s2-big2` 1.199/12 → 1.199/**4**、`s2-mega` 1.125/22 → 1.125/**4**。

**sweep（W=3, Java `S2Main partition`, `imbalance(max/mean) / weighted-cut`）**:

| network (nodes) | RANDOM | NAME_ORDERED | WEIGHTED_LPT_FM | GREEDY_REGION | METIS |
| --- | --- | --- | --- | --- | --- |
| s2-line (6) | 1.077 / 10 | 1.077 / 10 | 1.077 / 4 | 1.500 / 4 | **1.077 / 4** |
| s2-big2 (10) | 1.195 / 18 | 1.199 / 18 | 1.195 / 14 | 1.199 / 4 | 1.199 / **4** |
| s2-fat4 (20) | 1.050 / 40 | 1.050 / 46 | 1.050 / 24 | 1.050 / 24 | **1.050 / 24** |
| s2-triangle (3) | 1.000 / 6 | 1.000 / 6 | 1.000 / 6 | 1.000 / 6 | 1.000 / 6 |
| s2-ospf-bgp (3) | 1.167 / 6 | 1.167 / 6 | 1.167 / 6 | 1.167 / 6 | 1.167 / 6 |
| s2-mega (16) | 1.124 / 30 | 1.125 / 30 | 1.124 / 18 | 1.126 / 4 | 1.125 / **4** |

（表は `imbalance / cut` の順。cut は Java 側 union グラフの推定辺重みで、§6.6 の
`partition-metrics.py` とは重み定義が異なるため数値は直接比較しない。）

知見:

- METIS は全 testbed で balance を `WEIGHTED_LPT_FM` と同等まで改善し、cut は全網で最良または
  同値（`s2-big2`・`s2-mega` では最小の 4）。
- ただし **METIS は明確な勝者ではない**: balance は weight-aware scheme 間でほぼ差がなく
  （一様重み網では RANDOM でもほぼ均衡）、差が出るのは cut。これは論文 §5.6 の知見
  （scheme 間の差は小さく、支配要因は負荷分散）と一致する。
- したがって **METIS は「品質参照（quality reference）」として位置づけ、既定 scheme には
  しない**。既定は従来どおり `RANDOM`（デモ不変）、外部依存を避けたい実運用の推奨は
  `WEIGHTED_LPT_FM`、`gpmetis` のある環境では `METIS` を品質比較に使う。
- 正しさ: `S2_BASE_PORT=18800 JAVA_TOOL_OPTIONS=-Ds2.partition=METIS scripts/local-demo.sh 3
  s2-line` → `ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`
  （controller `weighted-cut=4 imbalance=1.077`、worker peak 137.5/138.1/132.7 MiB）。

**回帰テスト**: `NodePartitionerTest#testMetisGraphFormat`（`vwgt nbr ewgt` の順序を固定。外部
バイナリ不要）と `#testMetisBalancesUniformWeightLine`（一様重み line が 2/2/2 になることを
`gpmetis` がある場合のみ検証、無ければ skip）。バイナリ不在・失敗時は `WEIGHTED_LPT_FM` に
フォールバックする挙動は不変（`#testMetisFallsBackWhenBinaryMissing`）。
### 6.8 O6 ノード重みの実測校准（2026-09-13）

**動機.** v1 は全係数 1 の加算和で、FatTree では core/agg（peer 多・originate 少）と edge
（peer 少・originate 多）が k=4 で同じ重みになり、k=2 では順序が逆転した。partition の balance
は重みのノード間順序で決まるため、実測 route 数に合わせて係数を再フィットした。

**計測法.**

- **特徴量**は partitioner と同一の値を使う。`S2Main partition`（オフライン role）に
  `-Ds2.nodeWeightsDump=<file>` を付けると `NodeWeights` が config から集計した 7 特徴
  （interfaces / peers / originationPrefixes / aclLines / policyStatements / staticRoutes /
  vrfs）をノード別 TSV に出す。dump は当該 property が設定された時だけ書く（既定 off）。
- **測定コスト**は実 multi-worker 実行の controller が `result-<N>worker.txt` に書く
  `--- distributed ---`（ノード別 main RIB）から数えた**ノード別 route 数**。これは owned
  dataplane で partitioner が動かす retained `R_w` の支配項で、ノード単位・決定的に測れる。
  同規模では per-worker peak heap は worker 非依存の config floor が支配的なので使わない
  （このことは `s2-fat2`/`s2-fat4` の W=1 phase peak が `s2-line` と同程度であることが示す）。
- **testbed**（13 サンプル, 89 ノード）: `s2-line` / `s2-big-bgp` / `s2-big2` / `s2-huge` /
  `s2-mega`（line ladder）、`s2-fat2` / `s2-fat4`（DCN の role 差）、`s2-ospf` /
  `s2-ospf-bgp` / `s2-redist` / `s2-static` / `s2-agg` / `s2-external`。`s2-fat4` は既知の tie
  不安定（C1）で ribs=DIFF だが、コスト測定は partition 非依存なので使う。
- **fit.** partitioner は同一ネットワーク内のノードを比べるだけなので、ネットワークごとに
  特徴とコストの平均を引いてから非負 ridge 最小二乗する（within-network）。pooled fit だと
  ネットワーク規模（＝分割に不要な cross-network scale）が支配してノード間順序を説明できない。
  再現: `scripts/calibrate-weights.py fit --center --sample <name>:<result>:<features> ...`。

**係数.** within-network 比は `interfaces : peers : static = 0.68 : 1.81 : 0.99`。

| feature | v1 | fitted | calibrated (int) | 備考 |
| --- | --- | --- | --- | --- |
| interfaces | 1 | 0.683 | 1 | |
| peers | 1 | 1.813 | 3 | |
| originationPrefixes | 1 | 0.000 | 0 | 自ノードの originate 数は自 RIB をほとんど動かさない |
| aclLines | 1 | (unidentifiable) | 1 | dataset で一定。`s2-acl` の phase peak (59→121 MiB) で BDD コストを確認 |
| policyStatements | 1 | 0.000 | 0 | 生成 policy は BGP 構造の重複（v1 の支配項） |
| staticRoutes | 1 | 0.989 | 1 | |
| vrfs | 1 | (unidentifiable) | 1 | dataset で一定 |

`NodeWeights` は v1 の「ACL/policy 共通係数」を `ACL_LINE` と `POLICY_STATEMENT` に分離した。

**評価 (1): 相関**（weight vs 測定 route 数、ノード単位）。v1 → calibrated:

| network | n | v1 Pearson / Spearman | cal Pearson / Spearman |
| --- | --- | --- | --- |
| s2-fat2 | 5 | **-1.000 / -1.000** | **+1.000 / +1.000** |
| s2-fat4 | 20 | nan（重み一定） | +1.000 / +1.000 |
| s2-static | 2 | nan（重み一定） | +1.000 / +1.000 |
| s2-line | 6 | 1.000 / 1.000 | 1.000 / 1.000 |
| s2-mega | 16 | 1.000 / 1.000 | 1.000 / 1.000 |
| s2-redist | 4 | 0.845 / 0.894 | 0.845 / 0.894 |
| s2-agg | 3 | 0.866 / 0.500 | 0.756 / 0.500 |
| ALL (pooled) | 89 | 0.945 / 0.985 | 0.945 / **0.988** |

負相関または未定義（重み一定）のネットワーク数は 1 → 0。

**評価 (2): コスト考慮 imbalance**（`WEIGHTED_LPT_FM` の assignment を測定 route コストで
評価、max/mean）。v1 → calibrated:

| network | W | v1 | calibrated |
| --- | --- | --- | --- |
| s2-fat2 | 2 | 1.279 | **1.148** |
| s2-fat4 | 2 | 1.000 | 1.000 |
| s2-fat4 | 3 | 1.047 | 1.061 |
| s2-line | 2 | 1.000 | 1.000 |
| s2-line | 3 | 1.071 | 1.071 |
| s2-mega | 3 | 1.125 | 1.125 |

`s2-fat2` は改善。`s2-fat4` W=3 はわずかに悪化する: calibrated weight は peers を強く評価し
core:edge の重み比 18:12=1.5 に対し実測コスト比は 44:40=1.10 のため。線形 config 特徴 1 本では
core/edge のコスト比を表現できず、根本対策は §3.2 の topology 補正 (v2)。これは §6.9 で実装・測定した。

**既定挙動.** 既定 scheme は `RANDOM` で重みを使わないため不変。`s2-line` / `s2-mega` のデモは
MATCH のまま。calibrated `WEIGHTED_LPT_FM` でも `s2-line` / `s2-fat2` / `s2-mega` は MATCH
（`s2-fat4` は C1 の tie 不安定で ribs のみ DIFF、reachability / symbolic / answer は MATCH）。

変更ファイル: `NodeWeights.java`（`Features` + `-Ds2.nodeWeightsDump` + calibrated coefficients）、
`NodePartitionerTest.java`、`scripts/calibrate-weights.py`（新規）。

### 6.9 O6 v2 トポロジ補正（フルテーブル閉包）の実装と実測（2026-09-13）

**実装.** §3.2 v2 を `NodeWeights` に実装した。BGP session グラフ（`BgpTopology` の
hostname 隣接）を `CommunicationGraph.build` が構築し、`NodeWeights.compute(configs, bgpAdjacency)`
に渡す。各ノードについて BGP 連結成分を求め、その成分内で originate される prefix 数の総和
`fullTableRoutes(v)`（= そのノードが受信するフルテーブルの推定経路数、伝播閉包）を計算し、

```
weight_v2(v) = base(v) + V2_FULL_TABLE_WEIGHT * fullTableRoutes(v)
```

とする。`-Ds2.nodeWeightsV2=true` で有効（既定 off。既定 scheme `RANDOM` は重みを使わないため
デモ不変）。係数 `V2_FULL_TABLE_WEIGHT = 2`、較正用 override `-Ds2.nodeWeightsV2Scale`。
dump (`-Ds2.nodeWeightsDump`) に `bgpClosure` 列を追加し、`scripts/calibrate-weights.py` は
`fit --origin-closure-weight K` で補正込みの相関を出せる。

**重み比の補正.** `s2-fat4`: base core/agg=18, edge=12（比 1.50）、実測コスト比 44:40=1.10。
v2 (K=2) は 90:84（比 1.071）、K=2.4 で 1.100。`s2-fat2`: base 10:8、v2 (K=2) 27:25。

**評価 (1): 相関**（weight vs 測定 route 数）。補正項は testbed の BGP 連結成分が 1 個のため
**ノード間では定数**で、within-network の Pearson/Spearman は base と完全に同一（§6.8 の
calibrated 列と同じ）。pooled のみ K=2 で Pearson 0.945→1.000、Spearman 0.988→0.998 に上がるが、
これはネットワーク規模（閉包）と総 route 数の cross-network scale を拾ったもので、partitioner が
比較する within-network 順序ではない（pooled fit を避ける §6.8 の理由と同じ）。

**評価 (2): コスト考慮 imbalance**（`WEIGHTED_LPT_FM` の assignment を測定 route コストで採点、
max/mean）。base → v2 (K=2)：

| network | W | base (calibrated) | v2 | v1 (参考) |
| --- | --- | --- | --- | --- |
| s2-line | 2 / 3 | 1.000 / 1.071 | 1.000 / 1.071 | 1.000 / 1.071 |
| s2-fat2 | 2 | **1.148** | 1.148 | 1.279 |
| s2-fat4 | 2 | 1.000 | 1.000 | 1.000 |
| s2-fat4 | 3 | **1.061** | 1.061 | **1.047** |
| s2-ospf | 3 | 1.125 | 1.125 | — |
| s2-ospf-bgp | 2 | 1.176 | 1.176 | — |
| s2-redist | 2 / 3 | 1.143 / 1.143 | 1.143 / 1.143 | — |
| s2-static | 2 | 1.111 | 1.111 | — |
| s2-agg | 3 | 1.250 | 1.250 | — |
| s2-external | 2 | 1.273 | 1.273 | — |
| s2-big2 | 3 | 1.199 | 1.199 | — |
| s2-big-bgp | 3 | 1.003 | 1.003 | — |
| s2-huge | 3 | 1.125 | 1.125 | — |
| s2-mega | 3 | 1.125 | 1.125 | — |

**v2 は全 testbed で assignment・imbalance を変えなかった**（per-worker 測定コストまで一致）。
理由: 現 testbed は BGP 連結成分が 1 個なので補正項は全ノード共通の定数であり、重みの*差*を
変えない。`WEIGHTED_LPT_FM` の LPT は重み降順で、FM の改善量は cut 辺重みのみ、load cap は
平均重みに比例するため、一様な定数シフトでは最終 assignment が変わらない。係数を K=0..20 で
sweep しても同一だった。

**探索的知見（peer 項との緊張）.** v2 の閉包項ではなく BGP peer 係数を 0 にすると（`weight =
interfaces + static + vrfs + 閉包`）`s2-fat4` W=3 は 1.061→**1.047**（v1 と同じ最適）に戻るが、
`s2-fat2` W=2 は 1.148→**1.213** に悪化する。すなわち core/edge の大小は role 依存で、fat2 は
peers 項を必要とし fat4 は過大評価になる。この緊張は成分定数の補正では解けない（線形 config
特徴 1 本の限界）。

**結論.** v2 は設計意図どおり重み比を実測比へ圧縮するが、assignment / コスト考慮 imbalance を
改善しないため **既定 off のまま gate する**（`-Ds2.nodeWeightsV2=true`）。多重 BGP 成分や部分
テーブルの WAN では閉包がノード固有になり得るため、機構としては残す。FatTree の core/edge 比を
partition 結果に反映させるには、成分定数ではなく role ごとの重みスケール（例: 実測コスト比での
再校准、または FM の目的関数に測定コストを入れる）が必要で、本 O6 の範囲外。

**検証.** `bazel test //projects/s2:s2_tests` = 73 tests / 0 failures（v2 の成分閉包・加算・既定 off を
`NodePartitionerTest` に追加）。既定デモ `s2-line` / `s2-mega`（scheme RANDOM）は
`ribs/reachability/symbolic/answer = MATCH`。v2 を有効にした `WEIGHTED_LPT_FM` の `s2-line` W=3 も
MATCH。

### 6.10 O6 residual: role-level peer scaling の実装と実測（2026-09-13）

**動機.** §6.9 の残件「成分定数でない role 別スケール」への回答。§6.9 は BGP peer 係数を 0 にすると
`s2-fat4` W=3 が 1.061→1.047 に改善するが `s2-fat2` W=2 は 1.148→1.213 に悪化することを示した。
すなわち必要な peer 係数は FatTree の形（k）に依存し、単一係数では両立しない。

**実装（`-Ds2.nodeWeightsRoleScale=true`, 既定 off）.** `NodeWeights.adaptivePeerCoefficient` が
ネットワークごとに peer 項の係数を選ぶ:

```
if maxPeers == minPeers:            peerCoefficient = 0        # role 信号なし
elif meanInterfaces(peers=max) >= meanInterfaces(peers=min):
                                    peerCoefficient = 0        # interfaces が既に role を順序付け
else:                               peerCoefficient = BGP_PEER # peer 項で core を edge より上に保つ
```

`-Ds2.nodeWeightsPeerScale=<int>`（評価用 override）が設定されていればそれが最優先。全ノード同 peer
数の網では peer 項は元々 0 なので不変。`s2-fat4` は core/agg・edge とも interfaces=5 なので係数 0、
`s2-fat2` は core interfaces=3 < edge interfaces=4 なので係数 3 のまま。

**装置.** `-Ds2.nodeWeightsPeerScale` で係数を 0..5 に振り、`S2Main partition`（`WEIGHTED_LPT_FM`）
の assignment を `scripts/calibrate-weights.py imbalance`（測定コスト = 1-worker `result-1worker.txt`
の per-node main-RIB route 数）で採点（max/mean）。`scripts/partition-metrics.py --assignment ...
--weights <測定コスト>`（`--weights` は script の重みを上書きするので測定コストを渡せる）でも同じ
imbalance を再現する（fat4 1.061→1.047、fat2 1.148 不変）。

| network | W | P=0 | P=1 | P=2 | P=3 | P=4 | P=5 | role-scale |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| s2-fat2 | 2 | 1.213 | 1.213 | 1.148 | 1.148 | 1.148 | 1.148 | **1.148** |
| s2-fat4 | 3 | **1.047** | 1.061 | 1.061 | 1.061 | 1.061 | 1.061 | **1.047** |
| s2-line | 3 | 1.071 | 1.071 | 1.071 | 1.071 | 1.071 | 1.071 | 1.071 |
| s2-mega | 3 | 1.125 | 1.125 | 1.125 | 1.125 | 1.125 | 1.125 | 1.125 |
| s2-big2 | 3 | 1.199 | 1.199 | 1.199 | 1.199 | 1.199 | 1.199 | 1.199 |
| s2-big-bgp | 3 | 1.003 | 1.003 | 1.003 | 1.003 | 1.003 | 1.003 | 1.003 |

**結果.** 単一係数では fat2（P≥2 が必要）と fat4（P=0 が最良）が両立しない（負の結果）。adaptive
rule は両方を同時に満たす: fat2 1.148（calibrated と同値）、fat4 1.061→**1.047**（`v1` の最適値）、
他の testbed は assignment 自体が不変で metric も不変。13 testbed の検証で **metric の退行は 0**、
改善は fat4 W=3 の 1 件。`s2-agg` のみ assignment が変わるが metric は 1.250 のまま。

**採用判断.** この rule は 2 つの FatTree 形状（fat2/fat4）の interface/peer 関係から導いたもので、
より広い網での検証は未実施。`v2`/O6b と同じく **既定 off（gate）** とし、`-Ds2.nodeWeightsRoleScale=true`
で有効化する。既定 scheme は `auto`→`METIS`（重みを使わない）なので runner のデモには影響しない。
既定 on への昇格は、より多くの DCN/WAN testbed で退行がないことを確認してから。

**検証.** `bazel test //projects/s2:s2_tests`（`adaptivePeerCoefficient`・override 優先順位のテストを
追加）。`-Ds2.nodeWeightsRoleScale=true` の `WEIGHTED_LPT_FM` は Java 経由でも fat2=1.148 /
fat4=1.047 を再現（assignment はそれぞれ P=3 / P=0 と一致）。

### 6.11 AUTO 選定と runner 既定の確認（2026-09-13）

`S2Main partition`（union グラフの Java node weight、W は表の列）:

| network | W | RANDOM imb / cut | auto (=METIS) | WEIGHTED_LPT_FM | METIS |
| --- | --- | --- | --- | --- | --- |
| s2-line | 3 | 1.154 / 10 | 1.154 / **4** | 1.154 / **4** | 1.154 / **4** |
| s2-ospf-bgp | 3 | 1.364 / 6 | 1.364 / 6 | 1.364 / 6 | 1.364 / 6 |
| s2-big2 | 3 | 1.180 / 18 | 1.197 / **4** | 1.180 / 14 | 1.197 / **4** |
| s2-triangle | 3 | 1.000 / 6 | 1.000 / 6 | 1.000 / 6 | 1.000 / 6 |
| s2-fat2 | 2 | 1.217 / 2 | 1.217 / 2 | 1.130 / **4** | 1.217 / 2 |
| s2-fat4 | 3 | 1.096 / 40 | 1.038 / **24** | 1.096 / **24** | 1.038 / **24** |
| s2-mega | 3 | 1.121 / 30 | 1.124 / **4** | 1.121 / 18 | 1.124 / **4** |

`auto` は `s2-fat4` を **DCN (regular-degree fabric)**、他を WAN と分類し、`gpmetis` 導入環境なので
全網 `METIS` を選ぶ（`s2-fat4` 以外の WAN 判定は line/ring/小網を WAN 側に落とす設計どおり）。
controller ログ例:

```
S2 controller: partition scheme=METIS (requested=AUTO, shape=DCN (regular-degree fabric), gpmetis=available) ...
S2 controller: partition scheme=METIS (requested=AUTO, shape=WAN (sparse/irregular L3), gpmetis=available) ...
```

**正しさ（1 / 3 worker, runner 既定 auto と `-Ds2.partition=WEIGHTED_LPT_FM`）**: `s2-line` /
`s2-ospf-bgp` / `s2-big2` / `s2-triangle` の全 16 run が
`ribs=MATCH reachability=MATCH symbolic=MATCH answer=MATCH`。

### 6.12 O6 residual 追試: role-level peer scaling の形状一般化（2026-09-13）

**目的.** §6.10 の adaptive peer rule は fat2/fat4 の 2 形状でしか検証しておらず、「より広い
DCN/WAN testbed で退行がないことを確認してから既定 on」としていた。本節は k=6 FatTree と
hub/route-reflector 星を加え、既定 on へ昇格できるかを判定する。

**testbed 追加.**

- `networks/s2-fat6` = `scripts/gen-topology.py fattree --k 6 --originate 2`。45 switches
  （core 9 / agg 18 / edge 18。依頼時の「54」は標準 3 層の別の数え方で、本 generator は 45）。
  core/agg は 6 peer・7 interface、edge は 3 peer・6 interface で、rule は §6.10 と同じ
  「busiest tier の interface が最多 → peer 項を落とす」(coeff 0) 分岐に入る。
- `scripts/gen-topology.py` に `hub` サブコマンドを追加し `networks/s2-hub`（spokes 8,
  originate 2, 9 switches）を生成。hub は 8 peer・9 interface、leaf は 1 peer・4 interface。
  WAN の検証が line 系だけにならないようにするための star/RR 形状。

**方法.**

- **オフライン role**（軽量）: `S2Main partition <net> <W>` を
  `-Ds2.partition=WEIGHTED_LPT_FM` と `-Ds2.nodeWeightsRoleScale={true,false}` で W=2,3,4 実行し、
  controller が報告する `weighted-cut` と weight ベース `imbalance(max/mean)` を記録。
- **cost-aware**（O6 と同一手法）: 1-worker `result-1worker.txt` の per-node main-RIB route 数を
  測定コストとし、その run の assignment を `scripts/calibrate-weights.py imbalance` /
  `scripts/partition-metrics.py --weights` で採点（両者は同値を再現）。s2-mega の 1-worker run は
  host 高負荷のため省略し、mega はオフラインのみ（cost-aware は未測定と明記）。他の 6 網は
  1-worker が 5–6 秒で `MATCH`。fat6 も軽量だった（依頼時の想定よりはるかに小さく、skip 不要）。

**オフライン（重みベース、`cut / imbalance`; off → on）.**

| network (shapes) | W | off | on |
| --- | --- | --- | --- |
| s2-fat2 (DCN k=2, 5) | 2 / 3 / 4 | 4/1.130 · 4/1.174 · 8/1.391 | 4/1.130 · 4/1.174 · 8/1.391 |
| s2-fat4 (DCN k=4, 20) | 2 / 3 / 4 | 16/1.000 · 24/1.096 · 36/1.077 | 16/1.000 · 24/**1.050** · 36/**1.000** |
| s2-fat6 (DCN k=6, 45) | 2 / 3 / 4 | 54/**1.026** · 104/1.079 · 84/1.099 | 54/1.070 · 102/1.079 · 84/**1.076** |
| s2-line (WAN line, 6) | 2 / 3 / 4 | 2/1.000 · 4/1.154 · 8/1.231 | 2/1.000 · 4/**1.091** · 6/1.273 |
| s2-big2 (WAN, 10) | 2 / 3 / 4 | 2/1.000 · 14/1.180 · 12/1.191 | 2/1.000 · 14/1.195 · 12/1.198 |
| s2-mega (WAN, 16) | 2 / 3 / 4 | 6/1.000 · 18/1.121 · 14/1.002 | 6/1.000 · 18/1.124 · 14/1.000 |
| s2-hub (WAN star, 9) | 2 / 3 / 4 | 12/1.020 · 16/1.041 · 16/1.388 | 10/1.000 · 12/1.200 · 14/1.200 |

重みベースの imbalance は role が重み自体を変えるため自己言及的で、**hub W=3 のように
重みベースでは悪化 (1.041→1.200) でも測定コストでは改善 (後述 1.261→1.109)** する例がある。
判定には次表を使う。

**cost-aware（測定 main-RIB route 数, `imbalance(max/mean)`; off → on）.**

| network | W=2 | W=3 | W=4 |
| --- | --- | --- | --- |
| s2-fat2 | 1.148 → 1.148 | 1.180 → 1.180 | 1.443 → 1.443 |
| s2-fat4 | 1.000 → 1.000 | 1.061 → **1.047** | 1.019 → 1.019 |
| s2-fat6 | **1.023 → 1.068** | 1.068 → 1.073 | 1.071 → 1.071 |
| s2-line | 1.000 → 1.000 | 1.071 → 1.071 | 1.286 → 1.286 |
| s2-hub | **1.261 → 1.051** | **1.261 → 1.109** | 1.261 → 1.261 |
| s2-big2 | 1.000 → 1.000 | 1.199 → 1.199 | 1.200 → 1.200 |
| s2-mega | —（未測定） | — | — |

**知見.**

- **fat2 は完全に不変**: rule は §6.10 どおり peer 項 3 を維持し、assignment も測定コストも同一。
- **fat4 W=3 は 1.061→1.047**（§6.10 を再現）。W=2/4 は測定コスト同一（assignment は変わる）。
- **fat6 は退行**: W=2 が 1.023→1.068、W=3 が 1.068→1.073。rule は fat6 でも coeff 0 を選ぶ
  （core/agg 7 iface ≥ edge 6 iface）。重みは core/agg=8, edge=7 で実測比（heavy 93 : light 87 =
  1.069）に近いにもかかわらず、`WEIGHTED_LPT_FM` は heavy 27 ノードを off の 14/13 ではなく
  **15/12** に割り（per-worker コスト 2178/1899 vs off 2085/1992）、imbalance を悪化させる。
  すなわち重み*比*が実測に近づいても partitioner の balance 目的が max/mean コスト最小と一致
  せず、coeff 0 が割当を悪い側に倒す。fat4 で改善したのは interface が完全に一様（重み一定）に
  なる偶然に近い。
- **WAN star は改善**: hub W=2 1.261→1.051、W=3 1.261→1.109。peer 項 3 は hub の重みを
  34 : 8（≈4.3 倍）と過大評価し、coeff 0 が 10 : 5（2.0 倍）に緩める（実測コスト比 41 : 27 ≈ 1.5）。
  方向性は正しい。
- line / big2 / mega は assignment がほぼ不変で、測定コストも不変。

**判定: 既定 on へ昇格しない（gate 維持）.** 改善 3 件（fat4 W=3, hub W=2/W=3）に対し、
**狙った形状である FatTree の k=6 で有意な退行 1 件（fat6 W=2, +0.045）** と軽微な退行 1 件
（fat6 W=3, +0.005）がある。「より広い形状で退行なし」という昇格条件を満たさないため、
`-Ds2.nodeWeightsRoleScale` は**既定 off のまま**とする（`-Ds2.nodeWeightsRoleScale=true` で
opt-in）。runner 既定は `auto`→`METIS`（重み非依存）なので demo への影響はない。
WAN star の改善は「peer 項が role を過大評価する」という方向性の正しさを示すので、将来は
形状ごとの静的係数ではなく、伝播閉包/実測に基づく cost-aware partitioner（§3.2 v2 の延長、
FM 目的関数に測定コストを入れる等）で扱うのが筋。

**再現.**

```
# オフライン (on/off)。出力の weighted-cut / imbalance を記録。
S2_INPUT_DIR=$PWD/networks S2_OUTPUT_DIR=/tmp/rs-<net>-<W>-<on|off> \
  JAVA_TOOL_OPTIONS="-Ds2.partition=WEIGHTED_LPT_FM -Ds2.nodeWeightsRoleScale=<true|false>" \
  java -jar bazel-bin/projects/s2/s2_main_deploy.jar partition <net> <W>
# cost-aware: 1-worker の per-node route 数を測定コストにする。
S2_BASE_PORT=23100 scripts/local-demo.sh 1 <net>
python3 scripts/calibrate-weights.py imbalance \
  --sample <net>:results/local-<net>-1/result-1worker.txt:/tmp/rs-features/features-<net>.tsv \
  --assignment ON=<on assignment> --assignment OFF=<off assignment> --workers <W>
```

**検証.** `bazel test //projects/s2:s2_tests`（fat6 / hub 形状が coeff 0 分岐に入ることを
`NodePartitionerTest#testAdaptivePeerCoefficientOnFat6AndHubShapes` に追加）。
既定 demo `S2_BASE_PORT=23000 scripts/local-demo.sh 3 s2-line` は
`ribs/reachability/symbolic/answer = MATCH`。

---

## 7. マイルストーン

- **P0 計測基盤**: **完了** = `scripts/gen-topology.py`（FatTree/line）、`scripts/bench.sh`（＋phase 時刻）、`scripts/partition-metrics.py`（imbalance/cut）、`scripts/calibrate-weights.py`（O6 の特徴 dump・within-network フィット・コスト考慮 imbalance）、`scripts/ci-matrix.sh`、sidecar RPC stats、`OPS.md`。知見: FatTree eBGP k≥4 は tie 不安定（C1）→ MATCH 検証は tie 安定網で。
- **P1 shadow lazy 化・boundary-only 化**: **概ね実装済み（opt-in）** = `-Ds2.ownedDataplane`（config 由来 stub FIB、`IncrementalBdpEngine.dataPlaneNodes` で最終 dataplane を owned 限定）。caveat は **M4 で堅牢化済み**（tracks/VNI/tunnel/IPsec は full にフォールバック）。加えて **M1 `-Ds2.descriptorShadows`** で remote の policy 本体を削減。
- **P2 partitioner プラグイン化**: **完了（2026-09-13）**。新パッケージ `.../ibdp/partition/`
  に `NodePartitioner` + `RANDOM` / `NAME_ORDERED` / `WEIGHTED_LPT_FM` / `GREEDY_REGION` /
  `METIS` を実装。controller が union 通信グラフと `NodeWeights` を構築して assignment を1回だけ
  算出し、`S2ControlMessages.Start.assignment` で配布、worker は再計算しない。`-Ds2.partition=<scheme>`
  で選択（コード既定 `RANDOM` = 従来の hash-shuffle round-robin、デモ不変）。`METIS` は `gpmetis -seed=0`
  を起動し、バイナリ不在時は `WEIGHTED_LPT_FM` にフォールバックする。§3.4 の **`AUTO`**（DCN/WAN
  自動選択、`AutoSchemeSelector`）を追加し、**S2 runner の既定を `auto`** に変更（`scripts/local-demo.sh`
  と k8s worker/controller manifest が `JAVA_TOOL_OPTIONS` に `-Ds2.partition=auto` を付与。ユーザの
  `-D` が後勝ちで上書き可能）。評価 CLI `S2Main partition
  <net> <W>` が assignment と node weight を出力し、`scripts/partition-metrics.py
  --assignment ... --weights ...` で imbalance / weighted cut を測る（`--weights` は今回追加）。
- **P3 `PrefixDependencyGraph`**: **完了** = `PrefixDependencyGraph.java` + `PrefixSharder` 刷新（weighted WCC-LPT、degenerate フォールバック、決定性）、`PrefixSharderTest` 拡張。
- **P-X shard 数自動選択**: **完了** = `S2_PREFIX_SHARDS=auto`（別名 `-Ds2.prefixShardCount=auto`）。DPDG の成分数・重みから `PrefixShardCountSelector` が決定的に N を選ぶ（予算 `-Ds2.prefixShardBudgetMiB`、既定 192 MiB、上限 16）。sweep は `scripts/shard-sweep.sh`、測定は `M5-SCALE.md`。未設定時の挙動（sharding なし）は不変。
- **P4 評価 → 既定 scheme 決定 → `M5-SCALE.md` / `README.md` 更新**: **評価実施（§6.7, METIS 実測）**。
  既定は `RANDOM` のまま（デモ不変）、実運用の推奨は `WEIGHTED_LPT_FM`、`METIS` は品質参照。
  `README.md` への反映は未。

---

## 8. リスクと隣タブとの調整

- **競合**: `S2Main.java` / `S2BdpEngine.java` / `PrefixSharder.java` は隣タブのメモリ実験と重なる。partition 系は新パッケージ（`.../ibdp/partition/`）に隔離し、既存ファイルの変更を最小化する。P1 相当は実装済みなので、`S2Main` の変更は「assignment を controller で算出して配布」の1箇所 + scheme plumbing に限定し、決定性変更で `S2ControlMessages` に触れる。
- **決定性**: 乱択 scheme は controller 計算 + seed 固定で配布。worker 再計算をやめる。
- **METIS 依存**: `gpmetis` を評価環境に導入済み（§6.7）。無い環境では純 Java scheme に
  フォールバック（挙動は不変）。導入の可否は §9-3。
- **BGP 多重固定点**: cyclic equal-cost 網の tie-break 非決定（`M5-SCALE.md` Known residual）と partition の影響を混同しない。partition 評価は tie が安定な testbed で行う。
- **推定精度**: ノード/prefix 重みの推定が外れると scheme 比較が無意味化。**O6 で config 特徴の係数を実測 route 数に校准済み（§6.7）**。ただし config 特徴だけでは topology 由来のコスト（FatTree の core/edge 比、経路伝播閉包）を表現できないため、v2 topology 補正が残る。
- **owned モードの caveat**: owned は opt-in で、Track/VXLAN/IPsec/tunnel/BGP reachability 未対応。partition 評価は対応済み網で行い、既定化は caveat 解消後（REMAINING の owned-mode hardening）。
- **prefix closure（正しさ）**: `PrefixSharder.queryPrefixes` が aggregate / redistribution / external ads を落とすため、aggregate 入り網では (B) が経路欠落しうる。DPDG 着手前に修正 + aggregate snapshot で `MATCH` を確認する（section 4.5）。

---

## 9. 未決事項

1. P1 相当は実装済み。残るのは owned-mode hardening の caveat（Track/VXLAN/tunnel/BGP reachability）を誰がいつ埋めるか。
2. ノード重み推定の係数をどの testbed で校准するか: **解決（O6, §6.8）** = 13 testbed / 89 ノードのノード別 main-RIB route 数を測定コストとし、within-network 非負 ridge で `interfaces:peers:static = 1:3:1`、`originationPrefixes = policyStatements = 0` に校准。FatTree の順序逆転を解消（負相関 1→0）。残: core/edge のコスト比は config 特徴では表現できない。**v2 topology 補正（フルテーブル閉包）は実装済み・opt-in（§6.9）** だが、現 testbed では閉包が成分定数のため assignment は不変で、既定 off のまま。比を実際の partition に効かせるには role 別スケール等が必要（O6 residual）。
3. METIS を評価環境に常設するか（Docker image に入れるか）: **評価環境には導入済み**（§6.7）。
   Docker image への同梱は未対応。`gpmetis` 不在時は `WEIGHTED_LPT_FM` にフォールバックする。
4. prefix shard 数を実行時にどう決めるか: **解決（P-X）** = `S2_PREFIX_SHARDS=auto` が DPDG の成分重みから決定的に N を選ぶ（`PrefixShardCountSelector`、予算 `-Ds2.prefixShardBudgetMiB`、上限 16）。`scripts/shard-sweep.sh` で peak-vs-N を測定し既定を正当化（`M5-SCALE.md`）。
5. DCN 判定ヒューリスティクスの設計（名前規則に依存しすぎないか）: **解決（§3.4）** =
   `AutoSchemeSelector` が階層名・BGP overlay・規則的次数の 3 信号で決定的に分類し、`gpmetis`
   の有無で concrete scheme を選ぶ。限界（不規則 DCN / 密 WAN / 偶発一致）は明記。
