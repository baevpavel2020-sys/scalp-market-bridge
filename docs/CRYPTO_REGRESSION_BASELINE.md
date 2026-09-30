# Crypto Regression Baseline

Baseline SHA: 99d0a7428e132a5c688b08eda40f568ea146ced5

Observed public service surface:
- /prescan
- /market/BTCUSDT
- /market/BTCUSDT/spot
- /market/<symbol>
- /scan-auto
- /scan-auto/start
- /scan-batch
- /scan-batch/start
- /scan-job/<job_id>
- /scan/<symbol>
- /diagnostics/<symbol>

Observed engine versions from .scan/latest.json at baseline:
- prescan_v3_2_final
- scan_plus_v3_9_limit_plan
- scan_orchestrator_v2_1_adaptive_warmup
- scan_job_manager_v3_1_1_adaptive_warmup_fix

Baseline constraints:
- Preserve response compatibility while extracting modules.
- Preserve current Crypto semantics unless a change is explicitly versioned.
- price_discovery is an event/transition signal, not permission to overwrite confirmed higher-timeframe structure.
- Automated market snapshot commits on main are operational data and must not be treated as architecture changes.
