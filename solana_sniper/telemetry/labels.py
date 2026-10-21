"""Future labels, computed offline at export time and stored SEPARATELY
(exports/labels.csv) from detection-time features.

Every label has: horizon, reference point (time + event id), value point,
source, method, censored flag + reason, incomplete flag. Unavailable future is
censored (value empty), never zero, and the candidate is never dropped.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .model import parse_iso, utc_iso

LABEL_VERSION = 'labels/1'

_SERIES = {
    # obs_type -> (value column, source name)
    'market_dexscreener': ('price_usd', 'dexscreener.price_usd'),
    'curve_rpc': ('price_sol', 'pump_curve_rpc.price_sol'),
}


def _pct(new: Optional[float], ref: Optional[float]) -> Optional[float]:
    if new is None or ref is None or ref <= 0:
        return None
    return (new / ref - 1.0) * 100.0


def compute_labels(db: sqlite3.Connection, now_ts: float) -> List[Dict[str, Any]]:
    db.row_factory = sqlite3.Row
    computed_at = utc_iso(now_ts)
    followups: Dict[str, Dict[str, sqlite3.Row]] = defaultdict(dict)
    anchors: Dict[str, Tuple[float, Optional[str], Optional[str]]] = {}
    horizons: Dict[str, float] = {}
    for row in db.execute('SELECT * FROM followups ORDER BY row_seq'):
        cid = row['candidate_id']
        followups[cid].setdefault(row['horizon_label'], row)   # first record wins (immutable)
        horizons[row['horizon_label']] = float(row['horizon_sec'])
        anchor = parse_iso(row['anchor_time_utc'])
        if anchor is not None and cid not in anchors:
            anchors[cid] = (anchor, row['anchor_event_id'], row['mint'])

    series: Dict[Tuple[str, str], List[Tuple[float, float, str, Optional[str]]]] = defaultdict(list)
    obs_status: Dict[Tuple[str, str, str], str] = {}
    for row in db.execute("SELECT * FROM observations WHERE obs_type IN ('market_dexscreener','curve_rpc') "
                          'ORDER BY received_at_utc, row_seq'):
        column, _ = _SERIES[row['obs_type']]
        if row['horizon_label']:
            obs_status.setdefault((row['candidate_id'], row['obs_type'], row['horizon_label']), row['status'])
        value = row[column]
        ts = parse_iso(row['received_at_utc'])
        if row['status'] == 'ok' and value is not None and value > 0 and ts is not None:
            series[(row['candidate_id'], row['obs_type'])].append((ts, float(value), row['event_id'], row['horizon_label']))

    labels: List[Dict[str, Any]] = []
    ordered = sorted(horizons.items(), key=lambda kv: kv[1])
    for cid, (anchor, anchor_event, mint) in anchors.items():
        for obs_type, (column, source) in _SERIES.items():
            points = [p for p in series.get((cid, obs_type), []) if p[0] >= anchor]
            ref = points[0] if points else None
            for label, sec in ordered:
                target = anchor + sec
                base = dict(candidate_id=cid, mint=mint, horizon_label=label, horizon_sec=sec,
                            anchor_time_utc=utc_iso(anchor), anchor_event_id=anchor_event,
                            target_time_utc=utc_iso(target), source=source,
                            label_version=LABEL_VERSION, computed_at_utc=computed_at)
                at_h = next((p for p in points if p[3] == label), None)
                censored, reason = False, None
                if target > now_ts:
                    censored, reason = True, 'horizon_not_matured_at_export'
                elif ref is None:
                    censored, reason = True, 'no_reference_observation'
                elif at_h is None:
                    status = obs_status.get((cid, obs_type, label))
                    fu = followups.get(cid, {}).get(label)
                    censored = True
                    reason = f'no_ok_observation_at_horizon:{status or (fu["status"] if fu else "no_followup_record")}'
                ret = None if censored or at_h is None else _pct(at_h[1], ref[1])
                labels.append(dict(base, label_name=f'price_return_pct[{obs_type}]',
                                   method=f'({column}@horizon / first ok {column} at/after anchor) - 1',
                                   reference_time_utc=utc_iso(ref[0]) if ref else None,
                                   reference_event_id=ref[2] if ref else None,
                                   reference_value=ref[1] if ref else None,
                                   value_time_utc=utc_iso(at_h[0]) if at_h else None,
                                   value_event_id=at_h[2] if at_h else None,
                                   value=ret, censored=int(censored), censor_reason=reason,
                                   incomplete=int(censored)))
                # Path extremes up to the horizon (inclusive of its tolerance window).
                if ref is not None and target <= now_ts:
                    window = [p for p in points if p[0] <= target + max(2.0, 0.2 * sec)]
                    hi = max(window, key=lambda p: p[1])
                    lo = min(window, key=lambda p: p[1])
                    incomplete = int(at_h is None)
                    for name, point in (('max_upside_pct', hi), ('max_drawdown_pct', lo)):
                        labels.append(dict(base, label_name=f'{name}[{obs_type}]',
                                           method=f'extreme {column} over observed points in [reference, target+tolerance]; '
                                                  f'{len(window)} points; sparse sampling underestimates extremes',
                                           reference_time_utc=utc_iso(ref[0]), reference_event_id=ref[2],
                                           reference_value=ref[1], value_time_utc=utc_iso(point[0]),
                                           value_event_id=point[2], value=_pct(point[1], ref[1]),
                                           censored=0, censor_reason='horizon_observation_missing' if incomplete else None,
                                           incomplete=incomplete, points_used=len(window)))
                else:
                    for name in ('max_upside_pct', 'max_drawdown_pct'):
                        labels.append(dict(base, label_name=f'{name}[{obs_type}]', method='path extreme',
                                           value=None, censored=1,
                                           censor_reason=reason or 'no_reference_observation', incomplete=1))
    labels.extend(_quote_labels(db, now_ts, computed_at, horizons))
    return labels


def _quote_labels(db: sqlite3.Connection, now_ts: float, computed_at: str,
                  horizons: Dict[str, float]) -> List[Dict[str, Any]]:
    """Hypothetical quote round trip: shadow buy (SOL in) vs exact-size sell quote."""
    out: List[Dict[str, Any]] = []
    buys: Dict[str, sqlite3.Row] = {}
    failed: Dict[str, sqlite3.Row] = {}
    for row in db.execute("SELECT * FROM quotes WHERE purpose='shadow_buy' AND detected_by='collector' "
                          'ORDER BY row_seq'):
        if row['quote_pair_id'] and row['status'] == 'ok':
            buys.setdefault(row['quote_pair_id'], row)
        elif row['quote_pair_id']:
            failed[row['quote_pair_id']] = row   # last status wins (retries)
    sells: Dict[Tuple[str, str], sqlite3.Row] = {}
    for row in db.execute("SELECT * FROM quotes WHERE purpose IN ('shadow_sell_exact','followup_sell_exact') "
                          "AND detected_by='collector' ORDER BY row_seq"):
        label = row['horizon_label'] or 'shadow_immediate'
        key = (row['quote_pair_id'], label)
        # keep the last attempt of a retried request (earlier attempts are transient errors)
        if key not in sells or sells[key]['status'] != 'ok':
            sells[key] = row
    anchors = {r[0]: (parse_iso(r[1]), r[2]) for r in db.execute(
        'SELECT candidate_id, MIN(anchor_time_utc), MIN(anchor_event_id) FROM followups GROUP BY candidate_id')}
    for pair_id, buy in buys.items():
        lamports_in = int(buy['amount_raw'])
        anchor, anchor_event = anchors.get(buy['candidate_id'], (parse_iso(buy['received_at_utc']), None))
        labels = [('shadow_immediate', 0.0)] + sorted(horizons.items(), key=lambda kv: kv[1])
        for label, sec in labels:
            sell = sells.get((pair_id, label))
            target = (anchor or 0.0) + sec
            censored, reason, value, value_min = False, None, None, None
            if label != 'shadow_immediate' and target > now_ts:
                censored, reason = True, 'horizon_not_matured_at_export'
            elif sell is None:
                censored, reason = True, 'no_sell_quote_for_horizon'
            elif sell['status'] != 'ok':
                censored, reason = True, f'sell_quote_status:{sell["status"]}'
            else:
                value = (int(sell['out_amount_raw']) / lamports_in - 1.0) * 100.0
                if sell['min_out_raw']:
                    value_min = (int(sell['min_out_raw']) / lamports_in - 1.0) * 100.0
            common = dict(candidate_id=buy['candidate_id'], mint=buy['mint'], horizon_label=label,
                          horizon_sec=sec, anchor_time_utc=utc_iso(anchor) if anchor else None,
                          anchor_event_id=anchor_event, target_time_utc=utc_iso(target) if anchor else None,
                          source='jupiter_quote_only', label_version=LABEL_VERSION,
                          computed_at_utc=computed_at, reference_time_utc=buy['received_at_utc'],
                          reference_event_id=buy['event_id'], reference_value=lamports_in,
                          value_time_utc=sell['received_at_utc'] if sell else None,
                          value_event_id=sell['event_id'] if sell else None,
                          censored=int(censored), censor_reason=reason, incomplete=int(censored),
                          notional_rub=buy['notional_rub'],
                          route_available=None if sell is None or reason == 'horizon_not_matured_at_export'
                          else int(sell['status'] == 'ok'))
            out.append(dict(common, label_name='quote_roundtrip_return_pct',
                            method='sell_quote.out_amount_raw / shadow_buy.amount_raw(lamports) - 1; '
                                   'quote_only, no fees/priority, NOT a fill', value=value))
            out.append(dict(common, label_name='quote_roundtrip_return_minout_pct',
                            method='sell_quote.min_out_raw (slippage floor) / shadow_buy.amount_raw - 1; quote_only',
                            value=value_min))
    # Candidates without a usable reference quote stay in the label set: censored.
    for pair_id, row in failed.items():
        if pair_id in buys:
            continue
        anchor, anchor_event = anchors.get(row['candidate_id'], (parse_iso(row['received_at_utc']), None))
        for label, sec in [('shadow_immediate', 0.0)] + sorted(horizons.items(), key=lambda kv: kv[1]):
            for name in ('quote_roundtrip_return_pct', 'quote_roundtrip_return_minout_pct'):
                out.append(dict(candidate_id=row['candidate_id'], mint=row['mint'], label_name=name,
                                horizon_label=label, horizon_sec=sec,
                                anchor_time_utc=utc_iso(anchor) if anchor else None,
                                anchor_event_id=anchor_event,
                                target_time_utc=utc_iso(anchor + sec) if anchor else None,
                                source='jupiter_quote_only', method='no reference buy quote',
                                value=None, censored=1,
                                censor_reason=f'no_reference_quote:{row["status"]}', incomplete=1,
                                notional_rub=row['notional_rub'], label_version=LABEL_VERSION,
                                computed_at_utc=computed_at))
    return out


LABEL_COLUMNS = [
    'candidate_id', 'mint', 'label_name', 'horizon_label', 'horizon_sec', 'anchor_time_utc',
    'anchor_event_id', 'target_time_utc', 'reference_time_utc', 'reference_event_id',
    'reference_value', 'value_time_utc', 'value_event_id', 'value', 'censored', 'censor_reason',
    'incomplete', 'route_available', 'notional_rub', 'points_used', 'source', 'method',
    'label_version', 'computed_at_utc',
]
