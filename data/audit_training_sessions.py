import pandas as pd
import numpy as np
from datetime import datetime
import os, warnings
warnings.filterwarnings('ignore')

CSV = os.path.join('c:', os.sep, 'Users', 'cruzi', 'OneDrive', 'Desktop', 'GYM Data', 'training_sessions.csv')
df = pd.read_csv(CSV)
sep = '=' * 80

# CHECK 1
print(sep)
print('CHECK 1: BASIC STATS')
print(sep)
print(f'  Row count    : {df.shape[0]}')
print(f'  Column count : {df.shape[1]}')
print(f'  Columns      : {df.columns.tolist()}')
df['_dp'] = pd.to_datetime(df['date'], errors='coerce')
vd = df['_dp'].dropna()
dmin = vd.min().strftime('%Y-%m-%d')
dmax = vd.max().strftime('%Y-%m-%d')
print(f'  Date range   : {dmin} to {dmax}')
print('  Dtypes:')
for c in df.columns:
    if c.startswith('_'): continue
    print(f'    {c:30s} {str(df[c].dtype)}')
print()

# CHECK 2
print(sep)
print('CHECK 2: MISSING VALUES (per column)')
print(sep)
cols_orig = [c for c in df.columns if not c.startswith('_')]
missing = df[cols_orig].isnull().sum()
total_missing = missing.sum()
for c, v in missing.items():
    flag = ' <-- MISSING' if v > 0 else ''
    print(f'  {c:30s} {v:>5d}{flag}')
print(f'  {"TOTAL":30s} {total_missing:>5d}')
if total_missing == 0: print('  >> No missing values found.')
print()

# CHECK 3
print(sep)
print('CHECK 3: DATE VALIDATION (format + day_of_week match)')
print(sep)
bad_format = []
for i, row in df.iterrows():
    try:
        datetime.strptime(str(row['date']), '%Y-%m-%d')
    except Exception:
        bad_format.append((i, row['date']))
if bad_format:
    print(f'  Rows with bad date format: {len(bad_format)}')
    for idx, val in bad_format:
        print(f'    Row {idx}: {val}')
else:
    print('  Date format: ALL rows are valid YYYY-MM-DD.')

dn = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
mismatches_dow = []
for i, row in df.iterrows():
    d = df.loc[i, '_dp']
    if pd.isna(d): continue
    actual_day = dn[d.weekday()]
    if str(row['day_of_week']).strip() != actual_day:
        mismatches_dow.append((i, row['date'], row['day_of_week'], actual_day))
if mismatches_dow:
    print(f'  day_of_week mismatches: {len(mismatches_dow)}')
    for idx, dt, rep, act in mismatches_dow:
        print(f'    Row {idx}: date={dt}, reported={rep}, actual={act}')
else:
    print('  day_of_week: ALL rows match the actual calendar day.')
print()

# CHECK 4
print(sep)
print('CHECK 4: DATE RANGE & FUTURE DATE CHECK')
print(sep)
print(f'  Min date: {dmin}')
print(f'  Max date: {dmax}')
cutoff = pd.Timestamp('2026-02-28')
future = df[df['_dp'] > cutoff]
if len(future) > 0:
    print(f'  FUTURE DATES beyond Feb 2026: {len(future)}')
    for i, row in future.iterrows():
        print(f'    Row {i}: {row["date"]}')
else:
    print('  No dates beyond Feb 2026.')
print()

# CHECK 5
print(sep)
print('CHECK 5: DUPLICATE DATES')
print(sep)
dup_dates = df[df['date'].duplicated(keep=False)]
if len(dup_dates) > 0:
    grouped = dup_dates.groupby('date').size().reset_index(name='count')
    print(f'  Duplicate date entries: {len(grouped)} dates ({len(dup_dates)} total rows)')
    for _, r in grouped.iterrows():
        print(f'    {r["date"]} appears {r["count"]} times')
else:
    print('  No duplicate dates found.')
print()

# CHECK 6
print(sep)
print('CHECK 6: days_since_last VALIDATION')
print(sep)
dfs = df.sort_values('_dp').reset_index(drop=True)
mismatches_dsl = []
for i in range(1, len(dfs)):
    dc = dfs.loc[i, '_dp']
    dp2 = dfs.loc[i-1, '_dp']
    if pd.isna(dc) or pd.isna(dp2): continue
    eg = (dc - dp2).days
    rep = dfs.loc[i, 'days_since_last']
    if not np.isnan(rep) and int(rep) != eg:
        mismatches_dsl.append((dfs.loc[i, 'date'], int(rep), eg))
fdsl = dfs.loc[0, 'days_since_last']
print(f'  First row (date={dfs.loc[0, "date"]}): days_since_last = {fdsl}')
if mismatches_dsl:
    print(f'  Mismatches found: {len(mismatches_dsl)}')
    for dt, rep, exp in mismatches_dsl:
        print(f'    date={dt}: reported={rep}, expected={exp}, diff={rep-exp}')
else:
    print('  All days_since_last values match actual gaps.')
print()

# CHECK 7
print(sep)
print('CHECK 7: num_exercises vs exercises_list ITEM COUNT')
print(sep)
mismatches_ex = []
for i, row in df.iterrows():
    el = str(row['exercises_list'])
    if el == 'nan' or el.strip() == '':
        counted = 0
    else:
        counted = len([x.strip() for x in el.split(',') if x.strip()])
    rep = row['num_exercises']
    if not np.isnan(rep) and int(rep) != counted:
        mismatches_ex.append((i, row['date'], int(rep), counted, el[:80]))
if mismatches_ex:
    print(f'  Mismatches found: {len(mismatches_ex)}')
    for idx, dt, rep, cnt, el in mismatches_ex:
        print(f'    Row {idx} (date={dt}): reported={rep}, counted={cnt}')
        print(f'      exercises_list (trunc): {el}...')
else:
    print('  All num_exercises values match comma-separated count.')
print()

# CHECK 8
print(sep)
print('CHECK 8: NEGATIVE / ZERO / UNREASONABLE VALUES')
print(sep)
for col in ['total_volume', 'avg_weight', 'avg_reps']:
    neg = df[df[col] < 0]
    zero = df[df[col] == 0]
    print(f'  {col}:')
    print(f'    Negative: {len(neg)}')
    for i, row in neg.iterrows():
        print(f'      Row {i} (date={row["date"]}): {row[col]}')
    print(f'    Zero:     {len(zero)}')
    for i, row in zero.iterrows():
        print(f'      Row {i} (date={row["date"]}): {row[col]}')

dur_neg = df[df['session_duration_est'] < 0]
dur_over = df[df['session_duration_est'] > 300]
print('  session_duration_est:')
print(f'    Negative (< 0):     {len(dur_neg)}')
for i, row in dur_neg.iterrows():
    print(f'      Row {i} (date={row["date"]}): {row["session_duration_est"]}')
print(f'    Over 300 min (>5h): {len(dur_over)}')
for i, row in dur_over.iterrows():
    print(f'      Row {i} (date={row["date"]}): {row["session_duration_est"]}')
print()

# CHECK 9
print(sep)
print('CHECK 9: is_synthetic VALUES')
print(sep)
usyn = df['is_synthetic'].unique()
print(f'  Unique values: {sorted([str(v) for v in usyn])}')
valid_vals = {True, False, 'True', 'False'}
bad_syn = df[~df['is_synthetic'].isin(valid_vals)]
if len(bad_syn) > 0:
    print(f'  INVALID values: {len(bad_syn)}')
    for i, row in bad_syn.iterrows():
        print(f'    Row {i}: {repr(row["is_synthetic"])}')
else:
    print('  All values are valid True/False.')
sc = df['is_synthetic'].value_counts()
for val, cnt in sc.items():
    print(f'    {val}: {cnt}')
print()

# CHECK 10
print(sep)
print('CHECK 10: WORKOUT TYPE CONSISTENCY')
print(sep)
wc = df['workout_type'].value_counts()
print(f'  Unique workout_type values: {len(wc)}')
for wt, cnt in wc.items():
    pct = cnt / len(df) * 100
    print(f'    {wt:30s} {cnt:>5d}  ({pct:.1f}%)')
print()

# CHECK 11
print(sep)
print('CHECK 11: num_sets REASONABLENESS (flag >30 or <1)')
print(sep)
sh = df[df['num_sets'] > 30]
sl2 = df[df['num_sets'] < 1]
print(f'  num_sets > 30: {len(sh)} rows')
for i, row in sh.iterrows():
    print(f'    Row {i} (date={row["date"]}, type={row["workout_type"]}): num_sets={row["num_sets"]}')
print(f'  num_sets < 1:  {len(sl2)} rows')
for i, row in sl2.iterrows():
    print(f'    Row {i} (date={row["date"]}, type={row["workout_type"]}): num_sets={row["num_sets"]}')
ns = df['num_sets']
print(f'  Stats: min={ns.min()}, max={ns.max()}, mean={ns.mean():.2f}, median={ns.median():.1f}')
print()

# CHECK 12
print(sep)
print('CHECK 12: avg_reps REASONABLENESS (flag >30 or <1)')
print(sep)
rh = df[df['avg_reps'] > 30]
rl = df[df['avg_reps'] < 1]
print(f'  avg_reps > 30: {len(rh)} rows')
for i, row in rh.iterrows():
    print(f'    Row {i} (date={row["date"]}, type={row["workout_type"]}): avg_reps={row["avg_reps"]:.2f}')
print(f'  avg_reps < 1:  {len(rl)} rows')
for i, row in rl.iterrows():
    print(f'    Row {i} (date={row["date"]}, type={row["workout_type"]}): avg_reps={row["avg_reps"]:.2f}')
ar = df['avg_reps']
print(f'  Stats: min={ar.min():.2f}, max={ar.max():.2f}, mean={ar.mean():.2f}, median={ar.median():.2f}')
print()

# CHECK 13
print(sep)
print('CHECK 13: session_duration_est DISTRIBUTION')
print(sep)
dur = df['session_duration_est']
print(f'  Min      : {dur.min():.1f}')
print(f'  Max      : {dur.max():.1f}')
print(f'  Mean     : {dur.mean():.2f}')
print(f'  Median   : {dur.median():.1f}')
print(f'  Std      : {dur.std():.2f}')
print(f'  Zeros    : {(dur == 0).sum()}')
print(f'  Negatives: {(dur < 0).sum()}')
print('  Percentiles:')
for p in [5, 10, 25, 50, 75, 90, 95]:
    print(f'    {p:3d}th: {dur.quantile(p / 100):.1f}')
print()

# SUMMARY
print(sep)
print('AUDIT SUMMARY')
print(sep)
issues = 0
checks_ok = 0
def report(name, count):
    global issues, checks_ok
    if count > 0:
        issues += count
        print(f'  [ISSUE] {name}: {count} problem(s)')
    else:
        checks_ok += 1
        print(f'  [  OK ] {name}')

report('Bad date format', len(bad_format))
report('day_of_week mismatches', len(mismatches_dow))
report('Future dates beyond Feb 2026', len(future))
report('Duplicate dates', len(dup_dates))
report('days_since_last mismatches', len(mismatches_dsl))
report('num_exercises vs exercises_list', len(mismatches_ex))
report('Negative total_volume', int((df['total_volume'] < 0).sum()))
report('Negative avg_weight', int((df['avg_weight'] < 0).sum()))
report('Negative avg_reps', int((df['avg_reps'] < 0).sum()))
report('Duration < 0', len(dur_neg))
report('Duration > 300', len(dur_over))
report('Invalid is_synthetic', len(bad_syn))
report('num_sets > 30', len(sh))
report('num_sets < 1', len(sl2))
report('avg_reps > 30', len(rh))
report('avg_reps < 1', len(rl))
report('Missing values', int(total_missing))
print()
print(f'  Total checks passed : {checks_ok}')
print(f'  Total issue rows    : {issues}')
print(sep)
print('AUDIT COMPLETE')
print(sep)
