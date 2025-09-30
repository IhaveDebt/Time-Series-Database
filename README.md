/**
 * timeseries_db.ts
 *
 * Minimal in-memory time-series DB with insert, simple aggregate queries and downsampling.
 *
 * Run:
 *   ts-node src/timeseries_db.ts
 */
type Point = { ts: number; value: number };
type Series = { name: string; points: Point[] };

class TimeSeriesDB {
  series: Map<string, Series> = new Map();

  insert(name: string, ts: number, value: number) {
    const s = this.series.get(name) ?? { name, points: [] };
    s.points.push({ ts, value });
    this.series.set(name, s);
  }

  queryRange(name: string, from: number, to: number) : Point[] {
    const s = this.series.get(name);
    if (!s) return [];
    return s.points.filter(p => p.ts >= from && p.ts <= to).sort((a,b)=>a.ts-b.ts);
  }

  aggregate(name: string, from: number, to: number, stepMs: number) {
    const windowed: { start:number; avg:number; count:number }[] = [];
    const pts = this.queryRange(name, from, to);
    for (let start = from; start <= to; start += stepMs) {
      const end = start + stepMs - 1;
      const bucket = pts.filter(p => p.ts >= start && p.ts <= end);
      if (bucket.length === 0) {
        windowed.push({ start, avg: NaN, count: 0 });
      } else {
        const avg = bucket.reduce((s, p) => s + p.value, 0) / bucket.length;
        windowed.push({ start, avg, count: bucket.length });
      }
    }
    return windowed;
  }

  downsample(name: string, from:number, to:number, stepMs:number) {
    return this.aggregate(name, from, to, stepMs)
      .map(w => ({ ts: w.start, value: isNaN(w.avg) ? 0 : w.avg }));
  }

  exportCSV(name: string) {
    const s = this.series.get(name);
    if (!s) return '';
    const lines = ['ts,value', ...s.points.map(p => `${p.ts},${p.value}`)];
    return lines.join('\n');
  }
}

// demo
(async function demo(){
  const db = new TimeSeriesDB();
  const now = Date.now();
  for (let i=0;i<120;i++){
    db.insert('cpu', now - (119 - i)*1000, 30 + Math.sin(i/10)*10 + Math.random()*2);
  }
  console.log('Query last minute raw points:', db.queryRange('cpu', now - 60000, now).length);
  console.log('1s->10s downsample:', db.downsample('cpu', now - 60000, now, 10000).slice(0,5));
  console.log('CSV preview:\n', db.exportCSV('cpu').split('\n').slice(0,5).join('\n'));
})();
