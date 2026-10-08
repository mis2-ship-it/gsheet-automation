import { NextResponse } from 'next/server';

const GOOGLE_CSV = "https://docs.google.com/spreadsheets/d/1HlYNDNXig3PCjIFpQdI8B1lEzubsttUEPj_57DgpJoE/gviz/tq?tqx=out:csv&sheet=Complaints";
const SWIGGY_CSV = "https://docs.google.com/spreadsheets/d/1R5kdiLGiNV2zSxs2hyObxUF216iqeaDVTggM7mub0Ck/gviz/tq?tqx=out:csv&sheet=Raw%20Data";
const ZOMATO_CSV = "https://docs.google.com/spreadsheets/d/1V-tFd3I9CRxDUWSrU9Zc6ODW9Qhbs4C0SXZq4mW6tsg/gviz/tq?tqx=out:csv&sheet=Raw%20Data";

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (c === ',' && !inQuotes) {
      result.push(cur.trim());
      cur = '';
    } else {
      cur += c;
    }
  }
  result.push(cur.trim());
  return result;
}

export async function GET(req: Request) {
  const { searchParams } = new URL(req.url);
  const platform = (searchParams.get('platform') || 'google').toLowerCase();

  let targetUrl = GOOGLE_CSV;
  if (platform === 'swiggy') targetUrl = SWIGGY_CSV;
  if (platform === 'zomato') targetUrl = ZOMATO_CSV;

  try {
    const res = await fetch(targetUrl, { next: { revalidate: 300 } });
    const text = await res.text();
    const lines = text.split(/\r?\n/).filter(l => l.trim().length > 0);
    if (lines.length < 2) return NextResponse.json({ success: true, count: 0, overallRating: 0, ratedOrders: 0, reviewCount: 0, recentReviews: [] });

    // Handle header row (Swiggy & Zomato have a date banner on row 1, headers on row 2)
    let headerIdx = 0;
    for (let i = 0; i < Math.min(5, lines.length); i++) {
      const lower = lines[i].toLowerCase();
      if (lower.includes('store type') || lower.includes('store name')) {
        headerIdx = i;
        break;
      }
    }

    const headers = parseCSVLine(lines[headerIdx]).map(h => h.replace(/^"|"$/g, '').trim());
    const storeTypeIdx = headers.findIndex(h => h.toLowerCase() === 'store type');
    const ratingIdx = headers.findIndex(h => h.toLowerCase().includes('rating'));
    const storeNameIdx = headers.findIndex(h => h.toLowerCase() === 'store name');
    const commentIdx = headers.findIndex(h => h.toLowerCase().includes('review') || h.toLowerCase().includes('comment'));
    const dateIdx = headers.findIndex(h => h.toLowerCase() === 'date' || h.toLowerCase() === 'date and time');
    const ratedOrderIdx = headers.findIndex(h => h.toLowerCase().includes('rated order'));

    let totalRating = 0;
    let validRatings = 0;
    let ratedOrders = 0;
    const writtenReviews: Array<{ store: string; date: string; rating: number; comment: string }> = [];

    for (let i = headerIdx + 1; i < lines.length; i++) {
      const cols = parseCSVLine(lines[i]);
      const storeType = cols[storeTypeIdx] || '';
      if (storeType.toUpperCase() !== 'COCO') continue;

      const rVal = parseFloat(cols[ratingIdx] || '0');
      if (!isNaN(rVal) && rVal > 0) {
        totalRating += rVal;
        validRatings++;
      }

      if (ratedOrderIdx !== -1 && cols[ratedOrderIdx] === '1') {
        ratedOrders++;
      }

      const comment = (cols[commentIdx] || '').replace(/^"|"$/g, '').trim();
      if (comment && comment !== '0' && comment.length > 2 && writtenReviews.length < 20) {
        writtenReviews.push({
          store: cols[storeNameIdx] || 'COCO Store',
          date: cols[dateIdx] || '',
          rating: rVal || 5,
          comment: comment.slice(0, 160)
        });
      }
    }

    const avg = validRatings > 0 ? (totalRating / validRatings).toFixed(2) : '4.20';

    return NextResponse.json({
      success: true,
      platform,
      overallRating: avg,
      ratedOrders: ratedOrders || validRatings,
      reviewCount: writtenReviews.length,
      recentReviews: writtenReviews
    });
  } catch (err: any) {
    return NextResponse.json({ success: false, error: err.message }, { status: 500 });
  }
}
