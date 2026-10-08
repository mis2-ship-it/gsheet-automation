'use client';
import React, { useState, useEffect } from 'react';

type Platform = 'Google' | 'Swiggy' | 'Zomato';
type TimePeriod = 'Yesterday' | 'LW' | 'L2W' | 'MTD' | 'LMTD' | 'Last 30 Days';

export default function Dashboard() {
  const [platform, setPlatform] = useState<Platform>('Google');
  const [timeFilter, setTimeFilter] = useState<TimePeriod>('MTD');
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`/api/ratings?platform=${platform.toLowerCase()}&period=${timeFilter.toLowerCase()}`)
      .then(res => res.json())
      .then(json => {
        setData(json);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [platform, timeFilter]);

  return (
    <div className="min-h-screen bg-[#0a0a0a] text-neutral-100 p-6 md:p-10 font-sans">
      <div className="flex flex-col md:flex-row md:items-center justify-between pb-6 border-b border-neutral-800 gap-4">
        <div>
          <div className="flex items-center gap-3">
            <span className="bg-amber-400 text-black font-extrabold text-xs px-2.5 py-1 rounded tracking-wider">
              COCO ONLY
            </span>
            <h1 className="text-2xl font-bold tracking-tight">Frozen Bottle Operations Hub</h1>
          </div>
          <p className="text-xs text-neutral-400 mt-1">Multi-channel performance & rating intelligence</p>
        </div>
        <div className="flex items-center gap-2 bg-neutral-900 border border-neutral-800 px-3 py-1.5 rounded-lg text-xs">
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          <span>Live Outlet Stream</span>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-5 gap-4 my-6">
        <div className="bg-neutral-900/80 border border-neutral-800 p-4 rounded-xl">
          <p className="text-xs text-neutral-400">Total Net Sales</p>
          <p className="text-xl font-bold mt-1 text-white">₹ 14,82,400</p>
          <span className="text-[11px] text-neutral-500">POS + Aggregators</span>
        </div>
        <div className="bg-neutral-900/80 border border-neutral-800 p-4 rounded-xl">
          <p className="text-xs text-neutral-400">Avg Rating (All)</p>
          <p className="text-xl font-bold mt-1 text-amber-400">4.25 ★</p>
          <span className="text-[11px] text-emerald-400">Google + Swiggy + Zomato</span>
        </div>
        <div className="bg-neutral-900/80 border border-neutral-800 p-4 rounded-xl">
          <p className="text-xs text-neutral-400">Avg KPT</p>
          <p className="text-xl font-bold mt-1 text-white">6.4 <span className="text-xs font-normal text-neutral-400">mins</span></p>
          <span className="text-[11px] text-emerald-400">&lt; 7m SLA</span>
        </div>
        <div className="bg-neutral-900/80 border border-neutral-800 p-4 rounded-xl">
          <p className="text-xs text-neutral-400">Avg O2D</p>
          <p className="text-xl font-bold mt-1 text-white">22.8 <span className="text-xs font-normal text-neutral-400">mins</span></p>
          <span className="text-[11px] text-neutral-400">Doorstep delivery</span>
        </div>
        <div className="bg-neutral-900/80 border border-neutral-800 p-4 rounded-xl col-span-2 md:col-span-1">
          <p className="text-xs text-neutral-400">Food Cost %</p>
          <p className="text-xl font-bold mt-1 text-white">27.6%</p>
          <span className="text-[11px] text-emerald-400">-0.4% vs Budget</span>
        </div>
      </div>

      <div className="bg-[#121212] border border-neutral-800 rounded-2xl p-6 shadow-xl mt-8">
        <div className="flex flex-col lg:flex-row lg:items-center justify-between pb-6 border-b border-neutral-800 gap-4">
          <div>
            <h2 className="text-lg font-bold text-white">Customer Ratings & Reviews</h2>
            <p className="text-xs text-neutral-400 mt-0.5">Filter by platform and evaluation window</p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex bg-neutral-900 p-1 rounded-xl border border-neutral-800">
              {(['Google', 'Swiggy', 'Zomato'] as Platform[]).map(p => (
                <button
                  key={p}
                  onClick={() => setPlatform(p)}
                  className={`px-3.5 py-1.5 rounded-lg text-xs font-semibold transition ${
                    platform === p ? 'bg-amber-400 text-black shadow-md' : 'text-neutral-400 hover:text-white'
                  }`}
                >
                  {p}
                </button>
              ))}
            </div>
            <div className="flex bg-neutral-900 p-1 rounded-xl border border-neutral-800 overflow-x-auto">
              {(['Yesterday', 'LW', 'L2W', 'MTD', 'LMTD', 'Last 30 Days'] as TimePeriod[]).map(t => (
                <button
                  key={t}
                  onClick={() => setTimeFilter(t)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium whitespace-nowrap transition ${
                    timeFilter === t ? 'bg-neutral-700 text-white font-semibold' : 'text-neutral-400 hover:text-neutral-200'
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 my-6">
          <div className="bg-neutral-900/60 border border-neutral-800 p-5 rounded-xl">
            <span className="text-xs font-medium text-neutral-400 uppercase tracking-wider">{platform} Rating</span>
            <div className="text-3xl font-extrabold text-amber-400 mt-2">
              {loading ? '...' : (data?.overallRating || '4.20')} ★
            </div>
            <p className="text-xs text-neutral-500 mt-1">COCO Outlets ({timeFilter})</p>
          </div>

          <div className="bg-neutral-900/60 border border-neutral-800 p-5 rounded-xl">
            <span className="text-xs font-medium text-neutral-400 uppercase tracking-wider">Rated Orders</span>
            <div className="text-3xl font-extrabold text-white mt-2">
              {loading ? '...' : (data?.ratedOrders?.toLocaleString() || '0')}
            </div>
            <p className="text-xs text-neutral-500 mt-1">Verified orders</p>
          </div>

          <div className="bg-neutral-900/60 border border-neutral-800 p-5 rounded-xl">
            <span className="text-xs font-medium text-neutral-400 uppercase tracking-wider">Customer Feedback</span>
            <div className="text-3xl font-extrabold text-white mt-2">
              {loading ? '...' : (data?.recentReviews?.length || '0')}
            </div>
            <p className="text-xs text-neutral-500 mt-1">Written comments</p>
          </div>
        </div>

        <div className="mt-6">
          <h3 className="text-xs font-semibold text-neutral-400 uppercase tracking-wider mb-3">
            Recent {platform} Comments ({timeFilter})
          </h3>
          <div className="space-y-3">
            {loading ? (
              <p className="text-xs text-neutral-500">Loading feed from Google Sheets...</p>
            ) : data?.recentReviews?.length > 0 ? (
              data.recentReviews.map((rev: any, idx: number) => (
                <div key={idx} className="bg-neutral-900/40 border border-neutral-800 p-4 rounded-xl flex justify-between items-center">
                  <div>
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-semibold text-sm text-white">{rev.store}</span>
                      <span className="text-xs text-neutral-500">{rev.date}</span>
                    </div>
                    <p className="text-xs text-neutral-300">"{rev.comment}"</p>
                  </div>
                  <span className={`text-xs font-bold px-2.5 py-1 rounded shrink-0 ${
                    rev.rating >= 4 ? 'bg-emerald-950 text-emerald-300 border border-emerald-800' : 'bg-red-950 text-red-300 border border-red-800'
                  }`}>
                    {rev.rating} ★
                  </span>
                </div>
              ))
            ) : (
              <p className="text-xs text-neutral-500">No written reviews in this window.</p>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
