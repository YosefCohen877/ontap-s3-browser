/**
 * buckets.js — Bucket list view.
 * Renders bucket cards and navigates into a bucket on click.
 */

window.BucketView = (() => {
  const container = document.getElementById('bucketList');
  const createBucketBtn = document.getElementById('createBucketBtn');
  const COUNT_CACHE_KEY = 's3b-bucket-count-cache';
  const BUCKETS_CACHE_KEY = 's3b-buckets-cache';
  const BUCKETS_CACHE_TTL_MS = 30000;
  const COUNT_CACHE_TTL_MS = 120000;
  const COUNT_CONCURRENCY = 2;
  let _activeLoadToken = 0;
  let _countAbortController = null;

  function _readCountCache() {
    try {
      return JSON.parse(localStorage.getItem(COUNT_CACHE_KEY) || '{}');
    } catch {
      return {};
    }
  }

  function _writeCountCache(cache) {
    try {
      localStorage.setItem(COUNT_CACHE_KEY, JSON.stringify(cache));
    } catch {
      // Ignore storage quota/privacy errors.
    }
  }

  function _readBucketsCache() {
    try {
      return JSON.parse(localStorage.getItem(BUCKETS_CACHE_KEY) || 'null');
    } catch {
      return null;
    }
  }

  function _writeBucketsCache(buckets) {
    try {
      localStorage.setItem(BUCKETS_CACHE_KEY, JSON.stringify({ ts: Date.now(), buckets }));
    } catch {
      // Ignore storage quota/privacy errors.
    }
  }

  function _formatCount(count, cached = false) {
    return `${count.toLocaleString()} file${count === 1 ? '' : 's'}${cached ? ' (cached)' : ''}`;
  }

  function _isFresh(entry) {
    if (!entry || !Number.isFinite(entry.count) || !Number.isFinite(entry.ts)) return false;
    return (Date.now() - entry.ts) <= COUNT_CACHE_TTL_MS;
  }

  function _bucketIcon() {
    return `<svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
      <path d="M2 6a2 2 0 012-2h12a2 2 0 012 2v2a2 2 0 01-2 2H4a2 2 0 01-2-2V6z"/>
      <path fill-rule="evenodd" d="M2 12a2 2 0 012-2h12a2 2 0 012 2v2a2 2 0 01-2 2H4a2 2 0 01-2-2v-2zm14 1a1 1 0 11-2 0 1 1 0 012 0zM6 13a1 1 0 11-2 0 1 1 0 012 0z" clip-rule="evenodd"/>
    </svg>`;
  }

  // ── Loading state — clearly distinct from an error ──────────────────────
  function _renderLoading(slow = false) {
    container.innerHTML = `
      <div class="state-card" style="grid-column:1/-1" id="loadingCard">
        <div class="spinner"></div>
        <strong>${slow ? 'Still connecting…' : 'Connecting to S3…'}</strong>
        <span style="font-size:.8125rem;color:var(--clr-text-muted)">
          ${slow
            ? 'This is taking longer than expected. Check your endpoint and network.'
            : 'Fetching bucket list from ONTAP S3 endpoint.'}
        </span>
        ${slow ? `<button class="btn btn--ghost btn--sm" onclick="document.getElementById('navDiag').click()">
          Run Connection Test →
        </button>` : ''}
      </div>`;
  }

  // ── S3 error state — clear, actionable ───────────────────────────────────
  function _renderS3Error(err) {
    const title   = err?.title   || 'Cannot Connect to S3';
    const message = err?.message || 'Check your endpoint configuration and credentials.';
    const detail  = err?.detail  || '';
    container.innerHTML = `
      <div class="s3-error-banner" style="grid-column:1/-1">
        <div class="s3-error-banner__icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" aria-hidden="true">
            <path stroke-linecap="round" stroke-linejoin="round"
              d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z"/>
          </svg>
        </div>
        <div class="s3-error-banner__body">
          <strong class="s3-error-banner__title">${_esc(title)}</strong>
          <span class="s3-error-banner__msg">${_esc(message)}</span>
          ${detail ? `<details class="s3-error-banner__detail">
            <summary>Technical detail</summary>
            <pre>${_esc(detail)}</pre>
          </details>` : ''}
        </div>
        <div class="s3-error-banner__actions">
          <button class="btn btn--primary" id="goToDiagBtn">
            <svg viewBox="0 0 20 20" fill="currentColor" style="width:16px;height:16px" aria-hidden="true">
              <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"/>
            </svg>
            Run Connection Test
          </button>
          <button class="btn btn--ghost btn--sm" id="retryBucketsBtn">Retry</button>
        </div>
      </div>`;

    document.getElementById('goToDiagBtn')?.addEventListener('click', () => {
      document.getElementById('navDiag').click();
    });
    document.getElementById('retryBucketsBtn')?.addEventListener('click', load);
  }

  function _renderBuckets(buckets) {
    const cache = _readCountCache();
    if (!buckets.length) {
      container.innerHTML = `<div class="state-card">
        <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M5 3a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2V5a2 2 0 00-2-2H5zm0 2h10v7l-2.293-2.293a1 1 0 00-1.414 0L9 12l-1.293-1.293a1 1 0 00-1.414 0L4 13V5z" clip-rule="evenodd"/></svg>
        <strong>No buckets found</strong>
        <span>The configured credentials have access to 0 buckets.</span>
      </div>`;
      return;
    }

    container.innerHTML = buckets.map(b => `
      <div class="bucket-card" role="listitem" tabindex="0"
           data-bucket="${b.name}"
           aria-label="Open bucket ${b.name}">
        <div class="bucket-card__icon">${_bucketIcon()}</div>
        <div class="bucket-card__name">${_esc(b.name)}</div>
        <div class="bucket-card__meta">
          <span class="bucket-card__date">${b.created ? formatDate(b.created) : 'No date'}</span>
          <span class="bucket-card__count">${
            _isFresh(cache[b.name]) ? _formatCount(cache[b.name].count, true) : 'Counting files…'
          }</span>
        </div>
      </div>
    `).join('');

    container.querySelectorAll('.bucket-card').forEach(card => {
      const openBucket = () => Nav.toBrowser(card.dataset.bucket, '');
      card.addEventListener('click', openBucket);
      card.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') openBucket(); });
    });

  }

  async function _loadBucketCounts(loadToken) {
    if (_countAbortController) _countAbortController.abort();
    _countAbortController = new AbortController();

    const cache = _readCountCache();
    const cards = Array.from(container.querySelectorAll('.bucket-card'));
    const jobs = cards
      .map(card => {
        const bucket = card.dataset.bucket;
        const countEl = card.querySelector('.bucket-card__count');
        if (!bucket || !countEl) return null;
        return { bucket, countEl, cachedEntry: cache[bucket] };
      })
      .filter(Boolean)
      .filter(job => !_isFresh(job.cachedEntry));

    async function worker() {
      while (jobs.length > 0) {
        const job = jobs.shift();
        if (!job) return;
        if (loadToken !== _activeLoadToken || AppState.currentView !== 'buckets') return;

        try {
          const data = await API.bucketObjectCount(job.bucket, false, _countAbortController.signal);
          if (loadToken !== _activeLoadToken || AppState.currentView !== 'buckets') return;
          job.countEl.textContent = _formatCount(data.count, false);
          cache[job.bucket] = { count: data.count, ts: Date.now() };
          _writeCountCache(cache);
        } catch (err) {
          if (err?.name === 'AbortError') return;
          if (!_isFresh(job.cachedEntry)) {
            job.countEl.textContent = 'Count unavailable';
          }
        }
      }
    }

    await Promise.all(Array.from({ length: Math.min(COUNT_CONCURRENCY, jobs.length || 1) }, () => worker()));
  }

  function _esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  async function load(force = false) {
    const loadToken = ++_activeLoadToken;
    if (_countAbortController) {
      _countAbortController.abort();
      _countAbortController = null;
    }

    if (createBucketBtn) {
      createBucketBtn.hidden = !window.ServerFeatures?.create_bucket;
    }

    const bucketsCache = _readBucketsCache();
    const hasFreshBucketsCache = !!(
      !force &&
      bucketsCache &&
      Array.isArray(bucketsCache.buckets) &&
      Number.isFinite(bucketsCache.ts) &&
      (Date.now() - bucketsCache.ts) <= BUCKETS_CACHE_TTL_MS
    );

    if (hasFreshBucketsCache) {
      _renderBuckets(bucketsCache.buckets);
      _loadBucketCounts(loadToken);
      setStatus('ok');
      document.getElementById('endpointLabel').textContent =
        `${bucketsCache.buckets.length} bucket${bucketsCache.buckets.length !== 1 ? 's' : ''}`;
      window.AutoRefresh?.notifyRefreshed();
    } else {
      _renderLoading(false);
      setStatus('loading');
    }

    // After 4 s still loading → show a "taking longer than expected" hint
    const slowTimer = setTimeout(() => {
      // Only update if still showing the loading card (not replaced by results/error)
      if (document.getElementById('loadingCard')) {
        _renderLoading(true);
      }
    }, 4000);

    try {
      const data = await API.listBuckets();
      clearTimeout(slowTimer);

      // ── Auto-jump if single bucket is forced ─────────────────────────────
      if (data.forced && data.buckets && data.buckets.length === 1) {
        Nav.toBrowser(data.buckets[0].name, '', false); // false = replace rather than push if it's the very first hit
        return;
      }

      _renderBuckets(data.buckets);
      _loadBucketCounts(loadToken);
      _writeBucketsCache(data.buckets);
      setStatus('ok');
      document.getElementById('endpointLabel').textContent =
        `${data.buckets.length} bucket${data.buckets.length !== 1 ? 's' : ''}`;
      window.AutoRefresh?.notifyRefreshed();
    } catch (err) {
      clearTimeout(slowTimer);
      if (!hasFreshBucketsCache) {
        _renderS3Error(err);
      } else {
        Toast.error(err.message || 'Failed to refresh buckets');
      }
      setStatus('error');
      if (!hasFreshBucketsCache) {
        document.getElementById('endpointLabel').textContent = 'Not connected';
      }
    }
  }

  // Initial load when page is first shown
  load();

  document.getElementById('refreshBucketsBtn')?.addEventListener('click', () => load(true));
  createBucketBtn?.addEventListener('click', async () => {
    const raw = prompt('Enter a new bucket name:');
    if (raw === null) return;
    const bucket = raw.trim().toLowerCase();
    if (!bucket) {
      Toast.error('Bucket name is required.');
      return;
    }

    // S3/ONTAP-compatible conservative validation.
    if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || bucket.includes('..') || bucket.includes('-.') || bucket.includes('.-')) {
      Toast.error('Invalid bucket name. Use 3-63 chars: lowercase letters, numbers, dots, hyphens.');
      return;
    }

    try {
      await API.createBucket(bucket);
      Toast.success(`Bucket "${bucket}" created`);
      await load();
    } catch (err) {
      Toast.error(err.message || 'Failed to create bucket');
    }
  });

  return { load };
})();
