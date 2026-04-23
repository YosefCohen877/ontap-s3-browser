/**
 * buckets.js — Bucket list view.
 * Renders bucket cards and navigates into a bucket on click.
 */

window.BucketView = (() => {
  const container = document.getElementById('bucketList');
  const createBucketBtn = document.getElementById('createBucketBtn');
  const createBucketModal = document.getElementById('createBucketModal');
  const createBucketForm = document.getElementById('createBucketForm');
  const createBucketName = document.getElementById('createBucketName');
  const createBucketError = document.getElementById('createBucketError');
  const createBucketSubmitBtn = document.getElementById('createBucketSubmitBtn');
  const deleteBucketModal = document.getElementById('deleteBucketModal');
  const deleteBucketForm = document.getElementById('deleteBucketForm');
  const deleteBucketSubtitle = document.getElementById('deleteBucketSubtitle');
  const deleteBucketConfirmInput = document.getElementById('deleteBucketConfirmInput');
  const deleteBucketError = document.getElementById('deleteBucketError');
  const deleteBucketSubmitBtn = document.getElementById('deleteBucketSubmitBtn');
  const deleteBucketPurgeCheckbox = document.getElementById('deleteBucketPurgeCheckbox');
  let _deleteBucketTarget = null;
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
    document.getElementById('retryBucketsBtn')?.addEventListener('click', () => load(true));
  }

  const ICON_REFRESH = `<svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4 2a1 1 0 011 1v2.101a7.002 7.002 0 0111.601 2.566 1 1 0 11-1.885.666A5.002 5.002 0 005.999 7H9a1 1 0 010 2H4a1 1 0 01-1-1V3a1 1 0 011-1zm.008 9.057a1 1 0 011.276.61A5.002 5.002 0 0014.001 13H11a1 1 0 110-2h5a1 1 0 011 1v5a1 1 0 11-2 0v-2.101a7.002 7.002 0 01-11.601-2.566 1 1 0 01.61-1.276z" clip-rule="evenodd"/></svg>`;

  const ICON_LIFECYCLE = `<svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M10 2a8 8 0 100 16 8 8 0 000-16zm0 2a6 6 0 110 12 6 6 0 010-12zm1 2a1 1 0 10-2 0v4a1 1 0 00.293.707l2.5 2.5a1 1 0 001.414-1.414L11 9.586V6z" clip-rule="evenodd"/></svg>`;
  const ICON_TRASH = `<svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd"/></svg>`;

  function _bucketCardHtml(b, cache) {
    const denied = b.accessible === false;
    const showCount = !denied && window.ServerFeatures?.bucket_count;
    const countText = denied ? 'Access denied' :
      !showCount ? '' :
      _isFresh(cache[b.name]) ? _formatCount(cache[b.name].count, true) : 'Counting files…';
    const recountBtn = showCount ? `<button class="bucket-card__recount" data-recount="${_esc(b.name)}" title="Recount files">${ICON_REFRESH}</button>` : '';
    const canDelete = !denied && window.ServerFeatures?.delete_bucket;
    const deleteBtn = canDelete
      ? `<button class="bucket-card__action bucket-card__action--danger" data-delete-bucket="${_esc(b.name)}" title="Delete bucket" aria-label="Delete bucket ${_esc(b.name)}">${ICON_TRASH}</button>`
      : '';
    const lifecycleBtn = !denied ? `<button class="bucket-card__action" data-lifecycle="${_esc(b.name)}" title="Lifecycle rules" aria-label="Manage lifecycle rules for ${_esc(b.name)}">${ICON_LIFECYCLE}</button>` : '';

    return `
      <div class="bucket-card${denied ? ' bucket-card--denied' : ''}" role="listitem" tabindex="0"
           data-bucket="${_esc(b.name)}"
           aria-label="${denied ? 'No access to' : 'Open'} bucket ${_esc(b.name)}">
        <div class="bucket-card__top">
          <div class="bucket-card__icon">${_bucketIcon()}</div>
          <div class="bucket-card__actions">${deleteBtn}${lifecycleBtn}</div>
        </div>
        <div class="bucket-card__name">${_esc(b.name)}</div>
        <div class="bucket-card__meta">
          <span class="bucket-card__date">${b.created ? formatDate(b.created) : 'No date'}</span>
          <div class="bucket-card__count-wrap">
            <span class="bucket-card__count">${countText}</span>
            ${recountBtn}
          </div>
        </div>
      </div>`;
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

    const accessible = buckets.filter(b => b.accessible !== false);
    const denied     = buckets.filter(b => b.accessible === false);

    let html = accessible.map(b => _bucketCardHtml(b, cache)).join('');

    if (denied.length) {
      html += `<div class="bucket-section-header" style="grid-column:1/-1">
        <svg viewBox="0 0 20 20" fill="currentColor" style="width:16px;height:16px;opacity:.5" aria-hidden="true">
          <path fill-rule="evenodd" d="M5 9V7a5 5 0 0110 0v2a2 2 0 012 2v5a2 2 0 01-2 2H5a2 2 0 01-2-2v-5a2 2 0 012-2zm8-2v2H7V7a3 3 0 016 0z" clip-rule="evenodd"/>
        </svg>
        Buckets without permission
      </div>`;
      html += denied.map(b => _bucketCardHtml(b, cache)).join('');
    }

    container.innerHTML = html;

    container.querySelectorAll('.bucket-card').forEach(card => {
      const openBucket = () => Nav.toBrowser(card.dataset.bucket, '');
      card.addEventListener('click', openBucket);
      card.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') openBucket(); });
    });

    container.querySelectorAll('[data-lifecycle]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const bucket = btn.dataset.lifecycle;
        if (window.LifecycleModal && bucket) window.LifecycleModal.open(bucket);
      });
    });

    container.querySelectorAll('[data-delete-bucket]').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const bucket = btn.dataset.deleteBucket;
        if (bucket) _openDeleteBucketModal(bucket);
      });
    });

    container.querySelectorAll('.bucket-card__recount').forEach(btn => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const bucket = btn.dataset.recount;
        const countEl = btn.closest('.bucket-card__count-wrap')?.querySelector('.bucket-card__count');
        if (!countEl) return;

        btn.classList.add('bucket-card__recount--spinning');
        countEl.textContent = 'Counting…';

        try {
          const data = await API.bucketObjectCount(bucket, true);
          countEl.textContent = _formatCount(data.count, false);
          const cache = _readCountCache();
          cache[bucket] = { count: data.count, ts: Date.now() };
          _writeCountCache(cache);
        } catch (err) {
          countEl.textContent = 'Count failed';
        } finally {
          btn.classList.remove('bucket-card__recount--spinning');
        }
      });
    });

  }

  async function _loadBucketCounts(loadToken, forceRefresh = false) {
    if (!window.ServerFeatures?.bucket_count) return;
    if (_countAbortController) _countAbortController.abort();
    _countAbortController = new AbortController();

    const cache = forceRefresh ? {} : _readCountCache();
    if (forceRefresh) _writeCountCache({});

    const cards = Array.from(container.querySelectorAll('.bucket-card'));
    const jobs = cards
      .map(card => {
        const bucket = card.dataset.bucket;
        const countEl = card.querySelector('.bucket-card__count');
        if (!bucket || !countEl) return null;
        return { bucket, countEl, cachedEntry: cache[bucket] };
      })
      .filter(Boolean)
      .filter(job => forceRefresh || !_isFresh(job.cachedEntry));

    async function worker() {
      while (jobs.length > 0) {
        const job = jobs.shift();
        if (!job) return;
        if (loadToken !== _activeLoadToken || AppState.currentView !== 'buckets') return;

        try {
          const data = await API.bucketObjectCount(job.bucket, forceRefresh, _countAbortController.signal);
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
      _loadBucketCounts(loadToken, false);
      setStatus('ok');
      document.getElementById('endpointLabel').textContent =
        `${bucketsCache.buckets.length} bucket${bucketsCache.buckets.length !== 1 ? 's' : ''}`;
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
      const data = await API.listBuckets(force);
      clearTimeout(slowTimer);

      // ── Auto-jump if single bucket is forced ─────────────────────────────
      if (data.forced && data.buckets && data.buckets.length === 1) {
        Nav.toBrowser(data.buckets[0].name, '', false); // false = replace rather than push if it's the very first hit
        return;
      }

      _renderBuckets(data.buckets);
      _loadBucketCounts(loadToken, force);
      _writeBucketsCache(data.buckets);
      setStatus('ok');
      document.getElementById('endpointLabel').textContent =
        `${data.buckets.length} bucket${data.buckets.length !== 1 ? 's' : ''}`;
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

  function _hideCreateBucketModal() {
    if (!createBucketModal) return;
    createBucketModal.hidden = true;
    if (!deleteBucketModal || deleteBucketModal.hidden) {
      document.body.classList.remove('lc-modal-open');
    }
  }

  function _showCreateBucketModal() {
    if (!createBucketModal || !createBucketName) return;
    createBucketError.hidden = true;
    createBucketError.textContent = '';
    createBucketName.value = '';
    createBucketModal.hidden = false;
    document.body.classList.add('lc-modal-open');
    requestAnimationFrame(() => {
      try { createBucketName.focus(); } catch (_) { /* noop */ }
    });
  }

  function _validateBucketName(bucket) {
    if (!bucket) return 'Bucket name is required.';
    if (!/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket) || bucket.includes('..') || bucket.includes('-.') || bucket.includes('.-')) {
      return 'Invalid name. Use 3–63 characters: lowercase letters, numbers, dots, and hyphens; must start and end with a letter or number.';
    }
    return null;
  }

  createBucketModal?.querySelectorAll('[data-cb-close]').forEach((el) => {
    el.addEventListener('click', _hideCreateBucketModal);
  });

  function _syncDeleteConfirmButton() {
    if (!deleteBucketSubmitBtn || !deleteBucketConfirmInput || !_deleteBucketTarget) return;
    const ok = deleteBucketConfirmInput.value.trim() === _deleteBucketTarget;
    deleteBucketSubmitBtn.disabled = !ok;
  }

  function _hideDeleteBucketModal() {
    if (!deleteBucketModal) return;
    deleteBucketModal.hidden = true;
    _deleteBucketTarget = null;
    if (deleteBucketConfirmInput) deleteBucketConfirmInput.value = '';
    if (deleteBucketPurgeCheckbox) deleteBucketPurgeCheckbox.checked = false;
    if (deleteBucketError) {
      deleteBucketError.hidden = true;
      deleteBucketError.textContent = '';
    }
    _syncDeleteConfirmButton();
    if (!createBucketModal || createBucketModal.hidden) {
      document.body.classList.remove('lc-modal-open');
    }
  }

  function _openDeleteBucketModal(bucketName) {
    if (!deleteBucketModal || !deleteBucketSubtitle || !deleteBucketConfirmInput) return;
    if (!window.ServerFeatures?.delete_bucket) {
      Toast.error('Deleting buckets is disabled on this server.');
      return;
    }
    _deleteBucketTarget = bucketName;
    deleteBucketSubtitle.textContent = bucketName;
    deleteBucketConfirmInput.value = '';
    if (deleteBucketPurgeCheckbox) deleteBucketPurgeCheckbox.checked = false;
    if (deleteBucketError) {
      deleteBucketError.hidden = true;
      deleteBucketError.textContent = '';
    }
    deleteBucketModal.hidden = false;
    document.body.classList.add('lc-modal-open');
    _syncDeleteConfirmButton();
    requestAnimationFrame(() => {
      try { deleteBucketConfirmInput.focus(); } catch (_) { /* noop */ }
    });
  }

  deleteBucketModal?.querySelectorAll('[data-db-close]').forEach((el) => {
    el.addEventListener('click', _hideDeleteBucketModal);
  });

  deleteBucketConfirmInput?.addEventListener('input', _syncDeleteConfirmButton);

  deleteBucketForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!_deleteBucketTarget || !deleteBucketSubmitBtn) return;
    if (deleteBucketConfirmInput.value.trim() !== _deleteBucketTarget) return;

    deleteBucketError.hidden = true;
    deleteBucketSubmitBtn.disabled = true;
    const purge = !!(deleteBucketPurgeCheckbox && deleteBucketPurgeCheckbox.checked);
    try {
      await API.deleteBucket(_deleteBucketTarget, { purgeContents: purge });
      const c = _readCountCache();
      delete c[_deleteBucketTarget];
      _writeCountCache(c);
      Toast.success(
        purge
          ? `Bucket "${_deleteBucketTarget}" and all its objects were deleted`
          : `Bucket "${_deleteBucketTarget}" deleted`,
      );
      _hideDeleteBucketModal();
      await load(true);
    } catch (err) {
      const msg = err.message || 'Failed to delete bucket';
      deleteBucketError.textContent = msg;
      deleteBucketError.hidden = false;
      Toast.error(msg);
    } finally {
      _syncDeleteConfirmButton();
    }
  });

  document.addEventListener('keydown', (e) => {
    if (e.key !== 'Escape') return;
    if (deleteBucketModal && !deleteBucketModal.hidden) {
      _hideDeleteBucketModal();
      return;
    }
    if (createBucketModal && !createBucketModal.hidden) {
      _hideCreateBucketModal();
    }
  });

  createBucketForm?.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!createBucketName || !createBucketSubmitBtn) return;

    const bucket = createBucketName.value.trim().toLowerCase();
    const vErr = _validateBucketName(bucket);
    if (vErr) {
      createBucketError.textContent = vErr;
      createBucketError.hidden = false;
      Toast.error(vErr);
      return;
    }

    createBucketError.hidden = true;
    createBucketSubmitBtn.disabled = true;
    try {
      await API.createBucket(bucket);
      Toast.success(`Bucket "${bucket}" created`);
      _hideCreateBucketModal();
      await load(true);
    } catch (err) {
      const msg = err.message || 'Failed to create bucket';
      createBucketError.textContent = msg;
      createBucketError.hidden = false;
      Toast.error(msg);
    } finally {
      createBucketSubmitBtn.disabled = false;
    }
  });

  createBucketBtn?.addEventListener('click', () => {
    if (!window.ServerFeatures?.create_bucket) {
      Toast.error('Creating buckets is disabled on this server.');
      return;
    }
    _showCreateBucketModal();
  });

  return { load };
})();
