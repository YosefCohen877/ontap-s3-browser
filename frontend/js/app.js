/**
 * app.js — Application bootstrap: theme, routing, toasts, loading overlay.
 * Must be loaded after api.js and before feature modules.
 */

// ── State ──────────────────────────────────────────────────────────────────
window.AppState = {
  currentView: null,     // 'buckets' | 'browser' | 'diagnostics'
  currentBucket: null,
  currentPrefix: '',
};

// ── DOM refs ───────────────────────────────────────────────────────────────
const views = {
  buckets:     document.getElementById('viewBuckets'),
  browser:     document.getElementById('viewBrowser'),
  diagnostics: document.getElementById('viewDiagnostics'),
};
const navBtns = document.querySelectorAll('.nav-btn');
const homeBtn = document.getElementById('homeBtn');
const themeToggle  = document.getElementById('themeToggle');
const statusDot    = document.getElementById('statusDot');
const endpointLabel = document.getElementById('endpointLabel');
const toastContainer = document.getElementById('toastContainer');
const loadingOverlay = document.getElementById('loadingOverlay');

// ── Theme ──────────────────────────────────────────────────────────────────
function initTheme() {
  const saved = localStorage.getItem('s3b-theme') || 'dark';
  document.documentElement.setAttribute('data-theme', saved);
}

themeToggle.addEventListener('click', () => {
  const current = document.documentElement.getAttribute('data-theme');
  const next = current === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('s3b-theme', next);
});

// ── View routing ───────────────────────────────────────────────────────────
function showView(name) {
  Object.entries(views).forEach(([key, el]) => {
    el.hidden = (key !== name);
  });
  navBtns.forEach(btn => {
    btn.classList.toggle('active', btn.dataset.view === name);
  });
  const was = AppState.currentView;
  AppState.currentView = name;
  if (was === 'browser' && name !== 'browser') {
    window.AutoRefresh?.onViewHidden?.();
  } else if (name === 'browser' && was !== 'browser') {
    window.AutoRefresh?.onViewVisible?.();
  }
}

// Nav button clicks
navBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    const view = btn.dataset.view;
    if (view === 'buckets') {
      showView('buckets');
      window.BucketView?.load();
    } else if (view === 'diagnostics') {
      showView('diagnostics');
    }
  });
});

// ── Public navigation helpers (called by feature modules) ─────────────────
window.Nav = {
  // Update the browser's address bar without reloading
  _updateUrl() {
    let path = '/';
    if (AppState.currentBucket) {
      path = '/' + AppState.currentBucket;
      if (AppState.currentPrefix) {
        path += '/' + AppState.currentPrefix;
      }
    }
    if (window.location.pathname !== path) {
      window.history.pushState({ bucket: AppState.currentBucket, prefix: AppState.currentPrefix }, '', path);
    }
  },

  toBuckets(push = true) {
    AppState.currentBucket = null;
    AppState.currentPrefix = '';
    showView('buckets');
    window.BucketView?.load();
    if (push) this._updateUrl();
  },
  toBrowser(bucket, prefix = '', push = true) {
    AppState.currentBucket = bucket;
    AppState.currentPrefix = prefix;
    showView('browser');
    window.BrowserView?.load(bucket, prefix);
    if (push) this._updateUrl();
  },
};

// Handle Browser Back/Forward buttons
window.addEventListener('popstate', (e) => {
  const path = window.location.pathname.substring(1); // remove leading /
  if (!path || path.toLowerCase() === 'home') {
    window.Nav.toBuckets(false);
  } else {
    const parts = path.split('/');
    const bucket = parts[0];
    const prefix = parts.slice(1).join('/');
    window.Nav.toBrowser(bucket, prefix, false);
  }
});

// ── Toast notifications ────────────────────────────────────────────────────
window.Toast = {
  show(message, type = 'info', duration = 4000) {
    const el = document.createElement('div');
    el.className = `toast ${type}`;
    el.textContent = message;
    toastContainer.appendChild(el);
    setTimeout(() => el.remove(), duration);
  },
  success(msg) { this.show(msg, 'success'); },
  error(msg)   { this.show(msg, 'error', 6000); },
  info(msg)    { this.show(msg, 'info'); },
};

// ── Loading overlay ────────────────────────────────────────────────────────
window.Loading = {
  show() { loadingOverlay.hidden = false; loadingOverlay.removeAttribute('aria-hidden'); },
  hide() { loadingOverlay.hidden = true;  loadingOverlay.setAttribute('aria-hidden','true'); },
};

// ── Status dot ────────────────────────────────────────────────────────────
window.setStatus = function(state) {
  statusDot.className = 'status-dot ' + (state || '');
};

// ── Helpers ────────────────────────────────────────────────────────────────
window.formatBytes = function(bytes) {
  if (bytes === null || bytes === undefined) return '—';
  if (bytes === 0) return '0 B';
  const k = 1024, sizes = ['B','KB','MB','GB','TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return (bytes / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i];
};

window.formatDate = function(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString(undefined, { dateStyle:'medium', timeStyle:'short' });
  } catch { return iso; }
};

// ── Error renderer ─────────────────────────────────────────────────────────
window.renderError = function(err, container) {
  const title   = err?.title   || 'Error';
  const message = err?.message || String(err);
  const detail  = err?.detail  || '';
  container.innerHTML = `
    <div class="state-card error">
      <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
        <path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7 4a1 1 0 11-2 0 1 1 0 012 0zm-1-9a1 1 0 00-1 1v4a1 1 0 102 0V6a1 1 0 00-1-1z" clip-rule="evenodd"/>
      </svg>
      <strong>${title}</strong>
      <span>${message}</span>
      ${detail ? `<details><summary style="cursor:pointer;font-size:.75rem">Technical detail</summary><pre style="text-align:left;font-size:.7rem;white-space:pre-wrap;word-break:break-all;margin-top:.5rem">${detail}</pre></details>` : ''}
    </div>`;
};

// ── Init ───────────────────────────────────────────────────────────────────
initTheme();

// Global feature flags
window.ServerFeatures = {
  upload: false,
  delete: false,
  create_bucket: false,
  delete_bucket: false,
  bucket_count: true,
  bucket_lifecycle: false,
};

// Check backend health and feature flags
API.health()
  .then((data) => {
    setStatus('ok');
    if (data && data.features) {
      window.ServerFeatures.upload = !!data.features.upload;
      window.ServerFeatures.delete = !!data.features.delete;
      window.ServerFeatures.create_bucket = !!data.features.create_bucket;
      window.ServerFeatures.delete_bucket = !!data.features.delete_bucket;
      window.ServerFeatures.bucket_count = data.features.bucket_count !== false;
      window.ServerFeatures.bucket_lifecycle = !!data.features.bucket_lifecycle;
      const createBucketBtn = document.getElementById('createBucketBtn');
      if (createBucketBtn) {
        createBucketBtn.hidden = !window.ServerFeatures.create_bucket;
      }
    }
  })
  .catch(() => { setStatus('error'); });

// ── Init Routing ──────────────────────────────────────────────────────────
function initRouter() {
  const path = window.location.pathname.substring(1); // remove leading /
  if (!path || path.toLowerCase() === 'home') {
    showView('buckets');
    // window.BucketView.load() is called by the module itself on load
  } else {
    const parts = path.split('/');
    const bucket = parts[0];
    const prefix = parts.slice(1).join('/');
    // Use Nav to switch view and trigger load
    window.Nav.toBrowser(bucket, prefix, false); // false = don't push state since we are already there
  }
}

window.addEventListener('load', () => {
  initRouter();
});

// Brand/logo click goes back to home (bucket list)
homeBtn?.addEventListener('click', () => {
  window.Nav.toBuckets();
});

// ── Auto-refresh engine ────────────────────────────────────────────────────
window.AutoRefresh = (() => {
  const CIRCUMFERENCE = 2 * Math.PI * 9; // matches refresh-ring circle r="9"

  const refreshBar   = document.querySelector('#viewBrowser .browser-layout__refresh');
  const selectEl     = document.getElementById('browserAutoRefreshSelect');
  const ringEl       = refreshBar?.querySelector('.refresh-ring');
  const ringFill     = refreshBar?.querySelector('.refresh-ring__fill');
  const lastLabel    = document.getElementById('browserLastRefreshLabel');

  let _intervalSec = 0;   // 0 = off
  let _remaining   = 0;   // seconds left until next refresh
  let _tickTimer   = null;

  function _haltTimer() {
    if (_tickTimer) { clearInterval(_tickTimer); _tickTimer = null; }
  }

  // ── Countdown ring animation ─────────────────────────────────────────────
  function _setRing(fraction) {
    if (!ringFill) return;
    const offset = CIRCUMFERENCE * (1 - fraction);
    ringFill.style.strokeDashoffset = offset;
  }

  function _setActive(active) {
    if (ringEl) ringEl.classList.toggle('refresh-ring--active', active);
    if (!active) _setRing(0);
  }

  // ── Last-refreshed label (browser object list only — not the pager) ─────
  function _markRefreshed() {
    if (!lastLabel) return;
    const now = new Date();
    const hh  = String(now.getHours()).padStart(2, '0');
    const mm  = String(now.getMinutes()).padStart(2, '0');
    const ss  = String(now.getSeconds()).padStart(2, '0');
    lastLabel.textContent = `${hh}:${mm}:${ss}`;
    lastLabel.hidden = false;
  }

  // ── Trigger a refresh of the current view ────────────────────────────────
  // Auto-refresh applies while viewing objects inside a bucket (not on the bucket list).
  function _doRefresh() {
    if (AppState.currentView !== 'browser' || !AppState.currentBucket) return;
    window.BrowserView?.load(AppState.currentBucket, AppState.currentPrefix, true);
    _markRefreshed();
  }

  // ── Per-second tick ───────────────────────────────────────────────────────
  function _tick() {
    if (_intervalSec === 0) return;
    if (AppState.currentView !== 'browser') return;
    _remaining--;
    _setRing(_remaining / _intervalSec);

    if (_remaining <= 0) {
      _remaining = _intervalSec;
      _doRefresh();
    }
  }

  // ── Start / stop ──────────────────────────────────────────────────────────
  function _stop() {
    _haltTimer();
    _intervalSec = 0;
    _remaining   = 0;
    _setActive(false);
  }

  function _start(seconds) {
    _haltTimer();
    if (!seconds) return;
    _intervalSec = seconds;
    _remaining   = seconds;
    _setRing(1);   // full ring at start
    _setActive(true);
    _tickTimer = setInterval(_tick, 1000);
  }

  /** Pause the ticking interval when leaving the object browser (saves CPU). */
  function onViewHidden() {
    _haltTimer();
    _setActive(false);
    _setRing(0);
  }

  /** Resume countdown when entering the object browser if an interval is armed. */
  function onViewVisible() {
    if (_intervalSec <= 0 || _tickTimer) return;
    _remaining = _intervalSec;
    _setRing(1);
    _setActive(true);
    _tickTimer = setInterval(_tick, 1000);
  }

  // ── Public: called by BrowserView after a successful object list load ───
  function notifyRefreshed() {
    _markRefreshed();
    // Reset countdown so we always measure from the last *actual* refresh
    if (_intervalSec > 0) {
      _remaining = _intervalSec;
      _setRing(1);
    }
  }

  function _setSelectValues(val) {
    if (selectEl) selectEl.value = String(val);
  }

  // ── Restore persisted interval (timer starts only on the browser view) ───
  const saved = parseInt(localStorage.getItem('s3b-refresh') || '0', 10);
  if (saved) {
    _setSelectValues(saved);
    _intervalSec = saved;
    _remaining = saved;
  }

  // ── Dropdown change ─────────────────────────────────────────────────────
  if (selectEl) {
    selectEl.addEventListener('change', (e) => {
      const sec = parseInt(e.target.value, 10);
      _setSelectValues(sec);
      localStorage.setItem('s3b-refresh', String(sec));
      if (sec === 0) {
        _stop();
      } else {
        _start(sec);
        _doRefresh();
      }
    });
  }

  return { notifyRefreshed, stop: _stop, start: _start, onViewHidden, onViewVisible };
})();
