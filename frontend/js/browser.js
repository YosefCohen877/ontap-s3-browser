/**
 * browser.js — Object/prefix browser view.
 * Handles: breadcrumb nav, sort, search, object list rendering, detail pane.
 */

window.BrowserView = (() => {
  // DOM refs
  const objectList   = document.getElementById('objectList');
  const breadcrumb   = document.getElementById('breadcrumb');
  const detailContent = document.getElementById('detailContent');
  const detailPane   = document.getElementById('detailPane');
  const searchInput  = document.getElementById('searchInput');
  const objectCountLabel = document.getElementById('objectCountLabel');
  const sortSelect   = document.getElementById('sortSelect');
  const sortOrderBtn = document.getElementById('sortOrderBtn');
  const pageSizeSelect = document.getElementById('pageSizeSelect');
  const prevPageBtn = document.getElementById('prevPageBtn');
  const nextPageBtn = document.getElementById('nextPageBtn');
  const pageLabel = document.getElementById('pageLabel');
  const refreshBrowser = document.getElementById('refreshBrowserBtn');
  const uploadWrap   = document.getElementById('uploadWrap');
  const uploadInput  = document.getElementById('uploadInput');
  const uploadBtn    = document.getElementById('uploadBtn');

  let _bucket  = '';
  let _prefix  = '';
  let _sort    = 'modified';
  let _order   = 'desc';
  let _search  = '';
  let _searchTimer = null;
  let _pageSize = 20;
  let _nextToken = null;
  let _tokenHistory = [];
  let _pageNumber = 1;
  const FILE_COUNT_CACHE_KEY = 's3b-file-count-cache';
  function _setPagerControls() {
    if (prevPageBtn) prevPageBtn.disabled = _tokenHistory.length === 0;
    if (nextPageBtn) nextPageBtn.disabled = !_nextToken;
    if (pageLabel) pageLabel.textContent = `Page ${_pageNumber}`;
  }


  // ── Icons ────────────────────────────────────────────────────────────────
  const ICON_FOLDER = `<svg viewBox="0 0 20 20" fill="currentColor"><path d="M2 6a2 2 0 012-2h5l2 2h5a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z"/></svg>`;
  const ICON_FILE   = `<svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4 4a2 2 0 012-2h4.586A2 2 0 0112 2.586L15.414 6A2 2 0 0116 7.414V16a2 2 0 01-2 2H6a2 2 0 01-2-2V4zm2 6a1 1 0 011-1h6a1 1 0 110 2H7a1 1 0 01-1-1zm1 3a1 1 0 000 2h6a1 1 0 000-2H7z" clip-rule="evenodd"/></svg>`;

  function _esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function _countCacheId(bucket, prefix, search) {
    return `${bucket}|${prefix || ''}|${search || ''}`;
  }

  function _readCountCache() {
    try {
      return JSON.parse(localStorage.getItem(FILE_COUNT_CACHE_KEY) || '{}');
    } catch {
      return {};
    }
  }

  function _writeCountCache(cache) {
    try {
      localStorage.setItem(FILE_COUNT_CACHE_KEY, JSON.stringify(cache));
    } catch {
      // Ignore storage quota/privacy errors.
    }
  }

  // ── Breadcrumb ───────────────────────────────────────────────────────────
  function _renderBreadcrumb(bucket, prefix) {
    const parts = prefix ? prefix.split('/').filter(Boolean) : [];
    let html = `<button class="breadcrumb__item" data-prefix="" title="All buckets">🗂 Buckets</button>`;
    html += `<span class="breadcrumb__sep">›</span>`;
    html += `<button class="breadcrumb__item" data-prefix="" data-bucket="${_esc(bucket)}" title="Root of ${bucket}">
      ${_esc(bucket)}
    </button>`;

    let accumulated = '';
    parts.forEach((part, idx) => {
      accumulated += part + '/';
      const isLast = idx === parts.length - 1;
      html += `<span class="breadcrumb__sep">›</span>`;
      html += `<button class="breadcrumb__item ${isLast ? 'active' : ''}"
        data-prefix="${_esc(accumulated)}"
        data-bucket="${_esc(bucket)}">${_esc(part)}</button>`;
    });

    breadcrumb.innerHTML = html;

    // Attach events
    breadcrumb.querySelectorAll('.breadcrumb__item').forEach(btn => {
      btn.addEventListener('click', () => {
        if (btn.classList.contains('active')) return;
        if (!btn.dataset.bucket) {
          Nav.toBuckets();
        } else {
          Nav.toBrowser(btn.dataset.bucket, btn.dataset.prefix);
        }
      });
    });
  }

  // ── Skeleton ─────────────────────────────────────────────────────────────
  function _renderSkeleton() {
    objectList.innerHTML = Array(8).fill(0).map(() => `
      <div class="object-row" style="pointer-events:none">
        <div style="width:20px;height:20px;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="height:13px;width:60%;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="height:11px;width:50px;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="height:11px;width:90px;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
      </div>
    `).join('');
  }

  // ── Object list ──────────────────────────────────────────────────────────
  function _renderItems(items) {
    if (!items.length) {
      const msg = _search ? 'No objects match your search.' : 'This folder is empty.';
      objectList.innerHTML = `<div class="state-card">
        <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M4 3a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V5a2 2 0 00-2-2H4zm12 12H4l4-8 3 6 2-4 3 6z" clip-rule="evenodd"/></svg>
        ${msg}
      </div>`;
      return;
    }

    objectList.innerHTML = items.map(item => `
      <div class="object-row" role="listitem" tabindex="0"
           data-type="${item.type}" data-key="${_esc(item.key)}" data-name="${_esc(item.name)}"
           aria-label="${item.type === 'prefix' ? 'Folder' : 'File'}: ${_esc(item.name)}">
        <span class="object-row__icon ${item.type === 'prefix' ? 'folder' : 'file'}" aria-hidden="true">
          ${item.type === 'prefix' ? ICON_FOLDER : ICON_FILE}
        </span>
        <span class="object-row__name" title="${_esc(item.key)}">${_esc(item.name)}</span>
        <span class="object-row__size">${item.size !== null ? formatBytes(item.size) : ''}</span>
        <span class="object-row__date">${item.modified ? formatDate(item.modified) : ''}</span>
      </div>
    `).join('');

    objectList.querySelectorAll('.object-row').forEach(row => {
      const open = () => _handleRowClick(row);
      row.addEventListener('click', open);
      row.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') open(); });
    });
  }

  function _handleRowClick(row) {
    // Deselect previous
    objectList.querySelectorAll('.object-row.selected').forEach(r => r.classList.remove('selected'));
    row.classList.add('selected');

    if (row.dataset.type === 'prefix') {
      // Navigate into folder
      Nav.toBrowser(_bucket, row.dataset.key);
    } else {
      // Show detail pane for object
      _showDetail(_bucket, row.dataset.key, row.dataset.name);
    }
  }

  // ── Detail pane ──────────────────────────────────────────────────────────
  function _closeMobileDetail() {
    detailPane?.classList.remove('mobile-open');
  }

  async function _showDetail(bucket, key, name) {
    detailPane?.classList.add('mobile-open');

    detailContent.innerHTML = `
      <div class="detail-header">
        <div class="detail-header__top">
          <div class="detail-header__name">${_esc(name)}</div>
          <button class="detail-header__close" id="detailCloseBtn" aria-label="Close details">
            <svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clip-rule="evenodd"/></svg>
          </button>
        </div>
        <div class="detail-header__actions">
          <a class="btn btn--primary btn--sm"
             href="${API.downloadUrl(bucket, key)}"
             download="${_esc(name)}">
            <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M3 17a1 1 0 011-1h12a1 1 0 110 2H4a1 1 0 01-1-1zm3.293-7.707a1 1 0 011.414 0L9 10.586V3a1 1 0 112 0v7.586l1.293-1.293a1 1 0 111.414 1.414l-3 3a1 1 0 01-1.414 0l-3-3a1 1 0 010-1.414z" clip-rule="evenodd"/></svg>
            Download
          </a>
          ${window.ServerFeatures?.delete ? `
          <button class="btn btn--danger btn--sm" id="deleteObjectBtn">
             <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd"/></svg>
             Delete
          </button>
          ` : ''}
        </div>
      </div>
      <div class="detail-meta" id="detailMeta">
        <div class="state-card" style="padding:1rem">
          <div class="spinner" style="width:24px;height:24px;border-width:2px"></div>
        </div>
      </div>
      <div class="detail-preview" id="detailPreview"></div>`;

    document.getElementById('detailCloseBtn')?.addEventListener('click', _closeMobileDetail);

    const deleteBtn = document.getElementById('deleteObjectBtn');
    if (deleteBtn) {
      deleteBtn.addEventListener('click', async () => {
        if (!confirm(`Are you sure you want to delete ${name}?`)) return;
        try {
          await API.deleteObject(bucket, key);
          Toast.success(`Deleted ${name}`);
          detailContent.innerHTML = `<div class="detail-empty"><p>Object deleted</p></div>`;
          Nav.toBrowser(_bucket, _prefix, false); // Refresh without pushing history
        } catch (err) {
          Toast.error(`Failed to delete ${name}: ${err.message || 'Unknown error'}`);
        }
      });
    }

    // Load metadata
    try {
      const meta = await API.objectMeta(bucket, key);
      document.getElementById('detailMeta').innerHTML = `
        ${_metaRow('Size',         formatBytes(meta.size))}
        ${_metaRow('Modified',     formatDate(meta.modified))}
        ${_metaRow('Content-Type', meta.content_type || '—')}
        ${_metaRow('ETag',         meta.etag || '—')}
        ${meta.storage_class ? _metaRow('Storage Class', meta.storage_class) : ''}
        ${Object.entries(meta.user_metadata || {}).map(([k,v]) => _metaRow('x-amz-meta-'+k, v)).join('')}
      `;
    } catch (err) {
      document.getElementById('detailMeta').innerHTML = `
        <div class="text-sm text-muted" style="padding:.5rem">Could not load metadata: ${_esc(err.message)}</div>`;
    }

    // Load preview
    window.PreviewView?.loadPreview(bucket, key, document.getElementById('detailPreview'));
  }

  function _metaRow(key, value) {
    return `<div class="meta-row">
      <span class="meta-row__key">${_esc(key)}</span>
      <span class="meta-row__value">${_esc(String(value))}</span>
    </div>`;
  }

  // ── load ─────────────────────────────────────────────────────────────────
  async function load(bucket, prefix = '', resetPage = true) {
    const changedPath = _bucket !== bucket || _prefix !== prefix;
    _bucket = bucket;
    _prefix = prefix;
    if (changedPath || resetPage) {
      _nextToken = null;
      _tokenHistory = [];
      _pageNumber = 1;
      _setPagerControls();
    }
    _renderBreadcrumb(bucket, prefix);
    _renderSkeleton();

    if (uploadWrap) {
      uploadWrap.style.display = window.ServerFeatures?.upload ? '' : 'none';
    }

    // Reset detail pane
    detailContent.innerHTML = `<div class="detail-empty">
      <svg viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
        <rect x="8" y="6" width="24" height="30" rx="2" stroke="currentColor" stroke-width="2"/>
        <path d="M32 6l8 8v28a2 2 0 01-2 2H14" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
        <path d="M32 6v8h8" stroke="currentColor" stroke-width="2"/>
        <path d="M16 20h12M16 26h8" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
      </svg>
      <p>Select an object to view details</p>
    </div>`;

    try {
      if (objectCountLabel) {
        const countCache = _readCountCache();
        const cacheId = _countCacheId(bucket, prefix, _search);
        const cachedCount = countCache[cacheId];
        objectCountLabel.textContent = Number.isFinite(cachedCount)
          ? `${cachedCount.toLocaleString()} file${cachedCount === 1 ? '' : 's'} (cached)`
          : 'Loading…';
      }
      const tokenForRequest = _tokenHistory.length ? _tokenHistory[_tokenHistory.length - 1] : '';
      const data = await API.listObjects(bucket, prefix, _search, _sort, _order, _pageSize, tokenForRequest);
      _renderItems(data.items);
      _nextToken = data.next_token || null;
      _setPagerControls();
      if (objectCountLabel) {
        const fileCount = data.items.filter(item => item.type === 'object').length;
        const countCache = _readCountCache();
        countCache[_countCacheId(bucket, prefix, _search)] = fileCount;
        _writeCountCache(countCache);
        const fileLabel = `${fileCount.toLocaleString()} file${fileCount === 1 ? '' : 's'}`;
        objectCountLabel.textContent = _search
          ? `${fileLabel} matching "${_search}" (page size ${_pageSize})`
          : `${fileLabel} (page size ${_pageSize})`;
      }
      document.getElementById('endpointLabel').textContent =
        `${bucket}${prefix ? ' / ' + prefix : ''}`;
      window.AutoRefresh?.notifyRefreshed();
    } catch (err) {
      if (objectCountLabel) {
        objectCountLabel.textContent = 'Count unavailable';
      }
      renderError(err, objectList);
      Toast.error(err.title || 'Failed to load objects');
    }
  }

  // ── Sort & Search wiring ─────────────────────────────────────────────────
  sortSelect.addEventListener('change', () => {
    _sort = sortSelect.value;
    load(_bucket, _prefix, true);
  });

  sortOrderBtn.addEventListener('click', () => {
    _order = _order === 'asc' ? 'desc' : 'asc';
    sortOrderBtn.dataset.order = _order;
    sortOrderBtn.querySelector('.icon-sort-asc').style.display = _order === 'asc' ? '' : 'none';
    sortOrderBtn.querySelector('.icon-sort-desc').style.display = _order === 'desc' ? '' : 'none';
    load(_bucket, _prefix, true);
  });

  searchInput.addEventListener('input', () => {
    clearTimeout(_searchTimer);
    _searchTimer = setTimeout(() => {
      _search = searchInput.value.trim();
      load(_bucket, _prefix, true);
    }, 350);
  });

  pageSizeSelect?.addEventListener('change', () => {
    _pageSize = parseInt(pageSizeSelect.value, 10) || 20;
    load(_bucket, _prefix, true);
  });

  prevPageBtn?.addEventListener('click', () => {
    if (!_tokenHistory.length) return;
    _tokenHistory.pop();
    _pageNumber = Math.max(1, _pageNumber - 1);
    load(_bucket, _prefix, false);
  });

  nextPageBtn?.addEventListener('click', () => {
    if (!_nextToken) return;
    _tokenHistory.push(_nextToken);
    _pageNumber += 1;
    load(_bucket, _prefix, false);
  });

  refreshBrowser?.addEventListener('click', () => load(_bucket, _prefix, true));

  // ── Upload wiring ────────────────────────────────────────────────────────
  async function _uploadFiles(fileList) {
    if (!fileList || !fileList.length) return;
    for (let i = 0; i < fileList.length; i++) {
      const file = fileList[i];
      Toast.info(`Uploading ${file.name}...`);
      try {
        await API.uploadObject(_bucket, _prefix, file);
        Toast.success(`Uploaded ${file.name}`);
      } catch (err) {
        Toast.error(`Failed to upload ${file.name}: ${err.message || 'Unknown error'}`);
      }
    }
    Nav.toBrowser(_bucket, _prefix, false);
  }

  uploadBtn?.addEventListener('click', () => uploadInput?.click());

  uploadInput?.addEventListener('change', async (e) => {
    await _uploadFiles(e.target.files);
    uploadInput.value = '';
  });

  // ── Drag-and-drop upload ────────────────────────────────────────────────
  if (objectList) {
    let _dragCounter = 0;

    objectList.addEventListener('dragenter', (e) => {
      if (!window.ServerFeatures?.upload) return;
      e.preventDefault();
      _dragCounter++;
      objectList.classList.add('drop-active');
    });

    objectList.addEventListener('dragleave', () => {
      if (!window.ServerFeatures?.upload) return;
      _dragCounter--;
      if (_dragCounter <= 0) {
        _dragCounter = 0;
        objectList.classList.remove('drop-active');
      }
    });

    objectList.addEventListener('dragover', (e) => {
      if (!window.ServerFeatures?.upload) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
    });

    objectList.addEventListener('drop', async (e) => {
      if (!window.ServerFeatures?.upload) return;
      e.preventDefault();
      _dragCounter = 0;
      objectList.classList.remove('drop-active');
      await _uploadFiles(e.dataTransfer.files);
    });
  }

  if (sortSelect) sortSelect.value = _sort;
  if (pageSizeSelect) pageSizeSelect.value = String(_pageSize);
  if (sortOrderBtn) {
    sortOrderBtn.dataset.order = _order;
    sortOrderBtn.querySelector('.icon-sort-asc').style.display = _order === 'asc' ? '' : 'none';
    sortOrderBtn.querySelector('.icon-sort-desc').style.display = _order === 'desc' ? '' : 'none';
  }
  _setPagerControls();

  return { load };
})();
