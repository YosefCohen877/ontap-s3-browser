/**
 * browser.js — Object/prefix browser view.
 * Handles: breadcrumb nav, sort, search, object list rendering, detail pane,
 *          multi-select with bulk delete/download.
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
  const bulkActions  = document.getElementById('bulkActions');
  const selectAllCb  = document.getElementById('selectAllCheckbox');
  const bulkSelLabel = document.getElementById('bulkSelectionLabel');
  const bulkDownloadBtn = document.getElementById('bulkDownloadBtn');
  const bulkDeleteBtn   = document.getElementById('bulkDeleteBtn');
  const bulkTagBtn      = document.getElementById('bulkTagBtn');
  const objectTagsModal = document.getElementById('objectTagsModal');
  const objectTagRows   = document.getElementById('objectTagRows');
  const objectTagAddRowBtn = document.getElementById('objectTagAddRowBtn');
  const objectTagsSaveBtn  = document.getElementById('objectTagsSaveBtn');

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
  const _selectedKeys = new Set();
  let _currentFileKeys = [];
  const FILE_COUNT_CACHE_KEY = 's3b-file-count-cache';

  let _detailBucket = '';
  let _detailKey = '';
  let _tagModalMode = null;
  let _tagModalReplaceBucket = '';
  let _tagModalReplaceKey = '';
  let _tagModalMergeBucket = '';
  let _tagModalMergeKeys = [];
  function _setPagerControls() {
    if (prevPageBtn) prevPageBtn.disabled = _tokenHistory.length === 0;
    if (nextPageBtn) nextPageBtn.disabled = !_nextToken;
    if (pageLabel) pageLabel.textContent = `Page ${_pageNumber}`;
  }


  // ── Selection helpers ────────────────────────────────────────────────────
  function _clearSelection() {
    _selectedKeys.clear();
    _syncSelectionUI();
  }

  function _syncSelectionUI() {
    const count = _selectedKeys.size;
    const fileCount = _currentFileKeys.length;

    if (bulkActions) bulkActions.hidden = count === 0;
    if (bulkDeleteBtn) bulkDeleteBtn.hidden = !window.ServerFeatures?.delete;
    if (bulkTagBtn) bulkTagBtn.hidden = !window.ServerFeatures?.object_tagging;

    if (bulkSelLabel) {
      bulkSelLabel.textContent = count === 0
        ? 'None selected'
        : `${count} file${count === 1 ? '' : 's'} selected`;
    }

    if (selectAllCb) {
      selectAllCb.checked = fileCount > 0 && count === fileCount;
      selectAllCb.indeterminate = count > 0 && count < fileCount;
    }

    objectList.querySelectorAll('.object-row[data-type="object"]').forEach(row => {
      const cb = row.querySelector('.object-row__check input');
      const isSelected = _selectedKeys.has(row.dataset.key);
      if (cb) cb.checked = isSelected;
      row.classList.toggle('multi-selected', isSelected);
    });
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
        <div style="width:16px;height:16px;border-radius:3px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="width:20px;height:20px;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="height:13px;width:60%;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="height:11px;width:50px;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
        <div style="height:11px;width:90px;border-radius:4px;background:var(--clr-surface-3);animation:pulse 1.5s infinite"></div>
      </div>
    `).join('');
  }

  // ── Object list ──────────────────────────────────────────────────────────
  function _renderItems(items) {
    _currentFileKeys = items.filter(i => i.type === 'object').map(i => i.key);
    _selectedKeys.clear();

    if (!items.length) {
      const msg = _search ? 'No objects match your search.' : 'This folder is empty.';
      objectList.innerHTML = `<div class="state-card">
        <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M4 3a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V5a2 2 0 00-2-2H4zm12 12H4l4-8 3 6 2-4 3 6z" clip-rule="evenodd"/></svg>
        ${msg}
      </div>`;
      _syncSelectionUI();
      return;
    }

    objectList.innerHTML = items.map(item => {
      const isFile = item.type === 'object';
      const checkboxHtml = isFile
        ? `<label class="object-row__check" aria-label="Select ${_esc(item.name)}">
             <input type="checkbox" tabindex="-1">
           </label>`
        : `<span class="object-row__check-placeholder"></span>`;

      return `
      <div class="object-row ${isFile ? 'object-row--selectable' : ''}" role="listitem" tabindex="0"
           data-type="${item.type}" data-key="${_esc(item.key)}" data-name="${_esc(item.name)}"
           aria-label="${isFile ? 'File' : 'Folder'}: ${_esc(item.name)}">
        ${checkboxHtml}
        <span class="object-row__icon ${item.type === 'prefix' ? 'folder' : 'file'}" aria-hidden="true">
          ${item.type === 'prefix' ? ICON_FOLDER : ICON_FILE}
        </span>
        <span class="object-row__name" title="${_esc(item.key)}">${_esc(item.name)}</span>
        <span class="object-row__size">${item.size !== null ? formatBytes(item.size) : ''}</span>
        <span class="object-row__date">${item.modified ? formatDate(item.modified) : ''}</span>
      </div>`;
    }).join('');

    objectList.querySelectorAll('.object-row').forEach(row => {
      const cb = row.querySelector('.object-row__check input');

      if (cb) {
        cb.addEventListener('click', (e) => e.stopPropagation());
        cb.addEventListener('change', () => {
          if (cb.checked) {
            _selectedKeys.add(row.dataset.key);
          } else {
            _selectedKeys.delete(row.dataset.key);
          }
          row.classList.toggle('multi-selected', cb.checked);
          _syncSelectionUI();
        });
      }

      const open = (e) => {
        if (e.target.closest('.object-row__check')) return;
        _handleRowClick(row);
      };
      row.addEventListener('click', open);
      row.addEventListener('keydown', e => { if (e.key === 'Enter' || e.key === ' ') open(e); });
    });

    _syncSelectionUI();
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

  function _fillDetailMeta(meta) {
    const host = document.getElementById('detailMeta');
    if (!host) return;
    const tagStr = _formatObjectTags(meta.tags);
    host.innerHTML = `
        ${_metaRow('Size',         formatBytes(meta.size))}
        ${_metaRow('Modified',     formatDate(meta.modified))}
        ${_metaRow('Content-Type', meta.content_type || '—')}
        ${_metaRow('ETag',         meta.etag || '—')}
        ${_metaRow('Tags',         tagStr)}
        ${meta.storage_class ? _metaRow('Storage Class', meta.storage_class) : ''}
        ${Object.entries(meta.user_metadata || {}).map(([k,v]) => _metaRow('x-amz-meta-'+k, v)).join('')}
      `;
  }

  function _clearTagModalError() {
    const el = document.getElementById('objectTagsError');
    if (el) { el.hidden = true; el.textContent = ''; }
  }

  function _setTagModalError(msg) {
    const el = document.getElementById('objectTagsError');
    if (el) { el.hidden = false; el.textContent = msg; }
  }

  function _closeObjectTagsModal() {
    if (!objectTagsModal) return;
    objectTagsModal.hidden = true;
    document.body.classList.remove('object-tag-modal-open');
    _tagModalMode = null;
  }

  function _addObjectTagRow(keyVal = '', valueVal = '') {
    if (!objectTagRows) return;
    const row = document.createElement('div');
    row.className = 'object-tag-row';
    row.innerHTML = `
      <input class="lc-input object-tag-row__key" type="text" maxlength="128" placeholder="Key" value="${_esc(keyVal)}" autocomplete="off" spellcheck="false" aria-label="Tag key" />
      <input class="lc-input object-tag-row__value" type="text" maxlength="256" placeholder="Value" value="${_esc(valueVal)}" autocomplete="off" spellcheck="false" aria-label="Tag value" />
      <button type="button" class="icon-btn object-tag-row__remove" title="Remove row" aria-label="Remove tag row">
        <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clip-rule="evenodd"/></svg>
      </button>`;
    objectTagRows.appendChild(row);
    row.querySelector('.object-tag-row__remove')?.addEventListener('click', () => {
      row.remove();
      if (!objectTagRows.querySelector('.object-tag-row')) {
        _addObjectTagRow('', '');
      }
    });
  }

  function _renderObjectTagRows(initial) {
    if (!objectTagRows) return;
    objectTagRows.innerHTML = '';
    const list = initial && initial.length
      ? initial.map(t => ({ key: t.key || '', value: t.value != null ? String(t.value) : '' }))
      : [{ key: '', value: '' }];
    list.forEach(t => _addObjectTagRow(t.key, t.value));
  }

  function _collectTagsFromModalRows() {
    if (!objectTagRows) return { tags: [], error: 'Missing form' };
    const merged = new Map();
    objectTagRows.querySelectorAll('.object-tag-row').forEach(row => {
      const kIn = row.querySelector('.object-tag-row__key');
      const vIn = row.querySelector('.object-tag-row__value');
      const k = (kIn?.value || '').trim();
      const v = (vIn?.value || '').trim();
      if (!k) return;
      merged.set(k, v);
    });
    const tags = [...merged.entries()].map(([key, value]) => ({ key, value }));
    if (tags.length > 10) return { tags: [], error: 'At most 10 tags per object.' };
    return { tags, error: null };
  }

  function _openObjectTagsModal(mode, opts) {
    if (!objectTagsModal) return;
    _tagModalMode = mode;
    _clearTagModalError();
    document.body.classList.add('object-tag-modal-open');
    objectTagsModal.hidden = false;

    const titleEl = document.getElementById('objectTagsTitle');
    const subEl = document.getElementById('objectTagsSubtitle');
    const hintEl = document.getElementById('objectTagsHint');

    if (mode === 'replace') {
      if (titleEl) titleEl.textContent = 'Edit tags';
      if (subEl) subEl.textContent = opts.name || opts.key;
      if (hintEl) {
        hintEl.textContent = 'Replace all tags on this object. Remove every row and save to clear all tags.';
      }
      _tagModalReplaceBucket = opts.bucket;
      _tagModalReplaceKey = opts.key;
      _renderObjectTagRows(opts.initialTags);
    } else {
      if (titleEl) titleEl.textContent = 'Add tags';
      const n = opts.keys.length;
      if (subEl) subEl.textContent = `${n} object${n === 1 ? '' : 's'} selected`;
      if (hintEl) {
        hintEl.textContent = 'These tags are merged into each selected object. Existing keys are overwritten. Some objects may fail if merging would exceed 10 tags.';
      }
      _tagModalMergeBucket = opts.bucket;
      _tagModalMergeKeys = opts.keys;
      _renderObjectTagRows(opts.initialTags);
    }
  }

  async function _showDetail(bucket, key, name) {
    detailPane?.classList.add('mobile-open');
    _detailBucket = bucket;
    _detailKey = key;

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
          ${window.ServerFeatures?.object_tagging ? `
          <button class="btn btn--ghost btn--sm" type="button" id="editObjectTagsBtn" title="Edit S3 object tags">
            <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M17.414 2.586a2 2 0 00-2.828 0L7 10.172V13h2.828l7.586-7.586a2 2 0 000-2.828zM4 16a1 1 0 001 1h2l8-8-3-3-8 8v2z" clip-rule="evenodd"/></svg>
            Edit tags
          </button>
          ` : ''}
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

    const editTagsBtn = document.getElementById('editObjectTagsBtn');
    if (editTagsBtn) {
      editTagsBtn.addEventListener('click', async () => {
        try {
          const meta = await API.objectMeta(bucket, key);
          _openObjectTagsModal('replace', {
            bucket,
            key,
            name,
            initialTags: meta.tags || [],
          });
        } catch (err) {
          Toast.error(`Could not load tags: ${err.message || 'Unknown error'}`);
        }
      });
    }

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
      _fillDetailMeta(meta);
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

  /** S3 object tags from /api/object/meta (key/value pairs) */
  function _formatObjectTags(tags) {
    if (!Array.isArray(tags) || tags.length === 0) return '—';
    return tags.map(t => `${t.key}=${t.value}`).join(', ');
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

    _detailBucket = '';
    _detailKey = '';

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

  // ── Bulk-action wiring ─────────────────────────────────────────────────
  selectAllCb?.addEventListener('change', () => {
    if (selectAllCb.checked) {
      _currentFileKeys.forEach(k => _selectedKeys.add(k));
    } else {
      _selectedKeys.clear();
    }
    _syncSelectionUI();
  });

  bulkDownloadBtn?.addEventListener('click', () => {
    if (!_selectedKeys.size) return;
    const keys = [..._selectedKeys];
    let idx = 0;
    function _downloadNext() {
      if (idx >= keys.length) return;
      const key = keys[idx++];
      const a = document.createElement('a');
      a.href = API.downloadUrl(_bucket, key);
      a.download = key.split('/').pop() || 'download';
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(_downloadNext, 500);
    }
    Toast.info(`Downloading ${keys.length} file${keys.length === 1 ? '' : 's'}…`);
    _downloadNext();
  });

  bulkDeleteBtn?.addEventListener('click', async () => {
    if (!_selectedKeys.size) return;
    const count = _selectedKeys.size;
    if (!confirm(`Are you sure you want to delete ${count} file${count === 1 ? '' : 's'}?`)) return;
    try {
      const result = await API.deleteObjectsBulk(_bucket, [..._selectedKeys]);
      if (result.errors?.length) {
        Toast.error(`Deleted ${result.deleted}, but ${result.errors.length} failed`);
      } else {
        Toast.success(`Deleted ${result.deleted} file${result.deleted === 1 ? '' : 's'}`);
      }
      _clearSelection();
      load(_bucket, _prefix, false);
    } catch (err) {
      Toast.error(`Bulk delete failed: ${err.message || 'Unknown error'}`);
    }
  });

  bulkTagBtn?.addEventListener('click', () => {
    if (!_selectedKeys.size || !window.ServerFeatures?.object_tagging) return;
    _openObjectTagsModal('merge', {
      bucket: _bucket,
      keys: [..._selectedKeys],
      initialTags: [],
    });
  });

  objectTagAddRowBtn?.addEventListener('click', () => {
    const n = objectTagRows?.querySelectorAll('.object-tag-row').length || 0;
    if (n >= 10) {
      Toast.info('At most 10 tags per request.');
      return;
    }
    _addObjectTagRow('', '');
  });

  objectTagsModal?.querySelectorAll('[data-ot-close]').forEach((el) => {
    el.addEventListener('click', _closeObjectTagsModal);
  });

  objectTagsSaveBtn?.addEventListener('click', async () => {
    _clearTagModalError();
    const { tags, error } = _collectTagsFromModalRows();
    if (error) {
      _setTagModalError(error);
      return;
    }

    if (_tagModalMode === 'replace') {
      Loading.show();
      try {
        await API.putObjectTags(_tagModalReplaceBucket, _tagModalReplaceKey, tags);
        Toast.success(tags.length ? 'Tags saved' : 'All tags cleared');
        _closeObjectTagsModal();
        if (_detailBucket === _tagModalReplaceBucket && _detailKey === _tagModalReplaceKey) {
          try {
            const meta = await API.objectMeta(_detailBucket, _detailKey);
            _fillDetailMeta(meta);
          } catch { /* ignore */ }
        }
      } catch (err) {
        _setTagModalError(err.message || 'Request failed');
      } finally {
        Loading.hide();
      }
      return;
    }

    if (_tagModalMode === 'merge') {
      if (!tags.length) {
        _setTagModalError('Add at least one tag with a key.');
        return;
      }
      const mergeKeys = [..._tagModalMergeKeys];
      const mergeBucket = _tagModalMergeBucket;
      Loading.show();
      try {
        const result = await API.mergeObjectTagsBulk(mergeBucket, mergeKeys, tags);
        if (result.errors?.length) {
          Toast.error(`Updated ${result.updated}; ${result.errors.length} object(s) failed`);
        } else {
          Toast.success(`Updated tags on ${result.updated} object${result.updated === 1 ? '' : 's'}`);
        }
        _closeObjectTagsModal();
        if (_detailBucket === mergeBucket && mergeKeys.includes(_detailKey)) {
          try {
            const meta = await API.objectMeta(_detailBucket, _detailKey);
            _fillDetailMeta(meta);
          } catch { /* ignore */ }
        }
      } catch (err) {
        _setTagModalError(err.message || 'Request failed');
      } finally {
        Loading.hide();
      }
    }
  });

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
