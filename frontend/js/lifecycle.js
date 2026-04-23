/**
 * lifecycle.js — Bucket Lifecycle Configuration modal.
 *
 * Flow:
 *  - open(bucket) loads current rules via GET /api/bucket/{b}/lifecycle.
 *  - On success: render rules table + Add/Edit/Delete controls.
 *  - On 501 lifecycle_not_supported: render a version-matrix banner and
 *    hide mutation controls. The same UI stays useful once ONTAP is upgraded.
 *  - Mutations send the FULL rules array via PUT (S3 replace-all semantics).
 */

window.LifecycleModal = (() => {
  const $modal       = document.getElementById('lifecycleModal');
  const $edit        = document.getElementById('lifecycleEditModal');
  const $title       = document.getElementById('lcRulesTitle');
  const $subtitle    = document.getElementById('lcRulesSubtitle');
  const $banner      = document.getElementById('lcBanner');
  const $toolbar     = document.getElementById('lcRulesToolbar');
  const $tbody       = document.getElementById('lcRulesTbody');
  const $tableWrap   = document.getElementById('lcRulesTableWrap');
  const $empty       = document.getElementById('lcRulesEmpty');
  const $addBtn      = document.getElementById('lcAddRuleBtn');
  const $delAllBtn   = document.getElementById('lcDeleteAllBtn');
  const $reloadBtn   = document.getElementById('lcReloadBtn');

  // Edit form refs
  const $editTitle  = document.getElementById('lcEditTitle');
  const $ruleId     = document.getElementById('lcRuleId');
  const $ruleStatus = document.getElementById('lcRuleStatus');
  const $filtPrefix = document.getElementById('lcFilterPrefix');
  const $filtTags   = document.getElementById('lcFilterTags');
  const $addTagBtn  = document.getElementById('lcAddTagBtn');
  const $sizeGt     = document.getElementById('lcFilterSizeGt');
  const $sizeLt     = document.getElementById('lcFilterSizeLt');
  const $expDays    = document.getElementById('lcExpDays');
  const $expDate    = document.getElementById('lcExpDate');
  const $ncDays     = document.getElementById('lcNcDays');
  const $ncKeep     = document.getElementById('lcNcKeep');
  const $abortDays  = document.getElementById('lcAbortDays');
  const $expWarn    = document.getElementById('lcExpWarn');
  const $saveBtn    = document.getElementById('lcSaveRuleBtn');
  const $editError  = document.getElementById('lcEditError');
  const $previewBody = document.getElementById('lcRulePreviewBody');

  let _bucket = null;
  let _rules = [];        // normalized shape from backend
  let _editingIndex = -1; // -1 = new rule
  let _supported = true;

  function esc(str) {
    return String(str ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  // ── Version matrix banner (shown when ONTAP < 9.13.1 or other error) ─────
  function _renderNotSupportedBanner(err) {
    _supported = false;
    $banner.hidden = false;
    $banner.className = 'lc-banner lc-banner--warn';
    $banner.innerHTML = `
      <div class="lc-banner__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z"/>
        </svg>
      </div>
      <div class="lc-banner__body">
        <strong>Bucket lifecycle is not supported on this ONTAP version.</strong>
        <p>${esc(err.message || 'ONTAP returned NotImplemented for this request.')}</p>
        <table class="lc-version-matrix">
          <thead><tr><th>ONTAP version</th><th>Lifecycle management</th><th>Notes</th></tr></thead>
          <tbody>
            <tr><td>9.8 &ndash; 9.10.1</td><td><span class="lc-pill lc-pill--no">Not supported</span></td><td>No S3 lifecycle support at all</td></tr>
            <tr><td>9.11.1 / 9.12.1</td><td><span class="lc-pill lc-pill--no">Not supported</span></td><td>S3 API for buckets/objects exists but lifecycle rules are not yet available (neither via S3 API nor ONTAP REST)</td></tr>
            <tr><td>9.13.1+</td><td><span class="lc-pill lc-pill--yes">Supported</span></td><td>Full S3 API CRUD (expiration actions only)</td></tr>
            <tr><td>9.14.1+</td><td><span class="lc-pill lc-pill--yes">Supported</span></td><td>Also manageable via ONTAP System Manager</td></tr>
          </tbody>
        </table>
        <p class="lc-hint">Transitions to STANDARD_IA / GLACIER / DEEP_ARCHIVE are AWS-specific storage classes and are <strong>not supported by ONTAP at any version</strong>.</p>
      </div>`;
    $toolbar.hidden = true;
    $tableWrap.hidden = true;
  }

  function _renderGenericError(err) {
    $banner.hidden = false;
    $banner.className = 'lc-banner lc-banner--error';
    $banner.innerHTML = `
      <div class="lc-banner__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z"/>
        </svg>
      </div>
      <div class="lc-banner__body">
        <strong>${esc(err.title || 'Failed to load lifecycle rules')}</strong>
        <p>${esc(err.message || '')}</p>
        ${err.detail ? `<details><summary>Technical detail</summary><pre>${esc(err.detail)}</pre></details>` : ''}
      </div>`;
    $toolbar.hidden = true;
    $tableWrap.hidden = true;
  }

  function _renderRules() {
    $tbody.innerHTML = '';
    if (!_rules.length) {
      $empty.hidden = false;
      return;
    }
    $empty.hidden = true;

    const canMutate = !!window.ServerFeatures?.bucket_lifecycle && _supported;

    for (let i = 0; i < _rules.length; i++) {
      const r = _rules[i];
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td class="lc-col-id">${esc(r.id)}</td>
        <td><span class="lc-pill ${r.status === 'Enabled' ? 'lc-pill--yes' : 'lc-pill--off'}">${esc(r.status)}</span></td>
        <td class="lc-col-filter">${_filterSummary(r.filter)}</td>
        <td class="lc-col-actions">${_actionsSummary(r)}</td>
        <td class="lc-col-ops">
          <button class="icon-btn lc-row-btn" data-lc-edit="${i}" title="${canMutate ? 'Edit rule' : 'Edit (requires ENABLE_BUCKET_LIFECYCLE)'}" ${canMutate ? '' : 'disabled'}>
            <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
              <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z"/>
            </svg>
          </button>
          <button class="icon-btn lc-row-btn lc-row-btn--danger" data-lc-del="${i}" title="${canMutate ? 'Delete rule' : 'Delete (requires ENABLE_BUCKET_LIFECYCLE)'}" ${canMutate ? '' : 'disabled'}>
            <svg viewBox="0 0 20 20" fill="currentColor" aria-hidden="true">
              <path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd"/>
            </svg>
          </button>
        </td>`;
      $tbody.appendChild(tr);
    }

    $tbody.querySelectorAll('[data-lc-edit]').forEach(b => {
      b.addEventListener('click', () => _openEdit(parseInt(b.dataset.lcEdit, 10)));
    });
    $tbody.querySelectorAll('[data-lc-del]').forEach(b => {
      b.addEventListener('click', () => _deleteRule(parseInt(b.dataset.lcDel, 10)));
    });
  }

  function _filterSummary(f) {
    if (!f) return '<span class="lc-dim">All objects</span>';
    const parts = [];
    if (f.prefix) parts.push(`prefix: <code>${esc(f.prefix)}</code>`);
    if (Array.isArray(f.tags) && f.tags.length) {
      parts.push('tags: ' + f.tags.map(t => `<code>${esc(t.key)}=${esc(t.value)}</code>`).join(', '));
    }
    if (f.size_greater_than != null) parts.push(`size &gt; ${esc(f.size_greater_than)}`);
    if (f.size_less_than != null)    parts.push(`size &lt; ${esc(f.size_less_than)}`);
    return parts.length ? parts.join('<br>') : '<span class="lc-dim">All objects</span>';
  }

  function _actionsSummary(r) {
    const parts = [];
    if (r.expiration) {
      if (r.expiration.days != null) parts.push(`Expire after ${r.expiration.days} day(s)`);
      if (r.expiration.date) parts.push(`Expire on ${esc(r.expiration.date)}`);
      if (r.expiration.expired_object_delete_marker) parts.push('Remove expired delete markers');
    }
    if (r.noncurrent_version_expiration) {
      const n = r.noncurrent_version_expiration;
      const keep = n.newer_noncurrent_versions != null ? `, keep newest ${n.newer_noncurrent_versions}` : '';
      if (n.noncurrent_days != null) parts.push(`Noncurrent: delete after ${n.noncurrent_days} day(s)${keep}`);
    }
    if (r.abort_incomplete_multipart_upload && r.abort_incomplete_multipart_upload.days_after_initiation != null) {
      parts.push(`Abort MPU after ${r.abort_incomplete_multipart_upload.days_after_initiation} day(s)`);
    }
    if (r._has_unsupported_transitions) {
      parts.push('<span class="lc-warn-text">(has storage-class transitions — not supported by ONTAP)</span>');
    }
    return parts.length ? parts.join('<br>') : '<span class="lc-dim">No actions</span>';
  }

  // ── Modal show/hide ──────────────────────────────────────────────────────
  function _showModal() { $modal.hidden = false; document.body.classList.add('lc-modal-open'); }
  function _hideModal() { $modal.hidden = true; document.body.classList.remove('lc-modal-open'); _hideEdit(); }
  function _showEdit() { $edit.hidden = false; }
  function _hideEdit() { $edit.hidden = true; }

  $modal.querySelectorAll('[data-lc-close]').forEach(el => el.addEventListener('click', _hideModal));
  $edit.querySelectorAll('[data-lc-edit-close]').forEach(el => el.addEventListener('click', _hideEdit));
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      if (!$edit.hidden) _hideEdit();
      else if (!$modal.hidden) _hideModal();
    }
  });

  // ── Tabs ─────────────────────────────────────────────────────────────────
  $edit.querySelectorAll('.lc-tab').forEach(t => {
    t.addEventListener('click', () => {
      $edit.querySelectorAll('.lc-tab').forEach(x => {
        x.classList.remove('lc-tab--active');
        x.setAttribute('aria-selected', 'false');
      });
      t.classList.add('lc-tab--active');
      t.setAttribute('aria-selected', 'true');
      const name = t.dataset.lcTab;
      $edit.querySelectorAll('.lc-tab-panel').forEach(p => {
        const match = p.dataset.lcPanel === name;
        p.hidden = !match;
        p.classList.toggle('lc-tab-panel--active', match);
      });
    });
  });

  let _previewTimer = null;
  function _schedulePreviewUpdate() {
    if (!$previewBody) return;
    clearTimeout(_previewTimer);
    _previewTimer = setTimeout(_updateRulePreview, 60);
  }

  function _updateRulePreview() {
    if (!$previewBody) return;
    const id = ($ruleId?.value || '').trim();
    if (!id) {
      $previewBody.className = 'lc-rule-preview__body lc-rule-preview__body--pending';
      $previewBody.innerHTML = 'Enter a <strong>Rule ID</strong> above to preview this rule.';
      return;
    }

    try {
      const rule = _collectRule();
      $previewBody.className = 'lc-rule-preview__body lc-rule-preview__body--ok';
      const scopeHtml = _filterSummary(rule.filter);
      const actionsHtml = _actionsSummary(rule);
      $previewBody.innerHTML = `
        <div class="lc-preview-line"><span class="lc-preview-k">Applies to</span> ${scopeHtml}</div>
        <div class="lc-preview-line"><span class="lc-preview-k">Will</span> ${actionsHtml}</div>`;
    } catch (e) {
      $previewBody.className = 'lc-rule-preview__body lc-rule-preview__body--warn';
      const msg = e.message || 'Invalid rule';
      $previewBody.innerHTML = `<span class="lc-preview-issue">${esc(msg)}</span>`;
    }
  }

  $edit.addEventListener('input', _schedulePreviewUpdate);
  $edit.addEventListener('change', _schedulePreviewUpdate);

  // ── Load rules ───────────────────────────────────────────────────────────
  async function _load() {
    $banner.hidden = true;
    $toolbar.hidden = false;
    $tableWrap.hidden = false;
    $tbody.innerHTML = '<tr><td colspan="5" class="lc-loading"><div class="spinner"></div> Loading lifecycle rules…</td></tr>';
    $empty.hidden = true;
    _supported = true;

    try {
      const data = await API.getLifecycle(_bucket);
      _rules = data.rules || [];
      const canMutate = !!window.ServerFeatures?.bucket_lifecycle;
      if (!canMutate) {
        $banner.hidden = false;
        $banner.className = 'lc-banner lc-banner--info';
        $banner.innerHTML = `
          <div class="lc-banner__icon" aria-hidden="true">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path stroke-linecap="round" stroke-linejoin="round" d="M11.25 11.25l.041-.02a.75.75 0 011.063.852l-.708 2.836a.75.75 0 001.063.853l.041-.021M21 12a9 9 0 11-18 0 9 9 0 0118 0zm-9-3.75h.008v.008H12V8.25z"/></svg>
          </div>
          <div class="lc-banner__body">
            <strong>Read-only mode</strong>
            <p>Bucket lifecycle mutations are disabled. Set <code>ENABLE_BUCKET_LIFECYCLE=true</code> in the deployment environment to allow Add / Edit / Delete.</p>
          </div>`;
      }
      $addBtn.disabled = !canMutate;
      $delAllBtn.disabled = !canMutate || _rules.length === 0;
      _renderRules();
    } catch (err) {
      if (err.category === 'lifecycle_not_supported') {
        _renderNotSupportedBanner(err);
      } else {
        _renderGenericError(err);
      }
    }
  }

  // ── Open Add/Edit form ──────────────────────────────────────────────────
  function _clearTags() { $filtTags.innerHTML = ''; }
  function _addTagRow(key = '', value = '') {
    const row = document.createElement('div');
    row.className = 'lc-tag-row';
    row.innerHTML = `
      <input class="lc-input lc-input--tag" type="text" placeholder="Key" value="${esc(key)}" />
      <input class="lc-input lc-input--tag" type="text" placeholder="Value" value="${esc(value)}" />
      <button class="icon-btn lc-row-btn lc-row-btn--danger" type="button" aria-label="Remove tag">
        <svg viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clip-rule="evenodd"/></svg>
      </button>`;
    row.querySelector('button').addEventListener('click', () => {
      row.remove();
      _schedulePreviewUpdate();
    });
    $filtTags.appendChild(row);
  }

  $addTagBtn.addEventListener('click', () => {
    _addTagRow();
    _schedulePreviewUpdate();
  });

  function _clearEditError() {
    if (!$editError) return;
    $editError.hidden = true;
    $editError.textContent = '';
  }
  function _showEditError(msg) {
    if (!$editError) {
      Toast.error(msg);
      return;
    }
    $editError.textContent = msg;
    $editError.hidden = false;
  }

  function _resetEditForm() {
    _clearEditError();
    $ruleId.value = '';
    $ruleStatus.value = 'Enabled';
    $filtPrefix.value = '';
    _clearTags();
    $sizeGt.value = '';
    $sizeLt.value = '';
    const noneRadio = $edit.querySelector('input[name="lcExpMode"][value="none"]');
    if (noneRadio) noneRadio.checked = true;
    $expDays.value = '';
    $expDate.value = '';
    $ncDays.value = '';
    $ncKeep.value = '';
    $abortDays.value = '';
    $expWarn.hidden = true;
    $expWarn.textContent = '';
    $edit.querySelector('.lc-tab[data-lc-tab="actions"]')?.click();
  }

  function _openEdit(index) {
    _editingIndex = index;
    _resetEditForm();
    if (index === -1) {
      $editTitle.textContent = 'Add Lifecycle Rule';
      $ruleId.value = `rule-${Date.now().toString(36)}`;
    } else {
      const r = _rules[index];
      $editTitle.textContent = `Edit Rule: ${r.id}`;
      $ruleId.value = r.id || '';
      $ruleStatus.value = r.status || 'Enabled';
      if (r.filter) {
        $filtPrefix.value = r.filter.prefix || '';
        (r.filter.tags || []).forEach(t => _addTagRow(t.key, t.value));
        if (r.filter.size_greater_than != null) $sizeGt.value = r.filter.size_greater_than;
        if (r.filter.size_less_than != null)    $sizeLt.value = r.filter.size_less_than;
      }
      if (r.expiration) {
        if (r.expiration.days != null) {
          const el = $edit.querySelector('input[name="lcExpMode"][value="days"]');
          if (el) el.checked = true;
          $expDays.value = r.expiration.days;
        } else if (r.expiration.date) {
          const el = $edit.querySelector('input[name="lcExpMode"][value="date"]');
          if (el) el.checked = true;
          $expDate.value = r.expiration.date;
        } else if (r.expiration.expired_object_delete_marker) {
          const el = $edit.querySelector('input[name="lcExpMode"][value="marker"]');
          if (el) el.checked = true;
        }
      }
      if (r.noncurrent_version_expiration) {
        if (r.noncurrent_version_expiration.noncurrent_days != null)
          $ncDays.value = r.noncurrent_version_expiration.noncurrent_days;
        if (r.noncurrent_version_expiration.newer_noncurrent_versions != null)
          $ncKeep.value = r.noncurrent_version_expiration.newer_noncurrent_versions;
      }
      if (r.abort_incomplete_multipart_upload && r.abort_incomplete_multipart_upload.days_after_initiation != null) {
        $abortDays.value = r.abort_incomplete_multipart_upload.days_after_initiation;
      }
    }
    _showEdit();
    _updateRulePreview();
    requestAnimationFrame(() => { try { $ruleId.focus(); } catch (_) { /* noop */ } });
  }

  // ── Build rule object from form ──────────────────────────────────────────
  function _collectRule() {
    const id = ($ruleId.value || '').trim();
    if (!id) throw new Error('Rule ID is required.');

    const status = $ruleStatus.value;

    const tags = Array.from($filtTags.querySelectorAll('.lc-tag-row')).map(row => {
      const ins = row.querySelectorAll('input');
      return { key: ins[0].value.trim(), value: ins[1].value.trim() };
    }).filter(t => t.key);

    const prefix = $filtPrefix.value.trim();
    const sizeGt = $sizeGt.value !== '' ? parseInt($sizeGt.value, 10) : null;
    const sizeLt = $sizeLt.value !== '' ? parseInt($sizeLt.value, 10) : null;

    const hasFilter = prefix || tags.length || sizeGt != null || sizeLt != null;
    const filter = hasFilter ? {
      prefix: prefix || null,
      tags,
      size_greater_than: sizeGt,
      size_less_than: sizeLt,
    } : null;

    const modeInput = $edit.querySelector('input[name="lcExpMode"]:checked');
    if (!modeInput) throw new Error('Select an option under Current versions.');
    const mode = modeInput.value;
    let expiration = null;
    if (mode === 'days') {
      const d = parseInt($expDays.value, 10);
      if (!Number.isFinite(d) || d < 1) throw new Error('Expiration days must be a positive integer.');
      expiration = { days: d };
    } else if (mode === 'date') {
      if (!$expDate.value) throw new Error('Pick an expiration date.');
      expiration = { date: $expDate.value };
    } else if (mode === 'marker') {
      expiration = { expired_object_delete_marker: true };
    }

    let noncurrent = null;
    const ncDays = $ncDays.value !== '' ? parseInt($ncDays.value, 10) : null;
    const ncKeep = $ncKeep.value !== '' ? parseInt($ncKeep.value, 10) : null;
    if (ncDays != null || ncKeep != null) {
      if (ncDays != null && ncDays < 1) throw new Error('Noncurrent days must be a positive integer.');
      noncurrent = { noncurrent_days: ncDays, newer_noncurrent_versions: ncKeep };
    }

    let abortMPU = null;
    if ($abortDays.value !== '') {
      const a = parseInt($abortDays.value, 10);
      if (!Number.isFinite(a) || a < 1) throw new Error('Abort MPU days must be a positive integer.');
      abortMPU = { days_after_initiation: a };
    }

    if (!expiration && !noncurrent && !abortMPU) {
      throw new Error('Specify at least one action: expiration, noncurrent expiration, or abort MPU.');
    }

    if (tags.length && abortMPU) {
      throw new Error('Abort incomplete multipart upload cannot be combined with a tag filter.');
    }
    if (tags.length && expiration && expiration.expired_object_delete_marker) {
      throw new Error('Expired-object-delete-marker cannot be combined with a tag filter.');
    }

    return {
      id,
      status,
      filter,
      expiration,
      noncurrent_version_expiration: noncurrent,
      abort_incomplete_multipart_upload: abortMPU,
    };
  }

  // ── Save ────────────────────────────────────────────────────────────────
  $saveBtn.addEventListener('click', async () => {
    _clearEditError();
    let newRule;
    try {
      newRule = _collectRule();
    } catch (e) {
      const msg = e.message || 'Invalid rule';
      _showEditError(msg);
      Toast.error(msg);
      return;
    }

    const nextRules = _rules.slice();
    if (_editingIndex === -1) {
      if (nextRules.some(r => r.id === newRule.id)) {
        const msg = `A rule with ID "${newRule.id}" already exists.`;
        _showEditError(msg);
        Toast.error(msg);
        return;
      }
      nextRules.push(newRule);
    } else {
      const prev = nextRules[_editingIndex];
      if (prev && prev.id !== newRule.id && nextRules.some(r => r.id === newRule.id)) {
        const msg = `A rule with ID "${newRule.id}" already exists.`;
        _showEditError(msg);
        Toast.error(msg);
        return;
      }
      nextRules[_editingIndex] = newRule;
    }

    $saveBtn.disabled = true;
    try {
      await API.putLifecycle(_bucket, nextRules);
      _rules = nextRules;
      _clearEditError();
      Toast.success('Lifecycle rule saved');
      _hideEdit();
      $delAllBtn.disabled = _rules.length === 0;
      _renderRules();
    } catch (err) {
      if (err.category === 'lifecycle_not_supported') {
        _renderNotSupportedBanner(err);
        _hideEdit();
      } else {
        const msg = err.message || 'Failed to save rule';
        _showEditError(msg);
        Toast.error(msg);
      }
    } finally {
      $saveBtn.disabled = false;
    }
  });

  // ── Delete single rule ──────────────────────────────────────────────────
  async function _deleteRule(index) {
    const r = _rules[index];
    if (!r) return;
    if (!confirm(`Delete lifecycle rule "${r.id}"?`)) return;

    const nextRules = _rules.slice();
    nextRules.splice(index, 1);

    try {
      if (nextRules.length === 0) {
        await API.deleteLifecycle(_bucket);
      } else {
        await API.putLifecycle(_bucket, nextRules);
      }
      _rules = nextRules;
      Toast.success('Rule deleted');
      $delAllBtn.disabled = _rules.length === 0;
      _renderRules();
    } catch (err) {
      if (err.category === 'lifecycle_not_supported') {
        _renderNotSupportedBanner(err);
      } else {
        Toast.error(err.message || 'Failed to delete rule');
      }
    }
  }

  // ── Delete all rules ────────────────────────────────────────────────────
  $delAllBtn.addEventListener('click', async () => {
    if (!_rules.length) return;
    if (!confirm(`Delete ALL ${_rules.length} lifecycle rule(s) on "${_bucket}"?`)) return;
    try {
      await API.deleteLifecycle(_bucket);
      _rules = [];
      Toast.success('All lifecycle rules deleted');
      $delAllBtn.disabled = true;
      _renderRules();
    } catch (err) {
      if (err.category === 'lifecycle_not_supported') _renderNotSupportedBanner(err);
      else Toast.error(err.message || 'Failed to delete rules');
    }
  });

  // ── Add/Reload buttons ──────────────────────────────────────────────────
  $addBtn.addEventListener('click', () => {
    if (!window.ServerFeatures?.bucket_lifecycle) {
      Toast.error('Lifecycle mutations are disabled. Set ENABLE_BUCKET_LIFECYCLE=true on the server.');
      return;
    }
    _openEdit(-1);
  });
  $reloadBtn.addEventListener('click', _load);

  // ── Public API ──────────────────────────────────────────────────────────
  function open(bucket) {
    _bucket = bucket;
    $title.textContent = 'Bucket Lifecycle Rules';
    $subtitle.textContent = bucket;
    _rules = [];
    _editingIndex = -1;
    _showModal();
    _load();
  }

  return { open };
})();
