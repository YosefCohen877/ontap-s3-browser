/**
 * preview.js — File preview renderer for the detail pane.
 * Handles: text, JSON (pretty-printed), log, image, PDF, unsupported.
 */

window.PreviewView = (() => {
  let _activeBlobUrl = null;

  function _revokeBlobUrl() {
    if (_activeBlobUrl) {
      URL.revokeObjectURL(_activeBlobUrl);
      _activeBlobUrl = null;
    }
  }

  function _createBlobUrl(blob) {
    _revokeBlobUrl();
    _activeBlobUrl = URL.createObjectURL(blob);
    return _activeBlobUrl;
  }

  function _esc(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function _isJson(text) {
    try { JSON.parse(text); return true; } catch { return false; }
  }

  function _prettyJson(text) {
    try { return JSON.stringify(JSON.parse(text), null, 2); } catch { return text; }
  }

  async function loadPreview(bucket, key, container) {
    _revokeBlobUrl();

    container.innerHTML = `<div style="padding:1.25rem;display:flex;align-items:center;gap:.5rem;font-size:.8125rem;color:var(--clr-text-muted)">
      <div class="spinner" style="width:18px;height:18px;border-width:2px;flex-shrink:0"></div> Loading preview…
    </div>`;

    let data;
    try {
      data = await API.previewObject(bucket, key);
    } catch (err) {
      if (err.category === 'preview_unsupported') {
        container.innerHTML = `<div class="preview-unsupported">
          Preview not available for this file type.<br>
          <a class="btn btn--ghost btn--sm" style="margin-top:.5rem;display:inline-flex"
             href="${API.downloadUrl(bucket, key)}" download>Download to view</a>
        </div>`;
      } else if (err.category === 'preview_too_large') {
        container.innerHTML = `<div class="preview-unsupported">${_esc(err.message)}</div>`;
      } else {
        container.innerHTML = `<div class="preview-unsupported text-sm" style="color:var(--clr-error)">${_esc(err.title)}: ${_esc(err.message)}</div>`;
      }
      return;
    }

    // ── Text / JSON / Log ────────────────────────────────────────────────
    if (data && data.preview_type === 'text') {
      const isJson = _isJson(data.content);
      const displayText = isJson ? _prettyJson(data.content) : data.content;
      const label = isJson ? 'JSON Preview' : 'Text Preview';

      container.innerHTML = `
        <div class="preview-label">${label}${data.truncated ? ' <span style="color:var(--clr-warning)">(truncated at 512 KB)</span>' : ''}</div>
        <pre class="preview-text" dir="auto">${_esc(displayText)}</pre>
        ${data.truncated ? '<div class="preview-truncated">⚠ File is larger than 512 KB — showing first 512 KB only. Download for full content.</div>' : ''}
      `;
      return;
    }

    // ── Image (streaming response) ───────────────────────────────────────
    if (data instanceof Response) {
      const ct = data.headers.get('content-type') || '';
      if (ct.startsWith('image/')) {
        const url = _createBlobUrl(await data.blob());
        container.innerHTML = `
          <div class="preview-label">Image Preview</div>
          <img class="preview-image" src="${url}" alt="Preview" />
        `;
        return;
      }
      if (ct.startsWith('video/')) {
        const url = _createBlobUrl(await data.blob());
        container.innerHTML = `
          <div class="preview-label">Video Preview</div>
          <video class="preview-video" src="${url}" controls playsinline></video>
        `;
        return;
      }
      if (ct === 'application/pdf') {
        const url = _createBlobUrl(await data.blob());
        container.innerHTML = `
          <div class="preview-label">PDF Preview</div>
          <embed class="preview-pdf" src="${url}" type="application/pdf" />
        `;
        return;
      }
    }

    // Fallback
    container.innerHTML = `<div class="preview-unsupported">Preview not available.</div>`;
  }

  return { loadPreview };
})();
