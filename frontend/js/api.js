/**
 * api.js — Fetch wrapper for all backend API calls.
 * All requests include credentials for HTTP Basic Auth.
 * On error, parses structured error JSON from the backend.
 */

window.API = (() => {
  const BASE = '';

  async function _fetch(path, opts = {}) {
    const url = BASE + path;
    const response = await fetch(url, {
      credentials: 'include',
      ...opts,
      headers: {
        'Accept': 'application/json',
        ...(opts.headers || {}),
      },
    });

    if (response.status === 401) {
      throw { category: 'auth_required', title: 'Authentication Required', message: 'Please reload the page and enter your credentials.' };
    }

    if (!response.ok) {
      let errBody;
      try { errBody = await response.json(); } catch { errBody = null; }
      const detail = errBody?.detail || {};
      throw {
        category:   detail.category || 'http_error',
        title:      detail.title    || `HTTP ${response.status}`,
        message:    detail.message  || response.statusText,
        detail:     detail.detail   || null,
        httpStatus: response.status,
      };
    }

    // For streaming responses (download/preview image|pdf) return raw Response
    const ct = response.headers.get('content-type') || '';
    if (ct.startsWith('image/') || ct === 'application/pdf' || ct === 'application/octet-stream') {
      return response;
    }
    return response.json();
  }

  return {
    health:          ()         => _fetch('/api/health'),
    testConnection:  ()         => _fetch('/api/test-connection'),
    listBuckets:     ()         => _fetch('/api/buckets'),
    listObjects:     (bucket, prefix = '', search = '', sort = 'name', order = 'asc') =>
      _fetch(`/api/objects?bucket=${encodeURIComponent(bucket)}&prefix=${encodeURIComponent(prefix)}&search=${encodeURIComponent(search)}&sort=${sort}&order=${order}`),
    objectMeta:      (bucket, key) =>
      _fetch(`/api/object/meta?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`),
    previewObject:   (bucket, key) =>
      _fetch(`/api/object/preview?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`),
    bucketObjectCount: (bucket) =>
      _fetch(`/api/bucket-count?bucket=${encodeURIComponent(bucket)}`),
    downloadUrl:     (bucket, key) =>
      `/api/object/download?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`,
    uploadObject:    async (bucket, prefix, file) => {
      const formData = new FormData();
      formData.append('bucket', bucket);
      formData.append('prefix', prefix);
      formData.append('file', file);
      return _fetch('/api/object/upload', {
        method: 'POST',
        body: formData,
      });
    },
    deleteObject:    (bucket, key) =>
      _fetch(`/api/object?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`, {
        method: 'DELETE',
      }),
  };
})();
