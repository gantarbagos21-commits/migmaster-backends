const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');

function sendFile(res, file, contentType) {
  try {
    const data = fs.readFileSync(file);
    res.statusCode = 200;
    res.setHeader('Content-Type', contentType);
    res.setHeader('Cache-Control', 'public, max-age=300');
    res.end(data);
  } catch {
    res.statusCode = 404;
    res.end('Not found');
  }
}

module.exports = (req, res) => {
  const url = new URL(req.url || '/', 'http://localhost');
  const pathname = url.pathname;

  if (pathname === '/health') {
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json');
    return res.end(JSON.stringify({ ok: true, service: 'MigMaster frontend' }));
  }

  if (pathname === '/favicon.svg') {
    return sendFile(res, path.join(root, 'public', 'favicon.svg'), 'image/svg+xml');
  }

  if (pathname === '/robots.txt') {
    return sendFile(res, path.join(root, 'public', 'robots.txt'), 'text/plain; charset=utf-8');
  }

  return sendFile(res, path.join(root, 'index.html'), 'text/html; charset=utf-8');
};
