const fs = require('fs');
const vm = require('vm');
const html = fs.readFileSync(require('path').join(__dirname, '..', 'public', 'index.html'), 'utf8');
const scripts = [...html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)];
if (!scripts.length) throw new Error('No inline scripts found');
for (let i = 0; i < scripts.length; i++) {
  const code = scripts[i][1].trim();
  if (!code) continue;
  new vm.Script(code, { filename: `public/index.html:inline-script-${i + 1}` });
}
console.log(`Client syntax OK (${scripts.length} inline script block(s)).`);
