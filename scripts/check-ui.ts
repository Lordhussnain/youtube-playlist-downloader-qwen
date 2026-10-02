// scripts/check-ui.ts — syntax-gate the dashboard's inline JavaScript.
//
// web_ui.html is a single file with its logic in <script> blocks; nothing
// compiles it, so a stray brace only shows up as a blank dashboard at runtime.
// This extracts every inline script and parses it (no execution). Wired as
// `bun run check:ui` and into `bun run check`.

import { readFileSync } from "node:fs";

const html = readFileSync("web_ui.html", "utf-8");
const blocks = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)]
  .filter((m) => !/\ssrc\s*=/.test(m[0].slice(0, m[0].indexOf(">"))))
  .map((m) => m[1]);

if (blocks.length === 0) {
  console.error("check:ui — no inline <script> blocks found in web_ui.html");
  process.exit(1);
}

let failed = 0;
blocks.forEach((code, i) => {
  try {
    // Bun's transpiler parses without running. Scripts are classic (not
    // module) so top-level `await` would be a legitimate failure here too.
    new Bun.Transpiler({ loader: "js" }).transformSync(code);
  } catch (e: any) {
    failed++;
    console.error(`check:ui — syntax error in inline <script> #${i + 1}:\n${e?.message || e}`);
  }
});

if (failed > 0) process.exit(1);
console.log(`check:ui — ${blocks.length} inline script block(s) parse cleanly`);
