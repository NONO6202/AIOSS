import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";

const files = [
  "scripts/collect_dora_metrics.mjs",
  "scripts/create_ci_summary.mjs",
  "scripts/detect_changed_files.mjs",
  "scripts/lint.mjs",
  "scripts/render_dora_dashboard.mjs",
  "tests/repository.test.mjs",
];

for (const file of files) {
  if (!existsSync(file)) {
    throw new Error(`Missing lint target: ${file}`);
  }
  execFileSync(process.execPath, ["--check", file], { stdio: "inherit" });
}

console.log(`Checked ${files.length} JavaScript files.`);
