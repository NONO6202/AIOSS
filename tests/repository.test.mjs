import { readFileSync } from "node:fs";
import { test } from "node:test";
import assert from "node:assert/strict";

test("repository has required OSS governance files", () => {
  for (const file of ["README.md", "LICENSE", "CONTRIBUTING.md", "CODE_OF_CONDUCT.md"]) {
    assert.doesNotThrow(() => readFileSync(file, "utf8"), `${file} should exist`);
  }
});

test("CI workflow contains matrix, secrets, dependencies, and artifacts", () => {
  const workflow = readFileSync(".github/workflows/ci.yml", "utf8");
  const reusable = readFileSync(".github/workflows/reusable-node-check.yml", "utf8");
  const combinedWorkflow = `${workflow}\n${reusable}`;

  assert.match(workflow, /strategy:/);
  assert.match(workflow, /matrix:/);
  assert.match(workflow, /node-version:/);
  assert.match(workflow, /os:/);
  assert.match(workflow, /windows-latest/);
  assert.match(workflow, /"24"/);
  assert.match(combinedWorkflow, /secrets\.GITHUB_TOKEN/);
  assert.match(workflow, /needs: build/);
  assert.match(workflow, /-\s+test/);
  assert.match(combinedWorkflow, /upload-artifact/);
  assert.match(combinedWorkflow, /download-artifact/);
  assert.match(workflow, /reusable-node-check\.yml/);
  assert.match(workflow, /detect-changes/);
  assert.match(workflow, /dependency-cache/);
  assert.match(workflow, /cache-enabled:/);
});

test("week08 optimization files are present", () => {
  const reusable = readFileSync(".github/workflows/reusable-node-check.yml", "utf8");
  const composite = readFileSync(".github/actions/setup-node-project/action.yml", "utf8");
  const report = readFileSync("week08/CACHE_REPORT.md", "utf8");
  const detectChanges = readFileSync("scripts/detect_changed_files.mjs", "utf8");

  assert.match(reusable, /workflow_call:/);
  assert.match(composite, /using: composite/);
  assert.match(composite, /Setup Node\.js with npm cache/);
  assert.match(composite, /Setup Node\.js without dependency cache/);
  assert.match(report, /24886395644/);
  assert.match(detectChanges, /!\s*docsOnly\s*&&\s*files\.some\(isDeployRelevant\)/);
});

test("root README links week07 submission", () => {
  const readme = readFileSync("README.md", "utf8");
  assert.match(readme, /\[Week 07\]\(week07\/README\.md\)/);
  assert.match(readme, /\[Week 08\]\(week08\/README\.md\)/);
});
