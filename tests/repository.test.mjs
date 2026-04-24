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

  assert.match(workflow, /strategy:/);
  assert.match(workflow, /matrix:/);
  assert.match(workflow, /node-version:/);
  assert.match(workflow, /os:/);
  assert.match(workflow, /secrets\.GITHUB_TOKEN/);
  assert.match(workflow, /needs: build/);
  assert.match(workflow, /needs: test/);
  assert.match(workflow, /upload-artifact/);
  assert.match(workflow, /download-artifact/);
});

test("root README links week07 submission", () => {
  const readme = readFileSync("README.md", "utf8");
  assert.match(readme, /\[Week 07\]\(week07\/README\.md\)/);
});
