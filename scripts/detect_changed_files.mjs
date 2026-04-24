import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";

const eventName = process.env.GITHUB_EVENT_NAME || "local";
const baseRef = process.env.GITHUB_BASE_REF || "";
const before = process.env.GITHUB_EVENT_BEFORE || "";
const outputPath = process.env.GITHUB_OUTPUT || "";

function git(args) {
  return execFileSync("git", args, { encoding: "utf8" }).trim();
}

function changedFiles() {
  try {
    if (eventName === "pull_request" && baseRef) {
      return git(["diff", "--name-only", `origin/${baseRef}...HEAD`]).split("\n").filter(Boolean);
    }

    if (before && !/^0+$/.test(before)) {
      return git(["diff", "--name-only", `${before}..HEAD`]).split("\n").filter(Boolean);
    }

    return git(["diff", "--name-only", "HEAD~1..HEAD"]).split("\n").filter(Boolean);
  } catch {
    return [];
  }
}

function isDeployRelevant(file) {
  return [
    ".github/actions/",
    ".github/workflows/",
    "package.json",
    "package-lock.json",
    "scripts/",
    "tests/",
    "week02/",
    "week07/",
    "week08/",
  ].some((prefix) => file === prefix || file.startsWith(prefix));
}

function isMarkdownOnly(files) {
  return files.length > 0 && files.every((file) => file.endsWith(".md"));
}

const files = changedFiles();
const docsOnly = isMarkdownOnly(files);
const deployRequired = eventName === "workflow_dispatch" || (!docsOnly && files.some(isDeployRelevant));

const outputs = {
  "changed-files": files.join(","),
  "changed-count": String(files.length),
  "deploy-required": String(deployRequired),
  "docs-only": String(docsOnly),
};

for (const [key, value] of Object.entries(outputs)) {
  console.log(`${key}=${value}`);
}

if (outputPath) {
  appendFileSync(outputPath, Object.entries(outputs).map(([key, value]) => `${key}=${value}`).join("\n") + "\n");
}
