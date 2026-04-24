import { mkdirSync, readFileSync, writeFileSync } from "node:fs";

const metrics = JSON.parse(readFileSync("metrics/dora.json", "utf8"));
const generatedAt = new Date().toISOString();

const summary = {
  generatedAt,
  repository: process.env.GITHUB_REPOSITORY || metrics.repository,
  ref: process.env.GITHUB_REF_NAME || metrics.branch,
  nodeVersion: process.version,
  secretInjection: {
    githubTokenAvailable: Boolean(process.env.GITHUB_TOKEN),
    note: "Only secret availability is recorded. Secret values are never printed or persisted.",
  },
  artifacts: [
    "metrics/dora.json",
    "week02/assets/dora-dashboard.svg",
    "week02/dora-report.md",
    "week02/dashboard.html",
    "artifacts/ci-summary.json",
    "artifacts/ci-summary.md",
  ],
};

const markdown = `# CI Summary

Generated at: ${generatedAt}

Repository: \`${summary.repository}\`  
Ref: \`${summary.ref}\`  
Node.js: \`${summary.nodeVersion}\`

## Secret Injection

GitHub token available: \`${summary.secretInjection.githubTokenAvailable}\`

Secret values are not printed, uploaded, or written to artifacts.

## Artifacts

${summary.artifacts.map((artifact) => `- \`${artifact}\``).join("\n")}
`;

mkdirSync("artifacts", { recursive: true });
writeFileSync("artifacts/ci-summary.json", `${JSON.stringify(summary, null, 2)}\n`);
writeFileSync("artifacts/ci-summary.md", markdown);
