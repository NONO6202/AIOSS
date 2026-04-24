import { mkdirSync, readFileSync, writeFileSync } from "node:fs";

const metrics = JSON.parse(readFileSync("metrics/dora.json", "utf8"));

function format(value, digits = 1) {
  return Number.isFinite(value) ? value.toFixed(digits) : "N/A";
}

function escapeXml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

const cards = [
  {
    label: "Lead Time",
    value: `${format(metrics.summary.leadTimeHoursMedian)}h`,
    detail: "median commit-to-merge",
    color: "#2563eb",
  },
  {
    label: "Deployment Frequency",
    value: `${format(metrics.summary.deploymentFrequencyPerWeek)} / week`,
    detail: `${metrics.summary.deploymentCount} successful runs`,
    color: "#16a34a",
  },
  {
    label: "MTTR",
    value: `${format(metrics.summary.mttrHoursAverage)}h`,
    detail: `${metrics.counts.recoveredFailures} recovered failures`,
    color: "#f59e0b",
  },
  {
    label: "Change Failure Rate",
    value: `${format(metrics.summary.changeFailureRatePercent)}%`,
    detail: `${metrics.counts.failedWorkflowRuns}/${metrics.counts.workflowRuns} failed runs`,
    color: "#dc2626",
  },
];

const max = Math.max(...metrics.chart.values, 1);
const barRows = metrics.chart.labels
  .map((label, index) => {
    const value = metrics.chart.values[index];
    const width = Math.max(2, Math.round((value / max) * 360));
    const y = 292 + index * 54;
    return `
      <text x="50" y="${y}" class="axis">${escapeXml(label)}</text>
      <rect x="210" y="${y - 22}" width="360" height="24" rx="5" fill="#e5e7eb"/>
      <rect x="210" y="${y - 22}" width="${width}" height="24" rx="5" fill="${cards[index].color}"/>
      <text x="590" y="${y}" class="bar-value">${format(value)} ${escapeXml(metrics.chart.units[index])}</text>`;
  })
  .join("\n");

const cardNodes = cards
  .map((card, index) => {
    const x = 36 + index * 286;
    return `
      <rect x="${x}" y="92" width="260" height="126" rx="10" fill="#ffffff" stroke="#d1d5db"/>
      <rect x="${x}" y="92" width="260" height="8" rx="4" fill="${card.color}"/>
      <text x="${x + 20}" y="130" class="card-label">${escapeXml(card.label)}</text>
      <text x="${x + 20}" y="172" class="card-value">${escapeXml(card.value)}</text>
      <text x="${x + 20}" y="200" class="card-detail">${escapeXml(card.detail)}</text>`;
  })
  .join("\n");

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="620" viewBox="0 0 1200 620">
  <style>
    .title { font: 700 30px Arial, sans-serif; fill: #111827; }
    .subtitle { font: 15px Arial, sans-serif; fill: #4b5563; }
    .card-label { font: 700 15px Arial, sans-serif; fill: #374151; }
    .card-value { font: 700 30px Arial, sans-serif; fill: #111827; }
    .card-detail { font: 14px Arial, sans-serif; fill: #6b7280; }
    .section { font: 700 18px Arial, sans-serif; fill: #111827; }
    .axis { font: 14px Arial, sans-serif; fill: #374151; }
    .bar-value { font: 700 14px Arial, sans-serif; fill: #111827; }
    .note { font: 13px Arial, sans-serif; fill: #4b5563; }
  </style>
  <rect width="1200" height="620" fill="#f8fafc"/>
  <text x="36" y="48" class="title">DORA Metrics Dashboard</text>
  <text x="36" y="74" class="subtitle">${escapeXml(metrics.repository)} · ${escapeXml(metrics.branch)} · ${escapeXml(metrics.window.days)} days · generated ${escapeXml(metrics.generatedAt)}</text>
  ${cardNodes}
  <text x="36" y="256" class="section">Normalized metric preview</text>
  ${barRows}
  <text x="36" y="546" class="note">Note: Production deployment data is not available yet, so workflow runs are used as repository-level proxy signals.</text>
  <text x="36" y="570" class="note">Data source: ${escapeXml(metrics.dataSource)}. Raw JSON artifact: metrics/dora.json.</text>
</svg>
`;

const report = `# DORA Metrics Report

Generated at: ${metrics.generatedAt}

Repository: \`${metrics.repository}\`  
Branch: \`${metrics.branch}\`  
Window: ${metrics.window.days} days

## Summary

| Metric | Value | Collection Rule |
| --- | ---: | --- |
| Lead Time | ${format(metrics.summary.leadTimeHoursMedian)} hours | ${metrics.definitions.leadTime} |
| Deployment Frequency | ${format(metrics.summary.deploymentFrequencyPerWeek)} / week | ${metrics.definitions.deploymentFrequency} |
| MTTR | ${format(metrics.summary.mttrHoursAverage)} hours | ${metrics.definitions.mttr} |
| Change Failure Rate | ${format(metrics.summary.changeFailureRatePercent)}% | ${metrics.definitions.changeFailureRate} |

## Counts

| Signal | Count |
| --- | ---: |
| Local commits | ${metrics.counts.localCommits} |
| Merged PRs | ${metrics.counts.mergedPullRequests} |
| Completed workflow runs | ${metrics.counts.workflowRuns} |
| Successful workflow runs | ${metrics.counts.successfulWorkflowRuns} |
| Failed workflow runs | ${metrics.counts.failedWorkflowRuns} |
| Recovered failures | ${metrics.counts.recoveredFailures} |

## Notes

${metrics.notes.map((note) => `- ${note}`).join("\n")}
`;

mkdirSync("week02/assets", { recursive: true });
writeFileSync("week02/assets/dora-dashboard.svg", svg);
writeFileSync("week02/dora-report.md", report);
