import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";

const repository = process.env.GITHUB_REPOSITORY || "NONO6202/AIOSS";
const token = process.env.GITHUB_TOKEN || "";
const branch = process.env.GITHUB_REF_NAME || getGitBranch();
const windowDays = Number.parseInt(process.env.DORA_WINDOW_DAYS || "30", 10);
const now = new Date();
const since = new Date(now.getTime() - windowDays * 24 * 60 * 60 * 1000);

const headers = {
  Accept: "application/vnd.github+json",
  "X-GitHub-Api-Version": "2022-11-28",
  "User-Agent": "aioss-dora-metrics",
};

if (token) {
  headers.Authorization = `Bearer ${token}`;
}

function getGitBranch() {
  try {
    return execFileSync("git", ["branch", "--show-current"], { encoding: "utf8" }).trim() || "main";
  } catch {
    return "main";
  }
}

async function githubJson(path, params = {}) {
  const url = new URL(`https://api.github.com/repos/${repository}${path}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url, { headers });
  if (!response.ok) {
    throw new Error(`GitHub API ${response.status}: ${url.pathname}`);
  }
  return response.json();
}

async function paged(path, params = {}, maxPages = 5) {
  const rows = [];
  for (let page = 1; page <= maxPages; page += 1) {
    const data = await githubJson(path, { per_page: 100, page, ...params });
    const pageRows = Array.isArray(data) ? data : data.workflow_runs || data.deployments || data.items || [];
    rows.push(...pageRows);
    if (pageRows.length < 100) break;
  }
  return rows;
}

function iso(date) {
  return date ? new Date(date).toISOString() : null;
}

function hoursBetween(start, end) {
  return (new Date(end).getTime() - new Date(start).getTime()) / (60 * 60 * 1000);
}

function average(values) {
  const usable = values.filter((value) => Number.isFinite(value));
  if (!usable.length) return null;
  return usable.reduce((sum, value) => sum + value, 0) / usable.length;
}

function median(values) {
  const usable = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
  if (!usable.length) return null;
  const mid = Math.floor(usable.length / 2);
  return usable.length % 2 ? usable[mid] : (usable[mid - 1] + usable[mid]) / 2;
}

function readLocalCommitCount() {
  try {
    const output = execFileSync("git", ["log", `--since=${since.toISOString()}`, "--pretty=%H"], {
      encoding: "utf8",
    }).trim();
    return output ? output.split("\n").length : 0;
  } catch {
    return 0;
  }
}

function scoreValue(value, fallback = 0) {
  return Number.isFinite(value) ? value : fallback;
}

async function collect() {
  let pullRequests = [];
  let workflowRuns = [];
  let dataSource = "github-api";
  const notes = [
    "이 저장소는 운영 배포 시스템이 없으므로 GitHub 활동 데이터 기반의 DORA 프록시 지표를 수집한다.",
    "Deployment Frequency는 production deployment가 없을 때 기본 브랜치의 성공한 workflow run 수로 대체한다.",
    "MTTR은 실패한 workflow run 이후 같은 workflow/branch에서 다음 성공 run까지의 시간으로 계산한다.",
    "Change Failure Rate는 완료된 workflow run 중 실패/취소/타임아웃 비율로 계산한다.",
  ];

  try {
    pullRequests = await paged("/pulls", { state: "closed", sort: "updated", direction: "desc" });
    workflowRuns = await paged("/actions/runs", {
      branch,
      status: "completed",
      created: `>=${since.toISOString()}`,
    });
  } catch (error) {
    dataSource = "local-git-fallback";
    if (token) {
      notes.push(`GitHub API 수집 실패: ${error.message}`);
    } else {
      notes.push("로컬 실행에는 GitHub token이 없어 local git commit 정보만 fallback으로 사용했다.");
      notes.push("GitHub Actions 실행 시에는 GITHUB_TOKEN으로 PR과 workflow run 데이터를 수집한다.");
    }
  }

  const mergedPullRequests = pullRequests.filter((pr) => {
    return pr.merged_at && new Date(pr.merged_at) >= since;
  });

  const leadTimes = [];
  for (const pr of mergedPullRequests) {
    try {
      const commits = await paged(`/pulls/${pr.number}/commits`, {}, 2);
      const firstCommit = commits
        .map((commit) => commit.commit?.author?.date || commit.commit?.committer?.date)
        .filter(Boolean)
        .sort()[0];
      if (firstCommit) {
        leadTimes.push(hoursBetween(firstCommit, pr.merged_at));
      }
    } catch {
      if (pr.created_at) {
        leadTimes.push(hoursBetween(pr.created_at, pr.merged_at));
      }
    }
  }

  const completedRuns = workflowRuns.filter((run) => {
    return run.status === "completed" && new Date(run.created_at) >= since;
  });
  const successfulRuns = completedRuns.filter((run) => run.conclusion === "success");
  const failedRuns = completedRuns.filter((run) => {
    return ["failure", "cancelled", "timed_out", "action_required"].includes(run.conclusion);
  });

  const mttrValues = [];
  const sortedRuns = [...completedRuns].sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  for (const failed of failedRuns) {
    const recovered = sortedRuns.find((run) => {
      return (
        run.workflow_id === failed.workflow_id &&
        run.head_branch === failed.head_branch &&
        run.conclusion === "success" &&
        new Date(run.created_at) > new Date(failed.created_at)
      );
    });
    if (recovered) {
      mttrValues.push(hoursBetween(failed.updated_at || failed.created_at, recovered.created_at));
    }
  }

  const localCommitCount = readLocalCommitCount();
  const deploymentCount = successfulRuns.length;
  const deploymentFrequencyPerWeek = deploymentCount / Math.max(windowDays / 7, 1);
  const changeFailureRate =
    completedRuns.length > 0 ? (failedRuns.length / completedRuns.length) * 100 : null;

  const metrics = {
    generatedAt: now.toISOString(),
    repository,
    branch,
    window: {
      days: windowDays,
      since: since.toISOString(),
      until: now.toISOString(),
    },
    dataSource,
    summary: {
      leadTimeHoursMedian: median(leadTimes),
      leadTimeHoursAverage: average(leadTimes),
      deploymentFrequencyPerWeek,
      deploymentCount,
      mttrHoursAverage: average(mttrValues),
      changeFailureRatePercent: changeFailureRate,
    },
    counts: {
      localCommits: localCommitCount,
      mergedPullRequests: mergedPullRequests.length,
      workflowRuns: completedRuns.length,
      successfulWorkflowRuns: successfulRuns.length,
      failedWorkflowRuns: failedRuns.length,
      recoveredFailures: mttrValues.length,
    },
    chart: {
      labels: ["Lead Time", "Deploy Frequency", "MTTR", "Change Failure Rate"],
      values: [
        scoreValue(median(leadTimes)),
        scoreValue(deploymentFrequencyPerWeek),
        scoreValue(average(mttrValues)),
        scoreValue(changeFailureRate),
      ],
      units: ["hours", "deploys/week", "hours", "%"],
    },
    definitions: {
      leadTime:
        "merged PR 기준 첫 commit 시각부터 merge 시각까지의 중앙값. PR commit 조회 실패 시 PR 생성 시각을 사용한다.",
      deploymentFrequency:
        "지정 기간 동안 기본 브랜치에서 성공한 workflow run 수를 주 단위로 환산한 값.",
      mttr:
        "실패한 workflow run 이후 같은 workflow와 branch에서 다음 성공 run까지 걸린 평균 시간.",
      changeFailureRate:
        "완료된 workflow run 중 failure, cancelled, timed_out, action_required 결론의 비율.",
    },
    notes,
  };

  mkdirSync("metrics", { recursive: true });
  writeFileSync("metrics/dora.json", `${JSON.stringify(metrics, null, 2)}\n`);
}

collect().catch((error) => {
  console.error(error);
  process.exit(1);
});
