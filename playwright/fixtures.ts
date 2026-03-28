import { test as base, expect } from '@playwright/test';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import { spawn, type ChildProcess } from 'node:child_process';
import { cpSync, createWriteStream, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import net from 'node:net';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

type AppFixture = {
  baseUrl: string;
  repoUrl: string;
  tempDir: string;
  mainInitialSha: string;
  mainHeadSha: string;
  previewBranch: string;
  previewHeadSha: string;
};

type LoggedProcess = {
  child: ChildProcess;
  output: { value: string };
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, '..');
const fixtureProjectRoot = resolve(repoRoot, 'tests/fixtures/jaffle_shop_project');
const serverBinary = resolve(repoRoot, 'target/debug/dbtx-server');
const workerBinary = resolve(repoRoot, 'target/debug/dbtx-worker');

async function getFreePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const server = net.createServer();
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        server.close();
        reject(new Error('failed to allocate port'));
        return;
      }
      const { port } = address;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolvePort(port);
      });
    });
    server.on('error', reject);
  });
}

function createBareFixtureRepo(tempDir: string): {
  repoUrl: string;
  mainInitialSha: string;
  mainHeadSha: string;
  previewBranch: string;
  previewHeadSha: string;
} {
  const sourceDir = join(tempDir, 'repo-source');
  const bareDir = join(tempDir, 'repo.git');
  cpSync(fixtureProjectRoot, sourceDir, { recursive: true });
  rmSync(join(sourceDir, 'target'), { recursive: true, force: true });
  rmSync(join(sourceDir, 'logs'), { recursive: true, force: true });
  rmSync(join(sourceDir, 'warehouse.duckdb'), { force: true });
  execFileSync('git', ['init', '-b', 'main'], { cwd: sourceDir });
  execFileSync('git', ['config', 'user.name', 'Playwright'], { cwd: sourceDir });
  execFileSync('git', ['config', 'user.email', 'playwright@example.com'], { cwd: sourceDir });
  execFileSync('git', ['add', '.'], { cwd: sourceDir });
  execFileSync('git', ['commit', '-m', 'Initial fixture commit'], { cwd: sourceDir });
  const mainInitialSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: sourceDir })
    .toString()
    .trim();

  execFileSync(
    'git',
    ['commit', '--allow-empty', '-m', 'Second fixture commit'],
    { cwd: sourceDir },
  );
  const mainHeadSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: sourceDir })
    .toString()
    .trim();

  const previewBranch = 'preview';
  execFileSync('git', ['checkout', '-b', previewBranch, mainInitialSha], { cwd: sourceDir });
  execFileSync(
    'git',
    ['commit', '--allow-empty', '-m', 'Preview fixture commit'],
    { cwd: sourceDir },
  );
  const previewHeadSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: sourceDir })
    .toString()
    .trim();

  execFileSync('git', ['checkout', 'main'], { cwd: sourceDir });
  execFileSync('git', ['clone', '--bare', sourceDir, bareDir], { cwd: tempDir });
  return {
    repoUrl: `file://${bareDir}`,
    mainInitialSha,
    mainHeadSha,
    previewBranch,
    previewHeadSha,
  };
}

function spawnLoggedProcess(
  command: string,
  args: string[],
  options: {
    cwd: string;
    env: NodeJS.ProcessEnv;
    logPath: string;
  },
): LoggedProcess {
  const log = createWriteStream(options.logPath, { flags: 'a' });
  const output = { value: '' };
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout?.on('data', (chunk) => {
    output.value += chunk.toString();
  });
  child.stderr?.on('data', (chunk) => {
    output.value += chunk.toString();
  });
  child.stdout?.pipe(log);
  child.stderr?.pipe(log);
  return { child, output };
}

function tailFile(path: string): string {
  try {
    const content = readFileSync(path, 'utf8');
    const lines = content.trim().split('\n');
    return lines.slice(-40).join('\n');
  } catch {
    return '';
  }
}

async function waitForHttpOk(url: string, timeoutMs: number, logPath?: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until deadline.
    }
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 500));
  }
  const logTail = logPath ? tailFile(logPath) : '';
  throw new Error(
    logTail
      ? `timed out waiting for ${url}\n\nserver log tail:\n${logTail}`
      : `timed out waiting for ${url}`,
  );
}

async function waitForServerReady(
  process: LoggedProcess,
  url: string,
  timeoutMs: number,
  logPath: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (process.child.exitCode !== null) {
      const logTail = process.output.value.trim() || tailFile(logPath);
      throw new Error(
        logTail
          ? `server exited with code ${process.child.exitCode}\n\nserver log tail:\n${logTail}`
          : `server exited with code ${process.child.exitCode}`,
      );
    }
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until deadline.
    }
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 500));
  }
  const logTail = tailFile(logPath);
  throw new Error(
    logTail
      ? `timed out waiting for ${url}\n\nserver log tail:\n${logTail}`
      : `timed out waiting for ${url}`,
  );
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function migrate(baseUrl: string): Promise<void> {
  const response = await fetch(`${baseUrl}/v1/state/migrate`, { method: 'POST' });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`migration failed: ${response.status} ${body}`);
  }
}

async function waitForPostgresReady(
  container: StartedTestContainer,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const result = await container.exec(['pg_isready', '-U', 'dbtx', '-d', 'dbtx']);
      if (result.exitCode === 0) {
        return;
      }
    } catch {
      // Retry until deadline.
    }
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 500));
  }
  throw new Error('timed out waiting for postgres readiness');
}

async function waitForWorker(workerLogPath: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const content = readFileSync(workerLogPath, 'utf8');
      if (content.includes('starting dbtx worker') || content.includes('poll_interval_ms')) {
        return;
      }
    } catch {
      // Retry until deadline.
    }
    await new Promise((resolveSleep) => setTimeout(resolveSleep, 250));
  }
}

async function stopProcess(process: LoggedProcess | undefined): Promise<void> {
  if (!process || process.child.killed || process.child.exitCode !== null) return;
  process.child.kill('SIGTERM');
  await new Promise((resolveWait) => setTimeout(resolveWait, 500));
  if (process.child.exitCode === null && !process.child.killed) {
    process.child.kill('SIGKILL');
  }
}

export const test = base.extend<{ app: AppFixture }>({
  app: [async ({}, use) => {
    const tempDir = mkdtempSync(join(tmpdir(), 'dbtx-playwright-'));
    mkdirSync(join(tempDir, 'logs'));

    const container: StartedTestContainer = await new GenericContainer('postgres:16')
      .withEnvironment({
        POSTGRES_USER: 'dbtx',
        POSTGRES_PASSWORD: 'dbtx',
        POSTGRES_DB: 'dbtx',
      })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage('database system is ready to accept connections'))
      .start();

    const databaseUrl = `postgres://dbtx:dbtx@${container.getHost()}:${container.getMappedPort(5432)}/dbtx`;
    const serverPort = await getFreePort();
    const baseUrl = `http://127.0.0.1:${serverPort}`;
    const repo = createBareFixtureRepo(tempDir);
    const serverLogPath = join(tempDir, 'logs', 'server.log');

    await waitForPostgresReady(container, 30_000);

    const commonEnv = {
      ...process.env,
      DBTX_DATABASE_URL: databaseUrl,
    };

    let server: LoggedProcess | undefined;

    try {
      let lastServerError: unknown;
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        server = spawnLoggedProcess(
          serverBinary,
          ['--listen', `127.0.0.1:${serverPort}`],
          {
            cwd: repoRoot,
            env: {
              ...commonEnv,
              RUST_LOG: 'dbtx=warn',
            },
            logPath: serverLogPath,
          },
        );
        try {
          await waitForServerReady(server, `${baseUrl}/healthz`, 15_000, serverLogPath);
          lastServerError = undefined;
          break;
        } catch (error) {
          lastServerError = error;
          await stopProcess(server);
          server = undefined;
          if (attempt < 5) {
            await sleep(2_000);
          }
        }
      }
      if (lastServerError) {
        throw lastServerError;
      }
      await migrate(baseUrl);

      const worker = spawnLoggedProcess(
        workerBinary,
        [
          '--service-url',
          baseUrl,
          '--execution-mode',
          'server',
        ],
        {
          cwd: repoRoot,
          env: {
            ...commonEnv,
            RUST_LOG: 'dbtx=info',
          },
          logPath: join(tempDir, 'logs', 'worker.log'),
        },
      );

      try {
        await waitForWorker(join(tempDir, 'logs', 'worker.log'), 15_000);
        await use({
          baseUrl,
          repoUrl: repo.repoUrl,
          tempDir,
          mainInitialSha: repo.mainInitialSha,
          mainHeadSha: repo.mainHeadSha,
          previewBranch: repo.previewBranch,
          previewHeadSha: repo.previewHeadSha,
        });
      } finally {
        await stopProcess(worker);
      }
    } finally {
      await stopProcess(server);
      await container.stop();
      rmSync(tempDir, { recursive: true, force: true });
    }
  }, { scope: 'worker', timeout: 180_000 }],
});

export { expect };
