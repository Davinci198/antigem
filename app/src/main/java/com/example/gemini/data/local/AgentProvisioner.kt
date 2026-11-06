package com.example.gemini.data.local

import android.content.Context
import android.system.Os
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * Provisions the agent CLIs that live inside the Ubuntu container so the Android bridge and the
 * terminal find them without the user installing anything by hand.
 *
 * Coverage and install method (the same idea as `provisionAgyBinary`):
 *  - agy      -> official Antigravity CLI tarball, checksum pinned from the vendor manifest
 *  - claude   -> official Claude Code native linux-arm64 binary (no npm/node needed)
 *  - opencode -> official OpenCode platform binary taken from the npm tarball (no npm/node needed)
 *  - hermes   -> official Hermes Agent installer (needs git + curl inside the container)
 *
 * Everything is idempotent: a hidden marker file inside the rootfs records the installed version,
 * so a second call only logs "already installed". Downloads happen on the Android side (OkHttp,
 * with TLS and progress) and are written straight into the rootfs through the host path, so a
 * broken guest DNS never leaves a half-installed binary behind. Only `hermes` needs the guest
 * network, because its installer bootstraps its own Python/uv toolchain.
 *
 * The Termux variant (`com.termux`, shared UID) is untouched: there the CLIs are installed by the
 * terminal and the Settings > Claude Code flow.
 */
object AgentProvisioner {

    private const val TAG = "AgentProvisioner"

    const val AGY_PATH = "/root/.local/bin/agy"
    const val CLAUDE_PATH = "/root/.local/bin/claude"
    const val OPENCODE_PATH = "/root/.opencode/bin/opencode"
    const val HERMES_PATH = "/root/.local/bin/hermes"

    private const val GUEST_DOWNLOAD_DIR = "/root/.antigem-downloads"
    private const val GUEST_TMP_DIR = "/root/.antigem-tmp"

    private const val AGY_MANIFEST_URL =
        "https://antigravity-cli-auto-updater-974169037036.us-central1.run.app/manifests/linux_arm64.json"
    private const val CLAUDE_REGISTRY_URL = "https://registry.npmjs.org/@anthropic-ai/claude-code/latest"
    private const val CLAUDE_RELEASE_BASE = "https://downloads.claude.ai/claude-code-releases"
    private const val OPENCODE_PACKAGE_NAME = "@opencode/cli-linux-arm64"
    private const val HERMES_INSTALL_URL = "https://hermes-agent.nousresearch.com/install.sh"

    private const val APT_TIMEOUT_SECONDS = 15 * 60L
    private const val BINARY_INSTALL_TIMEOUT_SECONDS = 10 * 60L
    private const val HERMES_TIMEOUT_SECONDS = 30 * 60L
    private const val PROGRESS_STEP_BYTES = 16L * 1024L * 1024L

    private val agents = listOf(
        Agent(AGY_PATH, ".antigem-agy-version", "agy"),
        Agent(CLAUDE_PATH, ".antigem-claude-version", "claude"),
        Agent(OPENCODE_PATH, ".antigem-opencode-version", "opencode"),
        Agent(HERMES_PATH, ".antigem-hermes-version", "hermes"),
    )

    data class Agent(val guestPath: String, val markerName: String, val label: String)

    data class AgentInstallResult(
        val agent: String,
        val installed: Boolean,
        val version: String,
        val detail: String,
    )

    private val httpClient = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        // large archives (Claude Code is >300 MB) over a phone connection
        .readTimeout(120, TimeUnit.SECONDS)
        .callTimeout(0, TimeUnit.SECONDS)
        .retryOnConnectionFailure(true)
        .followRedirects(true)
        .followSslRedirects(true)
        .build()

    private val provisionMutex = Mutex()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile
    private var autoAttempts = 0

    /** Cheap check used before starting a background provision pass: no guest call, only file stats. */
    fun missingAgents(context: Context): List<Agent> {
        val appContext = context.applicationContext
        if (LocalEnvironmentManager.isTermuxPackage(appContext)) return emptyList()
        val ubuntuDir = LocalEnvironmentManager.getUbuntuRootDir(appContext)
        if (!ubuntuDir.isDirectory) return emptyList()
        return agents.filter { agent ->
            val target = File(ubuntuDir, agent.guestPath.removePrefix("/"))
            !target.canExecute() || markerFile(appContext, agent).readTextOrNull().isNullOrBlank()
        }
    }

    /**
     * Fire-and-forget provisioning for app/server start. Returns immediately; at most
     * [MAX_AUTO_ATTEMPTS] attempts per process so a dead network does not retry forever.
     */
    fun provisionInBackground(context: Context, force: Boolean = false) {
        val appContext = context.applicationContext
        if (LocalEnvironmentManager.isTermuxPackage(appContext)) return
        if (!LocalEnvironmentManager.isUbuntuInstalled(appContext)) return
        if (!force && missingAgents(appContext).isEmpty()) return
        if (autoAttempts >= MAX_AUTO_ATTEMPTS) return
        autoAttempts += 1
        scope.launch {
            try {
                provisionAll(appContext)
            } catch (e: Exception) {
                Log.w(TAG, "Background agent provisioning failed: ${e.message}")
            }
        }
    }

    /**
     * Installs every missing agent. Blocking: call it from a coroutine work scope, never from the
     * main thread. Progress is reported through [log] and echoed to Android logcat.
     */
    suspend fun provisionAll(
        context: Context,
        log: (String) -> Unit = {},
    ): List<AgentInstallResult> = withContext(Dispatchers.IO) {
        val appContext = context.applicationContext
        if (LocalEnvironmentManager.isTermuxPackage(appContext)) return@withContext emptyList()
        if (!LocalEnvironmentManager.isUbuntuInstalled(appContext)) return@withContext emptyList()

        provisionMutex.withLock {
            val results = mutableListOf<AgentInstallResult>()
            log("Agent tools: checking agy, claude, opencode and hermes inside the container...")

            val downloads = guestFile(appContext, GUEST_DOWNLOAD_DIR).apply { ensureDirectory() }
            guestFile(appContext, GUEST_TMP_DIR).apply { ensureDirectory() }

            results += installAgy(appContext, downloads, log)
            results += installClaude(appContext, downloads, log)
            results += installOpencode(appContext, downloads, log)
            results += installHermes(appContext, log)

            ensureGuestPath(appContext, log)

            results.forEach { result ->
                if (result.installed) {
                    log("Agent tools: ${result.agent} ${result.version} ready (${result.detail})")
                } else {
                    log("Agent tools: ${result.agent} NOT installed (${result.detail})")
                }
            }
            results
        }
    }

    // ---------------------------------------------------------------- agy

    private suspend fun installAgy(
        context: Context,
        downloads: File,
        log: (String) -> Unit,
    ): AgentInstallResult {
        val agent = "agy"
        val spec = agents.first { it.label == agent }
        val target = guestFile(context, AGY_PATH)
        val marker = markerFile(context, spec)
        if (target.canExecute() && !marker.readTextOrNull().isNullOrBlank()) {
            return AgentInstallResult(agent, true, marker.readTextOrNull().orEmpty(), "already installed")
        }

        val manifest = runCatching { JSONObject(fetchText(AGY_MANIFEST_URL)) }.getOrElse { error ->
            return AgentInstallResult(agent, false, "", "release manifest unavailable: ${error.message}")
        }
        val version = manifest.optString("version")
        val url = manifest.optString("url")
        val sha512 = manifest.optString("sha512")
        if (version.isBlank() || url.isBlank() || sha512.isBlank()) {
            return AgentInstallResult(agent, false, "", "release manifest is incomplete")
        }

        log("[agy] downloading Antigravity CLI $version...")
        val archiveName = "agy-$version.tar.gz"
        val archive = download(downloads, archiveName, url, agent, log)
            ?: return AgentInstallResult(agent, false, "", "download failed")

        val installed = runGuest(
            context = context,
            agent = agent,
            log = log,
            timeoutSeconds = BINARY_INSTALL_TIMEOUT_SECONDS,
            script = """
                set -e
                echo "$sha512  $GUEST_DOWNLOAD_DIR/$archiveName" | sha512sum -c -
                rm -rf $GUEST_TMP_DIR/agy
                mkdir -p $GUEST_TMP_DIR/agy /root/.local/bin
                tar -xzf $GUEST_DOWNLOAD_DIR/$archiveName -C $GUEST_TMP_DIR/agy
                bin=${'$'}(find $GUEST_TMP_DIR/agy -type f -name antigravity | head -n 1)
                [ -n "${'$'}bin" ] || { echo "antigravity binary is missing from the archive"; exit 3; }
                install -m 0755 "${'$'}bin" $AGY_PATH
                rm -rf $GUEST_TMP_DIR/agy $GUEST_DOWNLOAD_DIR/$archiveName
                $AGY_PATH --version
            """.trimIndent(),
        ) ?: return AgentInstallResult(agent, false, "", "install command failed").also { archive.delete() }

        archive.delete()
        marker.writeTextOrNull(version)
        return AgentInstallResult(agent, true, version, "installed $installed")
    }

    // ------------------------------------------------------------- claude

    private suspend fun installClaude(
        context: Context,
        downloads: File,
        log: (String) -> Unit,
    ): AgentInstallResult {
        val agent = "claude"
        val spec = agents.first { it.label == agent }
        val target = guestFile(context, CLAUDE_PATH)
        val marker = markerFile(context, spec)
        if (target.canExecute() && !marker.readTextOrNull().isNullOrBlank()) {
            return AgentInstallResult(agent, true, marker.readTextOrNull().orEmpty(), "already installed")
        }

        if (!ensureGuestLibs(context, log)) {
            return AgentInstallResult(agent, false, "", "container prerequisites missing (apt failed)")
        }

        val version = runCatching {
            JSONObject(fetchText(CLAUDE_REGISTRY_URL)).getString("version")
        }.getOrElse { error ->
            return AgentInstallResult(agent, false, "", "version lookup failed: ${error.message}")
        }
        val manifest = runCatching {
            JSONObject(fetchText("$CLAUDE_RELEASE_BASE/$version/manifest.json"))
        }.getOrElse { error ->
            return AgentInstallResult(agent, false, "", "release manifest unavailable: ${error.message}")
        }
        val platform = manifest.optJSONObject("platforms")?.optJSONObject("linux-arm64")
            ?: return AgentInstallResult(agent, false, "", "no linux-arm64 build in manifest")
        val checksum = platform.optString("checksum")
        if (checksum.isBlank()) return AgentInstallResult(agent, false, "", "missing linux-arm64 checksum")

        val sizeMb = platform.optLong("size") / (1024 * 1024)
        log("[claude] downloading Claude Code $version (${sizeMb} MB)...")
        val archiveName = "claude-$version"
        val archive = download(downloads, archiveName, "$CLAUDE_RELEASE_BASE/$version/linux-arm64/claude", agent, log)
            ?: return AgentInstallResult(agent, false, "", "download failed")

        val installed = runGuest(
            context = context,
            agent = agent,
            log = log,
            timeoutSeconds = BINARY_INSTALL_TIMEOUT_SECONDS,
            script = """
                set -e
                echo "$checksum  $GUEST_DOWNLOAD_DIR/$archiveName" | sha256sum -c -
                mkdir -p /root/.local/bin
                install -m 0755 $GUEST_DOWNLOAD_DIR/$archiveName $CLAUDE_PATH
                rm -f $GUEST_DOWNLOAD_DIR/$archiveName
                $CLAUDE_PATH --version
            """.trimIndent(),
        ) ?: return AgentInstallResult(agent, false, "", "install command failed").also { archive.delete() }

        archive.delete()
        marker.writeTextOrNull(version)
        return AgentInstallResult(agent, true, version, "installed $installed")
    }

    // ----------------------------------------------------------- opencode
    private suspend fun installOpencode(
        context: Context,
        downloads: File,
        log: (String) -> Unit,
    ): AgentInstallResult {
        val agent = "opencode"
        val spec = agents.first { it.label == agent }
        val target = guestFile(context, OPENCODE_PATH)
        val marker = markerFile(context, spec)
        if (target.canExecute() && !marker.readTextOrNull().isNullOrBlank()) {
            return AgentInstallResult(agent, true, marker.readTextOrNull().orEmpty(), "already installed")
        }

        if (!ensureGuestLibs(context, log)) {
            return AgentInstallResult(agent, false, "", "container prerequisites missing (apt failed)")
        }

        val packageJson = runCatching {
            JSONObject(fetchText("https://registry.npmjs.org/$OPENCODE_PACKAGE_NAME/latest"))
        }.getOrElse { error ->
            return AgentInstallResult(agent, false, "", "registry lookup failed: ${error.message}")
        }
        val version = packageJson.optString("version")
        val tarball = packageJson.optJSONObject("dist")?.optString("tarball").orEmpty()
        if (version.isBlank() || tarball.isBlank()) {
            return AgentInstallResult(agent, false, "", "registry entry is incomplete")
        }

        log("[opencode] downloading OpenCode $version...")
        val archiveName = "opencode-$version.tgz"
        val archive = download(downloads, archiveName, tarball, agent, log)
            ?: return AgentInstallResult(agent, false, "", "download failed")

        val installed = runGuest(
            context = context,
            agent = agent,
            log = log,
            timeoutSeconds = BINARY_INSTALL_TIMEOUT_SECONDS,
            script = """
                set -e
                rm -rf $GUEST_TMP_DIR/opencode
                mkdir -p $GUEST_TMP_DIR/opencode /root/.opencode/bin
                tar -xzf $GUEST_DOWNLOAD_DIR/$archiveName -C $GUEST_TMP_DIR/opencode
                src=$GUEST_TMP_DIR/opencode/package/bin/opencode
                [ -f "${'$'}src" ] || { echo "opencode binary is missing from the tarball"; exit 3; }
                install -m 0755 "${'$'}src" $OPENCODE_PATH
                rm -rf $GUEST_TMP_DIR/opencode $GUEST_DOWNLOAD_DIR/$archiveName
                $OPENCODE_PATH --version
            """.trimIndent(),
        ) ?: return AgentInstallResult(agent, false, "", "install command failed").also { archive.delete() }

        archive.delete()
        marker.writeTextOrNull(version)
        return AgentInstallResult(agent, true, version, "installed $installed")
    }

    // ------------------------------------------------------------- hermes

    private suspend fun installHermes(
        context: Context,
        log: (String) -> Unit,
    ): AgentInstallResult {
        val agent = "hermes"
        val spec = agents.first { it.label == agent }
        val marker = markerFile(context, spec)
        val installedTarget = guestFile(context, HERMES_PATH)
        val altTarget = guestFile(context, "/usr/local/bin/hermes")
        if ((installedTarget.canExecute() || altTarget.canExecute()) && !marker.readTextOrNull().isNullOrBlank()) {
            return AgentInstallResult(agent, true, marker.readTextOrNull().orEmpty(), "already installed")
        }

        log("[hermes] installing Hermes Agent (needs git + curl inside the container)...")
        val script = """
            set -e
            # Mirrors Mobile-Harness' known-good command for this installer: the
            # full build toolchain is required because pm.cli resolves some
            # dependencies as sdists. apt is run unconditionally — a half-finished
            # earlier apt run is the other way this install gets wedged.
            echo "installing build dependencies (git, curl, python3, build-essential)..."
            apt-get update -qq || true
            DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
              git curl ca-certificates libatomic1 \
              python3 python3-pip python3-venv build-essential python3-dev libffi-dev || true
            # uv refuses to hardlink on this filesystem (proot on f2fs); copy mode
            # is what Mobile-Harness uses for the same installer.
            export UV_LINK_MODE=copy
            mkdir -p /etc/uv
            printf 'link-mode = "copy"\n' > /etc/uv/uv.toml
            rm -f /var/lib/apt/lists/lock /var/lib/apt/lists/partial/.lock \
              /var/cache/apt/archives/lock /var/lib/dpkg/lock-frontend /var/lib/dpkg/lock 2>/dev/null || true
            dpkg --configure -a 2>/dev/null || true
            # A partially-extracted uv cache entry (missing WHEEL / dist-info) makes
            # pm.cli fail permanently with "Failed to install: <wheel>", so a retry
            # never recovers. Clear it before installing.
            rm -rf /root/.hermes/cache/uv/archive-v0 \
              /root/.hermes/cache/uv/sdists-v9 \
              /root/.hermes/cache/uv/wheels-v* 2>/dev/null || true
            # A non-git copy of the agent tree left by an aborted run must go, otherwise
            # the installer trusts it and skips the clone; a healthy git checkout stays.
            if [ -d /usr/local/lib/hermes-agent ] && [ ! -d /usr/local/lib/hermes-agent/.git ]; then
              rm -rf /usr/local/lib/hermes-agent
            fi
            if [ -d /root/.hermes/hermes-agent ] && [ ! -d /root/.hermes/hermes-agent/.git ]; then
              rm -rf /root/.hermes/hermes-agent
            fi
            mkdir -p $GUEST_DOWNLOAD_DIR
            curl -fsSL $HERMES_INSTALL_URL -o $GUEST_DOWNLOAD_DIR/hermes-install.sh
            # pm runs its uv children with an explicit --no-config, which disables
            # /etc/uv/uv.toml, and it filters UV_* out of their environment. With
            # --link2symlink in play, uv's default hardlink mode then dies with
            # "Operation not permitted" on the .proot.l2s.* sidecars proot creates.
            # The only place left to force copy mode is pm's own argv, so drop
            # --link-mode copy into the resolved uv commands. pm must be patched
            # twice: once after the clone (so the very first sync is covered) and
            # again after the install (in case a stage refreshed the checkout).
            patch_pm_uv() {
              for d in /root/.hermes/hermes-agent /usr/local/lib/hermes-agent; do
                f="${'$'}d/pm/environment.py"
                [ -f "${'$'}f" ] || continue
                # A previous run may have left a bad patch behind (an earlier version
                # rewrote both `"--compile-bytecode"]` occurrences and made the module
                # unparseable), so restore the checkout before editing.
                if [ -d "${'$'}d/.git" ]; then
                  git -C "${'$'}d" checkout -- pm/environment.py 2>/dev/null || true
                fi
                cat > /tmp/patch_pm.py <<'PYEOF'
import pathlib, re, sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
# 1) Repair damage an earlier version of this patch could have left: it rewrote
#    every `"--compile-bytecode"]`, including the fallback comprehension, which
#    made the module unparseable.
text = text.replace('arg != "--compile-bytecode", "--link-mode", "copy"]',
                    'arg != "--compile-bytecode"]')
# 2) Mask the fallback comprehension so only the sync command list gains flags.
guard = "@@FALLBACK_ARGS@@"
text = text.replace('arg != "--compile-bytecode"]', guard)
# 3) Collapse duplicates first, then add once, so repeat runs cannot stack flags.
text = re.sub(r'(, "--compile-bytecode")(?:, "--link-mode", "copy")*', r'\1', text)
text = text.replace(', "--compile-bytecode"]',
                    ', "--compile-bytecode", "--link-mode", "copy"]')
text = re.sub(r'("pip", "install", "--no-config")(?:, "--link-mode", "copy")*',
              r'\1, "--link-mode", "copy"', text)
# 4) Unmask.
text = text.replace(guard, 'arg != "--compile-bytecode"]')
path.write_text(text)
PYEOF
                python3 /tmp/patch_pm.py "${'$'}f" || true
                # Never ship an unparseable pm: it fails the whole install at import.
                python3 -c 'import ast,sys;ast.parse(open(sys.argv[1]).read())' "${'$'}f" 2>/dev/null \
                  || echo "warning: could not patch ${'$'}f, leaving it as-is"
              done
            }
            # Only stage banners and the failure cause are streamed: sweeping all of
            # apt's output into the app log buries the one line that matters. The
            # unfiltered transcript goes to a file so a failed run stays diagnosable.
            run_installer() {
              bash $GUEST_DOWNLOAD_DIR/hermes-install.sh --non-interactive --skip-browser --skip-computer-use < /dev/null 2>&1 \
                | tr '\r' '\n' | tee -a /sdcard/Download/hermes-full.log \
                | grep --line-buffered -iE '^(==>|\[|✔|✖|⚠)|error|caused by|failed|traceback'
              return "${'$'}{PIPESTATUS[0]}"
            }
            attempt_install() {
              patch_pm_uv
              run_installer
            }
            set +e
            attempt_install
            status=${'$'}?
            set -e
            if [ "${'$'}status" -ne 0 ]; then
              # A partially-extracted uv cache entry (missing files, or .proot.l2s.*
              # sidecars) makes every later attempt fail the same way, so a plain
              # retry never recovers. Clear the cache and the aborted checkout,
              # re-patch pm now that the clone exists, then retry from a clean slate.
              echo "hermes installer exited ${'$'}status; clearing the uv cache and retrying once"
              tail -40 /root/.hermes/logs/install.log 2>/dev/null | tr '\r' '\n' || true
              rm -rf /root/.hermes/cache/uv/archive-v0 /root/.hermes/cache/uv/sdists-v9 \
                /root/.hermes/cache/uv/wheels-v* 2>/dev/null || true
              set +e
              attempt_install
              status=${'$'}?
              set -e
            fi
            if [ "${'$'}status" -ne 0 ]; then
              echo "hermes installer exited ${'$'}status (full log: /root/.hermes/logs/install.log)"
              echo "---- installer log (tail) ----"
              tail -60 /root/.hermes/logs/install.log 2>/dev/null | tr '\r' '\n' || true
              echo "---- end installer log ----"
              cp /root/.hermes/logs/install.log /sdcard/Download/hermes-install-log.txt 2>/dev/null || true
              exit 5
            fi
            rm -f $GUEST_DOWNLOAD_DIR/hermes-install.sh
            if [ -x $HERMES_PATH ]; then
              ln -sf $HERMES_PATH /usr/local/bin/hermes
            elif [ ! -x /usr/local/bin/hermes ]; then
              echo "hermes binary was not installed by the official installer"
              exit 4
            fi
            hermes --version 2>/dev/null || $HERMES_PATH --version
        """.trimIndent()

        val version = runGuest(
            context = context,
            agent = agent,
            log = log,
            timeoutSeconds = HERMES_TIMEOUT_SECONDS,
            script = script,
        ) ?: return AgentInstallResult(agent, false, "", "install command failed")

        marker.writeTextOrNull(version.ifBlank { "installed" })
        return AgentInstallResult(agent, true, version.ifBlank { "installed" }, "installed")
    }

    // -------------------------------------------------------------- helpers

    /**
     * The Ubuntu base image ships glibc but neither libstdc++ nor a CA bundle.
     * Claude Code and OpenCode are Bun-compiled binaries that load libstdc++ at runtime, so both
     * need it before the first `--version` check. Runs at most once per container (marker file).
     */
    private suspend fun ensureGuestLibs(context: Context, log: (String) -> Unit): Boolean {
        val marker = File(LocalEnvironmentManager.getUbuntuRootDir(context), ".antigem-prereqs")
        if (marker.readTextOrNull() == "1") return true
        log("[agents] installing container prerequisites (libstdc++6, ca-certificates)...")
        val ok = runGuest(
            context = context,
            agent = "agents",
            log = log,
            timeoutSeconds = APT_TIMEOUT_SECONDS,
            script = """
                set -e
                if [ ! -e /usr/lib/aarch64-linux-gnu/libstdc++.so.6 ] || [ ! -e /etc/ssl/certs/ca-certificates.crt ]; then
                  apt-get update -qq
                  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends libstdc++6 ca-certificates
                fi
                echo PREREQS_OK
            """.trimIndent(),
        ) != null
        if (ok) marker.writeTextOrNull("1")
        return ok
    }

    /**
     * Makes the agent binaries reachable for the terminal and for the bridge:
     * `/root/.local/bin` (agy, claude, hermes) and `/root/.opencode/bin` (opencode) are added to
     * PATH, and both are symlinked into `/usr/local/bin`, which is already on every guest PATH.
     */
    private suspend fun ensureGuestPath(context: Context, log: (String) -> Unit) {
        val script = """
            set -e
            mkdir -p /root/.local/bin
            for name in agy claude opencode hermes; do
              if [ -x /root/.local/bin/${'$'}name ]; then
                ln -sf /root/.local/bin/${'$'}name /usr/local/bin/${'$'}name
              fi
            done
            if [ -x $OPENCODE_PATH ]; then
              ln -sf $OPENCODE_PATH /usr/local/bin/opencode
            fi
            if ! grep -q "antigem-agents" /etc/profile.d/antigem-agents.sh 2>/dev/null; then
              printf '%s\n' '# antigem-agents' 'export PATH="/root/.local/bin:/root/.opencode/bin:${'$'}PATH"' > /etc/profile.d/antigem-agents.sh
            fi
            if ! grep -q "antigem-agents" /root/.bashrc 2>/dev/null; then
              printf '%s\n' 'export PATH="/root/.local/bin:/root/.opencode/bin:${'$'}PATH"  # antigem-agents' >> /root/.bashrc
            fi
        """.trimIndent()
        val result = LocalEnvironmentManager.executeCommand(
            command = script,
            context = context,
            timeoutSeconds = 120,
        )
        if (result.exitCode != 0) {
            log("[agents] PATH setup failed: ${result.output.takeLast(300)}")
        }
    }

    /**
     * Runs [script] inside the container, streaming its output to [log]. Returns the last non-empty
     * output line (usually the `--version` answer) or null on failure.
     */
    private suspend fun runGuest(
        context: Context,
        agent: String,
        log: (String) -> Unit,
        timeoutSeconds: Long,
        script: String,
    ): String? {
        val lines = mutableListOf<String>()
        val result = LocalEnvironmentManager.executeCommand(
            command = script,
            context = context,
            timeoutSeconds = timeoutSeconds,
            onOutputLine = { line ->
                if (line.isNotBlank()) {
                    lines += line
                    log("[$agent] $line")
                }
            },
        )
        if (result.exitCode != 0) {
            Log.w(TAG, "[$agent] install exited ${result.exitCode}: ${result.output.takeLast(800)}")
            return null
        }
        return lines.lastOrNull { it.isNotBlank() }?.trim()
    }

    private fun download(
        downloads: File,
        fileName: String,
        url: String,
        agent: String,
        log: (String) -> Unit,
    ): File? {
        val target = File(downloads, fileName)
        val partial = File(downloads, "$fileName.part")
        return try {
            partial.delete()
            val request = Request.Builder().url(url).get().build()
            httpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    log("[$agent] download failed: HTTP ${response.code}")
                    return null
                }
                val total = response.body?.contentLength() ?: -1L
                var written = 0L
                var nextReport = PROGRESS_STEP_BYTES
                partial.outputStream().use { out ->
                    response.body?.byteStream()?.use { input ->
                        val buffer = ByteArray(64 * 1024)
                        while (true) {
                            val read = input.read(buffer)
                            if (read < 0) break
                            out.write(buffer, 0, read)
                            written += read
                            if (written >= nextReport) {
                                val mb = written / (1024 * 1024)
                                if (total > 0) {
                                    log("[$agent] downloaded $mb of ${total / (1024 * 1024)} MB")
                                } else {
                                    log("[$agent] downloaded $mb MB")
                                }
                                nextReport += PROGRESS_STEP_BYTES
                            }
                        }
                    }
                }
                if (total > 0 && written != total) {
                    partial.delete()
                    log("[$agent] download truncated ($written of $total bytes)")
                    return null
                }
            }
            partial.renameTo(target)
            Os.chmod(target.absolutePath, 0b110100100)
            target
        } catch (e: Exception) {
            partial.delete()
            log("[$agent] download failed: ${e.message}")
            null
        }
    }

    private fun fetchText(url: String): String {
        val request = Request.Builder().url(url).header("Accept", "application/json").get().build()
        httpClient.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("HTTP ${response.code} for $url")
            return response.body?.string() ?: throw IOException("empty body for $url")
        }
    }

    private fun guestFile(context: Context, guestPath: String): File =
        File(LocalEnvironmentManager.getUbuntuRootDir(context), guestPath.removePrefix("/"))

    private fun markerFile(context: Context, agent: Agent): File =
        File(LocalEnvironmentManager.getUbuntuRootDir(context), agent.markerName)

    private fun File.ensureDirectory() {
        if (!isDirectory) {
            mkdirs()
            try {
                Os.chmod(absolutePath, 0b111101101)
            } catch (e: Exception) {
                Log.w(TAG, "chmod failed for ${absolutePath}: ${e.message}")
            }
        }
    }

    private fun File.readTextOrNull(): String? = try {
        if (isFile) readText().trim() else null
    } catch (e: Exception) {
        null
    }

    private fun File.writeTextOrNull(text: String) {
        try {
            parentFile?.mkdirs()
            writeText(text)
        } catch (e: Exception) {
            Log.w(TAG, "could not write marker ${absolutePath}: ${e.message}")
        }
    }

    private const val MAX_AUTO_ATTEMPTS = 2
}
