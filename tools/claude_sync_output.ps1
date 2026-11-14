# Claude Code hook (Stop / SessionEnd, set in .claude/settings.json): copy a worktree
# session's .output into the main checkout's .output, so outputs outlive the worktree.
# One shared folder: a newer file overwrites the same path; nothing is deleted from it.
# Does nothing when run in the main checkout. Always exits 0, so it never blocks a session.
# (A junction .output -> main .output was rejected: `git worktree remove` deletes through it.)
try {
    $cwd = $null
    try { $cwd = ([Console]::In.ReadToEnd() | ConvertFrom-Json).cwd } catch {}
    if (-not $cwd) { $cwd = (Get-Location).Path }

    $top = git -C $cwd rev-parse --show-toplevel 2>$null
    $common = git -C $cwd rev-parse --path-format=absolute --git-common-dir 2>$null
    if (-not $top -or -not $common) { exit 0 }
    $main = Split-Path -Parent $common          # <main>\.git -> <main>
    if ((Resolve-Path $top).Path -eq (Resolve-Path $main).Path) { exit 0 }

    $src = Join-Path $top '.output'
    if (-not (Test-Path $src)) { exit 0 }
    robocopy $src (Join-Path $main '.output') /E /R:1 /W:1 /NJH /NJS /NP /NFL /NDL | Out-Null
} catch {}
exit 0
