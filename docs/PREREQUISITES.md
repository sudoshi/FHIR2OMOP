# Prerequisites

This document explains exactly how to install the prerequisites for the
FHIR2OMOP runbook and pipeline on macOS, Linux, and Windows.

For the runbook workflow itself, also see
[`RUNBOOK_TUI.md`](./RUNBOOK_TUI.md).

## What You Need

On any OS, you need:

- Python `3.11+`
- Google Cloud SDK, including `gcloud` and `bq`
- Application Default Credentials configured with
  `gcloud auth application-default login`
- `dbt-bigquery`
- Optional: `Rscript` if you want the DQD stage
- The runbook TUI dependencies in `tools/runbook/requirements.txt`

`bq` comes with the Google Cloud CLI, so you do not install it
separately.

## macOS

### 1. Install Python 3.11+

The Python docs recommend installing the latest Python 3 from
`python.org` on macOS.

1. Go to `https://www.python.org/downloads/macos/`
2. Download the latest Python `3.11+` macOS installer
3. Open the `.pkg` file and complete the installer

Verify:

```bash
python3 --version
python3 -m pip --version
```

### 2. Install Google Cloud CLI

Use Google’s official tarball installer:

```bash
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-darwin-arm.tar.gz
tar -xf google-cloud-cli-darwin-arm.tar.gz
./google-cloud-sdk/install.sh
```

If you are on an Intel Mac, replace `google-cloud-cli-darwin-arm.tar.gz`
with `google-cloud-cli-darwin-x86_64.tar.gz`.

Open a new terminal after installation, then verify:

```bash
gcloud --version
bq version
```

### 3. Configure Google Cloud auth

```bash
gcloud init
gcloud auth application-default login
```

Verify:

```bash
gcloud auth list
gcloud auth application-default print-access-token >/dev/null && echo "ADC OK"
```

### 4. Install dbt-bigquery

Create a dedicated virtual environment for dbt:

```bash
python3 -m venv ~/.venvs/dbt-bigquery
source ~/.venvs/dbt-bigquery/bin/activate
python -m pip install --upgrade pip
python -m pip install "dbt-bigquery"
```

Verify:

```bash
dbt --version
```

If `dbt` is not found after activation, run:

```bash
~/.venvs/dbt-bigquery/bin/dbt --version
```

### 5. Install R / Rscript (optional, only for DQD)

1. Go to `https://mac.r-project.org/`
2. Download the current macOS installer package for R
3. Open the installer and complete it

Verify:

```bash
Rscript --version
```

### 6. Install the runbook TUI dependencies

From the repo root:

```bash
make runbook-install
```

Or manually:

```bash
python3 -m venv tools/runbook/.venv
tools/runbook/.venv/bin/python -m pip install --upgrade pip
tools/runbook/.venv/bin/python -m pip install -r tools/runbook/requirements.txt
```

## Linux

Linux package names vary by distro. The commands below are the simplest
Ubuntu/Debian-style path. If you use Fedora, RHEL, Arch, etc., use your
distro’s equivalent packages.

### 1. Install Python 3.11+

Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip
```

Verify:

```bash
python3 --version
python3 -m pip --version
```

If your distro’s default `python3` is older than `3.11`, install a
newer Python package from your distro’s supported repositories before
continuing.

### 2. Install Google Cloud CLI

Use Google’s official Linux tarball installer:

```bash
curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz
tar -xf google-cloud-cli-linux-x86_64.tar.gz
./google-cloud-sdk/install.sh
```

On ARM Linux, replace the archive name with
`google-cloud-cli-linux-arm.tar.gz`.

Open a new shell after installation, then verify:

```bash
gcloud --version
bq version
```

### 3. Configure Google Cloud auth

```bash
gcloud init
gcloud auth application-default login
```

Verify:

```bash
gcloud auth list
gcloud auth application-default print-access-token >/dev/null && echo "ADC OK"
```

### 4. Install dbt-bigquery

Create a dedicated dbt virtual environment:

```bash
python3 -m venv ~/.venvs/dbt-bigquery
source ~/.venvs/dbt-bigquery/bin/activate
python -m pip install --upgrade pip
python -m pip install "dbt-bigquery"
```

Verify:

```bash
dbt --version
```

### 5. Install R / Rscript (optional, only for DQD)

Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y r-base
```

Verify:

```bash
Rscript --version
```

### 6. Install the runbook TUI dependencies

From the repo root:

```bash
make runbook-install
```

Or manually:

```bash
python3 -m venv tools/runbook/.venv
tools/runbook/.venv/bin/python -m pip install --upgrade pip
tools/runbook/.venv/bin/python -m pip install -r tools/runbook/requirements.txt
```

## Windows

For Windows, WSL is still the smoothest option. If you are running
natively, the steps below work without `make`.

### 1. Install Python 3.11+

The Python docs recommend the full installer for developers.

1. Go to `https://www.python.org/downloads/windows/`
2. Download the latest Python `3.11+` Windows installer
3. Run the installer
4. Check `Add python.exe to PATH`
5. Click `Install Now`

Verify in PowerShell:

```powershell
py -3 --version
py -3 -m pip --version
```

### 2. Install Google Cloud CLI

Use Google’s official Windows installer from PowerShell:

```powershell
(New-Object Net.WebClient).DownloadFile("https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe", "$env:Temp\GoogleCloudSDKInstaller.exe")
& $env:Temp\GoogleCloudSDKInstaller.exe
```

In the installer:

- leave the option to configure `gcloud` selected
- keep bundled Python enabled unless you have a specific reason not to

Verify:

```powershell
gcloud --version
bq version
```

### 3. Configure Google Cloud auth

```powershell
gcloud init
gcloud auth application-default login
```

Verify:

```powershell
gcloud auth list
gcloud auth application-default print-access-token | Out-Null; Write-Host "ADC OK"
```

### 4. Install dbt-bigquery

Create a dedicated dbt virtual environment:

```powershell
py -3 -m venv $HOME\.venvs\dbt-bigquery
& $HOME\.venvs\dbt-bigquery\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install "dbt-bigquery"
```

Verify:

```powershell
dbt --version
```

If PowerShell blocks activation scripts, run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Then activate again.

### 5. Install R / Rscript (optional, only for DQD)

1. Go to `https://cran.r-project.org/bin/windows/base/`
2. Download the current Windows installer for R
3. Run the installer and complete setup

Verify:

```powershell
Rscript --version
```

### 6. Install the runbook TUI dependencies

From PowerShell in the repo root:

```powershell
py -3 -m venv tools/runbook/.venv
.\tools\runbook\.venv\Scripts\python.exe -m pip install --upgrade pip
.\tools\runbook\.venv\Scripts\python.exe -m pip install -r tools/runbook/requirements.txt
```

Run the TUI directly:

```powershell
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --dry-run
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --check-connectivity
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --check-hashing
```

## Secret Source Notes

Supported runbook pepper sources are:

- `prompt`
- `env`
- `dotenv`
- `pass`
- `gcloud`

On Windows, `pass` is usually the least portable option. In practice,
`prompt`, `env`, `dotenv`, or `gcloud` are the better choices there.

## Final Verification

Before you try a real runbook execution, verify the full toolchain:

### macOS / Linux

```bash
python3 --version
gcloud --version
bq version
dbt --version
make runbook-dry-run
make runbook-check-connectivity
make runbook-check-hashing
```

### Windows PowerShell

```powershell
py -3 --version
gcloud --version
bq version
dbt --version
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --dry-run
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --check-connectivity
.\tools\runbook\.venv\Scripts\python.exe -m tools.runbook --check-hashing
```

## Sources

Official references used for the install guidance:

- Python on macOS:
  `https://docs.python.org/3.11/using/mac.html`
- Python on Windows:
  `https://docs.python.org/3.11/using/windows.html`
- Google Cloud CLI install:
  `https://cloud.google.com/sdk/docs/install`
- dbt-bigquery package:
  `https://pypi.org/project/dbt-bigquery/`
- R for Windows:
  `https://cran.r-project.org/bin/windows/base/`
- R for macOS:
  `https://mac.r-project.org/`
