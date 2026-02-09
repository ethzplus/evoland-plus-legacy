# PowerShell Migration Script for nascent-lulcc project structure
# Run this script from the evoland-with-baggage directory

param(
    [string]$DestinationPath = ".\nascent-lulcc-migrated",
    [switch]$Force
)

Write-Host "========================================"
Write-Host "Migrating to nascent-lulcc structure"
Write-Host "========================================"

# Check if we're in the right directory
if (!(Test-Path "R") -or !(Test-Path "inst")) {
    Write-Error "Please run this script from the evoland-with-baggage directory"
    exit 1
}

# Create destination directory
if (Test-Path $DestinationPath) {
    if ($Force) {
        Write-Host "Removing existing destination directory..."
        Remove-Item $DestinationPath -Recurse -Force
    } else {
        Write-Error "Destination directory already exists. Use -Force to overwrite."
        exit 1
    }
}

Write-Host "Creating directory structure in: $DestinationPath"

# Create main directories
New-Item -ItemType Directory -Path "$DestinationPath" -Force | Out-Null
New-Item -ItemType Directory -Path "$DestinationPath\src" -Force | Out-Null
New-Item -ItemType Directory -Path "$DestinationPath\scripts" -Force | Out-Null
New-Item -ItemType Directory -Path "$DestinationPath\config" -Force | Out-Null
New-Item -ItemType Directory -Path "$DestinationPath\environments" -Force | Out-Null
New-Item -ItemType Directory -Path "$DestinationPath\dinamica" -Force | Out-Null
New-Item -ItemType Directory -Path "$DestinationPath\docs" -Force | Out-Null

Write-Host "Directory structure created"

# Function to copy file with logging
function Copy-FileWithLog {
    param($Source, $Destination, $Description)
    
    if (Test-Path $Source) {
        Copy-Item $Source $Destination -Force
        Write-Host "  OK: $Description"
        return $true
    } else {
        Write-Host "  MISSING: $Description"
        return $false
    }
}

# Migrate R/ files to src/ (normalize file extensions to .r)
Write-Host "`nMigrating R source files to src/..."
$rFiles = Get-ChildItem "R" -File

$migratedCount = 0
foreach ($file in $rFiles) {
    $newName = $file.Name -replace '\.R$', '.r'  # Normalize to lowercase .r
    $success = Copy-FileWithLog $file.FullName "$DestinationPath\src\$newName" "R/$($file.Name) -> src/$newName"
    if ($success) { $migratedCount++ }
}

Write-Host "Migrated $migratedCount R source files"

# Migrate inst/ files to scripts/ 
Write-Host "`nMigrating executable scripts to scripts/..."
$scriptFiles = @(
    "inst\calculate_et0.r",
    "inst\calculate_mean_rsds_1981_2010.r",
    "inst\dist_calc_hpc.r",
    "inst\download_future_climate_data.py",
    "inst\download_historic_climate_data.py",
    "inst\master_pipeline.sh",
    "inst\process_climate_data.r",
    "inst\run_feature_selection.r",
    "inst\run_transition_modeling_pipeline.r",
    "inst\run_transition_modelling.r",
    "inst\setup_environments.sh",
    "inst\setup_hpc_directories.sh",
    "inst\submit_feature_selection.sh",
    "inst\submit_transition_modeling.sh",
    "scripts\run_climatic_data_prep.sbatch",
    "scripts\run_dist_calc.sbatch",
    "scripts\setup_dist_calc_env.sh"
)

$scriptCount = 0
foreach ($scriptFile in $scriptFiles) {
    if (Test-Path $scriptFile) {
        $fileName = Split-Path $scriptFile -Leaf
        Copy-Item $scriptFile "$DestinationPath\scripts\$fileName" -Force
        Write-Host "  OK: $scriptFile -> scripts/$fileName"
        $scriptCount++
    } else {
        Write-Host "  MISSING: $scriptFile"
    }
}

Write-Host "Migrated $scriptCount script files"

# Migrate configuration files
Write-Host "`nMigrating configuration files to config/..."
$configFiles = @(
    "inst\ancillary_data.yaml",
    "inst\model_specs.yaml",
    "inst\pred_data.yaml"
)

$configCount = 0
foreach ($configFile in $configFiles) {
    if (Test-Path $configFile) {
        $fileName = Split-Path $configFile -Leaf
        Copy-Item $configFile "$DestinationPath\config\$fileName" -Force
        Write-Host "  OK: $configFile -> config/$fileName"
        $configCount++
    } else {
        Write-Host "  MISSING: $configFile"
    }
}

Write-Host "Migrated $configCount configuration files"

# Migrate environment definitions
Write-Host "`nMigrating environment files to environments/..."
$envFiles = @(
    "envs\clim_data_env.yml",
    "envs\dist_calc_env.yml",
    "envs\feat_select_env.yaml",
    "envs\transition_model_env.yml"
)

$envCount = 0
foreach ($envFile in $envFiles) {
    if (Test-Path $envFile) {
        $fileName = Split-Path $envFile -Leaf
        Copy-Item $envFile "$DestinationPath\environments\$fileName" -Force
        Write-Host "  OK: $envFile -> environments/$fileName"
        $envCount++
    } else {
        Write-Host "  MISSING: $envFile"
    }
}

Write-Host "Migrated $envCount environment files"

# Migrate Dinamica model files
Write-Host "`nMigrating Dinamica model files to dinamica/..."
if (Test-Path "inst\dinamica_model") {
    Copy-Item "inst\dinamica_model" "$DestinationPath\dinamica" -Recurse -Force
    Write-Host "  OK: Dinamica model directory copied"
} else {
    Write-Host "  MISSING: Dinamica model directory not found"
}

# Migrate documentation
Write-Host "`nMigrating documentation to docs/..."
$docFiles = @(
    "inst\README_HPC.md",
    "inst\HPC_DIRECTORY_STRUCTURE.md",
    "inst\HPC_HOME_STRUCTURE.md"
)

$docCount = 0
foreach ($docFile in $docFiles) {
    if (Test-Path $docFile) {
        $fileName = Split-Path $docFile -Leaf
        Copy-Item $docFile "$DestinationPath\docs\$fileName" -Force
        Write-Host "  OK: $docFile -> docs/$fileName"
        $docCount++
    } else {
        Write-Host "  MISSING: $docFile"
    }
}

# Copy main project files
$mainFiles = @("README.md", "DESCRIPTION", "LICENSE", ".gitignore")
foreach ($mainFile in $mainFiles) {
    if (Test-Path $mainFile) {
        Copy-Item $mainFile "$DestinationPath\$mainFile" -Force
        Write-Host "  OK: $mainFile copied"
    }
}

Write-Host "Migrated $docCount documentation files"

# Create environment configuration template
Write-Host "`nCreating environment configuration template..."

$envTemplate = @'
#!/bin/bash
# Environment configuration for nascent-lulcc project

# Project paths  
export NASCENT_LULCC_HOME="/cluster/home/bblack/nascent-lulcc"
export NASCENT_LULCC_SRC="$NASCENT_LULCC_HOME/src"
export NASCENT_LULCC_SCRIPTS="$NASCENT_LULCC_HOME/scripts"
export NASCENT_LULCC_CONFIG="$NASCENT_LULCC_HOME/config"

# Scratch paths (create these on HPC scratch filesystem)
export NASCENT_LULCC_SCRATCH="/cluster/scratch/bblack/nascent-lulcc"
export NASCENT_LULCC_DATA="$NASCENT_LULCC_SCRATCH/data"
export NASCENT_LULCC_RESULTS="$NASCENT_LULCC_SCRATCH/results" 
export NASCENT_LULCC_LOGS="$NASCENT_LULCC_SCRATCH/logs"

# System paths
export CONDA_ENVS_PATH="/cluster/home/bblack/environments"
export R_LIBS_USER="/cluster/home/bblack/lib/R"
export MAMBA_EXE="/cluster/home/bblack/tools/micromamba"

# Temporary directories  
export TMPDIR="$NASCENT_LULCC_SCRATCH/temp"
export R_TMPDIR="$TMPDIR/R"

# Performance settings
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}
export GDAL_NUM_THREADS=ALL_CPUS
export GDAL_CACHEMAX=8192

# Create necessary directories
mkdir -p "$R_LIBS_USER" "$CONDA_ENVS_PATH"
'@

$envTemplate | Out-File -FilePath "$DestinationPath\.env.template" -Encoding UTF8
Write-Host "  OK: Created .env.template"

# Create project README
Write-Host "`nCreating project README..."

$readme = @'
# nascent-lulcc

Land Use/Land Cover Change (LULCC) modeling project for nascent transitions.

## Directory Structure

- **src/**: All R source functions (flat structure)
- **scripts/**: Executable scripts (R pipelines, shell scripts, Python scripts)
- **config/**: Configuration files (YAML, model specs)
- **environments/**: Conda environment definitions
- **dinamica/**: Dinamica EGO model files
- **docs/**: Documentation

## HPC Setup

1. Upload this directory to `/cluster/home/bblack/nascent-lulcc`
2. Copy `.env.template` to `.env` and customize paths
3. Source the environment: `source .env`
4. Create scratch directories on HPC scratch filesystem
5. Set up conda environments from `environments/` directory

## Environment Variables

The `.env` file defines all necessary paths:
- Project code in `/cluster/home/bblack/nascent-lulcc` 
- Data and results on scratch filesystem `/cluster/scratch/bblack/nascent-lulcc`

## Usage

Source the environment configuration in your job scripts:
```bash
source /cluster/home/bblack/nascent-lulcc/.env
```

Then run scripts from the scripts/ directory with proper paths set.
'@

$readme | Out-File -FilePath "$DestinationPath\README.md" -Encoding UTF8
Write-Host "  OK: Created project README.md"

# Create HPC setup script
Write-Host "`nCreating HPC setup script..."

$hpcSetup = @'
#!/bin/bash
# setup_hpc.sh - Run this on the HPC after uploading the project

PROJECT_DIR="/cluster/home/bblack/nascent-lulcc"
SCRATCH_DIR="/cluster/scratch/bblack/nascent-lulcc"

echo "Setting up nascent-lulcc on HPC..."

# Create scratch directories
mkdir -p "$SCRATCH_DIR/data/raw/lulc"
mkdir -p "$SCRATCH_DIR/data/raw/predictors" 
mkdir -p "$SCRATCH_DIR/data/raw/climate"
mkdir -p "$SCRATCH_DIR/data/processed/datasets"
mkdir -p "$SCRATCH_DIR/data/processed/models"
mkdir -p "$SCRATCH_DIR/results/feature_selection"
mkdir -p "$SCRATCH_DIR/results/models"
mkdir -p "$SCRATCH_DIR/results/evaluation"
mkdir -p "$SCRATCH_DIR/results/projections"
mkdir -p "$SCRATCH_DIR/logs/jobs"
mkdir -p "$SCRATCH_DIR/logs/runs"
mkdir -p "$SCRATCH_DIR/temp/R"

# Create home directories
mkdir -p "/cluster/home/bblack/environments"
mkdir -p "/cluster/home/bblack/lib/R"
mkdir -p "/cluster/home/bblack/tools"

# Copy environment template to active config
cp "$PROJECT_DIR/.env.template" "$PROJECT_DIR/.env"

# Make scripts executable
chmod +x "$PROJECT_DIR/scripts/"*.sh
chmod +x "$PROJECT_DIR/scripts/"*.sbatch

echo "HPC setup complete!"
echo ""
echo "Next steps:"
echo "1. Install micromamba in /cluster/home/bblack/tools/"
echo "2. Create conda environments from environments/ directory"
echo "3. Source the environment: source $PROJECT_DIR/.env"
echo "4. Update job scripts with correct paths"
'@

$hpcSetup | Out-File -FilePath "$DestinationPath\setup_hpc.sh" -Encoding UTF8
Write-Host "  OK: Created HPC setup script"

# Generate migration summary
Write-Host "`n========================================"
Write-Host "Migration Complete!"
Write-Host "========================================"

Write-Host "`nSummary:"
Write-Host "  - $migratedCount R source files -> src/"
Write-Host "  - $scriptCount executable scripts -> scripts/"
Write-Host "  - $configCount configuration files -> config/"  
Write-Host "  - $envCount environment files -> environments/"
Write-Host "  - Documentation -> docs/"
Write-Host "  - Dinamica model -> dinamica/"

Write-Host "`nGenerated files:"
Write-Host "  - .env.template (environment configuration)"
Write-Host "  - README.md (project documentation)"
Write-Host "  - setup_hpc.sh (HPC setup script)"

Write-Host "`nNext steps:"
Write-Host "  1. Review the migrated structure in: $DestinationPath"
Write-Host "  2. Upload to HPC: /cluster/home/bblack/nascent-lulcc"
Write-Host "  3. Run setup_hpc.sh on the HPC"
Write-Host "  4. Update script paths as needed"

Write-Host "`nMigration directory ready for HPC transfer!"