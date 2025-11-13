#!/bin/bash
#SBATCH --job-name=feat-select
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=48
#SBATCH --mem-per-cpu=2700M
#SBATCH --tmp=50G
#SBATCH --output=logs/feat-select-%j.out
#SBATCH --error=logs/feat-select-%j.err
#SBATCH --profile=task

###############################################################################
# SLURM Job Script for Feature Selection Pipeline
# 
# Usage: sbatch submit_feature_selection.sh
# 
# Monitor job: squeue -u $USER
# Cancel job: scancel <job_id>
# View output: tail -f logs/feat-select-<job_id>.out
###############################################################################

# ----------------------------
# 1) Load HPC modules
# ----------------------------
module load stack/2024-06

echo "================================================================"
echo "Job started at: $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Job name: $SLURM_JOB_NAME"
echo "Node: $SLURM_NODELIST"
echo "CPUs: $SLURM_CPUS_PER_TASK"
echo "Memory per CPU: 2700M"
echo "================================================================"
echo

# ----------------------------
# 2) Detect and load conda/mamba/micromamba
# ----------------------------
MAMBA_EXE="${MAMBA_EXE:-$HOME/micromamba/bin/micromamba}"
export CONDA_BIN=""

if [ -f "$MAMBA_EXE" ]; then
    # Micromamba or mamba found via MAMBA_EXE
    CONDA_BIN=$MAMBA_EXE
    eval "$($CONDA_BIN shell hook -s bash)"
    echo "✓ Using Micromamba/Mamba at: $CONDA_BIN"
elif command -v mamba &>/dev/null; then
    CONDA_BIN=$(command -v mamba)
    eval "$($CONDA_BIN shell hook -s bash)"
    echo "✓ Using Mamba at: $CONDA_BIN"
elif command -v conda &>/dev/null; then
    CONDA_BIN=$(command -v conda)
    eval "$($CONDA_BIN shell.bash hook)"
    echo "✓ Using Conda at: $CONDA_BIN"
else
    echo "✗ No micromamba/mamba/conda found. Please load a conda module first."
    exit 1
fi

# ----------------------------
# 3) Activate environment
# ----------------------------
ENV_NAME="feat_select_env"  # Change to your environment name

micromamba activate "$ENV_NAME" 2>/dev/null || \
mamba activate "$ENV_NAME" 2>/dev/null || \
conda activate "$ENV_NAME"

echo "✓ Activated environment: $ENV_NAME"
echo

 ----------------------------
# 4) Personal R libs & install RRF if needed
# ----------------------------
export R_LIBS_USER="$HOME/R_libs"
mkdir -p "$R_LIBS_USER"

# Check if RRF is installed, install if missing
echo "Checking for RRF package..."
Rscript -e "if (!require('RRF', quietly = TRUE)) { \
  cat('Installing RRF from CRAN...\n'); \
  install.packages('RRF', repos='https://cloud.r-project.org/', lib=Sys.getenv('R_LIBS_USER')); \
  cat('✓ RRF installed successfully.\n') \
} else { \
  cat('✓ RRF already installed.\n') \
}"
echo

# ----------------------------
# 5) Workdir & threading
# ----------------------------
cd "$SLURM_SUBMIT_DIR"
mkdir -p logs

export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}

echo "Working directory: $(pwd)"
echo "Environment variables set:"
echo "  R_LIBS_USER: $R_LIBS_USER"
echo "  OMP_NUM_THREADS: $OMP_NUM_THREADS"
echo "  OPENBLAS_NUM_THREADS: $OPENBLAS_NUM_THREADS"
echo "  MKL_NUM_THREADS: $MKL_NUM_THREADS"
echo

# ----------------------------
# 7) Check for required R scripts
# ----------------------------
if [ ! -f "setup.r" ]; then
    echo "✗ ERROR: setup.r not found in $(pwd)"
    exit 1
fi

if [ ! -f "transition_feature_selection.r" ]; then
    echo "✗ ERROR: transition_feature_selection.r not found in $(pwd)"
    exit 1
fi

echo "✓ Required scripts found:"
echo "  - setup.r"
echo "  - transition_transition_feature_selection.r"
echo

# ----------------------------
# 8) Create R execution script
# ----------------------------
cat > run_pipeline_${SLURM_JOB_ID}.r << 'EOF'
#!/usr/bin/env Rscript

# Capture start time
start_time <- Sys.time()

cat("\n========================================\n")
cat("Starting Feature Selection Pipeline\n")
cat("========================================\n\n")

# load packages
  library(dplyr)
  library(stringr)
  library(parallel)
  library(foreach)
  library(doParallel)
  library(RRF)
  library(yaml)
  library(jsonlite)
  library(arrow)


# Source setup script
cat("Sourcing setup.r...\n")
tryCatch({
  source("setup.r")
  cat("✓ setup.r sourced successfully.\n\n")
}, error = function(e) {
  cat(sprintf("✗ ERROR sourcing setup.r: %s\n", e$message))
  quit(status = 1)
})

# Source utils.r
cat("Sourcing utils.r...\n")
tryCatch({
  source("utils.r")
  cat("✓ utils.r sourced successfully.\n\n")
}, error = function(e) {
  cat(sprintf("✗ ERROR sourcing utils.r: %s\n", e$message))
  quit(status = 1)
})

# Source feature selection functions
cat("Sourcing transition_feature_selection.r...\n")
tryCatch({
  source("transition_feature_selection.r")
  cat("✓ transition_feature_selection.r sourced successfully.\n\n")
}, error = function(e) {
  cat(sprintf("✗ ERROR sourcing transition_feature_selection.r: %s\n", e$message))
  quit(status = 1)
})

# Get configuration
cat("Loading configuration...\n")
config <- tryCatch({
  get_config()
}, error = function(e) {
  cat(sprintf("✗ ERROR getting config: %s\n", e$message))
  quit(status = 1)
})

cat("✓ Configuration loaded successfully.\n")
cat(sprintf("  Regionalization: %s\n", 
            ifelse(isTRUE(config[["regionalization"]]), "ENABLED", "DISABLED")))
cat(sprintf("  Feature selection dir: %s\n\n", config[["feature_selection_dir"]]))

# Run feature selection pipeline
cat("Starting feature selection...\n\n")
results <- tryCatch({
  transition_feature_selection(
    config = config,
    use_regions = isTRUE(config[["regionalization"]]),
    refresh_cache = FALSE,
    save_debug = TRUE,
    debug_dir = file.path(config[["feature_selection_dir"]]),
    do_collinearity = TRUE,
    do_grrf = TRUE
  )
}, error = function(e) {
  cat(sprintf("\n\n✗ ERROR in feature selection: %s\n", e$message))
  cat("Traceback:\n")
  print(traceback())
  quit(status = 1)
})

# Report results
end_time <- Sys.time()
elapsed <- difftime(end_time, start_time, units = "mins")

cat("\n========================================\n")
cat("Pipeline Completed Successfully\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f minutes\n", as.numeric(elapsed)))
cat(sprintf("Results: %d rows\n", nrow(results)))
cat(sprintf("Successful: %d\n", sum(results$status == "success")))
cat(sprintf("Failed: %d\n", sum(results$status != "success")))

# Save summary statistics
summary_file <- file.path(config[["feature_selection_dir"]], 
                          sprintf("run_summary_%s.txt", Sys.getenv("SLURM_JOB_ID")))
sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID")))
cat(sprintf("Start time: %s\n", start_time))
cat(sprintf("End time: %s\n", end_time))
cat(sprintf("Runtime: %.2f minutes\n", as.numeric(elapsed)))
cat(sprintf("Total transitions: %d\n", nrow(results)))
cat(sprintf("Successful: %d\n", sum(results$status == "success")))
cat(sprintf("Failed: %d\n", sum(results$status != "success")))
cat("\nStatus breakdown:\n")
print(table(results$status))
sink()

cat(sprintf("\n✓ Summary saved to: %s\n", summary_file))
cat("\nDone!\n")

# Exit with success
quit(status = 0)
EOF

# ----------------------------
# 9) Run job
# ----------------------------
echo "================================================================"
echo "Executing feature selection pipeline..."
echo "================================================================"
echo

Rscript --vanilla run_pipeline_${SLURM_JOB_ID}.r

# Capture exit status
EXIT_STATUS=$?

# ----------------------------
# 10) Cleanup
# ----------------------------
rm -f run_pipeline_${SLURM_JOB_ID}.r

# Report completion
echo
echo "================================================================"
echo "Job completed at: $(date)"
echo "Exit status: $EXIT_STATUS"
echo "================================================================"

exit $EXIT_STATUS